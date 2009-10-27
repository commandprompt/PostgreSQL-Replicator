/*-----------------------
 * master.c
 * 		The Mammoth Replication master tuple handling routines
 *
 * Portions Copyright (c) 1996-2007, PostgreSQL Global Development Group.
 * Copyright (c) 2007, Command Prompt, Inc.
 *
 * $Id: master.c 2148 2009-05-26 09:12:41Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/repl_master_lo_refs.h"
#include "catalog/repl_slave_roles.h"
#include "catalog/replication.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/common_tables.h"
#include "mammoth_r/master.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/mcp_local_queue.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/promotion.h"
#include "mammoth_r/repl_limits.h"
#include "mammoth_r/txlog.h"
#include "nodes/makefuncs.h"
#include "postmaster/replication.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

bool	PGRUseDumpMode = false;

static void PGRDumpTables(MCPQueue *queue);
static void DumpRoles(MCPQueue *queue);
static void dump_one_sequence(Relation rel, Snapshot snap);
static void dump_one_table(Relation rel, Snapshot snap, bool table_dump);


/*
 * PGRDumpAll
 *
 * Produce a complete dump of the all replicated relations.
 *
 * We first send a catalog dump in a transaction, and then one user table per
 * transaction.
 *
 * Notice that we don't grab locks on the tables that we're going to dump.
 * This is a bit problematic because the tables could be dropped in the
 * meantime.  So we must cope with that possibility (and not produce a dump
 * for such a table).
 *
 * A further problem is that a table could be modified (by ways of ALTER
 * TABLE), but there's nothing we can do about it (except maybe disallow ALTER
 * TABLE of replicated relations -- FIXME someday).
 *
 */
ullong
PGRDumpAll(MCPQueue *master_mcpq)
{
    ullong  dump_recno;
	PGRUseDumpMode = true;

    elog(DEBUG5, "FULL DUMP");

	PG_TRY();
	{
		/* Put DUMP_START at the start of the dump */
		StartTransactionCommand();
		PGRCollectDumpStart();

		/* Put data from replication catalogs to the queue */
		dump_recno = PGRDumpCatalogs(master_mcpq, CommitFullDump);
		Assert(dump_recno != InvalidRecno);

		/* Commit catalog dump transaction and record it in the TXLOG */
		CommitTransactionCommand();
		TXLOGSetCommitted(dump_recno);

		DumpRoles(master_mcpq);
		
		PGRDumpTables(master_mcpq);
	}
	PG_CATCH();
	{
		PGRUseDumpMode = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	PGRUseDumpMode = false;
    return dump_recno;
}

/* 
 * Add roles and members to the dump. Note that the relation locking order 
 * is important, keep it coherent with the code in users.c to avoid deadlocks.
 * MERGE: GetTransactionSnapshot was replaced with SnapshotNow for catalog access.
 */
static void
DumpRoles(MCPQueue *q)
{
	MemoryContext	dumpcxt;
	List		   *replRoles = NIL,
				   *replRoleIds = NIL;
	ListCell	   *cell;
	Relation		authidRel,
					replRolesRel;
	HeapTuple		scantuple;
	HeapScanDesc	scan;

	MCPFile		   *txdata;
	ullong			recno;
	Snapshot		snap;
	
	int				roles_no = 0;

	elog(DEBUG3, "ROLES DUMP");

	dumpcxt = AllocSetContextCreate(TopMemoryContext,
									"dump context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	StartTransactionCommand();

	MemoryContextSwitchTo(dumpcxt);

	/* Put transaction file to the queue */
	LWLockAcquire(ReplicationCommitLock, LW_EXCLUSIVE);

	txdata = MCPLocalQueueGetFile(MasterLocalQueue);
	recno = MCPQueueCommit(q, txdata, InvalidRecno);

	LWLockRelease(ReplicationCommitLock);

	/* prevent altering or deleting roles while we are collecting them */
	authidRel = heap_open(AuthIdRelationId, AccessShareLock);

	/* get list of replicated roles from repl_slave_roles */
	replRolesRel = heap_open(ReplSlaveRolesRelationId, AccessShareLock);
	scan = heap_beginscan(replRolesRel, SnapshotNow, 0, NULL);

	while (HeapTupleIsValid(scantuple = 
							heap_getnext(scan, ForwardScanDirection)))
	{
		char	   *rolename;
		bool		exists;

		Form_repl_slave_roles rolesForm =
				(Form_repl_slave_roles) GETSTRUCT(scantuple);
		rolename = NameStr(rolesForm->rolename);

		/* check whether the role is already in the dump */
		exists = false;
		foreach(cell, replRoles)
		{
			if (strncmp(rolename, lfirst(cell), NAMEDATALEN) == 0)
			{
				exists = true;
				break;
			}
		}
		if (!exists)
			replRoles = lcons(pstrdup(rolename), replRoles);
	}
	
	heap_endscan(scan);

	foreach (cell, replRoles)
	{
		char	   *rolename = lfirst(cell);
		
		/* Look for the role's tuple in pg_authid */
		HeapTuple	tuple = SearchSysCache(AUTHNAME, PointerGetDatum(rolename),
										   0, 0, 0);
		if (HeapTupleIsValid(tuple))
		{
			Oid 	roleid;
			CollectNewRole(authidRel, tuple, GetCurrentCommandId(true));
			roles_no++;

			/* add role id to the list of replicated role ids */
			roleid = HeapTupleGetOid(tuple);
			replRoleIds = lcons_oid(roleid, replRoleIds);
			
			ReleaseSysCache(tuple);
		}	
	}
	
	/* process membership infromation */
	if (replRoleIds != NIL)
	{
		Relation 	authMembersRel;
		
		authMembersRel = heap_open(AuthMemRelationId, AccessShareLock);
		scan = heap_beginscan(authMembersRel, snap, 0, NULL);
		/* scan auth_members and look for replication roles */
		while (HeapTupleIsValid(scantuple = 
								heap_getnext(scan, ForwardScanDirection)))
		{
			Form_pg_auth_members membersForm = 
								(Form_pg_auth_members) GETSTRUCT(scantuple);
			
			/* check whether eith role, member or grantor is replicated */					
			if (list_member_oid(replRoleIds, membersForm->roleid) ||
				list_member_oid(replRoleIds, membersForm->member) ||
				list_member_oid(replRoleIds, membersForm->grantor))
			{
				/* collect membership information */
				CollectGrantRole(authMembersRel, scantuple,
				 				 GetCurrentCommandId(true));
			}
		}
		
		heap_endscan(scan);
		heap_close(authMembersRel, AccessShareLock);	
	}

	heap_close(replRolesRel, AccessShareLock);
	heap_close(authidRel, AccessShareLock);
	
	/* If we added some roles - commit a transaction. Otherwise force commit of
	 * the empty transaction. HACK: PGRCollectTxCommit will discard an empty
	 * transaction and we can't allow this because transaction file is already
	 * enqueued, thus we have to use a special-purpose code.
	 */
	if (roles_no > 0)
		PGRCollectTxCommit(CommitTableDump);
	else
	{
		/* send this transaction as a part of the full dump (avoid skipping it) 
		 */
		PGRCollectEmptyTx(MCP_QUEUE_FLAG_TABLE_DUMP);
	}
	MCPLocalQueueSwitchFile(MasterLocalQueue);

	CommitTransactionCommand();
	TXLOGSetCommitted(recno);
	
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(dumpcxt);
}

/*
 * PGRDumpTables
 * 		Produce a complete dump of all the replicated tables
 * 		(except Replicator's catalogs)
 *
 * We do this in one transaction per table, taking care to use a snapshot
 * grabbed while holding an exclusive lock also used for putting the dump
 * label in the queue.  This way, concurrent transactions can commit either
 * before the snapshot and dump label, or after both; in either case, a slave
 * replaying the sequence of commands in the queue will end up with a
 * consistent view of that table.
 */
static void
PGRDumpTables(MCPQueue *queue)
{
	Snapshot	snap;
	MemoryContext dumpcxt;
	List	   *relids;
	ListCell   *cell;
	MCPFile	   *dump_end_file;
	ullong		dump_end_recno;

	dumpcxt = AllocSetContextCreate(TopMemoryContext,
									"dump context",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);

	StartTransactionCommand();

	MemoryContextSwitchTo(dumpcxt);
	
	snap = RegisterSnapshot(GetTransactionSnapshot());
	
	relids = get_replicated_relids(snap);
	
	UnregisterSnapshot(snap);

	foreach(cell, relids)
	{
		Oid			relid = lfirst_oid(cell);
		Relation	rel;
		MCPFile	   *data_file;
		ullong		recno;

		LWLockAcquire(ReplicationCommitLock, LW_EXCLUSIVE);
		snap = RegisterSnapshot(GetTransactionSnapshot());

		data_file = MCPLocalQueueGetFile(MasterLocalQueue);
		recno = MCPQueueCommit(queue, data_file, InvalidRecno);

		LWLockRelease(ReplicationCommitLock);

		rel = RelationIdGetRelation(relid);

		/* the table does not exist anymore -- skip it */
		if (!RelationIsValid(rel))
		{
			/* Avoid leaking a snapshot by unregistering it first */
			UnregisterSnapshot(snap);
			continue;
		}
		LockRelation(rel, ShareUpdateExclusiveLock);

		if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
			dump_one_sequence(rel, snap);
		else if (rel->rd_rel->relkind == RELKIND_RELATION)
			dump_one_table(rel, snap, false);
		else
		{
			/*
			 * skip non-dumpable relations, but make noise about it so the user
			 * has a chance to correct the problem
			 */
			elog(WARNING, "skipping \"%s\": not a table or sequence",
				 RelationGetRelationName(rel));
		}

		/* close relation and release the lock */
		relation_close(rel, ShareUpdateExclusiveLock);

		PGRCollectTxCommit(CommitTableDump);

		/* Create a new file with the original path for the local queue. */
		MCPLocalQueueSwitchFile(MasterLocalQueue);
		
		UnregisterSnapshot(snap);

		CommitTransactionCommand();

		/* Record transaction commit in TXLOG */
		TXLOGSetCommitted(recno);

		/* prepare for the next table */
		StartTransactionCommand();
	}

	CommitTransactionCommand();

	/* Put a DUMP_END transaction */
	StartTransactionCommand();

	/* Put empty transaction header with dump end flag */
	PGRCollectEmptyTx(MCP_QUEUE_FLAG_DUMP_END);
	
	dump_end_file = MCPLocalQueueGetFile(MasterLocalQueue);

	LWLockAcquire(ReplicationCommitLock, LW_EXCLUSIVE);
	dump_end_recno = MCPQueueCommit(queue, dump_end_file, InvalidRecno);
	LWLockRelease(ReplicationCommitLock);

	MCPLocalQueueSwitchFile(MasterLocalQueue);

	CommitTransactionCommand();
	TXLOGSetCommitted(dump_end_recno);

	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(dumpcxt);
}

/*
 * PGRDumpCatalogs
 * 		Create a dump of the replication catalogs.
 *
 * We do it in a single transaction. In case of mode other than CommitNoDump
 * we also acquire ReplicationCommitLock and add a dump file to the queue,
 * returning its recno. Otherwise InvalidRecno is returned.
 */
ullong
PGRDumpCatalogs(MCPQueue *master_mcpq, CommitDumpMode mode)
{
	List           *relids;
	ListCell       *cell;
	Snapshot		serializable;
	ullong			dump_recno;
	MCPFile		   *data_file;

	/* Should only be called in the master */
	AssertState(replication_enable && replication_master);
	/* Make sure the queue is ready */
	Assert(master_mcpq != NULL);
	Assert(MasterLocalQueue != NULL);

	/* Check if promotion is in progress */
	if (ReplPromotionData->promotion_block_relations)
		ereport(WARNING,
				(errmsg("CATALOG DUMP: promotion in progress")));

	if (mode != CommitNoDump)
	{
		ullong cleanup_recno;

		/* disallow concurrent transactions from committing after this point */
		LWLockAcquire(ReplicationCommitLock, LW_EXCLUSIVE);

		/*
	 	 * Take our "serializable" snapshot.  This must be done _after_ we have
	 	 * acquired the ReplicationCommitLock, to make sure no transaction commits
	 	 * between our snapshot and our putting of the dump label in the queue.
	 	 * Note that in CommitNoDump case ReplicationCommmitLock was aleady acquired
	 	 * by the caller.
	 	 */

		serializable = RegisterSnapshot(GetTransactionSnapshot());

		/*
		 * Remove data which would become obsolete after processing a dump
		 * from the queue.
	 	 */
		LockReplicationQueue(master_mcpq, LW_EXCLUSIVE);
		cleanup_recno = MCPQueueGetFirstRecno(master_mcpq);
		MCPQueueCleanup(master_mcpq, cleanup_recno);
		UnlockReplicationQueue(master_mcpq);

		/* Save the local queue old pathname */
		data_file = MCPLocalQueueGetFile(MasterLocalQueue);

		/*
	 	 * Enqueue transaction data file. Contrary to the normal transaction we do
	 	 * this before placing actual data into the file.
	 	 */
		dump_recno = MCPQueueCommit(master_mcpq, data_file, InvalidRecno);

		/*
	 	 * We can release the ReplicationCommitLock here, because from this point on
	 	 * it's safe that other transactions start committing things.  Their data
	 	 * will be in the queue after our DumpLabel.
	 	 */
		LWLockRelease(ReplicationCommitLock);
	}
	else
	{
		/*
		 * If this call is a part of non-dump transaction then make sure that
		 * it is peformed during commit stage, i.e. we are holding a proper
		 * commit lock. However do grab a snapshot for acquiring tuples from
		 * replication catalogs.
		 */
		Assert(LWLockHeldByMe(ReplicationCommitLock));
		serializable = RegisterSnapshot(GetTransactionSnapshot());

		dump_recno = InvalidRecno;
	}

	elog(DEBUG5, "CATALOG DUMP");

	if (dump_recno != InvalidRecno)
		elog(DEBUG5, "DUMP record number: "UNI_LLU, dump_recno);
	else
		elog(DEBUG5, "DUMP record number: not determined yet");

	/*
	 * Truncate large objects references relation and put truncate request for
	 * slaves in the case of full dump.
	 */
	if (mode & CommitFullDump)
		heap_truncate(list_make1_oid(ReplMasterLoRefsId));

	relids = get_catalog_relids(NIL);

	foreach(cell, relids)
	{
		Oid			relid = lfirst_oid(cell);
		Relation	rel;

		rel = heap_open(relid, ShareUpdateExclusiveLock);

		dump_one_table(rel, serializable, false);

		/* close relation and release the lock */
		heap_close(rel, ShareUpdateExclusiveLock);
	}

	/* In case of non-dump transaction we were called from PGRCollectTxCommit */
	if (mode != CommitNoDump)
	{
		PGRCollectTxCommit(mode | CommitCatalogDump);

		/* Create a new file with the original path for the local queue. */
		MCPLocalQueueSwitchFile(MasterLocalQueue);
	}
	
	UnregisterSnapshot(serializable);

	elog(DEBUG5, "CATALOG DUMP collected");
	return dump_recno;
}

/*
 * PGRDumpSingleTable
 *
 * Produce a dump of a single table or sequence. Note that in case of
 * non-existent relation we still reserve a recno for the dump but don't set
 * the committed flag in TXLOG.
 */
void
PGRDumpSingleTable(MCPQueue *master_mcpq, char *relpath)
{
	Snapshot	snap;
    MCPFile   *dumpfile;
    MemoryContext   dumpcxt;
    char    *relname,
            *nspname;
    char    relpath_copy[MAX_REL_PATH];
    RangeVar    *rv;
    Relation    rel;
    ullong  recno;
    bool    apply;

    /* Should only be called on master */
    Assert(replication_enable & replication_master);
    /* Sanity checks */

	Assert(relpath != NULL);
    Assert(MasterLocalQueue != NULL);

    /* Do memory allocation in a separate context */
    dumpcxt = AllocSetContextCreate(TopMemoryContext,
                                    "table dump context",
                                    ALLOCSET_DEFAULT_MINSIZE,
                                    ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_DEFAULT_MAXSIZE);

    StartTransactionCommand();

    MemoryContextSwitchTo(dumpcxt);

	LWLockAcquire(ReplicationCommitLock, LW_EXCLUSIVE);

    /*
     * Acquire snapshot and put a transaction to the queue while
     * no other process can interfere (due to the lock). This
     * guarantees us that the data will be restored in the correct
     * order on the slave.
     */
	snap = RegisterSnapshot(GetTransactionSnapshot());

    dumpfile = MCPLocalQueueGetFile(MasterLocalQueue);

    /* Add this transaction to the queue */
    recno = MCPQueueCommit(master_mcpq, dumpfile, InvalidRecno);

    LWLockRelease(ReplicationCommitLock);

    elog(DEBUG5, "Producing TABLE DUMP for %s", relpath);

    /* convert relpath to the pair of relname/relnamepsace */
    strcpy(relpath_copy, relpath);

    relname = strtok(relpath_copy, ".");
    nspname = strtok(NULL, ".");

    Assert(relname != NULL);
    Assert(nspname != NULL);

    /*
     * Open a target relation and grab a lock (preventing vacuum
     * to run on this relation concurrently with this transaction).
     */
    rv = makeRangeVar(relname, nspname, -1);
    rel = heap_openrv(rv, ShareUpdateExclusiveLock);

    if (RelationIsValid(rel))
    {
        PG_TRY();
        {
            /* Data should be in the main (non-sequence) data file */
            PGRUseDumpMode = true;
            switch (rel->rd_rel->relkind)
            {
				/* 
				 * Force collecting referenced large objects with
				 * the last param of dump_one_table set to true.
				 */
                case RELKIND_RELATION:
                    dump_one_table(rel, snap, true);
                break;
                case RELKIND_SEQUENCE:
                    dump_one_sequence(rel, snap);
                break;

                default:
	    	        elog(WARNING, "skipping \"%s\": not a table or sequence",
				        RelationGetRelationName(rel));
                    apply = false;
            }
        }
        PG_CATCH();
        {
            PGRUseDumpMode = false;
			UnregisterSnapshot(snap);
            PG_RE_THROW();
        }
        PG_END_TRY();

        PGRUseDumpMode = false;

        /* Close the target relation and add the data file to the queue */
        heap_close(rel, ShareUpdateExclusiveLock);

        PGRCollectTxCommit(CommitTableDump);
        apply = true;

        /* Switch to the new file for further transactions */
        MCPLocalQueueSwitchFile(MasterLocalQueue);
    }
    else
    {
        elog(WARNING, "\"%s\" doesn't exist", relpath);
        apply = false;
    }

	UnregisterSnapshot(snap);

    CommitTransactionCommand();

    /* Record transaction commit in TXLOG */
    if (apply)
       TXLOGSetCommitted(recno);

    MemoryContextSwitchTo(TopMemoryContext);
    MemoryContextDelete(dumpcxt);
}

/*
 * dump_one_sequence
 *		Dump a single sequence.
 */
static void
dump_one_sequence(Relation rel, Snapshot snap)
{
	HeapScanDesc	scandesc;
	HeapTuple		tuple;
	int				cmd_id = 1;

	/*
	 * XXX this is very ugly.  We should use the functions in sequence.c
	 * to read the current value instead.
	 */
	scandesc = heap_beginscan(rel, snap, 0, NULL);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (HeapTupleIsValid(tuple))
	{
		bool	isnull;
		int64	value;

		value = DatumGetInt64(heap_getattr(tuple, 2, RelationGetDescr(rel),
										   &isnull));
		if (isnull)
			elog(ERROR, "sequence data unexpectedly null");

		PGRCollectSequence(rel, value, cmd_id++);
	}

	heap_endscan(scandesc);
}

/*
 * dump_one_table
 *		Dump a single table.
 */
static void
dump_one_table(Relation rel, Snapshot snap, bool table_dump)
{
	HeapScanDesc	scandesc;
	HeapTuple		tuple;
	int				cmd_id = 1;

	scandesc = heap_beginscan(rel, snap, 0, NULL);

	/* Send a truncate */
	PGRCollectTruncate(rel, cmd_id++);

	while (HeapTupleIsValid(tuple = heap_getnext(scandesc, ForwardScanDirection)))
	{
		/*
		 * Do not change command id here. By doing this we are writing
		 * headers only once for each table.
		 */
		PGRCollectInsert(rel, tuple, cmd_id, table_dump);
	}

	heap_endscan(scandesc);
}
