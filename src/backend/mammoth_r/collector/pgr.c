/*-----------------------
 * pgr.c
 * 		The Mammoth Replication master tuple handling routines
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group.
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: pgr.c 2220 2009-08-25 12:17:46Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/replication.h"
#include "catalog/repl_master_lo_refs.h"
#include "catalog/mammoth_indexing.h"
#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "libpq/libpq-fs.h"
#include "libpq/pqformat.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/backend_tables.h"
#include "mammoth_r/collcommon.h"
#include "mammoth_r/collector.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/master.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/mcp_local_queue.h"
#include "mammoth_r/promotion.h"
#include "mammoth_r/repl_tuples.h"
#include "miscadmin.h"
#include "parser/parse_oper.h"
#include "postmaster/replication.h"
#include "storage/large_object.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


static void		PGRUpdateLORef(int lo_oid, int cmd, 
							   HeapTuple tuple, int colNum, bool force_send);
static void		PGRUpdateLOsRef(HeapTuple tuple, int cmd, Relation tup_rel, 
								bool force_send);
static bool		PGRHasRef(Oid loid);
static void		PGRCollectTruncateLO(CommandId cid);
static void		PGRDumpHeaders(int cmd_type, char *relpath, int command_id,
							   int natts, bool trans);
static void		PGRDumpCommandEnd(void);
static void		dump_write_lo(HeapTuple tuple, Oid loid);
static char	   *build_relpath(Relation relation);
static void		build_network_tuple(HeapTuple tuple, TupleDesc tupdesc,
									StringInfo buf);
static void 	collect_role_members(Relation authMembersRel, 
									 HeapTuple membersTuple, 
									 CommandId cid, 
									 bool revoke, bool revoke_admin);
static HeapTuple make_repl_members_tuple(HeapTuple membersTuple,
										 TupleDesc *replMembersDesc, 
										 bool revoke_admin);

static HeapTuple make_diff_tuple(HeapTuple old_tuple, HeapTuple new_tuple, 				
								 TupleDesc tupdesc, bool **send_mask, 
								 int *mask_natts);

static HeapTuple make_index_tuple(Relation indexRel, HeapTuple tuple, 
								  TupleDesc tupdesc);



static CommandId	prev_cid = -1;
static CommandType	prev_cmd = UNDEFINED;

bool 	initial_is_tablecmd = false;
bool	current_is_tablecmd = false;

void
PGRCollectDumpStart(void)
{
    /* Request the slaves to truncate their large objects references */
	PGRCollectTruncateLO(GetCurrentCommandId(true));
}

void
PGRCollectTxBegin(void)
{
	if (!IsUnderPostmaster || !replication_enable || !replication_master)
		return;

	Assert(MasterMCPQueue != NULL);
	Assert(MasterLocalQueue != NULL);

	prev_cid = -1;
	prev_cmd = UNDEFINED;
	initial_is_tablecmd = false;
	current_is_tablecmd = false;

	/* XXX do we really need this here? */
	MCPLocalQueueSwitchFile(MasterLocalQueue);
}

void
PGRCollectTxAbort(void)
{
	MemoryContext oldcxt;
	if (!IsUnderPostmaster || !replication_enable || !replication_master)
		return;

	Assert(MasterMCPQueue != NULL);
	Assert(MasterLocalQueue != NULL);

	prev_cid = -1;
	prev_cmd = UNDEFINED;
	initial_is_tablecmd = false;
	current_is_tablecmd = false;

	/* Clear memory allocated for sequence replication in prev. transaction */
    MemoryContextReset(ReplRollbackContext);
	
	/* Switch to the safe memory context to deal with sequence replication */
	oldcxt = MemoryContextSwitchTo(ReplRollbackContext);

	if (MCPLocalQueueIsEmpty(MasterLocalQueue, false))
		MCPLocalQueueSwitchFile(MasterLocalQueue);
	else
		MCPLocalQueueFinalize(MasterLocalQueue, MCP_QUEUE_FLAG_DATA, false);

	MemoryContextSwitchTo(oldcxt);

}

/* Queue actions on transaction begin/commit/abort etc */
void
PGRCollectTxCommit(CommitDumpMode dump_mode)
{
	bool	tx_is_empty;

	Assert(MasterMCPQueue != NULL);
	Assert(MasterLocalQueue != NULL);

	if (!IsUnderPostmaster || !replication_enable || !replication_master)
		 return;

	/* if current transaction deals with 'alter table .. enable replication'
	 * kind of commands - collect the catalog dump here.
	 */
	if (initial_is_tablecmd)
	{
		Assert(dump_mode == CommitNoDump);

		elog(DEBUG5, "Transaction requires a dump of replication catalogs");

		/* allow appending data to the queue like in normal transaction */
		initial_is_tablecmd = false;
		PGRUseDumpMode = true;

        /*
         * Remove information about previous commands. This should be in sync
         * with what we are doing in PGRCollectTxBegin.
         */
        prev_cid = -1;
        prev_cmd = UNDEFINED;

		PGRDumpCatalogs(MasterMCPQueue, CommitNoDump);

		PGRUseDumpMode = false;

		/* Instruct the following code that we are committing a catalog dump */
		dump_mode = CommitCatalogDump;
	}

	/* Check if current transaction is empty */
	tx_is_empty = MCPLocalQueueIsEmpty(MasterLocalQueue, true);

	elog(DEBUG5, "Commit, transaction empty state: %s", 
		 tx_is_empty ? "EMPTY" : "NOT EMPTY");

    if (!tx_is_empty)
	{
		int		flags = 0;

		/* Write command end if necessary */
		PGRDumpCommandEnd();

		if (dump_mode == CommitNoDump)
			flags = MCP_QUEUE_FLAG_DATA;
		else
		{
			/* Check for different flavors of dump */
			if (dump_mode & CommitFullDump)
			{
				flags |= MCP_QUEUE_FLAG_DUMP_START;
#ifdef NOT_USED
				/* 
				 * TODO: put truncate flags for a full dump if it's type is 
				 * not refresh
				 */
				flags |= MCP_QUEUE_FLAG_TRUNC;
#endif
			}
			if (dump_mode & CommitCatalogDump)
				flags |= MCP_QUEUE_FLAG_CATALOG_DUMP;
			if (dump_mode & CommitTableDump)
				flags |= MCP_QUEUE_FLAG_TABLE_DUMP;
		}

		MCPLocalQueueFinalize(MasterLocalQueue, flags, true);
	}
   	else
		MCPLocalQueueSwitchFile(MasterLocalQueue);
}

/* 
 * PGRCollectTxCommit replacement in case we really need to put empty
 * transaction with a proper transaction header in the queue. This is
 * used only in the dump code
 */
void
PGRCollectEmptyTx(int flags)
{	
	Assert(MasterMCPQueue != NULL);
	Assert(MasterLocalQueue != NULL);

	if (!IsUnderPostmaster || !replication_enable || !replication_master)
		 return;

	MCPLocalQueueFinishEmptyTx(MasterLocalQueue, flags);
}

/* Put end of the command mark */
static void
PGRDumpCommandEnd(void)
{
	/* 
	 * These are command types for which the corresponding commands
	 * may have several data tuples at once.
	 */
	if (prev_cmd == PGR_CMD_INSERT || 
		prev_cmd == PGR_CMD_UPDATE ||
		prev_cmd == PGR_CMD_DELETE ||
		prev_cmd == PGR_CMD_GRANT_MEMBERS ||
		prev_cmd == PGR_CMD_REVOKE_MEMBERS)
	{
		elog(DEBUG5, "writing command end marker");
		MCPLocalQueueAppend(MasterLocalQueue, NULL, 
							0, TXL_FLAG_COMMAND_END, true);
	}
}

void
PGRCollectSequence(Relation relation, int64 value, CommandId command_id)
{
	char           *relpath;
	MemoryContext   qcontext;
	MemoryContext   oldcontext;

	elog(DEBUG5,
		 "PGRCollectSequence: relation %s, CID %d",
		 RelationGetRelationName(relation), command_id);

	Assert(relation->rd_rel->relkind == RELKIND_SEQUENCE);

	/* Allocate a temporary memory context so that we don't leak memory */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* build our relpath value */
	relpath = build_relpath(relation);
	PGRDumpHeaders(PGR_CMD_SEQUENCE, relpath, command_id, 0, false);
	/* 
	 * The last param of the following calls denotes that the data
	 * is non-transactional, i.e. should be replicated on rollback.
	 */
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, false);
	pfree(relpath);

	MCPLocalQueueAppend(MasterLocalQueue, (char *) &value, sizeof(int64), 0, false);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

void
PGRCollectTruncate(Relation relation, CommandId command_id)
{
	char           *relpath;
	MemoryContext   qcontext;
	MemoryContext   oldcontext;

	elog(DEBUG5,
		 "PGRCollectTruncate: relation %s, CID %d",
		 RelationGetRelationName(relation), command_id);

	Assert(relation->rd_rel->relkind == RELKIND_RELATION);

	/* Allocate a temporary memory context so that we don't leak memory */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* build our relpath value */
	relpath = build_relpath(relation);
	PGRDumpHeaders(PGR_CMD_TRUNCATE, relpath, command_id, 0, true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, true);
	pfree(relpath);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

/*
 * PGRUpdateLOsRef
 * Grabs the master OIDs from LO columns and passes them to PGRUpdateLORef.
 * See comments in PGRUpdateLORef for the meaning of the last parameter.
 */
static void
PGRUpdateLOsRef(HeapTuple tuple, int cmd, Relation tup_rel, bool force_send)
{
	bool	isnull;
	List   *attlist;
	ListCell *attcell;

	attlist = get_relation_lo_columns(tup_rel);

	foreach(attcell, attlist)
	{
		int		attno = lfirst_int(attcell);
		Oid		loid;
		Datum	attr;

		attr = heap_getattr(tuple, attno, RelationGetDescr(tup_rel), &isnull);
		/* skip NULL values ... */
		if (isnull)
			continue;

		/* ... and InvalidOid too */
		loid = DatumGetObjectId(attr);
		if (!OidIsValid(loid))
			continue;

		PGRUpdateLORef(loid, cmd, tuple, attno, force_send);
	}
	list_free(attlist);
}

/*
 * PGRUpdateLORef
 *
 * Updates references field in master_lo_refs if lo_oid is present in
 * master_lo_refs. Otherwise insert the new tuple to the master_lo_refs and
 * call PGRDumpNewLO. The last param forces collecting a large object even
 * if the references to the corresponding oid already exist in master_lo_refs.
 * The references are also not updated if it is set to true.
 */
static void
PGRUpdateLORef(int lo_oid, int cmd, HeapTuple tuple, int colNum, bool force_send)
{
	Relation        master_lo_refs;
	ScanKeyData     rel_keys[1];
	SysScanDesc		scandesc;
	HeapTuple       pgr_tuple, newtuple;
	Datum           values[Natts_master_lo_refs];
	char            nulls[Natts_master_lo_refs],
					repls[Natts_master_lo_refs];
	LargeObjectDesc *lobj;
	TupleDesc		desc;

	/* Sanity check */
	Assert(!force_send || cmd == PGR_CMD_INSERT);
	
	/* FIXME -- most of this stuff belongs in catalog/replication.c */

	MemSet(values, 0, Natts_master_lo_refs * sizeof(Datum));
	MemSet(nulls, ' ', Natts_master_lo_refs);
	MemSet(repls, ' ', Natts_master_lo_refs);

	master_lo_refs = heap_open(ReplMasterLoRefsId, RowExclusiveLock);

	desc = RelationGetDescr(master_lo_refs);

	/* Index scan master_lo_refs to find the tuple with oid=lo_oid */
	ScanKeyInit(&rel_keys[0], Anum_master_lo_refs_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(lo_oid));

	scandesc = systable_beginscan(master_lo_refs, ReplMasterLoRefsLoidIndexId,
								  true, SnapshotNow, 1, rel_keys);

	pgr_tuple = systable_getnext(scandesc);

	if (HeapTupleIsValid(pgr_tuple) && !force_send)
	{
		bool	isnull;
		int		val;
		Datum	attr;

		/*
		 * Tuple is present in master_lo_refs.  Update the "references" field
		 * according to command type.
		 */
		attr = heap_getattr(pgr_tuple, Anum_master_lo_refs_refcount,
							desc, &isnull);
		Assert(!isnull);
		val = DatumGetInt32(attr);

		if (cmd == PGR_CMD_INSERT)
			val++;
		else if (cmd == PGR_CMD_DELETE)
			val--;
		else
			elog(ERROR, "invalid command %s while updating LO refcount",
				 CmdTypeAsStr(cmd));

		values[Anum_master_lo_refs_refcount - 1] = Int32GetDatum(val);
		repls[Anum_master_lo_refs_refcount - 1] = 'r';

		newtuple = heap_modifytuple(pgr_tuple, desc, values, nulls, repls);
		simple_heap_update(master_lo_refs, &pgr_tuple->t_self, newtuple);

		CatalogUpdateIndexes(master_lo_refs, newtuple);

		heap_freetuple(newtuple);
		systable_endscan(scandesc);

		heap_close(master_lo_refs, RowExclusiveLock);

		/*
		 * Increment the command counter to make this tuple visible to the
		 * further commands in this transaction.  For example, in the case of
		 * processing a tuple with two or more large object references pointing
		 * to the same large object, when calling this function for the second
		 * LO column, we should be aware of the changes made by the processing
		 * of the first lo column.
		 */
		CommandCounterIncrement();
	}
	else
	{
		int			lo_size,
					lo_tuple_size;
		HeapTuple	newtuple;

		if (!HeapTupleIsValid(pgr_tuple))
		{
			/*
		 	 * Oid is not present in master_lo_refs.  Insert a tuple with this oid
		 	 * and refs=1 and call PGRCollectNewLO to send this LO data to slave.
		 	 */
			values[Anum_master_lo_refs_oid - 1] = ObjectIdGetDatum(lo_oid);
			values[Anum_master_lo_refs_refcount - 1] =
					(cmd == PGR_CMD_INSERT) ? Int32GetDatum(1) : Int32GetDatum(0);

			newtuple = heap_formtuple(desc, values, nulls);

			simple_heap_insert(master_lo_refs, newtuple);
			CatalogUpdateIndexes(master_lo_refs, newtuple);

			heap_freetuple(newtuple);
			systable_endscan(scandesc);

			heap_close(master_lo_refs, RowExclusiveLock);

			/*
		 	 * Increment command counter to make this tuple visible to future
		 	 * commands.  See related CCI comment above.
		 	 */
			CommandCounterIncrement();
		}
		
		/* FIXME -- use an appropiate memory context here */
		/* Dump large object data with PGR_CMD_NEW_LO command */
		lobj = inv_open(ObjectIdGetDatum(lo_oid), INV_READ | INV_WRITE,
						CurrentMemoryContext);
		if (!lobj)
			ereport(ERROR,
					(errmsg("Large object referenced in master_lo_refs"
							" doesn't exist, LO oid = %d", lo_oid)));

		/* Determine the size of the large object */
		lo_size = inv_seek(lobj, 0, SEEK_END);
		inv_seek(lobj, 0, SEEK_SET);

		/* Create the new tuple to contain lo data */
		newtuple = palloc(sizeof(HeapTupleData));

		lo_tuple_size = lo_size + offsetof(HeapTupleHeaderData, t_bits);

		newtuple->t_data = palloc(lo_tuple_size);
		newtuple->t_len = lo_size;

		/* Read the lo data to pass to the PGRCollectNewLO */
		if (inv_read(lobj, (char *)newtuple->t_data->t_bits, lo_size) != lo_size)
			ereport(ERROR,
					(errmsg("Error while reading %d bytes from LO oid = %d",
							lo_size, lo_oid)));

		/* Put the collected large object data into the queue */
		PGRCollectNewLO(newtuple, lo_oid, colNum, GetCurrentCommandId(false));

		pfree(newtuple->t_data);
		pfree(newtuple);
	}
}

/*
 * PGRHasRef
 *
 * Return whether a given LO oid has references in replicated tables
 */
static bool
PGRHasRef(Oid loid)
{
	int			refs;
	bool		hasref;

	refs = LOoidGetMasterRefs(loid, false);
	hasref = refs > 0;

	elog(DEBUG5, "PGRHasRef Oid %u: %s", loid, hasref ? "yes" : "no");
	return hasref;
}

void
PGRCollectUpdate(Relation relation, HeapTuple newtuple, HeapTuple oldtuple,
				 CommandId command_id)
{
	MemoryContext qcontext;
	MemoryContext oldcontext;
	Relation 	pkindex;
	TupleDesc	tupdesc;
	HeapTuple	pktuple;
	HeapTuple	send_tuple;
	char	   *relpath;
	bool	   *send_mask;
	int			mask_natts;
	StringInfoData	buf;

	tupdesc = RelationGetDescr(relation);

	elog(DEBUG5,
		 "PGRCollectUpdate: relation %s, CID %d",
		 RelationGetRelationName(relation), command_id);

	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* Update possible large object references contained in the tuples */
	PGRUpdateLOsRef(newtuple, PGR_CMD_DELETE, relation, false);
	PGRUpdateLOsRef(oldtuple, PGR_CMD_INSERT, relation, false);

	/*
	 * Form a tuple containing only the primary key values of the original
	 * tuple (other columns will be NULL).  This will be used in the slave to
	 * find the tuple that needs to be updated.  This tuple is stored in
	 * the variable old_tuple.
	 *
	 * NB -- we must cope with the case where the new tuple has more
	 * attributes than the old tuple.  They may have been added by an ALTER
	 * TABLE ADD COLUMN.
	 *
	 * FIXME -- we still have a problem when the number of attributes
	 * _decreases_.  This typically cannot happen, because ALTER TABLE DROP
	 * COLUMN does not remove attributes, but rather marks them as
	 * "attisdropped".  However, I think certain ALTER TABLE commands may
	 * rewrite the table and remove those useless attributes.
	 */	
	send_tuple = make_diff_tuple(oldtuple, newtuple, tupdesc, 
								 &send_mask, &mask_natts);

	/*
	 * For the primary key tuple, we only need to send the PK values, so
	 * fill "nulls" with nulls and mark only the PK attributes as not-null.
	 */
	pkindex = PGRFindPK(relation, AccessShareLock);
	
	pktuple = make_index_tuple(pkindex, oldtuple, tupdesc);

	relation_close(pkindex, AccessShareLock);

	elog(DEBUG5, "original tuple is %s", tupleToString(oldtuple, tupdesc));
	elog(DEBUG5, "new tuple is %s", tupleToString(newtuple, tupdesc));
	elog(DEBUG5, "PK to update is %s", tupleToString(pktuple, tupdesc));
	elog(DEBUG5, "tuple to send is %s", tupleToString(send_tuple, tupdesc));

	/* build our relpath value */
	relpath = build_relpath(relation);
	if ((command_id != prev_cid) || prev_cmd != PGR_CMD_UPDATE)
		PGRDumpHeaders(PGR_CMD_UPDATE, relpath, command_id,
					   HeapTupleHeaderGetNatts(send_tuple->t_data), true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);
	pfree(relpath);

	/* send the tuple with the new values ... */
	build_network_tuple(send_tuple, tupdesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	/* then the tuple with the PK of the tuple to update ... */
	build_network_tuple(pktuple, tupdesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	/*
	 * And finally send the update mask.  We could send a bitmap here and save
	 * a few bytes.
	 */
	buf.data = send_mask;
	buf.len = mask_natts;
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

/*
 * PGRCollectNewLO
 *		Replicate creation of a new large object
 */
void
PGRCollectNewLO(HeapTuple tuple, Oid loid, int colNo, CommandId cid)
{
	MemoryContext qcontext;
	MemoryContext oldcontext;
	char		  relpath[MAX_REL_PATH];

	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* build our relpath value */
	snprintf(relpath, MAX_REL_PATH, "%s.%s", "pg_catalog", "pg_largeobject");
	PGRDumpHeaders(PGR_CMD_NEW_LO, relpath, cid, 0, true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);
	dump_write_lo(tuple, loid);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

#ifdef DEBUG_REPLICATOR
static void
displayStringInfo(StringInfo str, StringInfo result)
{
	int	i;

	initStringInfo(result);

	for (i = 0; i < str->len; i++)
	{
		appendStringInfo(result, "%02X ", str->data[i]);
		if (i % 4 == 3)
			appendStringInfo(result, " ");
		if (i % 16 == 15)
			appendStringInfo(result, "\n");
	}
	appendStringInfo(result, " end \n");
}
#endif

/*
 * Given a tuple and corresponding descriptor, return a StringInfo containing
 * the over-the-wire representation of it.
 */
static void
build_network_tuple(HeapTuple tuple, TupleDesc tupdesc, StringInfo buf)
{
	int		i;
	volatile char   *prev_encoding = NULL; /* avoid resetting this var 
											  by siglongjmp */

	initStringInfo(buf);

	PG_TRY();
	{
		/* 
		 * XXX: we can't set a different encoding if current transaction
		 * is not in 'in progress' state (i.e. if it is commiting). This 
		 * currently affects only catalog dump transactions.
		 */
		if (replication_perform_encoding_conversion && IsTransactionState())
		{
			/* String data will be automatically converted to UTF8. */
			prev_encoding = pstrdup(GetConfigOption("client_encoding"));
			SetConfigOption("client_encoding", "utf8", 
							PGC_S_SESSION, PGC_S_CLIENT);
		}

		for (i = 0; i < tupdesc->natts; i++)
		{
			bool	isnull;
			Datum	attr;

			/* skip dropped columns */
			if (tupdesc->attrs[i]->attisdropped)
				continue;

			attr = heap_getattr(tuple, i + 1, tupdesc, &isnull);

			if (!isnull)
			{
				Oid		typid;
				Datum	output;
				Oid		typsend;
				bool	typisvarlena;
				uint32	len;

				/* Fetch the type output info */
				typid = tupdesc->attrs[i]->atttypid;
				getTypeBinaryOutputInfo(typid, &typsend, &typisvarlena);

				/* invoke the output function */
				output = OidFunctionCall1(typsend, attr);

				/* Make sure this is not compressed or toasted value */
				Assert(VARATT_IS_EXTENDED(DatumGetPointer(output)) == 0);
				/* append the value, which includes the byte length */
				len = VARSIZE(DatumGetPointer(output));

				/* 
			 	 * XXX: hack - put output data length as a first 4
			 	 * bytes converted to network order. The actual
				 * data that should be there is a size + varlena
				 * header bits, but since we need only the size
				 * field on the receiving end we can just skip
				 * compressed and TOAST bits and all the related
				 * complexety. Note that we don't expect compressed
				 * or toasted data here.
				 */
				*((int32 *) DatumGetPointer(output)) = htonl(len);

				appendBinaryStringInfo(buf, DatumGetPointer(output), len);

				/* and get rid of the intermediate result */
				pfree(DatumGetPointer(output));
			}
			else
			{
				/* for a NULL, we send a length of -1 */
				pq_sendint(buf, -1, sizeof(uint32));
			}
		}
#ifdef DEBUG_REPLICATOR
		{
			StringInfoData	stringified;

			displayStringInfo(buf, &stringified);
			elog(DEBUG5, "complete buffer to send:\n%s", stringified.data);
			pfree(stringified.data);
		}
#endif
	}
	PG_CATCH();
	{
		/* Set back a proper client encoding on error */
		if (replication_perform_encoding_conversion && IsTransactionState())
			SetConfigOption("client_encoding", (const char *)prev_encoding, 
							PGC_S_SESSION, PGC_S_CLIENT);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Set back a proper client encoding */
	if (replication_perform_encoding_conversion && IsTransactionState())
	{
		SetConfigOption("client_encoding", (const char *)prev_encoding, 
						PGC_S_SESSION, PGC_S_CLIENT);
		pfree((char *)prev_encoding);
	}

}

/* 
 * Prepares data of the INSERT command to be put into the queue. The last
 * parameter indicates  whether  we should force collecting related large
 * objects  even if the  catalogs indicate that  we've already  sent them
 * (required for a per-table dump).
 */
void
PGRCollectInsert(Relation relation, HeapTuple tuple, CommandId command_id, 
				 bool lo_tabledump_mode)
{
	MemoryContext	qcontext;
	MemoryContext	oldcontext;
	StringInfoData	buf;
	char		   *relpath;
	TupleDesc		tupdesc;

	elog(DEBUG5,
		 "PGRCollectInsert: relation %s, CID %d",
		 RelationGetRelationName(relation), command_id);

	tupdesc = RelationGetDescr(relation);

	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	PGRUpdateLOsRef(tuple, PGR_CMD_INSERT, relation, lo_tabledump_mode);

	/* add the relation to the transaction's list and possibly send a header */
	relpath = build_relpath(relation);
	if ((command_id != prev_cid) || prev_cmd != PGR_CMD_INSERT)
		PGRDumpHeaders(PGR_CMD_INSERT, relpath, command_id, 
					   HeapTupleHeaderGetNatts(tuple->t_data), true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);
	pfree(relpath);

	/* now send the tuple data */
	build_network_tuple(tuple, tupdesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	elog(DEBUG5, "tuple to insert is %s", tupleToString(tuple, tupdesc));

	MemoryContextSwitchTo(oldcontext);

	MemoryContextDelete(qcontext);
}

/*
 * Note that DELETE is the only command that only takes an ItemPointer.
 */
void
PGRCollectDelete(Relation relation, HeapTuple oldtuple, CommandId command_id)
{
	HeapTuple		tuple;
	Datum		   *values;
	bool		   *nulls;
	Form_pg_index	indexForm;
	Relation		pkindex;
	int				natts;
	int				pk_natts;
	int				i;
	MemoryContext	qcontext;
	MemoryContext	oldcontext;
	char		   *relpath;
	TupleDesc		tupdesc;
	StringInfoData	buf;

	elog(DEBUG5, "PGRCollectDelete: relation %s, CID %d",
		 RelationGetRelationName(relation), command_id);
		
	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	pkindex = PGRFindPK(relation, AccessShareLock);

	indexForm = pkindex->rd_index;
	pk_natts = indexForm->indnatts;
	tupdesc = RelationGetDescr(relation);

	natts = tupdesc->natts;

	if (RelationGetNamespace(relation) != PG_CATALOG_NAMESPACE)
	{
		/* Update large object references for this tuple */
		PGRUpdateLOsRef(oldtuple, PGR_CMD_DELETE, relation, false);
	}

	/* Fetch the original tuple values */
	values = palloc(sizeof(Datum) * natts);
	nulls = palloc(sizeof(bool) * natts);
	heap_deform_tuple(oldtuple, tupdesc, values, nulls);

	/* Set the PK atts as not-null, the rest as nulls */
	MemSet(nulls, true, natts);
	for (i = 0; i < pk_natts; i++)
		nulls[indexForm->indkey.values[i] - 1] = false;

	/* Close the primary key */
	relation_close(pkindex, AccessShareLock);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	elog(DEBUG5, "PK of tuple to delete is %s", tupleToString(tuple, tupdesc));

	/* add the table to the list, and possibly send a header */
	relpath = build_relpath(relation);
	if ((command_id != prev_cid) || prev_cmd != PGR_CMD_DELETE)
		PGRDumpHeaders(PGR_CMD_DELETE, relpath, command_id, 
					   HeapTupleHeaderGetNatts(tuple->t_data), true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, true);
	pfree(relpath);

	/* finally send the tuple itself */
	build_network_tuple(tuple, tupdesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

/*
 * PGRCollectWriteLO
 *		Replicate writing into a large object
 */
void
PGRCollectWriteLO(HeapTuple tuple, Oid loid, CommandId cid)
{
	char			relpath[MAX_REL_PATH];
	MemoryContext	qcontext;
	MemoryContext	oldcontext;

	/*
	 * Only collect inv_write for large objects referenced from master_lo_refs
	 */
	if (!PGRHasRef(loid))
	{
		elog(DEBUG5, "skip collecting inv_write for large object %d not referenced in pgr_master_lo", loid);
		return;
	}

	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* build our relpath value */
	snprintf(relpath, MAX_REL_PATH, "%s.%s",
			 "pg_catalog", "pg_largeobject");
	/* natts and command_id don't matter here */
	PGRDumpHeaders(PGR_CMD_WRITE_LO, relpath, cid, 0, true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);
	dump_write_lo(tuple, loid);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

void
PGRCollectDropLO(Oid loid, CommandId cid)
{
	char			relpath[MAX_REL_PATH];
	MemoryContext	qcontext;
	MemoryContext	oldcontext;
	LOData			lodata;

	/* allocate new context and switch to it to be safe */
	qcontext = AllocSetContextCreate(CurrentMemoryContext,
									 "Monitor Collect Context",
									 ALLOCSET_DEFAULT_MINSIZE,
									 ALLOCSET_DEFAULT_INITSIZE,
									 ALLOCSET_DEFAULT_MAXSIZE);
	oldcontext = MemoryContextSwitchTo(qcontext);

	/* build our relpath value */
	snprintf(relpath, MAX_REL_PATH, "%s.%s", "pg_catalog", "pg_largeobject");
	PGRDumpHeaders(PGR_CMD_DROP_LO, relpath, cid, 0, true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);

	/* When sending DropLO, only loid matters */
	lodata.data_size = 0;
	lodata.master_oid = loid;
	lodata.col_num = -1;

	/* Dump LoData information */
	MCPLocalQueueAppend(MasterLocalQueue, &lodata, sizeof(LOData), 0, true);

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qcontext);
}

/* Collect pg_authid tuple for replication of the new role */
void
CollectNewRole(Relation authidRel, HeapTuple roleTuple, CommandId cid)
{
	char 		*relpath;
	TupleDesc 	tupleDesc; 
	MemoryContext oldcxt,
				  qcxt;
	StringInfoData buf;
	
	/* Switch to the temprorary memory context */
	qcxt = AllocSetContextCreate(CurrentMemoryContext,
								 "CollectRoleData Context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);									
	oldcxt = MemoryContextSwitchTo(qcxt);
	
	relpath = build_relpath(authidRel);
	
	/* Add CREATE_ROLE command */
	PGRDumpHeaders(PGR_CMD_CREATE_ROLE, relpath, cid, 
				   HeapTupleHeaderGetNatts(roleTuple->t_data), true);
				
	/* Add relation name to the queue and free relation name buffer */
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, true);
	/* XXX: add repl_slave_roles relation name to the table list to
	 * pass the checks at slave forwarder process (we won't have
	 * pg_authid or pg_authmembers in the slave's table list, cause we never
	 * send dumps for these tables.
	 */
	MCPLocalQueueAddRelation(MasterLocalQueue, "pg_catalog.repl_slave_roles", 
							 false, true);
	pfree(relpath);
	
	/* Put role tuple data into the queue */
	tupleDesc = RelationGetDescr(authidRel);
	build_network_tuple(roleTuple, tupleDesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);
		
	MemoryContextSwitchTo(oldcxt);	
	MemoryContextDelete(qcxt);
}

/* Put data to the queue for the 'ALTER ROLE' command */
void 
CollectAlterRole(Relation authidRel, HeapTuple oldTuple, 
				 HeapTuple newTuple, CommandId cid)
{
	HeapTuple 		sendTuple;
	TupleDesc 		tupdesc;
	bool 		   *send_mask;
	bool 			isnull;
	int 			mask_natts;
	char		   *relpath;
	Datum 			rolenameDatum;
	char 		   *rolename;
	StringInfoData 	buf;
	
	MemoryContext 	qcxt,
					oldcxt;
	
	qcxt = AllocSetContextCreate(CurrentMemoryContext,
								 "CollectRoleData Context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(qcxt);
	
	tupdesc = RelationGetDescr(authidRel);
	
	sendTuple = make_diff_tuple(oldTuple, newTuple, tupdesc, 
								&send_mask, &mask_natts);
	
	rolenameDatum = heap_getattr(oldTuple, Anum_pg_authid_rolname,
								 tupdesc, &isnull);
	rolename = DatumGetCString(DirectFunctionCall1(nameout, rolenameDatum));
								
	relpath = build_relpath(authidRel);
	
	/* Add ALTER ROLE command */
	PGRDumpHeaders(PGR_CMD_ALTER_ROLE, relpath, cid, 
				   HeapTupleHeaderGetNatts(newTuple->t_data), true);
	
	/* Add relation name to the queue */
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, true);
	/* XXX: add repl_slave_roles relation name to the table list to
	 * pass the checks at slave forwarder process (we won't have
	 * pg_authid or pg_authmembers in the slave's table list, cause we never
	 * send dumps for these tables.
	 */
	MCPLocalQueueAddRelation(MasterLocalQueue, "pg_catalog.repl_slave_roles", 
							 false, true);	
	pfree(relpath);

	/* Add rolename to the queue */
	buf.data = rolename;
	buf.len = strlen(rolename) + 1;
	
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);
		
	/* See comments in PGRCollectUpdate for details on the code below */
	build_network_tuple(sendTuple, tupdesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);

	/* Add updatemask to the queue */
	buf.data = send_mask;
	buf.len = mask_natts;
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);
	
	MemoryContextSwitchTo(oldcxt);	
	MemoryContextDelete(qcxt);

}

/* Collect role members for grant role to role command replication */
void
CollectGrantRole(Relation authMembersRel, HeapTuple membersTuple, CommandId cid)
{
	collect_role_members(authMembersRel, membersTuple, cid, false, false);
}

/* Collect role members for revoke role from role command replication */
void
CollectRevokeRole(Relation authMembersRel, HeapTuple membersTuple, 
				  CommandId cid, bool revoke_admin)
{
	collect_role_members(authMembersRel, membersTuple, cid, true, revoke_admin);
}

/* A common code for CollectGrantRole and CollectRevokeRole */
static void
collect_role_members(Relation authMembersRel, HeapTuple membersTuple, 
					 CommandId cid, bool revoke, bool revoke_admin)
{
	HeapTuple 	replMembersTuple;
	TupleDesc 	replMembersDesc;
	char 		*relpath;
	StringInfoData buf;
	MemoryContext 	qcxt,
				  	oldcxt;
	int 			cmd;
	
	if (!revoke)
		cmd = PGR_CMD_GRANT_MEMBERS;
	else
		cmd = PGR_CMD_REVOKE_MEMBERS;
		
	/* Switch to the temprorary memory context */
	qcxt = AllocSetContextCreate(CurrentMemoryContext,
								 "CollectRoleMembers Context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);									
	oldcxt = MemoryContextSwitchTo(qcxt);
		
	relpath = build_relpath(authMembersRel);
	
	/* 
	 * Make a new tuple from pg_auth_members tuple by replacing
	 * OIDs with role names.
	 */
	replMembersTuple = make_repl_members_tuple(membersTuple, &replMembersDesc,
											   revoke_admin);
	
	if (cid != prev_cid || prev_cmd != cmd)
		PGRDumpHeaders(cmd, relpath, cid, 
				   	   HeapTupleHeaderGetNatts(replMembersTuple->t_data), true);
				
	/* Add relation name to the queue and free relation name buffer */
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, false, true);
	/* XXX: add repl_slave_roles relation name to the table list to
	 * pass the checks at slave forwarder process (we won't have
	 * pg_authid or pg_authmembers in the slave's table list, cause we never
	 * send dumps for these tables.
	 */
	MCPLocalQueueAddRelation(MasterLocalQueue, "pg_catalog.repl_slave_roles", 
							 false, true);	
	pfree(relpath);
	
	/* Put a transformed tuple to the queue */
	build_network_tuple(replMembersTuple, replMembersDesc, &buf);
	MCPLocalQueueAppend(MasterLocalQueue, buf.data, buf.len, 0, true);
	
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(qcxt);
}

/* 
 * Make a new tuple for replication from pg_auth_members tuple by replacing
 * all OID attributes with role names.
 */
static HeapTuple
make_repl_members_tuple(HeapTuple membersTuple,
						TupleDesc *replMembersDesc, 
						bool revoke_admin)
{
	Datum 	values[Natts_repl_members_tuple];
	bool	nulls[Natts_repl_members_tuple];
	HeapTuple replMembersTuple;
	char 	  *roleName,
			  *memberName,
			  *grantorName;
	Datum	   roleNameDatum,
			   memberNameDatum,
			   grantorNameDatum;
	
	/* Convert role oids to names */
	Form_pg_auth_members membersForm = 
								(Form_pg_auth_members) GETSTRUCT(membersTuple);
	
	MemSet(nulls, false, Natts_repl_members_tuple);
	
	/* Convert role OIDs to names */
	roleName = GetUserNameFromId(membersForm->roleid);
	memberName = GetUserNameFromId(membersForm->member);
	grantorName = GetUserNameFromId(membersForm->grantor);
	
	/* Make Names from CStrings */
	roleNameDatum = DirectFunctionCall1(namein, CStringGetDatum(roleName));
	memberNameDatum = DirectFunctionCall1(namein, CStringGetDatum(memberName));
	grantorNameDatum = 
					DirectFunctionCall1(namein, CStringGetDatum(grantorName));
	
	/* Get description for a tuple to be replicated */
	*replMembersDesc = MakeReplAuthMembersTupleDesc();
	
	/* 
	 * Note that we use attribute numbers from pg_auth_members, but construct
	 * a tuple with another description. This will work because a description
	 * obtained from MakeReplAuthMembersTupleDesc has attribute positions that
	 * match those from pg_auth_members.
	 */
	values[Anum_repl_members_tuple_rolename - 1] = roleNameDatum;
	values[Anum_repl_members_tuple_membername - 1] = memberNameDatum;
	values[Anum_repl_members_tuple_grantorname - 1] = grantorNameDatum;
	values[Anum_repl_members_tuple_admin_option - 1] =
								BoolGetDatum(membersForm->admin_option);
	values[Anum_repl_members_tuple_revoke_admin - 1] =
	 							BoolGetDatum(revoke_admin);

	
	replMembersTuple = heap_form_tuple(*replMembersDesc, values, nulls);
	return replMembersTuple;
}

/*
 * Internal workhorse for PGRCollectWriteLO and PGRCollectNewLO --
 * append the new large object data to the queue.
 *
 * XXX we're sending the complete pg_largeobject tuple; perhaps it would
 * make more sense to send the actual changes executed.
 */
static void
dump_write_lo(HeapTuple tuple, Oid loid)
{
	LOData	lodata;

	lodata.data_size = tuple->t_len;
	lodata.master_oid = loid;

	/*
	 * The column doesn't matter when sending inv_write data because we
	 * identify large object only by oid, and oid mapping is already present.
	 */
	lodata.col_num = -1;

	/* Dump LoData information */
	MCPLocalQueueAppend(MasterLocalQueue, &lodata, sizeof(LOData), 0, true);
	/* Dump large object's data */
	MCPLocalQueueAppend(MasterLocalQueue, (char *) tuple->t_data->t_bits,
   						tuple->t_len, 0, true);
}

/* Just dump simple plain header for truncate lo_refs relation */
static void
PGRCollectTruncateLO(CommandId cid)
{
	char		relpath[MAX_REL_PATH];

	/* build our relpath value */
	snprintf(relpath, MAX_REL_PATH, "%s.%s", "pg_catalog", "pg_largeobject");

	PGRDumpHeaders(PGR_CMD_TRUNCATE_LO, relpath, cid, 0, true);
	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, true, true);
}

static void
PGRDumpHeaders(int cmd_type, char *relpath, int command_id, int natts, bool trans)
{
	CommandHeader  *cmdh;
	PlainHeader		ph;
	int				pathlen;

	/* Finalize a previous command if necessary */
	if (prev_cid != -1)
		PGRDumpCommandEnd();

	ph.cmd_type = cmd_type;

	elog(DEBUG5, "dumping plain header type %s", CmdTypeAsStr(ph.cmd_type));

	MCPLocalQueueAppend(MasterLocalQueue, &ph, sizeof(PlainHeader), 0, trans);

	pathlen = strlen(relpath);

	cmdh = palloc0(MAXALIGN(sizeof(CommandHeader) + pathlen));
	memcpy(&(cmdh->rel_path), relpath, pathlen);

	cmdh->rel_path_len = pathlen;
	cmdh->cid = command_id;
	cmdh->natts = natts;

	elog(DEBUG5,
		 "dumping command header: cid=%d, rel_path=%s (len=%d)",
		 cmdh->cid, &(cmdh->rel_path), cmdh->rel_path_len);

	MCPLocalQueueAppend(MasterLocalQueue, cmdh,
						sizeof(CommandHeader) + cmdh->rel_path_len, 0, trans);
	pfree(cmdh);

	prev_cid = command_id;
	prev_cmd = cmd_type;
}

/*
 * Add a relation to the current table list, checking if it should raise dump
 * (not empty) or not.
 * XXX: Alvaro thinks that we have to get rid of this function.
 */
void
AddRelationToMasterTableList(Relation rel, bool enable)
{
	char	relpath[MAX_REL_PATH];
	int		raise_dump;

	if (rel->rd_rel->relkind == RELKIND_SEQUENCE)
		raise_dump = TableNoDump;
	else if (enable)
	{
		HeapScanDesc	scan;
		Snapshot		current;
	
		current = RegisterSnapshot(GetTransactionSnapshot());

		scan = heap_beginscan(rel, current, 0, NULL);

		/* Raise dump if the table is not empty */
		if (HeapTupleIsValid(heap_getnext(scan, ForwardScanDirection)))
			raise_dump = TableDump;
        else
            raise_dump = TableNoDump;

		heap_endscan(scan);
		
		UnregisterSnapshot(current);
		
	}
	else
		raise_dump = TableExpired;

	snprintf(relpath, MAX_REL_PATH, "%s.%s",
			 get_namespace_name(RelationGetNamespace(rel)),
			 RelationGetRelationName(rel));

	MCPLocalQueueAddRelation(MasterLocalQueue, relpath, raise_dump, true);

	elog(DEBUG5, "Added relation %s to the master table list, raise_dump = %d",
		 relpath, raise_dump);
}

/*
 * Returns a palloc'ed string with the given Relation's "relpath"
 * (i.e. something of the form "schema.table")
 */
static char *
build_relpath(Relation relation)
{
	char   *schemaname;
	char   *tablename;
	char   *relpath;
	int		relpath_len;

	schemaname = get_namespace_name(RelationGetNamespace(relation));
	tablename = RelationGetRelationName(relation);

	relpath_len = strlen(schemaname) + strlen(tablename) + 3;
	relpath = palloc(relpath_len);
	snprintf(relpath, relpath_len, "%s.%s", schemaname, tablename);

	pfree(schemaname);

	return relpath;
}

/* 
 * Make a new tuple, which contains only those attributes that are different in
 * old and new tuples. Equal attributes are set to NULL.
 *
 * The return value is the tuple containing the differing attributes.  On
 * return, send_mask points to an array to be used by heap_modify_tuple,
 * except that dropped columns are not present in the array, so the caller
 * needs to expand it with its own set of dropped columns.  mask_natts
 * contains the number of attributes in the returned tuple (including dropped
 * ones).
 *
 */
static HeapTuple
make_diff_tuple(HeapTuple oldtuple, HeapTuple newtuple, 
				TupleDesc tupdesc, bool **send_mask, int *mask_natts)
{
	int			old_natts = HeapTupleHeaderGetNatts(oldtuple->t_data);
	int			new_natts = HeapTupleHeaderGetNatts(newtuple->t_data);
	Datum	   *old_values;
	bool	   *old_nulls;
	Datum	   *new_values;
	bool	   *new_nulls;
	Datum	   *send_values;
	bool	   *send_nulls;
	int			i;
	int			j;
	int			max_natts = Max(old_natts, new_natts);
	bool	   *sendmask;
	
	Assert(send_mask != NULL);
	
	sendmask = palloc(sizeof(bool) * new_natts);

	/*
	 * We need to allocate no less than new_atts for the case when new_atts >
	 * old_atts since new tuple description assumes that the number of
	 * attributes is new_atts.
	 */
	old_values = palloc(sizeof(Datum) * max_natts);
	old_nulls = palloc(sizeof(bool) * max_natts);

	new_values = palloc(sizeof(Datum) * max_natts);
	new_nulls = palloc(sizeof(bool) * max_natts);

	send_values = palloc(sizeof(Datum) * max_natts);
	send_nulls = palloc(sizeof(bool) * max_natts);

	heap_deform_tuple(oldtuple, tupdesc, old_values, old_nulls);
	heap_deform_tuple(newtuple, tupdesc, new_values, new_nulls);

	/*
	 * Form a tuple to send as "new tuple".  This tuple may not actually be
	 * the same as the updated tuple -- it will contain NULLs for the
	 * attributes that are not being changed in this UPDATE.
	 */
	for (i = 0, j = -1; i < new_natts; i++)
	{
		Oid				cmp_opid;
		RegProcedure	cmp_proc;

		if (tupdesc->attrs[i]->attisdropped)
		{
			/* 
			 * We still have to set 'reasonable' values to array elements that
			 * correspond to dropped attributes to avoid confusing heap_form_tuple.
			 */
			send_nulls[i] = true;
			send_values[i] = (Datum) 0;
			continue;
		}

		/*
		 * This counter is used to keep track of the update mask we're
		 * going to send.  We need to count only non-dropped columns.  The
		 * slave constructs its mask based on the one we send, using its
		 * own set of dropped columns.  Note that this only affects the
		 * mask, not the nulls or values arrays, because those are used
		 * here to build the tuple we send out, whereas the mask is sent
		 * out verbatim.
		 *
		 * Note that incrementing it this early means that we need to
		 * initialize it as -1 at the start of the loop.
		 */
		j++;

		/*
		 * If the new tuple has a NULL here, send a NULL.
		 */
		if (new_nulls[i] == true)
		{
			sendmask[j] = true;
			send_nulls[i] = true;
			send_values[i] = (Datum) 0;
			continue;
		}
		/*
		 * if the new tuple has more attributes than the old one, send the
		 * attribute
		 */
		if (i >= old_natts)
		{
			sendmask[j] = true;
			send_nulls[i] = false;
			send_values[i] = new_values[i];
			continue;
		}

		/* if the old tuple has a NULL, send the attribute */
		if (old_nulls[i] == true)
		{
			sendmask[j] = true;
			send_nulls[i] = false;
			send_values[i] = new_values[i];
			continue;
		}

		/*
		 * The hard case: both are not null, so we have to check them for
		 * equality
		 */
		get_sort_group_operators(tupdesc->attrs[i]->atttypid, false, false,
								 false, NULL, &cmp_opid, NULL);
		
		/* if we can't find the operator, send the value */
		if (!OidIsValid(cmp_opid))
		{
			sendmask[j] = true;
			send_nulls[i] = false;
			send_values[i] = new_values[i];
			continue;
		}
		
		cmp_proc = get_opcode(cmp_opid);
		
		if (!RegProcedureIsValid(cmp_proc))
		{
			sendmask[j] = true;
			send_nulls[i] = false;
			send_values[i] = new_values[i];
			continue;
		}

		/* Finally call the equality function */
		if (DatumGetBool(OidFunctionCall2(cmp_proc, old_values[i],
										  new_values[i])))
		{
			/* Values are equal -- send a NULL, but mask it out */
			sendmask[j] = false;
			send_nulls[i] = true;
			send_values[i] = (Datum) 0;
		}
		else
		{
			/* Values are different; send the value and mark it for update */
			sendmask[j] = true;
			send_nulls[i] = false;
			send_values[i] = new_values[i];
		}
	}

	*send_mask = sendmask;
	*mask_natts = new_natts;

	return heap_form_tuple(tupdesc, send_values, send_nulls);	
}

/* 
 * Make a new tuple with index-only attributes filled, others nulled.
 * A caller should hold a lock on index relation. Note that the tuple
 * passed here may not match the tuple descriptor - it can be an old
 * tuple during update, that was created with a different tuple descriptor
 * and has a different number of attributes.
 */
static HeapTuple
make_index_tuple(Relation indexRel, HeapTuple tuple, TupleDesc tupdesc)
{	
	int 			natts;
	Form_pg_index 	indexForm;
	Datum 		   *values,
				   *idx_values;
	bool 		   *nulls,
				   *idx_nulls;
	int 			i;
	

	/* Get the number of attributes in the relation */
	natts = tupdesc->natts;
	
	idx_values = palloc(sizeof(Datum) * natts);
	idx_nulls = palloc(sizeof(bool) * natts);
	values = palloc(sizeof(Datum) * natts);
	nulls = palloc(sizeof(bool) * natts);
	
	indexForm = indexRel->rd_index;
	
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	MemSet(idx_nulls, true, natts);
	for (i = 0; i < indexForm->indnatts; i++)
	{
		int		attnum = indexForm->indkey.values[i] - 1;

		/* should not happen */
		if (attnum > natts)
			elog(ERROR, "Index attribute %d is null", attnum);

		idx_nulls[attnum] = false;
		idx_values[attnum] = values[attnum];
	}
	return heap_form_tuple(tupdesc, idx_values, idx_nulls);
}

/* Converts non-dropped column number to the absolute one. */
int
non_dropped_to_absolute_column_number(Relation rel, int non_dropped_colno)
{
	int 		i,
				j;
	TupleDesc	tupDesc;
	
	tupDesc = RelationGetDescr(rel);
	
	for (i = 0, j = 0; i < tupDesc->natts; i++)
	{
		/* skip dropped attributes */
		if (tupDesc->attrs[i]->attisdropped)
			continue;
		if (++j == non_dropped_colno)
			break;
	}
	if (j != non_dropped_colno)
		ereport(ERROR, 
				(errmsg("relation %s has less than %d non dropped columns", 
				 		RelationGetRelationName(rel), non_dropped_colno)));
				
	/* column numbers start from 1 */
	return i+1;
}
