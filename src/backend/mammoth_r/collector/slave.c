/*-----------------------
 * slave.c
 * 		The Mammoth Replication slave tuple handling routines
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group.
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: slave.c 2202 2009-07-08 18:14:27Z alvherre $
 *
 * ------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/replication.h"
#include "catalog/repl_relations.h"
#include "catalog/repl_slave_relations.h"
#include "catalog/repl_slave_lo_refs.h"
#include "catalog/repl_auth_members.h"
#include "catalog/repl_lo_columns.h"
#include "catalog/repl_acl.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "libpq/be-fsstubs.h"
#include "libpq/libpq-fs.h"
#include "libpq/pqformat.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/backend_tables.h"
#include "mammoth_r/collcommon.h"
#include "mammoth_r/collector.h"
#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/repl_tuples.h"
#include "nodes/makefuncs.h"
#include "parser/parse_oper.h"
#include "storage/large_object.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


static void PGRRestoreCommand(MCPQueue *q, PlainHeader *ph, int slaveno);
static HeapTuple pgr_formtuple(HeapTuple tuple, Relation relation);

static void PGRRestoreInsert(MCPQueue *q, PlainHeader *ph,
				 			 CommandHeader *cmdh);
static void PGRRestoreUpdate(MCPQueue *q, PlainHeader *ph,
				 			 CommandHeader *cmdh);
static void PGRRestoreDelete(MCPQueue *q, PlainHeader *ph,
							 CommandHeader *cmdh);
static void PGRRestoreCommandTruncate(CommandHeader *cmdh);
static void PGRRestoreCommandSequence(MCPQueue *q, CommandHeader *cmdh);
static void PGRRestoreWriteLo(MCPQueue *q, int cmd_type);
static void PGRRestoreDropLo(MCPQueue *q, int cmd_type);
static void RestoreNewRole(MCPQueue *q, PlainHeader *ph,
						   CommandHeader *cmdh, int slaveno);
static void RestoreAlterRole(MCPQueue *q, PlainHeader *ph,
							 CommandHeader *cmdh, int slavno);
static void RestoreGrantMembers(MCPQueue *q, PlainHeader *ph,
						   		CommandHeader *cmdh, int slaveno);
static void RestoreRevokeMembers(MCPQueue *q, PlainHeader *ph,
						   		 CommandHeader *cmdh, int slaveno);
static void restore_write_lo(int cmd, LOData *lodesc, char *lodata);
static void	PGRRestoreTruncateLO(void);
static List *make_new_replicated_list(List *tl_old, int slaveno);
static HeapTuple form_authmem_tuple(HeapTuple replTuple, 
									TupleDesc replDesc, TupleDesc authMemDesc);


/* skip tuples for 'relations' and 'slave_relations' if this flag is set */

/*
 * if this flag is true then relation dump is restored, otherwise it is
 * relation data.
 */
static bool relnames_dump_mode;

/*
 * A list of tables replicated by the slave. This list is used to:
 * - mark a table as dirty if we are waiting for the dump of this table
 * - detect a new table after restoring a catalog dump transaction.
 */
static bool replicated_list_changed = false;
static List *tl_replicated;


/*
 * PGRRestoreData
 *
 * Read messages from a MCP queue and restore whatever they indicate.  This
 * function has the side effect of advancing the "first record number" of the
 * queue, depending on messages read.
 */
void
PGRRestoreData(MCPQueue *mcpq, int slaveno)
{
	TxDataHeader	hdr;

	elog(DEBUG5, "PGRRestoreData starting: frecno="UNI_LLU,
		 MCPQueueGetFirstRecno(mcpq));

    replicated_list_changed = false;
	/*
	 * Read a transaction header first, making sure that this is a valid
	 * transaction data file.
	 */
	MCPQueueReadDataHeader(mcpq, &hdr);

	if (hdr.dh_flags & MCP_QUEUE_FLAG_CATALOG_DUMP ||
		hdr.dh_flags & MCP_QUEUE_FLAG_TABLE_DUMP)
	{
		/* process relation names from command header in a 'dump' mode */
		relnames_dump_mode = true;
	}
 
	else if (hdr.dh_flags & MCP_QUEUE_FLAG_DUMP_END)
	{
		/* end of the full dump, do nothing here */
		goto end;
	}

	/* Start a transaction for the commands we are going to restore */
    StartTransactionCommand();
	
	/* 
	 * Dump start truncates table data, thus by setting the tablelist empty
	 * we force the slave to fetch table dump for each replicted table from
	 * the MCP server
	 */
	if (hdr.dh_flags & MCP_QUEUE_FLAG_DUMP_START)
		tl_replicated = NIL;
	else
    	tl_replicated = RestoreTableList(REPLICATED_LIST_PATH);
	
	/* Make sure we have a snapshot set. */
	PushActiveSnapshot(GetTransactionSnapshot());

	while (true)
	{
		StringInfo		str;
		PlainHeader	   *ph;

		AssertState(IsTransactionState() && !IsAbortedTransactionBlockState());

		str = MCPQueueTxReadPacket(mcpq, true);

		/* end of transaction; break out of the outer loop */
		if (str == NULL)
		{
			elog(DEBUG5, "End of transaction file reached");
			break;
		}

		ph = (PlainHeader *) str->data;
		pfree(str);

		elog(DEBUG5, "command type %s", CmdTypeAsStr(ph->cmd_type));

		PGRRestoreCommand(mcpq, ph, slaveno);
		pfree(ph);
		CommandCounterIncrement();
	}

	 /* Get a list of replicated tables after restoring a catalog dump */
    if (hdr.dh_flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
        tl_replicated = make_new_replicated_list(tl_replicated, slaveno);

    if (replicated_list_changed)
        StoreTableList(REPLICATED_LIST_PATH, tl_replicated);

	PopActiveSnapshot();

	/* Commit the current transaction */
	CommitTransactionCommand();

end:
	relnames_dump_mode = false;
}

/* 
 * Compose a new list of replicated tables (after catalog dump).
 * Mark recently added tabless with raise_dump flag.
 */
static List *
make_new_replicated_list(List *tl_old, int slaveno)
{
	List 			*relids;
	List			*tl_new;
	ListCell		*cell;

	/* Store the old address of the dirty list before clearing it*/
	tl_new = NIL;

	/* Acquire a list of replicated relations */
	relids = get_master_and_slave_replicated_relids(slaveno);

	/* Add relations to the replicated list */
	foreach(cell, relids)
	{
		Oid         relid, 
                    nspid;
		char       *relname,
                   *nspname,
                   *relpath;
		TableDumpFlag	raise_dump;
        BackendTable    tab;
        BackendTable    old_tab;

		relid = lfirst_oid(cell);
        nspid = get_rel_namespace(relid);

        /* 
         * Skip tables from pg_catalog namespace. We don't need
         * to request per-table dumps for them. Also skip invalid
         * namespace and relation oids.
         */
         if (relid == InvalidOid || 
             nspid == InvalidOid || 
             IsSystemNamespace(nspid))
            continue;


         /* Get relation name and namespace */
         relname = get_rel_name(relid);

         if (relname == NULL)
         {
             elog(WARNING, "Could not acquire relname for relation id %d",
                  relid);
             continue;
         }

         nspname = get_namespace_name(nspid);

         if (nspname == NULL)
         {
             elog(WARNING, "Could not acquire nspname for namespace id %d",
                  nspid);
         }

    	relpath = palloc0(strlen(nspname) + strlen(relname) + 2);
		sprintf(relpath, "%s.%s", nspname, relname);

		pfree(nspname);
        pfree(relname);

    	/* check if we were already replicating this table */
		old_tab = TableListEntryByName(tl_old, relpath);
		/* Preserve raise_dump flag */
		if (old_tab != NULL)
			raise_dump = old_tab->raise_dump;
		else
			raise_dump = TableDump;

		/*
		 * make a new table from relpath and mark it as new only
		 * if we haven't replicated it yet
		 */
		tab = MakeBackendTable(relpath, raise_dump);

		elog(DEBUG2, "adding %s to the list of replicated tables (raise dump: %d)", 
             relpath, tab->raise_dump);

		tl_new = lappend(tl_new, tab);

	    pfree(relpath);
	}

	list_free_deep(tl_old);
    /* A new list should be stored on disk */
    replicated_list_changed = true;

    return tl_new;
}

void
SlaveTruncateAll(int slaveno)
{
	List			*repl_master_oids;
	List			*repl_slave_oids;
	ListCell		*cur;
	MemoryContext	SlaveTruncateCxt,
					oldcxt;
	Snapshot		snap;

	elog(DEBUG5, "SlaveTruncateAll");

	/* Make a separate context for all processing withing this function */
	SlaveTruncateCxt = AllocSetContextCreate(TopTransactionContext,
											 "Slave Truncate Context",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(SlaveTruncateCxt);

	/* Start transaction */
	StartTransactionCommand();

	snap = RegisterSnapshot(GetTransactionSnapshot());

	/* Get lists of relations replicated on master and on this slave */
	repl_master_oids = get_replicated_relids(snap);
	repl_master_oids = get_catalog_relids(repl_master_oids);

	repl_slave_oids = get_slave_replicated_relids(slaveno);

	/* Process only those relations which exist in both lists */
	foreach(cur, repl_master_oids)
	{
		Oid		relid = lfirst_oid(cur);
		Assert(OidIsValid(relid));

		/* Skip special relations from pg_catalog (like repl_relations)
		 * and sequences.
		 */
		if (get_rel_namespace(relid) == PG_CATALOG_NAMESPACE ||
			get_rel_relkind(relid) != RELKIND_RELATION)
			continue;

		elog(DEBUG5, "truncating relation %s", get_rel_name(relid));
		if (list_member_oid(repl_slave_oids, relid))
		{
			heap_pgr_truncate(relid);
		}
	}
	UnregisterSnapshot(snap);
	/* Finish transaction */
	CommitTransactionCommand();

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(SlaveTruncateCxt);
}

/*
 * Restore a command received from the master.
 *
 * This routine receives a header message, and reads as much messages as
 * appropiate for the type of message specified by the header. It optionally
 * applies them, depending on whether this slave believes the relation to be
 * replicated or not.
 */
static void
PGRRestoreCommand(MCPQueue *q, PlainHeader *ph, int slaveno)
{
	StringInfo		str;
	CommandHeader  *cmdh;

	/* Must be in a transaction already */
	Assert(IsTransactionState());

	str = MCPQueueTxReadPacket(q, false);
	cmdh = (CommandHeader *) str->data;
	pfree(str);

	elog(DEBUG5,
		 "PGRRestoreCommand: read cmd header; type %s, cid %d, relpath %s (len %d)",
		 CmdTypeAsStr(ph->cmd_type), cmdh->cid,
		 &(cmdh->rel_path), cmdh->rel_path_len);

	switch (ph->cmd_type)
	{
		case PGR_CMD_INSERT:
			PGRRestoreInsert(q, ph, cmdh);
			break;
		case PGR_CMD_UPDATE:
			PGRRestoreUpdate(q, ph, cmdh);
			break;
		case PGR_CMD_DELETE:
			PGRRestoreDelete(q, ph, cmdh);
			break;
		case PGR_CMD_TRUNCATE_LO:
			PGRRestoreTruncateLO();
			break;
		case PGR_CMD_DROP_LO:
			PGRRestoreDropLo(q, ph->cmd_type);
			break;
		case PGR_CMD_NEW_LO:
		case PGR_CMD_WRITE_LO:
			PGRRestoreWriteLo(q, ph->cmd_type);
			break;
		case PGR_CMD_SEQUENCE:
			PGRRestoreCommandSequence(q, cmdh);
			break;
		case PGR_CMD_TRUNCATE:
			PGRRestoreCommandTruncate(cmdh);
			break;
		case PGR_CMD_CREATE_ROLE:
			RestoreNewRole(q, ph, cmdh, slaveno);
			break;
		case PGR_CMD_ALTER_ROLE:
			RestoreAlterRole(q, ph, cmdh, slaveno);
			break;
		case PGR_CMD_GRANT_MEMBERS:
			RestoreGrantMembers(q, ph, cmdh, slaveno);
			break;
		case PGR_CMD_REVOKE_MEMBERS:
			RestoreRevokeMembers(q, ph, cmdh, slaveno);
			break;
		default:
			elog(ERROR, "unrecognized command type");
	}

	pfree(cmdh);

	elog(DEBUG5, "PGRRestoreCommand done");
}

/*
 * Return the relation to be used in a command, or NULL if it's not replicated
 * or it doesn't exist.
 */
static Relation
PGRRestoreGetRelation(char *cmd_relpath)
{
	Relation	relation = NULL;
	RangeVar   *rv;
	Oid			relid;
	char		path_copy[MAX_REL_PATH + 1];
	char	   *nspname;
	char	   *relname;

	strcpy(path_copy, cmd_relpath);

	nspname = strtok(path_copy, ".");
	relname = strtok(NULL, ".");

	Assert(nspname != NULL);
	Assert(relname != NULL);

	rv = makeRangeVar(nspname, relname, -1);
	relid = ReplRangeVarGetRelid(rv, true);
	pfree(rv);

	if (!OidIsValid(relid))
		return NULL;

    /* Check whether we are waiting for the dump of this table */
	if (tl_replicated != NIL)
	{
		BackendTable entry;

		entry = TableListEntryByName(tl_replicated, cmd_relpath);
		if (entry != NULL && GetTableDump(entry) != TableNoDump)
		{
			if (relnames_dump_mode == false)
			{
				/* This table is not yet restored during a dump, skip it */
				elog(DEBUG5, "%s is not restored from a dump yet, skip it",
					 cmd_relpath);
				return NULL;
			}
			else
			{
				elog(DEBUG5, "'wait for dump' flag is cleared for the table %s",
					 cmd_relpath);
				SetTableDump(entry, TableNoDump);
                /* Replicated list was changed and should be stored on disk */
                replicated_list_changed = true;
			}
		}
	}


	/*
	 * Lock the relation by OID, to prevent it being deleted from under us
	 * before we are able to open it.  We may block at this point if we're
	 * unable to get the lock right away.  Note that the locking code will
	 * invoke AcceptInvalidationMessages _without a refcount_, which will
	 * remove the relcache entry if the relation was dropped.  This is OK,
	 * because we would rebuild the entry if needed; but since the rel was
	 * dropped, what will happen is that we will get a NULL relation and thus
	 * disable replication for this packet.
	 */
	LockRelationOid(relid, RowExclusiveLock);

	/* Now try to get a relcache entry */
	relation = RelationIdGetRelation(relid);

	/*
	 * If the relation was deleted before we were able to get the lock, unlock
	 * it and disable replication.
	 */
	if (!RelationIsValid(relation))
	{
		UnlockRelationOid(relid, RowExclusiveLock);
		return NULL;
	}

	/* Check that it's still the same relation we intended to open */
	if (strncmp(RelationGetRelationName(relation), relname, NAMEDATALEN) != 0)
	{
		elog(WARNING, "relid %u is no longer %s (it's %s instead)",
			 RelationGetRelid(relation),
			 relname,
			 RelationGetRelationName(relation));
		relation_close(relation, RowExclusiveLock);
		return NULL;
	}
	/* XXX We should check the schema as well */

	/*
	 * The relation exists, but is it replicated?  If we're not going to apply
	 * changes to it, we'd better close it right away.
	 */
	if (!relation->rd_replicate)
	{
        elog(DEBUG5, "relation %s is not replicated by this slave",
             relname);
		relation_close(relation, RowExclusiveLock);
		return NULL;
	}

	return relation;
}

/*
 * Set up the ExecutorState and resultRelInfo that will be used in a command.
 */
static EState *
PGRRestoreInitExecutor(Relation relation)
{
	ResultRelInfo *resultRelInfo;
	EState	*state;

	state = CreateExecutorState();

	/* Everything else will be in the per-query context */
	MemoryContextSwitchTo(state->es_query_cxt);

	state->es_snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/*
	 * Create a ResultRelInfo which will be used in the ExecutorState.
	 * Note that it lives in state->es_query_cxt, so it will be released
	 * at the end when the ExecutorState is freed.
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1;
	resultRelInfo->ri_RelationDesc = relation;
	ExecOpenIndices(resultRelInfo);

	/* Copy the trigger descriptor. AFTER triggers will be fired on replication relations */
	resultRelInfo->ri_TrigDesc = CopyTriggerDesc(relation->trigdesc);
	if (resultRelInfo->ri_TrigDesc != NULL)
	{
		TupleDesc	tdesc = RelationGetDescr(relation);

		/* Allocate memory for trigger cache information */
		resultRelInfo->ri_TrigFunctions =
			(FmgrInfo *) palloc0(relation->trigdesc->numtriggers * sizeof(FmgrInfo));

		/* Make a slot for user for trigger output tuple */
		state->es_trig_tuple_slot = MakeSingleTupleTableSlot(tdesc);
		AfterTriggerBeginQuery();
	}
	else if (RelationGetNamespace(relation) == PG_CATALOG_NAMESPACE)
		elog(WARNING, "Replicated relation: %s from pg_catalog namespace"
			 " has no triggers defined", RelationGetRelationName(relation));
			
	/* Complete our ExecutorState */
	state->es_result_relations = resultRelInfo;
	state->es_num_result_relations = 1;
	state->es_result_relation_info = resultRelInfo;
	state->es_output_cid = FirstCommandId;	/* must be set later */
	return state;
}

/*
 * Do whatever is necessary to clean up stuff opened in PGRRestoreInitExecutor
 */
static void
PGRRestoreEndExecutor(EState *state)
{
	if (state->es_result_relation_info->ri_TrigDesc != NULL)
		AfterTriggerEndQuery(state);

	ExecCloseIndices(state->es_result_relation_info);

	/* Free a tuple table slot if it was created previously */
	if (state->es_trig_tuple_slot != NULL)
		ExecDropSingleTupleTableSlot(state->es_trig_tuple_slot);
		
	/* Unregister the executor snapshot */
	UnregisterSnapshot(state->es_snapshot);
	
	/* free executor state */
	FreeExecutorState(state);
}

/*
 * From a partial tuple of the PK values as transmitted by the master, get the
 * actual tuple from a table.
 *
 * Note: *indexscan points to an index scan that must be closed by the caller.
 * This is an API shortcoming that should be fixed.
 */
static HeapTuple
restore_get_tuple(Relation relation, Relation index_rel, EState *state,
				  List *lo_columns, HeapTuple valtuple,
				  IndexScanDesc *indexscan)
{
	int			i;
	ScanKey		keys = NULL;
	IndexScanDesc scandesc;
	int			natts = index_rel->rd_index->indnatts;
	TupleDesc	tdesc = RelationGetDescr(relation);

	/* Form the appropiate key for an indexscan on the primary key */
	keys = palloc(natts * sizeof(ScanKeyData));

	for (i = 0; i < natts; i++)
	{
		int				ind = index_rel->rd_index->indkey.values[i];
		Datum			value;
		Oid				cmp_opid;
		RegProcedure	opcode;
		bool			isnull;

		value = heap_getattr(valtuple, ind, tdesc, &isnull);
		if (isnull)
			elog(ERROR, "null attribute in primary key attribute");

		/*
		 * Check if this attribute has to be translated from master LO oid
		 * to slave oid, and do so if needed.
		 */
		if (list_member_int(lo_columns, ind))
		{
			Oid		oidval = LOoidGetSlaveOid(DatumGetObjectId(value), false);

			if (oidval != InvalidOid)
				value = ObjectIdGetDatum(oidval);
			else
			{
				/*
				 * Received update/delete/insert for LO and the mapping
				 * isn't here - shouldn't happen
				 */
				ereport(ERROR,
						(errmsg("could not replace large object ID in primary key attribute"),
						 errcontext("attno = %d, master_oid = %d",
									ind, DatumGetObjectId(value))));
			}
		}
		
		/* HACK: use nameeq to compare cstrings, since this type doesn't have a built-in
		 * comparison operator. 62 is the oid of nameeq
		 */
		if (index_rel->rd_att->attrs[i]->atttypid == CSTRINGOID)
			opcode = (RegProcedure) 62;
		else
		{

			get_sort_group_operators(index_rel->rd_att->attrs[i]->atttypid, false, false,
								 false, NULL, &cmp_opid, NULL);
			if (!OidIsValid(cmp_opid))
				elog(ERROR, "can't find an equality operator for type %u",
				 	 index_rel->rd_att->attrs[i]->atttypid);
			opcode = get_opcode(cmp_opid);
		}
		
		ScanKeyEntryInitialize(&keys[i], 0, i + 1,
					   		   BTEqualStrategyNumber,
					   		   InvalidOid,
					   		   opcode,
					   		   value);
		

	}

	/* Proceed with the index scan for the PK */
	scandesc = index_beginscan(relation, index_rel, state->es_snapshot,
							   natts, keys);
	*indexscan = scandesc;

	return index_getnext(scandesc, ForwardScanDirection);
}

/*
 * Construct a tuple from a replication message.
 *
 * Note that this leaks memory like crazy.  The caller is expected to invoke
 * this in a very short-lived memory context.
 */
static HeapTuple
receive_tuple(StringInfo buf, TupleDesc tupdesc)
{
	bool		   *nulls;
	Datum		   *values;
	StringInfoData	abuf;
	int				i;

	nulls = palloc(sizeof(bool) * tupdesc->natts);
	values = palloc(sizeof(Datum) * tupdesc->natts);

	initStringInfo(&abuf);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Oid		typrecv;
		Oid		typioparam;
		uint32	size;

		/* skip dropped atts */
		if (tupdesc->attrs[i]->attisdropped)
		{
			values[i] = PointerGetDatum(NULL);
			nulls[i] = true;
			continue;
		}

		size = pq_getmsgint(buf, 4);
		/* we got a NULL */
		if (size == -1)
		{
			values[i] = PointerGetDatum(NULL);
			nulls[i] = true;
			continue;
		}

		size -= 4;

		abuf.len = 0;
		abuf.data[0] = '\0';
		abuf.cursor = 0;

		/* copy data from the input message into the private message */
		appendBinaryStringInfo(&abuf, pq_getmsgbytes(buf, size), size);

		getTypeBinaryInputInfo(tupdesc->attrs[i]->atttypid, &typrecv,
							   &typioparam);

		/* and call the receive function */
		values[i] = OidFunctionCall3(typrecv,
									 PointerGetDatum(&abuf),
									 ObjectIdGetDatum(typioparam),
									 Int32GetDatum(-1));
		nulls[i] = false;

#ifdef DEBUG_REPLICATOR
		{
			Oid		typout;
			bool	varlena;

			getTypeOutputInfo(tupdesc->attrs[i]->atttypid, &typout, &varlena);
			elog(DEBUG5, "value received is %s",
				 DatumGetPointer(OidFunctionCall1(typout, values[i])));
		}
#endif
	}

	return heap_form_tuple(tupdesc, values, nulls);
}

static void
PGRRestoreInsert(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh)
{
	Relation	relation = NULL;
	Relation	index_rel = NULL;
	bool		rpt,
                initial;
	TupleDesc	tdesc = NULL;
	TupleTableSlot	*slot = NULL;
	List	   *lo_columns = NIL;
	EState	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt;

	relation = PGRRestoreGetRelation(&cmdh->rel_path);

	if (relation == NULL)
		rpt = false;
	else
	{
		MemoryContext	oldcxt;

		rpt = true;
		tdesc = RelationGetDescr(relation);

		/*
		 * Create a TupleTableSlot for the Executor operations.  Make sure
		 * it is long-lived.  XXX why doesn't it work to have it in the
		 * per-query context?
		 */
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		slot = MakeSingleTupleTableSlot(tdesc);
		MemoryContextSwitchTo(oldcxt);

		/* XXX fix memory management for lo_columns */
		/*
		 * Get list of this relation's lo_columns, to replace primary key
		 * attribute if it is an LO column.
		 */
		lo_columns = get_relation_lo_columns(relation);
		state = PGRRestoreInitExecutor(relation);

		/* Finally, set the primary key relation.  We'll need to scan it. */
		Assert(relation->rd_rel->relkind == RELKIND_RELATION);
		index_rel = PGRFindPK(relation, AccessShareLock);
	}

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    initial = true;

	while (true)
	{
		HeapTuple		tuple;
		HeapTuple		scan_tuple;
		IndexScanDesc   scandesc;
		StringInfo		buf;

		MemoryContextReset(PGRRestoreCmdCxt);

		/* 
         * read command data and possibly exit if none. Note that we
         * require a data for  at least one command to be there.
         */
		buf = MCPQueueTxReadPacket(q, !initial);
		if (buf == NULL)
			break;
		
		/*
		 * Advance a command counter for the next tuple to be inserted.  We
		 * also need to update our ExecutorState snapshot to this new
		 * CommandId, because we use it for the indexscan, which needs to
		 * be in sync with what we're going to update; otherwise we get an
		 * error because we would be trying to update an invisible tuple
		 * (i.e. one that we already updated earlier in this transaction.)
		 */
		if (rpt)
		{
			if (!initial)
				CommandCounterIncrement();
			state->es_snapshot->curcid = state->es_output_cid =
				GetCurrentCommandId(true);
		}
        initial = false;

		/*
		 * if we're not actually replicating this command, there's no need to
		 * go any further
		 */
		if (!rpt)
			continue;

		tuple = receive_tuple(buf, tdesc);

		elog(DEBUG5, "command type: %s, tuple: %s",
			 CmdTypeAsStr(ph->cmd_type), tupleToString(tuple, tdesc));

		/* the PK must not be already present on the table */
		scan_tuple = restore_get_tuple(relation, index_rel, state, lo_columns,
									   tuple, &scandesc);
		if (HeapTupleIsValid(scan_tuple))
			ereport(ERROR,
					(errmsg("invalid replication data"),
					 errcontext("Inserting tuple with already existing primary key values."),
					 errdetail("Tuple is %s", tupleToString(tuple, tdesc))));

		/* convert the LO oids into the slave values */
		tuple = pgr_formtuple(tuple, relation);

		/* OK, we're all set -- actually execute the stuff */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		/*
		 * The executor caches some trigger information, which it needs to
		 * persist across the transaction.  Make sure we don't allocate it
		 * in a short-lived context.
		 */
		MemoryContextSwitchTo(TopTransactionContext);
		ExecInsert(slot, NULL, NULL, NULL, state);
		MemoryContextSwitchTo(PGRRestoreCmdCxt);

		/* Clean up the PK indexscan */
		index_endscan(scandesc);

	}	/* loop around tuples in the command */

	/* cleanup if necessary */
	if (rpt)
	{
		PGRRestoreEndExecutor(state);

		/* Release previously allocated tuple table slot */		
		ExecDropSingleTupleTableSlot(slot);

		/* we keep the lock till commit */
		heap_close(relation, NoLock);

		/* close the primary key, if any */
		if (index_rel)
			relation_close(index_rel, NoLock);
	}

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

static void
PGRRestoreUpdate(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh)
{
	Relation	relation = NULL;
	Relation	index_rel = NULL;
	bool		rpt,
                initial;
	TupleDesc	tdesc = NULL;
	TupleTableSlot	*slot = NULL;
	List	   *lo_columns = NIL;
	EState	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt;

	relation = PGRRestoreGetRelation(&cmdh->rel_path);

	if (relation == NULL)
		rpt = false;
	else
	{
		MemoryContext	oldcxt;

		rpt = true;
		tdesc = RelationGetDescr(relation);

		/*
		 * Create a TupleTableSlot for the Executor operations.  Make sure
		 * it is long-lived.  XXX why doesn't it work to have it in the
		 * per-query context?
		 */
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		slot = MakeSingleTupleTableSlot(tdesc);
		MemoryContextSwitchTo(oldcxt);

		/* XXX fix memory management for lo_columns */
		/*
		 * Get list of this relation's lo_columns, to replace primary key
		 * attribute if it is an LO column.
		 */
		lo_columns = get_relation_lo_columns(relation);
		state = PGRRestoreInitExecutor(relation);

		/* Finally, set the primary key relation.  We'll need to scan it. */
		Assert(relation->rd_rel->relkind == RELKIND_RELATION);

		index_rel = PGRFindPK(relation, AccessShareLock);
	}

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    initial = true;

	while (true)
	{
		StringInfo		buf;
		StringInfo		buf2;
		StringInfo		str;
		bool		   *master_update_mask;
		bool		   *update_mask;
		HeapTuple		tuple;
		HeapTuple		pk_tuple;
		HeapTuple		scan_tuple;
		IndexScanDesc	scandesc;
		HeapTuple		update_tuple;
		int16			natts;
		Datum		   *values;
		bool		   *nulls;
		int				i;
		int				j;

		MemoryContextReset(PGRRestoreCmdCxt);

        /* 
         * read command data and possibly exit if none. Note that we
         * require data for at least one command to be there.
         */
		buf = MCPQueueTxReadPacket(q, !initial);
		if (buf == NULL)
			break;
	
		/*
		 * Advance our command counter for the next tuple to be updated.
		 * See comment in PGRRestoreInsert about this.
		 */
		if (rpt)
		{
			if (!initial)
				CommandCounterIncrement();
			state->es_snapshot->curcid = state->es_output_cid =
				GetCurrentCommandId(true);
		}
        initial = false;

		buf2 = MCPQueueTxReadPacket(q, false);
		str = MCPQueueTxReadPacket(q, false);
		master_update_mask = str->data;
		pfree(str);

		/*
		 * if we're not actually replicating this command, there's no need to
		 * go any further
		 */
		if (!rpt)
			continue;

		/* form the received tuples */
		tuple = receive_tuple(buf, tdesc);
		pk_tuple = receive_tuple(buf2, tdesc);

		/* search the PK in the table */
		scan_tuple = restore_get_tuple(relation, index_rel, state, lo_columns,
									   pk_tuple, &scandesc);

		elog(DEBUG5, "command type: %s, tuple: %s, new values: %s",
			 CmdTypeAsStr(ph->cmd_type), tupleToString(pk_tuple, tdesc),
			 tupleToString(tuple, tdesc));

		/* the PK must already be present in the table */
		if (!HeapTupleIsValid(scan_tuple))
			ereport(ERROR,
					(errmsg("can't locate tuple to update from PK values"),
					 errcontext("PK values are %s", tupleToString(pk_tuple,
																  tdesc)),
					 errcontext("command type %s",
								CmdTypeAsStr(ph->cmd_type))));

		/* OK, we're all set -- actually execute the stuff */
		natts = tdesc->natts;

		values = palloc(sizeof(Datum) * natts);
		nulls = palloc(sizeof(bool) * natts);

		/*
		 * The update mask is sent as an array of bools that comprises only
		 * non-dropped columns on the master, because the list of dropped columns
		 * on the slave could be different.  So we construct a new update mask
		 * here using the local dropped columns, and hope that the non-dropped
		 * columns match those on the master.
		 *
		 * FIXME need to check the length of the update mask to ensure that the
		 * slave does have at least the same number of columns
		 */
		update_mask = palloc(sizeof(bool) * tdesc->natts);
		for (i = 0, j = 0; i < tdesc->natts; i++)
		{
			if (!tdesc->attrs[i]->attisdropped)
				update_mask[i] = master_update_mask[j++];
			else
				update_mask[i] = false;
		}

		heap_deform_tuple(tuple, tdesc, values, nulls);
		update_tuple = heap_modify_tuple(scan_tuple,
										 RelationGetDescr(relation), values,
										 nulls, update_mask);

		tuple = pgr_formtuple(update_tuple, relation);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		/*
		 * The executor caches some trigger information, which it needs to
		 * persist across the transaction.  Make sure we don't allocate it
		 * in a short-lived context.
		 */
		MemoryContextSwitchTo(TopTransactionContext);
		ExecUpdate(slot, &(scan_tuple->t_self), NULL, NULL, state);
		MemoryContextSwitchTo(PGRRestoreCmdCxt);

		/* Clean up the PK indexscan */
		index_endscan(scandesc);

	}	/* loop around tuples in the command */

	/* cleanup if necessary */
	if (rpt)
	{
		PGRRestoreEndExecutor(state);
		
		/* Release previously allocated tuple table slot */		
		ExecDropSingleTupleTableSlot(slot);

		/* we keep the lock till commit */
		heap_close(relation, NoLock);

		/* close the primary key, if any */
		if (index_rel)
			relation_close(index_rel, NoLock);
	}

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

static void
PGRRestoreDelete(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh)
{
	Relation	relation = NULL;
	Relation	index_rel = NULL;
	bool		rpt,
                initial;
	TupleDesc	tdesc = NULL;
	TupleTableSlot	*slot = NULL;
	List	   *lo_columns = NIL;
	EState	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt;

	relation = PGRRestoreGetRelation(&cmdh->rel_path);

	if (relation == NULL)
		rpt = false;
	else
	{
		MemoryContext	oldcxt;

		rpt = true;
		tdesc = RelationGetDescr(relation);

		/*
		 * Create a TupleTableSlot for the Executor operations.  Make sure
		 * it is long-lived.  XXX why doesn't it work to have it in the
		 * per-query context?
		 */
		oldcxt = MemoryContextSwitchTo(TopTransactionContext);
		slot = MakeSingleTupleTableSlot(tdesc);
		MemoryContextSwitchTo(oldcxt);

		/* XXX fix memory management for lo_columns */
		/*
		 * Get list of this relation's lo_columns, to replace primary key
		 * attribute if it is an LO column.
		 */
		lo_columns = get_relation_lo_columns(relation);
		state = PGRRestoreInitExecutor(relation);

		/* Finally, set the primary key relation.  We'll need to scan it. */
		Assert(relation->rd_rel->relkind == RELKIND_RELATION);
		index_rel = PGRFindPK(relation, AccessShareLock);
	}

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    initial = true;

	while (true)
	{
		HeapTuple       tuple;
		HeapTuple       scan_tuple;
		IndexScanDesc   scandesc;
		StringInfo		buf;

		MemoryContextReset(PGRRestoreCmdCxt);

        /* 
         * read command data and possibly exit if none. Note that we
         * require a data for  at least one command to be there.
         */
		buf = MCPQueueTxReadPacket(q, !initial);
		if (buf == NULL)
			break;

		/*
		 * Advance our command counter for the next tuple to be deleted
		 * See comment in PGRRestoreInsert about this.
		 */
		if (rpt)
		{
			if (!initial)
				CommandCounterIncrement();
			state->es_snapshot->curcid = state->es_output_cid =
				GetCurrentCommandId(true);
		}
		initial = false;

		/*
		 * if we're not actually replicating this command, there's no need to
		 * go any further
		 */

		if (!rpt)
			continue;


		tuple = receive_tuple(buf, tdesc);

		scan_tuple = restore_get_tuple(relation, index_rel, state, lo_columns,
									   tuple, &scandesc);

		elog(DEBUG5, "command type: %s, tuple: %s",
			 CmdTypeAsStr(ph->cmd_type), tupleToString(tuple, tdesc));

		/* the PK must already be present in the table */
		if (!HeapTupleIsValid(scan_tuple))
			ereport(ERROR,
					(errmsg("can't locate tuple to delete from PK values"),
					 errcontext("PK values are %s", tupleToString(tuple,
																  tdesc)),
					 errcontext("command type %s",
					 			 CmdTypeAsStr(ph->cmd_type))));

		/* OK, we're all set -- actually execute the stuff */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		/*
		 * The executor caches some trigger information, which it needs to
		 * persist across the transaction.  Make sure we don't allocate it
		 * in a short-lived context.
		 */
		MemoryContextSwitchTo(TopTransactionContext);
		ExecDelete(&(scan_tuple->t_self), NULL, NULL, state);
		MemoryContextSwitchTo(PGRRestoreCmdCxt);

		/* Clean up the PK indexscan */
		index_endscan(scandesc);

	}	/* loop around tuples in the command */

	/* cleanup if necessary */
	if (rpt)
	{
		PGRRestoreEndExecutor(state);

		/* Release previously allocated tuple table slot */		
		ExecDropSingleTupleTableSlot(slot);

		/* we keep the lock till commit */
		heap_close(relation, NoLock);

		/* close the primary key, if any */
		if (index_rel)
			relation_close(index_rel, NoLock);
	}

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

/*
 * Restore a TRUNCATE command.  Note: this function does not leak any memory
 */
static void
PGRRestoreCommandTruncate(CommandHeader *cmdh)
{
	Relation	relation;

	relation = PGRRestoreGetRelation(&cmdh->rel_path);

	if (relation != NULL)
	{
		Oid relid = RelationGetRelid(relation);

		heap_pgr_truncate(relid);
		
		/* We need to clean up pg_depend manually for some catalogs */
		if (relid == ReplRelationsId || 
			relid == ReplSlaveRelationsId || 
			relid == ReplLoColumnsId)
		{
			elog(DEBUG5, "removing pg_depend entries for relation %s",
				 RelationGetRelationName(relation));

			deleteDependencyRecordsForClass(relid);
		}	

		/* we keep the lock till commit */
		relation_close(relation, NoLock);
	}
}

/*
 * Restore setval() or nextval().  Note: this function does not leak any memory
 */
static void
PGRRestoreCommandSequence(MCPQueue *q, CommandHeader *cmdh)
{
	int64	   *value;
	Relation	seq;
	StringInfo	str;

	str = MCPQueueTxReadPacket(q, false);
	value = (int64 *) str->data;
	pfree(str);
	seq = PGRRestoreGetRelation(&cmdh->rel_path);

	if (seq != NULL)
	{
		elog(DEBUG5, "Read sequence data value = "UINT64_FORMAT, *value);

		/* set the is_called flag for the sequence */
		do_setval(RelationGetRelid(seq), *value, true);

		/* we keep the lock till commit */
		heap_close(seq, NoLock);
	}

	pfree(value);
}

static void
PGRRestoreDropLo(MCPQueue *q, int cmd_type)
{
	Oid		slave_oid;
	LOData *lodesc;
	StringInfo	str;

	str = MCPQueueTxReadPacket(q, false);
	lodesc = (LOData *) str->data;
	pfree(str);

	/* Make sure we cleanup the LO state properly after this stuff */
	lo_noop();

	/* Get the Oid mapping and remove it */
	slave_oid = LOoidGetSlaveOid(lodesc->master_oid, true);

	/* Drop the corresponding large object */
	inv_drop(slave_oid);

	pfree(lodesc);
}

static void
PGRRestoreWriteLo(MCPQueue *q, int cmd_type)
{

	LOData *lodesc;
	char	*lodata;
	StringInfo	str;

	lo_noop();

	/* Read large object description */
	str = MCPQueueTxReadPacket(q, false);
	lodesc = (LOData *) str->data;
	pfree(str);

	/* Read lo data to write */
	str = MCPQueueTxReadPacket(q, false);
	lodata = (char *) str->data;
	pfree(str);

	restore_write_lo(cmd_type, lodesc, lodata);

	pfree(lodata);
	pfree(lodesc);
}

/*
 * restore_write_lo
 *		Restore a WRITE_LO or NEW_LO command
 *
 * If the command is NEW_LO, create the new large object and add the appropiate
 * mapping.
 *
 * In either case, write the data the LO data in lodata to the large object
 * pointed by lodesc.
 */
static void
restore_write_lo(int cmd, LOData *lodesc, char *lodata)
{
	Oid			master_oid,
				slave_oid;
	LargeObjectDesc *lobj;

	master_oid = lodesc->master_oid;

	/* If command is PGR_CMD_NEW_LO, insert new large object and mapping
	 * unless the mapping is already there, which means that we are restoring
	 * a per-table dump transaction. Just use an existing mapping in this case.
	 */
	if (cmd == PGR_CMD_NEW_LO)
	{
		slave_oid = LOoidGetSlaveOid(master_oid, false);
		if (!OidIsValid(slave_oid))
		{
			slave_oid = inv_create(InvalidOid);

			LOoidCreateMapping(master_oid, slave_oid);
		}
		lobj = inv_open(slave_oid, INV_READ|INV_WRITE, CurrentMemoryContext);
	}
	else if (cmd == PGR_CMD_WRITE_LO)
	{
		/*
		 * Get the mapping between received master and slave lo oids and open
		 * the large object with mapped oid.
		 */
		slave_oid = LOoidGetSlaveOid(master_oid, false);

		if (!OidIsValid(slave_oid))
			ereport(ERROR,
					(errmsg("couldn't find tuple in slave_lo_refs"),
					 errcontext("command: %s, master oid: %d",
								CmdTypeAsStr(cmd), master_oid)));

		/* FIXME: use an appropiate memory context here */
		/* Open large object with slave object id */
		lobj = inv_open(slave_oid, INV_READ|INV_WRITE, CurrentMemoryContext);
	}
	else
	{
		elog(ERROR, "invalid large object restore command %d", cmd);
		/* not reached, but keep compiler quiet */
		lobj = NULL;
		slave_oid = InvalidOid;
	}

	/* Write lodata to the opened or created large object lobj */
	if (lodesc->data_size != inv_write(lobj, lodata, lodesc->data_size))
		elog(ERROR, "can't write %d bytes to large object %u",
			 lodesc->data_size, lobj->id);

	/* Close large object */
	inv_close(lobj);
}

/*
 * PGRRestoreTruncateLO
 * 		Restore a TRUNCATE_LO command
 *
 * Truncate slave_lo_references relation and drop all large objects referenced
 * in it.
 */
static void
PGRRestoreTruncateLO(void)
{
	List	   *slave_loids = NIL;
	ListCell   *cell;

	/* Make sure we cleanup the LO state properly after this stuff */
	lo_noop();

	PushActiveSnapshot(GetTransactionSnapshot());

	/* Get the list of slave LO oids to delete */
	slave_loids = LOoidGetSlaveOids();

	/* Delete each large object */
	foreach(cell, slave_loids)
	{
		Oid		loid = lfirst_oid(cell);

		inv_drop(loid);
	}

	/* Truncate slave_lo_refs */
	heap_truncate(list_make1_oid(ReplSlaveLoRefsId));
	
	PopActiveSnapshot();

	/* clean up */
	list_free(slave_loids);
}

/* 
 * Create a new role using the data in the pg_authid tuple received 
 * or update an existing one. Most of the initialization code is taken 
 * from PGRRestoreInsert.
 */
static void
RestoreNewRole(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh, int slaveno)
{
	Relation		authRel = NULL;
	TupleDesc		authDesc = NULL;
	TupleTableSlot *slot = NULL;
	EState	   	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt, 
					oldcxt;
	HeapTuple 		tuple,
					oldtuple;
	bool 			isnull,
					rpt;
	Datum 			rolenameDatum;
	char 		   *rolename;
	StringInfo 		buf;
	

	authRel = heap_open(AuthIdRelationId, RowExclusiveLock);
	
	authDesc = RelationGetDescr(authRel);

	/*
	 * Create a TupleTableSlot for the Executor operations.  Make sure
	 * it is long-lived.  XXX why doesn't it work to have it in the
	 * per-query context?
	 */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	slot = MakeSingleTupleTableSlot(authDesc);
	MemoryContextSwitchTo(oldcxt);

	state = PGRRestoreInitExecutor(authRel);

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    /* read command data */
  	buf = MCPQueueTxReadPacket(q, false);

	/* Update the ExecutorState snapshot to the new CommandId */

	state->es_snapshot->curcid = state->es_output_cid = 
													GetCurrentCommandId(true);
  
	/* Get a tuple from the buffer */
	tuple = receive_tuple(buf, authDesc);
	rolenameDatum = heap_getattr(tuple, Anum_pg_authid_rolname, 
								 authDesc, &isnull);

	elog(DEBUG5, "command type: %s, tuple: %s",
		 CmdTypeAsStr(ph->cmd_type), tupleToString(tuple, authDesc));

	rolenameDatum = heap_getattr(tuple, Anum_pg_authid_rolname, 
								 authDesc, &isnull);
								
	rolename = DatumGetCString(DirectFunctionCall1(nameout, rolenameDatum));
	
	/* Check whether the role is replicated by this slave */
	if (RoleIsReplicatedBySlave(rolename, slaveno))
		rpt = true;
	else
		rpt = false;
	
	if (rpt)
	{
		oldtuple = SearchSysCache(AUTHNAME,
							  	  rolenameDatum, 
							  	  0, 0, 0);
												
		if (!HeapTupleIsValid(oldtuple))
		{
			/* Role doesn't exist yet, put a new tuple into pg_auth_members */
	
			/* OK, we're all set -- actually execute the stuff */
			ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		
			MemoryContextSwitchTo(TopTransactionContext);

			/*
		 	 * The executor caches some trigger information, which it needs to
		 	 * persist across the transaction.  Make sure we don't allocate it
		 	 * in a short-lived context.
		 	 */
			ExecInsert(slot, NULL, NULL, NULL, state);
			MemoryContextSwitchTo(PGRRestoreCmdCxt);
		}
		else
		{
			/* Modify existing tuple */
			HeapTuple 	update_tuple;
		
			Datum 		values[Natts_pg_authid];
			bool 		nulls[Natts_pg_authid];
			bool 		replace[Natts_pg_authid];
	
			heap_deform_tuple(tuple, authDesc, values, nulls);
	
			MemSet(replace, true, sizeof(replace));
		
			replace[Anum_pg_authid_rolname - 1] = false;
		
			update_tuple = heap_modify_tuple(oldtuple, authDesc, 
							  	  			 values, nulls, replace);
		
			/* See comments above */
			ExecStoreTuple(update_tuple, slot, InvalidBuffer, false);

			MemoryContextSwitchTo(TopTransactionContext);
			ExecUpdate(slot, &(oldtuple->t_self), NULL, NULL, state);
			MemoryContextSwitchTo(PGRRestoreCmdCxt);
			
			ReleaseSysCache(oldtuple);
					
		}
	}
	else
		elog(DEBUG5, "Role \"%s\" is not replicated by slave %d",
					 rolename, slaveno);
					
	PGRRestoreEndExecutor(state);

	/* Release previously allocated tuple table slot */		
	ExecDropSingleTupleTableSlot(slot);

	/* we keep the lock till commit */
	heap_close(authRel, NoLock);
	
	if (rpt)
		auth_file_update_needed();

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

/* Restore ALTER ROLE command */
static void 
RestoreAlterRole(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh, int slaveno)
{
	Relation		authidRel = NULL;
	HeapTuple 		tuple = NULL;
	TupleDesc		tupdesc = NULL;
	TupleTableSlot *slot = NULL;
	EState	   	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt, 
					oldcxt;
	bool 			rpt;
	char 		   *rolename;
	bool 		   *update_mask;
	StringInfo 		buf, 
					str,
					str2;
	
	authidRel = heap_open(AuthIdRelationId, RowExclusiveLock);
	
	tupdesc = RelationGetDescr(authidRel);

	/*
	 * Create a TupleTableSlot for the Executor operations.  Make sure
	 * it is long-lived.  XXX why doesn't it work to have it in the
	 * per-query context?
	 */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	slot = MakeSingleTupleTableSlot(tupdesc);
	MemoryContextSwitchTo(oldcxt);

	state = PGRRestoreInitExecutor(authidRel);

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    /* get the target role name */
  	str = MCPQueueTxReadPacket(q, false);
	rolename = str->data;
	pfree(str);
	
	/* get the tuple with differences between the old and new roles */
	buf = MCPQueueTxReadPacket(q, false);
	tuple = receive_tuple(buf, tupdesc);
		
	str2 = MCPQueueTxReadPacket(q, false);
	update_mask = str2->data;
	pfree(str2);

	/* Check whether the role is replicated by this slave */
	if (RoleIsReplicatedBySlave(rolename, slaveno))
		rpt = true;
	else
		rpt = false;
		
	if (rpt)
	{
		HeapTuple	oldtuple,
					newtuple;
		Datum 		*values;
		bool 	   *nulls;
		int 		natts;
		
		/* Update the ExecutorState snapshot to the new CommandId */
		state->es_snapshot->curcid = state->es_output_cid =
											GetCurrentCommandId(true);
		
		oldtuple = SearchSysCache(AUTHNAME,
							  	  PointerGetDatum(rolename), 
							  	  0, 0, 0);
												
		if (!HeapTupleIsValid(oldtuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("role \"%s\" does not exist", rolename)));
		
		natts = tupdesc->natts;
		
		values = palloc(sizeof(Datum) * natts);
		nulls = palloc(sizeof(Datum) * natts);
		
		heap_deform_tuple(tuple, tupdesc, values, nulls);
		newtuple = heap_modify_tuple(oldtuple, tupdesc, 
									 values, nulls, update_mask);
		
		/* See comments in PGRCollectUpdate */
		ExecStoreTuple(newtuple, slot, InvalidBuffer, false);
								
		MemoryContextSwitchTo(TopTransactionContext);
		ExecUpdate(slot, &(oldtuple->t_self), NULL, NULL, state);
		MemoryContextSwitchTo(PGRRestoreCmdCxt);
		
		ReleaseSysCache(oldtuple);
	}
	else
		elog(DEBUG5, "Role \"%s\" is not replicated by slave %d",
					 rolename, slaveno);
					
	PGRRestoreEndExecutor(state);

	/* Release previously allocated tuple table slot */		
	ExecDropSingleTupleTableSlot(slot);

	/* we keep the lock till commit */
	heap_close(authidRel, NoLock);
	
	if (rpt)
		auth_file_update_needed();

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

/* Restore grant role command */
static void 
RestoreGrantMembers(MCPQueue *q, PlainHeader *ph, CommandHeader *cmdh, 
					int slaveno)
{
	Relation	authMemRel = NULL;
    bool 		initial,
				need_update;
	TupleDesc	authMemDesc = NULL,
				replDesc = NULL;
	TupleTableSlot	*slot = NULL;
	EState	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt,
					oldcxt;

	authMemRel = heap_open(AuthMemRelationId, RowExclusiveLock);
	
	authMemDesc = RelationGetDescr(authMemRel);
	/* 
	 * Get descriptor for the tuple received from master. It differs from
	 * pg_auth_mem tuple by having namedata attributes instead of Oid ones.
	 */
	replDesc = MakeReplAuthMembersTupleDesc();

	/*
	 * Create a TupleTableSlot for the Executor operations.  Make sure
	 * it is long-lived.  XXX why doesn't it work to have it in the
	 * per-query context?
	 */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	slot = MakeSingleTupleTableSlot(authMemDesc);
	MemoryContextSwitchTo(oldcxt);
	
	state = PGRRestoreInitExecutor(authMemRel);

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    initial = true;
	need_update = false;
	
	while (true)
	{
		StringInfo				buf;
		HeapTuple       		replTuple,
								authMemTuple,
								oldtuple;
		Datum					roleDatum,
								memberDatum;
		Form_pg_auth_members 	membersForm;
		char 				   *roleName,
							   *memberName;
		bool 					isnull,
								rpt;
	
		MemoryContextReset(PGRRestoreCmdCxt);

        /* 
         * read command data and possibly exit if none. Note that we
         * require a data for  at least one command to be there.
         */
		buf = MCPQueueTxReadPacket(q, !initial);
		if (buf == NULL)
			break;

		/* Get a tuple from the buffer */
		replTuple = receive_tuple(buf, replDesc);
	
		elog(DEBUG5, "command type: %s, tuple: %s",
			 CmdTypeAsStr(ph->cmd_type), tupleToString(replTuple, replDesc));
		/* 
		 * Check whether we can apply the changes from this tuple.
		 * A pg_auth_members tuple is replicated if either role or
		 * member attributes correspond to the replicated roles.
		 */
		roleDatum = heap_getattr(replTuple, Anum_repl_members_tuple_rolename,
								 replDesc, &isnull);
		roleName = DatumGetCString(DirectFunctionCall1(nameout, roleDatum));
		memberDatum = heap_getattr(replTuple, Anum_repl_members_tuple_membername,
								   replDesc, &isnull);
		memberName = DatumGetCString(DirectFunctionCall1(nameout, memberDatum));
								
		if (!RoleIsReplicatedBySlave(roleName, slaveno) ||
			!RoleIsReplicatedBySlave(memberName, slaveno))
			rpt = false;
		else
			rpt = true;
	
		/*
		 * Advance our command counter for the next tuple to be deleted
		 * See comment in PGRRestoreInsert about this.
		 */
		if (rpt)
		{
			if (!initial)
				CommandCounterIncrement();
			state->es_snapshot->curcid = state->es_output_cid =
										GetCurrentCommandId(true);
		}
		initial = false;
		
		/* Skip if we have nothing to do here */
		if (!rpt)
			continue;

		need_update = true;
		
		/* 
		 * Make a new auth_members tuple. Skip restoring the tuple if some
		 * roles doesn't exist yet on the slave.
		 */
		authMemTuple = form_authmem_tuple(replTuple, replDesc, authMemDesc);		
		if (authMemTuple == NULL)
			continue;
			
		membersForm = (Form_pg_auth_members) GETSTRUCT(authMemTuple);
			
		/* 
		 * Check whether we already have existing tuple with matching role and
		 * grantor.
		 */
		oldtuple = SearchSysCache(AUTHMEMROLEMEM,
								  ObjectIdGetDatum(membersForm->roleid),
								  ObjectIdGetDatum(membersForm->member),
								  0, 0);
		/* (role, member) pair is not present in pg_auth_members yet, add it */
		if (!HeapTupleIsValid(oldtuple))
		{
			/* OK, we're all set -- actually execute the stuff */
			ExecStoreTuple(authMemTuple, slot, InvalidBuffer, false);
			
			/*
			 * The executor caches some trigger information, which it needs to
			 * persist across the transaction.  Make sure we don't allocate it
			 * in a short-lived context.
			 */
			MemoryContextSwitchTo(TopTransactionContext);
			ExecInsert(slot, NULL, NULL, NULL, state);
			MemoryContextSwitchTo(PGRRestoreCmdCxt);
						
		}
		else 
		{
			/* A tuple already exists, check whether we should alter it */
			Form_pg_auth_members oldMembersForm = 
									(Form_pg_auth_members) GETSTRUCT(oldtuple);
			/* 
			 * We should modify a tuple if
			 * - admin option for the old and the new one doesn't match
			 * - the grantor for the new tuple differs from the old one
			 */
			if (oldMembersForm->admin_option != membersForm->admin_option ||
				oldMembersForm->grantor != membersForm->grantor)
			{
				Datum 	values[Natts_pg_auth_members];
				bool 	nulls[Natts_pg_auth_members];
				bool 	replace[Natts_pg_auth_members];
				HeapTuple update_tuple;
				
				MemSet(replace, 0, sizeof(replace));
				
				heap_deform_tuple(authMemTuple, authMemDesc, values, nulls);
				replace[Anum_pg_auth_members_grantor - 1] = true;
				replace[Anum_pg_auth_members_admin_option - 1] = true;
				
				update_tuple = heap_modify_tuple(oldtuple, authMemDesc,
												 values, nulls, replace);
				
				/* OK, we're all set -- actually execute the stuff */
				ExecStoreTuple(update_tuple, slot, InvalidBuffer, false);

				/*
				 * The executor caches some trigger information, which it 
				 * needs to persist across the transaction.  Make sure we 
				 * don't allocate it in a short-lived context.
				 */
				MemoryContextSwitchTo(TopTransactionContext);
				ExecUpdate(slot, &(oldtuple->t_self), NULL, NULL, state);
				MemoryContextSwitchTo(PGRRestoreCmdCxt);	
				
			}
			else 
				ereport(WARNING, 
						(errmsg("Replication of grant role failed"),
						 errdetail("Role oid %d is already granted to oid %d",
									membersForm->roleid, membersForm->member)));
			ReleaseSysCache(oldtuple);
		}
	}	/* loop around tuples in the command */

	/* cleanup */

	PGRRestoreEndExecutor(state);

	/* Release previously allocated tuple table slot */		
	ExecDropSingleTupleTableSlot(slot);

	/* we keep the lock till commit */
	heap_close(authMemRel, NoLock);
	
	if (need_update)
		auth_file_update_needed();

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);

}

/* Restore revoke role command */
static void RestoreRevokeMembers(MCPQueue *q, PlainHeader *ph,
								 CommandHeader *cmdh, int slaveno)
{
	Relation	authMemRel = NULL;
    bool 		initial,
				need_update;
	TupleDesc	authMemDesc = NULL,
				replDesc = NULL;
	TupleTableSlot	*slot = NULL;
	EState	   *state = NULL;
	MemoryContext   PGRRestoreCmdCxt,
					oldcxt;

	authMemRel = heap_open(AuthMemRelationId, RowExclusiveLock);
	
	authMemDesc = RelationGetDescr(authMemRel);
	/* 
	 * Get descriptor for the tuple received from master. It differs from
	 * pg_auth_mem tuple by having namedata attributes instead of Oid ones.
	 */
	replDesc = MakeReplAuthMembersTupleDesc();

	/*
	 * Create a TupleTableSlot for the Executor operations.  Make sure
	 * it is long-lived.  XXX why doesn't it work to have it in the
	 * per-query context?
	 */
	oldcxt = MemoryContextSwitchTo(TopTransactionContext);
	slot = MakeSingleTupleTableSlot(authMemDesc);
	MemoryContextSwitchTo(oldcxt);
	
	state = PGRRestoreInitExecutor(authMemRel);

	PGRRestoreCmdCxt = AllocSetContextCreate(TopTransactionContext,
											 "PGRRestore Cmd Cxt",
											 ALLOCSET_DEFAULT_MINSIZE,
											 ALLOCSET_DEFAULT_INITSIZE,
											 ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(PGRRestoreCmdCxt);

    initial = true;
	need_update = false;

	while (true)
	{
		StringInfo				buf;
		HeapTuple       		replTuple,
								authMemTuple,
								oldtuple;	
		Form_pg_auth_members 	membersForm;
		Datum 					roleDatum,
								memberDatum;
		char 				   *roleName,
							   *memberName;
		bool					isnull,
								rpt,
								revoke_admin;
	
		MemoryContextReset(PGRRestoreCmdCxt);

        /* 
         * read command data and possibly exit if none. Note that we
         * require a data for  at least one command to be there.
         */
		buf = MCPQueueTxReadPacket(q, !initial);
		if (buf == NULL)
			break;

		/* Get a tuple from the buffer */
		replTuple = receive_tuple(buf, replDesc);
		
		elog(DEBUG5, "command type: %s, tuple: %s",
			 CmdTypeAsStr(ph->cmd_type), tupleToString(replTuple, replDesc));

		/* 
		 * Check whether we can apply the changes from this tuple.
		 * A pg_auth_members tuple is replicated if either role or
		 * member attributes correspond to the replicated roles.
		 */
		roleDatum = heap_getattr(replTuple, Anum_repl_members_tuple_rolename,
								 replDesc, &isnull);
		roleName = DatumGetCString(DirectFunctionCall1(nameout, roleDatum));
		memberDatum = heap_getattr(replTuple, Anum_repl_members_tuple_membername,
								   replDesc, &isnull);
		memberName = DatumGetCString(DirectFunctionCall1(nameout, memberDatum));
		revoke_admin = heap_getattr(replTuple,
									Anum_repl_members_tuple_revoke_admin,
									replDesc, &isnull);
								
		if (!RoleIsReplicatedBySlave(roleName, slaveno) ||
			!RoleIsReplicatedBySlave(memberName, slaveno))
			rpt = false;
		else
			rpt = true;
		
		/*
		 * Advance our command counter for the next tuple to be deleted
		 * See comment in PGRRestoreInsert about this.
		 */
		if (rpt)
		{
			if (!initial)
				CommandCounterIncrement();
			state->es_snapshot->curcid = state->es_output_cid =
										GetCurrentCommandId(true);
		}
		initial = false;
		
		/* Skip if we have nothing to do here */
		if (!rpt)
			continue;
			
		need_update = true;
		
		/* 
		 * Make a new auth_members tuple. Skip restoring the tuple if some
		 * roles doesn't exist yet on the slave.
		 */
		authMemTuple = form_authmem_tuple(replTuple, replDesc, authMemDesc);		
		if (authMemTuple == NULL)
			continue;
			
		membersForm = (Form_pg_auth_members) GETSTRUCT(authMemTuple);
			
		/* 
		 * Check whether we already have existing tuple with matching role and
		 * grantor.
		 */
		oldtuple = SearchSysCache(AUTHMEMROLEMEM,
								  ObjectIdGetDatum(membersForm->roleid),
								  ObjectIdGetDatum(membersForm->member),
								  0, 0);
								
		if (HeapTupleIsValid(oldtuple))
		{
			/* A tuple already exists, check whether we should remove it */
			Form_pg_auth_members oldMembersForm = 
									(Form_pg_auth_members) GETSTRUCT(oldtuple);
									
			/* Require a grantor to match also before removing a tuple */
			if (oldMembersForm->grantor == membersForm->grantor)
			{
				/*
				 * Check whether we have to update it (revoking admin rights) or
				 * just remove.
				 */
				if (revoke_admin)
				{
					Datum 		values[Natts_pg_auth_members];
					bool  		nulls[Natts_pg_auth_members];
					bool  		replace[Natts_pg_auth_members];
					HeapTuple 	newtuple;

					MemSet(values, 0, sizeof(values));
					MemSet(nulls, 0, sizeof(nulls));
					MemSet(replace, 0, sizeof(replace));

					/* reset admin options on a tuple to update */
					values[Anum_pg_auth_members_admin_option - 1] = BoolGetDatum(false);
					nulls[Anum_pg_auth_members_admin_option - 1] = false;
					replace[Anum_pg_auth_members_admin_option - 1] = true;

					newtuple = heap_modify_tuple(oldtuple, authMemDesc, 
												 values, nulls, replace);
					ExecStoreTuple(newtuple, slot, InvalidBuffer, false);

					MemoryContextSwitchTo(TopTransactionContext);
					ExecUpdate(slot, &oldtuple->t_self, NULL, NULL, state);
					MemoryContextSwitchTo(PGRRestoreCmdCxt);
				}
				else
				{

					/* Remove the old tuple */
					ExecStoreTuple(oldtuple, slot, InvalidBuffer, false);

					/*
					 * Switch to TopTransactionContext, see comments in
					 * PGRCollectInsert
					 */
					MemoryContextSwitchTo(TopTransactionContext);
					ExecDelete(&(oldtuple->t_self), NULL, NULL, state);
					MemoryContextSwitchTo(PGRRestoreCmdCxt);
				}
			}
			else
				ereport(WARNING, 
						(errmsg("Grantor oids doesn't match when revoking"
								" role oid %d from role oid %d",
								membersForm->roleid, 
								membersForm->member),
						 errdetail("Existing grantor oid %d, received oid %d ",
									oldMembersForm->grantor,
									membersForm->grantor)));		
			ReleaseSysCache(oldtuple);							
		}
		else
			ereport(WARNING, 
					(errmsg("Replication of revoke role failed"),
					 errdetail("role oid %d is not granted to role oid %d", 
								membersForm->roleid, membersForm->member)));
	}	/* loop around tuples in the command */

	/* cleanup */

	PGRRestoreEndExecutor(state);

	/* Release previously allocated tuple table slot */		
	ExecDropSingleTupleTableSlot(slot);

	/* we keep the lock till commit */
	heap_close(authMemRel, NoLock);
	
	if (need_update)
		auth_file_update_needed();

	MemoryContextSwitchTo(TopTransactionContext);
	MemoryContextDelete(PGRRestoreCmdCxt);
}

/*
 * pgr_formtuple
 *
 * Change all large object ids from master to corresponding slave oids
 * using PGRGetLOid to extract information from pg_catalog.repl_slave_lo_refs
 */
static HeapTuple
pgr_formtuple(HeapTuple tuple, Relation relation)
{
	TupleDesc	desc = RelationGetDescr(relation);
	int			natts = desc->natts;
	Datum		values[natts];
	char		nulls[natts];
	char		repl[natts];
	ListCell   *cell;
	List	   *cols = get_relation_lo_columns(relation);

	MemSet(nulls, ' ', natts);
	MemSet(values, 0, natts * sizeof(Datum));
	MemSet(repl, ' ', natts);

	foreach (cell, cols)
	{
		int		colno = lfirst_int(cell);
		bool	isnull;
		Oid		master_oid,
				slave_oid;

		/* Get loid from this attribute of current tuple */
		master_oid = DatumGetObjectId(heap_getattr(tuple, colno, desc, &isnull));

		if (!isnull)
		{
			/* Modify it according to the slave large object id */
			slave_oid = LOoidGetSlaveOid(master_oid, false);

			if (OidIsValid(slave_oid))
			{
				values[colno - 1] = ObjectIdGetDatum(slave_oid);
				repl[colno - 1] = 'r';
			}
		}
	}

	return heap_modifytuple(tuple, RelationGetDescr(relation), values, nulls,
							repl);
}

/* Make a new pg_auth_members tuple using the data from the master's tuple.
 * Caller should hold a lock on pg_auth_members relation.
 */
static HeapTuple
form_authmem_tuple(HeapTuple replTuple, 
				   TupleDesc replDesc, TupleDesc authMemDesc)
{
	bool 		skip = false;
	int 		i;
	
	Datum 		old_values[Natts_repl_members_tuple];
	Datum 		new_values[Natts_pg_auth_members];
	bool 		nulls[Natts_repl_members_tuple];
	

	heap_deform_tuple(replTuple, replDesc, old_values, nulls);
	
	/* Convert role names to Oids */
	for (i = 0;
		 i < Anum_repl_members_tuple_grantorname; i++)
	{
		char   *rolename;
		Oid 	roleid;
		
		rolename = DatumGetCString(DirectFunctionCall1(nameout, old_values[i]));
		roleid = get_roleid(rolename);
		
		if (!OidIsValid(roleid))
		{
			elog(WARNING, "role \"%s\" does not exist, skip restoring "
						  " current command", rolename);
			skip = true;
			break;	
		}
		new_values[i] = ObjectIdGetDatum(roleid);
	}
	if (!skip)
	{
		HeapTuple newtuple;
			
		new_values[Anum_pg_auth_members_admin_option - 1] =
						old_values[Anum_pg_auth_members_admin_option - 1];
		newtuple = heap_form_tuple(authMemDesc, new_values, nulls);
		return newtuple;
	}
	else
		return NULL;	
}
