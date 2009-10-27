/*
 * Triggers for Replicator relations in the pg_catalog.repl_* schema.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group.
 * Portions Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: pgr_relations.c 2147 2009-05-25 10:09:05Z alexk $
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/repl_relations.h"
#include "catalog/repl_slave_relations.h"
#include "catalog/repl_lo_columns.h"
#include "catalog/replication.h"
#include "commands/trigger.h"
#include "mammoth_r/agents.h"
#include "nodes/makefuncs.h"
#include "postmaster/replication.h"
#include "utils/builtins.h"
#include "utils/inval.h"


/*
 * replicate_relations
 *
 * Correct a pg_catalog.repl_relations tuple that was just replicated,
 * fixing the relid column to correspond to the Oid of the relation
 * actually present in this slave.
 */
Datum
replicate_relations(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple       tuple;
	HeapTuple       newtuple;
	RangeVar   *rv;
	Form_repl_relations relForm;
	Relation	relation;
	Oid			relid;
	bool		disable = false;
	char		repl_nulls[Natts_repl_relations];
	char		repl_repl[Natts_repl_relations];
	Datum		repl_values[Natts_repl_relations];
	Relation	replicated = InvalidRelation;

	elog(DEBUG5, "replicate_relations starting");

	/* sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "must be called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");

	if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		elog(ERROR, "triggers should be fired by INSERT");

	tuple = trigdata->tg_trigtuple;
	Assert(!HeapTupleHasNulls(tuple));

	/* Only an active slave has work to do here. */
	if (!replication_enable || replication_master)
		return PointerGetDatum(tuple);

	relation = trigdata->tg_relation;

	relForm = (Form_repl_relations) GETSTRUCT(tuple);

	/*
	 * Fetch the local OID of the table, and update the tuple to reflect
	 * it.
	 */
	rv = makeRangeVar(NameStr(relForm->namespace),
					  NameStr(relForm->relation), -1);

	relid = ReplRangeVarGetRelid(rv, true);

	/*
	 * Oops!  The table doesn't exist; turn the enable bit off.
	 */
	if (relid == InvalidOid)
	{
		elog(WARNING, "table %s.%s does not exist on slave",
			 NameStr(relForm->namespace), NameStr(relForm->relation));
		disable = true;
	}

	if (!disable)
	{
		/*
		 * Open the relation so that it doesn't go away under us.  XXX
		 * obviously there's a race condition here.
		 */
		replicated = relation_open(relid, AccessShareLock);

		/*
		 * Check if the tuple is for relation or sequence, and turn the
		 * enable bit off if not.  Arguably we should just skip inserting
		 * the tuple here, but the master should have disallowed inserting
		 * the tuple in the first place if it wasn't a table or sequence,
		 * so let's just insert the tuple because on a subsequent iteration
		 * it may happen that the user has dropped the relation on the
		 * slave and created the correct one.
		 * */
		if (replicated->rd_rel->relkind != RELKIND_RELATION &&
			replicated->rd_rel->relkind != RELKIND_SEQUENCE)
		{
			elog(WARNING, "skipping \"%s\": not a table or sequence",
				 RelationGetRelationName(replicated));
			relation_close(replicated, NoLock);
			disable = true;
		}
	}

	MemSet(repl_nulls, ' ', sizeof(repl_nulls));
	MemSet(repl_repl, ' ', sizeof(repl_repl));

	repl_repl[Anum_repl_relations_relid - 1] = 'r';

	if (!disable)
	{
		repl_values[Anum_repl_relations_relid - 1] = ObjectIdGetDatum(relid);
	}
	else
	{
		/* set relid to NULL, and enable to false */
		repl_nulls[Anum_repl_relations_relid - 1] = 'n';
		repl_repl[Anum_repl_relations_enable - 1] = 'r';
		repl_values[Anum_repl_relations_enable - 1] = BoolGetDatum(false);
	}

	newtuple = heap_modifytuple(tuple, RelationGetDescr(relation), repl_values,
								repl_nulls, repl_repl);

	/* Register dependency on the replication status */
	if (!disable)
	{
		ObjectAddress	depender;
		ObjectAddress	referenced;

		depender.classId = ReplRelationsId;
		depender.objectId = relid;
		depender.objectSubId = 0;

		referenced.classId = RelationRelationId;
		referenced.objectId = relid;
		referenced.objectSubId = 0;

		recordDependencyOn(&depender, &referenced, DEPENDENCY_NORMAL);

		/*
		 * Invalidate the relcache for this table, so that the replicate
		 * flag is correctly propagated.
		 */
		CacheInvalidateRelcache(replicated);

		/* close relation, but keep lock till commit */
		heap_close(replicated, NoLock);
	}

	elog(DEBUG5, "replicate_relations done");

	return PointerGetDatum(newtuple);
}

/*
 * replicate_slave_relations
 *
 * Correct a pg_catalog.repl_slave_relations tuple that was just replicated,
 * fixing the relid column to correspond to the Oid of the relation actually
 * present in this slave.
 */
Datum
replicate_slave_relations(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple       tuple;
	HeapTuple       newtuple;
	RangeVar   *rv;
	Form_repl_slave_relations relForm;
	Relation	relation;
	char		repl_nulls[Natts_repl_slave_relations];
	char		repl_repl[Natts_repl_slave_relations];
	Datum		repl_values[Natts_repl_slave_relations];
	Oid			relid;
	int			slaveno;
	bool		disable = false;
	Relation	replicated = InvalidRelation;

	elog(DEBUG5, "replicate_slave_relations starting");

	/* sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "must be called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");
	if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		elog(ERROR, "trigger should be fired by INSERT");

	tuple = trigdata->tg_trigtuple;
	Assert(!HeapTupleHasNulls(tuple));

	/* Only an active slave has work to do here. */
	if (!replication_enable || replication_master)
		return PointerGetDatum(tuple);

	relation = trigdata->tg_relation;

	relForm = (Form_repl_slave_relations) GETSTRUCT(tuple);

	/* Get a slave attribute to check if this slave tables are
	 * changed by this tuple
	 */
	slaveno = relForm->slave;

	/*
	 * Fetch the local OID of the table, and update the tuple to reflect
	 * it.
	 */
	rv = makeRangeVar(NameStr(relForm->namespace),
					  NameStr(relForm->relation), -1);

	relid = ReplRangeVarGetRelid(rv, true);

	/*
	 * Oops!  The table doesn't exist.  Turn the enable bit off.
	 */
	if (relid == InvalidOid)
	{
		elog(WARNING, "table %s.%s does not exist on slave",
			 NameStr(relForm->namespace), NameStr(relForm->relation));
		disable = true;
	}

	if (!disable)
	{
		/*
		 * Open the relation so that it doesn't go away under us.  XXX
		 * obviously there's a race condition here.
		 */
		replicated = relation_open(relid, AccessShareLock);

		/*
		 * Check if the tuple is for relation or sequence.  See comment in
		 * replicate_relations above.
		 */
		if (replicated->rd_rel->relkind != RELKIND_RELATION &&
			replicated->rd_rel->relkind != RELKIND_SEQUENCE)
		{
			elog(WARNING, "skipping \"%s\": not a table or sequence",
				 RelationGetRelationName(replicated));
			relation_close(replicated, NoLock);
			disable = true;
		}
	}

	MemSet(repl_nulls, ' ', sizeof(repl_nulls));
	MemSet(repl_repl, ' ', sizeof(repl_repl));

	repl_repl[Anum_repl_slave_relations_relid - 1] = 'r';

	if (!disable)
	{
		repl_values[Anum_repl_slave_relations_relid - 1] =
			ObjectIdGetDatum(relid);
	}
	else
	{
		/* set relid to NULL */
		repl_nulls[Anum_repl_slave_relations_relid - 1] = 'n';

		repl_repl[Anum_repl_slave_relations_enable - 1] = 'r';
		repl_values[Anum_repl_slave_relations_enable -1] =
			BoolGetDatum(false);
	}

	newtuple = heap_modifytuple(tuple, RelationGetDescr(relation), repl_values,
								repl_nulls, repl_repl);

	if (!disable)
	{
		/* Register dependency on the replication status */

		ObjectAddress	depender;
		ObjectAddress	referenced;

		/*
		 * objectSubId here indicates which slave this slave_relations
		 * entry is for
		 */
		depender.classId = ReplSlaveRelationsId;
		depender.objectId = relid;
		depender.objectSubId = slaveno;

		referenced.classId = RelationRelationId;
		referenced.objectId = relid;
		referenced.objectSubId = 0;

		recordDependencyOn(&depender, &referenced, DEPENDENCY_NORMAL);

		/*
		 * Invalidate the relcache for this file, so that the replicate
		 * flag is correctly propagated.
		 */
		CacheInvalidateRelcache(replicated);

		/* close relation, but keep lock till commit */
		heap_close(replicated, NoLock);
	}

	elog(DEBUG5, "replicate_slave_relations done");

	return PointerGetDatum(newtuple);
}

/*
 * replicate_lo_columns
 *
 * Correct a pg_catalog.repl_lo_columns tuple that was just replicated,
 * fixing the relid column to correspond to the Oid of the relation
 * actually present in this slave.
 */
Datum
replicate_lo_columns(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple       tuple;
	HeapTuple       newtuple;
	
	RangeVar   *rv;
	Form_repl_lo_columns relForm;
	Relation	relation;
	char		repl_nulls[Natts_repl_lo_columns];
	char		repl_repl[Natts_repl_lo_columns];
	Datum		repl_values[Natts_repl_lo_columns];
	Oid			relid;
	bool		disable = false;
	int			colNo;


	elog(DEBUG5, "replicate_lo_columns starting");

	/* sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "must be called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		tuple = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		tuple = trigdata->tg_newtuple;
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		tuple = trigdata->tg_trigtuple;
	else
	{
		/* keep compiler quiet */
		elog(ERROR, "unrecognized trigger event");
		tuple = NULL;
	}

	/* Only an active slave has work to do here. */
	if (!replication_enable || replication_master)
		return PointerGetDatum(tuple);
	
	relation = trigdata->tg_relation;

	if (HeapTupleHasNulls(tuple))
	{
		/* If we are here during INSERT - something went wrong */
		Assert(!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event));

		/* This tuple has null relid - modify it and put a temporary
		 * InvalidOid to the relid here. Note that we do this to avoid
		 * several calls to heap_getattr, because we can't use GETSTRUCT
		 * when a tuple has null atribute
		 */

		MemSet(repl_nulls, ' ', sizeof(repl_nulls));
		MemSet(repl_repl, ' ', sizeof(repl_repl));

		repl_repl[Anum_repl_lo_columns_relid - 1] = 'r';
		repl_values[Anum_repl_lo_columns_relid - 1] =
				ObjectIdGetDatum(InvalidOid);

		/* XXX: This assumes that memory for the current tuple be released
		 * at least at the end of the transactions, or memleak will happen
		 */
		tuple = heap_modifytuple(tuple, RelationGetDescr(relation), repl_values,
								 repl_nulls, repl_repl);

	}
	relForm = (Form_repl_lo_columns) GETSTRUCT(tuple);

	colNo = relForm->attnum;

	/* fetch the local OID of the table, and update the tuple to reflect it */
	rv = makeRangeVar(NameStr(relForm->namespace),
					  NameStr(relForm->relation), -1);

	relid = ReplRangeVarGetRelid(rv, true);

	/*
	 * Oops!  The table doesn't exist.  Inhibit the existance of the tuple
	 * in repl_slave_relations.
	 */
	if (relid == InvalidOid)
	{
		elog(WARNING, "table %s.%s does not exist on slave",
			 NameStr(relForm->namespace), NameStr(relForm->relation));
		disable = true;
	}

	
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		Relation	replicated = InvalidRelation;

		if (!disable)
		{
			/*
		 	 * Open the relation so that it doesn't go away under us.  XXX
		 	 * obviously there's a race condition here.
		 	 */
			replicated = heap_open(relid, AccessShareLock);

			/*
			* Check if the tuple is for relation or sequence.  See comment in
			* replicate_relations above.
			*/
			if (replicated->rd_rel->relkind != RELKIND_RELATION &&
						 replicated->rd_rel->relkind != RELKIND_SEQUENCE)
			{
				elog(WARNING, "skipping \"%s\": not a table or sequence",
					 RelationGetRelationName(replicated));
				relation_close(replicated, NoLock);
				disable = true;
			}
		}

		MemSet(repl_nulls, ' ', sizeof(repl_nulls));
		MemSet(repl_repl, ' ', sizeof(repl_repl));

		repl_repl[Anum_repl_lo_columns_relid - 1] = 'r';

		if (!disable)
		{
			repl_values[Anum_repl_lo_columns_relid - 1] =
					ObjectIdGetDatum(relid);
		}
		else
		{
			/* set relid to NULL */
			repl_nulls[Anum_repl_lo_columns_relid - 1] = 'n';

			repl_repl[Anum_repl_lo_columns_lo - 1] = 'r';
			repl_nulls[Anum_repl_lo_columns_lo - 1] = ' ';
			repl_values[Anum_repl_lo_columns_lo - 1] = BoolGetDatum(false);

		}

		newtuple = heap_modifytuple(tuple, RelationGetDescr(relation), repl_values,
									repl_nulls, repl_repl);

		if (!disable)
		{
			/* Register dependency on the replication status */
			ObjectAddress	depender;
			ObjectAddress	referenced;

			depender.classId = ReplLoColumnsId;
			depender.objectId = relid;
			depender.objectSubId = colNo;

			referenced.classId = RelationRelationId;
			referenced.objectId = relid;
			referenced.objectSubId = 0;

			recordDependencyOn(&depender, &referenced, DEPENDENCY_NORMAL);

			/*
		 	 * Invalidate the relcache for this file, to propagate the flag
		 	 * indicating this relation contains lo references
		 	 * XXX: this optimization is not implemented yet.
		 	 */
			CacheInvalidateRelcache(replicated);

			/* close relation, but keep lock till commit */
			heap_close(replicated, NoLock);
		}
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		if (relid != InvalidOid)
		{
			ObjectAddress	obj;

			/* Delete a registered dependency. This will also call the function
			 * to delete the actual tuple from pg_catalog.repl_relations, but is
			 * has no effect on the slave because the tuple was already deleted
			 * by the current transaction.
			 */			
			obj.objectId = relid;
			obj.objectSubId = colNo;
			obj.classId = ReplLoColumnsId;

			performDeletion(&obj, DROP_RESTRICT);
		}
		newtuple = tuple;
	}
	else
	{
		/* keep compiler quiet */
		elog(ERROR, "unrecognized trigger event");
		newtuple = NULL;
	}

	elog(DEBUG5, "replicate_lo_columns done");

	return PointerGetDatum(newtuple);
}
