/*
 * $Id: pgr_authid.c 2115 2009-04-28 15:16:11Z alexk $
 */

#include "postgres.h"

#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/repl_authid.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "postmaster/replication.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static void
create_new_role(Relation pg_authid_rel, HeapTuple tuple,
				TupleDesc descr);
static void     delete_role(Relation pg_authid_rel, HeapTuple tuple);

/*
 * replicate_authid
 *
 * React to a change in repl_authid by creating, deleting or changing roles
 * as appropiate.
 */
Datum
replicate_authid(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple       reltuple;
	TupleDesc       reldesc;
	Relation        pg_authid_rel;
	TupleDesc		pg_authid_dsc;

	MemoryContext   oldcxt, cxt;

	/* basic sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "trigger function not called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		reltuple = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		reltuple = trigdata->tg_newtuple;
	else
		reltuple = NULL;		/* keep compiler quiet */

	reldesc = RelationGetDescr(trigdata->tg_relation);

	if (!replication_enable || replication_master)
		return PointerGetDatum(reltuple);

	elog(DEBUG5, "replicate_authid starting");

	cxt = AllocSetContextCreate(CurTransactionContext,
								"replicate_authid cxt",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(cxt);

	/*
	 * Open and lock pg_authid, because we will be updating it.  Also we need
	 * to protect the writing of the user file.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, ExclusiveLock);
	pg_authid_dsc = RelationGetDescr(pg_authid_rel);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		HeapTuple       newtup;	/* pg_authid tuple of new role */
		HeapTuple       oldtup;	/* pg_authid tuple of old role */
		HeapTuple       tup;	/* one of the above, to be the update target */
		HeapTuple       new_tuple;		/* new tuple to be inserted in
										 * pg_authid */
		char           *newname;
		Datum           values[Natts_pg_authid];
		char            nulls[Natts_pg_authid];
		bool            is_null;

		/*---------
		 * We merge the INSERT and UPDATE cases by assuming that INSERT is an
		 * UPDATE where the old tuple represents a user that doesn't exist in
		 * the slave.  Then consider the result as four different cases:
		 *
		 * 1. Both users exist.  Delete the old and update the new.
		 * 2. Neither user exist.  Create the new user.  (INSERT should
		 *    commonly be this one.)
		 * 3. The old user exists.  Update it.
		 * 4. The new user exists.  Update it.
		 *---------
		 */
		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		{
			oldtup = NULL;
		}
		else
		{
			/* update */
			char           *rolename;

			rolename = SPI_getvalue(trigdata->tg_trigtuple,
									reldesc, Anum_repl_authid_rolname);
			oldtup = SearchSysCache(AUTHNAME,
									CStringGetDatum(rolename),
									0, 0, 0);
		}

		newname = SPI_getvalue(reltuple, reldesc, Anum_repl_authid_rolname);
		newtup = SearchSysCache(AUTHNAME,
								CStringGetDatum(newname),
								0, 0, 0);

		if (HeapTupleIsValid(oldtup) && HeapTupleIsValid(newtup) &&
			newtup != oldtup)
		{
			/*
			 * Case 1: both users exist (and they are different).  In this
			 * case, we must destroy the old user and update the new one to
			 * match the master.
			 */
			delete_role(pg_authid_rel, oldtup);

			ReleaseSysCache(oldtup);
			oldtup = NULL;
			/*
			 * fall through to the code below, which will update the new user
			 */
		}
		else if (!HeapTupleIsValid(oldtup) && !HeapTupleIsValid(newtup))
		{
			/*
			 * Case 2: neither role exist.  Create a new one.
			 */
			create_new_role(pg_authid_rel, reltuple, reldesc);

			/* all done */
			goto updated;
		}

		MemSet(nulls, ' ', sizeof(nulls));

		/*
		 * One of the roles exists, the other doesn't.  Update the existing
		 * one to match the master.
		 */
		if (HeapTupleIsValid(oldtup))
		{
			Assert(!HeapTupleIsValid(newtup) || (oldtup == newtup));

			tup = oldtup;
			/* get the rolename from the repl_shadow tuple */
			values[Anum_pg_authid_rolname - 1] =
				heap_getattr(reltuple, Anum_repl_authid_rolname, reldesc, 
							 &is_null);
			Assert(!is_null);
		}
		else
		{
			Assert(HeapTupleIsValid(newtup));
			Assert(!HeapTupleIsValid(oldtup));

			tup = newtup;
			/*
			 * get the rolename from the pg_authid tuple (i.e. we do not
			 * change the name)
			 */
			values[Anum_pg_authid_rolname - 1] =
				heap_getattr(newtup, Anum_pg_authid_rolname, pg_authid_dsc,
							 &is_null);
			Assert(!is_null);
		}

		/* use the replication.repl_authid tuple from here onwards */
		values[Anum_pg_authid_rolsuper - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolsuper, reldesc, &is_null);
		values[Anum_pg_authid_rolinherit - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolinherit, reldesc, &is_null);
		values[Anum_pg_authid_rolcreaterole - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolcreaterole, reldesc, &is_null);
		values[Anum_pg_authid_rolcreatedb - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolcreatedb, reldesc, &is_null);
		values[Anum_pg_authid_rolcatupdate - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolcatupdate, reldesc, &is_null);
		values[Anum_pg_authid_rolcanlogin - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolcanlogin, reldesc, &is_null);
		values[Anum_pg_authid_rolconnlimit - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolconnlimit, reldesc, &is_null);

		/* Fields above are NOT NULL, but these ones are not.  Be tidy. */
		values[Anum_pg_authid_rolpassword - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolpassword, reldesc, &is_null);
		nulls[Anum_pg_authid_rolpassword - 1] = is_null ? 'n' : ' ';

		values[Anum_pg_authid_rolvaliduntil - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolvaliduntil, reldesc, &is_null);
		nulls[Anum_pg_authid_rolvaliduntil - 1] = is_null ? 'n' : ' ';

		values[Anum_pg_authid_rolconfig - 1] =
			heap_getattr(reltuple, Anum_repl_authid_rolconfig, reldesc, &is_null);
		nulls[Anum_pg_authid_rolconfig - 1] = is_null ? 'n' : ' ';

		new_tuple = heap_formtuple(pg_authid_dsc, values, nulls);

		/* keep the Oid from the existing role */
		HeapTupleSetOid(new_tuple, HeapTupleGetOid(tup));

		simple_heap_update(pg_authid_rel, &tup->t_self, new_tuple);
		CatalogUpdateIndexes(pg_authid_rel, new_tuple);

		if (HeapTupleIsValid(oldtup))
			ReleaseSysCache(oldtup);
		if (HeapTupleIsValid(newtup))
			ReleaseSysCache(newtup);
	}
	/* The role was deleted in the master -- drop him here. */
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		HeapTuple       tuple;
		Datum           rolename;
		bool            is_null;

		elog(DEBUG5, "trigger fired by delete");

		rolename = heap_getattr(reltuple, Anum_repl_authid_rolname, reldesc, &is_null);
		if (is_null)
			elog(ERROR, "got a null rolename");

		tuple = SearchSysCache(AUTHNAME,
							   rolename, 0, 0, 0);

		if (!HeapTupleIsValid(tuple))
			goto done;

		delete_role(pg_authid_rel, tuple);

		ReleaseSysCache(tuple);
	}

updated:
	/* have the auth file updated at commit */
	auth_file_update_needed();

done:
	elog(DEBUG5, "replicate_authid: success");

	/* close the file, but keep the lock until commit */
	heap_close(pg_authid_rel, NoLock);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);

	return PointerGetDatum(reltuple);
}

/*
 * create_new_role
 * 		Insert a pg_authid entry from a replication.repl_shadow tuple.
 *
 * The first parameter is the pg_authid relation, already opened and suitably
 * locked.
 */
static void
create_new_role(Relation pg_authid_rel, HeapTuple tuple,
				TupleDesc descr)
{
	Datum           new_record[Natts_pg_authid];
	char            new_record_nulls[Natts_pg_authid];
	HeapTuple       newtup;
	bool            is_null;

	/*
	 * Build a tuple to insert
	 */
	new_record[Anum_pg_authid_rolname - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolname, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolname - 1] = ' ';

	new_record[Anum_pg_authid_rolsuper - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolsuper, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolsuper- 1] = ' ';
	
	new_record[Anum_pg_authid_rolinherit - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolinherit, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolinherit - 1] = ' ';

	new_record[Anum_pg_authid_rolcreaterole - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolcreaterole, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolcreaterole- 1] = ' ';
	
	new_record[Anum_pg_authid_rolcreatedb - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolcreatedb, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolcreatedb - 1] = ' ';

	new_record[Anum_pg_authid_rolcatupdate - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolcatupdate, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolcatupdate - 1] = ' ';

	new_record[Anum_pg_authid_rolcanlogin - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolcanlogin, descr, &is_null);
	Assert(!is_null);
	new_record_nulls[Anum_pg_authid_rolcanlogin - 1] = ' ';

	new_record[Anum_pg_authid_rolconnlimit - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolconnlimit, descr, &is_null);
	Assert(!is_null);	
	new_record_nulls[Anum_pg_authid_rolconnlimit - 1] = ' ';

	new_record[Anum_pg_authid_rolpassword - 1] =
		heap_getattr(tuple, Anum_repl_authid_rolpassword, descr, &is_null);
	new_record_nulls[Anum_pg_authid_rolpassword - 1] = is_null ? 'n' : ' ';

	new_record[Anum_pg_authid_rolvaliduntil - 1] =
			heap_getattr(tuple, Anum_repl_authid_rolvaliduntil, descr, &is_null);
	new_record_nulls[Anum_pg_authid_rolvaliduntil - 1] = is_null ? 'n' : ' ';

	new_record[Anum_pg_authid_rolconfig - 1] =
			heap_getattr(tuple, Anum_repl_authid_rolconfig, descr, &is_null);
	new_record_nulls[Anum_pg_authid_rolconfig - 1] = is_null ? 'n' : ' ';

	newtup = heap_formtuple(RelationGetDescr(pg_authid_rel),
							new_record, new_record_nulls);

	/* Insert the new record in the pg_authid table */
	simple_heap_insert(pg_authid_rel, newtup);

	/* Update indexes */
	CatalogUpdateIndexes(pg_authid_rel, newtup);

	heap_freetuple(newtup);
}

/*
 * Delete a role, based on a pg_authid tuple.
 *
 * First argument must be the pg_authid relation, already opened and suitably
 * locked.
 *
 * FIXME -- must remove the corresponding pg_auth_members tuple as well?  And
 * what about shared dependencies?
 */
static void
delete_role(Relation pg_authid, HeapTuple tuple)
{
	simple_heap_delete(pg_authid, &tuple->t_self);
}
