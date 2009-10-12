#include "postgres.h"

#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/repl_auth_members.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "lib/stringinfo.h"
#include "postmaster/replication.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * replicate_auth_members
 *
 * Replicate a change in the pg_auth_members catalog.
 *
 * Note that a tuple in that catalog can be updated only to change the admin
 * option or the grantor; the roleid or memberid attrs are never updated.
 * Because those are the primary key attrs, we can be sure that in the UPDATE
 * case there is only one tuple to act onto (not possibly two, like, for
 * example, pg_authid in the case of ALTER USER RENAME).  This simplifies this
 * code quite a bit.
 */
Datum
replicate_auth_members(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	TupleDesc       reldesc = RelationGetDescr(trigdata->tg_relation);
	HeapTuple       reltuple;
	Relation        pg_authid_rel;
	Relation        pg_authmem_rel;
	TupleDesc       pg_authmem_dsc;
	bool			isnull;
	Name			rolename;
	Name			membername;
	Oid				roleid;
	Oid				memberid;

	elog(DEBUG1, "replicate_auth_members starting");

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");

	/* Set the correct result tuple */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		reltuple = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		reltuple = trigdata->tg_newtuple;
	else
		reltuple = NULL;		/* keep compiler quiet */

	if (!replication_enable || replication_master)
		return PointerGetDatum(reltuple);

	/*
	 * Open pg_authid and secure exclusive lock on it, so we can safely write
	 * the flat file, and open pg_auth_members with a regular writer's lock.
	 * This must be done in this order to prevent deadlocks.
	 */
	pg_authid_rel = heap_open(AuthIdRelationId, ExclusiveLock);
	pg_authmem_rel = heap_open(AuthMemRelationId, AccessExclusiveLock);
	pg_authmem_dsc = RelationGetDescr(pg_authmem_rel);

	/* Get the intervening role ids */
	rolename = DatumGetName(heap_getattr(trigdata->tg_trigtuple,
										  Anum_repl_auth_members_rolename,
										  reldesc, &isnull));
	roleid = get_roleid(rolename->data);

	Assert(!isnull);
	membername = DatumGetName(heap_getattr(trigdata->tg_trigtuple,
										   Anum_repl_auth_members_membername,
										   reldesc, &isnull));
	memberid = get_roleid(membername->data);
	Assert(!isnull);

	/*
	 * If one of them doesn't exist in this slave, skip this operation
	 * altogether
	 */
	if (!OidIsValid(roleid) || !OidIsValid(memberid))
		goto notupdated;

	if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		HeapTuple	tup;

		/* Does this membership exist here?  If so, delete it */
		tup = SearchSysCache(AUTHMEMROLEMEM,
							 ObjectIdGetDatum(roleid),
							 ObjectIdGetDatum(memberid),
							 0, 0);

		if (!HeapTupleIsValid(tup))
			goto notupdated;

		simple_heap_delete(pg_authmem_rel, &tup->t_self);
		ReleaseSysCache(tup);
	}
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
			 TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		HeapTuple	tup;
		HeapTuple	newtup;
		Datum		values[Natts_pg_auth_members];
		bool		nulls[Natts_pg_auth_members];
		Oid			grantorid;
		Name		grantorname;

		/*
		 * Get the grantor's Oid.  If we don't have the grantor locally, just
		 * set it to be the role's Oid.  It's better to lose the grantor info
		 * than to actually lose the membership as a whole ...
		 */
		grantorname = DatumGetName(heap_getattr(trigdata->tg_trigtuple,
												Anum_repl_auth_members_grantorname,
												reldesc, &isnull));
		Assert(!isnull);
		grantorid = get_roleid(grantorname->data);
		if (!OidIsValid(grantorid))
			grantorid = roleid;

		tup = SearchSysCache(AUTHMEMROLEMEM,
							 ObjectIdGetDatum(roleid),
							 ObjectIdGetDatum(memberid),
							 0, 0);
		/* The check for validity is postponed */

		values[Anum_pg_auth_members_roleid - 1] = ObjectIdGetDatum(roleid);
		values[Anum_pg_auth_members_member - 1] = ObjectIdGetDatum(memberid);
		values[Anum_pg_auth_members_grantor - 1] = ObjectIdGetDatum(grantorid);
		/* Copy this one from the repl_auth_members tuple */
		values[Anum_pg_auth_members_admin_option - 1] =
			heap_getattr(reltuple, Anum_repl_auth_members_admin_option,
						 reldesc, &isnull);
		Assert(!isnull);

		MemSet(nulls, 0, sizeof(nulls));
		newtup = heap_form_tuple(pg_authmem_dsc, values, nulls);

		if (HeapTupleIsValid(tup))
		{
			/* update it */
			simple_heap_update(pg_authmem_rel, &tup->t_self, newtup);

			ReleaseSysCache(tup);
		}
		else
		{
			/* insert it */
			simple_heap_insert(pg_authmem_rel, newtup);
		}

		CatalogUpdateIndexes(pg_authmem_rel, newtup);
	}

	/* have the auth file updated on commit */
	auth_file_update_needed();

notupdated:
	/* close rels, but keep lock till commit */
	heap_close(pg_authmem_rel, NoLock);
	heap_close(pg_authid_rel, NoLock);

	elog(DEBUG1, "replicate_auth_members: done");

	return PointerGetDatum(reltuple);
}
