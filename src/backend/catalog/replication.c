/*---------------------------------------------------------------------
 *
 * replication.c
 * 		Replicator code for handling the pg_catalog.repl_* system catalogs.
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: replication.c 2209 2009-07-10 19:31:43Z alexk $
 *
 *---------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "catalog/indexing.h"
#include "catalog/mammoth_indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/repl_acl.h"
#include "catalog/repl_authid.h"
#include "catalog/repl_auth_members.h"
#include "catalog/repl_relations.h"
#include "catalog/repl_slave_relations.h"
#include "catalog/repl_slave_roles.h"
#include "catalog/repl_lo_columns.h"
#include "catalog/repl_master_lo_refs.h"
#include "catalog/repl_slave_lo_refs.h"
#include "catalog/replication.h"
#include "mammoth_r/pgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "postmaster/replication.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/relcache.h"
#include "utils/tqual.h"

static bool RelationNeedsReplicationMaster(Relation rel);
static bool RelationNeedsReplicationSlave(Relation rel, unsigned int slave);


/*
 * repl_open_lo_columns
 * 		open pg_catalog.repl_lo_columns and it's index on relid/attnum, lock
 * 		them with the specified lock mode, and return both.
 */
void
repl_open_lo_columns(Relation *rel, Relation *index, LOCKMODE mode)
{
	Relation	lo_columns;
	Relation	lo_columns_idx;

	/* Open heap and index, and lock the heap */
	lo_columns = heap_open(ReplLoColumnsId, mode);
	lo_columns_idx = index_open(ReplLoColumnsRelidAttnumIndexId, mode);

	*rel = lo_columns;
	*index = lo_columns_idx;
}

/*
 * repl_open_slave_lo_refs
 * 		open pg_catalog.repl_slave_lo_refs and it's index on master Oid, lock
 * 		them with the specified lock mode, and return both.
 */
void
repl_open_slave_lo_refs(Relation *rel, Relation *index, LOCKMODE mode)
{
	Relation	slave_lo_refs;
	Relation	slave_lo_refs_idx;

	/* Open heap and index, and lock the heap */
	slave_lo_refs = heap_open(ReplSlaveLoRefsId, mode);
	slave_lo_refs_idx = index_open(ReplSlaveLoRefsMasteroidIndexId, mode);

	*rel = slave_lo_refs;
	*index = slave_lo_refs_idx;
}

/*
 * Given a list, add the relids of the replication catalogs to it.
 * Note: this list had better match the one in RelationNeedsReplication.
 */
List *
get_catalog_relids(List *relids)
{
	relids = lcons_oid(ReplRelationsId, relids);
	relids = lcons_oid(ReplSlaveRelationsId, relids);
	relids = lcons_oid(ReplLoColumnsId, relids);
	relids = lcons_oid(ReplAuthIdRelationId, relids);
	relids = lcons_oid(ReplAuthMemRelationId, relids);
	relids = lcons_oid(ReplAclRelationId, relids);
	relids = lcons_oid(ReplSlaveRolesRelationId, relids);

	return relids;
}

/*
 * get_replicated_relids
 * 		Get the list of Oids of relations marked to be replicated in the
 * 		master, according to the passed snapshot (which should typically be
 * 		CurrentSnapshot or something equivalent.)
 *
 * This is quite straightforward -- scan pg_catalog.repl_relations and return
 * the mentioned relids that have enable=true.  Note: we do not include the
 * replication catalogs in this list.
 * XXX: can we use SnapshotNow and remove the only parameter?
 */
List *
get_replicated_relids(Snapshot snap)
{
	Relation	rels;
	HeapScanDesc scan;
	HeapTuple	tuple;
	List	   *relids = NIL;

	rels = heap_open(ReplRelationsId, AccessShareLock);
	scan = heap_beginscan(rels, snap, 0, NULL);

	while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_repl_relations		replForm;
		Relation	rel;

		/* skip tuples with nulls; they identify non-existant relations */
		if (HeapTupleHasNulls(tuple))
			continue;

		replForm = (Form_repl_relations) GETSTRUCT(tuple);
		if (!replForm->enable)
			continue;

		/*
		 * Verify that the relation still exists, for the case where invalid
		 * entries were left in pg_catalog.repl_relations.
		 */
		rel = RelationIdGetRelation(replForm->relid);
		if (!RelationIsValid(rel))
			continue;
		RelationClose(rel);

		relids = lappend_oid(relids, replForm->relid);
	}

	heap_endscan(scan);
	heap_close(rels, AccessShareLock);

	return relids;
}

/*
 * get_slave_replicated_relids
 * 		Ditto, but for a slave.
 *
 * Here we scan pg_catalog.repl_slave_relations and look for tuples with the
 * given slave ID.
 * MERGE: SerializableSnapshot was replaced by SnapshotNow for catalog access.
 */
List *
get_slave_replicated_relids(int slave)
{
	Relation	rels;
	SysScanDesc	scan;
	HeapTuple	tuple;
	List	   *relids = NIL;
	ScanKeyData key[2];

	ScanKeyInit(&key[0],
				Anum_repl_slave_relations_slave,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(slave));
	ScanKeyInit(&key[1],
				Anum_repl_slave_relations_enable,
				BTEqualStrategyNumber, F_BOOLEQ,
				BoolGetDatum(true));

	rels = heap_open(ReplSlaveRelationsId, AccessShareLock);
	scan = systable_beginscan(rels, InvalidOid, false, SnapshotNow,
							  2, key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_repl_slave_relations replForm;
		Relation	rel;

		/* skip tuples with nulls; they identify non-existant relations */
		if (HeapTupleHasNulls(tuple))
			continue;

		replForm = (Form_repl_slave_relations) GETSTRUCT(tuple);

		/*
		 * Verify that the relation still exists, for the case where invalid
		 * entries were left in pg_catalog.repl_slave_relations.
		 */
		rel = RelationIdGetRelation(replForm->relid);
		if (!RelationIsValid(rel))
			continue;
		RelationClose(rel);

		relids = lappend_oid(relids, replForm->relid);
	}

	systable_endscan(scan);
	heap_close(rels, AccessShareLock);

	/*
	 * HACK: add some pg_catalog tables the list of replicated tables.  It
	 * would be better to do this by inserting into the
	 * pg_catalog.repl_relations catalog at catalog creation, but it doesn't
	 * work for pg_catalog.repl_slave_relations because during dump, the master
	 * would send a TRUNCATE to the slaves and the information would be lost.
	 * And the master can't send the hardcoded info to the slaves either,
	 * because it doesn't know what slaves there are (which is an integral part
	 * of the slave_relations tuple).
	 */
	relids = get_catalog_relids(relids);

	return relids;
}

/*
 * Return list of oids for relations replicated by both master and the slave
 * with a gived ID.
 */
List *
get_master_and_slave_replicated_relids(int slaveno)
{
    Relation    repl_rels;
    List       *relids = NIL;
    List       *slave_relids;
    ListCell   *cell;
	Snapshot	CurrentSnapshot = GetTransactionSnapshot();

    Assert(CurrentSnapshot != NULL);

    /* Get a list of relations enabled in repl_slave_relations */
    slave_relids = get_slave_replicated_relids(slaveno);

    repl_rels = heap_open(ReplRelationsId, AccessShareLock);

    /*
     * For each relation replicated by the slave check whether it should
     * be replicated by master.
     */
    foreach(cell, slave_relids)
    {
        SysScanDesc scan;
        Oid         relid;
        HeapTuple   tuple;
        ScanKeyData keys[1];

        relid = lfirst_oid(cell);

        /* Skip special relations from pg_catalog */
        if (get_rel_namespace(relid) == PG_CATALOG_NAMESPACE)
            continue;

        ScanKeyInit(&keys[0], Anum_repl_relations_relid,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(relid));

        scan = systable_beginscan(repl_rels, ReplRelationsRelidIndexId,
                                  true, CurrentSnapshot, 1, keys);

        while (HeapTupleIsValid(tuple = systable_getnext(scan)))
        {
            bool    enable;

            /* Skip tuples with null relid attribute */
            if (HeapTupleHasNulls(tuple))
                continue;

            /*
             * Add relid to the slave's list only it replication is
             * enabled in repl_relations.
             */
            enable = ((Form_repl_relations) GETSTRUCT(tuple))->enable;
            if (enable)
                relids = lappend_oid(relids, relid);

        }
        systable_endscan(scan);
    }
    heap_close(repl_rels, AccessShareLock);

    /* Append system catalog relation */
    relids = get_catalog_relids(relids);
    return relids;
}

/*
 * get_relation_lo_columns
 *
 * Returns a list of the columns with references to large objects for the
 * given relation.
 */
List *
get_relation_lo_columns(Relation rel)
{
	Relation	rel_lo_columns,
				rel_lo_columns_idx;
	IndexScanDesc scan;
	ScanKeyData	keys[1];
	HeapTuple	scantuple;
	List	   *columns = NIL;
	Oid			relid;

	relid = RelationGetRelid(rel);

	/*
	 * Search pg_catalog.repl_lo_columns for the tuples with relation = relid
	 * and lo = true
	 */
	repl_open_lo_columns(&rel_lo_columns, &rel_lo_columns_idx, AccessShareLock);

	ScanKeyInit(&keys[0],
				Anum_repl_lo_columns_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = index_beginscan(rel_lo_columns, rel_lo_columns_idx, SnapshotNow,
						   1, keys);

	while (HeapTupleIsValid(scantuple = index_getnext(scan, ForwardScanDirection)))
	{
		Form_repl_lo_columns colForm;
		/* skip a tuple with null value (relid) - it marks non-existent relation
		 */
		if (HeapTupleHasNulls(scantuple))
			continue;

		colForm = (Form_repl_lo_columns) GETSTRUCT(scantuple);

		if (colForm->lo)
		{
			int abs_colNo = non_dropped_to_absolute_column_number(rel, colForm->attnum);
			columns = lappend_int(columns, abs_colNo);
		}
	}

	index_endscan(scan);
	heap_close(rel_lo_columns, AccessShareLock);
	heap_close(rel_lo_columns_idx, AccessShareLock);

	return columns;
}

/*
 * RelationNeedsReplication
 * 		For a given table, return whether it has been configured to be
 * 		replicated.
 *
 * 		Note that if this is a slave, it won't consider a table as enabled if
 * 		it's enabled only in the slave -- it needs to be enabled in the master
 * 		as well!
 */
bool
RelationNeedsReplication(Relation rel)
{
	if (replication_enable)
	{
		bool master;

		/*
		 * Some pg_catalog relations are hardcoded.
		 *
		 * NB -- these had better match the list in get_catalog_relids.
		 */
		if (RelationGetRelid(rel) == ReplRelationsId ||
			RelationGetRelid(rel) == ReplSlaveRelationsId ||
			RelationGetRelid(rel) == ReplLoColumnsId ||
			RelationGetRelid(rel) == ReplAuthIdRelationId ||
			RelationGetRelid(rel) == ReplAuthMemRelationId ||
			RelationGetRelid(rel) == ReplAclRelationId ||
			RelationGetRelid(rel) == ReplSlaveRolesRelationId)
			return true;

		/*
		 * If this is a regular system catalog (i.e. in the pg_catalog
		 * namespace), and not one of the special relations checked above,
		 * return false quickly.
		 */
		if (RelationGetNamespace(rel) == PG_CATALOG_NAMESPACE)
			return false;

		master = RelationNeedsReplicationMaster(rel);

		if (replication_master)
			return master;
		else if (replication_slave)
			return (master &&
					RelationNeedsReplicationSlave(rel, replication_slave_no));
	}

	return false;
}

/*
 * RelationNeedsReplicationMaster
 * 		For a given table, return whether this table has been configured to be
 * 		replicated in the master
 *
 * This scans pg_catalog.repl_relations looking for a tuple with the given
 * relid.  If found, return the setting of the "enable" flag; else return
 * false (i.e. a table not registered in pg_catalog.repl_relations is not
 * replicated).
 */
static bool
RelationNeedsReplicationMaster(Relation rel)
{
	Relation		repl_rels;
	HeapTuple		tuple;
	SysScanDesc		scan;
	ScanKeyData		key[1];
	bool			result;

	repl_rels = heap_open(ReplRelationsId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_repl_relations_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	scan = systable_beginscan(repl_rels, ReplRelationsRelidIndexId, true,
							  SnapshotNow, 1, key);
	tuple = systable_getnext(scan);

	/*
	 * If there is a tuple, check what it says.  Otherwise assume no
	 * replication. If the tuple has null relid attribute - then relations
	 * doesn't exist on slave and we forbid its replication
	 */
	if (HeapTupleIsValid(tuple) && !HeapTupleHasNulls(tuple))
		result = ((Form_repl_relations) GETSTRUCT(tuple))->enable;
	else
		result = false;

	/* Clean up */
	systable_endscan(scan);
	heap_close(repl_rels, AccessShareLock);

	return result;
}

/*
 * RelationNeedsReplicationSlave
 * 		For a given table, return whether this table has been configured to be
 * 		replicated in the current slave.
 *
 * Same as above, except it checks pg_catalog.repl_slave_relations.
 */
static bool
RelationNeedsReplicationSlave(Relation rel, uint32 slave)
{
	Relation		repl_rels;
	HeapTuple		tuple;
	SysScanDesc		scan;
	ScanKeyData		key[2];
	bool			result;

	repl_rels = heap_open(ReplSlaveRelationsId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_repl_slave_relations_slave,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(slave));
	ScanKeyInit(&key[1],
				Anum_repl_slave_relations_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(RelationGetRelid(rel)));

	scan = systable_beginscan(repl_rels, ReplSlaveRelationsSlaveRelidIndexId,
							  true, SnapshotNow, 2, key);

	tuple = systable_getnext(scan);

	/*
	 * If there is a tuple, check what it says.  Otherwise assume no
	 * replication. If the tuple has null relid attribute - then relations
	 * doesn't exist on slave and we forbid its replication
	 */
	if (HeapTupleIsValid(tuple) && !HeapTupleHasNulls(tuple))
		result = ((Form_repl_slave_relations) GETSTRUCT(tuple))->enable;
	else
		result = false;

	/* Clean up */
	systable_endscan(scan);
	heap_close(repl_rels, AccessShareLock);

	return result;
}

/*
 * Stop replicating a table, master-wise.
 *
 * We just remove the tuple from pg_catalog.repl_relations.
 */
void
StopReplicationMaster(Oid relid)
{
	HeapTuple	tuple;
	Relation	repl_rels;
	SysScanDesc scan;
	ScanKeyData	key[1];
	Relation	rel;
	bool		slave_replication = false;

	elog(DEBUG5, "StopReplicationMaster: called from the %s",
		 replication_master ? "master": "slave");

	slave_replication = replication_slave && replication_enable &&
		!IsReplicatorProcess();
	
	/*
	 * Get the repl_relations tuple for this table, if any, and
	 * delete it.
	 */
	repl_rels = heap_open(ReplRelationsId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repl_relations_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(repl_rels, ReplRelationsRelidIndexId, true,
							  SnapshotNow, 1, key);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		if (slave_replication)
			 ereport(ERROR,
					 (errmsg("relation %u couldn't be deleted on the slave", relid),
					  errdetail("It has references in pg_catalog.repl_relations"),
					  errhint("It looks like you should disable replication for"
							  " this relation on master")));
		simple_heap_delete(repl_rels, &tuple->t_self);
	}

	/*
	 * Flush this relation from the caches, so the replication status is
	 * updated on all backends.
	 */
	CacheInvalidateRelcacheByRelid(relid);

	/* mark the table as deleted from the table list */
	if (replication_enable && replication_master)
	{
		rel = relation_open(relid, AccessShareLock);
		AddRelationToMasterTableList(rel, false);
		relation_close(rel, AccessShareLock);
	}

	/* Clean up. */
	systable_endscan(scan);
	heap_close(repl_rels, RowExclusiveLock);
}

/*
 * Stop replicating a table in a given slave.
 *
 * We just remove the tuple from repl_slave_relations.
 */
void
StopReplicationSlave(Oid relid, int slave)
{
	HeapTuple	tuple;
	Relation	repl_slave_rels;
	SysScanDesc	scan;
	ScanKeyData	key[2];
	bool		slave_replication = false;

	elog(DEBUG5, "StopReplicationSlave: called from the %s",
		 replication_master ? "master": "slave");

	slave_replication = replication_slave && replication_enable &&
		!IsReplicatorProcess();

	/*
	 * Get the repl_slave_relations tuple for this table, if any, and
	 * delete it.
	 */
	repl_slave_rels = heap_open(ReplSlaveRelationsId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repl_slave_relations_slave,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(slave));
	ScanKeyInit(&key[1],
				Anum_repl_slave_relations_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(repl_slave_rels, ReplSlaveRelationsSlaveRelidIndexId,
							  true, SnapshotNow, 2, key);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
	{
		if (slave_replication)
			ereport(ERROR,
					(errmsg("relation %u couldn't be deleted on the slave", relid),
					 errdetail("It has references in pg_catalog.repl_slave_relations"),
					 errhint("It looks like you should disable replication for"
							 " this relation on master")));	
		simple_heap_delete(repl_slave_rels, &tuple->t_self);
	}

	/*
	 * Flush this relation from the caches, so the replication status is
	 * updated on all backends.
	 */
	CacheInvalidateRelcacheByRelid(relid);

	/* Clean up. */
	systable_endscan(scan);
	heap_close(repl_slave_rels, RowExclusiveLock);
}

/*
 * Stop replicating the given LO column.
 *
 * We just remove the tuple from pg_catalog.repl_lo_columns
 */
void
StopLoReplication(Oid relid, int attnum)
{
	HeapTuple	tuple;
	Relation	lo_column;
	Relation	lo_column_idx;
	IndexScanDesc scan;
	ScanKeyData	key[2];
	bool		slave_replication = false;

	elog(DEBUG5, "StopLOReplication: called from the %s",
		 replication_master ? "master": "slave");

	slave_replication = replication_slave && replication_enable &&
		!IsReplicatorProcess();

	repl_open_lo_columns(&lo_column, &lo_column_idx, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repl_lo_columns_idx_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[1],
				Anum_repl_lo_columns_idx_attnum,
				BTEqualStrategyNumber, F_INT4EQ,
				Int32GetDatum(attnum));

	scan = index_beginscan(lo_column, lo_column_idx, SnapshotNow,
						   2, key);

	tuple = index_getnext(scan, ForwardScanDirection);

	if (HeapTupleIsValid(tuple))
	{
		if (slave_replication)
			ereport(ERROR,
					(errmsg("relation %u could not be deleted", relid),
					 errdetail("It has references in pg_catalog.repl_lo_columns."),
					 errhint("Disable large object replication for this relation on the master first.")));	
		
		simple_heap_delete(lo_column, &tuple->t_self);
	}

	/* XXX we may have to cause a relcache invalidation here */

	/* Clean up */
	index_endscan(scan);
	heap_close(lo_column, RowExclusiveLock);
	relation_close(lo_column_idx, RowExclusiveLock);
}

/*
 * LOoidGetMasterRefs
 *
 * Return the number of references a LO oid registers in master_lo_refs.
 * Returns -1 if the Oid is not present in master_lo_refs.
 *
 * If "delete" is true, the tuple will be deleted from master_lo_refs if and
 * only if the number of references is 0.  Otherwise, an ERROR will be raised.
 * (If "delete" is false, no ERROR is raised.)
 */
int
LOoidGetMasterRefs(Oid lobjId, bool delete)
{
	Relation	master_lo_refs;
	SysScanDesc scan;
	ScanKeyData	key[1];
	HeapTuple	tuple;
	int			refs;

	master_lo_refs = heap_open(ReplMasterLoRefsId, RowExclusiveLock);

	ScanKeyInit(&key[0], Anum_master_lo_refs_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(lobjId));

	scan = systable_beginscan(master_lo_refs, ReplMasterLoRefsLoidIndexId,
							  true, SnapshotNow, 1, key);
	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		refs = -1;
	else
	{
		bool	isnull;
		Datum	attr;

		attr = heap_getattr(tuple, Anum_master_lo_refs_refcount,
				   			RelationGetDescr(master_lo_refs), &isnull);
		Assert(!isnull);

		refs = DatumGetInt32(attr);

		if (delete)
		{
			/*
			 * If we were asked to delete it, but there are some remaining
			 * references, abort the deletion.
			 */
			if (refs != 0)
				ereport(ERROR,
						(errmsg("large object %u couldn't be deleted", lobjId),
						 errdetail("%d %s found in master_lo_refs.",
								   refs, (refs > 1) ? "references" : "reference")));

			simple_heap_delete(master_lo_refs, &(tuple->t_self));
		}
	}

	systable_endscan(scan);
	heap_close(master_lo_refs, RowExclusiveLock);

	return refs;
}

/*
 * LOoidGetSlaveOid
 *		Get the slave LO Oid for a given master LO Oid.
 *
 * Return InvalidOid if master oid is not present in slave_lo_refs
 *
 * If "delete" is true, delete the tuple from slave_lo_refs.
 */
Oid
LOoidGetSlaveOid(Oid master_oid, bool delete)
{
	Oid			slave_oid = InvalidOid;
	Relation	slave_lo_refs,
				slave_lo_refs_idx;
	HeapTuple	tuple;
	IndexScanDesc scan;
	ScanKeyData	key[1];

	/* Open slave_lo_refs relation and index relation */
	repl_open_slave_lo_refs(&slave_lo_refs, &slave_lo_refs_idx, AccessShareLock);

	/* Make index scan for master_oid */
	ScanKeyInit(&key[0],
				Anum_slave_lo_refs_masteroid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(master_oid));

	scan = index_beginscan(slave_lo_refs, slave_lo_refs_idx, SnapshotNow,
						   1, key);
	tuple = index_getnext(scan, ForwardScanDirection);
	if (HeapTupleIsValid(tuple))
	{
		bool	isnull;
		Datum	attr;

		/* Get the corresponding slave oid */
		attr = heap_getattr(tuple, Anum_slave_lo_refs_slaveoid,
							RelationGetDescr(slave_lo_refs),
				   			&isnull);
		Assert(!isnull);
		slave_oid = DatumGetObjectId(attr);

		/* Delete the tuple if caller asked us to do it */
		if (delete)
			simple_heap_delete(slave_lo_refs, &(tuple->t_self));
	}

	index_endscan(scan);
	heap_close(slave_lo_refs, AccessShareLock);
	relation_close(slave_lo_refs_idx, AccessShareLock);

	elog(DEBUG5, "LOoidGetSlaveOid: oid %u on master is %u here",
		 master_oid, slave_oid);

	return slave_oid;
}

/*
 * LOoidGetSlaveOids
 * 		Return an Oid list of all the slave-side Oids of large object that
 * 		came from replication.
 */
List *
LOoidGetSlaveOids(void)
{
	Relation	slave_lo_refs,
				slave_lo_refs_idx;
	HeapTuple	tuple;
	HeapScanDesc scan;
	List	   *oidlist = NIL;

	/* Seqscan slave_lo_refs to fetch all slave Oids */
	repl_open_slave_lo_refs(&slave_lo_refs, &slave_lo_refs_idx, AccessShareLock);

	scan = heap_beginscan(slave_lo_refs, SnapshotNow, 0, NULL);
	while (HeapTupleIsValid(tuple = heap_getnext(scan, ForwardScanDirection)))
	{
		bool	isnull;
		Datum	attr;

		/* Get the slave Oid and stash it into the list */
		attr = heap_getattr(tuple, Anum_slave_lo_refs_slaveoid,
							RelationGetDescr(slave_lo_refs),
							&isnull);
		Assert(!isnull);
		oidlist = lappend_oid(oidlist, DatumGetObjectId(attr));
	}

	/* clean up */
	heap_endscan(scan);
	heap_close(slave_lo_refs, AccessShareLock);
	relation_close(slave_lo_refs_idx, AccessShareLock);

	return oidlist;
}

/*
 * LOoidCreateMapping
 *
 * Create a new map between master and slave large object Oids in slave_lo_refs
 */
void
LOoidCreateMapping(Oid master_oid, Oid slave_oid)
{
	Relation	slave_lo_refs,
				slave_lo_refs_idx;
	HeapTuple	refs_tup;
	Datum		values[Natts_slave_lo_refs];
	char		nulls[Natts_slave_lo_refs];

	/* Open slave_lo_refs to create the mapping between master and slave oids */
	repl_open_slave_lo_refs(&slave_lo_refs, &slave_lo_refs_idx,
							RowExclusiveLock);

	/* Form the tuple */
	MemSet(nulls, ' ', Natts_slave_lo_refs);
	values[Anum_slave_lo_refs_masteroid - 1] = ObjectIdGetDatum(master_oid);
	values[Anum_slave_lo_refs_slaveoid - 1] = ObjectIdGetDatum(slave_oid);
	refs_tup = heap_formtuple(RelationGetDescr(slave_lo_refs), values, nulls);

	/* And insert it */
	simple_heap_insert(slave_lo_refs, refs_tup);

	CatalogUpdateIndexes(slave_lo_refs, refs_tup);

	/* Clean palloced tuple after insertion */
	heap_freetuple(refs_tup);

	/* Close slave_lo_refs relation and its index */
	heap_close(slave_lo_refs, RowExclusiveLock);
	relation_close(slave_lo_refs_idx, RowExclusiveLock);
}
