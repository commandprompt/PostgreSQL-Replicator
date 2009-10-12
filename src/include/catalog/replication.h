/*---------------------------------------------------------------------
 *
 * replication.h
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: replication.h 2056 2009-03-19 15:52:37Z alvherre $
 *
 *---------------------------------------------------------------------
 */
#ifndef CATALOG_REPLICATION_H
#define CATALOG_REPLICATION_H

#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "storage/lock.h"

extern void repl_open_lo_columns(Relation *rel, Relation *index, LOCKMODE mode);
extern void repl_open_slave_lo_refs(Relation *rel, Relation *index, LOCKMODE mode);
extern List *get_replicated_relids(Snapshot snap);
extern List *get_catalog_relids(List *relids);
extern List *get_slave_replicated_relids(int slave);
extern List *get_master_and_slave_replicated_relids(int slave);
extern List *get_relation_lo_columns(Relation rel);
extern bool RelationNeedsReplication(Relation rel);
extern Oid get_pg_repl_lo_columns_oid(bool failOK);

extern void StopReplicationMaster(Oid relid);
extern void StopReplicationSlave(Oid relid, int slave);
extern void StopLoReplication(Oid relid, int attnum);

extern int LOoidGetMasterRefs(Oid lobjId, bool delete);
extern Oid LOoidGetSlaveOid(Oid master_oid, bool delete);
extern List *LOoidGetSlaveOids(void);
extern void LOoidCreateMapping(Oid master_oid, Oid slave_oid);

#endif /* CATALOG_REPLICATION_H */
