/*-------------------------------------------------------------------------
 *
 * mammoth_indexing.h
 *	  This file provides some definitions to support indexing
 *	  on Mammoth system catalogs
 *
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef MAMMOTH_INDEXING_H
#define MAMMOTH_INDEXING_H

#include "access/htup.h"


/*
 * These macros are just to keep the C compiler from spitting up on the
 * upcoming commands for genbki.sh.
 *
 * Note: copied from indexing.h
 *
 * The "primary key" lines are there to indicate the primary keys of each of
 * our catalogs.  We need those to allow replication to work on those catalogs
 * (which is why there's no need to declare a primary key on repl_versions).
 */
#define DECLARE_INDEX(name,oid,decl) extern int no_such_variable
#define DECLARE_UNIQUE_INDEX(name,oid,decl) extern int no_such_variable
#define BUILD_INDICES

DECLARE_UNIQUE_INDEX(repl_versions_version_index, 4539, on repl_versions using btree(version int4_ops));
#define ReplVersionsVersionIndexId	4539

DECLARE_UNIQUE_INDEX(repl_relations_relid_index, 4537, on repl_relations using btree(relid oid_ops));
#define ReplRelationsRelidIndexId	4537
DECLARE_UNIQUE_INDEX(repl_relations_nsp_name_index, 4538, on repl_relations using btree(namespace name_ops, relation name_ops));
#define ReplRelationsNspNameIndexId	4538
/* PRIMARY KEY 4538 */

DECLARE_UNIQUE_INDEX(repl_slave_rels_slave_relid_index, 4540, on repl_slave_relations using btree(slave int4_ops, relid oid_ops));
#define ReplSlaveRelationsSlaveRelidIndexId 4540
DECLARE_UNIQUE_INDEX(repl_slave_rels_slave_nsp_name_index, 4541, on repl_slave_relations using btree(slave int4_ops, namespace name_ops, relation name_ops));
#define ReplSlaveRelationsSlaveNspNameIndexId 4541
/* PRIMARY KEY 4541 */

DECLARE_UNIQUE_INDEX(repl_lo_columns_relid_attnum_index, 4542, on repl_lo_columns using btree(relid oid_ops, attnum int4_ops));
#define ReplLoColumnsRelidAttnumIndexId 4542
DECLARE_UNIQUE_INDEX(repl_lo_columns_nsp_name_attnum_index, 4543, on repl_lo_columns using btree(namespace name_ops, relation name_ops, attnum int4_ops));
#define ReplLoColumnsNspNameAttnumIndexId 4543
/* PRIMARY KEY 4543 */

DECLARE_UNIQUE_INDEX(repl_master_lo_refs_loid_index, 4544, on repl_master_lo_refs using btree(loid oid_ops));
#define ReplMasterLoRefsLoidIndexId 4544
/* PRIMARY KEY 4544 */

DECLARE_UNIQUE_INDEX(repl_slave_lo_refs_masteroid_index, 4545, on repl_slave_lo_refs using btree(masteroid oid_ops));
#define ReplSlaveLoRefsMasteroidIndexId 4545
/* PRIMARY KEY 4545 */

DECLARE_UNIQUE_INDEX(repl_authid_rolname_index, 4547, on repl_authid using btree(rolname name_ops));
#define ReplAuthIdRolnameIndexId 4547
/* PRIMARY KEY 4547 */

DECLARE_UNIQUE_INDEX(repl_authmem_rolname_membname_index, 4549, on repl_auth_members using btree(rolename name_ops, membername name_ops));
#define ReplAuthMembersRolnameMembernameIndexId 4549
/* PRIMARY KEY 4549 */

DECLARE_UNIQUE_INDEX(repl_acl_schema_relname_index, 4551, on repl_acl using btree(schema name_ops, relname name_ops));
#define ReplAclSchemaRelnameIndexId 4551
/* PRIMARY KEY 4551 */

DECLARE_UNIQUE_INDEX(repl_slave_roles_rolename_slave_index, 4563, on repl_slave_roles using btree(rolename name_ops, slave int4_ops));
#define ReplSlaveRolesRolenameSlaveIndexId 4563
/* PRIMARY KEY 4563 */

DECLARE_UNIQUE_INDEX(repl_forwarder_name_index, 4553, on repl_forwarder using btree(name name_ops));
#define ReplForwarderNameIndexId 4553
/* PRIMARY KEY 4553 */

/* last step of initialization script: build the indexes declared above */
BUILD_INDICES

#endif   /* MAMMOTH_INDEXING_H */
