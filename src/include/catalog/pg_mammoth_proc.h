/*
 * pg_mammoth_proc.h
 *
 * Function definitions for triggers used in Mammoth Replicator.
 *
 * These are here because we need to cheat to genbki.h to make it insert them
 * on pg_proc during init-mammoth-database.  This is just a crude hack.
 *
 * $Id$
 */

OPEN(pg_proc)

/* NOTE -- these must be kept in sync with the ones in pg_proc.h */
DATA(insert OID = 820 ( replicate_relations     PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_relations - _null_ _null_));
DESCR("fixes repl_relations entry on slaves");
DATA(insert OID = 821 ( replicate_slave_relations PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_slave_relations - _null_ _null_));
DESCR("fixes repl_slave_relations entry on slaves");
DATA(insert OID = 323 ( replicate_lo_columns    PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_lo_columns - _null_ _null_));
DESCR("fixes repl_lo_columns entry on slaves");
DATA(insert OID = 196 ( replicate_authid        PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_authid - _null_ _null_));
DESCR("creates roles from tuples in repl_authid");
DATA(insert OID = 324 ( replicate_auth_members  PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_auth_members - _null_ _null_));
DESCR("creates role members from tuples in repl_auth_members");
DATA(insert OID = 276 ( replicate_acl           PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ replicate_acl - _null_ _null_));
DESCR("updates ACL in tables from tuples in repl_acl");
DATA(insert OID = 824 (drop_replicated_role  PGNSP PGUID 12 1 0 f f t f v 0 2279 "" _null_ _null_ _null_ drop_replicated_role - _null_ _null_ ));
DESCR("drops a role on deleting a corresponding tuple from repl_roles");

DATA(insert OID = 2965 ( forwarder_status		PGNSP PGUID 12 1 0 f f t f v 0 2249 "" "{20,20,20,20,23,16,1184,1184}" "{o,o,o,o,o,o,o,o}" "{brecno,frecno,lrecno,vrecno,sync,empty,in_timestamp,out_timestamp}" forwarder_queue_status - _null_ _null_));
DESCR("reports current replication status for the forwarder");
DATA(insert OID = 2966 ( replication_status		PGNSP PGUID 12 1 0 f f t f v 0 2249 "" "{20,20,20,20,23,16,1184,1184}" "{o,o,o,o,o,o,o,o}" "{brecno,frecno,lrecno,vrecno,sync,empty,in_timestamp,out_timestamp}" mammoth_queue_status - _null_ _null_));
DESCR("reports current replication status");
DATA(insert OID = 2967 ( hosts_status			PGNSP PGUID 12 1 0 f f t t v 0 2249 "" "{23,16,23,1184,20,20}" "{o,o,o,o,o,o}" "{slaveno,connected,sync,timestamp,frecno,vrecno}" mammoth_hosts_status - _null_ _null_ ));
DESCR("reports current status of forwarder host connections");

