/*
 * 	repl_tuples.c
 *		Function definitions for functions that
 *		make tuple descriptors for replicator.
 *	
 * $Id: repl_tuples.c 2059 2009-03-20 00:13:46Z alexk $
 */

#include "postgres.h"

#include "funcapi.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "mammoth_r/repl_tuples.h"

/*
 * Creates a new tuple desc for replication of auth_members records.
 * We can't use the one from pg_auth_members because we need names
 * instead of OIDs.
 */
TupleDesc
MakeReplAuthMembersTupleDesc(void)
{
	TupleDesc 	desc;
	int 		natts = Natts_repl_members_tuple;

	desc = CreateTemplateTupleDesc(natts, false);
	/* Initialize attirbutes */
	TupleDescInitEntry(desc, (AttrNumber) 1, "rolename", NAMEOID, -1, 0);
	TupleDescInitEntry(desc, (AttrNumber) 2, "membername", NAMEOID, -1, 0);
	TupleDescInitEntry(desc, (AttrNumber) 3, "grantorname", NAMEOID, -1, 0);
	TupleDescInitEntry(desc, (AttrNumber) 4, "admin_option", BOOLOID, -1, 0);
	TupleDescInitEntry(desc, (AttrNumber) 5, "revoke_admin", BOOLOID, -1, 0);
	/* Finalize tupledesc creation */
	desc = BlessTupleDesc(desc);
	
	return desc;
}

