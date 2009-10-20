/*-------------------------------------------------------------------------
 *
 * aclchk.c
 *	  Routines to check access control permissions.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 * NOTES
 *	  See acl.h.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/mammoth_indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "commands/dbcommands.h"
#include "catalog/repl_acl.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "postmaster/replication.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


static void ExecGrant_Relation(InternalGrant *grantStmt);
static void ExecGrant_Database(InternalGrant *grantStmt);
static void ExecGrant_Fdw(InternalGrant *grantStmt);
static void ExecGrant_ForeignServer(InternalGrant *grantStmt);
static void ExecGrant_Function(InternalGrant *grantStmt);
static void ExecGrant_Language(InternalGrant *grantStmt);
static void ExecGrant_Namespace(InternalGrant *grantStmt);
static void ExecGrant_Tablespace(InternalGrant *grantStmt);

static List *objectNamesToOids(GrantObjectType objtype, List *objnames);
static void expand_col_privileges(List *colnames, Oid table_oid,
					  AclMode this_privileges,
					  AclMode *col_privileges,
					  int num_col_privileges);
static void expand_all_col_privileges(Oid table_oid, Form_pg_class classForm,
						  AclMode this_privileges,
						  AclMode *col_privileges,
						  int num_col_privileges);
static AclMode string_to_privilege(const char *privname);
static const char *privilege_to_string(AclMode privilege);
static AclMode restrict_and_check_grant(bool is_grant, AclMode avail_goptions,
						 bool all_privs, AclMode privileges,
						 Oid objectId, Oid grantorId,
						 AclObjectKind objkind, const char *objname,
						 AttrNumber att_number, const char *colname);
static AclMode pg_aclmask(AclObjectKind objkind, Oid table_oid, AttrNumber attnum,
		   Oid roleid, AclMode mask, AclMaskHow how);

static void replicate_grant(char *namespace, char *relname, Acl *new_acl);
static void escape_equal_sign(char *input, char *output);

#ifdef ACLDEBUG
static void
dumpacl(Acl *acl)
{
	int			i;
	AclItem    *aip;

	elog(DEBUG2, "acl size = %d, # acls = %d",
		 ACL_SIZE(acl), ACL_NUM(acl));
	aip = ACL_DAT(acl);
	for (i = 0; i < ACL_NUM(acl); ++i)
		elog(DEBUG2, "	acl[%d]: %s", i,
			 DatumGetCString(DirectFunctionCall1(aclitemout,
												 PointerGetDatum(aip + i))));
}
#endif   /* ACLDEBUG */


/*
 * If is_grant is true, adds the given privileges for the list of
 * grantees to the existing old_acl.  If is_grant is false, the
 * privileges for the given grantees are removed from old_acl.
 *
 * NB: the original old_acl is pfree'd.
 */
static Acl *
merge_acl_with_grant(Acl *old_acl, bool is_grant,
					 bool grant_option, DropBehavior behavior,
					 List *grantees, AclMode privileges,
					 Oid grantorId, Oid ownerId)
{
	unsigned	modechg;
	ListCell   *j;
	Acl		   *new_acl;

	modechg = is_grant ? ACL_MODECHG_ADD : ACL_MODECHG_DEL;

#ifdef ACLDEBUG
	dumpacl(old_acl);
#endif
	new_acl = old_acl;

	foreach(j, grantees)
	{
		AclItem aclitem;
		Acl		   *newer_acl;

		aclitem.	ai_grantee = lfirst_oid(j);

		/*
		 * Grant options can only be granted to individual roles, not PUBLIC.
		 * The reason is that if a user would re-grant a privilege that he
		 * held through PUBLIC, and later the user is removed, the situation
		 * is impossible to clean up.
		 */
		if (is_grant && grant_option && aclitem.ai_grantee == ACL_ID_PUBLIC)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_GRANT_OPERATION),
					 errmsg("grant options can only be granted to roles")));

		aclitem.	ai_grantor = grantorId;

		/*
		 * The asymmetry in the conditions here comes from the spec.  In
		 * GRANT, the grant_option flag signals WITH GRANT OPTION, which means
		 * to grant both the basic privilege and its grant option. But in
		 * REVOKE, plain revoke revokes both the basic privilege and its grant
		 * option, while REVOKE GRANT OPTION revokes only the option.
		 */
		ACLITEM_SET_PRIVS_GOPTIONS(aclitem,
					(is_grant || !grant_option) ? privileges : ACL_NO_RIGHTS,
				   (!is_grant || grant_option) ? privileges : ACL_NO_RIGHTS);

		newer_acl = aclupdate(new_acl, &aclitem, modechg, ownerId, behavior);

		/* avoid memory leak when there are many grantees */
		pfree(new_acl);
		new_acl = newer_acl;

#ifdef ACLDEBUG
		dumpacl(new_acl);
#endif
	}

	return new_acl;
}

/*
 * Restrict the privileges to what we can actually grant, and emit
 * the standards-mandated warning and error messages.
 */
static AclMode
restrict_and_check_grant(bool is_grant, AclMode avail_goptions, bool all_privs,
						 AclMode privileges, Oid objectId, Oid grantorId,
						 AclObjectKind objkind, const char *objname,
						 AttrNumber att_number, const char *colname)
{
	AclMode		this_privileges;
	AclMode		whole_mask;

	switch (objkind)
	{
		case ACL_KIND_COLUMN:
			whole_mask = ACL_ALL_RIGHTS_COLUMN;
			break;
		case ACL_KIND_CLASS:
			whole_mask = ACL_ALL_RIGHTS_RELATION;
			break;
		case ACL_KIND_SEQUENCE:
			whole_mask = ACL_ALL_RIGHTS_SEQUENCE;
			break;
		case ACL_KIND_DATABASE:
			whole_mask = ACL_ALL_RIGHTS_DATABASE;
			break;
		case ACL_KIND_PROC:
			whole_mask = ACL_ALL_RIGHTS_FUNCTION;
			break;
		case ACL_KIND_LANGUAGE:
			whole_mask = ACL_ALL_RIGHTS_LANGUAGE;
			break;
		case ACL_KIND_NAMESPACE:
			whole_mask = ACL_ALL_RIGHTS_NAMESPACE;
			break;
		case ACL_KIND_TABLESPACE:
			whole_mask = ACL_ALL_RIGHTS_TABLESPACE;
			break;
		case ACL_KIND_FDW:
			whole_mask = ACL_ALL_RIGHTS_FDW;
			break;
		case ACL_KIND_FOREIGN_SERVER:
			whole_mask = ACL_ALL_RIGHTS_FOREIGN_SERVER;
			break;
		default:
			elog(ERROR, "unrecognized object kind: %d", objkind);
			/* not reached, but keep compiler quiet */
			return ACL_NO_RIGHTS;
	}

	/*
	 * If we found no grant options, consider whether to issue a hard error.
	 * Per spec, having any privilege at all on the object will get you by
	 * here.
	 */
	if (avail_goptions == ACL_NO_RIGHTS)
	{
		if (pg_aclmask(objkind, objectId, att_number, grantorId,
					   whole_mask | ACL_GRANT_OPTION_FOR(whole_mask),
					   ACLMASK_ANY) == ACL_NO_RIGHTS)
		{
			if (objkind == ACL_KIND_COLUMN && colname)
				aclcheck_error_col(ACLCHECK_NO_PRIV, objkind, objname, colname);
			else
				aclcheck_error(ACLCHECK_NO_PRIV, objkind, objname);
		}
	}

	/*
	 * Restrict the operation to what we can actually grant or revoke, and
	 * issue a warning if appropriate.	(For REVOKE this isn't quite what the
	 * spec says to do: the spec seems to want a warning only if no privilege
	 * bits actually change in the ACL. In practice that behavior seems much
	 * too noisy, as well as inconsistent with the GRANT case.)
	 */
	this_privileges = privileges & ACL_OPTION_TO_PRIVS(avail_goptions);
	if (is_grant)
	{
		if (this_privileges == 0)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
				  errmsg("no privileges were granted for \"%s\"", objname)));
		else if (!all_privs && this_privileges != privileges)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_GRANTED),
			 errmsg("not all privileges were granted for \"%s\"", objname)));
	}
	else
	{
		if (this_privileges == 0)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
			  errmsg("no privileges could be revoked for \"%s\"", objname)));
		else if (!all_privs && this_privileges != privileges)
			ereport(WARNING,
					(errcode(ERRCODE_WARNING_PRIVILEGE_NOT_REVOKED),
					 errmsg("not all privileges could be revoked for \"%s\"", objname)));
	}

	return this_privileges;
}

/*
 * Called to execute the utility commands GRANT and REVOKE
 */
void
ExecuteGrantStmt(GrantStmt *stmt)
{
	InternalGrant istmt;
	ListCell   *cell;
	const char *errormsg;
	AclMode		all_privileges;

	/*
	 * Turn the regular GrantStmt into the InternalGrant form.
	 */
	istmt.is_grant = stmt->is_grant;
	istmt.objtype = stmt->objtype;
	istmt.objects = objectNamesToOids(stmt->objtype, stmt->objects);
	/* all_privs to be filled below */
	/* privileges to be filled below */
	istmt.col_privs = NIL;		/* may get filled below */
	istmt.grantees = NIL;		/* filled below */
	istmt.grant_option = stmt->grant_option;
	istmt.behavior = stmt->behavior;

	/*
	 * Convert the PrivGrantee list into an Oid list.  Note that at this point
	 * we insert an ACL_ID_PUBLIC into the list if an empty role name is
	 * detected (which is what the grammar uses if PUBLIC is found), so
	 * downstream there shouldn't be any additional work needed to support
	 * this case.
	 */
	foreach(cell, stmt->grantees)
	{
		PrivGrantee *grantee = (PrivGrantee *) lfirst(cell);

		if (grantee->rolname == NULL)
			istmt.grantees = lappend_oid(istmt.grantees, ACL_ID_PUBLIC);
		else
			istmt.grantees =
				lappend_oid(istmt.grantees,
							get_roleid_checked(grantee->rolname));
	}

	/*
	 * Convert stmt->privileges, a list of AccessPriv nodes, into an AclMode
	 * bitmask.  Note: objtype can't be ACL_OBJECT_COLUMN.
	 */
	switch (stmt->objtype)
	{
			/*
			 * Because this might be a sequence, we test both relation and
			 * sequence bits, and later do a more limited test when we know
			 * the object type.
			 */
		case ACL_OBJECT_RELATION:
			all_privileges = ACL_ALL_RIGHTS_RELATION | ACL_ALL_RIGHTS_SEQUENCE;
			errormsg = gettext_noop("invalid privilege type %s for relation");
			break;
		case ACL_OBJECT_SEQUENCE:
			all_privileges = ACL_ALL_RIGHTS_SEQUENCE;
			errormsg = gettext_noop("invalid privilege type %s for sequence");
			break;
		case ACL_OBJECT_DATABASE:
			all_privileges = ACL_ALL_RIGHTS_DATABASE;
			errormsg = gettext_noop("invalid privilege type %s for database");
			break;
		case ACL_OBJECT_FUNCTION:
			all_privileges = ACL_ALL_RIGHTS_FUNCTION;
			errormsg = gettext_noop("invalid privilege type %s for function");
			break;
		case ACL_OBJECT_LANGUAGE:
			all_privileges = ACL_ALL_RIGHTS_LANGUAGE;
			errormsg = gettext_noop("invalid privilege type %s for language");
			break;
		case ACL_OBJECT_NAMESPACE:
			all_privileges = ACL_ALL_RIGHTS_NAMESPACE;
			errormsg = gettext_noop("invalid privilege type %s for schema");
			break;
		case ACL_OBJECT_TABLESPACE:
			all_privileges = ACL_ALL_RIGHTS_TABLESPACE;
			errormsg = gettext_noop("invalid privilege type %s for tablespace");
			break;
		case ACL_OBJECT_FDW:
			all_privileges = ACL_ALL_RIGHTS_FDW;
			errormsg = gettext_noop("invalid privilege type %s for foreign-data wrapper");
			break;
		case ACL_OBJECT_FOREIGN_SERVER:
			all_privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;
			errormsg = gettext_noop("invalid privilege type %s for foreign server");
			break;
		default:
			/* keep compiler quiet */
			all_privileges = ACL_NO_RIGHTS;
			errormsg = NULL;
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) stmt->objtype);
	}

	if (stmt->privileges == NIL)
	{
		istmt.all_privs = true;

		/*
		 * will be turned into ACL_ALL_RIGHTS_* by the internal routines
		 * depending on the object type
		 */
		istmt.privileges = ACL_NO_RIGHTS;
	}
	else
	{
		istmt.all_privs = false;
		istmt.privileges = ACL_NO_RIGHTS;

		foreach(cell, stmt->privileges)
		{
			AccessPriv *privnode = (AccessPriv *) lfirst(cell);
			AclMode		priv;

			/*
			 * If it's a column-level specification, we just set it aside in
			 * col_privs for the moment; but insist it's for a relation.
			 */
			if (privnode->cols)
			{
				if (stmt->objtype != ACL_OBJECT_RELATION)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_GRANT_OPERATION),
							 errmsg("column privileges are only valid for relations")));
				istmt.col_privs = lappend(istmt.col_privs, privnode);
				continue;
			}

			if (privnode->priv_name == NULL)	/* parser mistake? */
				elog(ERROR, "AccessPriv node must specify privilege or columns");
			priv = string_to_privilege(privnode->priv_name);

			if (priv & ~((AclMode) all_privileges))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_GRANT_OPERATION),
						 errmsg(errormsg, privilege_to_string(priv))));

			istmt.privileges |= priv;
		}
	}

	ExecGrantStmt_oids(&istmt);
}

/*
 * ExecGrantStmt_oids
 *
 * "Internal" entrypoint for granting and revoking privileges.	This is
 * exported for pg_shdepend.c to use in revoking privileges when dropping
 * a role.
 */
void
ExecGrantStmt_oids(InternalGrant *istmt)
{
	switch (istmt->objtype)
	{
		case ACL_OBJECT_RELATION:
		case ACL_OBJECT_SEQUENCE:
			ExecGrant_Relation(istmt);
			break;
		case ACL_OBJECT_DATABASE:
			ExecGrant_Database(istmt);
			break;
		case ACL_OBJECT_FDW:
			ExecGrant_Fdw(istmt);
			break;
		case ACL_OBJECT_FOREIGN_SERVER:
			ExecGrant_ForeignServer(istmt);
			break;
		case ACL_OBJECT_FUNCTION:
			ExecGrant_Function(istmt);
			break;
		case ACL_OBJECT_LANGUAGE:
			ExecGrant_Language(istmt);
			break;
		case ACL_OBJECT_NAMESPACE:
			ExecGrant_Namespace(istmt);
			break;
		case ACL_OBJECT_TABLESPACE:
			ExecGrant_Tablespace(istmt);
			break;
		default:
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) istmt->objtype);
	}
}

/*
 * objectNamesToOids
 *
 * Turn a list of object names of a given type into an Oid list.
 */
static List *
objectNamesToOids(GrantObjectType objtype, List *objnames)
{
	List	   *objects = NIL;
	ListCell   *cell;

	Assert(objnames != NIL);

	switch (objtype)
	{
		case ACL_OBJECT_RELATION:
		case ACL_OBJECT_SEQUENCE:
			foreach(cell, objnames)
			{
				RangeVar   *relvar = (RangeVar *) lfirst(cell);
				Oid			relOid;

				relOid = RangeVarGetRelid(relvar, false);
				objects = lappend_oid(objects, relOid);
			}
			break;
		case ACL_OBJECT_DATABASE:
			foreach(cell, objnames)
			{
				char	   *dbname = strVal(lfirst(cell));
				Oid			dbid;

				dbid = get_database_oid(dbname);
				if (!OidIsValid(dbid))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_DATABASE),
							 errmsg("database \"%s\" does not exist",
									dbname)));
				objects = lappend_oid(objects, dbid);
			}
			break;
		case ACL_OBJECT_FUNCTION:
			foreach(cell, objnames)
			{
				FuncWithArgs *func = (FuncWithArgs *) lfirst(cell);
				Oid			funcid;

				funcid = LookupFuncNameTypeNames(func->funcname,
												 func->funcargs, false);
				objects = lappend_oid(objects, funcid);
			}
			break;
		case ACL_OBJECT_LANGUAGE:
			foreach(cell, objnames)
			{
				char	   *langname = strVal(lfirst(cell));
				HeapTuple	tuple;

				tuple = SearchSysCache(LANGNAME,
									   PointerGetDatum(langname),
									   0, 0, 0);
				if (!HeapTupleIsValid(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("language \"%s\" does not exist",
									langname)));

				objects = lappend_oid(objects, HeapTupleGetOid(tuple));

				ReleaseSysCache(tuple);
			}
			break;
		case ACL_OBJECT_NAMESPACE:
			foreach(cell, objnames)
			{
				char	   *nspname = strVal(lfirst(cell));
				HeapTuple	tuple;

				tuple = SearchSysCache(NAMESPACENAME,
									   CStringGetDatum(nspname),
									   0, 0, 0);
				if (!HeapTupleIsValid(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_SCHEMA),
							 errmsg("schema \"%s\" does not exist",
									nspname)));

				objects = lappend_oid(objects, HeapTupleGetOid(tuple));

				ReleaseSysCache(tuple);
			}
			break;
		case ACL_OBJECT_TABLESPACE:
			foreach(cell, objnames)
			{
				char	   *spcname = strVal(lfirst(cell));
				ScanKeyData entry[1];
				HeapScanDesc scan;
				HeapTuple	tuple;
				Relation	relation;

				relation = heap_open(TableSpaceRelationId, AccessShareLock);

				ScanKeyInit(&entry[0],
							Anum_pg_tablespace_spcname,
							BTEqualStrategyNumber, F_NAMEEQ,
							CStringGetDatum(spcname));

				scan = heap_beginscan(relation, SnapshotNow, 1, entry);
				tuple = heap_getnext(scan, ForwardScanDirection);
				if (!HeapTupleIsValid(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
					   errmsg("tablespace \"%s\" does not exist", spcname)));

				objects = lappend_oid(objects, HeapTupleGetOid(tuple));

				heap_endscan(scan);

				heap_close(relation, AccessShareLock);
			}
			break;
		case ACL_OBJECT_FDW:
			foreach(cell, objnames)
			{
				char	   *fdwname = strVal(lfirst(cell));
				Oid			fdwid = GetForeignDataWrapperOidByName(fdwname, false);

				objects = lappend_oid(objects, fdwid);
			}
			break;
		case ACL_OBJECT_FOREIGN_SERVER:
			foreach(cell, objnames)
			{
				char	   *srvname = strVal(lfirst(cell));
				Oid			srvid = GetForeignServerOidByName(srvname, false);

				objects = lappend_oid(objects, srvid);
			}
			break;
		default:
			elog(ERROR, "unrecognized GrantStmt.objtype: %d",
				 (int) objtype);
	}

	return objects;
}

/*
 * expand_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each
 * specified attribute.  The per-column array is indexed starting at
 * FirstLowInvalidHeapAttributeNumber, up to relation's last attribute.
 */
static void
expand_col_privileges(List *colnames, Oid table_oid,
					  AclMode this_privileges,
					  AclMode *col_privileges,
					  int num_col_privileges)
{
	ListCell   *cell;

	foreach(cell, colnames)
	{
		char	   *colname = strVal(lfirst(cell));
		AttrNumber	attnum;

		attnum = get_attnum(table_oid, colname);
		if (attnum == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							colname, get_rel_name(table_oid))));
		attnum -= FirstLowInvalidHeapAttributeNumber;
		if (attnum <= 0 || attnum >= num_col_privileges)
			elog(ERROR, "column number out of range");	/* safety check */
		col_privileges[attnum] |= this_privileges;
	}
}

/*
 * expand_all_col_privileges
 *
 * OR the specified privilege(s) into per-column array entries for each valid
 * attribute of a relation.  The per-column array is indexed starting at
 * FirstLowInvalidHeapAttributeNumber, up to relation's last attribute.
 */
static void
expand_all_col_privileges(Oid table_oid, Form_pg_class classForm,
						  AclMode this_privileges,
						  AclMode *col_privileges,
						  int num_col_privileges)
{
	AttrNumber	curr_att;

	Assert(classForm->relnatts - FirstLowInvalidHeapAttributeNumber < num_col_privileges);
	for (curr_att = FirstLowInvalidHeapAttributeNumber + 1;
		 curr_att <= classForm->relnatts;
		 curr_att++)
	{
		HeapTuple	attTuple;
		bool		isdropped;

		if (curr_att == InvalidAttrNumber)
			continue;

		/* Skip OID column if it doesn't exist */
		if (curr_att == ObjectIdAttributeNumber && !classForm->relhasoids)
			continue;

		/* Views don't have any system columns at all */
		if (classForm->relkind == RELKIND_VIEW && curr_att < 0)
			continue;

		attTuple = SearchSysCache(ATTNUM,
								  ObjectIdGetDatum(table_oid),
								  Int16GetDatum(curr_att),
								  0, 0);
		if (!HeapTupleIsValid(attTuple))
			elog(ERROR, "cache lookup failed for attribute %d of relation %u",
				 curr_att, table_oid);

		isdropped = ((Form_pg_attribute) GETSTRUCT(attTuple))->attisdropped;

		ReleaseSysCache(attTuple);

		/* ignore dropped columns */
		if (isdropped)
			continue;

		col_privileges[curr_att - FirstLowInvalidHeapAttributeNumber] |= this_privileges;
	}
}

/*
 *	This processes attributes, but expects to be called from
 *	ExecGrant_Relation, not directly from ExecGrantStmt.
 */
static void
ExecGrant_Attribute(InternalGrant *istmt, Oid relOid, const char *relname,
					AttrNumber attnum, Oid ownerId, AclMode col_privileges,
					Relation attRelation, const Acl *old_rel_acl)
{
	HeapTuple	attr_tuple;
	Form_pg_attribute pg_attribute_tuple;
	Acl		   *old_acl;
	Acl		   *new_acl;
	Acl		   *merged_acl;
	Datum		aclDatum;
	bool		isNull;
	Oid			grantorId;
	AclMode		avail_goptions;
	bool		need_update;
	HeapTuple	newtuple;
	Datum		values[Natts_pg_attribute];
	bool		nulls[Natts_pg_attribute];
	bool		replaces[Natts_pg_attribute];
	int			noldmembers;
	int			nnewmembers;
	Oid		   *oldmembers;
	Oid		   *newmembers;

	attr_tuple = SearchSysCache(ATTNUM,
								ObjectIdGetDatum(relOid),
								Int16GetDatum(attnum),
								0, 0);
	if (!HeapTupleIsValid(attr_tuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 attnum, relOid);
	pg_attribute_tuple = (Form_pg_attribute) GETSTRUCT(attr_tuple);

	/*
	 * Get working copy of existing ACL. If there's no ACL, substitute the
	 * proper default.
	 */
	aclDatum = SysCacheGetAttr(ATTNUM, attr_tuple, Anum_pg_attribute_attacl,
							   &isNull);
	if (isNull)
		old_acl = acldefault(ACL_OBJECT_COLUMN, ownerId);
	else
		old_acl = DatumGetAclPCopy(aclDatum);

	/*
	 * In select_best_grantor we should consider existing table-level ACL bits
	 * as well as the per-column ACL.  Build a new ACL that is their
	 * concatenation.  (This is a bit cheap and dirty compared to merging them
	 * properly with no duplications, but it's all we need here.)
	 */
	merged_acl = aclconcat(old_rel_acl, old_acl);

	/* Determine ID to do the grant as, and available grant options */
	select_best_grantor(GetUserId(), col_privileges,
						merged_acl, ownerId,
						&grantorId, &avail_goptions);

	pfree(merged_acl);

	/*
	 * Restrict the privileges to what we can actually grant, and emit the
	 * standards-mandated warning and error messages.  Note: we don't track
	 * whether the user actually used the ALL PRIVILEGES(columns) syntax for
	 * each column; we just approximate it by whether all the possible
	 * privileges are specified now.  Since the all_privs flag only determines
	 * whether a warning is issued, this seems close enough.
	 */
	col_privileges =
		restrict_and_check_grant(istmt->is_grant, avail_goptions,
								 (col_privileges == ACL_ALL_RIGHTS_COLUMN),
								 col_privileges,
								 relOid, grantorId, ACL_KIND_COLUMN,
								 relname, attnum,
								 NameStr(pg_attribute_tuple->attname));

	/*
	 * Generate new ACL.
	 *
	 * We need the members of both old and new ACLs so we can correct the
	 * shared dependency information.
	 */
	noldmembers = aclmembers(old_acl, &oldmembers);

	new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
								   istmt->grant_option,
								   istmt->behavior, istmt->grantees,
								   col_privileges, grantorId,
								   ownerId);

	nnewmembers = aclmembers(new_acl, &newmembers);

	/* finished building new ACL value, now insert it */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	/*
	 * If the updated ACL is empty, we can set attacl to null, and maybe even
	 * avoid an update of the pg_attribute row.  This is worth testing because
	 * we'll come through here multiple times for any relation-level REVOKE,
	 * even if there were never any column GRANTs.	Note we are assuming that
	 * the "default" ACL state for columns is empty.
	 */
	if (ACL_NUM(new_acl) > 0)
	{
		values[Anum_pg_attribute_attacl - 1] = PointerGetDatum(new_acl);
		need_update = true;
	}
	else
	{
		nulls[Anum_pg_attribute_attacl - 1] = true;
		need_update = !isNull;
	}
	replaces[Anum_pg_attribute_attacl - 1] = true;

	if (need_update)
	{
		newtuple = heap_modify_tuple(attr_tuple, RelationGetDescr(attRelation),
									 values, nulls, replaces);

		simple_heap_update(attRelation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(attRelation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(RelationRelationId, relOid, attnum,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);
	}

	pfree(new_acl);

	ReleaseSysCache(attr_tuple);
}

/*
 *	This processes both sequences and non-sequences.
 */
static void
ExecGrant_Relation(InternalGrant *istmt)
{
	Relation	relation;
	Relation	attRelation;
	ListCell   *cell;

	relation = heap_open(RelationRelationId, RowExclusiveLock);
	attRelation = heap_open(AttributeRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			relOid = lfirst_oid(cell);
		Datum		aclDatum;
		Form_pg_class pg_class_tuple;
		bool		isNull;
		AclMode		this_privileges;
		AclMode    *col_privileges;
		int			num_col_privileges;
		bool		have_col_privileges;
		Acl		   *old_acl;
		Acl		   *old_rel_acl;
		Oid			ownerId;
		HeapTuple	tuple;
		ListCell   *cell_colprivs;

		tuple = SearchSysCache(RELOID,
							   ObjectIdGetDatum(relOid),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", relOid);
		pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);

		/* Not sensible to grant on an index */
		if (pg_class_tuple->relkind == RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is an index",
							NameStr(pg_class_tuple->relname))));

		/* Composite types aren't tables either */
		if (pg_class_tuple->relkind == RELKIND_COMPOSITE_TYPE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a composite type",
							NameStr(pg_class_tuple->relname))));

		/* Used GRANT SEQUENCE on a non-sequence? */
		if (istmt->objtype == ACL_OBJECT_SEQUENCE &&
			pg_class_tuple->relkind != RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is not a sequence",
							NameStr(pg_class_tuple->relname))));

		/* Adjust the default permissions based on whether it is a sequence */
		if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		{
			if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
				this_privileges = ACL_ALL_RIGHTS_SEQUENCE;
			else
				this_privileges = ACL_ALL_RIGHTS_RELATION;
		}
		else
			this_privileges = istmt->privileges;

		/*
		 * The GRANT TABLE syntax can be used for sequences and non-sequences,
		 * so we have to look at the relkind to determine the supported
		 * permissions.  The OR of table and sequence permissions were already
		 * checked.
		 */
		if (istmt->objtype == ACL_OBJECT_RELATION)
		{
			if (pg_class_tuple->relkind == RELKIND_SEQUENCE)
			{
				/*
				 * For backward compatibility, just throw a warning for
				 * invalid sequence permissions when using the non-sequence
				 * GRANT syntax.
				 */
				if (this_privileges & ~((AclMode) ACL_ALL_RIGHTS_SEQUENCE))
				{
					/*
					 * Mention the object name because the user needs to know
					 * which operations succeeded.	This is required because
					 * WARNING allows the command to continue.
					 */
					ereport(WARNING,
							(errcode(ERRCODE_INVALID_GRANT_OPERATION),
							 errmsg("sequence \"%s\" only supports USAGE, SELECT, and UPDATE privileges",
									NameStr(pg_class_tuple->relname))));
					this_privileges &= (AclMode) ACL_ALL_RIGHTS_SEQUENCE;
				}
			}
			else
			{
				if (this_privileges & ~((AclMode) ACL_ALL_RIGHTS_RELATION))
				{
					/*
					 * USAGE is the only permission supported by sequences but
					 * not by non-sequences.  Don't mention the object name
					 * because we didn't in the combined TABLE | SEQUENCE
					 * check.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_GRANT_OPERATION),
						  errmsg("invalid privilege type USAGE for table")));
				}
			}
		}

		/*
		 * Set up array in which we'll accumulate any column privilege bits
		 * that need modification.	The array is indexed such that entry [0]
		 * corresponds to FirstLowInvalidHeapAttributeNumber.
		 */
		num_col_privileges = pg_class_tuple->relnatts - FirstLowInvalidHeapAttributeNumber + 1;
		col_privileges = (AclMode *) palloc0(num_col_privileges * sizeof(AclMode));
		have_col_privileges = false;

		/*
		 * If we are revoking relation privileges that are also column
		 * privileges, we must implicitly revoke them from each column too,
		 * per SQL spec.  (We don't need to implicitly add column privileges
		 * during GRANT because the permissions-checking code always checks
		 * both relation and per-column privileges.)
		 */
		if (!istmt->is_grant &&
			(this_privileges & ACL_ALL_RIGHTS_COLUMN) != 0)
		{
			expand_all_col_privileges(relOid, pg_class_tuple,
									  this_privileges & ACL_ALL_RIGHTS_COLUMN,
									  col_privileges,
									  num_col_privileges);
			have_col_privileges = true;
		}

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_class_tuple->relowner;
		aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(pg_class_tuple->relkind == RELKIND_SEQUENCE ?
								 ACL_OBJECT_SEQUENCE : ACL_OBJECT_RELATION,
								 ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Need an extra copy of original rel ACL for column handling */
		old_rel_acl = aclcopy(old_acl);

		/*
		 * Handle relation-level privileges, if any were specified
		 */
		if (this_privileges != ACL_NO_RIGHTS)
		{
			AclMode		avail_goptions;
			Acl		   *new_acl;
			Oid			grantorId;
			HeapTuple	newtuple;
			Datum		values[Natts_pg_class];
			bool		nulls[Natts_pg_class];
			bool		replaces[Natts_pg_class];
			int			noldmembers;
			int			nnewmembers;
			Oid		   *oldmembers;
			Oid		   *newmembers;

			/* Determine ID to do the grant as, and available grant options */
			select_best_grantor(GetUserId(), this_privileges,
								old_acl, ownerId,
								&grantorId, &avail_goptions);

			/*
			 * Restrict the privileges to what we can actually grant, and emit
			 * the standards-mandated warning and error messages.
			 */
			this_privileges =
				restrict_and_check_grant(istmt->is_grant, avail_goptions,
										 istmt->all_privs, this_privileges,
										 relOid, grantorId,
								  pg_class_tuple->relkind == RELKIND_SEQUENCE
										 ? ACL_KIND_SEQUENCE : ACL_KIND_CLASS,
										 NameStr(pg_class_tuple->relname),
										 0, NULL);

			/*
			 * Generate new ACL.
			 *
			 * We need the members of both old and new ACLs so we can correct
			 * the shared dependency information.
			 */
			noldmembers = aclmembers(old_acl, &oldmembers);

			new_acl = merge_acl_with_grant(old_acl,
										   istmt->is_grant,
										   istmt->grant_option,
										   istmt->behavior,
										   istmt->grantees,
										   this_privileges,
										   grantorId,
										   ownerId);

			nnewmembers = aclmembers(new_acl, &newmembers);

			/* finished building new ACL value, now insert it */
			MemSet(values, 0, sizeof(values));
			MemSet(nulls, false, sizeof(nulls));
			MemSet(replaces, false, sizeof(replaces));

			replaces[Anum_pg_class_relacl - 1] = true;
			values[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);

			newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation),
										 values, nulls, replaces);

			simple_heap_update(relation, &newtuple->t_self, newtuple);

			/* keep the catalog indexes up to date */
			CatalogUpdateIndexes(relation, newtuple);

			/* Update the shared dependency ACL info */
			updateAclDependencies(RelationRelationId, relOid, 0,
								  ownerId, istmt->is_grant,
								  noldmembers, oldmembers,
								  nnewmembers, newmembers);

			pfree(new_acl);
		}

		/*
		 * Handle column-level privileges, if any were specified or implied.
		 * We first expand the user-specified column privileges into the
		 * array, and then iterate over all nonempty array entries.
		 */
		foreach(cell_colprivs, istmt->col_privs)
		{
			AccessPriv *col_privs = (AccessPriv *) lfirst(cell_colprivs);

			if (col_privs->priv_name == NULL)
				this_privileges = ACL_ALL_RIGHTS_COLUMN;
			else
				this_privileges = string_to_privilege(col_privs->priv_name);

			if (this_privileges & ~((AclMode) ACL_ALL_RIGHTS_COLUMN))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_GRANT_OPERATION),
						 errmsg("invalid privilege type %s for column",
								privilege_to_string(this_privileges))));

			if (pg_class_tuple->relkind == RELKIND_SEQUENCE &&
				this_privileges & ~((AclMode) ACL_SELECT))
			{
				/*
				 * The only column privilege allowed on sequences is SELECT.
				 * This is a warning not error because we do it that way for
				 * relation-level privileges.
				 */
				ereport(WARNING,
						(errcode(ERRCODE_INVALID_GRANT_OPERATION),
						 errmsg("sequence \"%s\" only supports SELECT column privileges",
								NameStr(pg_class_tuple->relname))));

				this_privileges &= (AclMode) ACL_SELECT;
			}

			expand_col_privileges(col_privs->cols, relOid,
								  this_privileges,
								  col_privileges,
								  num_col_privileges);
			have_col_privileges = true;
		}

		if (have_col_privileges)
		{
			AttrNumber	i;

			for (i = 0; i < num_col_privileges; i++)
			{
				if (col_privileges[i] == ACL_NO_RIGHTS)
					continue;
				ExecGrant_Attribute(istmt,
									relOid,
									NameStr(pg_class_tuple->relname),
									i + FirstLowInvalidHeapAttributeNumber,
									ownerId,
									col_privileges[i],
									attRelation,
									old_rel_acl);
			}
		}

		pfree(old_rel_acl);
		pfree(col_privileges);

		ReleaseSysCache(tuple);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(attRelation, RowExclusiveLock);
	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Database(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_DATABASE;

	relation = heap_open(DatabaseRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			datId = lfirst_oid(cell);
		Form_pg_database pg_database_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_database];
		bool		nulls[Natts_pg_database];
		bool		replaces[Natts_pg_database];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		HeapTuple	tuple;

		tuple = SearchSysCache(DATABASEOID,
							   ObjectIdGetDatum(datId),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for database %u", datId);

		pg_database_tuple = (Form_pg_database) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_database_tuple->datdba;
		aclDatum = heap_getattr(tuple, Anum_pg_database_datacl,
								RelationGetDescr(relation), &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 datId, grantorId, ACL_KIND_DATABASE,
									 NameStr(pg_database_tuple->datname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_database_datacl - 1] = true;
		values[Anum_pg_database_datacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

	    if (replication_enable && replication_master)
		{
			char *namespace;

			namespace = get_namespace_name(pg_class_tuple->relnamespace);
			if (namespace == NULL)
				elog(ERROR, "cache lookup failed for namespace %u",
					 pg_class_tuple->relnamespace);
			replicate_grant(namespace, NameStr(pg_class_tuple->relname), new_acl);
		}

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(DatabaseRelationId, HeapTupleGetOid(tuple), 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

/*
 * return an escaped version of a string, according to the rules
 * in pgr_acl.c::parse_aclitem(), i.e. escape \ and = with \.
 */
static void
escape_equal_sign(char *input, char *output)
{
	int	len;
	int	i, j;

	if (input == NULL || input[0] == '\0')
		elog(ERROR, "string NULL or too short");
	len = strlen(input);

	for (i = 0, j = 0; i < len; )
	{
		if (input[i] == '\\' || input[i] == '=')
			output[j++] = '\\';

		output[j++] = input[i++];
	}
	output[j] = '\0';
}

/*
 * replicate_grant_internal
 *
 * Workhorse for replicate_grant and replicate_grants_on_role_rename.
 *
 * acl_rel is repl_acl, suitably locked (at least a writer's lock).  schema and
 * relname are the Datum representation of the Name of the involved relation.
 * new_acl is the new ACL of the table, in the format that pg_class keeps it.
 */
static void
replicate_grant_internal(Relation acl_rel, Datum nspname, Datum relname,
						 Acl *new_acl)
{
	char		nulls[Natts_repl_acl];
	Datum		values[Natts_repl_acl];
	Datum	   *array_vals;
	HeapTuple	itup;
	HeapTuple	newtup;
	int			i;
	ScanKeyData	key[2];
	SysScanDesc	scan;

	/* XXX what do we do in this case? is it even possible? */
	if (new_acl == NULL)
		elog(ERROR, "NULL acl not implemented yet");

	ScanKeyInit(&key[0],
				Anum_repl_acl_schema,
				BTEqualStrategyNumber, F_NAMEEQ,
				nspname);
	ScanKeyInit(&key[1],
				Anum_repl_acl_relname,
				BTEqualStrategyNumber, F_NAMEEQ,
				relname);
	scan = systable_beginscan(acl_rel, ReplAclSchemaRelnameIndexId, true,
							  SnapshotNow, 2, key);
	itup = systable_getnext(scan);
	/* the test for validity of the tuple is postponed */

	array_vals = (Datum *) palloc(sizeof(Datum) * ACL_NUM(new_acl));

	/* Need to fiddle a bit to form the new ACL to insert into the tuple */
	for (i = 0; i < ACL_NUM(new_acl); i++)
	{
		char		tmpl[NAMEDATALEN * 2 + 64];
		AclItem	   *item = &(ACL_DAT(new_acl)[i]);
		char	   *grantee;
		HeapTuple	grantor;
		Form_pg_authid shadowForm;

		if (item->ai_grantee == ACL_ID_PUBLIC)
			grantee = pstrdup("special PUBLIC");
		else
		{
			HeapTuple	gtup;
			Form_pg_authid	sForm;
			char		escaped_name[NAMEDATALEN * 2];
			
			grantee = palloc(5 + NAMEDATALEN * 2);

			gtup = SearchSysCache(AUTHOID,
								  ObjectIdGetDatum(item->ai_grantee),
								  0, 0, 0);
			if (!HeapTupleIsValid(gtup))
				elog(ERROR, "cache lookup failure for role %d",
					 item->ai_grantee);

			sForm = (Form_pg_authid) GETSTRUCT(gtup);
			escape_equal_sign(NameStr(sForm->rolname), escaped_name);
			sprintf(grantee, "user %s", escaped_name);
			ReleaseSysCache(gtup);
		}

		grantor = SearchSysCache(AUTHOID,
								 ObjectIdGetDatum(item->ai_grantor),
								 0, 0, 0);
		Assert(HeapTupleIsValid(grantor));

		shadowForm = (Form_pg_authid) GETSTRUCT(grantor);

		/*
		 * Note we don't quote the grantee's username, because it has special
		 * rules anyway.
		 */
		sprintf(tmpl, "%s=%u/%s",
				grantee, item->ai_privs,
				quote_identifier(NameStr(shadowForm->rolname)));

		array_vals[i] = DirectFunctionCall1(textin,
											CStringGetDatum(tmpl));

		ReleaseSysCache(grantor);
	}

	/* Form the new tuple for repl_acl */
	values[0] = nspname;
	values[1] = relname;
	values[2] = PointerGetDatum(construct_array(array_vals,
												ACL_NUM(new_acl),
												TEXTOID, -1, false, 'i'));
	for (i = 0; i < 3; i++)
		nulls[i] = ' ';

	newtup = heap_formtuple(RelationGetDescr(acl_rel),
							values, nulls);

	/* If the tuple is found, update it.  Else insert a new one. */
	if (HeapTupleIsValid(itup))
		simple_heap_update(acl_rel, &itup->t_self, newtup);
	else
		simple_heap_insert(acl_rel, newtup);

	/* Keep indexes current */
	CatalogUpdateIndexes(acl_rel, newtup);
	heap_freetuple(newtup);

	systable_endscan(scan);
}

static void
replicate_grant(char *namespace, char *table, Acl *new_acl)
{
	Relation	acl_rel;
	Datum		nspname;
	Datum		relname;

	/*
	 * Lock pg_catalog.repl_acl with a writer's lock.  Concurrent access to a
	 * relation's tuple is protected by GRANT's lock on the table itself.
	 */
	acl_rel = heap_open(ReplAclRelationId, RowExclusiveLock);

	/*
	 * Form the Datum values for the namespace and relname, which we need
	 * for searching the replicate_acl table.
	 */
	nspname = DirectFunctionCall1(namein,
								  CStringGetDatum(namespace));
	relname = DirectFunctionCall1(namein,
								  CStringGetDatum(table));

	/* do the actual work */
	replicate_grant_internal(acl_rel, nspname, relname, new_acl);

	/* all done, clean up */
	heap_close(acl_rel, RowExclusiveLock);
}

void
replicate_grants_on_role_rename(Oid roleid)
{
	List	   *rels;
	ListCell   *cell;
	Relation	repl_acl;

	/* get a writer's lock on repl_acl */
	repl_acl = heap_open(ReplAclRelationId, RowExclusiveLock);

	rels = shdepGetTablesDependingOnRole(roleid);
	foreach (cell, rels)
	{
		Oid		relid = lfirst_oid(cell);
		HeapTuple reltup;
		HeapTuple nsptup;
		bool	isNull;
		Acl	   *acl;
		Datum	aclDatum;
		Datum	schemaname;
		Datum	relname;
		Form_pg_class classForm;
		Form_pg_namespace nspForm;

		/*
		 * All this fiddling below is just to get the Datum value of the
		 * relname and schema name
		 *
		 * XXX do we need more locking on the relation?
		 */
		reltup = SearchSysCache(RELOID,
								ObjectIdGetDatum(relid),
								0, 0, 0);
		if (!HeapTupleIsValid(reltup))
			elog(ERROR, "relation %u went away", relid);

		classForm = (Form_pg_class) GETSTRUCT(reltup);

		nsptup = SearchSysCache(NAMESPACEOID,
								ObjectIdGetDatum(classForm->relnamespace),
								0, 0, 0);
		if (!HeapTupleIsValid(nsptup))
			elog(ERROR, "namespace %u went away", classForm->relnamespace);
		nspForm = (Form_pg_namespace) GETSTRUCT(nsptup);

		relname = NameGetDatum(&(classForm->relname));
		schemaname = NameGetDatum(&(nspForm->nspname));

		/* now get the new ACL of the relation */
		aclDatum = SysCacheGetAttr(RELOID, reltup, Anum_pg_class_relacl,
								   &isNull);
		if (isNull)
			acl = NULL;
		else
		{
			/* detoast rel's ACL if necessary */
			acl = DatumGetAclP(aclDatum);
		}

		/* do the actual work */
		replicate_grant_internal(repl_acl, schemaname, relname, acl);

		ReleaseSysCache(reltup);
		ReleaseSysCache(nsptup);
	}

	heap_close(repl_acl, RowExclusiveLock);
	if (rels != NIL)
		pfree(rels);
}

static void
ExecGrant_Fdw(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FDW;

	relation = heap_open(ForeignDataWrapperRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			fdwid = lfirst_oid(cell);
		Form_pg_foreign_data_wrapper pg_fdw_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_foreign_data_wrapper];
		bool		nulls[Natts_pg_foreign_data_wrapper];
		bool		replaces[Natts_pg_foreign_data_wrapper];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;

		tuple = SearchSysCache(FOREIGNDATAWRAPPEROID,
							   ObjectIdGetDatum(fdwid),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for foreign-data wrapper %u", fdwid);

		pg_fdw_tuple = (Form_pg_foreign_data_wrapper) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_fdw_tuple->fdwowner;
		aclDatum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tuple,
								   Anum_pg_foreign_data_wrapper_fdwacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FDW, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 fdwid, grantorId, ACL_KIND_FDW,
									 NameStr(pg_fdw_tuple->fdwname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_foreign_data_wrapper_fdwacl - 1] = true;
		values[Anum_pg_foreign_data_wrapper_fdwacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(ForeignDataWrapperRelationId,
							  HeapTupleGetOid(tuple), 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_ForeignServer(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FOREIGN_SERVER;

	relation = heap_open(ForeignServerRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			srvid = lfirst_oid(cell);
		Form_pg_foreign_server pg_server_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_foreign_server];
		bool		nulls[Natts_pg_foreign_server];
		bool		replaces[Natts_pg_foreign_server];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;

		tuple = SearchSysCache(FOREIGNSERVEROID,
							   ObjectIdGetDatum(srvid),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for foreign server %u", srvid);

		pg_server_tuple = (Form_pg_foreign_server) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_server_tuple->srvowner;
		aclDatum = SysCacheGetAttr(FOREIGNSERVEROID, tuple,
								   Anum_pg_foreign_server_srvacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
								   srvid, grantorId, ACL_KIND_FOREIGN_SERVER,
									 NameStr(pg_server_tuple->srvname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_foreign_server_srvacl - 1] = true;
		values[Anum_pg_foreign_server_srvacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(ForeignServerRelationId,
							  HeapTupleGetOid(tuple), 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Function(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_FUNCTION;

	relation = heap_open(ProcedureRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			funcId = lfirst_oid(cell);
		Form_pg_proc pg_proc_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_proc];
		bool		nulls[Natts_pg_proc];
		bool		replaces[Natts_pg_proc];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;

		tuple = SearchSysCache(PROCOID,
							   ObjectIdGetDatum(funcId),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for function %u", funcId);

		pg_proc_tuple = (Form_pg_proc) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_proc_tuple->proowner;
		aclDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 funcId, grantorId, ACL_KIND_PROC,
									 NameStr(pg_proc_tuple->proname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_proc_proacl - 1] = true;
		values[Anum_pg_proc_proacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(ProcedureRelationId, funcId, 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Language(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_LANGUAGE;

	relation = heap_open(LanguageRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			langId = lfirst_oid(cell);
		Form_pg_language pg_language_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_language];
		bool		nulls[Natts_pg_language];
		bool		replaces[Natts_pg_language];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;

		tuple = SearchSysCache(LANGOID,
							   ObjectIdGetDatum(langId),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for language %u", langId);

		pg_language_tuple = (Form_pg_language) GETSTRUCT(tuple);

		if (!pg_language_tuple->lanpltrusted)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("language \"%s\" is not trusted",
							NameStr(pg_language_tuple->lanname)),
				   errhint("Only superusers can use untrusted languages.")));

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_language_tuple->lanowner;
		aclDatum = SysCacheGetAttr(LANGNAME, tuple, Anum_pg_language_lanacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 langId, grantorId, ACL_KIND_LANGUAGE,
									 NameStr(pg_language_tuple->lanname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_language_lanacl - 1] = true;
		values[Anum_pg_language_lanacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(LanguageRelationId, HeapTupleGetOid(tuple), 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Namespace(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_NAMESPACE;

	relation = heap_open(NamespaceRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			nspid = lfirst_oid(cell);
		Form_pg_namespace pg_namespace_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	tuple;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_namespace];
		bool		nulls[Natts_pg_namespace];
		bool		replaces[Natts_pg_namespace];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;

		tuple = SearchSysCache(NAMESPACEOID,
							   ObjectIdGetDatum(nspid),
							   0, 0, 0);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for namespace %u", nspid);

		pg_namespace_tuple = (Form_pg_namespace) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_namespace_tuple->nspowner;
		aclDatum = SysCacheGetAttr(NAMESPACENAME, tuple,
								   Anum_pg_namespace_nspacl,
								   &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 nspid, grantorId, ACL_KIND_NAMESPACE,
									 NameStr(pg_namespace_tuple->nspname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_namespace_nspacl - 1] = true;
		values[Anum_pg_namespace_nspacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(NamespaceRelationId, HeapTupleGetOid(tuple), 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		ReleaseSysCache(tuple);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}

static void
ExecGrant_Tablespace(InternalGrant *istmt)
{
	Relation	relation;
	ListCell   *cell;

	if (istmt->all_privs && istmt->privileges == ACL_NO_RIGHTS)
		istmt->privileges = ACL_ALL_RIGHTS_TABLESPACE;

	relation = heap_open(TableSpaceRelationId, RowExclusiveLock);

	foreach(cell, istmt->objects)
	{
		Oid			tblId = lfirst_oid(cell);
		Form_pg_tablespace pg_tablespace_tuple;
		Datum		aclDatum;
		bool		isNull;
		AclMode		avail_goptions;
		AclMode		this_privileges;
		Acl		   *old_acl;
		Acl		   *new_acl;
		Oid			grantorId;
		Oid			ownerId;
		HeapTuple	newtuple;
		Datum		values[Natts_pg_tablespace];
		bool		nulls[Natts_pg_tablespace];
		bool		replaces[Natts_pg_tablespace];
		int			noldmembers;
		int			nnewmembers;
		Oid		   *oldmembers;
		Oid		   *newmembers;
		ScanKeyData entry[1];
		SysScanDesc scan;
		HeapTuple	tuple;

		/* There's no syscache for pg_tablespace, so must look the hard way */
		ScanKeyInit(&entry[0],
					ObjectIdAttributeNumber,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(tblId));
		scan = systable_beginscan(relation, TablespaceOidIndexId, true,
								  SnapshotNow, 1, entry);
		tuple = systable_getnext(scan);
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for tablespace %u", tblId);

		pg_tablespace_tuple = (Form_pg_tablespace) GETSTRUCT(tuple);

		/*
		 * Get owner ID and working copy of existing ACL. If there's no ACL,
		 * substitute the proper default.
		 */
		ownerId = pg_tablespace_tuple->spcowner;
		aclDatum = heap_getattr(tuple, Anum_pg_tablespace_spcacl,
								RelationGetDescr(relation), &isNull);
		if (isNull)
			old_acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
		else
			old_acl = DatumGetAclPCopy(aclDatum);

		/* Determine ID to do the grant as, and available grant options */
		select_best_grantor(GetUserId(), istmt->privileges,
							old_acl, ownerId,
							&grantorId, &avail_goptions);

		/*
		 * Restrict the privileges to what we can actually grant, and emit the
		 * standards-mandated warning and error messages.
		 */
		this_privileges =
			restrict_and_check_grant(istmt->is_grant, avail_goptions,
									 istmt->all_privs, istmt->privileges,
									 tblId, grantorId, ACL_KIND_TABLESPACE,
									 NameStr(pg_tablespace_tuple->spcname),
									 0, NULL);

		/*
		 * Generate new ACL.
		 *
		 * We need the members of both old and new ACLs so we can correct the
		 * shared dependency information.
		 */
		noldmembers = aclmembers(old_acl, &oldmembers);

		new_acl = merge_acl_with_grant(old_acl, istmt->is_grant,
									   istmt->grant_option, istmt->behavior,
									   istmt->grantees, this_privileges,
									   grantorId, ownerId);

		nnewmembers = aclmembers(new_acl, &newmembers);

		/* finished building new ACL value, now insert it */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pg_tablespace_spcacl - 1] = true;
		values[Anum_pg_tablespace_spcacl - 1] = PointerGetDatum(new_acl);

		newtuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values,
									 nulls, replaces);

		simple_heap_update(relation, &newtuple->t_self, newtuple);

		/* keep the catalog indexes up to date */
		CatalogUpdateIndexes(relation, newtuple);

		/* Update the shared dependency ACL info */
		updateAclDependencies(TableSpaceRelationId, tblId, 0,
							  ownerId, istmt->is_grant,
							  noldmembers, oldmembers,
							  nnewmembers, newmembers);

		systable_endscan(scan);

		pfree(new_acl);

		/* prevent error when processing duplicate objects */
		CommandCounterIncrement();
	}

	heap_close(relation, RowExclusiveLock);
}


static AclMode
string_to_privilege(const char *privname)
{
	if (strcmp(privname, "insert") == 0)
		return ACL_INSERT;
	if (strcmp(privname, "select") == 0)
		return ACL_SELECT;
	if (strcmp(privname, "update") == 0)
		return ACL_UPDATE;
	if (strcmp(privname, "delete") == 0)
		return ACL_DELETE;
	if (strcmp(privname, "truncate") == 0)
		return ACL_TRUNCATE;
	if (strcmp(privname, "references") == 0)
		return ACL_REFERENCES;
	if (strcmp(privname, "trigger") == 0)
		return ACL_TRIGGER;
	if (strcmp(privname, "execute") == 0)
		return ACL_EXECUTE;
	if (strcmp(privname, "usage") == 0)
		return ACL_USAGE;
	if (strcmp(privname, "create") == 0)
		return ACL_CREATE;
	if (strcmp(privname, "temporary") == 0)
		return ACL_CREATE_TEMP;
	if (strcmp(privname, "temp") == 0)
		return ACL_CREATE_TEMP;
	if (strcmp(privname, "connect") == 0)
		return ACL_CONNECT;
	if (strcmp(privname, "rule") == 0)
		return 0;				/* ignore old RULE privileges */
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("unrecognized privilege type \"%s\"", privname)));
	return 0;					/* appease compiler */
}

static const char *
privilege_to_string(AclMode privilege)
{
	switch (privilege)
	{
		case ACL_INSERT:
			return "INSERT";
		case ACL_SELECT:
			return "SELECT";
		case ACL_UPDATE:
			return "UPDATE";
		case ACL_DELETE:
			return "DELETE";
		case ACL_TRUNCATE:
			return "TRUNCATE";
		case ACL_REFERENCES:
			return "REFERENCES";
		case ACL_TRIGGER:
			return "TRIGGER";
		case ACL_EXECUTE:
			return "EXECUTE";
		case ACL_USAGE:
			return "USAGE";
		case ACL_CREATE:
			return "CREATE";
		case ACL_CREATE_TEMP:
			return "TEMP";
		case ACL_CONNECT:
			return "CONNECT";
		default:
			elog(ERROR, "unrecognized privilege: %d", (int) privilege);
	}
	return NULL;				/* appease compiler */
}

/*
 * Standardized reporting of aclcheck permissions failures.
 *
 * Note: we do not double-quote the %s's below, because many callers
 * supply strings that might be already quoted.
 */

static const char *const no_priv_msg[MAX_ACL_KIND] =
{
	/* ACL_KIND_COLUMN */
	gettext_noop("permission denied for column %s"),
	/* ACL_KIND_CLASS */
	gettext_noop("permission denied for relation %s"),
	/* ACL_KIND_SEQUENCE */
	gettext_noop("permission denied for sequence %s"),
	/* ACL_KIND_DATABASE */
	gettext_noop("permission denied for database %s"),
	/* ACL_KIND_PROC */
	gettext_noop("permission denied for function %s"),
	/* ACL_KIND_OPER */
	gettext_noop("permission denied for operator %s"),
	/* ACL_KIND_TYPE */
	gettext_noop("permission denied for type %s"),
	/* ACL_KIND_LANGUAGE */
	gettext_noop("permission denied for language %s"),
	/* ACL_KIND_NAMESPACE */
	gettext_noop("permission denied for schema %s"),
	/* ACL_KIND_OPCLASS */
	gettext_noop("permission denied for operator class %s"),
	/* ACL_KIND_OPFAMILY */
	gettext_noop("permission denied for operator family %s"),
	/* ACL_KIND_CONVERSION */
	gettext_noop("permission denied for conversion %s"),
	/* ACL_KIND_TABLESPACE */
	gettext_noop("permission denied for tablespace %s"),
	/* ACL_KIND_TSDICTIONARY */
	gettext_noop("permission denied for text search dictionary %s"),
	/* ACL_KIND_TSCONFIGURATION */
	gettext_noop("permission denied for text search configuration %s"),
	/* ACL_KIND_FDW */
	gettext_noop("permission denied for foreign-data wrapper %s"),
	/* ACL_KIND_FOREIGN_SERVER */
	gettext_noop("permission denied for foreign server %s")
};

static const char *const not_owner_msg[MAX_ACL_KIND] =
{
	/* ACL_KIND_COLUMN */
	gettext_noop("must be owner of relation %s"),
	/* ACL_KIND_CLASS */
	gettext_noop("must be owner of relation %s"),
	/* ACL_KIND_SEQUENCE */
	gettext_noop("must be owner of sequence %s"),
	/* ACL_KIND_DATABASE */
	gettext_noop("must be owner of database %s"),
	/* ACL_KIND_PROC */
	gettext_noop("must be owner of function %s"),
	/* ACL_KIND_OPER */
	gettext_noop("must be owner of operator %s"),
	/* ACL_KIND_TYPE */
	gettext_noop("must be owner of type %s"),
	/* ACL_KIND_LANGUAGE */
	gettext_noop("must be owner of language %s"),
	/* ACL_KIND_NAMESPACE */
	gettext_noop("must be owner of schema %s"),
	/* ACL_KIND_OPCLASS */
	gettext_noop("must be owner of operator class %s"),
	/* ACL_KIND_OPFAMILY */
	gettext_noop("must be owner of operator family %s"),
	/* ACL_KIND_CONVERSION */
	gettext_noop("must be owner of conversion %s"),
	/* ACL_KIND_TABLESPACE */
	gettext_noop("must be owner of tablespace %s"),
	/* ACL_KIND_TSDICTIONARY */
	gettext_noop("must be owner of text search dictionary %s"),
	/* ACL_KIND_TSCONFIGURATION */
	gettext_noop("must be owner of text search configuration %s"),
	/* ACL_KIND_FDW */
	gettext_noop("must be owner of foreign-data wrapper %s"),
	/* ACL_KIND_FOREIGN_SERVER */
	gettext_noop("must be owner of foreign server %s")
};


void
aclcheck_error(AclResult aclerr, AclObjectKind objectkind,
			   const char *objectname)
{
	switch (aclerr)
	{
		case ACLCHECK_OK:
			/* no error, so return to caller */
			break;
		case ACLCHECK_NO_PRIV:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg(no_priv_msg[objectkind], objectname)));
			break;
		case ACLCHECK_NOT_OWNER:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg(not_owner_msg[objectkind], objectname)));
			break;
		default:
			elog(ERROR, "unrecognized AclResult: %d", (int) aclerr);
			break;
	}
}


void
aclcheck_error_col(AclResult aclerr, AclObjectKind objectkind,
				   const char *objectname, const char *colname)
{
	switch (aclerr)
	{
		case ACLCHECK_OK:
			/* no error, so return to caller */
			break;
		case ACLCHECK_NO_PRIV:
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for column %s of relation %s",
							colname, objectname)));
			break;
		case ACLCHECK_NOT_OWNER:
			/* relation msg is OK since columns don't have separate owners */
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg(not_owner_msg[objectkind], objectname)));
			break;
		default:
			elog(ERROR, "unrecognized AclResult: %d", (int) aclerr);
			break;
	}
}


/* Check if given user has rolcatupdate privilege according to pg_authid */
static bool
has_rolcatupdate(Oid roleid)
{
	bool		rolcatupdate;
	HeapTuple	tuple;

	tuple = SearchSysCache(AUTHOID,
						   ObjectIdGetDatum(roleid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("role with OID %u does not exist", roleid)));

	rolcatupdate = ((Form_pg_authid) GETSTRUCT(tuple))->rolcatupdate;

	ReleaseSysCache(tuple);

	return rolcatupdate;
}

/*
 * Relay for the various pg_*_mask routines depending on object kind
 */
static AclMode
pg_aclmask(AclObjectKind objkind, Oid table_oid, AttrNumber attnum, Oid roleid,
		   AclMode mask, AclMaskHow how)
{
	switch (objkind)
	{
		case ACL_KIND_COLUMN:
			return
				pg_class_aclmask(table_oid, roleid, mask, how) |
				pg_attribute_aclmask(table_oid, attnum, roleid, mask, how);
		case ACL_KIND_CLASS:
		case ACL_KIND_SEQUENCE:
			return pg_class_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_DATABASE:
			return pg_database_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_PROC:
			return pg_proc_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_LANGUAGE:
			return pg_language_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_NAMESPACE:
			return pg_namespace_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_TABLESPACE:
			return pg_tablespace_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_FDW:
			return pg_foreign_data_wrapper_aclmask(table_oid, roleid, mask, how);
		case ACL_KIND_FOREIGN_SERVER:
			return pg_foreign_server_aclmask(table_oid, roleid, mask, how);
		default:
			elog(ERROR, "unrecognized objkind: %d",
				 (int) objkind);
			/* not reached, but keep compiler quiet */
			return ACL_NO_RIGHTS;
	}
}


/* ****************************************************************
 * Exported routines for examining a user's privileges for various objects
 *
 * See aclmask() for a description of the common API for these functions.
 *
 * Note: we give lookup failure the full ereport treatment because the
 * has_xxx_privilege() family of functions allow users to pass any random
 * OID to these functions.
 * ****************************************************************
 */

/*
 * Exported routine for examining a user's privileges for a column
 *
 * Note: this considers only privileges granted specifically on the column.
 * It is caller's responsibility to take relation-level privileges into account
 * as appropriate.	(For the same reason, we have no special case for
 * superuser-ness here.)
 */
AclMode
pg_attribute_aclmask(Oid table_oid, AttrNumber attnum, Oid roleid,
					 AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	classTuple;
	HeapTuple	attTuple;
	Form_pg_class classForm;
	Form_pg_attribute attributeForm;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/*
	 * First, get the column's ACL from its pg_attribute entry
	 */
	attTuple = SearchSysCache(ATTNUM,
							  ObjectIdGetDatum(table_oid),
							  Int16GetDatum(attnum),
							  0, 0);
	if (!HeapTupleIsValid(attTuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("attribute %d of relation with OID %u does not exist",
						attnum, table_oid)));
	attributeForm = (Form_pg_attribute) GETSTRUCT(attTuple);

	/* Throw error on dropped columns, too */
	if (attributeForm->attisdropped)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("attribute %d of relation with OID %u does not exist",
						attnum, table_oid)));

	aclDatum = SysCacheGetAttr(ATTNUM, attTuple, Anum_pg_attribute_attacl,
							   &isNull);

	/*
	 * Here we hard-wire knowledge that the default ACL for a column grants no
	 * privileges, so that we can fall out quickly in the very common case
	 * where attacl is null.
	 */
	if (isNull)
	{
		ReleaseSysCache(attTuple);
		return 0;
	}

	/*
	 * Must get the relation's ownerId from pg_class.  Since we already found
	 * a pg_attribute entry, the only likely reason for this to fail is that a
	 * concurrent DROP of the relation committed since then (which could only
	 * happen if we don't have lock on the relation).  We prefer to report "no
	 * privileges" rather than failing in such a case, so as to avoid unwanted
	 * failures in has_column_privilege() tests.
	 */
	classTuple = SearchSysCache(RELOID,
								ObjectIdGetDatum(table_oid),
								0, 0, 0);
	if (!HeapTupleIsValid(classTuple))
	{
		ReleaseSysCache(attTuple);
		return 0;
	}
	classForm = (Form_pg_class) GETSTRUCT(classTuple);

	ownerId = classForm->relowner;

	ReleaseSysCache(classTuple);

	/* detoast column's ACL if necessary */
	acl = DatumGetAclP(aclDatum);

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(attTuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a table
 */
AclMode
pg_class_aclmask(Oid table_oid, Oid roleid,
				 AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Form_pg_class classForm;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/*
	 * Must get the relation's tuple from pg_class
	 */
	tuple = SearchSysCache(RELOID,
						   ObjectIdGetDatum(table_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist",
						table_oid)));
	classForm = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * Deny anyone permission to update a system catalog unless
	 * pg_authid.rolcatupdate is set.	(This is to let superusers protect
	 * themselves from themselves.)  Also allow it if allowSystemTableMods.
	 *
	 * As of 7.4 we have some updatable system views; those shouldn't be
	 * protected in this way.  Assume the view rules can take care of
	 * themselves.	ACL_USAGE is if we ever have system sequences.
	 */
	if ((mask & (ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE)) &&
		IsSystemClass(classForm) &&
		classForm->relkind != RELKIND_VIEW &&
		!has_rolcatupdate(roleid) &&
		!allowSystemTableMods)
	{
#ifdef ACLDEBUG
		elog(DEBUG2, "permission denied for system catalog update");
#endif
		mask &= ~(ACL_INSERT | ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_USAGE);
	}

	/*
	 * Otherwise, superusers bypass all permission-checking.
	 */
	if (superuser_arg(roleid))
	{
#ifdef ACLDEBUG
		elog(DEBUG2, "OID %u is superuser, home free", roleid);
#endif
		ReleaseSysCache(tuple);
		return mask;
	}

	/*
	 * Normal case: get the relation's ACL from pg_class
	 */
	ownerId = classForm->relowner;

	aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(classForm->relkind == RELKIND_SEQUENCE ?
						 ACL_OBJECT_SEQUENCE : ACL_OBJECT_RELATION,
						 ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a database
 */
AclMode
pg_database_aclmask(Oid db_oid, Oid roleid,
					AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Get the database's ACL from pg_database
	 */
	tuple = SearchSysCache(DATABASEOID,
						   ObjectIdGetDatum(db_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database with OID %u does not exist", db_oid)));

	ownerId = ((Form_pg_database) GETSTRUCT(tuple))->datdba;

	aclDatum = SysCacheGetAttr(DATABASEOID, tuple, Anum_pg_database_datacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_DATABASE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a function
 */
AclMode
pg_proc_aclmask(Oid proc_oid, Oid roleid,
				AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Get the function's ACL from pg_proc
	 */
	tuple = SearchSysCache(PROCOID,
						   ObjectIdGetDatum(proc_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function with OID %u does not exist", proc_oid)));

	ownerId = ((Form_pg_proc) GETSTRUCT(tuple))->proowner;

	aclDatum = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FUNCTION, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a language
 */
AclMode
pg_language_aclmask(Oid lang_oid, Oid roleid,
					AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Get the language's ACL from pg_language
	 */
	tuple = SearchSysCache(LANGOID,
						   ObjectIdGetDatum(lang_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("language with OID %u does not exist", lang_oid)));

	ownerId = ((Form_pg_language) GETSTRUCT(tuple))->lanowner;

	aclDatum = SysCacheGetAttr(LANGOID, tuple, Anum_pg_language_lanacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_LANGUAGE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a namespace
 */
AclMode
pg_namespace_aclmask(Oid nsp_oid, Oid roleid,
					 AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * If we have been assigned this namespace as a temp namespace, check to
	 * make sure we have CREATE TEMP permission on the database, and if so act
	 * as though we have all standard (but not GRANT OPTION) permissions on
	 * the namespace.  If we don't have CREATE TEMP, act as though we have
	 * only USAGE (and not CREATE) rights.
	 *
	 * This may seem redundant given the check in InitTempTableNamespace, but
	 * it really isn't since current user ID may have changed since then. The
	 * upshot of this behavior is that a SECURITY DEFINER function can create
	 * temp tables that can then be accessed (if permission is granted) by
	 * code in the same session that doesn't have permissions to create temp
	 * tables.
	 *
	 * XXX Would it be safe to ereport a special error message as
	 * InitTempTableNamespace does?  Returning zero here means we'll get a
	 * generic "permission denied for schema pg_temp_N" message, which is not
	 * remarkably user-friendly.
	 */
	if (isTempNamespace(nsp_oid))
	{
		if (pg_database_aclcheck(MyDatabaseId, roleid,
								 ACL_CREATE_TEMP) == ACLCHECK_OK)
			return mask & ACL_ALL_RIGHTS_NAMESPACE;
		else
			return mask & ACL_USAGE;
	}

	/*
	 * Get the schema's ACL from pg_namespace
	 */
	tuple = SearchSysCache(NAMESPACEOID,
						   ObjectIdGetDatum(nsp_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

	aclDatum = SysCacheGetAttr(NAMESPACEOID, tuple, Anum_pg_namespace_nspacl,
							   &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_NAMESPACE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a tablespace
 */
AclMode
pg_tablespace_aclmask(Oid spc_oid, Oid roleid,
					  AclMode mask, AclMaskHow how)
{
	AclMode		result;
	Relation	pg_tablespace;
	ScanKeyData entry[1];
	SysScanDesc scan;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Get the tablespace's ACL from pg_tablespace
	 *
	 * There's no syscache for pg_tablespace, so must look the hard way
	 */
	pg_tablespace = heap_open(TableSpaceRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(spc_oid));
	scan = systable_beginscan(pg_tablespace, TablespaceOidIndexId, true,
							  SnapshotNow, 1, entry);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist", spc_oid)));

	ownerId = ((Form_pg_tablespace) GETSTRUCT(tuple))->spcowner;

	aclDatum = heap_getattr(tuple, Anum_pg_tablespace_spcacl,
							RelationGetDescr(pg_tablespace), &isNull);

	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_TABLESPACE, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	systable_endscan(scan);
	heap_close(pg_tablespace, AccessShareLock);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * data wrapper
 */
AclMode
pg_foreign_data_wrapper_aclmask(Oid fdw_oid, Oid roleid,
								AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	Form_pg_foreign_data_wrapper fdwForm;

	/* Bypass permission checks for superusers */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Must get the FDW's tuple from pg_foreign_data_wrapper
	 */
	tuple = SearchSysCache(FOREIGNDATAWRAPPEROID,
						   ObjectIdGetDatum(fdw_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errmsg("foreign-data wrapper with OID %u does not exist",
						fdw_oid)));
	fdwForm = (Form_pg_foreign_data_wrapper) GETSTRUCT(tuple);

	/*
	 * Normal case: get the FDW's ACL from pg_foreign_data_wrapper
	 */
	ownerId = fdwForm->fdwowner;

	aclDatum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tuple,
							   Anum_pg_foreign_data_wrapper_fdwacl, &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FDW, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for examining a user's privileges for a foreign
 * server.
 */
AclMode
pg_foreign_server_aclmask(Oid srv_oid, Oid roleid,
						  AclMode mask, AclMaskHow how)
{
	AclMode		result;
	HeapTuple	tuple;
	Datum		aclDatum;
	bool		isNull;
	Acl		   *acl;
	Oid			ownerId;

	Form_pg_foreign_server srvForm;

	/* Bypass permission checks for superusers */
	if (superuser_arg(roleid))
		return mask;

	/*
	 * Must get the FDW's tuple from pg_foreign_data_wrapper
	 */
	tuple = SearchSysCache(FOREIGNSERVEROID,
						   ObjectIdGetDatum(srv_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errmsg("foreign server with OID %u does not exist",
						srv_oid)));
	srvForm = (Form_pg_foreign_server) GETSTRUCT(tuple);

	/*
	 * Normal case: get the foreign server's ACL from pg_foreign_server
	 */
	ownerId = srvForm->srvowner;

	aclDatum = SysCacheGetAttr(FOREIGNSERVEROID, tuple,
							   Anum_pg_foreign_server_srvacl, &isNull);
	if (isNull)
	{
		/* No ACL, so build default ACL */
		acl = acldefault(ACL_OBJECT_FOREIGN_SERVER, ownerId);
		aclDatum = (Datum) 0;
	}
	else
	{
		/* detoast rel's ACL if necessary */
		acl = DatumGetAclP(aclDatum);
	}

	result = aclmask(acl, roleid, ownerId, mask, how);

	/* if we have a detoasted copy, free it */
	if (acl && (Pointer) acl != DatumGetPointer(aclDatum))
		pfree(acl);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * Exported routine for checking a user's access privileges to a column
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 *
 * As with pg_attribute_aclmask, only privileges granted directly on the
 * column are considered here.
 */
AclResult
pg_attribute_aclcheck(Oid table_oid, AttrNumber attnum,
					  Oid roleid, AclMode mode)
{
	if (pg_attribute_aclmask(table_oid, attnum, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to any/all columns
 *
 * If 'how' is ACLMASK_ANY, then returns ACLCHECK_OK if user has any of the
 * privileges identified by 'mode' on any non-dropped column in the relation;
 * otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 *
 * If 'how' is ACLMASK_ALL, then returns ACLCHECK_OK if user has any of the
 * privileges identified by 'mode' on each non-dropped column in the relation
 * (and there must be at least one such column); otherwise returns a suitable
 * error code (in practice, always ACLCHECK_NO_PRIV).
 *
 * As with pg_attribute_aclmask, only privileges granted directly on the
 * column(s) are considered here.
 *
 * Note: system columns are not considered here; there are cases where that
 * might be appropriate but there are also cases where it wouldn't.
 */
AclResult
pg_attribute_aclcheck_all(Oid table_oid, Oid roleid, AclMode mode,
						  AclMaskHow how)
{
	AclResult	result;
	HeapTuple	classTuple;
	Form_pg_class classForm;
	AttrNumber	nattrs;
	AttrNumber	curr_att;

	/*
	 * Must fetch pg_class row to check number of attributes.  As in
	 * pg_attribute_aclmask, we prefer to return "no privileges" instead of
	 * throwing an error if we get any unexpected lookup errors.
	 */
	classTuple = SearchSysCache(RELOID,
								ObjectIdGetDatum(table_oid),
								0, 0, 0);
	if (!HeapTupleIsValid(classTuple))
		return ACLCHECK_NO_PRIV;
	classForm = (Form_pg_class) GETSTRUCT(classTuple);

	nattrs = classForm->relnatts;

	ReleaseSysCache(classTuple);

	/*
	 * Initialize result in case there are no non-dropped columns.	We want to
	 * report failure in such cases for either value of 'how'.
	 */
	result = ACLCHECK_NO_PRIV;

	for (curr_att = 1; curr_att <= nattrs; curr_att++)
	{
		HeapTuple	attTuple;
		AclMode		attmask;

		attTuple = SearchSysCache(ATTNUM,
								  ObjectIdGetDatum(table_oid),
								  Int16GetDatum(curr_att),
								  0, 0);
		if (!HeapTupleIsValid(attTuple))
			continue;

		/* ignore dropped columns */
		if (((Form_pg_attribute) GETSTRUCT(attTuple))->attisdropped)
		{
			ReleaseSysCache(attTuple);
			continue;
		}

		/*
		 * Here we hard-wire knowledge that the default ACL for a column
		 * grants no privileges, so that we can fall out quickly in the very
		 * common case where attacl is null.
		 */
		if (heap_attisnull(attTuple, Anum_pg_attribute_attacl))
			attmask = 0;
		else
			attmask = pg_attribute_aclmask(table_oid, curr_att, roleid,
										   mode, ACLMASK_ANY);

		ReleaseSysCache(attTuple);

		if (attmask != 0)
		{
			result = ACLCHECK_OK;
			if (how == ACLMASK_ANY)
				break;			/* succeed on any success */
		}
		else
		{
			result = ACLCHECK_NO_PRIV;
			if (how == ACLMASK_ALL)
				break;			/* fail on any failure */
		}
	}

	return result;
}

/*
 * Exported routine for checking a user's access privileges to a table
 *
 * Returns ACLCHECK_OK if the user has any of the privileges identified by
 * 'mode'; otherwise returns a suitable error code (in practice, always
 * ACLCHECK_NO_PRIV).
 */
AclResult
pg_class_aclcheck(Oid table_oid, Oid roleid, AclMode mode)
{
	if (pg_class_aclmask(table_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a database
 */
AclResult
pg_database_aclcheck(Oid db_oid, Oid roleid, AclMode mode)
{
	if (pg_database_aclmask(db_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a function
 */
AclResult
pg_proc_aclcheck(Oid proc_oid, Oid roleid, AclMode mode)
{
	if (pg_proc_aclmask(proc_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a language
 */
AclResult
pg_language_aclcheck(Oid lang_oid, Oid roleid, AclMode mode)
{
	if (pg_language_aclmask(lang_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a namespace
 */
AclResult
pg_namespace_aclcheck(Oid nsp_oid, Oid roleid, AclMode mode)
{
	if (pg_namespace_aclmask(nsp_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a tablespace
 */
AclResult
pg_tablespace_aclcheck(Oid spc_oid, Oid roleid, AclMode mode)
{
	if (pg_tablespace_aclmask(spc_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * data wrapper
 */
AclResult
pg_foreign_data_wrapper_aclcheck(Oid fdw_oid, Oid roleid, AclMode mode)
{
	if (pg_foreign_data_wrapper_aclmask(fdw_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Exported routine for checking a user's access privileges to a foreign
 * server
 */
AclResult
pg_foreign_server_aclcheck(Oid srv_oid, Oid roleid, AclMode mode)
{
	if (pg_foreign_server_aclmask(srv_oid, roleid, mode, ACLMASK_ANY) != 0)
		return ACLCHECK_OK;
	else
		return ACLCHECK_NO_PRIV;
}

/*
 * Ownership check for a relation (specified by OID).
 */
bool
pg_class_ownercheck(Oid class_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(RELOID,
						   ObjectIdGetDatum(class_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation with OID %u does not exist", class_oid)));

	ownerId = ((Form_pg_class) GETSTRUCT(tuple))->relowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a type (specified by OID).
 */
bool
pg_type_ownercheck(Oid type_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(TYPEOID,
						   ObjectIdGetDatum(type_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type with OID %u does not exist", type_oid)));

	ownerId = ((Form_pg_type) GETSTRUCT(tuple))->typowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator (specified by OID).
 */
bool
pg_oper_ownercheck(Oid oper_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(OPEROID,
						   ObjectIdGetDatum(oper_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("operator with OID %u does not exist", oper_oid)));

	ownerId = ((Form_pg_operator) GETSTRUCT(tuple))->oprowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a function (specified by OID).
 */
bool
pg_proc_ownercheck(Oid proc_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(PROCOID,
						   ObjectIdGetDatum(proc_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function with OID %u does not exist", proc_oid)));

	ownerId = ((Form_pg_proc) GETSTRUCT(tuple))->proowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a procedural language (specified by OID)
 */
bool
pg_language_ownercheck(Oid lan_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(LANGOID,
						   ObjectIdGetDatum(lan_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("language with OID %u does not exist", lan_oid)));

	ownerId = ((Form_pg_language) GETSTRUCT(tuple))->lanowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a namespace (specified by OID).
 */
bool
pg_namespace_ownercheck(Oid nsp_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(NAMESPACEOID,
						   ObjectIdGetDatum(nsp_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema with OID %u does not exist", nsp_oid)));

	ownerId = ((Form_pg_namespace) GETSTRUCT(tuple))->nspowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a tablespace (specified by OID).
 */
bool
pg_tablespace_ownercheck(Oid spc_oid, Oid roleid)
{
	Relation	pg_tablespace;
	ScanKeyData entry[1];
	SysScanDesc scan;
	HeapTuple	spctuple;
	Oid			spcowner;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	/* There's no syscache for pg_tablespace, so must look the hard way */
	pg_tablespace = heap_open(TableSpaceRelationId, AccessShareLock);
	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(spc_oid));
	scan = systable_beginscan(pg_tablespace, TablespaceOidIndexId, true,
							  SnapshotNow, 1, entry);

	spctuple = systable_getnext(scan);

	if (!HeapTupleIsValid(spctuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace with OID %u does not exist", spc_oid)));

	spcowner = ((Form_pg_tablespace) GETSTRUCT(spctuple))->spcowner;

	systable_endscan(scan);
	heap_close(pg_tablespace, AccessShareLock);

	return has_privs_of_role(roleid, spcowner);
}

/*
 * Ownership check for an operator class (specified by OID).
 */
bool
pg_opclass_ownercheck(Oid opc_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(CLAOID,
						   ObjectIdGetDatum(opc_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator class with OID %u does not exist",
						opc_oid)));

	ownerId = ((Form_pg_opclass) GETSTRUCT(tuple))->opcowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for an operator family (specified by OID).
 */
bool
pg_opfamily_ownercheck(Oid opf_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(OPFAMILYOID,
						   ObjectIdGetDatum(opf_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("operator family with OID %u does not exist",
						opf_oid)));

	ownerId = ((Form_pg_opfamily) GETSTRUCT(tuple))->opfowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a text search dictionary (specified by OID).
 */
bool
pg_ts_dict_ownercheck(Oid dict_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(TSDICTOID,
						   ObjectIdGetDatum(dict_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("text search dictionary with OID %u does not exist",
						dict_oid)));

	ownerId = ((Form_pg_ts_dict) GETSTRUCT(tuple))->dictowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a text search configuration (specified by OID).
 */
bool
pg_ts_config_ownercheck(Oid cfg_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(TSCONFIGOID,
						   ObjectIdGetDatum(cfg_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
			   errmsg("text search configuration with OID %u does not exist",
					  cfg_oid)));

	ownerId = ((Form_pg_ts_config) GETSTRUCT(tuple))->cfgowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a foreign server (specified by OID).
 */
bool
pg_foreign_server_ownercheck(Oid srv_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(FOREIGNSERVEROID,
						   ObjectIdGetDatum(srv_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("foreign server with OID %u does not exist",
						srv_oid)));

	ownerId = ((Form_pg_foreign_server) GETSTRUCT(tuple))->srvowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}

/*
 * Ownership check for a database (specified by OID).
 */
bool
pg_database_ownercheck(Oid db_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			dba;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(DATABASEOID,
						   ObjectIdGetDatum(db_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_DATABASE),
				 errmsg("database with OID %u does not exist", db_oid)));

	dba = ((Form_pg_database) GETSTRUCT(tuple))->datdba;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, dba);
}

/*
 * Ownership check for a conversion (specified by OID).
 */
bool
pg_conversion_ownercheck(Oid conv_oid, Oid roleid)
{
	HeapTuple	tuple;
	Oid			ownerId;

	/* Superusers bypass all permission checking. */
	if (superuser_arg(roleid))
		return true;

	tuple = SearchSysCache(CONVOID,
						   ObjectIdGetDatum(conv_oid),
						   0, 0, 0);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("conversion with OID %u does not exist", conv_oid)));

	ownerId = ((Form_pg_conversion) GETSTRUCT(tuple))->conowner;

	ReleaseSysCache(tuple);

	return has_privs_of_role(roleid, ownerId);
}
