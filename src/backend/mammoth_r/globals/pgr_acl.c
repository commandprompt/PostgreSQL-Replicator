#include "postgres.h"

#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "postmaster/replication.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"


static Acl *convert_master_acl(ArrayType *master_acl);
static bool make_AclItem_from_str(char *acl_str, AclItem *item);
static int parse_aclitem(const char *acl_str, char *kind, char *grantee,
			  uint32 * privs, char *grantor);
static bool quoted_username_get_sysid(char *username, Oid *sysid);


/*
 * replicate_acl
 * 		Try to replicate the master's state of privileges on a table
 *
 * The master is assumed to store the privileges in replication.pgr_acl in
 * the format specified by make_AclItem_from_str.
 */
Datum
replicate_acl(PG_FUNCTION_ARGS)
{
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple       resulttuple;
	HeapTuple       tuple;
	HeapTuple       newtuple;
	TupleDesc       reldesc;
	Relation        pg_class_rel;
	Datum           master_acl;
	Datum           schema;
	Datum           table;
	Oid             namespcid;
	Datum           values[Natts_pg_class];
	char            nulls[Natts_pg_class];
	char            replaces[Natts_pg_class];
	bool            is_null;

	elog(DEBUG5, "replicate_acl starting");

	/* sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "must be called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		resulttuple = trigdata->tg_trigtuple;
	else
		resulttuple = trigdata->tg_newtuple;
	reldesc = RelationGetDescr(trigdata->tg_relation);

	/* Only an active slave has work to do here. */
	if (!replication_enable || replication_master)
		return PointerGetDatum(resulttuple);

	/*
	 * In a DELETE event we don't do anything.  This is because under normal
	 * operation, DELETE should never happen ...
	 */
	if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		return PointerGetDatum(resulttuple);

	/* Lock pg_class against concurrent modification */
	pg_class_rel = heap_open(RelationRelationId, RowExclusiveLock);

	/*
	 * We don't really care if this is an INSERT or UPDATE -- just update
	 * pg_class.relacl to the master's value.
	 *
	 * First look up the values for the relacl field.
	 */
	MemSet(values, 0, sizeof(values));
	MemSet(nulls, ' ', sizeof(nulls));
	MemSet(replaces, ' ', sizeof(replaces));

	master_acl = heap_getattr(resulttuple, 3, reldesc, &is_null);
	if (!is_null)
	{
		Acl            *new_acl;

		new_acl = convert_master_acl(DatumGetArrayTypeP(master_acl));
		values[Anum_pg_class_relacl - 1] = PointerGetDatum(new_acl);
		replaces[Anum_pg_class_relacl - 1] = 'r';
	}
	else
	{
		values[Anum_pg_class_relacl - 1] = (Datum) 0;
		nulls[Anum_pg_class_relacl - 1] = 'n';
		replaces[Anum_pg_class_relacl - 1] = 'r';
	}

	/* Look up the involved table. */
	schema = heap_getattr(resulttuple, 1, reldesc, &is_null);
	Assert(!is_null);
	table = heap_getattr(resulttuple, 2, reldesc, &is_null);
	Assert(!is_null);

	/* Look up the namespace Oid.  If it doesn't exist, do nothing. */
	namespcid = GetSysCacheOid(NAMESPACENAME,
							   schema,
							   0, 0, 0);
	if (!OidIsValid(namespcid))
		goto done;

	/* Look up the table itself.  If it doesn't exist, do nothing. */
	tuple = SearchSysCache(RELNAMENSP,
						   table, namespcid,
						   0, 0);
	if (!HeapTupleIsValid(tuple))
		goto done;

	/* Everything seems OK.  Create the new tuple. */
	newtuple = heap_modifytuple(tuple, RelationGetDescr(pg_class_rel),
								values, nulls, replaces);

	ReleaseSysCache(tuple);
	simple_heap_update(pg_class_rel, &newtuple->t_self, newtuple);

	/* keep the catalog indexes up to date */
	CatalogUpdateIndexes(pg_class_rel, newtuple);

done:
	/* close pg_class and release the lock */
	heap_close(pg_class_rel, RowExclusiveLock);

	elog(DEBUG5, "replicate_acl: success");
	return PointerGetDatum(resulttuple);
}

/*
 * Convert a text array into an Acl array.  The conversion is done according
 * to make_AclItem_from_str.
 */
static ArrayType *
convert_master_acl(ArrayType * master_acl)
{
	Datum          *elems;
	int             nelems;
	Datum          *new_elems;
	int             nnew_elems = 0;
	int             i;
	ArrayType      *new_acl;

	/* XXX: do we really don't expect NULLs in array here? */
	deconstruct_array(master_acl,
					  TEXTOID, -1, false, 'i',
					  &elems, NULL, &nelems);

	new_elems = (Datum *) palloc(sizeof(Datum) * nelems);

	for (i = 0; i < nelems; i++)
	{
		AclItem        *item;
		char           *acl_str;

		acl_str = DatumGetCString(DirectFunctionCall1(textout, elems[i]));

		item = palloc(sizeof(AclItem));

		elog(DEBUG5, "convert_master_acl: got element %d (%s)",
			 i, acl_str);

		if (make_AclItem_from_str(acl_str, item))
		{
			new_elems[nnew_elems++] = PointerGetDatum(item);
			elog(DEBUG5, "convert_master_acl: item %d good", i);
		}
	}

	new_acl = construct_array(new_elems, nnew_elems,
							  ACLITEMOID, 12, false, 'i');

	return new_acl;
}

/*
 * make_AclItem_from_str Convert a replicator.pgr_acl.acl element into an
 * AclItem.
 *
 * Format is ZZZ XXXX=aaaaa/YYYY, where ZZZ is either "special" or "user".
 * If ZZZ is "special", then XXXX must be "PUBLIC".  If ZZZ is
 * "user", then XXXX must be a username.  YYYY is a possibly quoted username,
 * and aaaa is an unsigned integer privileges.
 *
 * *item is the return AclItem, which is expected to have been allocated by
 * the caller.  If the function returns true, then it's a valid AclItem;
 * else, either the grantee or the grantor is not a valid local user and the
 * caller must not dereference *item.
 */
static          bool
make_AclItem_from_str(char *acl_str, AclItem *item)
{
	char            grantee[NAMEDATALEN];
	char            grantor[NAMEDATALEN];
#define MAX_KIND_LENGTH 20
	char            kind[MAX_KIND_LENGTH];
	int             ret;
	uint32          privs = 0;

	ret = parse_aclitem(acl_str, kind, grantee, &privs, grantor);

	if (ret != 4)
		elog(ERROR,
		 "make_AclItem_from_str: could not parse ACL item %s (got only %d)",
			 acl_str, ret);

	if (strcmp(kind, "special") == 0)
	{
		if (strcmp(grantee, "PUBLIC") == 0)
		{
			item->ai_grantee = 0;
		}
		else
		{
			elog(ERROR, "\"special\" not followed by PUBLIC");
			goto error;
		}
	}
	else if (strcmp(kind, "user") == 0)
	{
		bool            ok;

		ok = quoted_username_get_sysid(grantee, &(item->ai_grantee));
		if (!ok)
		{
			/*
			 * This is only a warning because not all the users in the master
			 * must exist in the slave.
			 */
			elog(WARNING, "\"user\" followed by invalid username %s", grantee);
			goto error;
		}
	}
	else
	{
		elog(ERROR, "invalid type %s", kind);
	}

	if (!quoted_username_get_sysid(grantor, &(item->ai_grantor)))
		goto error;

	item->ai_privs = privs;

	elog(DEBUG5, "make_AclItem_from_str: ACL was %s, parsed "
		 "grantee=%d,grantor=%d,privs=%u",
		 acl_str, item->ai_grantee, item->ai_grantor, item->ai_privs);

	return true;

error:
	return false;
}

/*
 * parse_aclitem
 * 		Parse an item according to the rules in make_AclItem_from_str.
 *
 * This is kind of an poor-man's specialized sscanf() with particular quoting
 * rules.  The rules are:
 *
 * 0. The format is "kind grantee=privs/grantor"
 *
 * 1. kind is a string taken verbatim from the original, up to
 * MAX_KIND_LENGTH - 1 chars.
 *
 * 2. grantee takes up to NAMEDATALEN - 1 chars, but \ and = must be escaped
 * with \.  All other characters are verbatim.  Any other character preceded
 * by a \ is copied verbatim (but the \ is skipped).
 *
 * 3. privs takes only digits, up to MAX_NUM_DIGITS - 1.
 *
 * 4. grantor is string taken verbatim from the original, up to NAMEDATALEN -
 * 1 chars.
 */
static int
parse_aclitem(const char *acl_str, char *kind, char *grantee, uint32 * privs,
			  char *grantor)
{
#define MAX_NUM_DIGITS 16
	int             i, j;
	int             scanned;
	char            privs_tmp[MAX_NUM_DIGITS];
	bool            ended;
	int             acl_str_len;

	/* how many items we have scanned */
	scanned = 0;

	if (acl_str == NULL || acl_str[0] == '\0')
	{
		elog(WARNING, "acl_str NULL or empty%s", "");
		return 0;
	}
	acl_str_len = strlen(acl_str);

	/* acl_str counter.  We start reading from the start. */
	i = 0;

	/* from the start to the first space, it's "kind". No quoting here. */
	j = 0;
	ended = false;
	while (!ended)
	{
		/* do not overrun output buffer */
		if (j >= MAX_KIND_LENGTH - 1)
		{
			elog(WARNING, "\"kind\" string too long in \"%s\"",
				 acl_str);
			return scanned;
		}
		/* do not exceed input string */
		if (i > acl_str_len)
		{
			elog(WARNING, "short string %s scanning kind",
				 acl_str);
			return scanned;
		}
		kind[j++] = acl_str[i++];
		if (acl_str[i] == ' ')
			ended = true;
	}
	/* zero-terminate the string */
	kind[j] = '\0';
	scanned++;

	/* skip the space */
	i++;

	/* Until the first unescaped =, it's "grantee" */
	j = 0;
	ended = false;
	while (!ended)
	{
		/* do not overrun output buffer */
		if (j >= NAMEDATALEN - 1)
		{
			elog(WARNING, "\"grantee\" string too long in \"%s\"",
				 acl_str);
			return scanned;
		}
		/* do not exceed input string */
		if (i > acl_str_len)
		{
			elog(WARNING, "short string %s scanning grantee",
				 acl_str);
			return scanned;
		}
		if (acl_str[i] == '\\')
			i++;
		grantee[j++] = acl_str[i++];
		if (acl_str[i] == '=')
			ended = true;
	}
	/* zero-terminate the string */
	grantee[j] = '\0';
	scanned++;

	/* skip the = */
	i++;

	/* Until the first /, it's the privs */
	j = 0;
	ended = false;
	while (!ended)
	{
		/* do not overrun output buffer */
		if (j >= MAX_NUM_DIGITS - 1)
		{
			elog(WARNING, "\"privs\" string too long in \"%s\"",
				 acl_str);
			return scanned;
		}
		/* do not exceed input string */
		if (i > acl_str_len)
		{
			elog(WARNING, "short string %s scanning privs",
				 acl_str);
			return scanned;
		}
		/* we take only digits here, thank you */
		if (!isdigit(acl_str[i]))
		{
			elog(WARNING, "non-digit char in privs in %s",
				 acl_str);
			return scanned;
		}
		privs_tmp[j++] = acl_str[i++];
		if (acl_str[i] == '/')
			ended = true;
	}
	/* zero-terminate the string */
	privs_tmp[j] = '\0';
	*privs = strtoul(privs_tmp, NULL, 10);
	scanned++;

	/* skip the / */
	i++;

	/* Remaining chars are the grantor */
	j = 0;
	ended = false;
	while (!ended)
	{
		/* do not overrun output buffer */
		if (j >= NAMEDATALEN - 1)
		{
			elog(WARNING, "\"grantor\" string too long in \"%s\"",
				 acl_str);
			return scanned;
		}
		/* do not exceed input string */
		if (i > acl_str_len)
		{
			elog(WARNING, "short string %s scanning grantor",
				 acl_str);
			return scanned;
		}
		grantor[j++] = acl_str[i++];
		if (i == acl_str_len)
			ended = true;
	}
	/* zero-terminate the string */
	grantor[j] = '\0';
	scanned++;

#undef MAX_KIND_LENGTH
#undef MAX_NUM_DIGITS

	return scanned;
}

/*
 * quoted_username_get_sysid
 * 		Get the usesysid for a possibly quoted username.
 *
 * The quoting rules we follow are those of quote_identifier.
 *
 * Return true if the username is found, false otherwise.  sysid is the
 * output parameter.  This function does not elog(ERROR)!
 */
static bool
quoted_username_get_sysid(char *username, Oid *sysid)
{
	char           *dequoted;
	bool            palloced;
	bool            result;
	HeapTuple       tup;

	Assert(PointerIsValid(username) && username[0] != '0');

	if (username[0] == '"')
	{
		int             len, i, j;

		len = strlen(username);

		dequoted = palloc(len);
		palloced = true;

		for (i = 0, j = 0; i < len - 1; i++, j++)
		{
			if (username[i] == '"')
				i++;
			dequoted[j] = username[i];
		}
	}
	else
	{
		dequoted = username;
		palloced = false;
	}

	elog(DEBUG5, "quoted_username_get_sysid: name is %s, dequoted is %s",
		 username, dequoted);

	tup = SearchSysCache(AUTHNAME,
						 CStringGetDatum(dequoted),
						 0, 0, 0);
	if (!HeapTupleIsValid(tup))
	{
		result = false;
		*sysid = 0;
	}
	else
	{
		*sysid = HeapTupleGetOid(tup);
		result = true;

		ReleaseSysCache(tup);
	}

	elog(DEBUG5, "quoted_username_get_sysid: name is %s, dequoted is %s, sysid=%d",
		 username, dequoted, *sysid);

	if (palloced)
		pfree(dequoted);

	return result;
}
