/*-----------------------
 * common.c
 * 		Some common routines for the collector/replay code
 *
 * Portions Copyright (c) 1996-2007, PostgreSQL Global Development Group.
 * Copyright (c) 2007, Command Prompt, Inc.
 *
 * $Id: common.c 2089 2009-03-27 16:22:19Z alvherre $
 *
 * ------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "mammoth_r/collcommon.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"

/*
 * Return the relcache entry corresponding to the primary key of the given
 * relation (which must be a plain heap relation).
 */
Relation
PGRFindPK(Relation rel, LOCKMODE lmode)
{
	List	   *indexes;
	ListCell   *cell;
	bool		found = false;
	Relation	indrel = NULL;

	Assert(rel->rd_rel->relkind == RELKIND_RELATION);

	indexes = RelationGetIndexList(rel);

	/*
	 * For each index, verify whether it's the PK or not.  At the end of this
	 * loop, either "found" will be false or the PK index will be open.
	 */
	foreach(cell, indexes)
	{
		Oid			indexid = lfirst_oid(cell);

		indrel = relation_open(indexid, NoLock);

		if (indrel->rd_index->indisprimary)
		{
			found = true;
			break;
		}

		/* close an index which is not the PK */
		relation_close(indrel, NoLock);
	}

	if (!found)
		elog(ERROR, "no primary index found for relation %s",
			 RelationGetRelationName(rel));

	if (indexes)
		list_free(indexes);

	/* Lock the index appropiately */
	LockRelation(indrel, lmode);
	return indrel;
}

/*
 * Return a palloc'ed string corresponding to a simple-minded text version
 * of the given tuple.  Mainly intended for debugging logs.
 *
 * Caller is responsible for pfreeing it.
 */
char *
tupleToString(HeapTuple tup, TupleDesc typeinfo)
{
	int		natts = typeinfo->natts;
	int		i;
	StringInfoData string;

	initStringInfo(&string);

	for (i = 0; i < natts; ++i)
	{
		Datum	origattr;
		char   *value;
		bool	isnull;
		Oid		typoutput;
		bool	typisvarlena;

		if (i > 0)
			appendStringInfoString(&string, "; ");

		origattr = heap_getattr(tup, i+1, typeinfo, &isnull);
		if (isnull)
			continue;
		if (typeinfo->attrs[i]->attisdropped)
		{
			/* XXX: Confusing if real attribute contains text 'dropped' */
			appendStringInfoString(&string, "dropped");
			continue;
		}
		getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
						  &typoutput, &typisvarlena);

		if (typisvarlena)
		{
			appendStringInfoString(&string, "varlena att");
		}
		else
		{
			value = DatumGetCString(OidFunctionCall3(typoutput,
													 origattr, 0,
													 Int32GetDatum(typeinfo->attrs[i]->atttypmod)));
			appendStringInfoString(&string, value);
			pfree(value);
		}
	}

	return string.data;
}


