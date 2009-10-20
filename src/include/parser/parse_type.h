/*-------------------------------------------------------------------------
 *
 * parse_type.h
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_TYPE_H
#define PARSE_TYPE_H

#include "access/htup.h"
#include "parser/parse_node.h"


typedef HeapTuple Type;

extern Type LookupTypeName(ParseState *pstate, const TypeName *typename,
			   int32 *typmod_p);
extern Type typenameType(ParseState *pstate, const TypeName *typename,
			 int32 *typmod_p);
extern Oid typenameTypeId(ParseState *pstate, const TypeName *typename,
			   int32 *typmod_p);

extern char *TypeNameToString(const TypeName *typename);
extern char *TypeNameListToString(List *typenames);

extern Type typeidType(Oid id);

extern Oid	typeTypeId(Type tp);
extern int16 typeLen(Type t);
extern bool typeByVal(Type t);
extern char *typeTypeName(Type t);
extern Oid	typeTypeRelid(Type typ);
extern Datum stringTypeDatum(Type tp, char *string, int32 atttypmod);

extern Oid	typeidTypeRelid(Oid type_id);

extern void parseTypeString(const char *str, Oid *type_id, int32 *typmod_p);

#define ISCOMPLEX(typeid) (typeidTypeRelid(typeid) != InvalidOid)

#endif   /* PARSE_TYPE_H */
