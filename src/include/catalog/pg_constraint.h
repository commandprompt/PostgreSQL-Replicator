/*-------------------------------------------------------------------------
 *
 * pg_constraint.h
 *	  definition of the system "constraint" relation (pg_constraint)
 *	  along with the relation's initial contents.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CONSTRAINT_H
#define PG_CONSTRAINT_H

#include "catalog/genbki.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_constraint definition.  cpp turns this into
 *		typedef struct FormData_pg_constraint
 * ----------------
 */
#define ConstraintRelationId  2606

CATALOG(pg_constraint,2606)
{
	/*
	 * conname + connamespace is deliberately not unique; we allow, for
	 * example, the same name to be used for constraints of different
	 * relations.  This is partly for backwards compatibility with past
	 * Postgres practice, and partly because we don't want to have to obtain a
	 * global lock to generate a globally unique name for a nameless
	 * constraint.	We associate a namespace with constraint names only for
	 * SQL92 compatibility.
	 */
	NameData	conname;		/* name of this constraint */
	Oid			connamespace;	/* OID of namespace containing constraint */
	char		contype;		/* constraint type; see codes below */
	bool		condeferrable;	/* deferrable constraint? */
	bool		condeferred;	/* deferred by default? */

	/*
	 * conrelid and conkey are only meaningful if the constraint applies to a
	 * specific relation (this excludes domain constraints and assertions).
	 * Otherwise conrelid is 0 and conkey is NULL.
	 */
	Oid			conrelid;		/* relation this constraint constrains */

	/*
	 * contypid links to the pg_type row for a domain if this is a domain
	 * constraint.	Otherwise it's 0.
	 *
	 * For SQL-style global ASSERTIONs, both conrelid and contypid would be
	 * zero. This is not presently supported, however.
	 */
	Oid			contypid;		/* domain this constraint constrains */

	/*
	 * These fields, plus confkey, are only meaningful for a foreign-key
	 * constraint.	Otherwise confrelid is 0 and the char fields are spaces.
	 */
	Oid			confrelid;		/* relation referenced by foreign key */
	char		confupdtype;	/* foreign key's ON UPDATE action */
	char		confdeltype;	/* foreign key's ON DELETE action */
	char		confmatchtype;	/* foreign key's match type */

	/* Has a local definition (hence, do not drop when coninhcount is 0) */
	bool		conislocal;

	/* Number of times inherited from direct parent relation(s) */
	int4		coninhcount;

	/*
	 * VARIABLE LENGTH FIELDS start here.  These fields may be NULL, too.
	 */

	/*
	 * Columns of conrelid that the constraint applies to
	 */
	int2		conkey[1];

	/*
	 * If a foreign key, the referenced columns of confrelid
	 */
	int2		confkey[1];

	/*
	 * If a foreign key, the OIDs of the PK = FK equality operators for each
	 * column of the constraint
	 */
	Oid			conpfeqop[1];

	/*
	 * If a foreign key, the OIDs of the PK = PK equality operators for each
	 * column of the constraint (i.e., equality for the referenced columns)
	 */
	Oid			conppeqop[1];

	/*
	 * If a foreign key, the OIDs of the FK = FK equality operators for each
	 * column of the constraint (i.e., equality for the referencing columns)
	 */
	Oid			conffeqop[1];

	/*
	 * If a check constraint, nodeToString representation of expression
	 */
	text		conbin;

	/*
	 * If a check constraint, source-text representation of expression
	 */
	text		consrc;
} FormData_pg_constraint;

/* ----------------
 *		Form_pg_constraint corresponds to a pointer to a tuple with
 *		the format of pg_constraint relation.
 * ----------------
 */
typedef FormData_pg_constraint *Form_pg_constraint;

/* ----------------
 *		compiler constants for pg_constraint
 * ----------------
 */
#define Natts_pg_constraint					20
#define Anum_pg_constraint_conname			1
#define Anum_pg_constraint_connamespace		2
#define Anum_pg_constraint_contype			3
#define Anum_pg_constraint_condeferrable	4
#define Anum_pg_constraint_condeferred		5
#define Anum_pg_constraint_conrelid			6
#define Anum_pg_constraint_contypid			7
#define Anum_pg_constraint_confrelid		8
#define Anum_pg_constraint_confupdtype		9
#define Anum_pg_constraint_confdeltype		10
#define Anum_pg_constraint_confmatchtype	11
#define Anum_pg_constraint_conislocal		12
#define Anum_pg_constraint_coninhcount		13
#define Anum_pg_constraint_conkey			14
#define Anum_pg_constraint_confkey			15
#define Anum_pg_constraint_conpfeqop		16
#define Anum_pg_constraint_conppeqop		17
#define Anum_pg_constraint_conffeqop		18
#define Anum_pg_constraint_conbin			19
#define Anum_pg_constraint_consrc			20


/* Valid values for contype */
#define CONSTRAINT_CHECK			'c'
#define CONSTRAINT_FOREIGN			'f'
#define CONSTRAINT_PRIMARY			'p'
#define CONSTRAINT_UNIQUE			'u'

/*
 * Valid values for confupdtype and confdeltype are the FKCONSTR_ACTION_xxx
 * constants defined in parsenodes.h.  Valid values for confmatchtype are
 * the FKCONSTR_MATCH_xxx constants defined in parsenodes.h.
 */

/*
 * Identify constraint type for lookup purposes
 */
typedef enum ConstraintCategory
{
	CONSTRAINT_RELATION,
	CONSTRAINT_DOMAIN,
	CONSTRAINT_ASSERTION		/* for future expansion */
} ConstraintCategory;

/*
 * prototypes for functions in pg_constraint.c
 */
extern Oid CreateConstraintEntry(const char *constraintName,
					  Oid constraintNamespace,
					  char constraintType,
					  bool isDeferrable,
					  bool isDeferred,
					  Oid relId,
					  const int16 *constraintKey,
					  int constraintNKeys,
					  Oid domainId,
					  Oid foreignRelId,
					  const int16 *foreignKey,
					  const Oid *pfEqOp,
					  const Oid *ppEqOp,
					  const Oid *ffEqOp,
					  int foreignNKeys,
					  char foreignUpdateType,
					  char foreignDeleteType,
					  char foreignMatchType,
					  Oid indexRelId,
					  Node *conExpr,
					  const char *conBin,
					  const char *conSrc,
					  bool conIsLocal,
					  int conInhCount);

extern void RemoveConstraintById(Oid conId);
extern void RenameConstraintById(Oid conId, const char *newname);

extern bool ConstraintNameIsUsed(ConstraintCategory conCat, Oid objId,
					 Oid objNamespace, const char *conname);
extern char *ChooseConstraintName(const char *name1, const char *name2,
					 const char *label, Oid namespace,
					 List *others);

extern void AlterConstraintNamespaces(Oid ownerId, Oid oldNspId,
						  Oid newNspId, bool isType);

#endif   /* PG_CONSTRAINT_H */
