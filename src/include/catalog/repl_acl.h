/*-------------------------------------------------------------------------
 *
 * repl_acl.h
 *	  definition of the repl_acl catalog.
 *
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_acl.h 458 2006-08-15 18:40:02Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_ACL_H
#define REPL_ACL_H


/* ----------------
 *		repl_acl definition.  cpp turns this into
 *		typedef struct FormData_repl_acl
 * ----------------
 */
#define ReplAclRelationId	4550

CATALOG(repl_acl,4550) BKI_WITHOUT_OIDS
{
	NameData	schema;			/* namespace name */
	NameData	relname;		/* relation name */
	text		acl[1];			/* ACL itself */
} FormData_repl_acl;


/* ----------------
 *		Form_repl_acl corresponds to a pointer to a tuple with
 *		the format of repl_acl relation.
 * ----------------
 */
typedef FormData_repl_acl *Form_repl_acl;

/* ----------------
 *		compiler constants for repl_acl
 * ----------------
 */
#define Natts_repl_acl			3
#define Anum_repl_acl_schema	1
#define Anum_repl_acl_relname	2
#define Anum_repl_acl_acl		3

#endif   /* REPL_ACL_H */
