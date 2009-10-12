/*-------------------------------------------------------------------------
 *
 * repl_authid.h
 *	  definition of the repl_authid catalog.
 *
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_authid.h 450 2006-08-11 22:13:17Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_AUTHID_H
#define REPL_AUTHID_H

/* See pg_authid.h */
#define timestamptz Datum


/* ----------------
 *		repl_authid definition.  cpp turns this into
 *		typedef struct FormData_repl_authid
 * ----------------
 */
#define ReplAuthIdRelationId	4546

CATALOG(repl_authid,4546) BKI_WITHOUT_OIDS
{
	NameData	rolname;		/* name of role */
	bool		rolsuper;		/* read this field via superuser() only! */
	bool		rolinherit;		/* inherit privileges from other roles? */
	bool		rolcreaterole;	/* allowed to create more roles? */
	bool		rolcreatedb;	/* allowed to create databases? */
	bool		rolcatupdate;	/* allowed to alter catalogs manually? */
	bool		rolcanlogin;	/* allowed to log in as session user? */
	int4		rolconnlimit;	/* max connections allowed (-1=no limit) */

	/* remaining fields may be null; use heap_getattr to read them! */
	text		rolpassword;	/* password, if any */
	timestamptz rolvaliduntil;	/* password expiration time, if any */
	text		rolconfig[1];	/* GUC settings to apply at login */
} FormData_repl_authid;

#undef timestamptz


/* ----------------
 *		Form_repl_authid corresponds to a pointer to a tuple with
 *		the format of repl_authid relation.
 * ----------------
 */
typedef FormData_repl_authid *Form_repl_authid;

/* ----------------
 *		compiler constants for repl_authid
 * ----------------
 */
#define Natts_repl_authid				11
#define Anum_repl_authid_rolname		1
#define Anum_repl_authid_rolsuper		2
#define Anum_repl_authid_rolinherit		3
#define Anum_repl_authid_rolcreaterole	4
#define Anum_repl_authid_rolcreatedb	5
#define Anum_repl_authid_rolcatupdate	6
#define Anum_repl_authid_rolcanlogin	7
#define Anum_repl_authid_rolconnlimit	8
#define Anum_repl_authid_rolpassword	9
#define Anum_repl_authid_rolvaliduntil	10
#define Anum_repl_authid_rolconfig		11

#endif   /* REPL_AUTHID_H */
