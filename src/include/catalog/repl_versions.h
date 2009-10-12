/*-------------------------------------------------------------------------
 *
 * repl_versions.h
 *	  Replicator info about installed pg_catalog.repl_* catalogs.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_versions.h 2013 2009-01-30 11:30:09Z alexk $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_VERSIONS_H
#define REPL_VERSIONS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* See pg_authid.h */
#define timestamptz	Datum


/* ----------------------------------------------------------------
 *		repl_versions definition.
 *
 *		cpp turns this into typedef struct FormData_repl_versions
 * ----------------------------------------------------------------
 */
#define ReplVersionsId	4536

CATALOG(repl_versions,4536) BKI_WITHOUT_OIDS
{
	int4		version;
	timestamptz	applied;
} FormData_repl_versions;

#undef timestamptz

/* ----------------
 *		Form_repl_versions corresponds to a pointer to a tuple with
 *		the format of repl_versions relation.
 * ----------------
 */
typedef FormData_repl_versions *Form_repl_versions;

/* ----------------
 *		compiler constants for repl_versions
 * ----------------
 */
#define Natts_repl_versions					2
#define Anum_repl_versions_version			1
#define Anum_repl_versions_applied			2

DATA(insert ( 2009013001  "2009-01-30 00:00:00.00000+00" ));

#endif   /* REPL_VERSIONS_H */
