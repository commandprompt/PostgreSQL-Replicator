/*-------------------------------------------------------------------------
 *
 * repl_lo_columns.h
 *	  Replicator info about a table.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#ifndef REPL_LO_COLUMNS_H
#define REPL_LO_COLUMNS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		repl_lo_columns definition.
 *
 *		cpp turns this into typedef struct FormData_repl_lo_columns
 * ----------------------------------------------------------------
 */
#define ReplLoColumnsId		4531

CATALOG(repl_lo_columns,4531) BKI_WITHOUT_OIDS
{
	Oid			relid;
	NameData	namespace;
	NameData	relation;
	int4		attnum;
	bool		lo;
} FormData_repl_lo_columns;

/* ----------------
 *		Form_repl_lo_columns corresponds to a pointer to a tuple with
 *		the format of repl_lo_columns relation.
 * ----------------
 */
typedef FormData_repl_lo_columns *Form_repl_lo_columns;

/* ----------------
 *		compiler constants for repl_lo_columns
 * ----------------
 */
#define Natts_repl_lo_columns					5
#define Anum_repl_lo_columns_relid				1
#define Anum_repl_lo_columns_namespace			2
#define Anum_repl_lo_columns_relation			3
#define Anum_repl_lo_columns_attnum				4
#define Anum_repl_lo_columns_lo					5

#define Anum_repl_lo_columns_idx_relid			1
#define Anum_repl_lo_columns_idx_attnum			2
/* ----------------
 *		initial contents of repl_lo_columns are NOTHING.
 * ----------------
 */

#endif   /* REPL_LO_COLUMNS_H */
