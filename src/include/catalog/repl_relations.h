/*-------------------------------------------------------------------------
 *
 * repl_relations.h
 *	  Replicator info about a table.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_relations.h 415 2006-07-29 05:51:02Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_RELATIONS_H
#define REPL_RELATIONS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		repl_relations definition.
 *
 *		cpp turns this into typedef struct FormData_repl_relations
 * ----------------------------------------------------------------
 */
#define ReplRelationsId		4533

CATALOG(repl_relations,4533) BKI_WITHOUT_OIDS
{
	Oid			relid;
	bool		enable;
	NameData	namespace;
	NameData	relation;
} FormData_repl_relations;

/* ----------------
 *		Form_repl_relations corresponds to a pointer to a tuple with
 *		the format of repl_relations relation.
 * ----------------
 */
typedef FormData_repl_relations *Form_repl_relations;

/* ----------------
 *		compiler constants for repl_relations
 * ----------------
 */
#define Natts_repl_relations					4
#define Anum_repl_relations_relid				1
#define Anum_repl_relations_enable				2
#define Anum_repl_relations_namespace			3
#define Anum_repl_relations_relation			4

/* ----------------
 *		initial contents of repl_relations are NOTHING.
 * ----------------
 */

#endif   /* REPL_RELATIONS_H */
