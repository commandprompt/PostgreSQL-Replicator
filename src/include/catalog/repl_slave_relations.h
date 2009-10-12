/*-------------------------------------------------------------------------
 *
 * repl_slave_relations.h
 *	  Replicator info about a table.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_slave_relations.h 432 2006-08-07 20:16:19Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_SLAVE_RELATIONS_H
#define REPL_SLAVE_RELATIONS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		repl_slave_relations definition.
 *
 *		cpp turns this into typedef struct FormData_repl_slave_relations
 * ----------------------------------------------------------------
 */
#define ReplSlaveRelationsId	4535

CATALOG(repl_slave_relations,4535) BKI_WITHOUT_OIDS
{
	int4		slave;
	Oid			relid;
	bool		enable;
	NameData	namespace;
	NameData	relation;
} FormData_repl_slave_relations;

/* ----------------
 *		Form_repl_slave_relations corresponds to a pointer to a tuple with
 *		the format of repl_slave_relations relation.
 * ----------------
 */
typedef FormData_repl_slave_relations *Form_repl_slave_relations;

/* ----------------
 *		compiler constants for repl_slave_relations
 * ----------------
 */
#define Natts_repl_slave_relations					5
#define Anum_repl_slave_relations_slave				1
#define Anum_repl_slave_relations_relid				2
#define Anum_repl_slave_relations_enable			3
#define Anum_repl_slave_relations_namespace			4
#define Anum_repl_slave_relations_relation			5

/* ----------------
 *		initial contents of repl_slave_relations are NOTHING.
 * ----------------
 */

#endif   /* REPL_SLAVE_RELATIONS_H */
