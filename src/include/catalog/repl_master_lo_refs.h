/*-------------------------------------------------------------------------
 *
 * repl_master_lo_refs.h
 *	  Replicator info about lo references on master.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_master_lo_refs.h 432 2006-08-07 20:16:19Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_MASTER_LO_REFS_H
#define REPL_MASTER_LO_REFS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		master_lo_refs definition.
 *
 *		cpp turns this into typedef struct FormData_repl_relations
 * ----------------------------------------------------------------
 */
#define	ReplMasterLoRefsId	4532

CATALOG(repl_master_lo_refs,4532) BKI_WITHOUT_OIDS
{
	Oid			loid;
	int4		refcount;
} FormData_master_lo_refs;

/* ----------------
 *		Form_master_lo_refs corresponds to a pointer to a tuple with
 *		the format of master_lo_refs relation.
 * ----------------
 */
typedef FormData_master_lo_refs *Form_master_lo_refs;

/* ----------------
 *		compiler constants for master_lo_refs
 * ----------------
 */
#define Natts_master_lo_refs					2
#define Anum_master_lo_refs_oid					1
#define Anum_master_lo_refs_refcount			2

/* ----------------
 *		initial contents of master_lo_refs are NOTHING.
 * ----------------
 */

#endif   /* REPL_MASTER_LO_REFS_H */
