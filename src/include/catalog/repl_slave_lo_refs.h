/*-------------------------------------------------------------------------
 *
 * repl_slave_lo_refs.h
 *	  Replicator info to match lo oid references from master on the slave side.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_slave_lo_refs.h 432 2006-08-07 20:16:19Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_SLAVE_LO_REFS_H
#define REPL_SLAVE_LO_REFS_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		slave_lo_refs definition.
 *
 *		cpp turns this into typedef struct FormData_repl_relations
 * ----------------------------------------------------------------
 */
#define	ReplSlaveLoRefsId	4534

CATALOG(repl_slave_lo_refs,4534) BKI_WITHOUT_OIDS
{
	Oid			masteroid;
	Oid			slaveoid;
} FormData_slave_lo_refs;

/* ----------------
 *		Form_slave_lo_refs corresponds to a pointer to a tuple with
 *		the format of slave_lo_refs relation.
 * ----------------
 */
typedef FormData_slave_lo_refs *Form_slave_lo_refs;

/* ----------------
 *		compiler constants for slave_lo_refs
 * ----------------
 */
#define Natts_slave_lo_refs					2
#define Anum_slave_lo_refs_masteroid		1
#define Anum_slave_lo_refs_slaveoid			2

/* ----------------
 *		initial contents of slave_lo_refs are NOTHING.
 * ----------------
 */

#endif   /* REPL_SAVE_LO_REFS_H */
