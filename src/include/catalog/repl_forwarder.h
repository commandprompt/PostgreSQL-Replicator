/*-------------------------------------------------------------------------
 *
 * repl_forwarder.h
 *	  Replicator info about a forwarder.
 *
 *
 * Portions Copyright (c) 2006, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_forwarder.h 2117 2009-04-30 22:32:24Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_FORWARDER_H
#define REPL_FORWARDER_H

#include "utils/inet.h"

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		repl_forwarder definition.
 *
 *		cpp turns this into typedef struct FormData_repl_forwarder
 * ----------------------------------------------------------------
 */
#define ReplForwarderId		4552

/*
 * Note: if you change this, you must update include/catalog/pg_attribute.h too
 */
CATALOG(repl_forwarder,4552) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	NameData	name;
	inet		host;
	int4		port;
	text		authkey;
	bool		ssl;
	bool		active;
} FormData_repl_forwarder;

/* ----------------
 *		Form_repl_forwarder corresponds to a pointer to a tuple with
 *		the format of repl_forwarder relation.
 * ----------------
 */
typedef FormData_repl_forwarder *Form_repl_forwarder;

/* ----------------
 *		compiler constants for repl_forwarder
 * ----------------
 */
#define Natts_repl_forwarder				6
#define Anum_repl_forwarder_name			1
#define Anum_repl_forwarder_host			2
#define Anum_repl_forwarder_port			3
#define Anum_repl_forwarder_authkey			4
#define Anum_repl_forwarder_ssl				5
#define Anum_repl_forwarder_active			6

/* ----------------
 *		initial contents of repl_forwarder are NOTHING.
 * ----------------
 */

#endif   /* REPL_FORWARDER_H */
