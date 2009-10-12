/*
 *  repl_slave_roles.h
 *		Description of pg_replication.repl_slave_roles catalog
 *
 * Portions Copyright (c) 2008, Command Prompt Inc.
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 */

#ifndef REPL_SLAVE_ROLES_H
#define REPL_SLAVE_ROLES_H

/* ----------------
 *		postgres.h contains the system type definitions and the
 *		CATALOG(), BOOTSTRAP and DATA() sugar words so this file
 *		can be read by both genbki.sh and the C compiler.
 * ----------------
 */

/* ----------------------------------------------------------------
 *		repl_slave_roles definition.
 *
 *		cpp turns this into typedef struct FormData_repl_slave_relations
 * ----------------------------------------------------------------
 */
#define ReplSlaveRolesRelationId	4561

CATALOG(repl_slave_roles,4561) BKI_WITHOUT_OIDS
{
	NameData	rolename;
	int4		slave;
	bool		enable;
} FormData_repl_slave_roles;

/* ----------------
 *		Form_repl_roles corresponds to a pointer to a tuple with
 *		the format of repl_slave_relations relation.
 * ----------------
 */
typedef FormData_repl_slave_roles *Form_repl_slave_roles;

/* ----------------
 *		compiler constants for repl_slave_relations
 * ----------------
 */
#define Natts_repl_slave_roles						3
#define Anum_repl_slave_roles_rolename				1
#define Anum_repl_slave_roles_slave 				2
#define Anum_repl_slave_roles_enable				3

#endif /* REPL_SLAVE_ROLES_H */
