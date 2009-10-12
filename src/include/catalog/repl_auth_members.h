/*-------------------------------------------------------------------------
 *
 * repl_auth_members.h
 *	  definition of Mammoth Replicator's auth_member replication catalog
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id: repl_auth_members.h 453 2006-08-15 16:18:57Z alvherre $
 *
 * NOTES
 *	  the genbki.sh script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPL_AUTH_MEMBERS_H
#define REPL_AUTH_MEMBERS_H

/* ----------------
 *		repl_auth_members definition.  cpp turns this into
 *		typedef struct FormData_repl_auth_members
 * ----------------
 */
#define ReplAuthMemRelationId	4548

CATALOG(repl_auth_members,4548) BKI_WITHOUT_OIDS
{
	NameData	rolename;		/* name of a role */
	NameData	membername;		/* name of a member of that role */
	NameData	grantorname;	/* who granted the membership? */
	bool		admin_option;	/* granted with admin option? */
} FormData_repl_auth_members;

/* ----------------
 *		Form_repl_auth_members corresponds to a pointer to a tuple with
 *		the format of repl_auth_members relation.
 * ----------------
 */
typedef FormData_repl_auth_members *Form_repl_auth_members;

/* ----------------
 *		compiler constants for repl_auth_members
 * ----------------
 */
#define Natts_repl_auth_members					4
#define Anum_repl_auth_members_rolename			1
#define Anum_repl_auth_members_membername		2
#define Anum_repl_auth_members_grantorname		3
#define Anum_repl_auth_members_admin_option		4

#endif   /* REPL_AUTH_MEMBERS_H */
