/* 
 * repl_tuples.h 
 * 
 * $Id: repl_tuples.h 2050 2009-03-13 16:14:15Z alexk $
 */

#ifndef _REPL_TUPLES_H
#define	_REPL_TUPLES_H

#include "access/tupdesc.h"

/*
 * Definitions for repl_members tuple - a tuple, constructed to
 * replicate auth_members tuples.
 */
#define Natts_repl_members_tuple 5

#define Anum_repl_members_tuple_rolename    	1
#define Anum_repl_members_tuple_membername  	2
#define Anum_repl_members_tuple_grantorname 	3
#define Anum_repl_members_tuple_admin_option 	4
#define Anum_repl_members_tuple_revoke_admin 	5

extern TupleDesc MakeReplAuthMembersTupleDesc(void);


#endif	/* _REPL_TUPLES_H */

