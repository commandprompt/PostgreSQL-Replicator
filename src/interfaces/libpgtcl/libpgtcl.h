/*-------------------------------------------------------------------------
 *
 * libpgtcl.h
 *
 *	libpgtcl is a tcl package for front-ends to interface with PostgreSQL.
 *	It's a Tcl wrapper for libpq.
 *
 * Portions Copyright (c) 1996-2000, PostgreSQL, Inc
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef LIBPGTCL_H
#define LIBPGTCL_H

#include "tcl.h"

extern int	Pgtcl_Init(Tcl_Interp *interp);
extern int	Pgtcl_SafeInit(Tcl_Interp *interp);

#endif	 /* LIBPGTCL_H */
