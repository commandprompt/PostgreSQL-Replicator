/*-------------------------------------------------------------------------
 *
 * execFlatten.h
 *	  prototypes for execFlatten.c.
 *
 *
 * Portions Copyright (c) 1996-2000, PostgreSQL, Inc
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#ifndef EXECFLATTEN_H
#define EXECFLATTEN_H

#include "nodes/execnodes.h"
#include "nodes/relation.h"

extern Datum ExecEvalIter(Iter *iterNode, ExprContext *econtext, bool *resultIsNull, bool *iterIsDone);

extern void ExecEvalFjoin(TargetEntry *tlist, ExprContext *econtext, bool *isNullVect, bool *fj_isDone);


#endif	 /* EXECFLATTEN_H */
