/*-------------------------------------------------------------------------
 *
 * printtup.h--
 *
 *
 *
 * Copyright (c) 1994, Regents of the University of California
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include <access/htup.h>
#include <access/tupdesc.h>

extern Oid	typtoout(Oid type);
extern void printtup(HeapTuple tuple, TupleDesc typeinfo);
extern void showatts(char *name, TupleDesc attinfo);
extern void debugtup(HeapTuple tuple, TupleDesc typeinfo);
extern void printtup_internal(HeapTuple tuple, TupleDesc typeinfo);
extern Oid	gettypelem(Oid type);

#endif							/* PRINTTUP_H */
