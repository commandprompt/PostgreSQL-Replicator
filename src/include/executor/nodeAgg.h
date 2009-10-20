/*-------------------------------------------------------------------------
 *
 * nodeAgg.h
 *	  prototypes for nodeAgg.c
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAGG_H
#define NODEAGG_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsAgg(Agg *node);
extern AggState *ExecInitAgg(Agg *node, EState *estate, int eflags);
extern TupleTableSlot *ExecAgg(AggState *node);
extern void ExecEndAgg(AggState *node);
extern void ExecReScanAgg(AggState *node, ExprContext *exprCtxt);

extern Size hash_agg_entry_size(int numAggs);

extern Datum aggregate_dummy(PG_FUNCTION_ARGS);

#endif   /* NODEAGG_H */
