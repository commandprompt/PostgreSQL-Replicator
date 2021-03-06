/*-------------------------------------------------------------------------
 *
 * nodeCtescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECTESCAN_H
#define NODECTESCAN_H

#include "nodes/execnodes.h"

extern int	ExecCountSlotsCteScan(CteScan *node);
extern CteScanState *ExecInitCteScan(CteScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecCteScan(CteScanState *node);
extern void ExecEndCteScan(CteScanState *node);
extern void ExecCteScanReScan(CteScanState *node, ExprContext *exprCtxt);

#endif   /* NODECTESCAN_H */
