/*
 * $Id: collcommon.h 2089 2009-03-27 16:22:19Z alvherre $
 */
#ifndef COLLCOMMON_H
#define COLLCOMMON_H

#include "utils/rel.h"
#include "storage/lock.h"
#include "access/htup.h"

extern Relation PGRFindPK(Relation rel, LOCKMODE lmode);
extern char *tupleToString(HeapTuple tup, TupleDesc typeinfo);

#endif /* COLLCOMMON_H */
