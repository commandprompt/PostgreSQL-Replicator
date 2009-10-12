/*
 * Mammoth Replicator master agent dump related functions
 *
 * $Id: master.h 2089 2009-03-27 16:22:19Z alvherre $
 */
#ifndef MASTER_H
#define MASTER_H

#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/pgr.h"

extern ullong PGRDumpAll(MCPQueue *master_mcpq);
extern ullong PGRDumpCatalogs(MCPQueue *master_mcpq, CommitDumpMode mode);
extern void PGRDumpSingleTable(MCPQueue *master_mcpq, char *relpath);

#endif /* MASTER_H */
