/*------------------------------
 * agents.h
 * 		Exports from files in mammoth_r/agents/
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: agents.h 2147 2009-05-25 10:09:05Z alexk $
 *
 *------------------------------
 */
#ifndef AGENTS_H
#define AGENTS_H

#include "mammoth_r/mcp_local_queue.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/paths.h"
#include "nodes/pg_list.h"

/* collector/master.c */
extern bool	PGRUseDumpMode;

/* Replicator process globally known variables */
extern MCPQueue *MasterMCPQueue;	/* valid on all processes in the master */
extern MCPLocalQueue *MasterLocalQueue; /* ditto */

extern MCPQueue *SlaveMCPQueue;		/* valid only on replicator process in the
									 * slave */
/* in slave_mcp_agent.c */
extern int		ReplicationSlaveMain(MCPQueue *queue, int hostno);

/* in master_mcp_agent.c */
extern int		ReplicationMasterMain(MCPQueue *queue);
extern void		MasterFlushReplicationData(ullong flush_recno);
extern off_t	PGRGetCurrentLocalOffset(void);
extern void		PGRAbortSubTransaction(off_t offset);

/* in collector/slave.c */
#define REPLICATED_LIST_PATH	MAMMOTH_BACKEND_DIR"/replicated_list"
extern void PGRRestoreData(MCPQueue *mcpq, int slaveno);
extern void SlaveTruncateAll(int hostno);
extern List *SlaveGetReplicated(void);

#endif   /* AGENTS_H */
