/*------------------------------
 * mcp_local_queue.h
 * 		Exports from mammoth_r/agents/mcp_local_queue.c
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_local_queue.h 2093 2009-04-09 19:05:33Z alvherre $
 *------------------------------
 */
#ifndef MCP_LOCAL_QUEUE_H
#define MCP_LOCAL_QUEUE_H

#include "lib/stringinfo.h"
#include "mammoth_r/mcp_queue.h"
#include "utils/pg_crc.h"

#define TXL_FLAG_COMMAND_END 	1
#define TXL_FLAG_COMPRESSED 	2
#define TXL_FLAG_CHECKSUMMED	4

typedef struct MCPQueueTxlDataHeader
{
   pg_crc32	tdh_crc32;
   uint32   tdh_datalen;
   char     tdh_flags;
} MCPQueueTxlDataHeader;

typedef struct MCPLocalQueue MCPLocalQueue;

extern MCPLocalQueue *MCPLocalQueueInit(MCPQueue *q);
extern void MCPLocalQueueSwitchFile(MCPLocalQueue *lq);
extern void MCPLocalQueueClose(MCPLocalQueue *lq);
extern void MCPLocalQueueAppend(MCPLocalQueue *lq, void *data, uint32 datalen,
								char flags, bool trans);
extern void MCPLocalQueueAddRelation(MCPLocalQueue *lq, char *relpath,
									 int raise_dump, bool trans);
extern void MCPLocalQueueFinalize(MCPLocalQueue *lq, int flags,
								  bool is_commit);
extern bool MCPLocalQueueIsEmpty(MCPLocalQueue *lq, bool is_commit);
extern void MCPLocalQueueTruncate(MCPLocalQueue *lq);
extern MCPFile *MCPLocalQueueGetFile(MCPLocalQueue *lq);
extern StringInfo MCPQueueTxReadPacket(MCPQueue *q, bool allow_end);
extern void MCPLocalQueueFinishEmptyTx(MCPLocalQueue *lq, int flags);
extern void LocalQueueAddSlaveToBitmapset(MCPLocalQueue *lq, int slaveno);

#endif /* MCP_LOCAL_QUEUE_H */
