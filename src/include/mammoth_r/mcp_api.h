/*
 * mcp_api.h
 * 		Prototypes and definitions for functions from mcp_api.c
 *
 * $Id: mcp_api.h 2109 2009-04-23 16:46:20Z alvherre $
 */
#ifndef MCP_API_H
#define MCP_API_H

#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_queue.h"
#include "nodes/pg_list.h"

typedef enum FullDumpProgress
{
    FDnone,
    FDrequested,
    FDinprogress
} FullDumpProgress;

#define FDProgressAsString(progress) \
    (( progress == TDnone) ? "none" : \
     (progress == TDrequested) ? "requested" : \
     (progress == TDinprogress) ? "in progress" : "unknown")


extern void SendQueueTransaction(MCPQueue *q, ullong recno,
					 bool (*tablelist_hook)(TxDataHeader *, List *, void *), 
					 void *tablelist_arg,
					 off_t (*message_hook)(TxDataHeader *, void *, ullong), 
					 void *message_arg);

extern MCPMsg *ReceiveQueueTransaction(MCPQueue *q, 
						void (*tablelist_hook)(List *, TxDataHeader *, ullong, void *), 
						void *tablelist_arg,
						bool (*message_hook)(bool, MCPMsg *, void *),
						void *message_arg);

#endif /* MCP_API_H */
