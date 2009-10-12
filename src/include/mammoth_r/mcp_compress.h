/*
 * mcp_compress.h
 * 		Declarations for Replicator's compression code.
 *
 * $Id: mcp_compress.h 1763 2008-06-24 21:16:57Z alvherre $
 */

#ifndef MCP_COMPRESS_H
#define MCP_COMPRESS_H

#include "lib/stringinfo.h"
#include "mammoth_r/mcp_local_queue.h"

#define MCP_DATA_COMPRESSED 0x1

extern void *MCPCompressData(void *data, MCPQueueTxlDataHeader *dh);
extern StringInfo MCPDecompressData(StringInfo comp, MCPQueueTxlDataHeader *dh);

#endif /* MCP_COMPRESS_H */
