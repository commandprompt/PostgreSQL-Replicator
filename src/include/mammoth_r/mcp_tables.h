/*
 * mcp_tables.h
 * 		Interface functions for MCP server relation name tables.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2008, Command Prompt, Inc.
 *
 *
 * $Id: mcp_tables.h 2117 2009-04-30 22:32:24Z alvherre $
 */

#ifndef __MCP_TABLES_H_
#define __MCP_TABLES_H_

#include "mammoth_r/common_tables.h"

typedef enum TableDumpProgress
{
	TDnone = 0,
	TDrequested = 1,
	TDinprogress = 2
} TableDumpProgress;

#define TDProgressAsString(progress) \
    (( progress == TDnone) ? "none" : \
     (progress == TDrequested) ? "requested" : \
     (progress == TDinprogress) ? "in progress" : "unknown")

extern MCPTable MakeMCPTable(char *relpath);
extern MCPTable CopyMCPTable(MCPTable copy);

#endif
