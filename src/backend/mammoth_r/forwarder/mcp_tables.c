/*-------------------------------------------------------------------------
* mcp_tables.c
*      MCP server storage for table names and replication flags..
*
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
* Copyright (c) 2008, Command Prompt, Inc.
*
* $Id: mcp_tables.c 2117 2009-04-30 22:32:24Z alvherre $
*
* -------------------------------------------------------------------------
*/
#include "postgres.h"

#include "mammoth_r/common_tables.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/mcp_tables.h"

/* 
 * Makes a new table with a given nspname and relname, other attributes
 * are assigned to their default values.
 */
MCPTable 
MakeMCPTable(char *relpath)
{
	MCPTable result = (MCPTable) MakeTable(relpath, TABLEID_MCP);

	/* ReqSatisfied is initially set to 'none' and dump_recno is invalid 
	 * If either of these values is different then the dump was requested 
	 */
	result->dump_recno = InvalidRecno;
	result->req_satisfied = TDnone;

	memset(result->on_slave, 0, MCP_MAX_SLAVES * sizeof(bool));
	memset(result->slave_req, 0, MCP_MAX_SLAVES * sizeof(bool));

	return result;
}

/* Makes a palloced copy of an existing table */ 
MCPTable
CopyMCPTable(MCPTable copy)
{
	MCPTable	result;
	Assert(copy->id == TABLEID_MCP);
	result = (MCPTable) CopyTable((Table)copy);

	result->dump_recno = copy->dump_recno;
	result->req_satisfied = copy->req_satisfied;

	memcpy(result->on_slave, copy->on_slave, MCP_MAX_SLAVES);
	memcpy(result->slave_req, copy->slave_req, MCP_MAX_SLAVES);

	return result;
}


