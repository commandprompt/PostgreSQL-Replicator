/*-------------------------------------------------------------------------
* common_tables.c
*      Implementation for generic relation name tables.
*
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
* Copyright (c) 2006, Command Prompt, Inc.
*
* $Id: common_tables.c 1794 2008-07-04 14:24:33Z alexk $
*
* -------------------------------------------------------------------------
*/


#include "postgres.h"
#include "mammoth_r/mcp_file.h"
#include "mammoth_r/common_tables.h"
#include <unistd.h>


static void StoreBackendSpecific(MCPFile *mcpf, BackendTable tab);
static void StoreMCPSpecific(MCPFile *mcpf, MCPTable tab);
static void ReadBackendSpecific(MCPFile *mcpf, BackendTable tab);
static void ReadMCPSpecific(MCPFile *mcpf, MCPTable tab);

static int TableSize[TABLE_TYPES_NO] = {
    sizeof(TableData),
    sizeof(BackendTableData),
    sizeof(MCPTableData)
};


size_t
GetTableSize(TableId tab_id)
{
	if (tab_id < 0 && tab_id >= TABLE_TYPES_NO)
		elog(ERROR, "Can't get size for table id: %d", tab_id);

	return	TableSize[tab_id];
}

/* Check if 2 table are equal */
bool
TablesEqual(Table a, Table b)
{
	if (a == NULL || b == NULL)
	{
		return a == b;
	}
	else
		return (strncmp(a->relpath, b->relpath, MAX_REL_PATH) == 0);
}

/*
 * Create new Table from name and raise_dump flag.  We allocate both the
 * TableData node and the name string in a single palloc chunk, so that it's
 * easier to free (and also has only one palloc header instead of two).
 */
Table
MakeTable(char *relpath, TableId tab_id)
{
	Table	newtable;

	newtable = palloc0(GetTableSize(tab_id));
	newtable->id = tab_id;

	memcpy(newtable->relpath, relpath, strlen(relpath));

	return newtable;
}

Table
CopyTable(Table copy)
{
	Table	newtable;
	size_t	sz_newtable = GetTableSize(copy->id);

	newtable = (Table) palloc(sz_newtable);
	memcpy(newtable, copy, sz_newtable);
	return newtable;
}

/* This can be extended to display specific information about the table */
void
ShowTable(int elevel, Table tab)
{
	elog(elevel, "Table %s type %s", tab->relpath, TableIdAsString(tab->id));
}

void
StoreTable(MCPFile *mcpf, Table tab)
{
	uint16		len;

	len = strlen(tab->relpath);
    Assert(len > 0);

    ShowTable(DEBUG5, tab);

	/* write type flag */
	MCPFileWrite(mcpf, &(tab->id), sizeof(uint8));
	/* write relpath length */
	MCPFileWrite(mcpf, &len, sizeof(uint16)); 
	/* write table relpath */
	MCPFileWrite(mcpf, tab->relpath, len);

	/* write the rest of the table's data depeding on a table type */
	switch(tab->id)
	{
		case TABLEID_BACKEND:
			StoreBackendSpecific(mcpf, (BackendTable) tab);
			break;
		case TABLEID_MCP:
			StoreMCPSpecific(mcpf, (MCPTable) tab);
            break;
		default:
			elog(ERROR, "unrecognized table type %d", tab->id);
	}
}

static void
StoreBackendSpecific(MCPFile *mcpf, BackendTable tab)
{
	MCPFileWrite(mcpf, &(tab->raise_dump), sizeof(uint8));
}

static void
StoreMCPSpecific(MCPFile *mcpf, MCPTable tab)
{
	MCPFileWrite(mcpf, &(tab->dump_recno), sizeof(ullong));
	MCPFileWrite(mcpf, &(tab->req_satisfied), sizeof(uint8));
	
	MCPFileWrite(mcpf, tab->on_slave, sizeof(bool) * MCP_MAX_SLAVES);
	MCPFileWrite(mcpf, tab->slave_req, sizeof(bool) * MCP_MAX_SLAVES);
}

/*
 * ReadTable
 * 		Read one table element from the given file
 *
 * The format is:
 * uint8		type
 * uint16		relpath length
 * variable		relpath
 * variable		type-specific bits
 */
Table
ReadTable(MCPFile *mcpf)
{
	uint16	len;
	uint8	id;
	char   *relpath;
	Table	result;

	/* Read the type of the table */	
	MCPFileRead(mcpf, &id, sizeof(uint8), false);

	/* Read relpath length */
	MCPFileRead(mcpf, &len, sizeof(uint16), false);
	if (len == 0 || len > MAX_REL_PATH)
		ereport(ERROR,
				(errmsg("table list relpath length outside boundaries 1..%d",
						MAX_REL_PATH)));

	/* Alloc and read the relpath */
	relpath = palloc0(len + 1);
	MCPFileRead(mcpf, relpath, len, false);

	/* Make a new table of the given type */
	result = MakeTable(relpath, id);

	/* Read the rest of the table's data */
	switch (result->id)
	{
		case TABLEID_BACKEND:
			ReadBackendSpecific(mcpf, (BackendTable) result);
			break;
		case TABLEID_MCP:
			ReadMCPSpecific(mcpf, (MCPTable) result);
			break;
		default:
			elog(ERROR, "unrecognized table type %d", result->id);
	}

	pfree(relpath);
    ShowTable(DEBUG5, result);
	return result;
}

static void
ReadBackendSpecific(MCPFile *mcpf, BackendTable tab)
{
	MCPFileRead(mcpf, &(tab->raise_dump), sizeof(uint8), false);
}

static void
ReadMCPSpecific(MCPFile *mcpf, MCPTable tab)
{
	MCPFileRead(mcpf, &(tab->dump_recno), sizeof(ullong), false);
	MCPFileRead(mcpf, &(tab->req_satisfied), sizeof(uint8), false);
	
	MCPFileRead(mcpf, tab->on_slave, sizeof(bool) * MCP_MAX_SLAVES, false);
	MCPFileRead(mcpf, tab->slave_req, sizeof(bool) * MCP_MAX_SLAVES, false);
}
