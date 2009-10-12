/*-------------------------------------------------------------------------
* common_tables.h
*      Prototypes for Mammoth Replicator Table functions
*
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
* Copyright (c) 2006, Command Prompt, Inc.
*
* $Id: common_tables.h 2089 2009-03-27 16:22:19Z alvherre $
*
* -------------------------------------------------------------------------
*/

#ifndef COMMON_TABLES_H
#define COMMON_TABLES_H

#include "mammoth_r/mcp_file.h"
#include "mammoth_r/repl_limits.h"
#include "pg_config_manual.h"


/* Maximum length of the relation name path (namespace + name) */
#define MAX_REL_PATH (NAMEDATALEN * 2 + 1)

/* common table */
typedef struct TableData
{
	uint8	id;
	char	relpath[MAX_REL_PATH];
	char	data[1];
} TableData;

typedef struct TableData *Table;

/* backend table */
typedef struct BackendTableData
{
	uint8	id;
	char	relpath[MAX_REL_PATH];
	uint8	raise_dump;
} BackendTableData;

typedef struct BackendTableData *BackendTable;

/* Like BackendTable, but used for network transfers */
typedef struct WireBackendTableData
{
	uint8	raise_dump;
	char	relpath[1];		/* VARIABLE LENGTH DATA */
} WireBackendTableData;
typedef struct WireBackendTableData *WireBackendTable;

/* MCP table */
typedef struct MCPTableData
{
	uint8	id;
	char	relpath[MAX_REL_PATH];
	ullong	dump_recno; 	/* recno of the latest dump for this table */
	uint8	req_satisfied; /* whether the dump for this table is in the queue */
	bool	on_slave[MCP_MAX_SLAVES]; /* which slave are replicating this table*/
	bool	slave_req[MCP_MAX_SLAVES]; /* which slaves waits for the dump */
} MCPTableData;

typedef struct MCPTableData *MCPTable;

#define TABLE_TYPES_NO	3

typedef enum TableId
{
	TABLEID_GENERAL = 0,
	TABLEID_BACKEND = 1,
	TABLEID_MCP = 2
} TableId;

#define TableIdAsString(TableId) ((TableId == TABLEID_GENERAL ? "General" : \
								  (TableId == TABLEID_BACKEND) ? "Backend" : \
								  (TableId == TABLEID_MCP) ? "MCP" : "Unknown"))

extern size_t	GetTableSize(TableId id);
extern bool		TablesEqual(Table a, Table b);
extern Table	MakeTable(char *relpath, TableId tab_id);
extern Table	CopyTable(Table copy);
extern void		ShowTable(int elevel, Table tab);
extern void		StoreTable(MCPFile *mcpf, Table tab);
extern Table	ReadTable(MCPFile *mcpf);

#endif /* COMMON_TABLES_H */
