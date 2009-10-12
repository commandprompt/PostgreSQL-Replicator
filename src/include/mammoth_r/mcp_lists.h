/*-------------------------------------------------------------------------
* mcp_lists.h
*      Prototypes for Mammoth Replicator TableList functions.
*
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
* Copyright (c) 2006, Command Prompt, Inc.
*
* $Id$
*
* -------------------------------------------------------------------------
*/
#ifndef MCP_LISTS_H
#define MCP_LISTS_H

#include "nodes/pg_list.h"

extern void	   *TableListEntry(List *list, void *b);
extern void	   *TableListEntryByName(List *list, char *name);

extern List	   *RestoreTableList(char *path);
extern void		StoreTableList(char *path, List *tablist);

#endif /* MCP_LISTS_H */
