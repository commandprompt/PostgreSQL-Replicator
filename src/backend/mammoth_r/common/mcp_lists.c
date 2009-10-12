/*-------------------------------------------------------------------------
* mcp_lists.c
*      Implementation for Mammoth Replicator TableList functions
*
* Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
* Copyright (c) 2006, Command Prompt, Inc.
*
* $Id: mcp_lists.c 2115 2009-04-28 15:16:11Z alexk $
*
* -------------------------------------------------------------------------
*/
#include "postgres.h"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "mammoth_r/mcp_file.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/common_tables.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"

/*
 * Check if the given table list contains an entry with the name of the table
 * argument.  Returns said element if it exists, or NULL otherwise.
 */
void *
TableListEntry(List *list, void *b)
{
	ListCell *cell;

	if (b == NULL)
		return NULL;

	foreach(cell, list)
	{
		Table	tab = lfirst(cell);

		if (strncmp(tab->relpath, ((Table) b)->relpath, MAX_REL_PATH) == 0)
			return tab;
	}
	return NULL;
}

/* 
 * Variation of the previous routine that receives a string table name instead
 * of the complete table data.
 */
void *
TableListEntryByName(List *list, char *name)
{
	ListCell *cell;

	foreach(cell, list)
	{
		Table	tab = lfirst(cell);

		if (strncmp(tab->relpath, name, MAX_REL_PATH) == 0)
			return tab;
	}

	return NULL;
}

/*
 * Reads a table list from the given file and returns it.
 *
 * The "path" argument is the filename which contains the table list.
 */
List *
RestoreTableList(char *path)
{
	List	   *tablist = NIL;
	MCPFile    *mf_list = MCPFileCreate(path);

	elog(DEBUG5, "restore table list from %s", path);

	/*
	 * If file exists, read its contents.  If anything weird happens while
	 * reading the list, remove it and error out; we expect that a subsequent
	 * iteration will find the file missing and act accordingly.
	 */
	if (MCPFileExists(mf_list))
	{
		PG_TRY();
		{
			int		i;
			int		t_count;

			/* read the table count */
			MCPFileRead(mf_list, &t_count, sizeof(int), false);

			/* read each table */
			for (i = 0; i < t_count; i++)
			{
				Table	current;

				current = ReadTable(mf_list);
				tablist = lappend(tablist, current);
			}
		}
		PG_CATCH();
		{
			MCPFileClose(mf_list);
			MCPFileUnlink(mf_list);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	MCPFileDestroy(mf_list);
	return tablist;
}

/*
 * Save the table list to the given file, in the format described above.
 * Note: to avoid partial writes we append data to the temporary file
 * and rename it to the table list file name only at the end of all writes.
 */
void
StoreTableList(char *path, List *tablist)
{
	int			len;
	char		*tmp_path;
	ListCell	*cell;
	MCPFile		*mf_list;

	elog(DEBUG5, "store table list in %s", path);

	/* Make temporary file */
	tmp_path = palloc(strlen(path) + 5);  /* strlen("_tmp") +1 */
	sprintf(tmp_path, "%s_tmp", path);

	mf_list = MCPFileCreate(tmp_path);
	MCPFileCreateFile(mf_list);

	len = list_length(tablist);
	MCPFileWrite(mf_list, &len, sizeof(len));

	foreach (cell, tablist)
	{
		Table tab = lfirst(cell);

		StoreTable(mf_list, tab);		
	}
	/* Rename the list file to its final name */
	MCPFileRename(mf_list, path);
	MCPFileDestroy(mf_list);
}
