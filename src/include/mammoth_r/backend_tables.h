/* -------------------------------------------------------------------------
 * backend_tables.h
 * 		Function interface for Replicator relation name
 * 		functions the backend process.
 *
 * $Id: backend_tables.h 1695 2008-04-09 17:27:29Z alexk $
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BACKEND_TABLE_H_
#define __BACKEND_TABLE_H_

#include "common_tables.h"

typedef enum TableDumpFlag
{
	TableNoDump = 0,
	TableDump = 1,
	TableExpired = 2
} TableDumpFlag;

BackendTable MakeBackendTable(char *relpath, TableDumpFlag flag);

BackendTable CopyBackendTable(BackendTable copy);

void SetTableDump(BackendTable tab, TableDumpFlag flag);

TableDumpFlag GetTableDump(BackendTable tab);

#endif

