/*-------------------------------------------------------------------------
* backend_tables.c
*		Implementation for Replicator relation name table
*		functions in the backend process.
*
* $Id: backend_tables.c 1695 2008-04-09 17:27:29Z alexk $
*
* -------------------------------------------------------------------------
*/

#include "postgres.h"
#include "mammoth_r/common_tables.h"
#include "mammoth_r/backend_tables.h"

BackendTable
MakeBackendTable(char *relpath, TableDumpFlag flag)
{   
    BackendTable result = (BackendTable) MakeTable(relpath, TABLEID_BACKEND);
    result->raise_dump = flag;
    
    return result;
}   

BackendTable
CopyBackendTable(BackendTable copy)
{   
    BackendTable    result;
    Assert(copy->id == TABLEID_BACKEND);

    result = (BackendTable) CopyTable((Table) copy);
    result->raise_dump = copy->raise_dump;
    
    return result;
}   

void
SetTableDump(BackendTable tab, TableDumpFlag flag)
{   
    Assert(tab->id == TABLEID_BACKEND);
    tab->raise_dump = flag;

}

TableDumpFlag
GetTableDump(BackendTable tab)
{
    Assert(tab->id == TABLEID_BACKEND);
    return tab->raise_dump;
}

