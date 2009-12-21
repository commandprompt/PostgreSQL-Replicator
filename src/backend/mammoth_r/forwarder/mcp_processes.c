/* 
 * mcp_processes.c
 *
 * Common stuff for MCP subprocesses.
 * 
 * $Id: mcp_processes.c 2227 2009-09-22 07:04:32Z alexk $
 */ 
#include "postgres.h"

#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_promotion.h"
#include "mammoth_r/paths.h"
#include "storage/shmem.h"

#define STATE_FILE_PATH MAMMOTH_FORWARDER_DIR"/forwarder_state"
#define STATE_FILE_VERSION 2009040801

/* Pointers to structures in shared memory */ 
MCPDumpVars			*DumpCtl = NULL;
MCPControlVars		*ServerCtl = NULL;

bool	sigusr1_rcvd = false;
bool	promotion_signal = false;
bool 	run_encoding_check = false;

/* Global variables init functions */
static void 
MCPInitDumpVars(bool attach)
{
	bool	found;
	MCPDumpVars *vars;
	
/* XXX: EXEC_BACKEND case is missing */
#ifndef EXEC_BACKEND
	if (attach)
		return;
#endif

	vars = (MCPDumpVars *)
		ShmemInitStruct("DumpVars", sizeof(MCPDumpVars), &found);
	Assert(attach == found);
	
	MemSet(vars, 0, sizeof(MCPDumpVars));
	vars->mcp_dump_progress = FDnone;
	
	DumpCtl = vars;
}

static void 
MCPInitControlVars(bool attach)
{
	bool	found;
	MCPControlVars	*vars;

/* XXX: EXEC_BACKEND case is missing */
#ifndef EXEC_BACKEND
	if (attach)
		return;
#endif
	
	vars = (MCPControlVars *)
		ShmemInitStruct("ControlVars", sizeof(MCPControlVars), &found);
	Assert(attach == found);

	MemSet(vars, 0, sizeof(MCPControlVars));
	
	ServerCtl = vars;
}

void
MCPInitVars(bool allocate)
{
	MCPInitDumpVars(!allocate);
	MCPInitPromotionVars(!allocate);
	MCPInitControlVars(!allocate);
}

/* Full dump progress functions */
void
FullDumpSetProgress(FullDumpProgress val)
{	
	Assert(LWLockHeldByMe(MCPServerLock));
	
	if (DumpCtl->mcp_dump_progress != val)
	{
		elog(DEBUG2, "FULL DUMP progress flag %s -> %s",
			 FDProgressAsString(DumpCtl->mcp_dump_progress), 
			 FDProgressAsString(val));
		DumpCtl->mcp_dump_progress = val;
	}
}

FullDumpProgress
FullDumpGetProgress(void)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	return DumpCtl->mcp_dump_progress;
}

/* Table dump request functions */
bool
TableDumpIsRequested(void)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	return DumpCtl->table_dump_request;
}

void
TableDumpSetRequest(bool request)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	elog(DEBUG2, "TABLE DUMP request set to: %d", (int)request);
	DumpCtl->table_dump_request = request;
}

/* FULL DUMP recno functions */
void
FullDumpSetStartRecno(ullong recno)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	elog(DEBUG2, "FULL DUMP recno: "UINT64_FORMAT" -> "UINT64_FORMAT,
		 DumpCtl->mcp_dump_start_recno, recno);
	DumpCtl->mcp_dump_start_recno = recno;
}

void
FullDumpSetEndRecno(ullong recno)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	DumpCtl->mcp_dump_end_recno = recno;
}

ullong
FullDumpGetStartRecno(void)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	return DumpCtl->mcp_dump_start_recno;
}

ullong
FullDumpGetEndRecno(void)
{
	Assert(LWLockHeldByMe(MCPServerLock));
	return DumpCtl->mcp_dump_end_recno;
}

/*
 * callback for MCPQueue events
 */
static void
FullDumpQueueCallbk(MCPQevent event, ullong recno, void *arg)
{
	/* Reset the dump recnos if they are removed by queue pruning */
	if (event == MCPQ_EVENT_PRUNE)
	{
		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		if (DumpCtl->mcp_dump_start_recno < recno)
			DumpCtl->mcp_dump_start_recno = InvalidRecno;
		if (DumpCtl->mcp_dump_end_recno < recno)
			DumpCtl->mcp_dump_end_recno = InvalidRecno;
		LWLockRelease(MCPServerLock);
	}
}

void
RegisterFullDumpQueueCallback(MCPQueue *q)
{
	MCPQRegisterCallback(q, FullDumpQueueCallbk, NULL);
}

/* Write dump variables into the forwarder state file */
void
WriteForwarderStateFile(void)
{
	MCPFile    	*mf_state; 
	char 	   	*filename, 
				*tmp_filename;
	MCPDumpVars *localDumpCtl;
	uint32		version;
	
	elog(DEBUG3, "Writing forwarder state file");
	
	filename = STATE_FILE_PATH;
	tmp_filename = palloc(strlen(filename) + 5);
	snprintf(tmp_filename, strlen(filename) + 5, "%s_tmp", filename);
	
	/* Create a temporary file */
	mf_state = MCPFileCreate(tmp_filename);
	MCPFileCreateFile(mf_state);
	MCPFileOpen(mf_state);
	
	/* Write current state file version */
	version = STATE_FILE_VERSION;
	MCPFileWrite(mf_state, &version, sizeof(version));
	
	/* Get a local copy of dump variables to avoid holding a lock during IO */
	localDumpCtl = palloc(sizeof(MCPDumpVars));
	
	LWLockAcquire(MCPServerLock, LW_SHARED);
	memcpy(localDumpCtl, DumpCtl, sizeof(MCPDumpVars));
	LWLockRelease(MCPServerLock);
	
	MCPFileWrite(mf_state, localDumpCtl, sizeof(MCPDumpVars));
	
	/* Rename temporary file to the state file name */
	MCPFileRename(mf_state, filename);
	
	/* Close files and release memory */
	MCPFileClose(mf_state);
	MCPFileDestroy(mf_state);
	
	pfree(tmp_filename);
	pfree(localDumpCtl);
}

/* Read dump variables from the forwarder state file */
void
ReadForwarderStateFile(void)
{
	MCPFile    	*mf_state;
	char 	   	*filename;
	MCPDumpVars *localDumpCtl;
	
	elog(DEBUG3, "Reading forwarder state file");
	
	filename = STATE_FILE_PATH;
	localDumpCtl = palloc(sizeof(MCPDumpVars));
	
	/* Create MCPFile object and open the state file */
	mf_state = MCPFileCreate(filename);
	if (MCPFileExists(mf_state))
	{
		uint32 	version;
		size_t	toread;
		bool 	warn = false;

		MCPFileOpen(mf_state);
		if (!MCPFileRead(mf_state, &version, sizeof(uint32), true))
		{
			warn = true;
			toread = sizeof(uint32);
		}
		/* Check version information */
		else if (version != STATE_FILE_VERSION)
			ereport(WARNING,
					(errmsg("Wrong version of MCP dump state file"),
					 errdetail("current version %u, found %u",
							   STATE_FILE_VERSION, version)));
		else if (!MCPFileRead(mf_state, localDumpCtl, sizeof(MCPDumpVars), true))
		{
			warn = true;
			toread = sizeof(MCPDumpVars);
		}
		else
		{
			/* Set DumpCtl variables with the data read from state file */
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
			memcpy(DumpCtl, localDumpCtl, sizeof(MCPDumpVars));
			LWLockRelease(MCPServerLock);
		}

		/*
		 * Report a warning if necessary; we also save the file under another name
		 * for possible future examination.
		 */
		if (warn)
		{
			char    *debug_filename;

			ereport(WARNING,
					(errmsg("EOF received when reading %lu bytes at offset "UINT64_FORMAT" from %s", 
							(unsigned long) toread,
							(ullong) MCPFileSeek(mf_state, 0, SEEK_CUR),
							filename),
					 errhint("%s is probably corrupted", filename)));

			/* Append _debug to a file name for futher examination by user */
			debug_filename =  palloc(strlen(filename) + 7);
			snprintf(debug_filename, strlen(filename) + 7, "%s_debug", filename);

			MCPFileRename(mf_state, debug_filename);
			pfree(debug_filename);	
		}
		/* close the state file */
		MCPFileClose(mf_state);
	}
	/* release memory */
	MCPFileDestroy(mf_state);

	pfree(localDumpCtl);
}

/* Delete forwarder state file if it exists */
void
RemoveForwarderStateFile(void)
{
	MCPFile *mf_state;
	char    *filename;
	
	elog(DEBUG3, "Removing forwarder state file");
	
	filename = STATE_FILE_PATH;
	mf_state = MCPFileCreate(filename);

	if (MCPFileExists(mf_state))
		MCPFileUnlink(mf_state);
	
	/* release memory */
	MCPFileDestroy(mf_state);
}
