/*-----------------------
 * promotion.c
 * 		The Mammoth Replication promotion implementation
 *
 * This code is in charge of initiating and controlling promotion, both on the
 * master side and on the slave side.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: promotion.c 2115 2009-04-28 15:16:11Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "mammoth_r/agents.h"
#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/promotion.h"
#include "mammoth_r/signals.h"
#include "miscadmin.h"
#include "postmaster/replication.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/pmsignal.h"
#include "utils/guc.h"


/*
 * Promotion shared memory data
 */

ReplicationPromotionData   *ReplPromotionData;

static void write_promotion_file(ReplicationMode mode, uint32 slaveno,
								 bool force);

/*
 * BackendInitiatePromotion
 *
 * Act on receiving a PROMOTE command.
 *
 * Note that this is executed by the backend, not the replicator process.  So
 * we just set the needed flags in shared memory and signal the postmaster,
 * which will in turn signal the replication process.  (We cannot signal the
 * replication process ourselves because we don't know its true PID -- it may
 * have changed after this backend started.)
 */
void
BackendInitiatePromotion(PromoteStmt *stmt)
{
	bool in_progress = true;
	
	LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

	if (!ReplPromotionData->promotion_in_progress)
	{
		/* Set the promotion flags in shared memory */
		ReplSharedSignalData->promotion = true;
		ReplPromotionData->force_promotion = stmt->force;
		ReplPromotionData->back_promotion = stmt->back;
		in_progress = false;
	}
	LWLockRelease(ReplicationLock);
	
	/* Better do it here when lock is released */
	if (in_progress)
	{
		elog(WARNING, "Promotion is already in progress");
		return;
	}

	/* Signal the postmaster so that it'll signal the replicator process */
	SendPostmasterSignal(PMSIGNAL_REPLICATOR);

	elog(NOTICE, "promotion process initiated");
}

/*
 * SlaveInitiatePromotion
 *
 * Start moving the wheels for an upcoming promotion.
 *
 * This function is run by the slave replication process when a promotion
 * signal has been received in the recent past.  This is a safe place to run,
 * i.e. the slave is not currently processing a data stream.
 */
void
SlaveInitiatePromotion(bool force)
{
	MCPMsg	*sm;

	sm = palloc0(sizeof(MCPMsg) + sizeof(bool) - 1);

	Assert(IsReplicatorProcess() && replication_enable && replication_slave);

	elog(LOG, "%spromotion initiated", force ? "forced " : "");

	sm->flags |= MCP_MSG_FLAG_PROMOTE_NOTIFY;
	memcpy(sm->data, &force, sizeof(bool));
	sm->datalen = sizeof(bool);

	MCPSendMsg(sm, true);
	MCPMsgPrint(DEBUG3, "Slave-SendMsg", sm);

	pfree(sm);
}

void
InitiateBackPromotion(void)
{
	MCPMsg sm;

	Assert(IsReplicatorProcess() && replication_enable);

	elog(LOG, "back promotion initiated");

	MemSet(&sm, 0, sizeof(MCPMsg));
	sm.flags |= MCP_MSG_FLAG_PROMOTE_BACK;

	MCPSendMsg(&sm, true);

	MCPMsgPrint(DEBUG3, "SendMsg", &sm);
}


void
SlaveCompletePromotion(bool force)
{
	Assert(IsReplicatorProcess() && replication_enable && replication_slave);

	elog(LOG, "finishing promotion process");

	write_promotion_file(REPLICATION_MODE_SLAVE, replication_slave_no, force);

	/* all is well */
	proc_exit(REPLICATOR_EXIT_SLAVE_PROMOTION);
}

void
MasterCompletePromotion(void)
{
	/* 
	 * Note: promotion_in_progress would be cleared at start of the 
	 * new agent process after promotion.
	 */
	Assert(IsReplicatorProcess() && replication_enable && replication_master);

	elog(LOG, "finishing promotion process");

	write_promotion_file(REPLICATION_MODE_MASTER, 0, false);

	proc_exit(REPLICATOR_EXIT_MASTER_DEMOTION);
}

static void
write_promotion_file(ReplicationMode mode, uint32 slaveno, bool force)
{
	bool	master;
	int		fd;
	char	path[MAXPGPATH];
	int		ret;
	off_t	off;
	struct	stat st_buf;

	Assert(mode == REPLICATION_MODE_MASTER || mode == REPLICATION_MODE_SLAVE);

	master = mode == REPLICATION_MODE_MASTER;
		
	snprintf(path, MAXPGPATH, "%s/%s", DataDir, PROMOTION_FILE_NAME);
	
	/* Check if the path points to a symlink to prevent a possible
	 * security issue in overwriting the symlinked file
	 */
	ret = stat(path, &st_buf);
	if (ret == 0)
	{	
		if (S_ISLNK(st_buf.st_mode))
			elog(ERROR, "promotion file couldn't be a symlink");
	}
	else if (errno != ENOENT)
		elog(ERROR, "couldn't stat %s", path);
	
	fd = open(path, O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	if (fd < 0)
		elog(ERROR, "could not open %s: %m", path);
	
	/* we write at the end of the file */
	off = lseek(fd, 0, SEEK_END);
	if (off < 0)
		elog(ERROR, "could not seek to end of file: %m");

	ret = write(fd, &master, sizeof(bool));
	if (ret != sizeof(bool))
	{
		if (ret < 0)
			elog(ERROR, "could not write to %s: %m", path);
		elog(ERROR, "attempted to write %d bytes to %s, wrote only %d",
			 (int) sizeof(bool), path, ret);
	}
	ret = write(fd, &force, sizeof(bool));
	if (ret != sizeof(bool))
	{
		if (ret < 0)
			elog(ERROR, "could not write to %s: %m", path);
		elog(ERROR, "attempted to write %d bytes to %s, wrote only %d",
			 (int) sizeof(bool), path, ret);
	}
	ret = write(fd, &slaveno, sizeof(uint32));
	if (ret != sizeof(uint32))
	{
		if (ret < 0)
			elog(ERROR, "could not write to %s: %m", path);
		elog(ERROR, "attempted to write %d bytes to %s, wrote only %d",
			 (int) sizeof(uint32), path, ret);
	}
	ret = close(fd);
	if (ret < 0)
		elog(ERROR, "could not close %s: %m", path);
}

/*
 * process_promotion_file
 *
 * Read the promotion file, and determine whether this node should be promoted
 * or not.  If so, do it by setting the GUC variables appropiately.  Naturally,
 * this must be done in the postmaster initialization _before_ starting the
 * replication process.
 */
void
process_promotion_file(void)
{
	char	path[MAXPGPATH];
	int		fd;
	uint32	slaveno;
	uint32	dest;
	int		ret;
	off_t	off;
	bool	force;
	bool	master;

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, PROMOTION_FILE_NAME);
	fd = open(path, O_RDONLY);
	if (fd < 0)
	{
		/* if the file doesn't exist, do nothing.  Any other error is fatal */
		if (errno == ENOENT)
			return;
		elog(PANIC, "could not open %s: %m", path);
	}

	/*
	 * FIXME -- we should write a "version" header or something, to avoid
	 * reading files written in different layouts, etc
	 */

	/*
	 * offset to go to is the size of the stuff we need to read, from the end
	 * of the file
	 */
	dest = sizeof(uint32) + sizeof(bool) + sizeof(bool);
	off = lseek(fd, - (off_t) dest, SEEK_END);
	if (off < 0)
	{
		/*
		 * the file has less bytes than we need to read -- ignore it (and
		 * remove it to avoid leaving junk behind)
		 */
		if (errno == EINVAL)
		{
			unlink(path);
			return;
		}
		
		/* any other error is fatal */
		elog(PANIC, "could not seek to %u: %m", dest);
	}

	ret = read(fd, (void *) &master, sizeof(bool));
	if (ret != sizeof(bool))
		elog(FATAL, "could not read %d bytes from %s", (int) sizeof(bool), path);
	ret = read(fd, (void *) &force, sizeof(bool));
	if (ret != sizeof(bool))
		elog(FATAL, "could not read %d bytes from %s", (int) sizeof(bool), path);
	ret = read(fd, (void *) &slaveno, sizeof(uint32));
	if (ret != sizeof(uint32))
		elog(FATAL, "could not read %d bytes from %s", (int) sizeof(uint32), path);

	/*
	 * if we got this far, it means there was a promotion in effect when the
	 * postmaster was shut down.
	 */
	if (master)
	{
		char	slave_no[16];

		/* it was a master, so make it a slave */
		snprintf(slave_no, 16, "%d", slaveno);

		SetConfigOption("replication_mode", "slave", PGC_POSTMASTER, PGC_S_OVERRIDE);
		SetConfigOption("replication_slave_no", slave_no, PGC_POSTMASTER, PGC_S_OVERRIDE);
	}
	else
	{
		/* it was a slave, so promote it to master */
		SetConfigOption("replication_mode", "master", PGC_POSTMASTER, PGC_S_OVERRIDE);
	}

	ret = close(fd);
	if (ret < 0)
		elog(ERROR, "could not close %s: %m", path);

}
