/* -----------------------
 * mcp_helper.c
 *
 * This is the forwarder helper process.  It is in charge of running
 * queue maintenance and other background tasks in a forwarder node.
 *
 * $Id: mcp_helper.c 2186 2009-06-25 12:14:51Z alexk $
 * -----------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <unistd.h>

#include "libpq/pqsignal.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/txlog.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/ps_status.h"


static void do_queue_optimization(MCPQueue *q, MCPHosts *h);
static void advance_safe_recno(MCPHosts *h);
static void sigquit_handler(SIGNAL_ARGS);
static void sigterm_handler(SIGNAL_ARGS);

static bool terminate = false;

void
ForwarderHelperMain(int argc, char *argv)
{
	MCPQueue   *q;
	MCPHosts   *h;
	int			rounds_to_optimize = ForwarderOptimizerRounds;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	/* reset MyProcPid */
	MyProcPid = getpid();
	
	/* Lose the postmaster's on-exit routines */
	on_exit_reset();
	
	/* identify myself via ps */
	init_ps_display("forwarder helper process", "", "", "");
	set_ps_display("", true);

	pqsignal(SIGQUIT, sigquit_handler);
	pqsignal(SIGTERM, sigterm_handler);
	pqsignal(SIGINT, SIG_IGN);

	/*
     * Create a dummy PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitAuxiliaryProcess();
#endif

	/* now we can get signalled */
	PG_SETMASK(&UnBlockSig);

	/* initialize the queue */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	q = MCPQueueInit(true);
	h = MCPHostsInit();
	LWLockRelease(MCPServerLock);

	/* switch to the forwarder txlog and start it up */
	SelectActiveTxlog(true);
	LockReplicationQueue(q, LW_SHARED);
	TXLOGStartup(MCPQueueGetFirstRecno(q));
	UnlockReplicationQueue(q);

	for (;;)
	{
		/*
		 * Reduce the number of loop rounds until the queue optimization 
		 * can be checked and probably fired
		 */
		rounds_to_optimize--;

		if (terminate)
			break;

		if (rounds_to_optimize <= 0)
		{
			do_queue_optimization(q, h);

			/* Restore optimization counter */
			rounds_to_optimize = ForwarderOptimizerRounds;
		}

		advance_safe_recno(h);

		if (terminate)
			break;

		/* sleep for a bit */
		pg_usleep(ForwarderOptimizerNaptime * 1000000L);
	}
	
	WriteForwarderStateFile();	
	proc_exit(0);
}

static void
do_queue_optimization(MCPQueue *q, MCPHosts *h)
{
	ullong 	new_recno;
	bool	doit = false;

	elog(DEBUG3, "running optimization checks");

	MCPQueueLogHdrStatus(DEBUG4, q, "PRE OPTIMIZE");
	MCPHostsLogTabStatus(DEBUG4, h, -1, "PRE OPTIMIZE", ServerCtl->node_pid);

	/*
	 * Get the minimum among confirmed recnos of connected slaves, and set
	 * the queue's AckRecno and FirstRecno to it.
	 */
	LWLockAcquire(MCPServerLock, LW_SHARED);
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	new_recno = MCPHostsGetMinAckedRecno(h, ServerCtl->node_pid);
	LWLockRelease(MCPHostsLock);
	LWLockRelease(MCPServerLock);

	if (new_recno != InvalidRecno)
	{
		LockReplicationQueue(q, LW_EXCLUSIVE);
		MCPQueueSetAckRecno(q, new_recno);
		MCPQueueSetFirstRecno(q, new_recno + 1);
		UnlockReplicationQueue(q);
		doit = true;
	}

	/* Remove records */
	if (doit)
	{
		LockReplicationQueue(q, LW_EXCLUSIVE);
		MCPQueuePrune(q);
		UnlockReplicationQueue(q);
	}

	MCPQueueLogHdrStatus(DEBUG4, q, "POST OPTIMIZE");
	MCPHostsLogTabStatus(DEBUG4, h, -1, "POST OPTIMIZE", ServerCtl->node_pid);
}

/*
 * Try to advance the global safe-to-ack recno counter.  This lets the
 * master process send an updated ACK message to the master node.
 */
static void
advance_safe_recno(MCPHosts *h)
{
	int		i;
	ullong	safe;
	bool	signal_master = false;

	LWLockAcquire(MCPServerLock, LW_SHARED);
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	safe = InvalidRecno;
	for (i = 0; i < MCPHostsGetMaxHosts(h); i++)
	{
		ullong	acked;

		/* ignore unconnected slaves */
		if (ServerCtl->node_pid[i + 1] == 0)
			continue;

		acked = MCPHostsGetHostRecno(h, McphHostRecnoKindAcked, i);
		/*
		 * If a slave hasn't set an acked recno, don't advance the global
		 * counter
		 */
		if (acked == InvalidRecno)
		{
			safe = InvalidRecno;
			break;
		}
		if (safe == InvalidRecno)
			safe = acked;
		else if (acked < safe)
			safe = acked;
	}
	LWLockRelease(MCPServerLock);

	if (safe != InvalidRecno &&
		safe > MCPHostsGetRecno(h, McphRecnoKindSafeToAck))
	{
		LWLockRelease(MCPHostsLock);
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		MCPHostsSetRecno(h, McphRecnoKindSafeToAck, safe);
		signal_master = true;
	}
	LWLockRelease(MCPHostsLock);

	/* need to wake up the master process so that it sees our state changes */
	if (signal_master)
	{
		LWLockAcquire(MCPServerLock, LW_SHARED);
		WakeupMaster();
		LWLockRelease(MCPServerLock);
	}
}

static void
sigquit_handler(SIGNAL_ARGS)
{
	exit(1);
}

static void
sigterm_handler(SIGNAL_ARGS)
{
	elog(LOG, "forwarder helper shutting down");
	terminate = true;
}
