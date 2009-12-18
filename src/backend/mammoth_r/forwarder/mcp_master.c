/*
 * mcp_master.c
 *		MCP server master process handler
 *
 * $Id: mcp_master.c 2204 2009-07-08 18:46:22Z alvherre $
 */
#include "postgres.h"

#include <signal.h>
#include <time.h>

#include "libpq/pqsignal.h"
#include "mammoth_r/backend_tables.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/fwsignals.h"
#include "mammoth_r/mcp_hosts.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_promotion.h"
#include "mammoth_r/mcp_tables.h"
#include "mammoth_r/txlog.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"


typedef enum
{
	master_promotion_none,
	master_promotion_started,
	master_promotion_wait_ready,
	master_promotion_wait_make,
	master_promotion_send_make,
	master_promotion_cancelled
} MasterPromotion;

typedef struct MasterState
{
	ullong		ms_ack_recno;
	time_t		ms_last_msg_time;
	time_t		ms_check_start_time;
	bool		ms_echo_request;
	MCPQueue   *ms_queue;
	MCPHosts   *ms_hosts;
	List	   *ms_tablelist;
	uint32		current_tablelist_rev;
	MasterPromotion ms_promotion;
} MasterState;

#define MCPMasterPromotionAsString(prom) \
	(((prom) == master_promotion_none) ? "none" : \
	 ((prom) == master_promotion_started) ? "started" : \
	 ((prom) == master_promotion_wait_ready) ? "wait ready" : \
	 ((prom) == master_promotion_wait_make) ? "wait make" : \
	 ((prom) == master_promotion_send_make) ? "send make" : \
	 ((prom) == master_promotion_cancelled) ? "cancelled" : \
	 "unknown")

#define LOG_PROMOTION_STATE(elevel, state) \
	elog(elevel, "MCP Master promotion state: %s", \
		 MCPMasterPromotionAsString(state->ms_promotion))

static void McpMasterLoop(MasterState *state);
static void MCPMasterFinishPromotion(void);
static void MCPMasterCancelPromotion(MasterState *state);
static void MCPMasterStartPromotion(MasterState *state, int prev_slaveno);
static void ReceiveMessageFromMaster(MasterState *state);
static void SendMessagesToMaster(MasterState *state);
static bool ActOnMasterCheckState(MasterState *state);
static void MasterNotifySlaves(MasterState *state);
static void procexit_master_cleanup(int code, Datum arg);
static bool MCPMasterMessageHook(bool committed, MCPMsg *rm, void *state_arg);
static void MCPMasterActOnPromotionSignal(MasterState *state);
static void SlaveNextPromotionState(MasterState *state);
static void MasterRestoreTableList(MasterState *state);
static void MasterStoreTableList(MasterState *state);
static void ReceiveMasterTableList(List *tl_received, TxDataHeader *hdr, 
								   ullong recno, void *arg_state);
static void SendTableDumps(MasterState *state);


void
HandleMasterConnection(MCPQueue *q, MCPHosts *h)
{
	MasterState *state;
	MCPQSync	sync;
	ullong		initial_recno;

	set_ps_display("startup", false);
	
	state = (MasterState *) palloc(sizeof(MasterState));

	state->ms_last_msg_time = time(NULL);
	state->ms_check_start_time = (time_t)0;
	state->ms_queue = q;
	state->ms_hosts = h;
	state->ms_echo_request = false;
	state->ms_ack_recno = 0;

	/* We are not in promotion right after (re)connection. */
	state->ms_promotion = master_promotion_none;

	/* 
	 * Let's check if MCP considers that promotion is still in progress either 
	 * for a master of a slave. If that happens - then we probably  terminated 
	 * abnormally without cleaning correspondent sysids and promotion flags in 
	 * shared memory. The main difference from MCPMasterCancelPromotion call
	 * is that we clear the promotion stack here.
	 */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	PromotionResetAtStartup(0);
	LWLockRelease(MCPServerLock);

	/* Load the table list */
	LWLockAcquire(MCPTableListLock, LW_SHARED);
	MasterRestoreTableList(state);
	LWLockRelease(MCPTableListLock);

	/* register callback to cleanup at finish */
	on_proc_exit(procexit_master_cleanup, PointerGetDatum(state));

	/* now we can get signalled */
	PG_SETMASK(&UnBlockSig);

	/* send initial recno to the master */
	LockReplicationQueue(q, LW_SHARED);
	initial_recno = MCPQueueGetLastRecno(q);
	if (initial_recno != InvalidRecno)
		initial_recno += 1;
	/* fetch initial sync state while we have the lock */
	sync = MCPQueueGetSync(q);
	UnlockReplicationQueue(q);

	MCPSendInitialRecno(initial_recno);

	LOG_PROMOTION_STATE(DEBUG2, state);

	/*
	 * If we are not in sync state, request a dump from the master.  Note:
	 * checking the flag after releasing the lock is safe because we cannot
	 * get sync'ed by a different process.
	 */
	if (sync != MCPQSynced)
	{
		/* simulate receiving REQDUMP from a slave */
		elog(LOG, "queue desynchronized => request FULL DUMP");
		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		FullDumpSetProgress(FDrequested);
		LWLockRelease(MCPServerLock);
	}

	/* go on forever until we're told to close up */
	McpMasterLoop(state);
}

/*
 * Main loop of the master process.  If this ever returns, close shop and go
 * home.
 */
static void
McpMasterLoop(MasterState *state)
{
	while (true)
	{
		int			ret;
		time_t		finish_time;

	
		SendMessagesToMaster(state);

		CHECK_FOR_INTERRUPTS();

		/* Is there something on our input buffer? */
		while (MCPMsgAvailable())
		{
			ReceiveMessageFromMaster(state);
			SendMessagesToMaster(state);

			CHECK_FOR_INTERRUPTS();
		}

		/*
		 * If we need to check the master connection, we can't sleep forever --
		 * the select() must be interrupted at an appropiate time, so that we
		 * can timely inform the slave that it can promote if that's the case.
		 *
		 * XXX we sleep a bit more than we actually need to, so that we are sure
		 * the timeout has elapsed when we are awakened.
		 */
		LWLockAcquire(MCPServerLock, LW_SHARED);
		if (PromotionCtl->master_check_state != master_check_none)
			finish_time = time(NULL) + ForwarderEchoTimeout + 1;
		else
			finish_time = -1;
		LWLockRelease(MCPServerLock);

		set_ps_display("waiting for data", false);

		CHECK_FOR_INTERRUPTS();

		ret = mcpWaitTimed(MyProcPort, true, false, finish_time);

		/* If MCP is terminating - process cleanup and exit */
		if (ServerCtl->mcp_cancel)
			return;

		if (ret == EOF)
		{
			if (errno != EINTR)
				elog(ERROR, "select() failed: %m");
		}
		else if (ret > 0)
		{
			/* The master is talking to us -- listen to it */
			do
			{
				ReceiveMessageFromMaster(state);
				SendMessagesToMaster(state);

				CHECK_FOR_INTERRUPTS();
			}
			while (MCPMsgAvailable());
		}
		else if (ret == 0)
			CHECK_FOR_INTERRUPTS(); /* timeout -- nothing to do */

		if (sigusr1_rcvd)
			sigusr1_rcvd = false;

		/* Act on promotion related signal from the slave */
		if (promotion_signal)
		{
			promotion_signal = false;
			MCPMasterActOnPromotionSignal(state);
			LOG_PROMOTION_STATE(DEBUG2, state);
		}

		if (ActOnMasterCheckState(state))
			return;

	}
}

/*
 * Cleanup routine, used as a proc_exit callback.
 */
static void
procexit_master_cleanup(int code, Datum arg)
{
	MasterState	*state = (MasterState *) DatumGetPointer(arg);

	elog(DEBUG2, "shutting down master (code: %d)", code);
	LOG_PROMOTION_STATE(DEBUG2, state);

	/* Close the queue and save the queue header to a file */
	MCPQueueDestroy(state->ms_queue);

	/* Close hosts and save the hosts header as well */
	MCPHostsClose(state->ms_hosts);
}

/*
 * Take appropiate action when a slave is querying when it wants to do a
 * PROMOTE FORCE.
 *
 * Returns true when this process should terminate, i.e., when the master does
 * not respond in ForwarderEchoTimeout seconds.
 */
static bool
ActOnMasterCheckState(MasterState *state)
{
	bool	retval = false;

	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

	if (PromotionCtl->master_check_state == master_check_started)
	{
		/* First check when the last message from master was received */
		if ((int) difftime(time(NULL), state->ms_last_msg_time) < ForwarderEchoTimeout)
			PromotionCtl->master_check_state = master_check_finished;
		else
			state->ms_echo_request = true;
	}

	if (PromotionCtl->master_check_state == master_check_inprogress)
	{
		if (state->ms_last_msg_time >= state->ms_check_start_time)
		{
			/* MCP received message from master after check had started */
			PromotionCtl->master_check_state = master_check_finished;
		}
		else if ((int) difftime(time(NULL), state->ms_check_start_time) >
				 ForwarderEchoTimeout)
		{
			/* Check timeout, consider master disconnected */
			elog(LOG, "MCP disconnected from master due to timeout");

			/* master_check_state will be changed inside proc_exit handler
			 * after disconnecting from the actual master. We can't do this
			 * right here because slave checks both master_check_state and
			 * master connection when deciding whether it may promote.
			 */
			retval = true;
		}
	}
	LWLockRelease(MCPServerLock);

	return retval;
}

/*
 * Send queued messages to the master.
 */
static void
SendMessagesToMaster(MasterState *state)
{
	MCPMsg	sm;
	bool 	table_dump_request;
	FullDumpProgress mcp_dump_progress;

	memset(&sm, 0, sizeof(MCPMsg));

	set_ps_display("sending messages to master", false);

	LWLockAcquire(MCPServerLock, LW_SHARED);
	table_dump_request = TableDumpIsRequested();
	mcp_dump_progress = FullDumpGetProgress();
	LWLockRelease(MCPServerLock);

	/* 
	 * Act on various master promotion states.
	 */
	if (state->ms_promotion == master_promotion_cancelled)
	{
		/* Notify master that promotion was cancelled */
		sm.flags |= MCP_MSG_FLAG_PROMOTE_CANCEL;

		elog(DEBUG3, "sending promotion cancel message");

		/* Inform the slave that promotion is no longer valid */
		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		PromotionCtl->promotion_cancelled = true;
		SlaveNextPromotionState(state);
		MCPMasterCancelPromotion(state);
		LWLockRelease(MCPServerLock);

		LOG_PROMOTION_STATE(DEBUG2, state);
	}
	else if (state->ms_promotion == master_promotion_started)
	{
		/* Notify master that promotion was started*/
		sm.flags |= MCP_MSG_FLAG_PROMOTE;
		state->ms_promotion = master_promotion_wait_ready;

		LOG_PROMOTION_STATE(DEBUG2, state);
	}
	else if (state->ms_promotion == master_promotion_send_make)
	{
		/* Ask master to die to be resurrected as a slave */
		MCPMasterFinishPromotion();
		state->ms_promotion = master_promotion_none;

		LOG_PROMOTION_STATE(DEBUG2, state);
	}

    /* Check if we should respond to a table dump request from the slave */
    if (table_dump_request == true)
    {
		/* 
		 * Make sure you don't change shared mcp_dump_progress here since 
		 * we use its cached value from the local variable in the code below.
		 */

		if (mcp_dump_progress == FDnone)
			SendTableDumps(state);
		else
			elog(LOG, "TABLE DUMP requests were not sent due to FULL DUMP request");

		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		TableDumpSetRequest(false);
		LWLockRelease(MCPServerLock);
    }

    /* Check if a dump was requested */
	if (mcp_dump_progress == FDrequested)
	{
		elog(LOG, "sending FULL DUMP request");
	    /* If we are in a state that requires a dump, ask the master for it. */
        sm.flags |= MCP_MSG_FLAG_REQFULL;

		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		FullDumpSetProgress(FDinprogress);
		LWLockRelease(MCPServerLock);

		/* ACKs are discarded upon requesting a dump from master */
		state->ms_ack_recno = InvalidRecno;
	}
	else if (state->ms_ack_recno != InvalidRecno)
	{
		sm.flags |= MCP_MSG_FLAG_ACK;
		elog(DEBUG3,
			 "MCP send ACK to Master => ACK recno: " UNI_LLU,
			 state->ms_ack_recno);
		sm.recno = state->ms_ack_recno;
		state->ms_ack_recno = InvalidRecno;
	}

	if (state->ms_echo_request)
	{
		/* Send ECHO message to master */
		sm.flags |= MCP_MSG_FLAG_ECHO;
		state->ms_check_start_time = time(NULL);
		state->ms_echo_request = false;

		/* Advance force promotion master check to the next state */	
		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		PromotionCtl->master_check_state = master_check_inprogress;
		LWLockRelease(MCPServerLock);
	}

	if (sm.flags != 0)
	{
		MCPSendMsg(&sm, true);
		MCPMsgPrint(DEBUG3, "MCP-M-Send", &sm);
	}
}

bool	
MCPMasterMessageHook(bool committed, MCPMsg *rm, void *state_arg)
{
	bool			skip = false;
	static bool		dump_in_progress = false;
	
	MasterState 	*state = (MasterState *) state_arg;

	if (!committed)
	{
		/* Actions performed before the commit */

		state->ms_last_msg_time = time(NULL);

		if (rm->flags & MCP_MSG_FLAG_ECHO)
		{
			elog(DEBUG3, "ECHO FLAG received from master");
			skip = true;
		}
	
		/*
		 * XXX: currently neither ReplicationQueueLock nor HostsLock is held
		 * during truncate, which might in theory cause a slave process to
		 * try sending the data being truncated. This should be fixed by holding
		 * a lock, perhaps a new one called ReplicationQueueTruncateLock.
		 */
		if (rm->flags & MCP_QUEUE_FLAG_TRUNC)
		{
			ullong recno = *((ullong *)rm->data);
	
			elog(DEBUG2, "MCP Receive TRUNC => TRUNC recno: "
				 UNI_LLU", SYNC: 1,  mcp_dump_request = false,"
				 "mcp_sync_request = false, mcp_request_in_process = 0",
				 recno);
	
			MCPHostsCleanup(state->ms_hosts, state->ms_queue, recno);
			
			/* 
			 * If truncate is a standalone message - don't put it to the queue.
			 */
			if (rm->flags == MCP_QUEUE_FLAG_TRUNC)
				skip = true;
		}

		if (rm->flags & MCP_MSG_FLAG_PROMOTE_CANCEL)
		{
			elog(WARNING, "Received promotion cancel request from master");
			if (state->ms_promotion != master_promotion_none)
			{
				/* 
				 * Do not set it to the cancelled state, because there is no
				 * need to send PROMOTE_CANCEL to a master.
				 */
				LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

				MCPMasterCancelPromotion(state);

				/* Still we have to notify the slave */
				PromotionCtl->promotion_cancelled = true;
				SlaveNextPromotionState(state);

				LWLockRelease(MCPServerLock);
			}
			LOG_PROMOTION_STATE(DEBUG2, state);
		}

		if (rm->flags & MCP_MSG_FLAG_PROMOTE_READY)
		{
			/* 
			 * Either transition to the 'wait for promote_make' state or cancel 
			 * a promotion depending on a current state.
			 */
			 if (state->ms_promotion == master_promotion_wait_ready)
			 {
				state->ms_promotion = master_promotion_wait_make;

				LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

				SlaveNextPromotionState(state);

				LWLockRelease(MCPServerLock);
			}
			else
			{
				/* A slave would be notified after sending PROMOTE_CANCEL */
				state->ms_promotion = master_promotion_cancelled;
			}
			LOG_PROMOTION_STATE(DEBUG2, state);
			skip = true;
		}

		/* Deal with a back promotion request from master */
		if (rm->flags & MCP_MSG_FLAG_PROMOTE_BACK)
		{
			int 	prev_slaveno;

			/* 
			 * Check if we are asking for a new promotion in the middle of the
			 * existing one.
			 */
			prev_slaveno = PromotionStackPeek();

			if (prev_slaveno == -1)
			{
				elog(WARNING, "back promotion requested but no promotions performed yet");
				state->ms_promotion = master_promotion_cancelled;
			}
			else
			{
				/* Try to start a new promotion */
				if (state->ms_promotion == master_promotion_none)
				{
					LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);	
					MCPMasterStartPromotion(state, prev_slaveno);
					LWLockRelease(MCPServerLock);
				}
				else
				{
					/* If we are already in promotion - cancel both currently requested
					 * promotion and an existing one. This is the agent's task to track
					 * that master is not trying to send more than one promotion request
					 * at a time.
					 */
					state->ms_promotion = master_promotion_cancelled;

				}
			}
			LOG_PROMOTION_STATE(DEBUG2, state);
			/* Don't put this message to the queue */
			skip = true;
		}
	}
	else
	{
		/* Post-commit actions */
		if (rm->flags & MCP_QUEUE_FLAG_DUMP_START)
		{
			ereport(LOG,
					(errmsg("received start of dump"),
					 errdetail("Record number "UNI_LLU, rm->recno)));

			/* 
			 * Set the position of a 'cached' dump in queue and reset the
			 * dump-in-progress flag.
             */
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
			FullDumpSetProgress(FDnone);
			FullDumpSetStartRecno(rm->recno);
			FullDumpSetEndRecno(InvalidRecno);
			LWLockRelease(MCPServerLock);
			
			dump_in_progress = true;
	
			/* set ACK recno */
			state->ms_ack_recno = rm->recno;
	
			/* The queue data is consistent now */
			LockReplicationQueue(state->ms_queue, LW_EXCLUSIVE);
			MCPQueueSetSync(state->ms_queue, MCPQSynced);
			UnlockReplicationQueue(state->ms_queue);
		}
		else if (rm->flags & MCP_QUEUE_FLAG_DUMP_END)
		{
			ereport(LOG,
					(errmsg("received end of dump"),
					 errdetail("Record number "UNI_LLU, rm->recno)));

			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
			FullDumpSetEndRecno(rm->recno);			
			LWLockRelease(MCPServerLock);			

		}
		if (rm->flags & MCP_QUEUE_FLAG_TABLE_DUMP ||
			rm->flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
		{
			/* Set ACK recno */
			state->ms_ack_recno = rm->recno;
		}
		else if (rm->flags & MCP_QUEUE_FLAG_DATA)
		{
			LWLockAcquire(MCPServerLock, LW_SHARED);
			/*
			 * Do not send ACK for non-dump transactions received while
			 * the queue was not in sync.
			 */
			if (FullDumpGetProgress() == FDnone)
				state->ms_ack_recno = rm->recno;

			LWLockRelease(MCPServerLock);
		}

		if (rm->recno == InvalidRecno)
			elog(ERROR,
				 "attempted to commit a transaction with invalid record number");

		/* Commit the transaction */
		/* Clear the empty state of the queue */
		LockReplicationQueue(state->ms_queue, LW_EXCLUSIVE);
		MCPQueueTxCommit(state->ms_queue, rm->recno);
		TXLOGSetCommitted(rm->recno);
		UnlockReplicationQueue(state->ms_queue);

		/* 
		 * Recheck that we are still receiving a full dump.  If we aren't,
		 * clear the dump_in_progress flag.
		 */
		if (dump_in_progress)
		{
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

			if (FullDumpGetStartRecno() == InvalidRecno || 
				FullDumpGetEndRecno() != InvalidRecno)
			{
				dump_in_progress = false;
			}
			LWLockRelease(MCPServerLock);
		}			

		/* Tell the slaves they have a new data to replicate */
		MasterNotifySlaves(state);
	}

	return skip;
}	

/* Check a tablelist received from the master */
static void
ReceiveMasterTableList(List *tl_received, TxDataHeader *hdr, 
					   ullong recno, void *arg_state)
{
	MasterState *state = (MasterState *) arg_state;

	if (hdr->dh_flags & MCP_QUEUE_FLAG_TABLE_DUMP)
	{
		ListCell    *cell;
		char   *path = MCPGetTableListFilename(MCP_LIST_SUFFIX);

		LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);

		/* check if we don't have the latest table list */
		if (state->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
		{
			list_free_deep(state->ms_tablelist);
			state->ms_tablelist = NIL;
			MasterRestoreTableList(state);
		}
		/* Check if we should add new tables to the MCP table list */
		foreach(cell, tl_received)
		{
			MCPTable		t_found;
			BackendTable	t_current = (BackendTable) lfirst(cell);

			/* Look for the table in the master's list */
			t_found = TableListEntry(state->ms_tablelist, t_current);

			ereport(DEBUG3,
					(errmsg("Received dump for the table %s", 
							t_current->relpath),
					 errdetail("recno "UNI_LLU, recno)));

			/* If we didn't find the table - make it and add to the MCP table list */
			if (t_found == NULL)
			{
				t_found = MakeMCPTable(t_current->relpath);
				state->ms_tablelist = lappend(state->ms_tablelist, t_found);
				elog(DEBUG3, "Table %s added to the table list", 
					 t_found->relpath);
			}

			/* We have just received a dump for this table */
            t_found->dump_recno = recno;
			t_found->req_satisfied = TDnone;
		}
		/* Store changes on disk */
		MasterStoreTableList(state);

		LWLockRelease(MCPTableListLock);

		pfree(path);
	}
}

/*
 * Read a message from the socket.
 */
static void
ReceiveMessageFromMaster(MasterState *state)
{
	MCPMsg	*rm;

	set_ps_display("receiving a message from master", false);

	rm = ReceiveQueueTransaction(state->ms_queue,
								 ReceiveMasterTableList, (void *) state,
								 MCPMasterMessageHook, (void *) state);

	MCPReleaseMsg(rm);
}

/* Wake up the slave processes */
static void
MasterNotifySlaves(MasterState *state)
{
	int		i,
			j;
	pid_t	pids[MCP_MAX_SLAVES];

	j = 0;
	LWLockAcquire(MCPServerLock, LW_SHARED);
	for (i = 1; i < MCP_MAX_SLAVES + 1; i++)
		if (ServerCtl->node_pid[i] != 0)
			pids[j++] = ServerCtl->node_pid[i];
	/* release lwlock before doing any system call */
	LWLockRelease(MCPServerLock);

	for (i = 0; i < j; i++)
		kill(pids[i], SIGUSR1);
}

/* 
 * Command slave to change its promotion state.
 */
static void
SlaveNextPromotionState(MasterState *state)
{
	int 	slaveno;
	pid_t 	slavepid;

	Assert(LWLockHeldByMe(MCPServerLock));

	/* send a sigusr2 signal to a currently promoted slave */
	slaveno = PromotionCtl->promotion_slave_no;

	/* Check if promotion slaveno is valid and slave is connected */
	if (slaveno >= 0 && slaveno < MCP_MAX_SLAVES &&
		PromotionCtl->slave_sysid == ServerCtl->node_sysid[slaveno + 1] &&
		PromotionCtl->slave_sysid != 0)
	{
		slavepid = ServerCtl->node_pid[slaveno + 1];
		if (slavepid > 0)
			SendForwarderChildSignal(slaveno + 1, FORWARDER_SIGNAL_PROMOTION);
		else
		{
			elog(WARNING,
				 "slave %d is not alive but marked as participating in promotion",
				 slaveno);
			/* Cancel current promotion for master */
			PromotionCtl->slave_sysid = 0;
			if (state->ms_promotion != master_promotion_none)
				state->ms_promotion = master_promotion_cancelled;
		}
	}
	else
		elog(WARNING,
			 "attempted to send promotion signal to a slave that is either disconnected or not in promotion");

	LOG_PROMOTION_STATE(DEBUG2, state);
}

/* 
 * A handler for changing master promotion state in response to a request
 * from a promoted slave. Note that we change only those master states that
 * depends on a slave state here. Promotion cancel request is transmitted
 * by SIGUSR2 signal with promotion flag set.
 */
static void
MCPMasterActOnPromotionSignal(MasterState *state)
{
	bool	cancel;

	/* Ignore slave promotion signals if we already cancelled promotion */
	if (state->ms_promotion == master_promotion_cancelled)
		return;

	/* 
	 * Advance to the next promotion state or cancel current promotion,
	 * depending on the shared memory flag.  We clear the promotion cancel flag
	 * after storing its state locally.
	 */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	cancel = PromotionCtl->promotion_cancelled;
	PromotionCtl->promotion_cancelled = false;
	LWLockRelease(MCPServerLock);

	if (cancel)
	{
		if (state->ms_promotion != master_promotion_none)
		{
			elog(WARNING, "master promotion was cancelled");
			state->ms_promotion = master_promotion_cancelled;
		}
	}
	else if (state->ms_promotion != master_promotion_cancelled)
	{
		/* 
		 * Advance to the next master state. Note that we do this
		 * only for the states that should be changed by a notification
		 * from a slave, for other states we emit WARNING.
		 */
		if (state->ms_promotion == master_promotion_none ||
			state->ms_promotion == master_promotion_wait_make)
			state->ms_promotion++;
		else
		{
			elog(WARNING,
				 "master promotion state %d doesn't expect a signal from the slave",
				 state->ms_promotion);
			state->ms_promotion = master_promotion_cancelled;
		}
	}
	else
		elog(WARNING,
			 "received a request to change promotion state after promotion was cancelled");

	LOG_PROMOTION_STATE(DEBUG2, state);
}

/* Start MCP promotion by the master MCP process */
static void
MCPMasterStartPromotion(MasterState *state, int prev_slaveno)
{
	int 	promotion_slaveno;
	bool 	allow = true;

	/* 
	 * don't check if promotion is allowed here since this function is used
	 * only in context of back promotion, which should be always allowed for
	 * the slave since we already performed 'forward' promotion with it.
	 */
	Assert(LWLockHeldByMe(MCPServerLock));
	promotion_slaveno = PromotionCtl->promotion_slave_no;

	/* 
	 * Check if MCP is already performing a promotion. Look at the
	 * SlaveStartPromotion code for details. Note that we check this for the
	 * slave to avoid missing a force promotion check. A caller of this
	 * function is responsible for checking master's state.
	 */
	if (promotion_slaveno >= 0 &&
		ServerCtl->node_pid[promotion_slaveno + 1] != 0 &&
		ServerCtl->node_sysid[promotion_slaveno + 1] == PromotionCtl->slave_sysid)
	{
		allow = false;
	}
	else
	{
		promotion_slaveno = prev_slaveno;

		elog(DEBUG2, "starting back promotion from the master, slave to promote: %d",
			 prev_slaveno);
		if (ServerCtl->node_pid[promotion_slaveno + 1] == 0)
		{
			elog(WARNING, "slave %d is not connected, promotion is cancelled",
				 promotion_slaveno);
			allow = false;
		}
		else
		{
			/*
			 * store sysids of agent processes involved in promotion in a
			 * shared memory area
			 */
			PromotionCtl->master_sysid = ServerCtl->node_sysid[0];
			PromotionCtl->slave_sysid = ServerCtl->node_sysid[promotion_slaveno + 1];

			/* clear promotion cancellation flag */
			PromotionCtl->promotion_cancelled = false;
			PromotionCtl->promotion_slave_no = promotion_slaveno;
			PromotionCtl->promotion_type = back_promotion;
			state->ms_promotion = master_promotion_started;

			/* Inform the slave about start of the promotion */
			SlaveNextPromotionState(state);
		}
	}

	if (!allow)
	{
		elog(WARNING, "promotion was cancelled by master");
		state->ms_promotion = master_promotion_cancelled;
	}

	LOG_PROMOTION_STATE(DEBUG2, state);
}

static void
MCPMasterFinishPromotion(void)
{
	MCPMsg *sm = palloc0(sizeof(MCPMsg) + 
						 sizeof(PromotionCtl->promotion_slave_no) - 1);
	
	sm->flags |= MCP_MSG_FLAG_PROMOTE_MAKE;
	memcpy(sm->data, &(PromotionCtl->promotion_slave_no), 
		   sizeof(PromotionCtl->promotion_slave_no));
	sm->datalen = sizeof(PromotionCtl->promotion_slave_no);
	
	MCPSendMsg(sm, true);
	MCPMsgPrint(DEBUG3, "MCP-M-Send", sm);
	
	MCPReleaseMsg(sm);
}

/* 
 * Cancel promotion for the master process. The PROMOTE_CANCEL message is
 * sent elsewhere.
 *
 * A promotion can be cancelled by several ways:
 *
 * 1) A notification from the slave process participating in promotion.
 *
 * 2) By receiving PROMOTE_CANCEL message from the master.
 *
 * 3) Internally, by observing an inconsistent promotion state or coming
 * across some other error.
 *
 * We don't inform the slave (with setting promotion_cancelled in shmem
 * and sending a signal) in the 1st case, don't send PROMOTE_CANCEL to
 * a master in the second and do both of these actions in the 3d case.
 * This code contains common actions valid for each of these cases.
 */
static void
MCPMasterCancelPromotion(MasterState *state)
{
	Assert(LWLockHeldByMe(MCPServerLock));

	state->ms_promotion = master_promotion_none;

	/* 
	 * Reset master's promotion sysid. Check if slave has also reset its
	 * sysid, clear other promotion related shared variables.
	 */
	PromotionCtl->master_sysid = 0;
	if (PromotionCtl->slave_sysid == 0)
	{
		PromotionCtl->promotion_slave_no = -1;
		PromotionCtl->promotion_cancelled = false;
	}

	LOG_PROMOTION_STATE(DEBUG2, state);
}

static void
MasterStoreTableList(MasterState *state)
{
	char   *path;
	Assert(state != NULL);
	Assert(LWLockHeldByMe(MCPTableListLock));

	path = MCPGetTableListFilename(MCP_LIST_SUFFIX);
	StoreTableList(path, state->ms_tablelist);

	/* Increase the latest revision number */
	state->current_tablelist_rev = ++ServerCtl->latest_tablelist_rev;

	pfree(path);
}

static void
MasterRestoreTableList(MasterState *state)
{
	char   *path;

	Assert(state != NULL);
	Assert(LWLockHeldByMe(MCPTableListLock));

	path = MCPGetTableListFilename(MCP_LIST_SUFFIX);
    state->ms_tablelist = RestoreTableList(path);

	 /* We are up-to-date with a table list */
    state->current_tablelist_rev = ServerCtl->latest_tablelist_rev;

    pfree(path);
}


/* Check for table dump flags set and send corresponding table dump requests */
static void
SendTableDumps(MasterState *state)
{
    ListCell   *cell;
    MCPMsg   *sm;
    bool    changed = false;

	set_ps_display("sending table dump requests", false);

    /* we are going to change some table flags */
    LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);

    /* Get the latest version of table list if needed */
    if (state->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
    {
        list_free_deep(state->ms_tablelist);
        state->ms_tablelist = NIL;
        MasterRestoreTableList(state);
    }

    sm = palloc0(sizeof(MCPMsg) + MAX_REL_PATH);

    /* scan each tables for dump request */
    foreach(cell, state->ms_tablelist)
    {
        MCPTable    cur = (MCPTable) lfirst(cell);
        Assert(cur->id == TABLEID_MCP);

        if (cur->req_satisfied == TDrequested)
        {
            /* Compose a dump request message for this table */
            strncpy(sm->data, cur->relpath, MAX_REL_PATH);
            sm->datalen = strlen(sm->data);
            sm->flags = MCP_MSG_FLAG_REQTABLE;

            /* Add it to the sending buffer */
            MCPSendMsg(sm, false);
            MCPMsgPrint(DEBUG3, "MCP-M-Send", sm);

            /* flag the request as not satisfied by master */
            cur->req_satisfied = TDinprogress;
            changed = true;
        }
    }

    /* send all messages from the buffer */
    MCPFlush();

    /* Store table changes on disk */
    if (changed)
        MasterStoreTableList(state);

    LWLockRelease(MCPTableListLock);

    pfree(sm);
}
