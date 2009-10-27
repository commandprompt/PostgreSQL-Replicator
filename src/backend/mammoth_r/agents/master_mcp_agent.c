/*
 * master_mcp_agent.c
 *
 * 		Master-side replication code, also known as Queue Monitor Process or
 * 		Master Queue Process.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group.
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: master_mcp_agent.c 2213 2009-07-28 10:35:46Z alexk $
 */
#include "postgres.h"

#include <sys/types.h>
#include <signal.h>
#include <time.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "libpq/libpq.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/collector.h"
#include "mammoth_r/forwcmds.h"
#include "mammoth_r/master.h"
#include "mammoth_r/mcp_api.h"
#include "mammoth_r/mcp_compress.h"
#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/txlog.h"
#include "mammoth_r/promotion.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/replication.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

typedef struct MasterState
{
    FullDumpProgress    FDprogress;
    ullong  FDrecno;

	MCPQueue 	*master_mcpq;
} MasterState;

typedef enum MasterPromotionState
{
	master_no_promotion,
	master_promotion_started,
	master_promotion_send_ready,
	master_promotion_wait_make,
	master_promotion_completed,
	master_promotion_cancelled
} MasterPromotionState;

#define MasterPromotionAsString \
	((master_promotion == master_no_promotion) ? "none" : \
	 (master_promotion == master_promotion_started) ? "started" : \
	 (master_promotion == master_promotion_send_ready) ? "send ready" : \
	 (master_promotion == master_promotion_wait_make) ? "wait make" : \
	 (master_promotion == master_promotion_completed) ? "completed" : \
	 (master_promotion == master_promotion_cancelled) ? "cancelled" : \
	 "unknown" )

#define DISPLAY_MASTER_PROMOTION_STATE	\
	elog(DEBUG2, "Master promotion state is: %s", MasterPromotionAsString)

static MasterState	global_state;
static MasterPromotionState 	master_promotion;

static void SendQueuedMessages(MasterState *state);
static void ProcessMCPMessage(MasterState *state);
static void MasterActOnQueueStatus(MasterState *state);
static void MasterActOnPromotion(MasterState *state, 
                                 MasterPromotionState promotion_state);
static void connect_callback(void *arg);
static void authenticate_callback(void *arg);

/*
 * flag to tell whether we're starting up.  This must be outside
 * ReplicatorMasterMain to avoid being restored in the longjmp() call
 */
static bool		doing_socket_setup = false;

static void
ReceiveInitialRecno(MCPQueue *q)
{
	ullong	initial_recno;

	initial_recno = MCPRecvInitialRecno();
	/* initial_recno contains lrecno + 1 from MCP server, or InvalidRecno */

	LockReplicationQueue(q, LW_EXCLUSIVE);
	if (MCPQueueGetFirstRecno(q) != initial_recno)
	{

		if (initial_recno == InvalidRecno)
		{
			/* MCP server unsynced, must cause a dump */
			ereport(LOG,
					(errmsg("setting queue to unsync"),
					 errdetail("First recno requested InvalidRecno.")));
			MCPQueueSetSync(q, MCPQUnsynced);
		}
		else
		{
			if (initial_recno > MCPQueueGetAckRecno(q) &&
				initial_recno <= MCPQueueGetLastRecno(q) &&
				initial_recno >= MCPQueueGetInitialRecno(q))
			{
				/* OK to serve from current queue */
				elog(LOG,
					 "first recno correction: "UNI_LLU" -> "UNI_LLU,
					 MCPQueueGetFirstRecno(q), initial_recno);
				MCPQueueSetFirstRecno(q, initial_recno);
			}
			else
			{
				ereport(WARNING,
						(errmsg("setting queue to unsync"),
						 errdetail("First recno requested "UNI_LLU" not in range.",
								   initial_recno)));
				MCPQueueSetSync(q, MCPQUnsynced);
			}
		}
	}
	UnlockReplicationQueue(q);
}

int
ReplicationMasterMain(MCPQueue *q)
{
	sigjmp_buf	local_setjmp_buf;
	volatile MemoryContext	mastercxt = NULL;
	/* forwarder config */
	char	   *forwname;
	char	   *forwaddr;
	int			forwport;
	bool		forwssl;
	char	   *forwkey;
	char 	   *encoding;
	/* minimal interval in seconds between sequential vacuum calls */
	double		prune_min_interval = 5;
	/* when the last vacuum call finished */
	double		last_prune_time = time(NULL);
	ErrorContextCallback errcontext;
	uint64		sysid;
	int			sync;

	/* MasterState initialization */
	MasterState *state = &global_state;

    state->FDprogress = FDnone;
    state->FDrecno = InvalidRecno;
	state->master_mcpq = q;

	if (sigsetjmp(local_setjmp_buf, 1) != 0)
	{
		bool	ret = false;

		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/*
		 * Release any held LWLocks.  Note: this is a kludge; the lwlocks
		 * should really be released by AbortOutOfAnyTransaction.  But since
		 * we sometimes acquire locks and fail when not in a transaction, we
		 * must do part of the cleanup ourselves.  Actually, all cleanup that
		 * AbortOutOfAnyTransaction does should be duplicated here (at least
		 * until we find a better solution).
		 */
		LWLockReleaseAll();

		QueryCancelPending = false;
		disable_sig_alarm(true);
		QueryCancelPending = false;		/* again in case timeout occurred */

		/* Report the error to the client and/or server log */
		EmitErrorReport();

		/* If we have an active queue transaction - close it */
		if (MCPQueueGetDatafile(q) != NULL)
			MCPQueueTxClose(q);

        /* Reset the current dump state */
        state->FDprogress = FDnone;
        state->FDrecno = InvalidRecno;

		/* call module-specific cleanup routines */
		MCPFileCleanup();

		/* Do the recovery */
		AbortOutOfAnyTransaction();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		if (doing_socket_setup)
			ret = true;
		/*
		 * Reset the socket to get rid of errors due to the data left in its
		 * buffers
		 */
		pq_reset();
		pq_comm_reset();
		if (MyProcPort)
		{
			if (MyProcPort->sock != -1)
				StreamClose(MyProcPort->sock);
			pfree(MyProcPort);
			MyProcPort = NULL;
		}

		/* delete our private context */
		if (mastercxt != NULL)
		{
			MemoryContextDelete(mastercxt);
			mastercxt = NULL;
		}

		/* Now we can resume interrupts */
		InterruptHoldoffCount = 0;

		if (ret)
			return 0;

		/* don't fill the logs as fast as we can */
		pg_usleep(1000000L);
	}
	PG_exception_stack = &local_setjmp_buf;

	/* Create a separate context so that we don't leak too much memory */
	mastercxt = AllocSetContextCreate(TopMemoryContext,
									  "Queue Monitor Context",
									  ALLOCSET_DEFAULT_MINSIZE,
									  ALLOCSET_DEFAULT_INITSIZE,
									  ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(mastercxt);

	/* read the forwarder configuration */
	init_forwarder_config(&forwname, &forwaddr, &forwport, &forwkey, &forwssl);

	/* get database encoding or use utf8 if forced by config option */
	if (replication_perform_encoding_conversion)
		encoding = pstrdup(pg_encoding_to_char(PG_UTF8));
	else
		encoding = pstrdup(GetDatabaseEncodingName());

#ifdef USE_SSL
	if (forwssl)
		secure_initialize();
#endif

	/* set error context callbacks for nicer error messages */
	errcontext.callback = connect_callback;
	errcontext.arg = NULL;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/* during connection and authentication, errors are handled specially */
	doing_socket_setup = true;

	MyProcPort = OpenMcpConnection(forwname, forwaddr, forwport);
	SendMcpStartupPacket(MyProcPort, forwssl, CLIENT_ROLE_MASTER, 0);

	errcontext.callback = authenticate_callback;

	/* grab our sysid */
	LWLockAcquire(ControlFileLock, LW_SHARED);
	sysid = repl_get_sysid();
	LWLockRelease(ControlFileLock);

	/* grab queue data */
	LockReplicationQueue(q, LW_SHARED);
	sync = MCPQueueGetSync(q);
	UnlockReplicationQueue(q);

	if (McpAuthenticateToServer(sysid, forwkey, encoding) == STATUS_ERROR)
		elog(ERROR, "problem authenticating to server");

	/* socket setup done, revert to normal behavior */
	error_context_stack = errcontext.previous;
	doing_socket_setup = false;

	/* Is this a promoted slave? */
	LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

	ReplPromotionData->promotion_in_progress = false;
	ReplPromotionData->promotion_block_relations = false;

	LWLockRelease(ReplicationLock);

	/* set initial frecno, as received from the MCP server */
	ReceiveInitialRecno(q);

	ereport(LOG,
		   (errmsg("master connected to forwarder")));

	while (true)
	{
		int			ret;

		/*
		 * Send the messages already in queue as first step, in case someone
		 * has been writing while we weren't connected
		 */
		SendQueuedMessages(state);

		/* Prune the queue if needed */
		if (difftime(time(NULL), last_prune_time) >= prune_min_interval)
		{
			MCPQueueLogHdrStatus(DEBUG4, q, "Before prune");

			LockReplicationQueue(q, LW_EXCLUSIVE);
			MCPQueuePrune(q);
			UnlockReplicationQueue(q);

			last_prune_time = time(NULL);
			MCPQueueLogHdrStatus(DEBUG4, q, "After prune");
		}

		/*
		 * Act on MCP messages while there is stuff in the internal socket
		 * buffer.  Note that we don't really care if there is unread stuff in
		 * the kernel socket buffer, because that will be detected by the
		 * select() call and processed below.
		 */
		while (MCPMsgAvailable())
			ProcessMCPMessage(state);

		/* If the queue is in need of attention, take care of it */
		MasterActOnQueueStatus(state);

		/* Check if user triggered promotion on master. We ignore anything 
		 * but back promotion here, but emit a warning for wrong promotion type 
		 */
		if (promotion_request)
		{
			LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

			promotion_request = false;

            /* Cancel setting promotion_request on receiving sigusr1 */
            ReplSignalData->promotion = false;

			if (!ReplPromotionData->promotion_in_progress)
			{

				if (ReplPromotionData->back_promotion)
				{
					ReplPromotionData->back_promotion = false;
					InitiateBackPromotion();
					/* 
					 * By setting promotion in progress status here we disallow
					 * subsequent promotion requests during the time window
					 * between sending PROMOTE_BACK to the MCP and receiving
					 * a corresponding PROMOTE from it.
					 */
					ReplPromotionData->promotion_in_progress = true;
				}
				else
					elog(WARNING, "Unknown promotion request, back: %d, force: %d, in_progress: %d",
						 ReplPromotionData->back_promotion, ReplPromotionData->force_promotion,
					 	ReplPromotionData->promotion_in_progress);
			}
			else
			{
                /* 
                 * A race condition happened and one or several of our backends
                 * managed to send us multiple promotion signals before we have
                 * set promotion_in_progress in MQP.
                 */
                elog(WARNING, "Multiple promotion requests received");
			}
			LWLockRelease(ReplicationLock);
		}

		elog(DEBUG2, "waiting for activity");

        PG_SETMASK(&UnBlockSig);
		ret = mcpWaitTimed(MyProcPort, true, false, (time_t) -1);
        PG_SETMASK(&BlockSig);

		if (ret < 0)
		{
			/*
			 * The select() call will get a signal from the postmaster or from
			 * the backends anytime they flush something into the queue.
			 *
			 * If select() fails with a different errno than EINTR, then
			 * something bad happened.
			 */
			if (errno != EINTR)
				elog(ERROR, "select: %m");
		}
		else if (ret > 0)
		{
			/*
			 * If the MCP server is trying to talk to us, listen to it.  Note we
			 * read a single message from the socket, but this doesn't matter too
			 * much since we'll process the whole buffer in the loop above.
			 */
			ProcessMCPMessage(state);
		}

		SendQueuedMessages(state);
	}

	pq_comm_reset();
	StreamClose(MyProcPort->sock);

	return 0;
}

/*
 * MasterActOnQueueStatus
 *
 * If the queue is unsynced, or if the user signalled us to send a dump, do it.
 * This routine is also in charge of processing the promotion by sending the
 * acknowledging promotion message when all queued messages have been sent.
 */
static void
MasterActOnQueueStatus(MasterState *state)
{
    MCPQSync    mcpq_sync = MCPQSynced;
	MCPQueue 	*q = state->master_mcpq; 

	LockReplicationQueue(q, LW_SHARED);
    mcpq_sync = MCPQueueGetSync(q);
	UnlockReplicationQueue(q);

    /* If the queue is not synced - start a full dump */
    if (mcpq_sync != MCPQSynced)
    {
        elog(LOG, "processing truncate & full dump: %s",
			 MCPQSyncAsString(mcpq_sync));

        state->FDrecno = PGRDumpAll(q);
        state->FDprogress = FDinprogress;

		LockReplicationQueue(q, LW_EXCLUSIVE);
        /* Set the queue to the normal operational mode */
        MCPQueueSetSync(q, MCPQSynced);

        /* XXX: why do we need this ? */
        MCPQueueSetAckRecno(q, 0);
        
		UnlockReplicationQueue(q);

        /* A dump request cancells promotion */
		if (master_promotion != master_no_promotion)
		{
			master_promotion = master_promotion_cancelled;
			elog(WARNING, "promotion is cancelled by the dump");
		}
		DISPLAY_MASTER_PROMOTION_STATE;

    }
	/* Make sure we have released the queue lock here */
	Assert(!QueueLockHeldByMe(q));

    /* Perform promotion state related actions */
    MasterActOnPromotion(state, master_promotion);

	/* Make sure to actually send the messages */
	SendQueuedMessages(state);
}

static void
MasterActOnPromotion(MasterState *state, MasterPromotionState promotion_state)
{
    MCPQueue    *q = state->master_mcpq;

 	/* Act on different  master promotion states */
	if (promotion_state == master_promotion_started)
	{
		/* 
		 * Check if we have sent all the queue data already.
		 * We would send a READY message to a slave after this.
		 */
		LockReplicationQueue(q, LW_EXCLUSIVE);
		if (MCPQueueGetLastRecno(q) < MCPQueueGetFirstRecno(q))
			master_promotion = master_promotion_send_ready;
		UnlockReplicationQueue(q);

		DISPLAY_MASTER_PROMOTION_STATE;
	}

	if (promotion_state == master_promotion_send_ready)
	{
		/* Send PROMOTE READY message to MCP */
		MCPMsg	sm;

		memset(&sm, 0, sizeof(MCPMsg));
		sm.flags |= MCP_MSG_FLAG_PROMOTE_READY;
		MCPSendMsg(&sm, true);
		MCPMsgPrint(DEBUG3, "M-MCP-Send", &sm);
		/* 
		 * Change master promotion state to waiting for the 
		 * PROMOTE_MAKE message 
		 */
		master_promotion = master_promotion_wait_make;

		DISPLAY_MASTER_PROMOTION_STATE;
	}

	if (promotion_state == master_promotion_cancelled)
	{
		/* Send PROMOTE_CANCEL message to MCP and cancel promotion */
		MCPMsg 	sm;
		
		memset(&sm, 0, sizeof(MCPMsg));
		sm.flags |= MCP_MSG_FLAG_PROMOTE_CANCEL;
		MCPSendMsg(&sm, true);
		MCPMsgPrint(DEBUG3, "M-MCP-Send", &sm);

		/* Reset local and shared promotion states */
		master_promotion = master_no_promotion;

		LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

		ReplPromotionData->promotion_in_progress = false;
		ReplPromotionData->promotion_block_relations = false;
		
		LWLockRelease(ReplicationLock);
		DISPLAY_MASTER_PROMOTION_STATE;
	}
}

/*
 * If there is some stuff in the queue to send to the MCP server, do it.
 */
static void
SendQueuedMessages(MasterState *state)
{
	ullong	first,
			last;
	MCPQueue	*q = state->master_mcpq;

	MCPQueueLogHdrStatus(DEBUG4, q, "SendQueuedMessages");

	LockReplicationQueue(q, LW_SHARED);
	first = MCPQueueGetFirstRecno(q);
	last = MCPQueueGetLastRecno(q);
	UnlockReplicationQueue(q);

	/* Send all committed transaction data */
	while (first <= last && TXLOGIsCommitted(first))
	{
		SendQueueTransaction(q, first, NULL, NULL, NULL, NULL);

		LockReplicationQueue(q, LW_EXCLUSIVE);
		MCPQueueNextTx(q);
		MCPQueueSetDequeueTimestamp(q); 
		first = MCPQueueGetFirstRecno(q);
		last = MCPQueueGetLastRecno(q);
		UnlockReplicationQueue(q);
	}
}

/*
 * Returns the file size of the local queue file.  The main usage for this is
 * to be able to truncate that file when a subtransaction aborts later.  (So
 * this is called when the subtransaction begins).
 */
off_t
PGRGetCurrentLocalOffset(void)
{
	off_t		offset;
	MCPFile	   *lq_file;
	
	Assert(replication_enable);
	Assert(replication_master);
	Assert(MasterLocalQueue != NULL);

	lq_file = MCPLocalQueueGetFile(MasterLocalQueue);
	offset = MCPFileSeek(lq_file, 0, SEEK_CUR);

	return offset;
}

/*
 * PGRAbortSubTransaction
 * 		Abort a subtransaction.
 *
 * We just truncate the local queue to the given offset, which was saved
 * previously with PGRGetCurrentLocalOffset.
 */
void
PGRAbortSubTransaction(off_t offset)
{
	MCPFile    *lq_file;
	Assert(MasterLocalQueue != NULL);

	lq_file = MCPLocalQueueGetFile(MasterLocalQueue);
	MCPFileTruncate(lq_file, offset);
	MCPFileSeek(lq_file, offset, SEEK_SET);
}


/*
 * ProcessMCPMessage
 *
 * Process a single message from the MCP server.  Returns false if it gets a
 * disconnect message from the MCP server.
 */
static void
ProcessMCPMessage(MasterState *state)
{
	MCPMsg     *rm;
	MCPQueue	*q = state->master_mcpq;

	rm = MCPRecvMsg();
	MCPMsgPrint(DEBUG3, "MCPRecvMsg", rm);

	/*
	 * If master receives a promotion cancel message, unset the
	 * ReplPromotionData->promotion_in_progress flag and forget about the
	 * promotion.
	 */
	if (rm->flags & MCP_MSG_FLAG_PROMOTE_CANCEL)
	{
		/* 
		 * Clear promotion in progress state and allow back modifications
		 * for the replicated tables.
		 */
		LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

		ReplPromotionData->promotion_in_progress = false;
		ReplPromotionData->promotion_block_relations = false;

		LWLockRelease(ReplicationLock);
		master_promotion = master_no_promotion;

		elog(LOG, "promotion cancelled by MCP server");

		DISPLAY_MASTER_PROMOTION_STATE;
	}


	if (rm->flags & MCP_MSG_FLAG_ECHO)
	{
		/* Send reply immediately */
		MCPMsg	msg;

		MemSet(&msg, 0, sizeof(MCPMsg));
		msg.flags |= MCP_MSG_FLAG_ECHO;
		msg.recno = InvalidRecno;
		MCPSendMsg(&msg, true);
	}

	if (rm->flags & MCP_MSG_FLAG_REQFULL)
	{

        if (state->FDprogress == FDnone)
        {
			elog(LOG, "FULL DUMP request");

			LockReplicationQueue(q, LW_EXCLUSIVE);
		    MCPQueueSetSync(q, MCPQUnsynced);
			UnlockReplicationQueue(q);

            state->FDprogress = FDrequested;
        }
        else
            elog(LOG, "received duplicate FULL DUMP request");
	}

	if (rm->flags & MCP_MSG_FLAG_REQTABLE)
	{
		char   *relpath = palloc(rm->datalen + 1);

		memcpy(relpath, rm->data, rm->datalen);
		relpath[rm->datalen] = '\0';

		if (state->FDprogress != FDnone)
			elog(LOG, "DUMP REQUEST for table \"%s\" is discarded due"
					  " to FULL DUMP in progress ", relpath);
		else
		{
			elog(LOG, "DUMP REQUEST for table \"%s\"", relpath);
			PGRDumpSingleTable(q, relpath);
		}
		pfree(relpath);
	}

	/* Accept ACKs only for records numbers greater than current vrecno */		
	LockReplicationQueue(q, LW_EXCLUSIVE);
	if (rm->flags & MCP_MSG_FLAG_ACK && rm->recno > 0)
	{
		if (rm->recno > MCPQueueGetAckRecno(q))
		{
			elog(LOG,
			 	"ACK accepted vrecno: "UNI_LLU" -> "UNI_LLU,
			 	MCPQueueGetAckRecno(q), rm->recno);
			MCPQueueSetAckRecno(q, rm->recno);
            if (state->FDprogress == FDinprogress && 
                rm->recno >= state->FDrecno)
            {
                elog(LOG, "Full dump with recno "UNI_LLU" was received by MCP",
                     state->FDrecno);
                state->FDprogress = FDnone;
                state->FDrecno = InvalidRecno;
            }

		}
		else
		{
			elog(WARNING,
			 	"ACK desynchronized: rm->recno: "UNI_LLU"  vrecno: "UNI_LLU,
			 	rm->recno, MCPQueueGetAckRecno(q));
	
			if (MCPQueueGetSync(q) != MCPQUnsynced)
				MCPQueueSetSync(q, MCPQUnsynced);
		}
	}
	UnlockReplicationQueue(q);

	/*
	 * Received a PROMOTE message from the MCP server.  This was
	 * forwarded from an authorized slave.  We need to lock the
	 * replicated tables, and wait for the current transaction to end.
	 * After that we will send a "promotion ready" flag and the slave
	 * will be promoted.
	 */
	if (rm->flags & MCP_MSG_FLAG_PROMOTE)
	{
		if (master_promotion == master_no_promotion)
		{
			/* Start promotion and block tables modification by the backends */
			master_promotion = master_promotion_started;
			elog(DEBUG2, "Master promotion started");

			LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

			ReplPromotionData->promotion_in_progress = true;
			ReplPromotionData->promotion_block_relations = true;

			LWLockRelease(ReplicationLock);
		}
		else
			master_promotion = master_promotion_cancelled;

		DISPLAY_MASTER_PROMOTION_STATE;

	}

	if (rm->flags & MCP_MSG_FLAG_PROMOTE_MAKE)
	{
		if (master_promotion == master_promotion_wait_make)
		{
			/* Make sure all the data from queue were sent */
#ifdef USE_ASSERT_CHECKING
			if (assert_enabled)
			{
				LockReplicationQueue(q, LW_SHARED);
				/* 
				 * Check if either we have sent all the data from the queue or
				 * that the queue is empty.
				 */
				Assert(MCPQueueGetLastRecno(q) < MCPQueueGetFirstRecno(q));
				UnlockReplicationQueue(q);
			}
#endif
			master_promotion = master_promotion_completed;
			MasterCompletePromotion();
		}
		else
			master_promotion = master_promotion_cancelled;

		DISPLAY_MASTER_PROMOTION_STATE;
	}
	MCPReleaseMsg(rm);

	/* Did we change the status of the queue? */
	MasterActOnQueueStatus(state);

	/*
	 * If the above code produced stuff which should be sent to the MCP server,
	 * do it.  This keeps the invariant that we only ever read a single message
	 * from the MCP server before starting to send stuff to it.
	 */
	SendQueuedMessages(state);
}

/* error context callback.  It could be improved. */
static void
connect_callback(void *arg)
{
	errcontext("master connecting to MCP server");
}

/* error context callback.  It could be improved. */
static void
authenticate_callback(void *arg)
{
	errcontext("master authenticating to MCP server");
}

