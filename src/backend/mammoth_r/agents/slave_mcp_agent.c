/*
 * slave_mcp_agent.c
 *
 * 		Slave-side replication code, also known as Replication Process or
 * 		Slave Queue Process (SQP)
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group.
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: slave_mcp_agent.c 2147 2009-05-25 10:09:05Z alexk $
 */
#include "postgres.h"

#include <time.h>

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/pg_namespace.h"
#include "catalog/replication.h"
#include "commands/async.h"
#include "commands/dbcommands.h"
#include "libpq/pqsignal.h"
#include "libpq/libpq.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/backend_tables.h"
#include "mammoth_r/collector.h"
#include "mammoth_r/forwcmds.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/promotion.h"
#include "mammoth_r/mcp_api.h"
#include "mammoth_r/mcp_compress.h"
#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/txlog.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/replication.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


typedef enum
{
	slave_no_promotion,
	slave_promotion_started,
	slave_promotion_wait_ready,
	slave_promotion_wait_restore,
	slave_promotion_send_ready,
	slave_promotion_wait_make,
	slave_promotion_completed,
	slave_promotion_cancelled
} SlavePromotionState;

static SlavePromotionState 	slave_promotion = slave_no_promotion;

#define SlavePromotionAsString(_sp) \
	(((_sp) == slave_no_promotion) ? "none" : \
	((_sp) == slave_promotion_started) ? "started" : \
	((_sp) == slave_promotion_wait_ready) ? "wait_ready" : \
	((_sp) == slave_promotion_wait_restore) ? "wait_restore" : \
	((_sp) == slave_promotion_send_ready) ? "send_ready" : \
	((_sp) == slave_promotion_wait_make) ? "wait_make" : \
	((_sp) == slave_promotion_completed) ? "completed" : \
	((_sp) == slave_promotion_cancelled) ? "cancelled" : \
	"unknown")

typedef enum
{
	slave_no_force_promotion,
	slave_force_promotion_started,
	slave_force_promotion_wait_ready,
	slave_force_promotion_wait_restore,
	slave_force_promotion_completed,
	slave_force_promotion_cancelled
} SlaveForcePromotionState;

static SlaveForcePromotionState slave_force_promotion = slave_no_force_promotion;

#define SlaveForcePromotionAsString(_sfp) \
	(((_sfp) == slave_no_force_promotion) ? "none" : \
	((_sfp) == slave_force_promotion_started) ? "started" : \
	((_sfp) == slave_force_promotion_wait_ready) ? "wait_ready" : \
	((_sfp) == slave_force_promotion_wait_restore) ? "wait_restore" : \
	((_sfp) == slave_force_promotion_completed) ? "completed" : \
	((_sfp) == slave_force_promotion_cancelled) ? "cancelled" : \
	"unknown")

#define DISPLAY_SLAVE_PROMOTION_STATES(elevel) \
	elog(elevel, \
		"promotion: %s, " \
		"force_promotion: %s", SlavePromotionAsString(slave_promotion), \
		 SlaveForcePromotionAsString(slave_force_promotion))

/* minimal interval in seconds between sequential vacuum calls */
#define PRUNE_MIN_INTERVAL		5

typedef struct SlaveState
{
	bool 	bss_dump_inprogress;

	ullong 	bss_mcp_ack_recno;

	/* consider dump fully restored when we restore past this recno */
	ullong 	bss_dump_end_recno;

	int 	bss_slaveno;
	MCPQueue *slave_mcpq;
} SlaveState;

/*
 * These variables are outside ReplicationSlaveMain to avoid being restored
 * by longjmp().  XXX -- wouldn't it be enough to mark them 'volatile'?
 */
/* Slave global state */
static SlaveState	global_state;
/*flag to tell whether we're starting up */
static bool		doing_socket_setup = false;
/* slave should send tablelist to MCP if this flag is set */
static bool		slave_should_send_tablelist;

MemoryContext SlaveContext;


static void SlaveSendTablesToMCP(void);
static void SlaveReceiveMessage(SlaveState *state);
static void SlaveSendTablelist(SlaveState *state);
static void SlaveSendPromotionMsg(int flag);
static void SlaveSendAck(SlaveState *state);
static void SlaveRestoreData(SlaveState *state);
static void SlaveStartPromotion(void);
static void connect_callback(void *arg);
static void authenticate_callback(void *arg);
static bool SlaveMessageHook(bool after, MCPMsg *msg, void *arg);
static bool slave_pre_commit_actions(MCPMsg *msg, SlaveState *state);
static void slave_post_commit_actions(MCPMsg *msg, SlaveState *state);

int
ReplicationSlaveMain(MCPQueue *q, int hostno)
{
	sigjmp_buf	local_sigjmp_buf;

	/* forwarder config */
	char	   *forwname;
	char	   *forwaddr;
	int			forwport;
	bool		forwssl;
	char	   *forwkey;
	char 	   *encoding;

	/* when the last vacuum call finished */
	time_t		last_prune_time = time(NULL);

	/* the time at which we received the last message */
	time_t		timein;
	bool		batch_mode;
	uint64		sysid;
	SlaveState *state = &global_state;
	ullong		initial_recno;

	ErrorContextCallback errcontext;
	SlaveContext = NULL;

	Assert(replication_enable && replication_slave);

	slave_should_send_tablelist = true;

	/* Initialize slave's state structure members */
	state->bss_dump_inprogress = false;
	state->bss_mcp_ack_recno = InvalidRecno;
	state->bss_dump_end_recno = InvalidRecno;
	state->bss_slaveno = hostno;
	state->slave_mcpq = q;

	slave_promotion = slave_no_promotion;
	slave_force_promotion = slave_no_force_promotion;

	if (replication_perform_encoding_conversion)
	{
		/*
	 	 * Set client_encoding to UTF8 to convert stuff flowing from the MCP
	 	 * server.  We need to be in a transaction for this to work.
	 	 */
		StartTransactionCommand();
		SetConfigOption("client_encoding", "utf8", PGC_S_SESSION, PGC_S_CLIENT);
		CommitTransactionCommand();
	}

	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		ErrorData	*edata;
		bool	ret = false;
		bool	disconnect = false;

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

		/* If we have an active transaction - close it */
		if (MCPQueueGetDatafile(q) != NULL)
			MCPQueueTxClose(q);

		/* call module-specific cleanup routines */
		MCPFileCleanup();

		/* cancel active promotions */
		if (slave_promotion != slave_no_promotion)
			slave_promotion = slave_promotion_cancelled;
		else if (slave_force_promotion == slave_no_force_promotion)
			slave_force_promotion = slave_force_promotion_cancelled;

		/* Do the recovery */
		AbortOutOfAnyTransaction();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */

		MemoryContextSwitchTo(TopMemoryContext);

		/* Examine the error code */
		edata = CopyErrorData();
		disconnect = (edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN ||
		 			  edata->sqlerrcode == ERRCODE_CONNECTION_FAILURE);
		FreeErrorData(edata);

		if (disconnect)
			elog(WARNING, "peer disconnected");

		FlushErrorState();

		/* don't keep trying if there was a problem with the socket */
		if (doing_socket_setup || disconnect)
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
		if (SlaveContext != NULL)
		{
			MemoryContextDelete(SlaveContext);
			SlaveContext = NULL;
		}

		/* Now we can resume interrupts */
		InterruptHoldoffCount = 0;

		if (ret)
		{
			/* don't fill the logs as fast as we can */
			pg_usleep(1000000L);
			return 0;
		}

		elog(WARNING, "error detected during restore process; requesting dump");
		/* don't fill the logs as fast as we can */
		pg_usleep(1000000L);

		LockReplicationQueue(q, LW_EXCLUSIVE);
		MCPQueueSetSync(q, MCPQUnsynced);
		UnlockReplicationQueue(q);
	}
	PG_exception_stack = &local_sigjmp_buf;

	/* this context is where we keep all of our stuff */
	SlaveContext = AllocSetContextCreate(TopMemoryContext,
										 "SlaveContext",
										 ALLOCSET_SMALL_MINSIZE,
										 ALLOCSET_SMALL_INITSIZE,
										 ALLOCSET_SMALL_MAXSIZE);
	MemoryContextSwitchTo(SlaveContext);

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
	errcontext.arg = &hostno;
	errcontext.previous = error_context_stack;
	error_context_stack = &errcontext;

	/* during connection and authentication, errors are handled specially */
	doing_socket_setup = true;

	MyProcPort = OpenMcpConnection(forwname, forwaddr, forwport);
	SendMcpStartupPacket(MyProcPort, forwssl, CLIENT_ROLE_SLAVE, hostno);

	errcontext.callback = authenticate_callback;

	/* grab our sysid */
	LWLockAcquire(ControlFileLock, LW_SHARED);
	sysid = ControlFile->system_identifier;
	LWLockRelease(ControlFileLock);

	/* authenticate to server */
	if (McpAuthenticateToServer(sysid, forwkey, encoding) == STATUS_ERROR)
		elog(ERROR, "problem authenticating to server");

	/* socket setup done, revert to normal behavior */
	error_context_stack = errcontext.previous;
	doing_socket_setup = false;

	elog(LOG, "slave %d connected to MCP", hostno);

	/* Is this a demoted master? */
	LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

	ReplPromotionData->promotion_in_progress = false;
	ReplPromotionData->promotion_block_relations = false;

	LWLockRelease(ReplicationLock);

	/*
	 * Send the initial recno, or InvalidRecno if not in sync.  The MCP process
	 * is expected to start sending a dump if required.
	 */
	LockReplicationQueue(q, LW_SHARED);
	initial_recno = MCPQueueGetSync(q) == MCPQSynced ?
		MCPQueueGetLastRecno(q) : InvalidRecno;
	UnlockReplicationQueue(q);
	MCPSendInitialRecno(initial_recno);

	/* Send the tablelist at connection start */
	SlaveSendTablelist(state);

	/*
	 * Batch mode shenanigans.  If batch mode timeout is zero or negative,
	 * mark the feature as disabled.
	 */
	if (replication_slave_batch_mode_timeout <= 0 ||
		!replication_slave_batch_mode)
	{
		batch_mode = false;
		timein = 0;	/* keep compiler quiet */
	}
	else
	{
		batch_mode = true;
		timein = time(NULL);
	}

	while (true)
	{
		int		ret;
		time_t	secs;

		MCPQueueLogHdrStatus(DEBUG4, q, "slave");

		/* If promotion was requested, initiate the promotion process. */
		if (promotion_request)
		{
			LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);

			promotion_request = false;
			/* Cancel setting promotion_request on receiving sigusr1 */
			ReplSignalData->promotion = false;

			if (!ReplPromotionData->promotion_in_progress)
			{
				SlaveStartPromotion();

				/*
			 	 * We shouldn't be starting a new promotion if we have
			 	 * an existing one in progress.
			 	 */
				Assert(slave_force_promotion == slave_no_force_promotion &&
					   slave_promotion == slave_no_promotion);

				if (ReplPromotionData->force_promotion)
					slave_force_promotion = slave_force_promotion_wait_ready;
				else
					slave_promotion = slave_promotion_wait_ready;
			}
			else
			{
				/*
				 * A race condition happened and one or several of our backends
				 * managed to send us multiple promotion signals before we have
				 * set promotion_in_progress in SQP.
				 */
				elog(WARNING, "Multiple promotion requests received");
			}

			/*
			 * Since we have already set a promotion type in the state variables
			 * we can just clear shmem promotion data.
			 */
			ReplPromotionData->force_promotion = false;
			ReplPromotionData->back_promotion = false;

			LWLockRelease(ReplicationLock);

			DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);
		}

		/* Prepare to sleep; set a timeout if the configuration requires it */
		if (batch_mode)
			secs = time(NULL) + replication_slave_batch_mode_timeout;
		else
			secs = (time_t) -1;

		ret = mcpWaitTimed(MyProcPort, true, false, secs);
		if (ret < 0)
		{
			if (errno == EINTR)
				elog(NOTICE, "select interrupted by signal");
			else
				elog(ERROR, "select failed: %m");
		}
		else if (ret > 0)
		{
			/*
			 * We receive a single message, and empty the socket buffer in the
			 * loop below.
			 *
			 * We don't need to restore the messages right away, because they
			 * usually come in batches of transactions, and the batches are not
			 * processed until they are complete.
			 */
			SlaveReceiveMessage(state);

			/* We do send ACKs as soon as we can, however */
			if ((state->bss_mcp_ack_recno != InvalidRecno) &&
				!state->bss_dump_inprogress)
				SlaveSendAck(state);

			if (batch_mode)
				timein = time(NULL);
		}

		/* Get stuff from the libpq's buffer until we empty it */
		while (MCPMsgAvailable())
		{
			SlaveReceiveMessage(state);

			/*
			 * As above, we send ACKs as soon as we can, but don't restore
			 * right away
			 */
			if ((state->bss_mcp_ack_recno != InvalidRecno) &&
				!state->bss_dump_inprogress)
				SlaveSendAck(state);

			/* we don't update "timein" here */
		}

		/* Now we can restore the messages we accumulated */
		for (;;)
		{
			SlaveRestoreData(state);

			if (slave_should_send_tablelist)
				SlaveSendTablelist(state);
			else
				break;
		}

		DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);

		/* Deal with post-restore promotion states */
		if (slave_promotion == slave_promotion_send_ready)
		{
			/* Send promotion ready message to MCP */
			SlaveSendPromotionMsg(MCP_MSG_FLAG_PROMOTE_SLAVE_READY);
			slave_promotion = slave_promotion_wait_make;
		}
		else if (slave_force_promotion == slave_force_promotion_completed)
		{
			SlaveCompletePromotion(true);
		}
		else if (slave_promotion == slave_promotion_cancelled ||
				 slave_force_promotion == slave_force_promotion_cancelled)
		{
			/* discard current promotion and send a cancellation request */
			SlaveSendPromotionMsg(MCP_MSG_FLAG_PROMOTE_CANCEL);

			slave_promotion = slave_no_promotion;
			slave_force_promotion = slave_no_force_promotion;

			LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);
			ReplPromotionData->promotion_in_progress = false;
			LWLockRelease(ReplicationLock);
		}

		/*
		 * Prune the queue if necessary and possible.  Make sure we wait at
		 * least PRUNE_MIN_INTERVAL seconds before vacuuming.
		 */
		if (difftime(time(NULL), last_prune_time) >= PRUNE_MIN_INTERVAL)
		{
			LockReplicationQueue(q, LW_EXCLUSIVE);
			MCPQueuePrune(q);
			UnlockReplicationQueue(q);

			last_prune_time = time(NULL);
		}

		/*
		 * In batch mode, disconnect when the timeout since the last message
		 * has elapsed
		 */
		if (batch_mode)
		{
			time_t	curtime;

			curtime = time(NULL);

			if (curtime - timein >= replication_slave_batch_mode_timeout)
				break;
		}
	}

	pq_comm_reset();
	StreamClose(MyProcPort->sock);

	return 1;
}

/*
 * Slave restore data. Restore is called only when there is unrestored data in
 * the queue and this data belongs to a new transaction which has been
 * completely received. If the function interrupts while not all available data
 * were restored (most likely to do some actions spawned by the already restored
 * data, like sending table list to MCP) it returns false.
 */
static void
SlaveRestoreData(SlaveState *state)
{
	MCPQueue	*q = state->slave_mcpq;

	LockReplicationQueue(q, LW_EXCLUSIVE);
	ereport(DEBUG4,
			(errmsg("RESTORE STARTED"),
			 errcontext("lrecno: "UNI_LLU"\tfrecno: "UNI_LLU"\tvrecno: "UNI_LLU,
						MCPQueueGetLastRecno(q),
						MCPQueueGetFirstRecno(q),
						MCPQueueGetAckRecno(q))));

	/* Process only confirmed received data */
	while (MCPQueueGetFirstRecno(q) <= MCPQueueGetLastRecno(q) &&
		   MCPQueueGetFirstRecno(q) <= MCPQueueGetAckRecno(q))
	{
		TxDataHeader	hdr;
		ullong			recno;

		recno = MCPQueueGetFirstRecno(q);
		elog(DEBUG4, "RESTORING transaction recno "UNI_LLU, recno);

		/*
		 * If we don't have to restore the transaction at recno, proceed to
		 * the next one
		 */
		if (!TXLOGIsCommitted(recno))
		{
			MCPQueueNextTx(q);
			continue;
		}

		MCPQueueTxOpen(q, recno);

		MCPQueueReadDataHeader(q, &hdr);

		if (hdr.dh_flags & MCP_QUEUE_FLAG_DUMP_START)
			SlaveTruncateAll(state->bss_slaveno);
		else
		{
			if (MCPQueueGetSync(q) != MCPQSynced)
			{
				elog(DEBUG2, "queue is not in sync, restore cancelled");
				goto final;
			}
		}

		/* Restore this transaction, but only if we don't have to skip it */
		if (!(hdr.dh_flags & MCP_QUEUE_FLAG_EMPTY))
			PGRRestoreData(q, state->bss_slaveno);
		else
			elog(DEBUG2, "restore not launched, transaction is empty");
			
		elog(DEBUG4, "PGRRestoreData done: frecno="UNI_LLU, 
					  MCPQueueGetFirstRecno(q));
			

		MCPQueueTxClose(q);
		MCPQueueNextTx(q);
		MCPQueueSetDequeueTimestamp(q);

		/* Check if we have to send table list to MCP */
		if (hdr.dh_flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
		{
			slave_should_send_tablelist = true;
			elog(DEBUG2, "Catalog dump received: sending table list");
			break;
		}
	}

	ereport(DEBUG4,
			(errmsg("RESTORE STOPPED"),
			 errcontext("lrecno: "UNI_LLU"\tfrecno: "UNI_LLU"\tvrecno: "UNI_LLU,
						MCPQueueGetLastRecno(q),
						MCPQueueGetFirstRecno(q),
						MCPQueueGetAckRecno(q))));

	/* Check if all the data were restored and we can go on with promotion */
	if (MCPQueueGetFirstRecno(q) > MCPQueueGetLastRecno(q))
	{
		if (slave_promotion == slave_promotion_wait_restore)
			slave_promotion = slave_promotion_send_ready;
		else if (slave_force_promotion == slave_force_promotion_wait_restore)
			slave_force_promotion = slave_force_promotion_completed;
		DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);
	}

final:
	UnlockReplicationQueue(q);
}

/*
 * Send the table list to the MCP server.
 */
static void
SlaveSendTablelist(SlaveState *state)
{
	if (!slave_should_send_tablelist)
		return;

	/* reset the flag */
	slave_should_send_tablelist = false;

	/* OK, go ahead */
	elog(DEBUG2, "sending table list to MCP");
	StartTransactionCommand();
	ActiveSnapshot = CopySnapshot(GetTransactionSnapshot());
	SlaveSendTablesToMCP();
	CommitTransactionCommand();
}

static void
SlaveSendPromotionMsg(int flag)
{
	MCPMsg		sm;

	MemSet(&sm, 0, sizeof(MCPMsg));
	sm.flags |= flag;
	sm.recno = InvalidRecno;

	MCPSendMsg(&sm, true);
	MCPMsgPrint(DEBUG3, "Slave-SendMsg", &sm);
}

static void
SlaveSendAck(SlaveState *state)
{
	MCPMsg          sm;

	if (state->bss_mcp_ack_recno == InvalidRecno)
		return;

	MemSet(&sm, 0, sizeof(MCPMsg));
	sm.flags |= MCP_MSG_FLAG_ACK;
	sm.recno = state->bss_mcp_ack_recno;
	MCPMsgPrint(DEBUG3, "slave send ACK", &sm);
	MCPSendMsg(&sm, true);

	/* Set the queue's AckRecno to what we just sent */
	LockReplicationQueue(state->slave_mcpq, LW_EXCLUSIVE);
	MCPQueueSetAckRecno(state->slave_mcpq, state->bss_mcp_ack_recno);
	UnlockReplicationQueue(state->slave_mcpq);

	/* and update our internal state */
	state->bss_mcp_ack_recno = InvalidRecno;
}

static bool 
SlaveMessageHook(bool after, MCPMsg *msg, void *arg)
{
	bool		skip = false;
	SlaveState *state =  (SlaveState *) arg;

	if (!after)
		skip = slave_pre_commit_actions(msg, state);
	else
		slave_post_commit_actions(msg, state);

	return skip;
}

static bool
slave_pre_commit_actions(MCPMsg *msg, SlaveState *state)
{
	bool skip = false;

	/* Process message and decide whether to put it to the queue */
	if (msg->flags & MCP_MSG_FLAG_PROMOTE_CANCEL)
	{
		/*
		 * If a slave receives a promotion cancel message, unset the
		 * ReplPromotionData->promotion_in_progress flag and forget 
		 * about the promotion.
		 */
		slave_promotion = slave_no_promotion;
		slave_force_promotion = slave_no_force_promotion;

		LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);
		ReplPromotionData->promotion_in_progress = false;
		LWLockRelease(ReplicationLock);

		elog(WARNING, "promotion cancelled by MCP server");

		DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);
	}
	else if (msg->flags & MCP_MSG_FLAG_PROMOTE_READY)
	{
		elog(DEBUG2, "received PROMOTE_READY message");
		DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);

		if (slave_promotion == slave_promotion_wait_ready)
			slave_promotion = slave_promotion_wait_restore;
		else
		{
			DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);
			ereport(WARNING,
					(errmsg("promotion cancelled"),
					 errdetail("Received PROMOTE_READY with invalid slave "
							   "promotion state.")));

			if (slave_promotion != slave_no_promotion)
				slave_promotion = slave_promotion_cancelled;
		}
	}
	else if (msg->flags & MCP_MSG_FLAG_PROMOTE_FORCE)
	{
		if (slave_force_promotion == slave_force_promotion_wait_ready)
			slave_force_promotion = slave_force_promotion_wait_restore;
		else
		{
			DISPLAY_SLAVE_PROMOTION_STATES(DEBUG2);

			ereport(WARNING,
					(errmsg("force promotion cancelled"),
					 errdetail("Received PROMOTE_FORCE with invalid slave "
							   "force promotion state.")));

			if (slave_force_promotion != slave_no_force_promotion)
				slave_force_promotion = slave_force_promotion_cancelled;
		}
	}
	else if (msg->flags & MCP_MSG_FLAG_PROMOTE_MAKE)
	{
		if (slave_promotion == slave_promotion_wait_make)
		{
			slave_promotion = slave_promotion_completed;
			/* Complete the promotion */
			SlaveCompletePromotion(false);
		}
		else
		{
			ereport(WARNING,
					(errmsg("promotion cancelled"),
					 errdetail("Received PROMOTE_MAKE when slave promotion "
							   "state was %s.",
							   SlavePromotionAsString(slave_promotion))));
			slave_promotion = slave_promotion_cancelled;
		}   
	}
	else
	{
		/*
		 * Truncate the MCP queue if MCP_QUEUE_FLAG_TRUNC is received from
		 * master.
		 */
		if (msg->flags & MCP_QUEUE_FLAG_TRUNC)
		{
			ullong recno = *((ullong *)msg->data);

			elog(LOG, "TRUNCATE and SET SYNC to "UNI_LLU, recno);

			LockReplicationQueue(state->slave_mcpq, LW_EXCLUSIVE);
			MCPQueueCleanup(state->slave_mcpq, recno);
			MCPQueueSetSync(state->slave_mcpq, MCPQSynced);
			UnlockReplicationQueue(state->slave_mcpq);
		}

		if (msg->flags & MCP_QUEUE_FLAG_DUMP_START)
		{
			ereport(LOG,
					(errmsg("received start of dump"),
					 errdetail("Record number "UNI_LLU, msg->recno)));
					
			state->bss_dump_inprogress = false;
		}
		else if (msg->flags & MCP_QUEUE_FLAG_DUMP_END)
		{
			ereport(LOG,
					(errmsg("received end of dump"),
					 errdetail("Record number "UNI_LLU, msg->recno)));
			state->bss_dump_end_recno = msg->recno;
		}

		/* Actions on DATA or DUMP transaction */
		if (msg->flags & MCP_QUEUE_FLAG_DATA ||
			MessageTypeIsDump(msg->flags))
		{
			elog(DEBUG4, "received transaction "UNI_LLU, msg->recno);
			state->bss_mcp_ack_recno = msg->recno;
		}
	}

	/* Do not put messages with InvalidRecno to the queue */

	if (msg->recno != InvalidRecno)
		skip = false;
	else
		skip = true;

	return skip;

}
static void
slave_post_commit_actions(MCPMsg *msg, SlaveState *state)
{
	/* Process messages when they are already in queue */
	if (msg->flags & MCP_QUEUE_FLAG_DUMP_START)
	{
		Assert(msg->recno != InvalidRecno);

		/* 
		 * Set current recno to the first recno of the dump,
		 * skipping all the pre-dump messages in queue.
		 */
		LockReplicationQueue(state->slave_mcpq, LW_EXCLUSIVE);
		MCPQueueSetFirstRecno(state->slave_mcpq, msg->recno);
		UnlockReplicationQueue(state->slave_mcpq);

	}
	if (msg->recno != InvalidRecno)
	{
		LockReplicationQueue(state->slave_mcpq, LW_EXCLUSIVE);
		MCPQueueTxCommit(state->slave_mcpq, msg->recno);
		TXLOGSetCommitted(msg->recno);
		UnlockReplicationQueue(state->slave_mcpq);
	}
	else
		elog(ERROR, "Message with recno 0 found in the queue");
}
/*
 * Receive a single message from the MCP server.
 */
static void
SlaveReceiveMessage(SlaveState *state)
{
	MCPMsg	   *rm;

	rm = ReceiveQueueTransaction(state->slave_mcpq, NULL, NULL, 
								 SlaveMessageHook, (void *) state);

	MCPReleaseMsg(rm);
}

static void
SlaveSendTablesToMCP(void)
{
	MCPMsg	   *sm;
	List	   *relids,
			   *replicated_rels,
			   *special_rels = NIL;
	ListCell   *cell;
	uint32		flags;
	WireBackendTable wt;
	int				length,
					total, i;

	sm = palloc(sizeof(MCPMsg) + sizeof(WireBackendTableData) + MAX_REL_PATH);
	flags = MCP_MSG_FLAG_TABLE_LIST_BEGIN;

	relids = get_master_and_slave_replicated_relids(replication_slave_no);

	/*
	 * Get the list of replicated relations to check if we should request
	 * dumps for each of them. XXX: The 'dump required' flag should probably be
	 * in the repl_slave_relations catalog to avoid having several lists of
	 * replicated tables.
	 */
	replicated_rels = RestoreTableList(REPLICATED_LIST_PATH);
	
	/* 
	 * Make a list of special relations. Special are relations that are not
	 * receiving data directly like other replicated relations, but used to
	 * indicate replication of some database object (i.e. large objects, roles)
	 */
	special_rels = lappend(special_rels, pstrdup("pg_catalog.pg_largeobject"));
	special_rels = lappend(special_rels, pstrdup("pg_catalog.pg_authid"));
	special_rels = lappend(special_rels, pstrdup("pg_catalog.pg_auth_members"));

	/* Send each table to the MCP in a new message */
	foreach(cell, relids)
	{
		Oid         relid,
					nspid;
		Relation    rel;
		char       *nspname,
				   *relname;
		BackendTable replicated;

		relid = lfirst_oid(cell);
		nspid = get_rel_namespace(relid);

		/* Sanity checks */
		Assert(relid != InvalidOid);
		Assert(nspid != InvalidOid);

		/* Skip relations from system catalog namespace */
		if (nspid == PG_CATALOG_NAMESPACE)
			continue;

		/* 
		 * Open relation to check whether it still exists. Keep it
		 * open to make sure it won't gone until we form a list
		 */
		rel = relation_open(relid, AccessShareLock);

		MemSet(sm, 0, sizeof(MCPMsg) + sizeof(WireBackendTableData) +
			   MAX_REL_PATH);

		sm->flags = MCP_MSG_FLAG_TABLE_LIST | flags;

		wt = (WireBackendTable) sm->data;

		nspname = get_namespace_name(nspid);
		relname = get_rel_name(relid);

		Assert(relname != NULL);
		Assert(nspname != NULL);

		length = snprintf(wt->relpath, MAX_REL_PATH, "%s.%s",
						  nspname, relname);

		pfree(relname);
		pfree(nspname);

		sm->datalen = sizeof(WireBackendTableData) + length;

		/* Get a table from replicated list for the currect relid */
		replicated = (BackendTable)
			TableListEntryByName(replicated_rels, wt->relpath);

		Assert(replicated ? replicated->id == TABLEID_BACKEND : true);

		/* If there is not matching table - don't ask for dump */
		wt->raise_dump = replicated ? replicated->raise_dump : TableNoDump;

		elog(DEBUG3, "sending table %s (raise dump: %d) to MCP server",
			 wt->relpath, wt->raise_dump);
		MCPSendMsg(sm, false);

		/* Close target relation */
		relation_close(rel, AccessShareLock);

		flags = 0;
	}

	/* 
	 * XXX: HACK
	 * Send special relations and flush the queue after the last of them is sent
	 */
	total = list_length(special_rels);
	i = 0;
	foreach(cell, special_rels)
	{
		MemSet(sm, 0, sizeof(MCPMsg) + sizeof(WireBackendTableData) +
		   	   MAX_REL_PATH);
		sm->flags = MCP_MSG_FLAG_TABLE_LIST; 
		if (++i == total )
			sm->flags |= MCP_MSG_FLAG_TABLE_LIST_END;
		wt = (WireBackendTable) sm->data;
		/* Never ask for a dump of special relations */
		wt->raise_dump = TableNoDump;
		length = snprintf(wt->relpath, MAX_REL_PATH, "%s",
					  	 (char *)lfirst(cell));
		sm->datalen = sizeof(WireBackendTableData) + length;

		elog(DEBUG3, "sending table %s (raise dump: %d) to MCP server",
		 	 wt->relpath, wt->raise_dump);

		MCPSendMsg(sm, (i == total) ? true: false);

	}
	pfree(sm);
	list_free_deep(replicated_rels);
	list_free_deep(special_rels);
}

/*
 * Try to start promotion, running the corresponding function for the force
 * and back promotion types.
 */
static void
SlaveStartPromotion(void)
{
	Assert(LWLockHeldByMe(ReplicationLock));
	/*
	 * Set promotion in progress flag.  We'll clear it on slave, after
	 * the end of the promotion or upon receiving PROMOTION_CANCEL
	 * message from MCP.
	 */
	ReplPromotionData->promotion_in_progress = true;

	if (!ReplPromotionData->back_promotion)
		SlaveInitiatePromotion(ReplPromotionData->force_promotion);
	else
	{
		/*
		 * Clear force promotion flag in case it was set before.
		 * Force promotion is not compatible with PROMOTE BACK.
		 */
		InitiateBackPromotion();
	}
}

/* error context callback.  It could be improved. */
static void
connect_callback(void *arg)
{
	int		slaveno = *(int *) arg;
	errcontext("slave %d connecting to MCP server", slaveno);
}

/* error context callback.  It could be improved. */
static void
authenticate_callback(void *arg)
{
	int		slaveno = *(int *) arg;

	errcontext("slave %d authenticating to MCP server", slaveno);
}
