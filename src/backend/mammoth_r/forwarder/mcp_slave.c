/*
 * mcp_slave.c
 *		MCP server slave process implementation
 *
 * $Id: mcp_slave.c 2186 2009-06-25 12:14:51Z alexk $
 */
#include "postgres.h"

#include <signal.h>

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
#include "nodes/bitmapset.h"
#include "storage/ipc.h"
#include "utils/ps_status.h"


typedef enum
{
	slave_promotion_none,
	slave_promotion_wait_ready,
	slave_promotion_send_ready,
	slave_promotion_wait_slave_ready,
	slave_promotion_send_make,
	slave_promotion_completed,
	slave_promotion_cancelled
} MCPSlavePromotionState;

typedef enum
{
	slave_force_promotion_none,
	slave_force_promotion_check_master,
	slave_force_promotion_accepted,
	slave_force_promotion_completed,
	slave_force_promotion_cancelled
} MCPSlaveForcePromotionState;

typedef struct SlaveStatus
{
	/*
	 * Wait for the table list from slave and don't send anything except dump
	 * messages to this slave
	 */
	bool		ss_wait_list;
	List	   *ss_tablelist;
	List	   *ss_recv_list;
	MCPQueue   *ss_queue;
	MCPHosts   *ss_hosts;
	int			ss_hostno;
	ullong		ss_wait_list_recno;
	MCPSlavePromotionState ss_promotion;
	MCPSlaveForcePromotionState ss_force_promotion;
	uint32		current_tablelist_rev;
	pg_enc 		peer_encoding;
} SlaveStatus;

#define PromotionAsString(prom) \
	(((prom) == slave_promotion_none) ? "none" : \
	 ((prom) == slave_promotion_wait_ready) ? "wait ready" : \
	 ((prom) == slave_promotion_send_ready) ? "send ready" : \
	 ((prom) == slave_promotion_wait_slave_ready) ? "wait slave ready" : \
	 ((prom) == slave_promotion_send_make) ? "send make" : \
	 ((prom) == slave_promotion_completed) ? "completed" : \
	 ((prom) == slave_promotion_cancelled) ? "cancelled" : \
	 "unknown")

#define ForcePromotionAsString(fprom) \
	(((fprom) == slave_force_promotion_none) ? "none" : \
	 ((fprom) == slave_force_promotion_check_master) ? "check master" : \
	 ((fprom) == slave_force_promotion_accepted) ? "accepted" : \
	 ((fprom) == slave_force_promotion_completed) ? "completed" : \
	 ((fprom) == slave_force_promotion_cancelled) ? "cancelled" : \
	 "unknown")

#define LOG_PROMOTION_STATES(elevel, status) \
	(elog(elevel, "slave promotion: %s, force promotion: %s", \
		 PromotionAsString(status->ss_promotion), \
		 ForcePromotionAsString(status->ss_force_promotion)))
	

static void SlaveMainLoop(SlaveStatus *status);
static void SlaveCorrectQueue(SlaveStatus *status);
static void check_sync_status(SlaveStatus *status);
static void SlaveSendMessages(SlaveStatus *status);
static void SlaveSendQueuedMessages(SlaveStatus *status);
static void SlaveSendDirectMessages(SlaveStatus *status);
static void ReceiveSlaveMessage(SlaveStatus *status);
static bool ProcessSlaveDumpRequest(SlaveStatus *status);
static void ProcessForcePromotion(SlaveStatus *status);
static bool IsPromotionAllowed(int hostno);
static void RecvSlaveTable(SlaveStatus *state, MCPMsg *msg);
static void procexit_slave_cleanup(int code, Datum arg);

static void MasterNextPromotionState(SlaveStatus *status);
static void MCPSlaveActOnPromotionSignal(SlaveStatus *status);
static void MCPSlaveCancelPromotion(SlaveStatus *status);
static void SlaveStartPromotion(SlaveStatus *status, bool force);
static void SlaveMergeTableLists(SlaveStatus *state, ullong recno);
static void SlaveStoreTableList(SlaveStatus *state);
static void SlaveRestoreTableList(SlaveStatus *state);
static bool MCPSlaveActOnTableRequest(SlaveStatus *state, 
									  MCPTable tab, ullong recno);


void
HandleSlaveConnection(MCPQueue *q, MCPHosts *h, int slave_no, pg_enc encoding)
{
	SlaveStatus *status;
	bool		request_dump;
	uint32		flags;

	set_ps_display("startup", false);

	status = palloc(sizeof(SlaveStatus));

	status->ss_hostno = slave_no;
	status->ss_wait_list = false;
	status->ss_tablelist = NIL;
	status->ss_recv_list = NIL;
	status->current_tablelist_rev = 0;
	status->ss_wait_list_recno = InvalidRecno;

	status->ss_queue = q;
	status->ss_hosts = h;

	/* We are not in promotion right after (re)connection */
	status->ss_promotion = slave_promotion_none;
	status->ss_force_promotion = slave_force_promotion_none;
	
	/* set peer encoding */
	status->peer_encoding = encoding;

	/* 
	 * Check if MCP considers us either a promotion master or a promotion
	 * slave. If that happens, then we probably terminated abnormally without
	 * cleaning our sysid and promotion flags in shared memory.
	 */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	PromotionResetAtStartup(status->ss_hostno + 1);
	LWLockRelease(MCPServerLock);

	/* Load the table list */
	LWLockAcquire(MCPTableListLock, LW_SHARED);
	SlaveRestoreTableList(status);
	LWLockRelease(MCPTableListLock);

	/* register callback to cleanup at finish */
	on_proc_exit(procexit_slave_cleanup, PointerGetDatum(status));

	/* now we can get signalled */
	PG_SETMASK(&UnBlockSig);
	LOG_PROMOTION_STATES(DEBUG2, status);

	/* report queue status */
	MCPQueueLogHdrStatus(DEBUG4, q, "MCP queue");
	MCPHostsLogTabStatus(DEBUG4, h, status->ss_hostno, "Host tab", ServerCtl->node_pid);
		
	LWLockAcquire(MCPServerLock, LW_SHARED);
	elog(DEBUG4, "dump_recno "UNI_LLU, FullDumpGetStartRecno());
	LWLockRelease(MCPServerLock);

	/* make sure we agree with the slave as to what to send next */
	SlaveCorrectQueue(status);

	/* find out if it needs a dump */
	request_dump = false;
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	flags = MCPHostsGetFlags(h, status->ss_hostno);
	
	/* 
	 * Determine whether a new dump is required. 
	 * See comments in check_sync_status for details.
	 */
	if (MCPHostsGetHostRecno(h, McphHostRecnoKindSendNext,
							 status->ss_hostno) == InvalidRecno ||
	   (MCPHostsGetSync(h, status->ss_hostno) == MCPQUnsynced &&
	   !(flags & MCP_HOST_FLAG_ACCEPT_DATA_DURING_DUMP)))					
		request_dump = true;
	LWLockRelease(MCPHostsLock);

	if (request_dump)
		ProcessSlaveDumpRequest(status);

	/* go on forever until we're told to close up */
	SlaveMainLoop(status);

	LOG_PROMOTION_STATES(DEBUG2, status);
}

/*
 * Main loop of the slave process.  If this ever returns, close shop and go
 * home.
 */
static void
SlaveMainLoop(SlaveStatus *status)
{
	while (true)
	{
		int		ret;
		int		secs;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Check if there's something in the socket's input buffer, and
		 * process it before sleeping if so.
		 */
		while (MCPMsgAvailable())
		{
			ReceiveSlaveMessage(status);

			/* send queued messages to the slave */
			SlaveSendMessages(status);

			CHECK_FOR_INTERRUPTS();
		}
		SlaveSendMessages(status);

		/*
		 * If there's a PROMOTE FORCE in progress, we cannot sleep forever --
		 * we must wake soon to check whether the master process answered to our
		 * check request.
		 *
		 * XXX Note that we sleep one second more than actually necessary, to
		 * prevent sleeping twice as long if we are awakened the first time
		 * just before the master checks its state.
		 */
		LWLockAcquire(MCPServerLock, LW_SHARED);

		if (PromotionCtl->master_check_state != master_check_none)
			secs = time(NULL) + ForwarderEchoTimeout + 2;
		else
			secs = -1;

		LWLockRelease(MCPServerLock);
	
		set_ps_display("waiting for slave data", false);

		CHECK_FOR_INTERRUPTS();

		ret = mcpWaitTimed(MyProcPort, true, false, secs);

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
			/* Receive messages from the slave */
			do
			{
				ReceiveSlaveMessage(status);

				CHECK_FOR_INTERRUPTS();
			}
			while (MCPMsgAvailable());
		}
		else if (ret == 0)
			;	/* timeout -- nothing to do */

		if (sigusr1_rcvd)
			sigusr1_rcvd = false;

		if (promotion_signal)
		{
			promotion_signal = false;
			MCPSlaveActOnPromotionSignal(status);
			LOG_PROMOTION_STATES(DEBUG2, status);
		}
		
		if (run_encoding_check)
		{
			pg_enc 	master_encoding;
			
			run_encoding_check = false;
			
			LWLockAcquire(MCPHostsLock, LW_SHARED);
			master_encoding = MCPHostsGetEncoding(status->ss_hosts);
			LWLockRelease(MCPHostsLock);
			
			/* check whether the slave's encoding match the new master's one */
			if (status->peer_encoding != master_encoding)
			{
				ereport(ERROR, 
						(errmsg("master encoding changed, encoding mismatch"),
						 errdetail("slave encoding \"%s\" doesn't match master encoding \"%s\"",
						 pg_encoding_to_char(status->peer_encoding),
						 pg_encoding_to_char(master_encoding))));	
			}
		}

		/*
		 * Process force promotion request - run force promotion only 
		 * if master is disconnected
		 */
		if (status->ss_force_promotion == slave_force_promotion_check_master)
		{
			/* Check if master is connected and send either force
			 * promotion or promotion cancel request to slave
			 */
			ProcessForcePromotion(status);
			LOG_PROMOTION_STATES(DEBUG2, status);
		}
	
		check_sync_status(status);

		CHECK_FOR_INTERRUPTS();

		/* Send messages to the slave */
		SlaveSendMessages(status);
	}
}

/*
 * Set the queue so that the slave will start getting messages from the queue
 * starting from the first one it doesn't have.
 *
 * If master is connected - then we look at the queue to determine whether
 * there is a recno in the queue the slave can resume receiving data from.
 * Otherwise, the slave's data is assumed to be correct, and the check gets
 * postponed until the time the master connects. In both cases, the host
 * gets desynced upon receiving Invalid recno from the slave.
 */ 
 
static void
SlaveCorrectQueue(SlaveStatus *status)
{
	ullong	initial_recno;

	/* get the initial recno from the slave */
	initial_recno = MCPRecvInitialRecno();
	elog(DEBUG3, "received initial recno from slave (%d): "UNI_LLU, 
				 status->ss_hostno, initial_recno);

	/* 
	 * The following code acts differently depending on whether the master
	 * is connected or not; thus, hold the server's lock here to avoid master's
	 * connection in the middle of the code's execution.
	 */
	LWLockAcquire(MCPServerLock, LW_SHARED);
	
	LockReplicationQueue(status->ss_queue, LW_SHARED);
	LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);

	if (initial_recno == InvalidRecno)
	{
		MCPHostsSetSync(status->ss_hosts, status->ss_hostno, MCPQUnsynced);
	}
	else
	{
		if (ServerCtl->node_pid[0] == 0)
		{
			MCPHostsSetHostRecno(status->ss_hosts, McphHostRecnoKindSendNext,
							 	 status->ss_hostno, initial_recno);
			MCPHostsSetHostRecno(status->ss_hosts, McphHostRecnoKindAcked,
							 	 status->ss_hostno, InvalidRecno);
			MCPHostsSetSync(status->ss_hosts, status->ss_hostno, MCPQSynced);
		} else
		{
			/* 
			 * master is connected, use the forwarder's queue to decide
			 * whether to put this slave to desync. 
			 */ 
			ullong fw_brecno = MCPQueueGetInitialRecno(status->ss_queue);
			
			if (initial_recno < fw_brecno)
				MCPHostsSetSync(status->ss_hosts, status->ss_hostno, MCPQResynced);			
		}			
	}

	/* otherwise, it is case (1) -- do nothing */
	LWLockRelease(MCPHostsLock);
	UnlockReplicationQueue(status->ss_queue);
	LWLockRelease(MCPServerLock);
}

static void
check_sync_status(SlaveStatus *status)
{
	bool	unsynced = false;
	uint32	flags;

	/* 
	 * Don't call for a new full dump if the queue is desync, but the flag
	 * is set that indicates that slave is in the middle of sending an
	 * existing dump.
	 */
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	flags = MCPHostsGetFlags(status->ss_hosts, status->ss_hostno);
	if ((MCPHostsGetSync(status->ss_hosts, status->ss_hostno) == MCPQUnsynced)
		&& !(flags & MCP_HOST_FLAG_ACCEPT_DATA_DURING_DUMP))
			unsynced = true;
	LWLockRelease(MCPHostsLock);

	if (unsynced)
	{
		bool	request = false;

		LWLockAcquire(MCPServerLock, LW_SHARED);
		if (FullDumpGetProgress() == FDnone)
			request = true;
		LWLockRelease(MCPServerLock);

		if (request)
			ProcessSlaveDumpRequest(status);
	}
}

/*
 * Cleanup routine, used as a proc_exit callback.
 */
static void
procexit_slave_cleanup(int code, Datum arg)
{
	SlaveStatus *status = (SlaveStatus *) DatumGetPointer(arg);

	elog(DEBUG2, "shutting down slave %d (code: %d)", status->ss_hostno, code);

	/* Close the queue and store the queue header on disk */
	MCPQueueDestroy(status->ss_queue);

	/* Close hosts and write the hosts header as well */
	MCPHostsClose(status->ss_hosts);
}

/* Receive a single table from the slave and add it to the current table list */
static void
RecvSlaveTable(SlaveStatus *state, MCPMsg *msg)
{
	BackendTable	new_table;
	WireBackendTable wt = (WireBackendTable) msg->data;

	/* Make new MCP table and set it as replicated by the current slave */
	new_table = MakeBackendTable(wt->relpath, wt->raise_dump);

	state->ss_recv_list = lappend(state->ss_recv_list, new_table);
}

/*		IsPromotionAllowed
 *
 *	Determines if selected slave is allowed to promote
 */
static bool
IsPromotionAllowed(int hostno)
{
	return list_member_int(ParsedForwarderPromoteAcl, hostno);
}

static void
SlaveSendMessages(SlaveStatus *status)
{
	/* First, send all messages from the queue */
	SlaveSendQueuedMessages(status);

	/* Second, send 'direct' messages */
	SlaveSendDirectMessages(status);
}

static void
SlaveSendDirectMessages(SlaveStatus *status)
{
	MCPMsg sm;
	MCPQSync	sync;

	MemSet(&sm, 0, sizeof(MCPMsg));
	
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	sync = MCPHostsGetSync(status->ss_hosts, status->ss_hostno);
	LWLockRelease(MCPHostsLock);

	if (sync == MCPQResynced)
	{
		/* Special case, we need data in the message sent */
		MCPMsg     *msg;
		ullong		new_initial_recno;
		
		msg = palloc(sizeof(MCPMsg) + sizeof(ullong) - 1);

		LWLockAcquire(MCPHostsLock, LW_SHARED);
		new_initial_recno = MCPHostsGetHostRecno(status->ss_hosts,
												 McphHostRecnoKindSendNext,
												 status->ss_hostno);
		LWLockRelease(MCPHostsLock);
		/* Form a message with the new queue parameters for the slave */
		msg->flags = MCP_MSG_FLAG_QUEUE_CORRECTION;
		msg->recno = InvalidRecno;		
		memcpy(msg->data, &new_initial_recno, sizeof(ullong));
		elog(DEBUG3, "Sending queue correction at recno "UNI_LLU, new_initial_recno);
		
		/* 
		 * Send a message with would set the slave's queue to start
		 * with a record we put in it.
		 */
		MCPMsgPrint(DEBUG3, "Send", msg);
		MCPSendMsg(msg, true);
		
		/* Desync the host to force it to ask for a full dump */
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		MCPHostsSetSync(status->ss_hosts, status->ss_hostno, MCPQUnsynced);
		LWLockRelease(MCPHostsLock);
		
		return;
	}
	/* Other cases (with static message without any data) */

	/* 
	 * If we should send a PROMOTE_READY message at the current step of
	 * promotion and the slave has sent everything from the queue, send it.
	 */
	if (status->ss_promotion == slave_promotion_send_ready)
	{
		/* Check if we have already sent all the queue messages to a slave */
		LockReplicationQueue(status->ss_queue, LW_SHARED);
		LWLockAcquire(MCPHostsLock, LW_SHARED);

		if (MCPHostsGetHostRecno(status->ss_hosts, McphHostRecnoKindSendNext,
								 status->ss_hostno) > 
			MCPQueueGetLastRecno(status->ss_queue))
		{
			sm.flags |= MCP_MSG_FLAG_PROMOTE_READY;
			status->ss_promotion = slave_promotion_wait_slave_ready;
		}
		LWLockRelease(MCPHostsLock);
		UnlockReplicationQueue(status->ss_queue);
		LOG_PROMOTION_STATES(DEBUG2, status);
	}
	else if (status->ss_promotion == slave_promotion_cancelled ||
			 status->ss_force_promotion == slave_force_promotion_cancelled)
	{
		elog(DEBUG2, "cancelling promotion");
		sm.flags |= MCP_MSG_FLAG_PROMOTE_CANCEL;

		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

		/* 
		 * If promotion type is not force - ask master to clear its 
		 * promotion state.
		 */
		if (status->ss_promotion == slave_promotion_cancelled)
		{
			PromotionCtl->promotion_cancelled = true;
			MasterNextPromotionState(status);
		}

		MCPSlaveCancelPromotion(status);

		LWLockRelease(MCPServerLock);
		LOG_PROMOTION_STATES(DEBUG2, status);
	}
	else if (status->ss_promotion == slave_promotion_send_make)
	{
		/* send promote make and finish promotion */
		sm.flags |= MCP_MSG_FLAG_PROMOTE_MAKE;
		status->ss_promotion = slave_promotion_completed;
		LOG_PROMOTION_STATES(DEBUG2, status);
	}
	else if (status->ss_force_promotion == slave_force_promotion_accepted)
	{
		/* Check if we have already sent all the queue data */
		LockReplicationQueue(status->ss_queue, LW_SHARED);
		LWLockAcquire(MCPHostsLock, LW_SHARED);

		if (MCPHostsGetHostRecno(status->ss_hosts, McphHostRecnoKindSendNext,
								 status->ss_hostno) >
			MCPQueueGetLastRecno(status->ss_queue))
		{
			/* Time to die -- send promote force to a slave */
			sm.flags |= MCP_MSG_FLAG_PROMOTE_FORCE;
			status->ss_force_promotion = slave_force_promotion_completed;
		}
		LWLockRelease(MCPHostsLock);
		UnlockReplicationQueue(status->ss_queue);
		LOG_PROMOTION_STATES(DEBUG2, status);
	}

	/* If we should send anything, do it */
	if (sm.flags != 0)
	{
		sm.recno = InvalidRecno;
		MCPMsgPrint(DEBUG3, "Send", &sm);
		MCPSendMsg(&sm, true);
	}
}

/* 
 * SlaveTableListHook
 *		Hook for SendQueueTransaction (1st hook argument)
 *
 * Check if current transaction has tables that are replicated by the slave.
 * If not, return false.
 */
static bool
SlaveTableListHook(TxDataHeader *hdr, List *TableList, void *status_arg)
{
	ListCell    *cell;
	SlaveStatus *state;
	MCPHosts	*hosts;
	MCPQSync	 sync;
	bool	replicate_tx,
			early_skip;
	int32	hostno,
			host_flags;

   
	state = (SlaveStatus *) status_arg;
	hosts = state->ss_hosts;
	hostno = state->ss_hostno;

	replicate_tx = early_skip = false;
	
	
	/* Upon receiving dump start there are 2 possibilities:
	 *  - host is in sync, all dump transactions intil the end of dump
	 *	 should be skipped.
	 *  - host in not in sync, start sending all transactions.
	 */
	if (hdr->dh_flags & MCP_QUEUE_FLAG_DUMP_START) 
	{
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		sync = MCPHostsGetSync(hosts, hostno);
		if (sync == MCPQSynced)
			MCPHostsSetFlags(hosts, hostno, MCP_HOST_FLAG_SKIP_DUMP);
		else
			MCPHostsSetFlags(hosts, hostno, 
							 MCP_HOST_FLAG_ACCEPT_DATA_DURING_DUMP);
		LWLockRelease(MCPHostsLock);
	}
	
	/* 
	 * Clear flag bits set at dump start and set the queue to sync if it was
	 * not in sync previously.
	 */
	if (hdr->dh_flags & MCP_QUEUE_FLAG_DUMP_END) 
	{
		int32 flags, 
			  old_flags;
		
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		
		old_flags = flags = MCPHostsGetFlags(hosts, hostno);
		flags &= ~(MCP_HOST_FLAG_SKIP_DUMP| 
				   MCP_HOST_FLAG_ACCEPT_DATA_DURING_DUMP);
		MCPHostsSetFlags(hosts, hostno, flags);
		sync = MCPHostsGetSync(hosts, hostno);
		
		LWLockRelease(MCPHostsLock);
		
		if (sync != MCPQSynced)
		{
			elog(DEBUG4, "Host %d sync: %s -> MCPQSynced", 
                 hostno, MCPQSyncAsString(sync));
			LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
			MCPHostsSetSync(hosts, hostno, MCPQSynced);
			LWLockRelease(MCPHostsLock);
		}
		/* 
		 * Use the value of host's flags before this transaction
		 * to decide whether to skip it.
		 */
		if (old_flags & MCP_HOST_FLAG_SKIP_DUMP)
			goto final;
	}
	
	
	/* 
	 * Decide to skip this transaction:
	 * - if slave is not in sync and this this transaction is not a part of
	 *	 the full dump.
	 * - if this transaction belongs to the full dump and SKIP_DUMP flag is set.
	 * One important exclusion is that we avoid to skip TABLE DUMP transactions.
	 * The special case, that leads to this decision, is the following:
	 * Consider the new table enabled for replication for the particular slave
	 * during full dump, between the moment the catalog dump transaction is
	 * collected and the moment snapshot is taken for getting the list of
	 * table dumps to collect. Now, a catalog dump, that is not a part of the
	 * full dump, related to recent enable replication, would indicate that
	 * the corresponding table should be replicated by the slave. On the other
	 * hand, the table dump for this table, as a part of the full dump, will
	 * be skipped, and since it goes after the catalog dump, the slave won't
	 * ask for additional table dumps. To avoid this we have to check every
	 * table dump, whether it is part of the full dump or not.
	 */
	LWLockAcquire(MCPHostsLock, LW_SHARED);	
	sync = MCPHostsGetSync(hosts, hostno);
	host_flags = MCPHostsGetFlags(hosts, hostno); 
	
	if ((sync != MCPQSynced && MessageTypeIsData(hdr->dh_flags) &&
		!(host_flags & MCP_HOST_FLAG_ACCEPT_DATA_DURING_DUMP)) 
		||
		(MessageTypeBelongsToFullDump(hdr->dh_flags) &&
		!(hdr->dh_flags & MCP_QUEUE_FLAG_TABLE_DUMP) &&
		(host_flags & MCP_HOST_FLAG_SKIP_DUMP)))
			early_skip = true;
						
	LWLockRelease(MCPHostsLock);
	
	if (early_skip)
	{
		elog(DEBUG3, "Early skip condition was triggered");
		elog(DEBUG3, "Is message dump? %d", MessageTypeBelongsToFullDump(hdr->dh_flags));
		elog(DEBUG3, "Is message data? %d", MessageTypeIsData(hdr->dh_flags));
		elog(DEBUG3, "Host's flags: %d", host_flags);
		goto final;
	}
	
	/* Normal processing of message flags */	
    if (hdr->dh_flags & MCP_QUEUE_FLAG_DUMP_START)
    {
        ListCell   *cell;
        bool    changed = false;

        LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);
		/* get the newest table list */
        if (state->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
            SlaveRestoreTableList(state);

        /* Set dump requested flag for all replicated tables */
        foreach(cell, state->ss_tablelist)
        {
            MCPTable   cur = (MCPTable) lfirst(cell);

            Assert(cur->id == TABLEID_MCP);
            if (cur->on_slave[state->ss_hostno])
            {
                cur->slave_req[state->ss_hostno] = true;
                changed = true;
            }
        }

        if (changed)
            SlaveStoreTableList(state);

        LWLockRelease(MCPTableListLock);

        replicate_tx = true;
    }

	/* Always replicate CATALOG_DUMP transactions */
	if (hdr->dh_flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
        replicate_tx = true;
	

	/* Check if some tables were not received during full dump. Never complain
	 * about pg_catalog. tables, cause they are not necesssary included in 
	 * every dump.
	 */
    if (hdr->dh_flags & MCP_QUEUE_FLAG_DUMP_END)
    {
        ListCell   *cell;

        LWLockAcquire(MCPTableListLock, LW_SHARED);

        if (state->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
            SlaveRestoreTableList(state);

        foreach(cell, state->ss_tablelist)
        {
            MCPTable   cur = (MCPTable) lfirst(cell);

            Assert(cur->id == TABLEID_MCP);

            if (cur->on_slave[state->ss_hostno] && 
                cur->slave_req[state->ss_hostno])
            {
				if (strncmp(cur->relpath, "pg_catalog", 10) == 0)
					cur->slave_req[state->ss_hostno] = false;
				else
                	elog(WARNING, 
                     	 "data for relation %s has not been received during FULL DUMP",
                     	 cur->relpath);
            }
        }

        LWLockRelease(MCPTableListLock);

        replicate_tx = true;
    }

	/* 
	 * We forward a dump transaction to a slave only if the slave
	 * has actually requested the dump.
	 */
	if (hdr->dh_flags & MCP_QUEUE_FLAG_TABLE_DUMP)
	{
		LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);

		/* Load the latest version of tables */
		if (state->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
			SlaveRestoreTableList(state);

		/* Check if we have a received table name in the table list */
		foreach(cell, TableList)
		{
			BackendTable	txtable = lfirst(cell);
			MCPTable		slavetable;

			slavetable = TableListEntry(state->ss_tablelist, txtable);
			
			/* 
			 * The table should be present in the table list, since the master
			 * forwarder process adds it upon receiving from the master.
			 */
			Assert(slavetable != NULL);
			
			if (slavetable->on_slave[state->ss_hostno] && 
				slavetable->slave_req[state->ss_hostno])
			{

				replicate_tx = true;
				/* clear the table request state for the slave */
				slavetable->slave_req[state->ss_hostno] = false;
			}
			/* 
			 * Send the table dump even if it's not *yet* replicated by the
			 * slave if the transaction's bitmapset indicates that the slave
			 * will replicate it after processing the catalog dump.
			 */
			else if ((slavetable->on_slave[state->ss_hostno] == false) &&
					 state->ss_wait_list)
			{
				Bitmapset   *bms = palloc(sizeof(Bitmapset) + 
							 		(hdr->nwords - 1) * sizeof(bitmapword));
				bms->nwords = hdr->nwords;
				memcpy(bms->words, hdr->words, 
					   bms->nwords * sizeof(bitmapword));
				/* check whether the slave's bit in the bitmapset is set */
				if (bms_is_member(state->ss_hostno, bms))
				{
					elog(DEBUG2, 
						"slave %d is present in transaction bitmapset",
						state->ss_hostno);
					
					slavetable->on_slave[state->ss_hostno] = true;
					slavetable->slave_req[state->ss_hostno] = false;
					replicate_tx = true;
				}
				pfree(bms);
			}
			
			if (!replicate_tx &&
				strncmp(slavetable->relpath, "pg_catalog.repl_slave_roles", NAMEDATALEN) == 0)
			{
				/* XXX: always accept repl_slave_relations dump. This relation
				 * is a special case, since it's not a part of the catalog
				 * dump, but a replication catalog (FIXME).
				 */
				slavetable->on_slave[state->ss_hostno] = true;
				slavetable->slave_req[state->ss_hostno] = false;
				replicate_tx = true;				
			}

			elog(DEBUG2, "dump transaction for table \"%s\": %s",
				 txtable->relpath, replicate_tx ? "replicated" : "not replicated");
		}
		/* Write changes to disk if any */
		if (replicate_tx)
			SlaveStoreTableList(state);

		LWLockRelease(MCPTableListLock);
	}
	else if (hdr->dh_flags & MCP_QUEUE_FLAG_DATA)
	{
		char	*rp = NULL;

		/*
		 * Note: we may use outdated table list here, since we don't reread it
		 * from disk like in the table dump case, which saves us a lock. We can
		 * guarantee, however, that the information we use is up-to-date since
		 * this code is executed as a part of the process that exclusively
		 * changes that kind of information for this slave.
		 */
		replicate_tx = false;

		foreach(cell, TableList)
		{
			BackendTable	txtable = lfirst(cell);
			MCPTable		slavetable;
			
			/* 
			 * Check for special cases. One is the repl_slave_relations
			 * (see comments in TABLE_DUMP). Another is a single pg_largeobject
			 * relation, related to lo changes without changes to the tables
			 * with lo reference (i.e. by a direct call to lo_write).
			 * Note: these relations are not part of the master's list, thus
			 * the check is performed before the table is checked against it.
			 */
			if (txtable != NULL &&
			    (strncmp(txtable->relpath, "pg_catalog.repl_slave_roles", 
						 NAMEDATALEN) == 0 ||
				 (strncmp(txtable->relpath, "pg_catalog.pg_largeobject", 
						  NAMEDATALEN) == 0 && list_length(TableList) == 1)))
			{
				elog(DEBUG5, "special case table %s", txtable->relpath);
				rp = txtable->relpath;
				replicate_tx = true;
				break;
			}

			slavetable = TableListEntry(state->ss_tablelist, txtable);
			if (slavetable != NULL &&
				slavetable->on_slave[state->ss_hostno] && 
				!slavetable->slave_req[state->ss_hostno])
			{
				rp = slavetable->relpath;
				replicate_tx = true;
				break;
			}
		}

		if (replicate_tx)
		{
			Assert(rp != NULL);
			elog(DEBUG2, "data replicated (includes table %s)", rp);
		}
		else
			elog(DEBUG2, "data transaction not replicated");
	}
final:
	return replicate_tx;
}

/*
 * SlaveMessageHook
 * 		Hook for SendQueueTransaction (2nd hook argument)
 */
static off_t
SlaveMessageHook(TxDataHeader *hdr, void *status_arg, ullong recno)
{
	SlaveStatus *status = (SlaveStatus *) status_arg;

	/*
	 * If this is a catalog dump, stop sending anything until the slave
	 * returns the table list
	 */
	if (hdr->dh_flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
	{
		status->ss_wait_list = true;
		status->ss_wait_list_recno = recno + 1;
		elog(DEBUG2,
			 "list wait flag set for slave %d recno "UNI_LLU, 
			 status->ss_hostno, status->ss_wait_list_recno);		
	}

	/* 
	 * Avoid sending table lists on the slave. We have to send all the data
	 * before dh_listoffset to accomplish this. But first we need to change
	 * dh_len field.
	 */
	if (hdr->dh_listoffset != (off_t) 0)
		hdr->dh_len = hdr->dh_listoffset;
	hdr->dh_listoffset = (off_t) 0;
	return hdr->dh_len;
}


/* Send messages from the queue to the slave */
static void
SlaveSendQueuedMessages(SlaveStatus *status)
{
	ullong		recno;
	ullong		last_recno;
	uint32		hostno = status->ss_hostno;
	MCPHosts   *h = status->ss_hosts;

	set_ps_display("sending messages to slave", false);

	/* Get recno of the transaction to send */
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	recno = MCPHostsGetHostRecno(h, McphHostRecnoKindSendNext, hostno);
	LWLockRelease(MCPHostsLock);

	/* Get recno of the last transaction to send */
	LockReplicationQueue(status->ss_queue, LW_SHARED);
	last_recno = MCPQueueGetLastRecno(status->ss_queue);
	UnlockReplicationQueue(status->ss_queue);

	while (recno <= last_recno && TXLOGIsCommitted(recno))
	{
		ullong	next_recno;

		/* Send the current transaction */
		elog(LOG, "sending transaction "UNI_LLU" to slave", recno);

		SendQueueTransaction(status->ss_queue, recno,
							 SlaveTableListHook, (void *) status,
							 SlaveMessageHook, (void *) status);

		/* refresh our knowledge about queue end */
		LockReplicationQueue(status->ss_queue, LW_SHARED);
		last_recno = MCPQueueGetLastRecno(status->ss_queue);
		UnlockReplicationQueue(status->ss_queue);

		/*
		 * Advance our next-to-send record number, but do so carefully:
		 * our ReadNext pointer could have been moved by the master process.
		 */
		LWLockAcquire(MCPHostsLock, LW_SHARED);
		next_recno = MCPHostsGetHostRecno(h, McphHostRecnoKindSendNext, hostno);
		if (next_recno == recno)
			/* master hasn't moved it -- do so ourselves */
			recno = MCPHostsNextTx(h, hostno, last_recno);
		else
			/* master has moved it - start from the new recno */
			recno = next_recno;
		LWLockRelease(MCPHostsLock);

		/* If the slave sent a message, go fetch it */
		if (mcpWaitTimed(MyProcPort, true, false, 0))
			break;
	}
}

/* Receive a single message from the slave */
static void
ReceiveSlaveMessage(SlaveStatus *status)
{
	MCPMsg         *rm;

	set_ps_display("receiving a message from slave", false);
	rm = MCPRecvMsg();
	
	MCPMsgPrint(DEBUG3, "Recv", rm);

	/* Process promote notification message from slave */
	if (rm->flags & MCP_MSG_FLAG_PROMOTE_NOTIFY)
	{
		bool	force = *(bool *)rm->data;

		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

		SlaveStartPromotion(status, force);

		LWLockRelease(MCPServerLock);
		LOG_PROMOTION_STATES(DEBUG2, status);
	}

	/* Deal with back promotion request from a slave */
	else if (rm->flags & MCP_MSG_FLAG_PROMOTE_BACK)
	{
		/* 
		 * get a previous promotion slave number from a promotion stack
		 * and check if we have one (i.e. promotion stack is not empty)
		 */
		int		prev_promotion_slaveno = PromotionStackPeek();
		bool	cancel = true;

		if (prev_promotion_slaveno == -1)
			elog(WARNING, "back promotion request but no promotions performed yet");
		else if (prev_promotion_slaveno != status->ss_hostno)
			elog(WARNING, "back promotion but this slave is not the former master");
		else
		{
			/* back promotion is not compatible with a force promotion */
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

			SlaveStartPromotion(status, false);

			LWLockRelease(MCPServerLock);

			/* 
			 * if promotion was not cancelled - record the fact that this is back
			 * promotion.
			 */
			if (status->ss_promotion != slave_promotion_cancelled)
			{
				LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

				PromotionCtl->promotion_type = back_promotion;

				LWLockRelease(MCPServerLock);

				cancel = false;
			}
		}

		if (cancel)
			status->ss_promotion = slave_promotion_cancelled;
		LOG_PROMOTION_STATES(DEBUG2, status);
	}
	else if (rm->flags & MCP_MSG_FLAG_PROMOTE_CANCEL)
	{
		elog(WARNING, "Received promotion cancel request from slave %d",
			 status->ss_hostno);
		if (status->ss_promotion != slave_promotion_none ||
			status->ss_force_promotion != slave_force_promotion_none)
		{
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

			/* notify the master process if this is not a force promotion */
			if (status->ss_promotion != slave_promotion_none)
			{
				PromotionCtl->promotion_cancelled = true;
				MasterNextPromotionState(status);
			}
			MCPSlaveCancelPromotion(status);

			LWLockRelease(MCPServerLock);
		}
		LOG_PROMOTION_STATES(DEBUG2, status);
	}

	/* Receive single table name from slave */
	if (rm->flags & MCP_MSG_FLAG_TABLE_LIST)
		RecvSlaveTable(status, rm);

	if (rm->flags & MCP_MSG_FLAG_TABLE_LIST_END)
	{
		ListCell *cell;

		/*
		 * The reason we acquire MCPServerLock early is that we can potentially
		 * change DumpCtl fields and we have to follow the locking order to
		 * avoid deadlocks
		 */
		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);

		SlaveMergeTableLists(status, rm->recno);

		LWLockRelease(MCPTableListLock);
		LWLockRelease(MCPServerLock);

		elog(DEBUG2, "New table list");
		foreach(cell, status->ss_tablelist)
		{
			TableData *tab = lfirst(cell);
			ShowTable(DEBUG5, tab);
		}
		/* Clear the 'wait for the table list' flag */
		if (status->ss_wait_list && rm->recno == status->ss_wait_list_recno)
		{
			status->ss_wait_list = false;
			elog(DEBUG2, "list wait flag cleared for slave %d recno "UNI_LLU,
				 status->ss_hostno, status->ss_wait_list_recno);
		}
	}

	if (rm->flags & MCP_MSG_FLAG_REQFULL)
	{
		/* Acquires HostLock internally */		
		ProcessSlaveDumpRequest(status);
	}

	if (rm->flags & MCP_MSG_FLAG_ACK)
	{
		ullong		prevack;

		elog(DEBUG2, "received ACK from slave => vrecno = "UNI_LLU,
			 rm->recno);
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		prevack = MCPHostsGetHostRecno(status->ss_hosts, McphHostRecnoKindAcked,
									   status->ss_hostno);
		if (prevack <= rm->recno)
		{
			MCPHostsSetHostRecno(status->ss_hosts, McphHostRecnoKindAcked,
								 status->ss_hostno, rm->recno);
			elog(LOG, "received ACK for message "UNI_LLU, rm->recno);
		}
		else
			elog(WARNING, "received ACK for already confirmed transaction");
		LWLockRelease(MCPHostsLock);
	}
	
	/* Should be placed after ACK and TABLELIST processing code */
	if (rm->flags & MCP_MSG_FLAG_PROMOTE_SLAVE_READY)
	{
		if (status->ss_promotion == slave_promotion_wait_slave_ready)
		{
			status->ss_promotion = slave_promotion_send_make;

			/* Notify master process that it should send promote make to an agent */
			LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
			MasterNextPromotionState(status);
			LWLockRelease(MCPServerLock);
		}
		else
		{
			elog(WARNING, "received PROMOTE_SLAVE_READY with unexpected "
				 "slave promotion state: %d, promotion is cancelled",
				status->ss_promotion);

			status->ss_promotion = slave_promotion_cancelled;
		}
		LOG_PROMOTION_STATES(DEBUG2, status);
	}

	MCPReleaseMsg(rm);
}

/*
 * Actions to process the dump request either received by slave DESYNC message
 * or triggered by the queue not in sync state. Returns true if the state of the
 * slave was changed (i.e. host sync was changed or mcp_dump_request was set),
 * otherwise returns false
 */
static bool
ProcessSlaveDumpRequest(SlaveStatus *status)
{
	bool	result = true,
			request = false;
	ullong	stored_dump_recno,
			host_vrecno;
	MCPHosts *h = status->ss_hosts;
	int		hostno = status->ss_hostno;

	LWLockAcquire(MCPServerLock, LW_SHARED);
	LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);

	stored_dump_recno = FullDumpGetStartRecno();
	host_vrecno = MCPHostsGetHostRecno(h, McphHostRecnoKindAcked, hostno);

	/* dump at recno 0 (invalid) means no dump */
	ereport(DEBUG2,
			(errmsg("slave dump request"),
			 errcontext("dump in queue: recno "UNI_LLU,
						stored_dump_recno)));

	if (stored_dump_recno != InvalidRecno && host_vrecno < stored_dump_recno)
	{
		/*
		 * Skip all messages up to the dump start point. The queue will be
		 * sent to a sync state when the dump will be sent to the slave.
		 */
		elog(DEBUG2, "using dump stored on MCP server");

		MCPHostsSetHostRecno(h, McphHostRecnoKindSendNext, hostno, stored_dump_recno);
		result = false;
	}
	else
	{
		/* Decide whether to request a dump from master */
		FullDumpProgress	progress = FullDumpGetProgress();

		if (progress == FDnone)
		{
			/* Requesting a dump from master */
			elog(DEBUG2, "requesting dump from master");
			request = true;
			MCPHostsSetSync(h, hostno, MCPQUnsynced);
		}
		else
		{
			elog(DEBUG2, "dump was already requested: dump progress = %s",
				 FDProgressAsString(progress));
			result = false;
			/* Set the slave's sync state to unsynced */
			if (MCPHostsGetSync(h, hostno) == MCPQSynced)
				MCPHostsSetSync(h, hostno, MCPQUnsynced);
		}
	}
	LWLockRelease(MCPHostsLock);
	LWLockRelease(MCPServerLock);

	if (request)
	{
		ListCell   *cell;
		bool    changed = false;

		/* Set dump request state for each table replicated by this slave */
		LWLockAcquire(MCPTableListLock, LW_EXCLUSIVE);

		/* Check if we have the latest list of tables */
		if (status->current_tablelist_rev != ServerCtl->latest_tablelist_rev)
			SlaveRestoreTableList(status);

		foreach(cell, status->ss_tablelist)
		{
			MCPTable   cur = (MCPTable) lfirst(cell);
			Assert(cur->id == TABLEID_MCP);

			/* 
			 * If the table is replicated by this slave, set a dump request for
			 * it. Note that we request a dump even if it was already requested
			 * by this or some other slave since we want to receive all the
			 * tables for the full dump request.
			 */
			if (cur->on_slave[status->ss_hostno])
			{
				/* set request status unconditionally */
				cur->req_satisfied = TDrequested;
				cur->slave_req[status->ss_hostno] = true;
				changed = true;
			}
		}

		if (changed)
		{
			/* Write table list changes back to disk */
			SlaveStoreTableList(status);
		}

		LWLockRelease(MCPTableListLock);

		LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
		FullDumpSetProgress(FDrequested);
		/* Wake master to react on mcp_dump_request */
		WakeupMaster();
		LWLockRelease(MCPServerLock);
	}

	return result;
}

static void
ProcessForcePromotion(SlaveStatus *status)
{
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

	elog(DEBUG2, "ProcessForcePromotion");

	if (PromotionCtl->master_check_state == master_check_none && 
		PEER_CONNECTED(0))
	{
		elog(DEBUG2, "Checking if the master is still connected to MCP");
		PromotionCtl->master_check_state = master_check_started;
		WakeupMaster();
	}
	else if (PromotionCtl->master_check_state == master_check_finished)
		PromotionCtl->master_check_state = master_check_none;

	if (PromotionCtl->master_check_state == master_check_none && 
		PEER_CONNECTED(0))
	{
		ereport(WARNING,
				(errmsg("force promotion request discarded"),
				 errdetail("Master is connected."),
				 errhint("Use plain PROMOTE instead.")));

		/* Send promotion cancel message to slave */
		status->ss_force_promotion = slave_force_promotion_cancelled;	
	}
	else if (!PEER_CONNECTED(0))
	{
		status->ss_force_promotion = slave_force_promotion_accepted;

		elog(DEBUG2, "Master process is not connected to MCP, "
			 "proceed with force promotion for the slave(%d)", 
			 status->ss_hostno);
	}
	LWLockRelease(MCPServerLock);
	LOG_PROMOTION_STATES(DEBUG2, status);
}

/*
 * Wake master to react on various conditions.
 */
void
WakeupMaster(void)
{
	Assert(LWLockHeldByMe(MCPServerLock));

	if (ServerCtl->node_pid[0] != 0)
		kill(ServerCtl->node_pid[0], SIGUSR1);
}

/*
 * Tell master process to advance its promotion state.
 */
static void
MasterNextPromotionState(SlaveStatus *status)
{
	Assert(LWLockHeldByMe(MCPServerLock));

	if (PromotionCtl->master_sysid == ServerCtl->node_sysid[0])
	{
		if (ServerCtl->node_pid[0] != 0)
			SendForwarderChildSignal(0, FORWARDER_SIGNAL_PROMOTION);
		else
		{
			elog(WARNING,
				 "master process is not alive but marked as participating in promotion");
			PromotionCtl->master_sysid = 0;
			status->ss_promotion = slave_promotion_cancelled;
		}
	}
	else
		ereport(WARNING,
				(errmsg("attempted to send promotion signal to a master that is either disconnected or not in promotion")));

	LOG_PROMOTION_STATES(DEBUG2, status);
}

/*
 * A handler for changing slave's normal promotion state in response to a signal
 * received from master. Currenly only slave_promotion_wait_ready state requires
 * interaction from a master process. Note that force promotion is performed
 * solely on the slave and doesn't involve this function. Also note that the
 * promotion cancel request is transmitted using the SIGUSR2 with promotion_cancel
 * flag set.
 */
static void
MCPSlaveActOnPromotionSignal(SlaveStatus *status)
{
	bool	cancel;

	/* Ignore master promotion signals if we are already cancelled promotion */	
	if (status->ss_promotion == slave_promotion_cancelled ||
		status->ss_force_promotion == slave_force_promotion_cancelled)
		return;

	/* 
	 * We probably should check if we are the slave that is in promotion. However,
	 * even if we are not a slave_promotion would be equal to no promotion state
	 * and promotion cancel would fire.
	 */

	/* Clear promotion flag after storing its state locally */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	cancel = PromotionCtl->promotion_cancelled;
	PromotionCtl->promotion_cancelled = false;
	LWLockRelease(MCPServerLock);
	
	/* 
	 * Cancel promotion if promotion was cancelled explicitely or if this is a force
	 * promotion (receiving signal from master already means that the master is alive
	 * and there's no sense in continuing with force promotion).
	 */
	if (status->ss_force_promotion != slave_force_promotion_none)
	{
		ereport(WARNING,
				(errmsg("force promotion cancelled"),
				 errdetail("Master is alive.")));
		status->ss_force_promotion = slave_force_promotion_cancelled;
	}
	else if (cancel)
	{
		if (status->ss_promotion != slave_promotion_none)
		{
			elog(WARNING, "slave promotion was cancelled");
			status->ss_promotion = slave_promotion_cancelled;
		}
	}
	else if (status->ss_promotion != slave_promotion_cancelled)
	{
		/* 
		 * Advance to the next slave state. Only slave_promotion_wait_ready
		 * and slave_promotion_none requires waiting for master.
		 */
		if (status->ss_promotion == slave_promotion_wait_ready ||
			(status->ss_promotion == slave_promotion_none &&
		     PromotionCtl->promotion_type == back_promotion))
			status->ss_promotion++;
		else
		{
			elog(WARNING,
				 "got unexpected signal from master when in promotion state %s",
				 PromotionAsString(status->ss_promotion));

			/* cancel promotion globally */
			status->ss_promotion = slave_promotion_cancelled;
		}
	}
	else
		elog(WARNING,
			 "received a request to change promotion state after promotion was cancelled");
}

/*
 * Start MCP promotion in response to receiving a PROMOTE_NOTIFY message from
 * slave
 */
static void
SlaveStartPromotion(SlaveStatus *status, bool force)
{
	/* Check if promotion is allowed for the slave */
	int		hostno = status->ss_hostno;
	int		promotion_slave_no = PromotionCtl->promotion_slave_no;
	bool	allow = IsPromotionAllowed(hostno);

	Assert(LWLockHeldByMe(MCPServerLock));

	/* 
	 * Check if MCP is already performing a promotion. This is a bit tricky
	 * since we don't have a global promotion status.  Instead we check if
	 * promotion slave number is set; if it is, check that the slave's sysid
	 * matches the stored promotion slave sysid. Thus even if a previous slave
	 * failed to reset promotion_slave_no, a promotion slave sysid would either
	 * not match the sysid of a slave with promotion slave number (if another
	 * slave connected) or will be 0 if the slave reconnected.
	 *
	 * Note that we cancel promotion of this slave even if it was in promotion
	 * before (i.e. a corresponding slave has sent 2 consecutive PROMOTE_NOTIFY
	 * messages); it's the agent task to disallow promotion if one is already
	 * in progress.
	 */
	if (promotion_slave_no >= 0 && 
		ServerCtl->node_pid[promotion_slave_no + 1] != 0 &&
		ServerCtl->node_sysid[promotion_slave_no + 1] == PromotionCtl->slave_sysid)
	{
		allow = false;
	}

	if (allow && status->ss_promotion != slave_promotion_cancelled)
	{
		elog(LOG, "received promotion notification from slave %d", hostno);

		/* if a normal promotion was requested and master is dead - cancel it */
		if (force == false && ServerCtl->node_pid[0] == 0)
		{
			allow = false;
			ereport(WARNING, 
				(errmsg("promotion of slave %d cancelled because master is not present", 
						hostno),
				 errhint("To promote a single slave use PROMOTE FORCE.")));

		}
		else
		{
			/* write sysids to a promotion shared state */
			PromotionCtl->slave_sysid = ServerCtl->node_sysid[hostno + 1];
			/* note: will be always 0 here in case of force promotion */
			PromotionCtl->master_sysid = ServerCtl->node_sysid[0];
			/* 
			 * reset a cancelled status if it remained set from the previous 
			 * promotion and set promotion slave number.
			 */
			PromotionCtl->promotion_cancelled = false;
			PromotionCtl->promotion_slave_no = hostno;
			PromotionCtl->promotion_type = force ? force_promotion : normal_promotion;

			if (!force)
			{
				/* start a normal promotion and inform master about this */
				status->ss_promotion = slave_promotion_wait_ready;
				MasterNextPromotionState(status);
			}
			else
			{
				/* 
				 * start a force promotion. Wake up master process just in case
				 * it is present.
				 */ 
				status->ss_force_promotion = slave_force_promotion_check_master;
				WakeupMaster();
			}
		}
	}
	else
		allow = false;

	/* Check if we would have to complain */
	if (!allow)
	{
		elog(WARNING,
			 "promotion of slave %d is not allowed or already cancelled",
			 hostno);
		/* 
		 * Don't set a global promotion_cancelled flag since a master doesn't know
		 * anything about a promotion here.
		 */
		if (!force)
			status->ss_promotion = slave_promotion_cancelled;
		else
			status->ss_force_promotion = slave_force_promotion_cancelled;
	}
}

/* 
 * Cancel promotion for the slave process. For details about promotion
 * cancellation see MCPMasterCancelPromotion.
 */
static void
MCPSlaveCancelPromotion(SlaveStatus *status)
{
	Assert(LWLockHeldByMe(MCPServerLock));

	status->ss_promotion = slave_promotion_none;
	status->ss_force_promotion = slave_force_promotion_none;

	/* 
	 * Reset slave's promotion sysid. If master has already reset its sysid
	 * than clear promotion shared flags.
	 */
	PromotionCtl->slave_sysid = 0;
	if (PromotionCtl->master_sysid == 0)
	{
		PromotionCtl->promotion_slave_no = -1;
		PromotionCtl->promotion_cancelled = false;
	}
}

/* 
 * Merge table list received from the slave with a current
 * list stored on disk. Recno is the record of table list 
 * message received from the slave and is the slave's next
 * record to restore at the time of sending the table list.
 */
static void 
SlaveMergeTableLists(SlaveStatus *state, ullong recno)
{
	ListCell   *cell;
	bool	changed;
    bool    wake_master = false;

	/* read the latest tablelist from disk */
	SlaveRestoreTableList(state);

	/* 
	 * Walk through the current list and check if we have received a new
	 * table or if an existing table was not replicated by this slave.
	 */
	changed = false;

	foreach(cell, state->ss_recv_list)
	{
		BackendTable t_received = lfirst(cell);
		MCPTable	t_found;

		/* 
		 * Check if we already have the table in the table list on MCP.
		 * Make the one if we haven't and add it to the MCP list.
		 */
		t_found = TableListEntry(state->ss_tablelist, t_received);
		if (t_found == NULL)
		{
			t_found = MakeMCPTable(t_received->relpath);
			state->ss_tablelist = lappend(state->ss_tablelist, t_found);

			elog(DEBUG2, "table %s added to the table list",
				 t_found->relpath);
		}

		/* Check if the table is replicated by the current slave */
		if (t_found && !t_found->on_slave[state->ss_hostno])
		{
			t_found->on_slave[state->ss_hostno] = true;
			changed = true;
			elog(DEBUG2, "enabled replication for table \"%s\"",
				 t_found->relpath);
		}
		/* Check if the slave requested a dump for this table */
		if (t_received->raise_dump == TableDump)
		{
        	changed = true;
			wake_master |= MCPSlaveActOnTableRequest(state, t_found, recno);
		}
	}

	list_free_deep(state->ss_recv_list);
	state->ss_recv_list = NIL;

	/* Write changes back on disk if any */
	if (changed)
		SlaveStoreTableList(state);

	if (wake_master)
	{
		/*
		 * Inform master that it has to check whether there are tables for
		 * which it should request a dump.
		 *
		 * Note: it's OK to set it multiple times even if there are no tables
		 * to request a dump for -- just an extra check on master.  We don't
		 * do anything on this flag if req_satisfied isn't TDrequested for
		 * at least some table.
		 */
		TableDumpSetRequest(true);
		WakeupMaster();
    	elog(DEBUG2, "table dump requests forwarded to master");
    }
}

/*
 * Check if a table dump should be requested for the given table.
 * Recno is the next to restore record number received from the slave.
 */
static bool
MCPSlaveActOnTableRequest(SlaveStatus *state, MCPTable tab, ullong recno)
{
	bool	reqdump = false;

	/* Check if the dump hasn't been requested yet */
	if (tab->req_satisfied == TDnone)
	{
	 	/*
		 * Check if the dump is in queue but the slave has already advanced
		 * after that point (in which case we need to request it again). This
		 * also covers the case of the table dump not requested before, since
		 * InvalidRecno as the dump_recno is less than any valid recno.
	 	 */
		reqdump = tab->dump_recno < recno;
	}

	if (reqdump)
	{
		tab->req_satisfied = TDrequested;
	
		elog(DEBUG2,
			 "dump request from slave %d for table \"%s\"",
			 state->ss_hostno, tab->relpath);
	}
	else
	{
		if (tab->req_satisfied != TDnone)
			elog(DEBUG2,
			 	 "dump for table \"%s\" was already requested by the slaves",
			 	 tab->relpath);
		else
			elog(DEBUG2, "dump for table \"%s\" is already in the queue",
				 tab->relpath);
	}
	if (reqdump || tab->req_satisfied != TDnone)
	{
		/* Set 'requested by the slave' state for the table */
    	tab->slave_req[state->ss_hostno] = true;
	}
    return reqdump;
}

static void 
SlaveStoreTableList(SlaveStatus *state)
{
	char	*path;

	Assert(LWLockHeldByMe(MCPTableListLock));

	path = MCPGetTableListFilename(MCP_LIST_SUFFIX);

	/* Store table list on disk */
	StoreTableList(path, state->ss_tablelist);

	/* Increase the latest revision number */
	state->current_tablelist_rev = ++ServerCtl->latest_tablelist_rev;

	pfree(path);
}

static void 
SlaveRestoreTableList(SlaveStatus *state)
{
	char    *path;

	Assert(LWLockHeldByMe(MCPTableListLock));

	path = MCPGetTableListFilename(MCP_LIST_SUFFIX);

    if (state->ss_tablelist != NIL)
        list_free_deep(state->ss_tablelist);

	state->ss_tablelist = RestoreTableList(path);

	/* We are up-to-date with a table list */
	state->current_tablelist_rev = ServerCtl->latest_tablelist_rev;
}
