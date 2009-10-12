/* -----------------------
 * mcp_server.c
 * 		The Mammoth Control Processor (MCP) server implementation
 *
 * The MCP server acts as the link between master and all slaves and controls
 * the replication.  Slaves never connect directly to the master.  Instead,
 * they connect to the MCP server, and the MCP sends them the data received
 * from the master.
 *
 * $Id: mcp_server.c 2126 2009-05-07 21:25:22Z alexk $
 * -----------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>

#include "libpq/pqsignal.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/fwsignals.h"
#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_promotion.h"
#include "mammoth_r/paths.h"
#include "mammoth_r/txlog.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"


MCPQueue   *ForwarderQueue = NULL;
MCPHosts   *ForwarderHosts = NULL;

int 		ForwarderProcessId; /* id of the current forwarder process */

/* MCP signal processing */
static void sigquit_handler_child(SIGNAL_ARGS);
static void sigterm_handler_child(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void sigusr2_handler(SIGNAL_ARGS);

/*
 * Reset my PID and sysid in shared memory; used as a proc_exit callback.
 */
static void
reset_node_ids(int code, Datum arg)
{
	int		node = DatumGetInt32(arg);

	ServerCtl->node_pid[node] = 0;
	ServerCtl->node_sysid[node] = 0;
}

/*
 * Register the new process in the ServerCtl struct.  This could cause an
 * ERROR if this peer is already connected.
 */
static void
RegisterServerProcess(int pid, char mode, int slave_no, uint64 node_id)
{
	int		node = -1;

	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);

	if (mode == MCP_TYPE_MASTER)
	{
		if (ServerCtl->node_pid[0] != 0)
			elog(ERROR, "master is already connected");
		node = 0;
	}
	else if (mode == MCP_TYPE_SLAVE)
	{
		if (ServerCtl->node_pid[slave_no + 1] != 0)
			elog(ERROR, "slave %d is already connected", slave_no);
		node = slave_no + 1;
	}
	else
		Assert(false);

	ServerCtl->node_pid[node] = pid;
	ServerCtl->node_sysid[node] = node_id;
	
	/* initialize global forwarder-specific process id */
	ForwarderProcessId = node;
	
	on_proc_exit(reset_node_ids, (Datum) node);

	LWLockRelease(MCPServerLock);
}

/*
 * Initialize the forwarder environment
 */
void
ForwarderInitialize(void)
{
	/* nothing to do here at present ... */
}

/*
 * initialize the child process, do the authentication step, and continue as a
 * master or slave process as appropriate.
 */
int
MCPServerHandleConnection(Port *port)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext ctx;
	int32		peerid = -1;
	int			status;
	char		mode;
	int			slave_no;
	uint64		node_id;
	pg_enc 		peer_encoding,
				master_encoding;

	/* Release postmaster's working memory context */
	MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(PostmasterContext);
	PostmasterContext = NULL;

	ClientAuthInProgress = true;	/* limit visibility of log mesages */

	/*
	 * Allow authorization phase to be interrupted; install a signal handler
	 * that will terminate the process without further ado, if killed in this
	 * phase.  A more elaborate handler must be installed later.
	 */
	pqsignal(SIGTERM, authdie);
	pqsignal(SIGQUIT, authdie);
	pqsignal(SIGINT, authdie);
	PG_SETMASK(&AuthBlockSig);

	/*
	 * If possible, make this process a group leader, so that the postmaster
	 * can signal any child processes too.	(We do this now on the off chance
	 * that something might spawn a child process during authentication.)
	 */
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(FATAL, "setsid() failed: %m");
#endif

	/* initialize our connection */
	MCPConnectionInit(port);

	/*
	 * Ready to begin client interaction.  We will give up and exit(0) after a
	 * time delay, so that a broken client can't hog a connection
	 * indefinitely.  PreAuthDelay doesn't count against the time limit.
	 */
	if (!enable_sig_alarm(AuthenticationTimeout * 1000, false))
		elog(FATAL, "could not set timer for authorization timeout");

	/*
	 * receive the startup packet.  Note that we possibly set the "allow_ssl"
	 * parameter to true here; if this was compiled without SSL support, this
	 * routine is in charge of raising an error.
	 */
	status = ProcessMcpStartupPacket(port, ForwarderRequireSSL, &mode, &slave_no);
	if (status != STATUS_OK)
		proc_exit(0);

	/* if SSL is to be established, it should already be by this point */

	/*
	 * Make sure the peer is coming from an accepted IP.  FIXME -- the master
	 * should have already said in the startup packet whether it is a promoted
	 * slave, and what slave ID it had.
	 */
	status = STATUS_ERROR;
	if (mode == MCP_TYPE_MASTER)
		status = MCPMasterConnectionAllowed(&port->raddr, false);
	else if (mode == MCP_TYPE_SLAVE)
		status = MCPSlaveConnectionAllowed(&port->raddr, slave_no);

	if (status != STATUS_OK)
		elog(ERROR, "connection disallowed by ACL");

	/* Now perform authentication exchange */
	status = McpReceiveClientAuth(&node_id, &peer_encoding);
	if (status != STATUS_OK)
		proc_exit(0);

	/*
	 * Done with authentication.  Disable timeout, and prevent SIGTERM/SIGQUIT
	 * again until backend startup is complete.
	 */
	if (!disable_sig_alarm(false))
		elog(FATAL, "could not disable timer for authorization timeout");
	PG_SETMASK(&BlockSig);

	if (Log_connections)
		ereport(LOG,
				(errmsg("forwarder connection authorized")));
	
	elog(DEBUG2, "peer connected with nodeid " UNI_LLU" data encoding %s",
		 		 node_id, pg_encoding_to_char(peer_encoding));

	/*
	 * Initialize the elog subsystem.  All other initialization must come
	 * after the exception stack is fully set up!
	 */
	/* Prepare to handle elog(ERROR) */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		EmitErrorReport();
		MemoryContextSwitchTo(TopMemoryContext);
		FlushErrorState();

		/*
		 * Release lwlocks in case a process dies uncleanly.  This is not
		 * very kosher; what other cleanup do we need here?
		 */
		LWLockReleaseAll();

		/* Clean up MCPFile module */
		MCPFileCleanup();

		proc_exit(0);
	}

	PG_exception_stack = &local_sigjmp_buf;

	/* Now block signals until we're ready to get them */
	PG_SETMASK(&BlockSig);

	/* grab our PGPROC so we can get lwlocks */
	InitProcess();

	/* Reinitialize global variables. */
	MCPInitVars(false);

	/* Create the child process memory context */
	ctx = AllocSetContextCreate(TopMemoryContext,
								"Child Context",
								ALLOCSET_DEFAULT_MINSIZE,
								ALLOCSET_DEFAULT_INITSIZE,
								ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContextSwitchTo(ctx);

	/* initialize the queue */
	LWLockAcquire(MCPServerLock, LW_EXCLUSIVE);
	ForwarderQueue = MCPQueueInit(true);
	ForwarderHosts = MCPHostsInit();
	LWLockRelease(MCPServerLock);

	/* switch to the forwarder txlog and start it up */
	SelectActiveTxlog(true);
	LockReplicationQueue(ForwarderQueue, LW_SHARED);
	TXLOGStartup(MCPQueueGetFirstRecno(ForwarderQueue));
	UnlockReplicationQueue(ForwarderQueue);

	switch (mode)
	{
		case MCP_TYPE_MASTER:
			peerid = 0;
			init_ps_display("forwarder process", "master", "", "");
			set_ps_display("", true);
			break;
		case MCP_TYPE_SLAVE:
			{
				char	buf[32];

				peerid = slave_no + 1;
				sprintf(buf, "slave %d", slave_no);
				init_ps_display("forwarder process", buf, "", "");
				set_ps_display("", true);
				break;
			}
		default:
			peerid = -1;	/* keep compiler quiet */
			elog(PANIC, "unrecognized peer type %c", mode);
			break;
	}

	/*
	 * Note: we register the promotion cleanup handler first, because we need
	 * it to run after the connection cleanup handler.  Doing it the other way
	 * around opens a race condition for the PROMOTE FORCE logic for checking
	 * whether a master is connected.
	 */
	on_proc_exit(promotion_cleanup, Int32GetDatum(peerid)); 

	/* Register the process in the ServerCtl struct. */
	RegisterServerProcess(MyProcPid, mode, slave_no, node_id);

	/*
	 * Set up real signal handlers after successfull authentication.  Note:
	 * signals remain blocked until one the HandleConnection routines unblock
	 * them.
	 */
	ImmediateInterruptOK = false;
	InterruptHoldoffCount = 0;
	ProcDiePending = false;

	/* Reset all signal flags for this process in shared memory */
	ResetForwarderChildSignals();
	
	pqsignal(SIGQUIT, sigquit_handler_child);
	pqsignal(SIGTERM, sigterm_handler_child);
	pqsignal(SIGINT, sigterm_handler_child);
	pqsignal(SIGUSR1, sigusr1_handler);
	pqsignal(SIGUSR2, sigusr2_handler);
	
	LWLockAcquire(MCPHostsLock, LW_SHARED);
	master_encoding = MCPHostsGetEncoding(ForwarderHosts);
	LWLockRelease(MCPHostsLock);
	
	if (mode == MCP_TYPE_MASTER && master_encoding != peer_encoding)
	{
		int 	i;
		/* set new master encoding */
		master_encoding = peer_encoding;
		
		LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
		MCPHostsSetEncoding(ForwarderHosts, master_encoding);
		LWLockRelease(MCPHostsLock);
		
		/* signal slaves to recheck their encoding */
		LWLockAcquire(MCPServerLock, LW_SHARED);
		for (i = 1; i < MCP_MAX_SLAVES; i++)
			SendForwarderChildSignal(i, FORWARDER_SIGNAL_CHECK_ENCODING);
		LWLockRelease(MCPServerLock);	
	}
	else if (mode == MCP_TYPE_SLAVE &&
			master_encoding != _PG_LAST_ENCODING_ &&
			master_encoding != peer_encoding)
			ereport(ERROR, 
					(errmsg("encoding mismatch"),
					 errdetail("slave encoding \"%s\" doesn't match master encoding \"%s\"",
					 pg_encoding_to_char(peer_encoding),
					 pg_encoding_to_char(master_encoding))));
	
	
	switch (mode)
	{
		case MCP_TYPE_MASTER:
			elog(LOG, "master process started");
			HandleMasterConnection(ForwarderQueue, ForwarderHosts);
			elog(LOG, "master process stopped");
			break;

		case MCP_TYPE_SLAVE:
			elog(LOG, "slave %d process started", slave_no);
			HandleSlaveConnection(ForwarderQueue, ForwarderHosts, 
								  slave_no, peer_encoding); 
			elog(LOG, "slave process stopped");
			break;
		
		default:
			elog(ERROR, "invalid peer type: %c", mode);
	}

	MCPFileCleanup();

	return 0;
}

/*
 * Returns the name of the table list file.  "suffix" is a string to be
 * appended, which should be specific to each slave or to a master.
 */
char *
MCPGetTableListFilename(char *suffix)
{
	char   *filename;

	filename = (char *) palloc(MAXPGPATH);
	snprintf(filename, MAXPGPATH,
			 MAMMOTH_FORWARDER_DIR "/table_list.%s", suffix);

	return filename;
}

/*
 * Regular shutdown handler.
 */
static void
sigterm_handler_child(SIGNAL_ARGS)
{
	/* Avoid nested proc_exit calls */
	if (!proc_exit_inprogress)
	{
		elog(DEBUG2, "child received a signal to terminate");
		InterruptPending = true;
		ProcDiePending = true;
		if (ImmediateInterruptOK && InterruptHoldoffCount == 0)
			ProcessInterrupts();
	}
}

/*
 * Emergency shutdown handler
 */
static void
sigquit_handler_child(SIGNAL_ARGS)
{
	/*
	 * DO NOT proc_exit() -- we're here because shared memory may be corrupted,
	 * so we don't want to try to clean up. Just nail the windows shut and get
	 * out of town.
	 *
	 * Note we do exit(1) not exit(0).	This is to force the MCP server into a
	 * system reset cycle if some idiot DBA sends a manual SIGQUIT to a random
	 * backend.  This is necessary precisely because we don't clean up our
	 * shared memory state.
	 */
	exit(1);
}

static void
sigusr1_handler(SIGNAL_ARGS)
{
	sigusr1_rcvd = true;
}

static void
sigusr2_handler(SIGNAL_ARGS)
{
	if (CheckForwarderChildSignal(FORWARDER_SIGNAL_PROMOTION))
		promotion_signal = true;
	if (CheckForwarderChildSignal(FORWARDER_SIGNAL_CHECK_ENCODING))
		run_encoding_check = true;
}

void
forwarder_reset_shared(void)
{
	MemoryContext old_cxt;

	/* initialize shared memory for mcp hosts */
	MCPHostsShmemInit();

	/* 
	 * XXX: Hack, the following call is expected to initialize
	 * shared memory variables, thus it should be context insensitive.
	 * However, PromotionACL is a immutable list that is set from the
	 * GUC variable, it should be initialized in a TopMemoryContext.
	 */
	old_cxt = MemoryContextSwitchTo(TopMemoryContext);
	/* Initialize global MCP variables */
	MCPInitVars(true);
	
	/* Initialize shmem structures supporing forwarder signals */
	FWSignalsInit();
	
	MemoryContextSwitchTo(old_cxt);
}
