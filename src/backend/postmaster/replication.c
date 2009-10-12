/*-----------------------
 * replication.c
 * 		The Mammoth Replication controlling daemon, either master or slave.
 *
 * This code is in charge of starting and stopping the replicator or
 * monitor process, depending on whether this is a master or slave server;
 * stopping same during a promotion, and generally taking care of making sure
 * the replication goes on sanely.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: replication.c 2210 2009-07-15 14:23:54Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/mammoth_indexing.h"
#include "catalog/namespace.h"
#include "catalog/repl_versions.h"
#include "libpq/pqsignal.h"
#include "mammoth_r/agents.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/forwcmds.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/promotion.h"
#include "mammoth_r/txlog.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "postmaster/fork_process.h"
#include "storage/pmsignal.h"
#include "postmaster/postmaster.h"
#include "postmaster/replication.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/ps_status.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>

#define REPLICATION_CATALOG_VERSION 2009013001

/* GUC variables */
bool replication_enable;
ReplicationMode replication_mode;
bool replication_slave_batch_mode;
bool replication_compression;
bool replication_use_utf8_encoding;
int replication_slave_no;
int replication_slave_batch_mode_timeout;
int replication_zlib_comp = 9;		/* PGLZ_Strategy */
char *replication_database;
char *replication_data_path;

/* True if the replication configuration is kosher */
bool replication_process_enable;
/* Turn off encoding conversion if database is SQL_ASCII */
bool replication_encoding_conversion_enable = true;

/*
 * Some global variables for replication status.
 */
static bool		am_replicator = false;
static time_t	time_replic_stop = 0;
static time_t	time_helper_stop = 0;

/*
 * For non-persistent slave connections: true when a connection is
 * requested
 */
static bool connection_requested = false;

/*
 * The MCPQueue variables
 */
MCPQueue	   *SlaveMCPQueue = NULL;
MCPQueue	   *MasterMCPQueue = NULL;
MCPLocalQueue  *MasterLocalQueue = NULL;

NON_EXEC_STATIC void ReplicationMain(int argc, char *argv[]);
#ifdef EXEC_BACKEND
static pid_t replic_forkexec(void);
static pid_t fwhelper_forkexec(void);
#endif

static void check_replication_schema(void);
static int handle_slave(void);
static int handle_master(void);

static void handle_sig_term(SIGNAL_ARGS);
static void handle_sig_quit(SIGNAL_ARGS);
static void sigusr1_handler(SIGNAL_ARGS);
static void FloatExceptionHandler(SIGNAL_ARGS);
static void PGREndConnection(int code, Datum arg);

/* A struct to hold all the data in shared memory */
typedef struct
{
	ReplicationSignalData		signal;
	ReplicationPromotionData	promotion;
} ReplicationSharedData;


void
replication_init(void)
{
	Oid		dbid,
			tblspcid;

	/* read the promotion file */
	process_promotion_file();

	/* Make sure replication_database is valid. */
	if (strcmp(replication_database, "") == 0)
		ereport(FATAL,
				(errmsg("replication database not specified"),
				 errhint("Specify the database you want to replicate with "
						 "the \"replication_database\" config option.")));
	if (!FindMyDatabase(replication_database, &dbid, &tblspcid) || !OidIsValid(dbid))
		ereport(FATAL,
				(errmsg("invalid replication database"),
				 errhint("Specify a valid database in \"replication_database\"")));

	/* check the forwarder configuration */
	check_forwarder_config();
}

/*
 * replication_start
 * 		Start the replication process, either slave or master.
 *
 * This code is heavily based on 8.1's autovacuum code, q.v.
 */
int
replication_start(void)
{
	pid_t	replic_pid;
	time_t	currtime;

	if (!ReplicationActive())
		return 0;

	/*
	 * Don't start replicator right after it died.  The stop time is set
	 * elsewhere, but note that it should be careful not to set it if the
	 * stop was because of a promotion.
	 */
	currtime = time(NULL);
#define MIN_REPLIC_WAIT 60
	if (currtime - time_replic_stop < MIN_REPLIC_WAIT)
		return 0;

	elog(LOG, "starting replication process");

#ifdef EXEC_BACKEND
	switch ((replic_pid = replic_forkexec()))
#else
	switch ((replic_pid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork replication process: %m")));
			break;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ReplicationMain(0, NULL);
			break;
#endif
		default:
			return (int) replic_pid;
	}

	/* shouldn't get here */
	return 0;
}

#ifdef EXEC_BACKEND
/*
 * replic_forkexec()
 *
 * Format up the arglist for the replication process, then fork and exec.
 */
static pid_t
replic_forkexec(void)
{
	char   *av[10];
	int		ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkrepl";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif /* EXEC_BACKEND */

void
replication_stopped(bool cause_delay)
{
	/*
	 * We don't want to start a new replication process right after the one
	 * that just stopped did it because of a crash.  So set the "stop time"
	 * here, and elsewhere make sure we don't start a new replication process
	 * unless some time has elapsed.
	 *
	 * However, on promotion we set the stop time to 0, so that the new process
	 * is started right away; we don't want to delay the promotion
	 * needlessly.  (This also applies to slave processes that die because they
	 * are in batch update mode.)
	 */
	if (cause_delay)
		time_replic_stop = time(NULL);
	else
		time_replic_stop = 0;
	elog(LOG, "replication process stopped");
}

bool
ReplicationActive(void)
{
	return replication_enable;
}

bool
IsReplicatorProcess(void)
{
	return am_replicator;
}

void
ReplicationMain(int argc, char *argv[])
{
	sigjmp_buf  local_sigjmp_buf;

	/* we are a postmaster subprocess now */
	IsUnderPostmaster = true;
	am_replicator = true;

	/* reset MyProcPid */
	MyProcPid = getpid();

	/* Lose the postmaster's on-exit routines */
	on_exit_reset();

	/* Identify myself via ps */
	if (replication_master)
		init_ps_display("queue monitor process", "", "", "");
	else
	{
		char msg[40];

		Assert(replication_slave);

		snprintf(msg, 40, "slave %d", replication_slave_no);
		init_ps_display("replicator process", msg, "", "");
	}
	set_ps_display("", true);

	SetProcessingMode(InitProcessing);

	/* 
	 * if possible, make this process a group leader, so that the
	 * postmaster  can  signal  any child process  too.   We don't 
	 * expect  the replication  process  to have children,  we are
	 * doing this for consistency reasons.
	 */ 
#ifdef HAVE_SETSID
	if (setsid() < 0)
		elog(ERROR, "setsid() failed: %m");
#endif

	/*
	 * Set up signal handlers.  We operate on databases much like a regular
	 * backend, so we use the same signal handling.  See equivalent code in
	 * tcop/postgres.c.
	 *
	 * Currently, we don't pay attention to postgresql.conf changes, so we can
	 * ignore SIGHUP.
	 */
	pqsignal(SIGHUP, SIG_IGN);

	/* We ignore SIGINT because we don't expect input from the user. */
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, handle_sig_term);
	pqsignal(SIGQUIT, handle_sig_quit);

	/*
	 * The signal handler for various timeout events.  Currently we use it
	 * only for deadlock checks.
	 */
	pqsignal(SIGALRM, handle_sig_alarm);

	pqsignal(SIGPIPE, SIG_IGN);

	/* SIGUSR1 means a backend needs something, need to check shmem */
	pqsignal(SIGUSR1, sigusr1_handler);

	/* We don't listen for async notifies */
	pqsignal(SIGUSR2, SIG_IGN);
	pqsignal(SIGFPE, FloatExceptionHandler);
	pqsignal(SIGCHLD, SIG_DFL);

	/* Early initialization */
	BaseInit();

    /*
     * Create a per-backend PGPROC struct in shared memory, except in the
     * EXEC_BACKEND case where this was done in SubPostmasterMain. We must do
     * this before we can use LWLocks (and in the EXEC_BACKEND case we already
     * had to do some stuff with LWLocks).
     */
#ifndef EXEC_BACKEND
    InitProcess();
#endif

	InitPostgres(replication_database, InvalidOid, NULL, NULL);

	SetProcessingMode(NormalProcessing);

	elog(LOG, "replicator process started");
	
	if (replication_use_utf8_encoding && 
	   !replication_encoding_conversion_enable)
	{
		ereport(WARNING, 
		(errmsg("replication encoding conversion is not applicable to database with SQL_ASCII encoding"),
		errhint("turn off replication_use_utf8 configuration option")));
	}

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * See notes in postgres.c about the design of this coding.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * At this point we do a non-graceful shutdown, so that any error will
		 * be handled as a process crash.  Since we called InitPostgres, a
		 * ProcKill was registered to be called at process exit, so it will
		 * clean any locks we may be holding.
		 */
		proc_exit(1);
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/* verify that the user installed the correct pg_catalog.repl_* catalogs */
	check_replication_schema();

	PG_SETMASK(&UnBlockSig);

    if (replication_slave)
		handle_slave();
	else
	{
		Assert(replication_master);
		handle_master();
	}
}

/*
 * Initialize and open the MCP queues. The queue data files and shared memory
 * structures should be already created at startup.
 *
 * This is part of InitPostgres.
 */
void
InitializeMCPQueue(void)
{
	/* 
	 * Initialize the forwarder's queue for reporting purposes. Do it only
	 * if forwarder is enabled in the config.
	 */
	if (ForwarderEnable)
	{
		ForwarderQueue = MCPQueueInit(true);
		/* we don't need TXLOG for this queue */
		ForwarderHosts = MCPHostsInit();
	}

	if (!replication_enable)
		return;

	/* The master queue is needed on all backends */
	if (replication_master)
	{
		/*
		 * XXX Maybe this should be on critical section, so that errors are
		 * upgraded to a higher severity level?
		 */
		MasterMCPQueue = MCPQueueInit(false);
		LockReplicationQueue(MasterMCPQueue, LW_SHARED);
		TXLOGStartup(MCPQueueGetFirstRecno(MasterMCPQueue));
		UnlockReplicationQueue(MasterMCPQueue);
		MasterLocalQueue = MCPLocalQueueInit(MasterMCPQueue);

		on_proc_exit(PGREndConnection, 0);
	}
	/* The slave queue is needed on all backends (for reporting purposes) */
	if (replication_slave)
	{
		SlaveMCPQueue = MCPQueueInit(false);
		/* we don't need TXLOG for this queue */

		on_proc_exit(PGREndConnection, 0);
	}
}

/*
 * Initialization of shared memory
 */
int
ReplicationShmemSize(void)
{
	return MAXALIGN(sizeof(ReplicationSharedData));
}

void
ReplicationShmemInit(void)
{
	ReplicationSharedData  *shared;
	bool					found;

	shared = (ReplicationSharedData *)
		ShmemInitStruct("ReplicationData",
						ReplicationShmemSize(),
						&found);

	if (!IsUnderPostmaster)
	{
		Assert(!found);

		memset(shared, 0, sizeof(ReplicationSharedData));
	}
	else
		Assert(found);

	ReplSignalData = &(shared->signal);
	ReplPromotionData = &(shared->promotion);
}

/*
 * check_replication_schema
 *
 * Verify that the user already installed the correct pg_catalog.repl_*
 * catalogs.  The system does not work without it.
 *
 * We are already connected to the replicated database when this is run,
 * but there is no transaction running, and there will be no transaction
 * when it exists.
 */
static void
check_replication_schema(void)
{
	Relation	vers;
	SysScanDesc scan;
	HeapTuple	tuple;
	Form_repl_versions versionForm;

	StartTransactionCommand();

	/* Verify that the repl_versions table exists */
	vers = RelationIdGetRelation(ReplVersionsId);
	if (!RelationIsValid(vers))
		goto common_failure;

	/* Reopen it, with an appropiate lock */
	vers = heap_open(ReplVersionsId, AccessShareLock);

	/* Release the refcount acquired by RelationIdGetRelation */
	RelationDecrementReferenceCount(vers);

	/* ... and that it has the latest version applied */
	scan = systable_beginscan(vers, ReplVersionsVersionIndexId, true, 
							  SnapshotNow, 0, NULL);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		goto common_failure;

	/*
	 * Advance until the end of the scan (This is a hack -- should use a
	 * backwards scan instead)
	 */
	while (HeapTupleIsValid(tuple))
	{
		HeapTuple	nexttuple = systable_getnext(scan);

		if (HeapTupleIsValid(nexttuple))
			tuple = nexttuple;
		else
			break;
	}

	versionForm = (Form_repl_versions) GETSTRUCT(tuple);
	if (versionForm->version != REPLICATION_CATALOG_VERSION)
	{
		ereport(LOG,
				(errmsg("wrong version of replication schema found"),
				 errdetail("Latest version applied is %d, expected %d.",
						   versionForm->version,
						   REPLICATION_CATALOG_VERSION)));
		goto failure;
	}

	systable_endscan(scan);
	heap_close(vers, AccessShareLock);

	/* Everything seems to be OK */
	CommitTransactionCommand();
	return;

common_failure:
	ereport(LOG,
			(errmsg("replication schema not installed"),
			 errhint("Run init-mammoth-database to create the replication schema.")));
failure:
	proc_exit(REPLICATOR_EXIT_WRONG_SCHEMA);
}

static int
handle_slave(void)
{
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		if (!replication_slave_batch_mode ||
			(replication_slave_batch_mode && connection_requested))
		{
			int		ret;

			/* Reset the connect-on-demand flag for the next iteration */
			connection_requested = false;

			elog(LOG, "slave %d connecting to MCP server",
				 replication_slave_no);

			ret = ReplicationSlaveMain(SlaveMCPQueue, replication_slave_no);

			/* In case signals were blocked in previous call - unblock them */
			PG_SETMASK(&UnBlockSig);

			if (ret == -1)
				break;
		}
		pg_usleep(5 * 1000 * 1000);	/* 5 secs */
	}
	return 1;
}

static int
handle_master(void)
{
	LockReplicationQueue(MasterMCPQueue, LW_SHARED);

	elog(DEBUG4, "MCPQueue lrecno="UNI_LLU,
		 MCPQueueGetLastRecno(MasterMCPQueue));

	UnlockReplicationQueue(MasterMCPQueue);

	for (;;)
	{
		int ret;

		CHECK_FOR_INTERRUPTS();

		ret = ReplicationMasterMain(MasterMCPQueue);

		/* if signals were blocked in previous call, unblock them */
		PG_SETMASK(&UnBlockSig);

		if (ret == -1)
			break;

		pg_usleep(5 * 1000 * 1000);	/* 5 secs */
	}
	return 1;
}

static void
handle_sig_term(SIGNAL_ARGS)
{
	elog(LOG, "SIGTERM received in replication process");
	proc_exit(0);
}

static void
handle_sig_quit(SIGNAL_ARGS)
{
	elog(LOG, "SIGQUIT received in replication process");
	proc_exit(0);
}

static void
FloatExceptionHandler(SIGNAL_ARGS)
{
	elog(LOG, "float exception handler");
}

/*
 * on_proc_exit handler
 *
 * close the MCPQueue on backend exit
 */
static void
PGREndConnection(int code, Datum arg)
{
	elog(DEBUG2, "PGREndConnection");
	if (MasterMCPQueue != NULL)
	{
		MCPQueueDestroy(MasterMCPQueue);
		MasterMCPQueue = NULL;

		if (MasterLocalQueue != NULL)
		{
			MCPLocalQueueClose(MasterLocalQueue);
			MasterLocalQueue = NULL;
		}
	}
	if (SlaveMCPQueue != NULL)
	{
		MCPQueueDestroy(SlaveMCPQueue);
		SlaveMCPQueue = NULL;
	}
}

/*
 * sigusr1_handler
 *
 * Receives a SIGUSR1 signal, supposedly coming from the Postmaster.  This
 * signal means some user issued a PROMOTE command on a backend; the backend
 * signalled the postmaster, and the postmaster signalled us.  We cannot act
 * on the signal immediately because we may be processing other messages
 * incoming from the master.  So just set a flag which will be seen the next
 * time we run the outer loop in slave_mcp_agent.
 *
 * The complete information about the promotion request was already saved by
 * the original backend in shared memory.
 */
static void
sigusr1_handler(SIGNAL_ARGS)
{
	elog(DEBUG2, "sigusr1_handler");

	if (ReplSignalData->batchupdate)
		connection_requested = true;

	/* promotion? */
	if (ReplSignalData->promotion)
		promotion_request = true;
}

/*
 * ProcessMcpStmt
 *
 * Executes a MCP {BATCHUPDATE, REFRESH} command.
 */
void
ProcessMcpStmt(McpStmt *stmt)
{
	if (!replication_enable)
		elog(ERROR, "MCP commands can only be used in replication mode");

	switch (stmt->kind)
	{
		case McpBatchUpdate:
			{
				if (!replication_slave)
					elog(ERROR, "MCP BATCHUPDATE is slave only command");
				if (replication_slave && !replication_slave_batch_mode)
					elog(ERROR, "Enable replication_slave_batch_mode "
						 "configuration parameter to use this command");

				LWLockAcquire(ReplicationLock, LW_EXCLUSIVE);
				ReplSignalData->batchupdate = true;
				LWLockRelease(ReplicationLock);

				SendPostmasterSignal(PMSIGNAL_REPLICATOR);
				break;
			}
	}
}

/*
 * Act on receiving a PROMOTE command.
 */
void
ProcessPromoteStmt(PromoteStmt *stmt)
{
	elog(DEBUG2, "PROMOTE%s%s",
		 stmt->force ? " FORCE" : "",
		 stmt->back ? " BACK" : "");
	
	if (!replication_enable)
		elog(ERROR, "Promotion commands can only be used in replication mode");

	/* BACK PROMOTION can be launched from master */
	if (!replication_slave && !stmt->back)
		elog(ERROR, "PROMOTE or PROMOTE FORCE can only be used in a slave");
	if (replication_slave && replication_slave_batch_mode)
		elog(ERROR, "PROMOTE can only be used when replication_slave_batch_mode"
			 " configuration parameter is disabled");
	if (!superuser())
		elog(ERROR, "PROMOTE requires superuser rights");

	BackendInitiatePromotion(stmt);
}

/*
 *
 * forwarder_helper_start
 *		Start the forwarder helper process.
 */
int
forwarder_helper_start(void)
{
	pid_t	fwhpid;
	time_t	currtime;
	
	/* Don't start the helper right after it died.  The stop time is
	 * set elsewhere.
	 */
	currtime = time(NULL);
#define MIN_HELPER_WAIT 60
	if (currtime - time_helper_stop < MIN_HELPER_WAIT)
		return 0;

	elog(LOG, "starting forwarder helper process");

#ifdef EXEC_BACKEND
	switch ((fwhpid = fwhelper_forkexec()))
#else
	switch ((fwhpid = fork_process()))
#endif
	{
		case -1:
			ereport(LOG,
					(errmsg("could not fork forwarder helper process: %m")));
			break;

#ifndef EXEC_BACKEND
		case 0:
			/* in postmaster child ... */
			/* Close the postmaster's sockets */
			ClosePostmasterPorts(false);

			ForwarderHelperMain(0, NULL);
			break;
#endif
		default:
			return (int) fwhpid;
	}

	/* shouldn't get here */
	return 0;
}

#ifdef EXEC_BACKEND
/*
 * fwhelper_forkexec()
 *
 * Format up the arglist for the forwarder helper process, then fork and exec.
 */
static pid_t
fwhelper_forkexec(void)
{
	char   *av[10];
	int		ac = 0;

	av[ac++] = "postgres";
	av[ac++] = "--forkfwhlpr";
	av[ac++] = NULL;			/* filled in by postmaster_forkexec */
	av[ac] = NULL;

	Assert(ac < lengthof(av));

	return postmaster_forkexec(ac, av);
}
#endif /* EXEC_BACKEND */

void
forwarder_helper_stopped(void)
{
	time_helper_stop = time(NULL);
}
