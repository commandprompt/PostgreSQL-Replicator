/*
 * 		fwsignals.c
 *			Stuff for reusing SIGUSR2 in forwarder processes to send
 *			multiple signal 'reasons' from one forwarder process to
 *			another. This is modelled after pmsignal.c in postmaster.
 *
 * $Id: fwsignals.c 2110 2009-04-23 17:24:45Z alvherre $ 
 */
#include "postgres.h"

#include <signal.h>

#include "mammoth_r/fwsignals.h"
#include "mammoth_r/mcp_processes.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* export from mcp_server.c */
extern int ForwarderProcessId;

/* description of signals for a single process */
typedef struct ForwarderSignal
{
	sig_atomic_t sigflags[NUM_FORWARDER_SIGNALS];
} ForwarderSignal;

ForwarderSignal *FWSignals;

/* Initialization of forwarder signal flags in shared memory */
void
FWSignalsInit(void)
{
	bool 	found;
	
	FWSignals = (ForwarderSignal *) 
		ShmemInitStruct("FWSignals",
						(MCP_MAX_SLAVES + 1) * sizeof(ForwarderSignal),
						&found);
	if (!found)
		MemSet(FWSignals, 0, sizeof(ForwarderSignal) * (MCP_MAX_SLAVES + 1));
						
}

/* Functions for sending/receiving signals in forwarder subprocesses */
void 
SendForwarderChildSignal(int nodeno, ForwarderSignalReason reason)
{
	int 	result;
	pid_t 	node_pid;
	
	/* make sure we hold a lock, the signal will be sent to the right process */
	Assert(LWLockHeldByMe(MCPServerLock));
	
	node_pid = ServerCtl->node_pid[nodeno];
	
	if (node_pid != 0)
	{
		/* Set the reason in shared memory and send a signal to check its */
		FWSignals[nodeno].sigflags[reason] = true;
		
		result = kill(node_pid, SIGUSR2);
		if (result != 0)
			elog(WARNING, "kill failed: %m");
	}
	
}

/* 
 * Check whether the specific signal is received by the node, clear the 
 * signal flag 
 */
bool 
CheckForwarderChildSignal(ForwarderSignalReason reason)
{
	int 	nodeno = ForwarderProcessId;
	
	if (FWSignals[nodeno].sigflags[reason])
	{
		FWSignals[nodeno].sigflags[reason] = false;
		return true;
	}
	else
		return false;
}

/* 
 * Reset all signal flags for a specific process. Should be called once
 * during process startup.
 */
void
ResetForwarderChildSignals(void)
{
	int nodeno = ForwarderProcessId;
	
	MemSet(FWSignals[nodeno].sigflags, 0, sizeof(FWSignals[nodeno].sigflags));
}

