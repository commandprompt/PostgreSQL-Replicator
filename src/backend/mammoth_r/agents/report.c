/*
 *
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/mcp_processes.h"
#include "postmaster/replication.h"
#include "mammoth_r/agents.h"

extern Datum mammoth_queue_status(PG_FUNCTION_ARGS);
extern Datum forwarder_queue_status(PG_FUNCTION_ARGS);
extern Datum mammoth_hosts_status(PG_FUNCTION_ARGS);

static HeapTuple _get_queue_status(bool forwarder);

HeapTuple
_get_queue_status(bool forwarder)
{
	Datum		values[8];
	bool		nulls[8];
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	MCPQueue   *queue;
	TimestampTz in_timestamp;
	TimestampTz out_timestamp;

	tupdesc = CreateTemplateTupleDesc(8, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "brecno",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "frecno",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "lrecno",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "vrecno",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "sync",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "empty",
					   BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 7, "in_timestamp",
					   TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 8, "out_timestamp",
					   TIMESTAMPTZOID, -1, 0);
	
	tupdesc = BlessTupleDesc(tupdesc);

	MemSet(nulls, 0, sizeof(nulls));

	if (forwarder)
		queue = ForwarderQueue;
	else
	{
		if (replication_master)
			queue = MasterMCPQueue;
		else
			queue = SlaveMCPQueue;
	}
	Assert(queue != NULL);

	LockReplicationQueue(queue, LW_SHARED);

	values[0] = Int64GetDatum(MCPQueueGetInitialRecno(queue));
	values[1] = Int64GetDatum(MCPQueueGetFirstRecno(queue));
	values[2] = Int64GetDatum(MCPQueueGetLastRecno(queue));
	values[3] = Int64GetDatum(MCPQueueGetAckRecno(queue));
	values[4] = Int32GetDatum(MCPQueueGetSync(queue));
	values[5] = BoolGetDatum(MCPQueueIsEmpty(queue));

	in_timestamp = time_t_to_timestamptz(MCPQueueGetEnqueueTimestamp(queue));
	out_timestamp = time_t_to_timestamptz(MCPQueueGetDequeueTimestamp(queue));
	
	values[6] = TimestampTzGetDatum(in_timestamp);
	values[7] = TimestampTzGetDatum(out_timestamp);
	
	UnlockReplicationQueue(queue);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return tuple;
}

Datum
mammoth_queue_status(PG_FUNCTION_ARGS)
{
	HeapTuple 	tuple;
	
	if (!replication_enable)
		ereport(ERROR, 
				(errmsg("can't acquire queue status"),
				errhint("replication must be enabled")));

	tuple = _get_queue_status(false);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
forwarder_queue_status(PG_FUNCTION_ARGS)
{
	HeapTuple 	tuple;

	if (!ForwarderEnable)
		ereport(ERROR, (errmsg("can't acquire forwarder status"),
						errhint("forwarder must be enabled")));

	tuple = _get_queue_status(true);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
mammoth_hosts_status(PG_FUNCTION_ARGS)
{
	FuncCallContext    *funcctx;
	MemoryContext 		oldctx;

	if (!ForwarderEnable)
		ereport(ERROR, (errmsg("can't acquire forwarder hosts status"),
						errhint("forwarder must be enabled")));

	Assert(ForwarderHosts != NULL);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc 	tupdesc;
		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		
		tupdesc =  CreateTemplateTupleDesc(6, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "slaveno",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "connected",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sync",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "timestamp",
						   TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "frecno",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "vrecno",
						   INT8OID, -1, 0);
	
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		
		/* Set max_calls to the number of slaves */
		LWLockAcquire(MCPHostsLock, LW_SHARED);
		funcctx->max_calls = MCPHostsGetMaxHosts(ForwarderHosts);
		LWLockRelease(MCPHostsLock);

		MemoryContextSwitchTo(oldctx);
	}
	funcctx = SRF_PERCALL_SETUP();
	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum 	values[6];
		bool 	nulls[6];
		bool 	present;
		TimestampTz ts;
		HeapTuple 	tuple;

		int 	slaveno = funcctx->call_cntr;
		MemSet(nulls, 0, sizeof(nulls));

		LWLockAcquire(MCPServerLock, LW_SHARED);
		present = ServerCtl->node_pid[slaveno];
		LWLockRelease(MCPServerLock);

		values[0] = Int32GetDatum(slaveno);
		values[1] = BoolGetDatum(present);

		/* Acquire data from the hosts */
		LWLockAcquire(MCPHostsLock, LW_SHARED);

		values[2] = Int32GetDatum(MCPHostsGetSync(ForwarderHosts, slaveno));
		ts = time_t_to_timestamptz(MCPHostsGetTimestamp(ForwarderHosts, 
														slaveno));
		values[3] = TimestampTzGetDatum(ts);
		values[4] = Int64GetDatum(MCPHostsGetHostRecno(ForwarderHosts, 
													   McphHostRecnoKindSendNext,
													   slaveno));
		values[5] = Int64GetDatum(MCPHostsGetHostRecno(ForwarderHosts, 
													   McphHostRecnoKindAcked,
													   slaveno));

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		LWLockRelease(MCPHostsLock);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
		SRF_RETURN_DONE(funcctx);
}
