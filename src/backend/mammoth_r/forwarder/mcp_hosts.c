/*-----------------------
 * mcp_hosts.c
 * 		The Mammoth Replication MCP Hosts implementation
 *
 * MCP Hosts is a struct for keeping track of status of slave nodes in the
 * replication forwarder.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_hosts.c 2227 2009-09-22 07:04:32Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include <time.h>

#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_hosts.h"
#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/paths.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/guc.h"


int mcp_max_slaves = MCP_MAX_SLAVES;

typedef struct TxHostsRecord
{
	ullong      frecno;			/* first recno */
	ullong      vrecno;			/* acknowledged recno */
	time_t      timestamp;		/* timestamp of last get operation */
	MCPQSync    sync;			/* synchronized status */
} TxHostsRecord;

/* typedef appears in mcp_hosts.h */
struct MCPHosts
{
	int				h_fid;			/* id of txdata file */
#define TXHmagic 0x54584800
#define TXHinit  0x494E4954
	int         	h_maxhosts;		/* maximum hosts number which want to
									   retrieve data from MCP queue */
	pg_enc			h_encoding;		/* encoding of the MCP queue data */
	TxHostsRecord	h_hosts[MCP_MAX_SLAVES];
	LWLockId		h_locks[MCP_MAX_SLAVES];
};

/* when stored on disk, it doesn't have h_locks */
#define MCPHOSTS_DISKSZ	(offsetof(MCPHosts, h_locks))

/*
 * Having a host locked means holding its individual lock or alternatively
 * holding the global lock.
 */
#define ASSERT_HOST_LOCK_HELD(_h_, _hostno_) \
	Assert(LWLockHeldByMe((_h_)->h_locks[_hostno_]) || \
		   LWLockHeldByMe(MCPHostsLock))

/* the path where this stuff lives on disk */
#define HOSTS_FILENAME (MAMMOTH_FORWARDER_DIR "/hosts")


/*
 * Create a MCPHosts struct, open it and return it.
 */
MCPHosts *
MCPHostsInit(void)
{
	MCPHosts   *h;
	bool		found;

	h = ShmemInitStruct("Hosts HDR", sizeof(MCPHosts), &found);
	Assert(found);

	LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
	if (h->h_fid == TXHinit)
	{
		MCPFile		*f = MCPFileCreate(HOSTS_FILENAME);

		MCPFileOpen(f);
		MCPFileRead(f, h, MCPHOSTS_DISKSZ, false);

		if (h->h_fid != TXHmagic)
			elog(ERROR, "incorrect .txh file identificator %d, expected %d",
				 h->h_fid, TXHmagic);

		/* XXX anything else? */

		MCPFileDestroy(f);
	}
	LWLockRelease(MCPHostsLock);

	return h;
}

/*
 * Initialize shared memory to hold the header and per-host records
 */
void
MCPHostsShmemInit(void)
{
	MCPHosts   *hosts;
	bool		found;

	hosts = ShmemInitStruct("Hosts HDR", sizeof(MCPHosts), &found);
	if (!found)
	{
		int		i;

		MemSet(hosts, 0, sizeof(MCPHosts));
		hosts->h_fid = TXHinit;

		for (i = 0; i < MCP_MAX_SLAVES; i++)
			hosts->h_locks[i] = LWLockAssign();
	}
}

/*
 * Bootstrap an MCPHosts header.
 */
void
BootStrapMCPHosts(void)
{
	MCPHosts	h;
	MCPFile	   *f = MCPFileCreate(HOSTS_FILENAME);

	if (!MCPFileCreateFile(f))
		ereport(ERROR,
				(errmsg("could not create hosts header file \"%s\": %m",
						MCPFileGetPath(f))));

	MemSet(&h, 0, sizeof(MCPHosts));
	h.h_fid = TXHmagic;
	h.h_maxhosts = MCP_MAX_SLAVES;

	/* encoding is initially undefined */
	h.h_encoding = _PG_LAST_ENCODING_;

	MCPFileWrite(f, &h, MCPHOSTS_DISKSZ);

	MCPFileDestroy(f);
}

/*
 * Write hosts info to disk.
 */
void
MCPHostsClose(MCPHosts *h)
{
	MCPFile	   *f;

	f = MCPFileCreate(HOSTS_FILENAME);

	MCPFileOpen(f);
	MCPFileWrite(f, h, MCPHOSTS_DISKSZ);
	MCPFileDestroy(f);
}

/* Advance slave current transaction recno to the next transaction */
void
MCPHostsNextTx(MCPHosts *h, MCPQueue *q, int hostno, ullong last_recno)
{
	if (hostno >= h->h_maxhosts)
		elog(ERROR, "hostno %d out of boundary", hostno);

	ASSERT_HOST_LOCK_HELD(h, hostno);

	/*
	 * Make sure we don't have open transaction while switching the current
	 * one.
	 */
	Assert(MCPQueueGetDatafile(q) == NULL);

	if (h->h_hosts[hostno].frecno > last_recno)
		return;

	h->h_hosts[hostno].frecno++;
	h->h_hosts[hostno].timestamp = time(NULL);
}

/* 
 * Get the minimum of acknowledged records among the connected
 * hosts. Any record number not higher than this minimum corresponds
 * to a no longer needed queue transaction.
 */ 
ullong
MCPHostsGetMinAckedRecno(MCPHosts *h, pid_t *node_pid)
{
	ullong          recno = InvalidRecno;
	int             i;

	Assert(LWLockHeldByMe(MCPHostsLock));

	/* Looking for min h_hosts[i].vrecno  */
	for (i = 0; i < h->h_maxhosts; i++)
	{
		/* Ignore disconnected slaves */
		if (h->h_hosts[i].vrecno == InvalidRecno ||
			node_pid[i + 1] == 0)
			continue;
		if (recno == InvalidRecno)
			recno = h->h_hosts[i].vrecno;
		else if (recno > h->h_hosts[i].vrecno)
			recno = h->h_hosts[i].vrecno;
	}
	return recno;
}

/*
 * Gets the record number before which all records can be removed.
 * No actual queue modifications here.
 */
ullong
MCPHostsGetPruningRecno(MCPHosts *h, MCPQueue *q, 
						ullong vrecno, 
						ullong dump_start_recno,
						ullong dump_end_recno,
						off_t  dump_cache_max_size, 
						pid_t *node_pid)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	Assert(QueueLockHeldByMe(q));

	elog(DEBUG4, "MCPHostsOptimizeQueue");
		
	/* check whether the dump is in the queue */
	if (dump_start_recno != InvalidRecno)
	{
		/* check whether some of the slaves are behind full dump */
		if (vrecno < dump_start_recno - 1)
		{
			int 	i;
			/* desync those slaves that are behind the dump */
			for (i = 0; i < h->h_maxhosts; i++) 
			{
				if ((node_pid[i] != 0) && 
					(h->h_hosts[i].vrecno != InvalidRecno) &&
					(h->h_hosts[i].vrecno < dump_start_recno - 1))
						h->h_hosts[i].sync = MCPQUnsynced;
			}
			vrecno = dump_start_recno - 1;
		}
		else if ((dump_end_recno != InvalidRecno) && (vrecno >= dump_end_recno))
		{
			/* 
			 * Check if we can get of the full dump in queue, which will happen
			 * if total size of the data we can potentially remove will be larger
			 * than dump_cache_max_size. Otherwise we'll keep the dump and data
			 * after it as a cache for new slaves.
			 */
			off_t unused_data_size = MCPQueueCalculateSize(q, vrecno + 1);
			
			elog(DEBUG4, "max unused queue size: "UINT64_FORMAT, (ullong) dump_cache_max_size);			
			if (unused_data_size < dump_cache_max_size)
			{
				/* limit is not reached, do not remove anything */
				elog(DEBUG4, "current unused queue size: "UINT64_FORMAT, 
					 (ullong) unused_data_size);
				vrecno = InvalidRecno;
			}
			else
				elog(DEBUG4, "size of the queue data to be removed: "UINT64_FORMAT, 
					 (ullong) unused_data_size);	
		}
		else
		{
			/* 
			 * either slaves are still restoring the dump or dump is not received
			 * completely yet. Anyway, we can't remove it.
			 */
			vrecno = InvalidRecno;
		}
	}
	if (vrecno == InvalidRecno)
		elog(DEBUG4, "pruning recno is not available");
	else
		elog(DEBUG4, "pruning recno is "UINT64_FORMAT, vrecno);
	return vrecno;
}

/* 
 * MCPHostsCleanup
 *
 * Removes data that is no longer needed from the queue, and adjusts
 * slaves next records if necessary. 
 */
void
MCPHostsCleanup(MCPHosts *h, MCPQueue *q, ullong recno)
{
	int		hostno = 0;

	LWLockAcquire(ReplicationQueueTruncateLock, LW_EXCLUSIVE);

	LockReplicationQueue(q, LW_EXCLUSIVE);

	/* need to fix up MCPHosts before removing anything from the queue */
	MCPHostsLockAll(h, LW_EXCLUSIVE);
	for (; hostno < h->h_maxhosts; hostno++)
	{
		if (MCPHostsGetFirstRecno(h, hostno) < recno)
			MCPHostsSetFirstRecno(h, hostno, recno);
	}
	MCPHostsUnlockAll(h);

	MCPQueueCleanup(q, recno);

	UnlockReplicationQueue(q);

	LWLockRelease(ReplicationQueueTruncateLock);
}

/* Return first-to-process recno for the slave */
ullong
MCPHostsGetFirstRecno(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	return h->h_hosts[hostno].frecno;
}

/* Set first-to-process recno for the slave */
void
MCPHostsSetFirstRecno(MCPHosts *h, int hostno, ullong new_frecno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	h->h_hosts[hostno].frecno = new_frecno;
}

/* Return acknowledged recno for the slave */
ullong
MCPHostsGetAckedRecno(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	return h->h_hosts[hostno].vrecno;
}

/* Set acknowledged recno for the slave */
void
MCPHostsSetAckedRecno(MCPHosts *h, int hostno, ullong new_vrecno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	h->h_hosts[hostno].vrecno = new_vrecno;
}

/* Return the sync status for the slave */
MCPQSync
MCPHostsGetSync(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	return h->h_hosts[hostno].sync;
}

/* Set sync status for the given slave */
void
MCPHostsSetSync(MCPHosts *h, int hostno, MCPQSync sync)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	elog(DEBUG4, "MCPHostSetSync %s -> %s",
		 MCPQSyncAsString(h->h_hosts[hostno].sync),
		 MCPQSyncAsString(sync));
	h->h_hosts[hostno].sync = sync;
}

/*
 * Log the specified slave status to the server log.  If hostno is passed as
 * -1, all slaves are reported.
 */
void
MCPHostsLogTabStatus(int elevel, MCPHosts *h, int hostno, char *prefix, pid_t *node_pid)
{
	int		startpoint;
	int		endpoint;

	if (hostno == -1)
	{
		startpoint = 0;
		endpoint = h->h_maxhosts - 1;
	}
	else
		startpoint = endpoint = hostno;

	for (hostno = startpoint; hostno <= endpoint; hostno++)
	{
		if (node_pid[hostno + 1] == 0)
			continue;
		elog(elevel,
			 "%s: slave(%d), f="UNI_LLU" v="UNI_LLU" sync: %s",
			 prefix,
			 hostno,
			 h->h_hosts[hostno].frecno,
			 h->h_hosts[hostno].vrecno,
			 MCPQSyncAsString(h->h_hosts[hostno].sync));
	}
}

time_t
MCPHostsGetTimestamp(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);
	return h->h_hosts[hostno].timestamp;
}

int
MCPHostsGetMaxHosts(MCPHosts *h)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return h->h_maxhosts;
}

void
MCPHostLock(MCPHosts *h, int hostno, LWLockMode mode)
{
	LWLockAcquire(MCPHostsLock, mode);
	LWLockAcquire(h->h_locks[hostno], mode);
	LWLockRelease(MCPHostsLock);
}

void
MCPHostUnlock(MCPHosts *h, int hostno)
{
	LWLockRelease(h->h_locks[hostno]);
}

void
MCPHostsLockAll(MCPHosts *h, LWLockMode mode)
{
	int 	i;

	LWLockAcquire(MCPHostsLock, mode);
	for (i = 0; i < h->h_maxhosts; i++)
		LWLockAcquire(h->h_locks[i], mode);
}

void
MCPHostsUnlockAll(MCPHosts *h)
{
	int 	i;

	for (i = 0; i < h->h_maxhosts; i++)
		LWLockRelease(h->h_locks[i]);

	LWLockRelease(MCPHostsLock);
}

/* Set/get encoding attribute in the hosts header */
void
MCPHostsSetEncoding(MCPHosts *h, pg_enc new_encoding)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	h->h_encoding = new_encoding;
}

pg_enc
MCPHostsGetEncoding(MCPHosts *h)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return h->h_encoding;
}
