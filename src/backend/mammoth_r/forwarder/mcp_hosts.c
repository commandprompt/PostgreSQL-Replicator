/*-----------------------
 * mcp_hosts.c
 * 		The Mammoth Replication MCP Hosts implementation
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

typedef struct TxHostsHeader
{
	int			fid;			/* id of txdata file */
#define TXHmagic 0x54584800
#define TXHinit  0x494E4954
	int         maxhosts;		/* maximum hosts number which want to retrieve
								 * data from MCP queue */
	pg_enc		encoding;		/* encoding of the MCP queue data */
} TxHostsHeader;

typedef struct TxHostsRecord
{
	ullong      frecno;			/* first recno */
	ullong      vrecno;			/* acknowledged recno */
	time_t      timestamp;		/* timestamp of last get operation */
	MCPQSync    sync;			/* synchronized status */
} TxHostsRecord;

#define TxHostsFullSize \
	(sizeof(TxHostsHeader) + MCP_MAX_SLAVES * sizeof(TxHostsRecord))


struct MCPHosts
{
	MCPFile		   *txhosts;
	TxHostsHeader  *txhosts_hdr;
	TxHostsRecord  *txhosts_tab;
	LWLockId	   *txhosts_locks;
};

static void mcp_hosts_open(MCPHosts *h);

/*
 * Having a host locked means holding its individual lock or alternatively
 * holding the global lock.
 */
#define ASSERT_HOST_LOCK_HELD(_h_, _hostno_) \
	Assert(LWLockHeldByMe((_h_)->txhosts_locks[(_hostno_)]) || \
		   LWLockHeldByMe(MCPHostsLock))

/*
 * Create an empty MCPHosts struct, open it and return it.
 */
MCPHosts *
MCPHostsInit(void)
{
	MCPHosts   *h;
	char		filename[64] = MAMMOTH_FORWARDER_DIR "/hosts";

	h = (MCPHosts *) palloc0(sizeof(MCPHosts));

	h->txhosts = MCPFileCreate(filename);

	h->txhosts_hdr = NULL;
	h->txhosts_tab = NULL;

	mcp_hosts_open(h);

	return h;
}

/* Initialize shared memory to hold host's header */
void
MCPHostsShmemInit(void)
{
	TxHostsHeader	*shared;
	bool			found;
	
	shared = ShmemInitStruct("Hosts HDR", TxHostsFullSize,
							 &found);
	if (!IsUnderPostmaster)
	{
		Assert(!found);
		memset(shared, 0, TxHostsFullSize);
		shared->fid = TXHinit;
	}
	else
		Assert(found);
}

/*
 * Open the hosts file and map the interesting portion to a shared memory area
 */
static void
mcp_hosts_open(MCPHosts *h)
{
	off_t		sz;
	int			i;
	LWLockId   *HostLocks;
	bool		found;

	/* Open TxHosts file */
	MCPFileCreateFile(h->txhosts);
	sz = MCPFileSeek(h->txhosts, 0, SEEK_END);
	if (sz < sizeof(TxHostsHeader))
	{
		TxHostsHeader   hdr;
		TxHostsRecord   rec;

		MCPFileTruncate(h->txhosts, 0);
		MCPFileSeek(h->txhosts, 0, SEEK_SET);

		memset(&hdr, 0, sizeof(TxHostsHeader));
		hdr.fid = TXHmagic;
		hdr.maxhosts = MCP_MAX_SLAVES;
		/* encoding is undefined for the empty hosts */
		hdr.encoding = _PG_LAST_ENCODING_;

		MCPFileWrite(h->txhosts, &hdr, sizeof(TxHostsHeader));

		memset(&rec, 0, sizeof(TxHostsRecord));
		for (i = 0; i < hdr.maxhosts; i++)
			MCPFileWrite(h->txhosts, &rec, sizeof(TxHostsRecord));
	}
	MCPFileSeek(h->txhosts, 0, SEEK_SET);

	HostLocks = (LWLockId *) palloc(sizeof(LWLockId) * MCP_MAX_SLAVES);

	for (i = 0; i < MCP_MAX_SLAVES; i++)
		HostLocks[i] = i + MCPHostsLock + 1;
	h->txhosts_locks = HostLocks;		

	/* attach to the host's shared memory */
	h->txhosts_hdr = ShmemInitStruct("Hosts HDR", TxHostsFullSize, &found);

	/* It should be already initialized */
	Assert(found);

	/* Make sure we don't have several processes performing the next check
	 * concurrently.
	 */
	LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
	if (h->txhosts_hdr->fid == TXHinit)
	{
		/* MCPHosts is opened first time */
		MCPFileRead(h->txhosts, h->txhosts_hdr, TxHostsFullSize, false);
	}
	LWLockRelease(MCPHostsLock);

	if (h->txhosts_hdr->fid != TXHmagic)
		elog(ERROR, "incorrect .txh file identificator %d, expected %d",
			 h->txhosts_hdr->fid, TXHmagic);

	h->txhosts_tab = (TxHostsRecord *)
		((char *) h->txhosts_hdr + sizeof(TxHostsHeader));
}

/* Close hosts file */
void
MCPHostsClose(MCPHosts *h)
{
	if (h->txhosts)
	{
		if (h->txhosts_hdr)
		{
			MCPFileSeek(h->txhosts, 0, SEEK_SET);
			MCPFileWrite(h->txhosts, h->txhosts_hdr, TxHostsFullSize);
		}
		MCPFileDestroy(h->txhosts);
		pfree(h->txhosts_locks);
	}

	pfree(h);
}

/* Advance slave current transaction recno to the next transaction */
void
MCPHostsNextTx(MCPHosts *h, MCPQueue *q, int hostno, ullong last_recno)
{
	if (hostno >= h->txhosts_hdr->maxhosts)
		elog(ERROR, "hostno %d out of boundary", hostno);

	ASSERT_HOST_LOCK_HELD(h, hostno);

	/*
	 * Make sure we don't have open transaction while switching the current
	 * one.
	 */
	Assert(MCPQueueGetDatafile(q) == NULL);

	if (h->txhosts_tab[hostno].frecno > last_recno)
		return;

	h->txhosts_tab[hostno].frecno++;
	h->txhosts_tab[hostno].timestamp = time(NULL);
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

	/* Looking for min txhosts_tab[i].vrecno  */
	for (i = 0; i < h->txhosts_hdr->maxhosts; i++)
	{
		/* Ignore disconnected slaves */
		if (h->txhosts_tab[i].vrecno == InvalidRecno ||
			node_pid[i + 1] == 0)
			continue;
		if (recno == InvalidRecno)
			recno = h->txhosts_tab[i].vrecno;
		else if (recno > h->txhosts_tab[i].vrecno)
			recno = h->txhosts_tab[i].vrecno;
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
			for (i = 0; i < h->txhosts_hdr->maxhosts; i++) 
			{
				if ((node_pid[i] != 0) && 
					(h->txhosts_tab[i].vrecno != InvalidRecno) &&
					(h->txhosts_tab[i].vrecno < dump_start_recno - 1))
						h->txhosts_tab[i].sync = MCPQUnsynced;
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
	for (; hostno < h->txhosts_hdr->maxhosts; hostno++)
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

	return h->txhosts_tab[hostno].frecno;
}

/* Set first-to-process recno for the slave */
void
MCPHostsSetFirstRecno(MCPHosts *h, int hostno, ullong new_frecno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	h->txhosts_tab[hostno].frecno = new_frecno;
}

/* Return acknowledged recno for the slave */
ullong
MCPHostsGetAckedRecno(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	return h->txhosts_tab[hostno].vrecno;
}

/* Set acknowledged recno for the slave */
void
MCPHostsSetAckedRecno(MCPHosts *h, int hostno, ullong new_vrecno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	h->txhosts_tab[hostno].vrecno = new_vrecno;
}

/* Return the sync status for the slave */
MCPQSync
MCPHostsGetSync(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	return h->txhosts_tab[hostno].sync;
}

/* Set sync status for the given slave */
void
MCPHostsSetSync(MCPHosts *h, int hostno, MCPQSync sync)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);

	elog(DEBUG4, "MCPHostSetSync %s -> %s",
		 MCPQSyncAsString(h->txhosts_tab[hostno].sync),
		 MCPQSyncAsString(sync));
	h->txhosts_tab[hostno].sync = sync;
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
		endpoint = h->txhosts_hdr->maxhosts - 1;
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
			 h->txhosts_tab[hostno].frecno,
			 h->txhosts_tab[hostno].vrecno,
			 MCPQSyncAsString(h->txhosts_tab[hostno].sync));
	}
}

time_t
MCPHostsGetTimestamp(MCPHosts *h, int hostno)
{
	ASSERT_HOST_LOCK_HELD(h, hostno);
	return h->txhosts_tab[hostno].timestamp;
}

int
MCPHostsGetMaxHosts(MCPHosts *h)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return h->txhosts_hdr->maxhosts;
}

void
MCPHostLock(MCPHosts *h, int hostno, LWLockMode mode)
{
	LWLockAcquire(MCPHostsLock, mode);
	LWLockAcquire(h->txhosts_locks[hostno], mode);
	LWLockRelease(MCPHostsLock);
}

void
MCPHostUnlock(MCPHosts *h, int hostno)
{
	LWLockRelease(h->txhosts_locks[hostno]);
}

void
MCPHostsLockAll(MCPHosts *h, LWLockMode mode)
{
	int 	i;

	LWLockAcquire(MCPHostsLock, mode);
	for (i = 0; i < h->txhosts_hdr->maxhosts; i++)
		LWLockAcquire(h->txhosts_locks[i], mode);
}

void
MCPHostsUnlockAll(MCPHosts *h)
{
	int 	i;

	for (i = 0; i < h->txhosts_hdr->maxhosts; i++)
		LWLockRelease(h->txhosts_locks[i]);

	LWLockRelease(MCPHostsLock);
}

/* Set/get encoding attribute in the hosts header */
void
MCPHostsSetEncoding(MCPHosts *h, pg_enc new_encoding)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	h->txhosts_hdr->encoding = new_encoding;
}

pg_enc
MCPHostsGetEncoding(MCPHosts *h)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return h->txhosts_hdr->encoding;
}
