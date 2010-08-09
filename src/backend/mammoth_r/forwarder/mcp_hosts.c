/*-----------------------
 * mcp_hosts.c
 * 		The Mammoth Replication MCP Hosts implementation
 *
 * MCP Hosts is a struct for keeping track of status of slave nodes in the
 * replication forwarder.
 * 
 * Note that there are some strange locking considerations regarding some
 * recnos in each host's record (TxHostRecord).  A host process can read and
 * write its own values with only a shared lock.  If any other process wants to
 * access another host's values, *it needs to grab an exclusive lock*, even if
 * it's to read the values.  This allows better concurrency in the common case
 * where each host is advancing its recno pointers independently, and still be
 * correct in the uncommon case where the master process needs to advance them
 * all in case of receiving a full dump.
 *
 * This applies to McphHostRecnoKindSendNext only.
 *
 * All other members of the MCPHosts struct use locks normally.
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
	ullong		recnos[McphHostRecnoKindMax];
	time_t      timestamp;		/* timestamp of last get operation */
	MCPQSync    sync;			/* synchronized status */
	uint32		flags;			/* per slave flags */
} TxHostsRecord;

/* typedef appears in mcp_hosts.h */
struct MCPHosts
{
	int				h_fid;			/* id of hosts file */
#define TXHmagic 0x54584900
#define TXHinit  0x494E4954
	int         	h_maxhosts;		/* maximum hosts number which want to
									   retrieve data from MCP queue */
	pg_enc			h_encoding;		/* encoding of the MCP queue data */
	ullong			h_recnos[McphRecnoKindMax];
	TxHostsRecord	h_hosts[MCP_MAX_SLAVES];
};

#define MCPHOSTS_DISKSZ	(sizeof(MCPHosts))

#define ASSERT_HOST_LOCK_HELD(_h_, _hostno_) \
	Assert(LWLockHeldByMe(MCPHostsLock))

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
			elog(ERROR, "incorrect .txh file identificator %x, expected %x",
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
		MemSet(hosts, 0, sizeof(MCPHosts));
		hosts->h_fid = TXHinit;
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

/*
 * Advance slave current transaction recno to the next transaction.  last_recno
 * is the caller-specified end-of-queue; if we're past that, don't change our
 * value.  Return our next value to send.
 *
 * Note locking considerations, in the comment at the top of the file.
 */
ullong
MCPHostsNextTx(MCPHosts *h, int hostno, ullong last_recno)
{
	if (hostno >= h->h_maxhosts)
		elog(ERROR, "hostno %d out of boundary", hostno);

	ASSERT_HOST_LOCK_HELD(h, hostno);

	/* don't increment SendNext if we're past the given last_recno */
	if (h->h_hosts[hostno].recnos[McphHostRecnoKindSendNext] > last_recno)
		return h->h_hosts[hostno].recnos[McphHostRecnoKindSendNext];

	h->h_hosts[hostno].timestamp = time(NULL);
	return ++h->h_hosts[hostno].recnos[McphHostRecnoKindSendNext];
}

/* 
 * Get the minimum of acknowledged records among the connected
 * hosts. Any record number lower than this minimum corresponds
 * to a no longer needed queue transaction.
 */ 
ullong
MCPHostsGetMinAckedRecno(MCPHosts *h, pid_t *node_pid)
{
	ullong		newvalue = InvalidRecno;
	int			i;

	Assert(LWLockHeldByMe(MCPHostsLock));

	/* Looking for the minimum acked recno across all connected slaves */
	for (i = 0; i < h->h_maxhosts; i++)
	{
		ullong	thishost;

		/* Ignore disconnected slaves */
		if (node_pid[i + 1] == 0)
			continue;

		thishost = h->h_hosts[i].recnos[McphHostRecnoKindAcked];
		/* if a connected slave hasn't set Acked, can't prune anything */
		if (thishost == InvalidRecno)
			return InvalidRecno;
		if (newvalue == InvalidRecno || newvalue > thishost)
			newvalue = thishost;
	}

	return newvalue;
}

/*
 * Get and set routines for record numbers stored in the MCPHosts header.
 * Note locking considerations, in the comment at the top of the file.
 */
void
MCPHostsSetRecno(MCPHosts *h, McphRecnoKind kind, ullong recno)
{
	Assert(LWLockHeldByMe(MCPHostsLock));

	h->h_recnos[kind] = recno;
}

ullong
MCPHostsGetRecno(MCPHosts *h, McphRecnoKind kind)
{
	Assert(LWLockHeldByMe(MCPHostsLock));

	return h->h_recnos[kind];
}
void
MCPHostsSetHostRecno(MCPHosts *h, McphHostRecnoKind kind, int host, ullong recno)
{
	Assert(LWLockHeldByMe(MCPHostsLock));

	h->h_hosts[host].recnos[kind] = recno;
}


ullong
MCPHostsGetHostRecno(MCPHosts *h, McphHostRecnoKind kind, int host)
{
	Assert(LWLockHeldByMe(MCPHostsLock));

	return h->h_hosts[host].recnos[kind];
}

/* 
 * MCPHostsCleanup
 *
 * This is called when the master process gets a TRUNC message from the
 * master, which means that the master is sending a new full dump.  What
 * we need to do here is make sure that all slaves are now set to read
 * from the start of that dump, instead of their natural progression of
 * reading the messages they are currently pointing to.
 */
void
MCPHostsCleanup(MCPHosts *h, MCPQueue *q, ullong recno)
{
	int		hostno;

	LWLockAcquire(MCPHostsLock, LW_EXCLUSIVE);
	for (hostno = 0; hostno < h->h_maxhosts; hostno++)
	{
		if (MCPHostsGetHostRecno(h, McphHostRecnoKindSendNext, hostno) < recno)
			MCPHostsSetHostRecno(h, McphHostRecnoKindSendNext, hostno, recno);
	}
	LWLockRelease(MCPHostsLock);
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
			 h->h_hosts[hostno].recnos[McphHostRecnoKindSendNext],
			 h->h_hosts[hostno].recnos[McphHostRecnoKindAcked],
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

uint32
MCPHostsSetFlags(MCPHosts *h, int hostno, uint32 flags) 
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return (h->h_hosts[hostno].flags |= flags);
}

uint32
MCPHostsGetFlags(MCPHosts *h, int hostno)
{
	Assert(LWLockHeldByMe(MCPHostsLock));
	return h->h_hosts[hostno].flags;
}

