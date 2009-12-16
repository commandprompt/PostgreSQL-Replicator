/*-----------------------
 * mcp_queue.c
 * 		The Mammoth Replication MCP Queue implementation
 *
 * This code is all about creating, opening, closing, and otherwise
 * manipulating MCP queues, including dumping data on them and extracting it.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_queue.c 2186 2009-06-25 12:14:51Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "mammoth_r/forwarder.h"
#include "mammoth_r/mcp_api.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/paths.h"
#include "mammoth_r/txlog.h"
#include "miscadmin.h"
#include "postmaster/replication.h"
#include "storage/backendid.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"


/*
 * We use record numbers (recnos) to control the queue bounds and to track
 * sent, received, restored and confirmed data.
 *
 * The meaning of individual recnos in the MCPQueue struct is as follows:
 *
 * brecno - recno of the begining of the queue. No data
 * 			can be inserted before the brecno. Offsets in the queue file
 *			are calculated as (recno - brecno) * TxQueueRecordSize.
 *			brecno is altered in queue optimization routines, like
 *			MCPQueueVacuum.
 *
 * lrecno - the last recno in the queue. All new data are appended
 *			at lrecno + 1 and lrecno would increase after this.
 *
 * vrecno - the highest record number corresponding acknowledged (verified)
 *			data. It is used by the PostgreSQL data senders (MQP or MCP slave
 *			connection handlers) to track the last acknowldeged by the receivers
 *			(MCP master handler or SQP) data. We use it with SQP to mark the 
 *			last record of the latest transaction we receive completely from
 *			MCP (slave doesn't try to restore a transaction until it receives 
 *			TXEND message for it).
 *
 * frecno - this is a first to process record number. It has different meaning
 *			depending on where it is used:
 *
 *	master:	frecno is the number of the first record waiting to be put
 *			to the queue buffer and to be sent to MCP. It is increased
 *			while sending data to MCP. If frecno is greater then lrecno then
 *			all the data from the master queue are sent to MCP. It shouldn't
 *			be greater then lrecno + 1.
 *	slave:	frecno is the number of the first record waiting to be restored
 *			(transfered from the queue to the PostgreSQL database).
 *			frecno - 1 is the last recno which was restored. If frecno is
 *			greater then lrecno then all the data are restored from the slave's
 *			queue. It shouldn't be greater then lrecno +1.
 *	MCP:	MCP slave connection handler uses frecno in MCPHostRecord
 *			for the same reason as master.
 *
 * system_identifier - in the case of a master process in the MCP server,
 *			this corresponds to the master's system identifier.  Useful
 *			for determining that a different master connects.
 *
 * NOTICE:	MQP is the master queue process, the one which is implemented by
 *			master_mcp_agent.c, SQP is the slave queue process (actually this
 *			backend processes data restoring stuff too), implemented in
 *			slave_mcp_agent.c.
 */
typedef struct
{
	int32		fid;			/* id of txdata file */
#define TXQmagic 0x54585100
	ullong      brecno;			/* base record number */
	ullong      frecno;			/* current record number */
	ullong      lrecno;			/* last record number */
	ullong      vrecno;			/* last record number acknowledged by peer */
	bool		empty;			/* queue emptiness indicator */
	MCPQSync	sync;			/* synchronization flag */
	uint64		system_identifier; /* peer sysid */
	time_t 		enqueue_timestamp; /* time of the latest enqueue operation */
	time_t  	dequeue_timestamp; /* time of the latest dequeue operation */
	char        end;
} TxQueueHeader;


/* typedef appears in mcp_queue.h */
struct MCPQueue
{
	MCPFile	   *txhdr;			/* a file containing queue header */
	MCPFile	   *txdata;			/* currently open xact data file, or NULL */
	TxQueueHeader *txqueue_hdr;	/* queue header data in shared memory */
	const char *name;		/* this queue's name (for debugging purposes) */
	const char *dir;		/* this queue's dir name */
	LWLockId	lock;			/* this queue's lock */
};

static MCPQueue BackendMCPQueueData =
{
	NULL,
	NULL,
	NULL,
	"Backend replication queue",
	MAMMOTH_BACKEND_DIR "/queue",
	BackendQueueLock
};

static MCPQueue ForwarderMCPQueueData =
{
	NULL,
	NULL,
	NULL,
	"Forwarder replication queue",
	MAMMOTH_FORWARDER_DIR "/queue",
	ForwarderQueueLock
};


static void MCPQueueSetEmpty(MCPQueue *q, bool empty);
static void MCPQueueTruncate(MCPQueue *q, ullong new_brecno);

/*
 * return a palloc'd filename from the given format and va_args.
 */
static char *
get_allocated_filename(char *format, ...)
{
	int			len = 8;
	char	   *filename;

	for (;;)
	{
		va_list	ap;
		int		ret;

		filename = palloc(len);
		va_start(ap, format);
		ret = vsnprintf(filename, len, format, ap);
		va_end(ap);

		if (ret >= len)
		{
			len = ret + 1;
			pfree(filename);
			continue;
		}
		if (ret == -1)
		{
			len *= 2;
			pfree(filename);
			continue;
		}
		break;
	}

	return filename;
}

/*
 * queue setup helper: create the MCPFile object to the queue header
 */
static void
mcpq_setup_hdr(MCPQueue *q)
{
	char	   *filename;

	Assert(q->txhdr == NULL);

   	filename = get_allocated_filename("%s/queue.hdr", q->dir);
	q->txhdr = MCPFileCreate(filename);
	pfree(filename);
}

/*
 * shmem init helper: setup the shared memory area and read the queue header
 * onto it.
 */
static void
mcpq_shmeminit(MCPQueue *queue)
{
	bool			found;
	MemoryContext	oldcxt;

	/*
	 * FIXME -- storing this stuff in TopMemoryContext is probably not
	 * ideal; this should be fixed sometime.
	 */
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	queue->txhdr = NULL;
	mcpq_setup_hdr(queue);

	queue->txqueue_hdr = ShmemInitStruct(queue->name,
						  				 sizeof(TxQueueHeader), &found);
	/*
	 * note: we used to memset this to zero, but since we're about to read
	 * the header file, it seems quite pointless.
	 */

	/* Read the header into shared memory */
	MCPFileOpen(queue->txhdr);
	MCPFileSeek(queue->txhdr, 0, SEEK_SET);
	MCPFileRead(queue->txhdr, queue->txqueue_hdr, sizeof(TxQueueHeader), false);

	/* verify the "signature" bytes */
	if (queue->txqueue_hdr->fid != TXQmagic)
		elog(FATAL, "incorrect .txq file identificator, read %03x expected %03x",
			 queue->txqueue_hdr->fid, TXQmagic);

	queue->txdata = NULL;

	MemoryContextSwitchTo(oldcxt);
}

/* 
 * Initialize shared memory area for queue headers.
 */
void
MCPQueueShmemInit(void)
{
	if (replication_enable)
		mcpq_shmeminit(&BackendMCPQueueData);
	if (ForwarderEnable)
		mcpq_shmeminit(&ForwarderMCPQueueData);
}

Size
MCPQueueShmemSize(void)
{
	Size	sz = 0;

	if (replication_enable)
		sz += sizeof(TxQueueHeader);
	if (ForwarderEnable)
		sz += sizeof(TxQueueHeader);

	return sz;
}

/* 
 * MCPQueueInit
 * 		Initialize a MCP queue for this process.
 *
 * Can be either a "forwarder" queue or a normal backend queue.
 */
MCPQueue *
MCPQueueInit(bool forwarder)
{
	return forwarder ? &ForwarderMCPQueueData : &BackendMCPQueueData;
}

/*
 * Helper for BootStrapMCPQueue
 */
static void
mcpq_bootstrap(MCPQueue *q)
{
	TxQueueHeader	hdr;

	mcpq_setup_hdr(q);

	if (!MCPFileCreateFile(q->txhdr))
		ereport(ERROR,
				(errmsg("could not create queue header file \"%s\": %m",
						MCPFileGetPath(q->txhdr))));

	/* Initial (empty) queue */
	hdr.fid = TXQmagic;
	hdr.frecno = 1;
	hdr.brecno = 1;
	hdr.lrecno = InvalidRecno;
	hdr.vrecno = InvalidRecno;
	hdr.empty = true;
	hdr.system_identifier = 0;
	hdr.enqueue_timestamp = (time_t) 0;
	hdr.dequeue_timestamp = (time_t) 0;

	MCPFileWrite(q->txhdr, &hdr, sizeof(TxQueueHeader));
}

/*
 * BootStrapMCPQueue
 * 		Create the necessary files for MCPQueue to begin operating.
 *
 * This must be called once in the life of a Replicator installation.
 */
void
BootStrapMCPQueue(void)
{
	mcpq_bootstrap(&BackendMCPQueueData);
	mcpq_bootstrap(&ForwarderMCPQueueData);
}

/*
 * MCPQueueFlush
 * 		Flush the queue header to disk.
 */
static void
MCPQueueFlush(MCPQueue *q)
{
	if (!q->txqueue_hdr)
		return;
	if (!q->txhdr)
		return;

	MCPFileSeek(q->txhdr, 0, SEEK_SET);
	MCPFileWrite(q->txhdr, q->txqueue_hdr, sizeof(TxQueueHeader));
	MCPFileSync(q->txhdr);
}

/*
 * MCPQueueDestroy
 *		Write the queue header from shmem area into the queue header file
 */
void
MCPQueueDestroy(MCPQueue *q)
{
	MCPQueueFlush(q);
	MCPFileDestroy(q->txhdr);
	q->txhdr = NULL;
	if (q->txdata != NULL)
	{
		elog(WARNING,
			 "queue transaction file was not closed before closing the queue");
		MCPFileDestroy(q->txdata);
		q->txdata = NULL;
	}
}

/*
 * MCPQDatafileName
 *
 * Returns the palloc'ed pathname of the data file corresponding to the given
 * recno.
 */
static char *
MCPQDatafileName(MCPQueue *q, ullong recno)
{
	return get_allocated_filename("%s/%016lX.txd", q->dir, recno);
}

/*
 * MCPQueueGetLocalFilename
 *
 * Returns the palloc'd pathname of the data file for the local queue file.
 *
 * This is here because the MCPQueue->dir is used to construct the filename.
 */
char *
MCPQueueGetLocalFilename(MCPQueue *q, char *base)
{
	/*
	 * The queue name must be unique for each backend -- use our BackendId
	 * for that.
	 */
	return get_allocated_filename("%s/%s_%04X.txl", q->dir, base, MyBackendId);
}

/*
 * Write data to the transaction data file, which should have been opened by
 * MCPQueueTxOpen.
 */
void
MCPQueuePutData(MCPQueue *q, void *data, ssize_t datalen)
{
	/* Append message data to the queue data file */
	if (datalen > 0)
		MCPFileWrite(q->txdata, data, datalen);
}

/*
 * MCPQueueReadDataHeader
 *		Read the data header from the currently open transaction data file
 */
void
MCPQueueReadDataHeader(MCPQueue *q, TxDataHeader *hdr)
{
	Assert(hdr != NULL);

	MCPFileSeek(q->txdata, 0, SEEK_SET);
	MCPFileRead(q->txdata, hdr, sizeof(TxDataHeader), false);

	/* Check the data header ID */
	if (hdr->dh_id != DATA_HEADER_ID)
		ereport(ERROR, 
				(errmsg("data header is corrupted"),
				 errdetail("Read 0x%x, expected 0x%x.", hdr->dh_id,
						   DATA_HEADER_ID),
				 errcontext("Reading \"%s\".", MCPFileGetPath(q->txdata))));
}

/*
 * MCPQueueReadData
 * 		Read the given number of bytes from the currently open transaction data
 * 		file
 *
 * Returns true if there is data to return, false if EOF and eof_allowed.
 * (If eof_allowed is false, MCPFileRead throws error).
 */
bool
MCPQueueReadData(MCPQueue *q, void *buf, ssize_t len, bool eof_allowed)
{
	return MCPFileRead(q->txdata, buf, len, eof_allowed);
}

/*
 * MCPQueueNextTx
 * 		Advance the "first recno" pointer of this queue if there is a
 * 		transaction available to be read.
 */
void
MCPQueueNextTx(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));

	/* Make sure there's no open transaction */
	Assert(q->txdata == NULL);

	if (q->txqueue_hdr->frecno > q->txqueue_hdr->lrecno)
		return;

	q->txqueue_hdr->frecno++;
}

/*
 * MCPQueueTxOpen
 * 		Open the transaction file corresponding to the passed recno.
 *
 * If the data file doesn't exist, create it; otherwise just open it.
 */
void
MCPQueueTxOpen(MCPQueue *q, ullong recno)
{
	char   *filename;

	/*
	 * Make sure we are not opening a transaction in the middle of an already
	 * running one.
	 */
	if (q->txdata)
		ereport(ERROR,
				(errmsg("a transaction has already been queued: \"%s\"",
						MCPFileGetPath(q->txdata))));

	filename = MCPQDatafileName(q, recno);
	q->txdata = MCPFileCreate(filename);
	pfree(filename);
	MCPFileCreateFile(q->txdata);
}

/*
 * MCPQueueTxClose
 * 		Close the currently open transaction data file.
 */
void
MCPQueueTxClose(MCPQueue *q)
{
	MCPFileDestroy(q->txdata);
	q->txdata = NULL;
}

/* 
 * MCPQueueTxCommit
 *		Mark the transaction as committed.
 *
 * We only need to clear the empty queue state here
 */
void
MCPQueueTxCommit(MCPQueue *q, ullong recno)
{
	Assert(LWLockHeldByMe(q->lock));
	Assert(recno != InvalidRecno);

	 /* clear the empty flag from transaction, if set */
	if (MCPQueueIsEmpty(q))
		MCPQueueSetEmpty(q, false);
}

/*
 * Converts MCPFile to the transaction with a given number in the queue.  Use
 * recno = InvalidRecno to make the queue assign the next unused recno.
 */
ullong
MCPQueueCommit(MCPQueue *q, MCPFile *txdata, ullong recno)
{
	char   *new_name;

	Assert(!LWLockHeldByMe(q->lock));

	if (recno == InvalidRecno)
	{
		/* Use next unassigned recno for the next transaction */
		LockReplicationQueue(q, LW_EXCLUSIVE);

		/* If the queue is empty we should setup a queue tail recno first */
		if (MCPQueueIsEmpty(q))
		{
			recno = MCPQueueGetInitialRecno(q);
			MCPQueueSetLastRecno(q, recno);
			MCPQueueSetEmpty(q, false);
		}
		else
		{
			recno = MCPQueueGetLastRecno(q);
			MCPQueueSetLastRecno(q, ++recno);
		}
		MCPQueueSetEnqueueTimestamp(q);
		
		UnlockReplicationQueue(q);
	}
	else
	{
		/*
		 * If we were passed a valid recno, make sure the queue is prepared to
		 * receive it.
		 */
		if (!MCPQueueIsEmpty(q))
			elog(PANIC, "non-invalid recno passed to empty queue");
	}

	new_name = MCPQDatafileName(q, recno);
	MCPFileRename(txdata, new_name);
	pfree(new_name);

	/* fsync the data file so that it persists on crash */
	MCPFileSync(txdata);

	elog(DEBUG4, "Transaction is placed in queue, recno "UNI_LLU, recno);
	return recno;
}

MCPFile *
MCPQueueGetDatafile(MCPQueue *q)
{
	return q->txdata;
}

/* Calculate total size of transaction data laying before the recno. */
ssize_t
MCPQueueCalculateSize(MCPQueue *q, ullong recno)
{
	ssize_t		total = 0;

	/* We need a lock here since we rely on brecno */
	Assert(LWLockHeldByMe(q->lock));

	if (recno > q->txqueue_hdr->brecno)
	{
		char	   *filename;
		struct stat	st;
		int			ret;
		ullong 		i = q->txqueue_hdr->brecno;
		ullong 		max_recno = Min(recno, q->txqueue_hdr->lrecno);

		for (; i < max_recno; i++)
		{
			/* Skip not yet committed transactions */
			if (!TXLOGIsCommitted(i))
				continue;

			filename = MCPQDatafileName(q, i);
			ret = stat(filename, &st);
			if (ret < 0)
				elog(ERROR, "could not stat %s: %m", filename);
			total += st.st_size;
			pfree(filename);
		}
	}
	return total;
}

/* MCPQueuePrune
 *
 * Free the queue space by deleting already confirmed records data.
 *
 * It just detects the safe point for truncating the queue and calls
 * MCPQueueTruncate.
 */
void
MCPQueuePrune(MCPQueue *q)
{
	ullong vacrecno;

	Assert(LWLockHeldByMe(q->lock));

	/*
	 * Pruning condition: all records from the queue should be already
	 * confirmed.
	 */
	vacrecno = Min(q->txqueue_hdr->vrecno, q->txqueue_hdr->frecno - 1);

	/* no new confirmed records, bail out */
	if (vacrecno + 1 <= q->txqueue_hdr->brecno || vacrecno == InvalidRecno)
		return;

	/* Remove obsolete records. */
	MCPQueueTruncate(q, vacrecno + 1);
}


/*
 * Remove data transactions and TXLOG segments up to the given transaction.  A
 * transaction with the given number will become the initial queueue tx.
 */
static void
MCPQueueTruncate(MCPQueue *q, ullong new_brecno)
{
	ullong	recno;

	Assert(LWLockHeldByMe(q->lock));
	elog(DEBUG4, "queue truncate called with recno "UNI_LLU, new_brecno);

	if (MCPQueueIsEmpty(q))
	{
		ereport(DEBUG4,
				(errmsg("truncate cancelled"),
				 errdetail("Queue is empty.")));
		return;
	}

	/* Perform truncate only if data to be truncated is actually in queue */
	if (q->txqueue_hdr->brecno > new_brecno ||
		q->txqueue_hdr->lrecno + 1 < new_brecno)
	{
		ereport(ERROR, 
			 (errmsg("truncation point of out queue bounds"),
			  errdetail("Queue start: "UNI_LLU" queue end: "UNI_LLU
						" truncate point: "UNI_LLU,
						q->txqueue_hdr->brecno, q->txqueue_hdr->lrecno, new_brecno)));
	}

	/* Remove obsolete TXLOG segments. Only remove existing segments. */
	TXLOGTruncate(Min(MCPQueueGetLastRecno(q), new_brecno));

	/* Remove all data files before the cutoff transaction */
	for (recno = q->txqueue_hdr->brecno; recno < new_brecno; recno++)
	{
		char	*filename = MCPQDatafileName(q, recno);

		if (unlink(filename) < 0)
			ereport(WARNING,
					(errmsg("could not unlink %s: %m", filename)));
		pfree(filename);
	}

	/* Set a new initial recno in a queue header */
	q->txqueue_hdr->brecno = new_brecno;

	/* Pull 'next to process' record numbers if it is out of the queue */
	if (q->txqueue_hdr->frecno < new_brecno)
		q->txqueue_hdr->frecno = new_brecno;

	/* Mark last recno as invalid if the queue becomes empty */
	if (MCPQueueGetLastRecno(q) < new_brecno)
		MCPQueueSetEmpty(q, true);
}

/*
 * Remove all data from the queue and set a new queue beginning.
 *
 * Caller must hold the queue lock.
 */
void
MCPQueueCleanup(MCPQueue *q, ullong new_brecno)
{
	Assert(LWLockHeldByMe(q->lock));

	/* Remove all data from the queue */
	MCPQueueTruncate(q, MCPQueueGetLastRecno(q) + 1);

	/* Set up a new TXLOG page for the queue begin */
	TXLOGZeroPageByRecno(new_brecno);

	/* lrecno is set to invalid in MCPQueueTruncate */
	MCPQueueSetInitialRecno(q, new_brecno);
	MCPQueueSetFirstRecno(q, new_brecno);
	MCPQueueSetAckRecno(q, InvalidRecno);
}

ullong
MCPQueueGetFirstRecno(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));

	/* For a fresh queue it should be started from initial queue recno */
	if (q->txqueue_hdr->frecno == InvalidRecno)
		q->txqueue_hdr->frecno = q->txqueue_hdr->brecno;

	return q->txqueue_hdr->frecno;
}

void
MCPQueueSetFirstRecno(MCPQueue *q, ullong recno)
{
	Assert(LWLockHeldByMe(q->lock));

	q->txqueue_hdr->frecno = recno;
}

ullong
MCPQueueGetLastRecno(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));

	return q->txqueue_hdr->lrecno;
}

void
MCPQueueSetLastRecno(MCPQueue *q, ullong lrecno)
{
	Assert(LWLockHeldByMe(q->lock));

	q->txqueue_hdr->lrecno = lrecno;
}

ullong
MCPQueueGetAckRecno(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	return q->txqueue_hdr->vrecno;
}

void
MCPQueueSetAckRecno(MCPQueue *q, ullong vrecno)
{
	Assert(LWLockHeldByMe(q->lock));
	q->txqueue_hdr->vrecno = vrecno;
}

ullong
MCPQueueGetInitialRecno(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	return q->txqueue_hdr->brecno;
}

void
MCPQueueSetInitialRecno(MCPQueue *q, ullong brecno)
{
	Assert(LWLockHeldByMe(q->lock));
	q->txqueue_hdr->brecno = brecno;
}

MCPQSync
MCPQueueGetSync(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));

	return q->txqueue_hdr->sync;
}

void
MCPQueueSetSync(MCPQueue *q, MCPQSync sync)
{
	Assert(LWLockHeldByMe(q->lock));

	elog(DEBUG4, "MCPQueueSetSync %s -> %s",
		 MCPQSyncAsString(q->txqueue_hdr->sync),
		 MCPQSyncAsString(sync));

	q->txqueue_hdr->sync = sync;
}

bool
MCPQueueIsEmpty(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	return q->txqueue_hdr->empty;
}

static void
MCPQueueSetEmpty(MCPQueue *q, bool empty)
{
	Assert(LWLockHeldByMe(q->lock));
	elog(DEBUG4, "Setting queue empty state: %s", empty ? "true" : "false");
	q->txqueue_hdr->empty = empty;
}

/*
 * Report the status of the queue header to the server log, at the specified
 * log level
 */
void
MCPQueueLogHdrStatus(int elevel, MCPQueue *queue, char *prefix)
{
	elog(elevel, "%s: recnos l="UNI_LLU" f="UNI_LLU" v="UNI_LLU" b="UNI_LLU" sync = %s",
		 prefix,
		 queue->txqueue_hdr->lrecno,
		 queue->txqueue_hdr->frecno,
		 queue->txqueue_hdr->vrecno,
		 queue->txqueue_hdr->brecno,
		 MCPQSyncAsString(queue->txqueue_hdr->sync));
}

/* Returns a timestamp of the latest queue 'write' operation */
time_t
MCPQueueGetEnqueueTimestamp(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	return q->txqueue_hdr->enqueue_timestamp;
}

/* Set the queue timestamp field */
void
MCPQueueSetEnqueueTimestamp(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	q->txqueue_hdr->enqueue_timestamp = time(NULL);
}

time_t
MCPQueueGetDequeueTimestamp(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	return q->txqueue_hdr->dequeue_timestamp;
}

/* Set the queue timestamp field */
void
MCPQueueSetDequeueTimestamp(MCPQueue *q)
{
	Assert(LWLockHeldByMe(q->lock));
	q->txqueue_hdr->dequeue_timestamp = time(NULL);
}

void
LockReplicationQueue(MCPQueue *q, LWLockMode mode)
{
	LWLockAcquire(q->lock, mode);
}

void
UnlockReplicationQueue(MCPQueue *q)
{
	LWLockRelease(q->lock);
}

bool
QueueLockHeldByMe(MCPQueue *q)
{
	return LWLockHeldByMe(q->lock);
}
