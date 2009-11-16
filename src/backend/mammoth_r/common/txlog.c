/*
 * txlog.c
 * 		Mammoth Replication transactional log SLRU implementation.
 *
 * Each replicated transaction is represented by one bit in SLRU, 0 for
 * an incomplete or not-yet-happened transaction, and 1 for transactions that
 * are complete and ready to be replicated.
 *
 * Note that there are two TXLOG areas, one for the forwarder subprocesses and
 * a separate one for master/slave (client) replication processes.  Both are
 * initialized at startup, and the one to use is chosen during the initial
 * process setup.
 *
 * This module implementation is loosely based on clog.c.
 *
 * XXX: Currently unsigned long long is used to mark a record number
 * corresponding to individual transaction.  Even when we have 2^16
 * transactions per page it still requires 2^64/2^16 pages to represent the
 * whole 64 bit range of transaction numbers, which is usually bigger than int
 * used to represent a page number in the simple LRU code. From the other POV,
 * 2^31*2^16 of records is quite a large number, which is unlikely to be
 * reached.  The worst case is the case when the platform doesn't support
 * 64-bit integers, so int32 is used for unsigned long long, which makes us
 * think about preventing errors due to the recno wraparound. 
 *
 * $Id: txlog.c 2093 2009-04-09 19:05:33Z alvherre $
 */
#include "postgres.h"

#include "access/slru.h"
#include "access/transam.h"
#include "mammoth_r/paths.h"
#include "mammoth_r/txlog.h"


/* Defines for TXLOG page sizes. A page size is the same as BLCKSZ */
#define TXLOG_BITS_PER_XACT		1
#define TXLOG_XACTS_PER_BYTE	8
#define TXLOG_XACTS_PER_PAGE	(BLCKSZ * TXLOG_XACTS_PER_BYTE)
#define TXLOG_XACT_BITMASK		((1 << TXLOG_BITS_PER_XACT) - 1)

#define INVALID_PAGE_NUMBER		-1

#define RecnoToPage(recno)		((recno) / (ullong) TXLOG_XACTS_PER_PAGE)
#define RecnoToPgIndex(recno)	((recno) % (ullong) TXLOG_XACTS_PER_PAGE)
#define RecnoToByte(recno)		(RecnoToPgIndex(recno) / TXLOG_XACTS_PER_BYTE)
#define RecnoToBIndex(recno)	(recno % (ullong) TXLOG_XACTS_PER_BYTE)

#define REPL_TX_STATE_EMPTY 0x00
#define REPL_TX_STATE_COMMITTED 0x01

/* paths to our SLRU areas */
#define BACKEND_TXLOG_DIR		MAMMOTH_BACKEND_DIR"/txlog"
#define FORWARDER_TXLOG_DIR		MAMMOTH_FORWARDER_DIR"/txlog"

typedef struct TxlogCtlData
{
	SlruCtlData *slruCtl;
	char	   *const name;
	char	   *const dir;
	LWLockId	lock;
} TxlogCtlData;

/* Shared memory TXLOG structures */
static SlruCtlData	BackendTxlogSlruData;
static SlruCtlData 	ForwarderTxlogSlruData;

/* TXLOG related functions */
static void WriteZeroPageXlogRecord(int page);
static void WriteTruncateXlogRecord(int cutoffpage);
static void	WriteCommitXlogRecord(ullong recno);

static const TxlogCtlData BackendTxlogCtlData =
{
	&BackendTxlogSlruData,
	"Backend Txlog Ctl",
	BACKEND_TXLOG_DIR,
	BackendTxlogControlLock
};

static const TxlogCtlData ForwarderTxlogCtlData =
{
	&ForwarderTxlogSlruData,
	"Forwarder Txlog Ctl",
	FORWARDER_TXLOG_DIR,
	ForwarderTxlogControlLock
};

#define BackendTxlogCtl (&BackendTxlogCtlData)
#define ForwarderTxlogCtl (&ForwarderTxlogCtlData)

/*
 * Only one TXLOG can be in use active any one process.  We initialize this to
 * NULL and have interested processes select at runtime the one to use.
 */
static const TxlogCtlData		*activeTxlogCtl = NULL;

typedef struct TxlogRedoData
{
	bool 	isforwarder;
	int		pageno;
} TxlogRedoData;

typedef TxlogRedoData *TxlogRedo;

typedef struct TxlogRedoCommitData
{
	bool	isforwarder;
	ullong	recno;
} TxlogRedoCommitData;

typedef TxlogRedoCommitData *TxlogRedoCommit;

static bool TXLOGPagePrecedes(int page1, int page2);
static void TXLOGSetStatus(ullong recno, bool committed);
static void TXLOGExtend(const TxlogCtlData *txlog, ullong freshRecno, bool force);


/* Shared memory TXLOG initialization routines */
Size
TXLOGShmemSize(void)
{
	/*
	 * Note that we always reserve space here, even when the features are
	 * turned off.  This could be improved ...
	 */
	return SimpleLruShmemSize(NUM_TXLOG_BUFFERS, 0) * 2;
}

static void
txlog_shmem_init(const TxlogCtlData *ctl)
{
	ctl->slruCtl->PagePrecedes = TXLOGPagePrecedes;

	SimpleLruInit(ctl->slruCtl, ctl->name,
				  NUM_TXLOG_BUFFERS, 0, ctl->lock,
				  ctl->dir);

	/* Set 'not started up' indicator */
	ctl->slruCtl->shared->latest_page_number = INVALID_PAGE_NUMBER;
}

/*
 * TXLOGShmemInit
 *		Initialize shared memory for TXLOG.
 *
 * We init both our TXLOG structs here.
 */
void
TXLOGShmemInit(void)
{
	txlog_shmem_init(ForwarderTxlogCtl);
	txlog_shmem_init(BackendTxlogCtl);
}

/*
 * SelectactiveTxlog
 * 		Choose a TXLOG for the lifetime of this process.
 */
void
SelectActiveTxlog(bool forwarder)
{
	Assert(activeTxlogCtl == NULL);
	activeTxlogCtl = forwarder ? ForwarderTxlogCtl : BackendTxlogCtl;
}

bool
TXLOGIsForwarder(void)
{
	Assert(activeTxlogCtl != NULL);
	return (activeTxlogCtl == ForwarderTxlogCtl);
}

static void
txlog_create(const TxlogCtlData *ctl)
{
	int		slotno;

	LWLockAcquire(ctl->lock, LW_EXCLUSIVE);

	/* Zero the initial TXLOG page */
	slotno = SimpleLruZeroPage(ctl->slruCtl, 0);

	/* Make sure it is written out */
	SimpleLruWritePage(ctl->slruCtl, slotno, NULL);
	Assert(!ctl->slruCtl->shared->page_dirty[slotno]);

	LWLockRelease(ctl->lock);
}

/*
 * BootstrapTXLOG
 * 		Initial TXLOG segment creation
 *
 * This function should be called once in the life of an installation, to
 * create the initial TXLOG files.
 */
void
BootstrapTXLOG(void)
{
	txlog_create(ForwarderTxlogCtl);
	txlog_create(BackendTxlogCtl);
}

/* Startup function */
void
TXLOGStartup(ullong recno)
{
	int		pageno;

	/* XXX: assume that frecno is not zero */
   	pageno = RecnoToPage(recno);

	LWLockAcquire(activeTxlogCtl->lock, LW_EXCLUSIVE);

	/* Was the startup already performed ? */
	if (activeTxlogCtl->slruCtl->shared->latest_page_number == INVALID_PAGE_NUMBER)
		activeTxlogCtl->slruCtl->shared->latest_page_number = pageno;	

	LWLockRelease(activeTxlogCtl->lock);
}

/*
 * CheckPointTXLOG
 * 		Flush dirty pages from TXLOG structs
 *
 * Note we flush both forwarder and backend TXLOG here.  It's not worth
 * "optimizing", because if one of them is not being used, flushing will be
 * a noop anyway.
 */
void
CheckPointTXLOG(void)
{
	SimpleLruFlush(ForwarderTxlogCtlData.slruCtl, false);
	SimpleLruFlush(BackendTxlogCtlData.slruCtl, false);
}

/* Define which TXLOG page is older for truncation purposes */
static bool
TXLOGPagePrecedes(int page1, int page2)
{
	return (page1 < page2);
}

static void
TXLOGSetStatus(ullong recno, bool committed)
{
	int		pageno,
			slotno,
			byteno,
			bshift;
	char   *byteptr;
	SlruCtlData *slru = activeTxlogCtl->slruCtl;

	pageno = RecnoToPage(recno);
	byteno = RecnoToByte(recno);
	bshift = RecnoToBIndex(recno) * TXLOG_BITS_PER_XACT;

	/* Make a new txlog segment if needed */
	TXLOGExtend(activeTxlogCtl, recno, false);

	LWLockAcquire(activeTxlogCtl->lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(slru, pageno, false, InvalidTransactionId);
	byteptr = slru->shared->page_buffer[slotno] + byteno;

	/* XXX: The following assumes we use only 1 bit per transaction. */
	if (committed)	
	{
		/* WAL-log the commit. XXX: is it ok to hold txlog->lock here ? */
		WriteCommitXlogRecord(recno);
		
		/* Transaction should not be marked as committed yet */
		Assert(((*byteptr >> bshift) & TXLOG_XACT_BITMASK) == REPL_TX_STATE_EMPTY);		
		
		/* XXX: This assumes we use only 1 bit per transaction. */
		*byteptr |= (1 << bshift);
	}
	else
		*byteptr &= ~(1 << bshift);
	
	slru->shared->page_dirty[slotno] = true;
	
	LWLockRelease(activeTxlogCtl->lock);
}

/* Set committed status for a transaction identified by the recno */
void
TXLOGSetCommitted(ullong recno)
{
	elog(DEBUG5, "Committing transaction "UNI_LLU, recno);
	TXLOGSetStatus(recno, true);
}

/* Set committed status for a transaction identified by the recno */
void
TXLOGClearCommitted(ullong recno)
{
	elog(DEBUG5, "Uncommitting transaction "UNI_LLU, recno);
	TXLOGSetStatus(recno, false);
}

/* Check status of a transaction identified by the recno */
bool
TXLOGIsCommitted(ullong recno)
{
	int		pageno,
			slotno,
			byteno,
			bshift;
	char   *byteptr;
	char	status;
	SlruCtlData *slru = activeTxlogCtl->slruCtl;
	
	pageno = RecnoToPage(recno);
	byteno = RecnoToByte(recno);
	bshift = RecnoToBIndex(recno) * TXLOG_BITS_PER_XACT;

	LWLockAcquire(activeTxlogCtl->lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(slru, pageno, false, InvalidTransactionId);
	byteptr = slru->shared->page_buffer[slotno] + byteno;

	status = (*byteptr >> bshift) & TXLOG_XACT_BITMASK;

	LWLockRelease(activeTxlogCtl->lock);
	
	elog(DEBUG5, "Checking transaction "UNI_LLU" committed %d", recno, status);
	return status;
}

/*
 * TXLOGExtend
 *		Extend TXLOG if the recno requires a new page
 *
 * Some callers want to be able to forcibly create a segment.
 */
static void
TXLOGExtend(const TxlogCtlData *txlog, ullong freshRecno, bool force)
{
	int		pageno;

	/* Fast exit if this is not start of a page */
	if (!force && RecnoToPgIndex(freshRecno) != 0)
		return;

	/* If we hit a new page, zero it */
	pageno = RecnoToPage(freshRecno);
	
	/* wal-log new page creation */
	WriteZeroPageXlogRecord(pageno);
	
	LWLockAcquire(txlog->lock, LW_EXCLUSIVE);
	SimpleLruZeroPage(txlog->slruCtl, pageno);
	LWLockRelease(txlog->lock);
}

/* Truncate TXLOG pages up to the oldestRecno */
void
TXLOGTruncate(ullong oldestRecno)
{
	int		cutoffpage;

	cutoffpage = RecnoToPage(oldestRecno);
	if (!SlruScanDirectory(activeTxlogCtl->slruCtl, cutoffpage, false))
		return;

	/* Write all dirty pages on disk */
	CheckPointTXLOG();
	
	/* Wal-log the truncation */
	WriteTruncateXlogRecord(cutoffpage);

	SimpleLruTruncate(activeTxlogCtl->slruCtl, cutoffpage);
}

/*
 * Zero a page which holds data for a given transaction. This function is
 * required if an already truncated TXLOG page is needed once more.
 */
void
TXLOGZeroPageByRecno(ullong recno)
{
	TXLOGExtend(activeTxlogCtl, recno, true);
}

/* Insert a truncate wal record */
static void
WriteTruncateXlogRecord(int cutoffpage)
{
	XLogRecPtr 		ptr;
	XLogRecData 	rdata;
	TxlogRedoData 	redo;
	
	redo.isforwarder = TXLOGIsForwarder();
	redo.pageno = cutoffpage;
	
	rdata.data = (char *) &redo;
	rdata.len = sizeof(TxlogRedoData);
	/* we don't write shared memory buffers */
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	
	/* insert a wal record */
	ptr = XLogInsert(RM_RLOG_ID, RLOG_TRUNCATE, &rdata);
	/* make sure it's flushed to disk */
	XLogFlush(ptr);
}

/* Ditto for zeropage */
static void
WriteZeroPageXlogRecord(int page)
{
	XLogRecPtr 		ptr;
	XLogRecData 	rdata;
	TxlogRedoData 	redo;
	
	redo.isforwarder = TXLOGIsForwarder();
	redo.pageno = page;
	
	rdata.data = (char *) &redo;
	rdata.len = sizeof(TxlogRedoData);
	/* we don't write shared memory buffers */
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	
	/* insert a wal record */
	ptr = XLogInsert(RM_RLOG_ID, RLOG_ZEROPAGE, &rdata);
	/* XXX: The code in clog doesn't call XLogFlush here, neither do we */
}

/* Insert 'rlog commit' record */
static void
WriteCommitXlogRecord(ullong recno)
{
	XLogRecPtr			ptr;
	XLogRecData			rdata;
	TxlogRedoCommitData	redo;
	
	redo.isforwarder = TXLOGIsForwarder();
	redo.recno = recno;
	
	rdata.data = (char *)&redo;
	rdata.len = sizeof(TxlogRedoCommitData);
	/* we don't write shared memory buffers */
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;
	
	ptr = XLogInsert(RM_RLOG_ID, RLOG_COMMIT, &rdata);
	/* make sure it's flushed to disk */
	XLogFlush(ptr);
}

/* RLOG resource-manager's routines */
void
rlog_redo(XLogRecPtr lsn, XLogRecord *rec)
{
	uint8	info = rec->xl_info & ~XLR_INFO_MASK;
	
	/* we don't ever write full buffer contents */
	Assert(!(info & XLR_BKP_BLOCK_MASK));
	
	if (info == RLOG_ZEROPAGE)
	{
		int 				slotno;
		TxlogRedoData		redo;
		const TxlogCtlData	*ctl;
		
		memcpy(&redo, XLogRecGetData(rec), sizeof(TxlogRedoData));
		
		ctl = (redo.isforwarder) ? ForwarderTxlogCtl : BackendTxlogCtl;
		
		LWLockAcquire(ctl->lock, LW_EXCLUSIVE);
		slotno = SimpleLruZeroPage(ctl->slruCtl, redo.pageno);
		SimpleLruWritePage(ctl->slruCtl, slotno, NULL);
		
		Assert(!ctl->slruCtl->shared->page_dirty[slotno]);
		
		LWLockRelease(ctl->lock);		
	} 
	else if (info == RLOG_TRUNCATE)
	{
		TxlogRedoData		redo;
		const TxlogCtlData	*ctl;
		
		memcpy(&redo, XLogRecGetData(rec), sizeof(TxlogRedoData));
		
		ctl = (redo.isforwarder) ? ForwarderTxlogCtl : BackendTxlogCtl;
		
		/* 
		 * During XLOG reply, latest_page_number isn't set up yet; insert a
		 * suitable value to bypass the sanity check in SimpleLruTruncate
		 */
		ctl->slruCtl->shared->latest_page_number = redo.pageno;
		SimpleLruTruncate(ctl->slruCtl, redo.pageno);
	}
	else if (info == RLOG_COMMIT)
	{
		int 				pageno,
							slotno,
							byteno,
							bshift;
		char 			   *byteptr;
		
		TxlogRedoCommitData	redo;
		const TxlogCtlData	*ctl;
		
		memcpy(&redo, XLogRecGetData(rec), sizeof(TxlogRedoCommitData));
		
		ctl = (redo.isforwarder) ? ForwarderTxlogCtl : BackendTxlogCtl;
		
		pageno = RecnoToPage(redo.recno);
		byteno = RecnoToByte(redo.recno);
		bshift = RecnoToBIndex(redo.recno) * TXLOG_BITS_PER_XACT;
		
		LWLockAcquire(ctl->lock, LW_EXCLUSIVE);
		
		slotno = SimpleLruReadPage(ctl->slruCtl, pageno, false, InvalidTransactionId);
		byteptr = ctl->slruCtl->shared->page_buffer[slotno] + byteno;
		*byteptr |= (1 << bshift);
		ctl->slruCtl->shared->page_dirty[slotno] = true;
		
		/* XXX: is it necessary to write a page here ? */
		SimpleLruWritePage(ctl->slruCtl, slotno, NULL);
		Assert(!ctl->slruCtl->shared->page_dirty[slotno]);
		
		LWLockRelease(ctl->lock);
		
	}
	else
		elog(PANIC, "rlog_redo: unknown op code %u", info);
}

void
rlog_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8 info 	= xl_info & ~XLR_INFO_MASK;
	
	if (info == RLOG_ZEROPAGE)
	{
		TxlogRedoData 	redo;
		
		memcpy(&redo, rec, sizeof(TxlogRedoData));
		appendStringInfo(buf, "%s zeropage: %d", 
						(redo.isforwarder ? "forwarder" : "backend"), 
						redo.pageno);
		
	} 
	else if (info == RLOG_TRUNCATE)
	{
		TxlogRedoData 	redo;
		
		memcpy(&redo, rec, sizeof(TxlogRedoData));
		appendStringInfo(buf, "%s truncate: %d",
						 (redo.isforwarder ? "forwarder" : "backend"),
						redo.pageno);
	} 
	else if (info == RLOG_COMMIT)
	{
		TxlogRedoCommitData	redo;
		memcpy(&redo, rec, sizeof(TxlogRedoCommitData));
		appendStringInfo(buf, "%s commit: "UINT64_FORMAT,
						  (redo.isforwarder ? "forwarder" : "backend"),
						redo.recno);	
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}
