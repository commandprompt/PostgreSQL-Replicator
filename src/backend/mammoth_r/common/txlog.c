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

/*
 * Only one TXLOG can be in use in any one process.  We initialize this to
 * NULL and have interested processes select at runtime the one to use.
 */
static const TxlogCtlData		*activeTxlog = NULL;


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
	txlog_shmem_init(&ForwarderTxlogCtlData);
	txlog_shmem_init(&BackendTxlogCtlData);
}

/*
 * SelectActiveTxlog
 * 		Choose a TXLOG for the lifetime of this process.
 */
void
SelectActiveTxlog(bool forwarder)
{
	Assert(activeTxlog == NULL);
	activeTxlog = forwarder ? &ForwarderTxlogCtlData : &BackendTxlogCtlData;
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
	txlog_create(&ForwarderTxlogCtlData);
	txlog_create(&BackendTxlogCtlData);
}

/* Startup function */
void
TXLOGStartup(ullong recno)
{
	int		pageno;

	/* XXX: assume that frecno is not zero */
   	pageno = RecnoToPage(recno);

	LWLockAcquire(activeTxlog->lock, LW_EXCLUSIVE);

	/* Was the startup already performed ? */
	if (activeTxlog->slruCtl->shared->latest_page_number == INVALID_PAGE_NUMBER)
		activeTxlog->slruCtl->shared->latest_page_number = pageno;	

	LWLockRelease(activeTxlog->lock);
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
	SlruCtlData *slru = activeTxlog->slruCtl;

	pageno = RecnoToPage(recno);
	byteno = RecnoToByte(recno);
	bshift = RecnoToBIndex(recno) * TXLOG_BITS_PER_XACT;

	/* Make a new txlog segment if needed */
	TXLOGExtend(activeTxlog, recno, false);

	LWLockAcquire(activeTxlog->lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(slru, pageno, false, InvalidTransactionId);
	byteptr = slru->shared->page_buffer[slotno] + byteno;

	/* XXX: The following assumes we use only 1 bit per transaction. */
	if (committed)	
	{
		/* Transaction should not be marked as committed yet */
		Assert(((*byteptr >> bshift) & TXLOG_XACT_BITMASK) == REPL_TX_STATE_EMPTY);

		/* XXX: This assumes we use only 1 bit per transaction. */
		*byteptr |= (1 << bshift);
	}
	else
		*byteptr &= ~(1 << bshift);
	
	slru->shared->page_dirty[slotno] = true;
	
	LWLockRelease(activeTxlog->lock);
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
	SlruCtlData *slru = activeTxlog->slruCtl;
	
	pageno = RecnoToPage(recno);
	byteno = RecnoToByte(recno);
	bshift = RecnoToBIndex(recno) * TXLOG_BITS_PER_XACT;

	LWLockAcquire(activeTxlog->lock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(slru, pageno, false, InvalidTransactionId);
	byteptr = slru->shared->page_buffer[slotno] + byteno;

	status = (*byteptr >> bshift) & TXLOG_XACT_BITMASK;

	LWLockRelease(activeTxlog->lock);
	
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
	if (!SlruScanDirectory(activeTxlog->slruCtl, cutoffpage, false))
		return;

	/* Write all dirty pages on disk */
	CheckPointTXLOG();

	SimpleLruTruncate(activeTxlog->slruCtl, cutoffpage);
}

/*
 * Zero a page which holds data for a given transaction. This function is
 * required if an already truncated TXLOG page is needed once more.
 */
void
TXLOGZeroPageByRecno(ullong recno)
{
	TXLOGExtend(activeTxlog, recno, true);
}
