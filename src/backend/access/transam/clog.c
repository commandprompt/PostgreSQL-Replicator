/*-------------------------------------------------------------------------
 *
 * clog.c
 *		PostgreSQL transaction-commit-log manager
 *
 * This module replaces the old "pg_log" access code, which treated pg_log
 * essentially like a relation, in that it went through the regular buffer
 * manager.  The problem with that was that there wasn't any good way to
 * recycle storage space for transactions so old that they'll never be
 * looked up again.  Now we use specialized access code so that the commit
 * log can be broken into relatively small, independent segments.
 *
 * XLOG interactions: this module generates an XLOG record whenever a new
 * CLOG page is initialized to zeroes.	Other writes of CLOG come from
 * recording of transaction commit or abort in xact.c, which generates its
 * own XLOG records for these events and will re-perform the status update
 * on redo; so we need make no additional XLOG entry here.	Also, the XLOG
 * is guaranteed flushed through the XLOG commit record before we are called
 * to log a commit, so the WAL rule "write xlog before data" is satisfied
 * automatically for commits, and we don't really care for aborts.  Therefore,
 * we don't need to mark CLOG pages with LSN information; we have enough
 * synchronization already.
 *
 * Portions Copyright (c) 1996-2004, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/clog.h"
#include "access/slru.h"
#include "postmaster/bgwriter.h"


/*
 * Defines for CLOG page sizes.  A page is the same BLCKSZ as is used
 * everywhere else in Postgres.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * CLOG page numbering also wraps around at 0xFFFFFFFF/CLOG_XACTS_PER_PAGE,
 * and CLOG segment numbering at 0xFFFFFFFF/CLOG_XACTS_PER_SEGMENT.  We need
 * take no explicit notice of that fact in this module, except when comparing
 * segment and page numbers in TruncateCLOG (see CLOGPagePrecedes).
 */

/* We need two bits per xact, so four xacts fit in a byte */
#define CLOG_BITS_PER_XACT	2
#define CLOG_XACTS_PER_BYTE 4
#define CLOG_XACTS_PER_PAGE (BLCKSZ * CLOG_XACTS_PER_BYTE)
#define CLOG_XACT_BITMASK	((1 << CLOG_BITS_PER_XACT) - 1)

#define TransactionIdToPage(xid)	((xid) / (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToPgIndex(xid) ((xid) % (TransactionId) CLOG_XACTS_PER_PAGE)
#define TransactionIdToByte(xid)	(TransactionIdToPgIndex(xid) / CLOG_XACTS_PER_BYTE)
#define TransactionIdToBIndex(xid)	((xid) % (TransactionId) CLOG_XACTS_PER_BYTE)


/*
 * Link to shared-memory data structures for CLOG control
 */
static SlruCtlData ClogCtlData;

#define ClogCtl (&ClogCtlData)


static int	ZeroCLOGPage(int pageno, bool writeXlog);
static bool CLOGPagePrecedes(int page1, int page2);
static void WriteZeroPageXlogRec(int pageno);


/*
 * Record the final state of a transaction in the commit log.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogUpdate() in transam.c is the intended caller.
 */
void
TransactionIdSetStatus(TransactionId xid, XidStatus status)
{
	int			pageno = TransactionIdToPage(xid);
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	int			slotno;
	char	   *byteptr;
	char		byteval;

	Assert(status == TRANSACTION_STATUS_COMMITTED ||
		   status == TRANSACTION_STATUS_ABORTED ||
		   status == TRANSACTION_STATUS_SUB_COMMITTED);

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(ClogCtl, pageno, xid);
	byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;

	/* Current state should be 0, subcommitted or target state */
	Assert(((*byteptr >> bshift) & CLOG_XACT_BITMASK) == 0 ||
		   ((*byteptr >> bshift) & CLOG_XACT_BITMASK) == TRANSACTION_STATUS_SUB_COMMITTED ||
		   ((*byteptr >> bshift) & CLOG_XACT_BITMASK) == status);

	/* note this assumes exclusive access to the clog page */
	byteval = *byteptr;
	byteval &= ~(((1 << CLOG_BITS_PER_XACT) - 1) << bshift);
	byteval |= (status << bshift);
	*byteptr = byteval;

	ClogCtl->shared->page_status[slotno] = SLRU_PAGE_DIRTY;

	LWLockRelease(CLogControlLock);
}

/*
 * Interrogate the state of a transaction in the commit log.
 *
 * NB: this is a low-level routine and is NOT the preferred entry point
 * for most uses; TransactionLogFetch() in transam.c is the intended caller.
 */
XidStatus
TransactionIdGetStatus(TransactionId xid)
{
	int			pageno = TransactionIdToPage(xid);
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	int			slotno;
	char	   *byteptr;
	XidStatus	status;

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	slotno = SimpleLruReadPage(ClogCtl, pageno, xid);
	byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;

	status = (*byteptr >> bshift) & CLOG_XACT_BITMASK;

	LWLockRelease(CLogControlLock);

	return status;
}


/*
 * Initialization of shared memory for CLOG
 */

int
CLOGShmemSize(void)
{
	return SimpleLruShmemSize();
}

void
CLOGShmemInit(void)
{
	ClogCtl->PagePrecedes = CLOGPagePrecedes;
	SimpleLruInit(ClogCtl, "CLOG Ctl", CLogControlLock, "pg_clog");
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial CLOG segment.  (The CLOG directory is assumed to
 * have been created by the initdb shell script, and CLOGShmemInit
 * must have been called already.)
 */
void
BootStrapCLOG(void)
{
	int			slotno;

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/* Create and zero the first page of the commit log */
	slotno = ZeroCLOGPage(0, false);

	/* Make sure it's written out */
	SimpleLruWritePage(ClogCtl, slotno, NULL);
	Assert(ClogCtl->shared->page_status[slotno] == SLRU_PAGE_CLEAN);

	LWLockRelease(CLogControlLock);
}

/*
 * Initialize (or reinitialize) a page of CLOG to zeroes.
 * If writeXlog is TRUE, also emit an XLOG record saying we did this.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
static int
ZeroCLOGPage(int pageno, bool writeXlog)
{
	int			slotno;

	slotno = SimpleLruZeroPage(ClogCtl, pageno);

	if (writeXlog)
		WriteZeroPageXlogRec(pageno);

	return slotno;
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized ShmemVariableCache->nextXid.
 */
void
StartupCLOG(void)
{
	TransactionId xid = ShmemVariableCache->nextXid;
	int			pageno = TransactionIdToPage(xid);
	int			byteno = TransactionIdToByte(xid);
	int			bshift = TransactionIdToBIndex(xid) * CLOG_BITS_PER_XACT;
	int			slotno;
	char	   *byteptr;

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/*
	 * Initialize our idea of the latest page number.
	 */
	ClogCtl->shared->latest_page_number = pageno;

	/*
	 * Zero out the remainder of the current clog page.  Under normal
	 * circumstances it should be zeroes already, but it seems at least
	 * theoretically possible that XLOG replay will have settled on a
	 * nextXID value that is less than the last XID actually used and
	 * marked by the previous database lifecycle (since subtransaction
	 * commit writes clog but makes no WAL entry).  Let's just be safe.
	 * (We need not worry about pages beyond the current one, since those
	 * will be zeroed when first used.)
	 */
	slotno = SimpleLruReadPage(ClogCtl, pageno, xid);
	byteptr = ClogCtl->shared->page_buffer[slotno] + byteno;

	/* Zero so-far-unused positions in the current byte */
	*byteptr &= (1 << bshift) - 1;
	/* Zero the rest of the page */
	MemSet(byteptr + 1, 0, BLCKSZ - byteno - 1);

	ClogCtl->shared->page_status[slotno] = SLRU_PAGE_DIRTY;

	LWLockRelease(CLogControlLock);
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
void
ShutdownCLOG(void)
{
	/* Flush dirty CLOG pages to disk */
	SimpleLruFlush(ClogCtl, false);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
void
CheckPointCLOG(void)
{
	/* Flush dirty CLOG pages to disk */
	SimpleLruFlush(ClogCtl, true);
}


/*
 * Make sure that CLOG has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty clog or xlog page to make room
 * in shared memory.
 */
void
ExtendCLOG(TransactionId newestXact)
{
	int			pageno;

	/*
	 * No work except at first XID of a page.  But beware: just after
	 * wraparound, the first XID of page zero is FirstNormalTransactionId.
	 */
	if (TransactionIdToPgIndex(newestXact) != 0 &&
		!TransactionIdEquals(newestXact, FirstNormalTransactionId))
		return;

	pageno = TransactionIdToPage(newestXact);

	LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

	/* Zero the page and make an XLOG entry about it */
	ZeroCLOGPage(pageno, true);

	LWLockRelease(CLogControlLock);
}


/*
 * Remove all CLOG segments before the one holding the passed transaction ID
 *
 * When this is called, we know that the database logically contains no
 * reference to transaction IDs older than oldestXact.	However, we must
 * not truncate the CLOG until we have performed a checkpoint, to ensure
 * that no such references remain on disk either; else a crash just after
 * the truncation might leave us with a problem.  Since CLOG segments hold
 * a large number of transactions, the opportunity to actually remove a
 * segment is fairly rare, and so it seems best not to do the checkpoint
 * unless we have confirmed that there is a removable segment.	Therefore
 * we issue the checkpoint command here, not in higher-level code as might
 * seem cleaner.
 */
void
TruncateCLOG(TransactionId oldestXact)
{
	int			cutoffPage;

	/*
	 * The cutoff point is the start of the segment containing oldestXact.
	 * We pass the *page* containing oldestXact to SimpleLruTruncate.
	 */
	cutoffPage = TransactionIdToPage(oldestXact);

	/* Check to see if there's any files that could be removed */
	if (!SlruScanDirectory(ClogCtl, cutoffPage, false))
		return;					/* nothing to remove */

	/* Perform a CHECKPOINT */
	RequestCheckpoint(true);

	/* Now we can remove the old CLOG segment(s) */
	SimpleLruTruncate(ClogCtl, cutoffPage);
}


/*
 * Decide which of two CLOG page numbers is "older" for truncation purposes.
 *
 * We need to use comparison of TransactionIds here in order to do the right
 * thing with wraparound XID arithmetic.  However, if we are asked about
 * page number zero, we don't want to hand InvalidTransactionId to
 * TransactionIdPrecedes: it'll get weird about permanent xact IDs.  So,
 * offset both xids by FirstNormalTransactionId to avoid that.
 */
static bool
CLOGPagePrecedes(int page1, int page2)
{
	TransactionId xid1;
	TransactionId xid2;

	xid1 = ((TransactionId) page1) * CLOG_XACTS_PER_PAGE;
	xid1 += FirstNormalTransactionId;
	xid2 = ((TransactionId) page2) * CLOG_XACTS_PER_PAGE;
	xid2 += FirstNormalTransactionId;

	return TransactionIdPrecedes(xid1, xid2);
}


/*
 * Write a ZEROPAGE xlog record
 *
 * Note: xlog record is marked as outside transaction control, since we
 * want it to be redone whether the invoking transaction commits or not.
 * (Besides which, this is normally done just before entering a transaction.)
 */
static void
WriteZeroPageXlogRec(int pageno)
{
	XLogRecData rdata;

	rdata.buffer = InvalidBuffer;
	rdata.data = (char *) (&pageno);
	rdata.len = sizeof(int);
	rdata.next = NULL;
	(void) XLogInsert(RM_CLOG_ID, CLOG_ZEROPAGE | XLOG_NO_TRAN, &rdata);
}

/*
 * CLOG resource manager's routines
 */
void
clog_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	if (info == CLOG_ZEROPAGE)
	{
		int			pageno;
		int			slotno;

		memcpy(&pageno, XLogRecGetData(record), sizeof(int));

		LWLockAcquire(CLogControlLock, LW_EXCLUSIVE);

		slotno = ZeroCLOGPage(pageno, false);
		SimpleLruWritePage(ClogCtl, slotno, NULL);
		Assert(ClogCtl->shared->page_status[slotno] == SLRU_PAGE_CLEAN);

		LWLockRelease(CLogControlLock);
	}
}

void
clog_undo(XLogRecPtr lsn, XLogRecord *record)
{
}

void
clog_desc(char *buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	if (info == CLOG_ZEROPAGE)
	{
		int			pageno;

		memcpy(&pageno, rec, sizeof(int));
		sprintf(buf + strlen(buf), "zeropage: %d", pageno);
	}
	else
		strcat(buf, "UNKNOWN");
}
