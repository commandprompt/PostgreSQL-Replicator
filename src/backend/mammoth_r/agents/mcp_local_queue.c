/*-----------------------
 * mcp_local_queue.c
 * 		The Mammoth Replication MCP Local Queue implementation
 *
 * This is only used by the master.  The backends copy data in this queue
 * (which is unique to each backend), and at the end of transaction the data
 * is moved to the main queue.
 *
 * This code used to be part of MCPQueue, but because of being specific to the
 * master, it certainly does not belong in there.  This allows us to simplify
 * a couple of functions, and it frees the MCP server from having to deal with
 * it.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_local_queue.c 2115 2009-04-28 15:16:11Z alexk $
 *
 * ------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <unistd.h>

#include "mammoth_r/agents.h"
#include "mammoth_r/backend_tables.h"
#include "mammoth_r/collector.h"
#include "mammoth_r/mcp_compress.h"
#include "mammoth_r/mcp_lists.h"
#include "mammoth_r/mcp_local_queue.h"
#include "mammoth_r/pgr.h"
#include "mammoth_r/promotion.h"
#include "nodes/bitmapset.h"
#include "postmaster/replication.h"
#include "utils/guc.h"
#include "utils/memutils.h"

static void reset_files(MCPLocalQueue *lq);
static void lq_merge_files(MCPLocalQueue *lq);
static void lq_write_tablelist(MCPFile *mf_trans, List *tlist);


/*
 * MCPLocalQueue is where we keep track of replicated changes made by the
 * current transaction. It has two kinds of attributes, transactional ones
 * describe data that is replicated only on transaction commit, and data
 * that should be replicated even on rollback (sequences) are described
 * with non-transactional attributes.
 *
 * The caller must invoke MCPLocalQueueFinalize when the transaction
 * is about to be closed.
 */
/* typedef appears in mcp_local_queue.h */

struct MCPLocalQueue
{
	MemoryContext	lq_context;		/* for table lists */
	MCPFile		   *lq_trans;		/* transactional updates */
	MCPFile		   *lq_nontrans;	/* non-transaction (sequence) data */
	bool			lq_trans_changed;
	bool			lq_nontrans_changed;
	List		   *lq_tablist; 
	List		   *lq_nt_tablist;
	Bitmapset	   *lq_bms;
};


MCPLocalQueue *
MCPLocalQueueInit(MCPQueue *q)
{
	MCPLocalQueue *lq;
	char	   *path,
			   *nt_path;

	lq = palloc0(sizeof(MCPLocalQueue));

	lq->lq_context = AllocSetContextCreate(TopMemoryContext,
										   "Local Queue Context",
										   ALLOCSET_DEFAULT_MINSIZE,
										   ALLOCSET_DEFAULT_INITSIZE,
										   ALLOCSET_DEFAULT_MAXSIZE);
	/* initialize the file */
	path = MCPQueueGetLocalFilename(q, "t");
	nt_path = MCPQueueGetLocalFilename(q, "nt");
	lq->lq_trans = MCPFileCreate(path);
	lq->lq_nontrans = MCPFileCreate(nt_path);
	pfree(path);
	pfree(nt_path);

	/* and make sure it's created and empty */
	reset_files(lq);

	return lq;
}

static void
reset_files(MCPLocalQueue *lq)
{
	TxDataHeader	hdr;

	/* TODO: we really don't need to truncate or write queue header
	 * to the files that were not changed during the prev. transaction.
	 */

	/* Create and open the file.  If it existed already, truncate it. */
	if (!MCPFileCreateFile(lq->lq_trans))
	{
		MCPFileTruncate(lq->lq_trans, 0);

		/* seek to start-of-file since truncate doesn't change file position */
		MCPFileSeek(lq->lq_trans, 0, SEEK_SET);
	}

	/* Ditto for the non-transaction file */
	if (!MCPFileCreateFile(lq->lq_nontrans))
	{
		MCPFileTruncate(lq->lq_nontrans, 0);
		MCPFileSeek(lq->lq_nontrans, 0, SEEK_SET);
	}

	/* Write a transaction header */
	MemSet(&hdr, 0, sizeof(TxDataHeader));
	MCPFileWrite(lq->lq_trans, &hdr, sizeof(TxDataHeader));
	MCPFileWrite(lq->lq_nontrans, &hdr, sizeof(TxDataHeader));

	/* and reset state */
	lq->lq_trans_changed = lq->lq_nontrans_changed = false;
	/* 
	 * Clear table lists since they can be non empty if transaction
	 * was aborted.
	 */
	MemoryContextReset(lq->lq_context);
	lq->lq_tablist = lq->lq_nt_tablist = NIL;
	lq->lq_bms = NULL;
}

/*
 * MCPLocalQueueSwitchFile
 *
 * Create a new disk file (or truncate existing) and write its transaction hdr,
 * with this local queue's sanctioned name.
 */
void
MCPLocalQueueSwitchFile(MCPLocalQueue *lq)
{
	/* Close the old files */
	MCPFileClose(lq->lq_trans);	
	MCPFileClose(lq->lq_nontrans);

	/* and empty it */
	reset_files(lq);
}

/*
 * MCPLocalQueueClose
 */
void
MCPLocalQueueClose(MCPLocalQueue *lq)
{
	Assert(lq != NULL);

	/* remove the file and free memory */
	MCPFileDestroy(lq->lq_trans);
	MCPFileDestroy(lq->lq_nontrans);
	pfree(lq);
}

/*
 * MCPLocalQueueAppend
 * 		Append a header and data to the local queue
 *
 * The "trans" argument indicates whether the data is transactional (i.e. must
 * be rolled back on transaction abort) or not (i.e. it behaves like
 * sequences).
 *
 * Note: we assume that the current file offset is always at the end of the
 * file.
 */
void
MCPLocalQueueAppend(MCPLocalQueue *lq, void *data, uint32 datalen, char flags,
					bool trans)
{
	MCPQueueTxlDataHeader dh;
	void	   *compdata;
	MCPFile	   *which;
	pg_crc32	crc32;

	/* force use of the transactional file in dump mode */
	if (PGRUseDumpMode)
		trans = true;

	/* If promotion is in progress, abort the current transaction */
	if (ReplPromotionData->promotion_block_relations && !IsReplicatorProcess())
		ereport(ERROR,
				(errmsg("promotion in progress")));

	memset(&dh, 0, sizeof(dh));
	dh.tdh_datalen = datalen;
	dh.tdh_flags |= flags;
	
	if (!lq->lq_trans_changed && !lq->lq_nontrans_changed)
	{
		/* The first command in this transaction */
		if (current_is_tablecmd)
		{
			initial_is_tablecmd = true;
			elog(DEBUG5,
				 "transaction causes catalog dump, skipping command collection");
		}
	}
	else if (current_is_tablecmd != initial_is_tablecmd)
		ereport(ERROR,
				(errmsg("command combination disallowed"),
				 errdetail("Commands that alter replication state cannot be "
						   "mixed with replicated actions.")));

	/* pick a proper file to write changes and make note that our file changed */
	if (trans)
	{
		lq->lq_trans_changed = true;
		which = lq->lq_trans;
	}
	else
	{
		lq->lq_nontrans_changed = true;
		which = lq->lq_nontrans;
	}

    /*
	 * Do nothing for commands from tablecmds.c (but note that the
	 * "lq_foo_changed" bit has been set nevertheless).
	 */
    if (current_is_tablecmd)
        return;

	/* 
	 * Run compression code if replication_compression param is set and
	 * the data allows compression that is controlled by the compress param.
	 */
	if (replication_compression && dh.tdh_datalen > 0)
		compdata = MCPCompressData(data, &dh);
	else
		compdata = data;

	/* 
	 * Since crc32 will be calculated with this flag set on a slave we should
	 * set it on a master as well before calculating crc.
	 */ 
	dh.tdh_flags |= TXL_FLAG_CHECKSUMMED;

	INIT_CRC32(crc32);
	/* Consider both the header and the data when calculating a checksum */
	COMP_CRC32(crc32, &dh, sizeof(MCPQueueTxlDataHeader));
	COMP_CRC32(crc32, compdata, dh.tdh_datalen);
	FIN_CRC32(crc32);
	/* Store a calculated checksum in the header */
	dh.tdh_crc32 = crc32;

	MCPFileWrite(which, &dh, sizeof(MCPQueueTxlDataHeader));

	if (dh.tdh_datalen > 0)
		MCPFileWrite(which, compdata, dh.tdh_datalen);

	/* Free memory used by compression */
	if (compdata != data)
	{
		Assert(datalen != dh.tdh_datalen);
		elog(DEBUG5, "data size: raw %d, compressed %d", datalen, dh.tdh_datalen);
		pfree(compdata);
	}
}

/* 
 * MCPLocalQueueAddRelation
 *		Add the given relation to the indicated table list, trans is true
 *		for a transactional data and false otherwise.
 */
void
MCPLocalQueueAddRelation(MCPLocalQueue *lq, char *relpath, int raise_dump,
						 bool trans)
{
	List	   *which;
	BackendTable tab;

	/* force use of the transactional file in dump mode */
	if (PGRUseDumpMode)
		trans = true;

	/* pick the correct table list */
	if (trans)
	{
		Assert(lq->lq_trans_changed);
		which = lq->lq_tablist;
	}
	else
	{
		Assert(lq->lq_nontrans_changed);
		which = lq->lq_nt_tablist;
	}

	tab = TableListEntryByName(which, relpath);
	if (tab == NULL)
	{
		MemoryContext	oldcxt;

		/* make sure the Table allocations happen on a long-lived memory context */
		oldcxt = MemoryContextSwitchTo(lq->lq_context);
		tab = MakeBackendTable(relpath, raise_dump);
		which = lappend(which, tab);

		/* put the picked list changes back to the original list */
		if (trans)
			lq->lq_tablist = which;
		else
			lq->lq_nt_tablist = which;

		MemoryContextSwitchTo(oldcxt);
	}
	else
	{
		/*
		 * If we were not previously requesting a dump, but the new addition
		 * wants a dump, ask for it.
		 */
		if (tab->raise_dump != TableNoDump)
			tab->raise_dump = raise_dump;
	}
}

/* lq_merge_files
 * 		Merge transactional and non-transactional data files and
 * 		write the resulting data to the transactional file.
 */
static void 
lq_merge_files(MCPLocalQueue *lq)
{
	uint32	nread,
			nleft;
	char    *buf;
   
	Assert(lq->lq_trans_changed);
	Assert(lq->lq_nontrans_changed);

	/* Assuming that lq->nontrans is positioned to EOF */
	nleft = MCPFileSeek(lq->lq_nontrans, 0, SEEK_CUR) - sizeof(TxDataHeader);

	/* Move nontrans file pointer to a position right after the data header */
	MCPFileSeek(lq->lq_nontrans, sizeof(TxDataHeader), SEEK_SET);

/* TODO: What is the optimal value for this constant ? */
#define BUFFERSIZE 8192
	buf = palloc(BUFFERSIZE);

	/* Assuming that lq->trans is positioned at EOF */
	do
	{
		nread = Min(BUFFERSIZE, nleft);

		MCPFileRead(lq->lq_nontrans, buf, nread, false);
		MCPFileWrite(lq->lq_trans, buf, nread);
	
		nleft -= nread;
	}
	while (nleft > 0);
}

/* Write tablelist data -- See TxReadTableList if you change anything */
static void
lq_write_tablelist(MCPFile *mf_trans, List *tlist)
{
	ListCell    *cell;

	foreach (cell, tlist)
	{
		Table		tab = lfirst(cell);

		StoreTable(mf_trans, tab);
	}
}

/*
 * MCPLocalQueueFinalize
 * 		Tell MCPLocalQueue that the transaction has ended
 *
 * Here we need to prepare the replication data file to ship.  This routine is
 * in charge of deciding what data file to use (i.e. transactional or non-
 * transactional); if both are needed, make sure they are both copied.  Also,
 * append the table list to the file and finalize the data header.
 */
void
MCPLocalQueueFinalize(MCPLocalQueue *lq, int flags, bool is_commit)
{
	off_t			listoff;
	int				total;
	TxDataHeader 	hdr;

	Assert((lq->lq_trans_changed && lq->lq_tablist != NULL) ||
		   (lq->lq_nontrans_changed && lq->lq_nt_tablist != NULL));

	/* Check if we have anything to do here */
	Assert((lq->lq_trans_changed && is_commit ) || lq->lq_nontrans_changed);

	/* Merge transactional and nontransaction data */

	if (lq->lq_trans_changed && lq->lq_nontrans_changed && is_commit)
	{
		/* if we need both files - merge them */
		lq_merge_files(lq);
	}
	else if (!lq->lq_trans_changed || !is_commit)
	{
		/* if only nontransactional data is needed - swap MCPFile pointers */
		MCPFile    *tmpf = lq->lq_trans;

		lq->lq_trans = lq->lq_nontrans;
		lq->lq_nontrans = tmpf;
	}

	listoff = MCPFileSeek(lq->lq_trans, 0, SEEK_END);

	/* Append guardian word before the tablelist data */
	MCPFileWrite(lq->lq_trans, (void *)"TS", 2);

	/* Write total number of tables */
	total = list_length(lq->lq_nt_tablist);
	if (is_commit)
		total += list_length(lq->lq_tablist);

	Assert(total > 0);

	MCPFileWrite(lq->lq_trans, &total, sizeof(uint32));

	/* Write transactional table list only on commit */
	if (is_commit)
		lq_write_tablelist(lq->lq_trans, lq->lq_tablist);

	lq_write_tablelist(lq->lq_trans, lq->lq_nt_tablist);

	/* Write finishing guardian word */
	MCPFileWrite(lq->lq_trans, (void *)"TE", 2);

	/* prepare to write the data header */
	MemSet(&hdr, 0, sizeof(TxDataHeader));
	hdr.dh_id = DATA_HEADER_ID;
	hdr.dh_flags = flags;
	hdr.dh_listoffset = listoff;
	/* total file length */
	hdr.dh_len = MCPFileSeek(lq->lq_trans, 0, SEEK_CUR);

	/* write the header at the start of the file */
	MCPFileSeek(lq->lq_trans, 0, SEEK_SET);
	MCPFileWrite(lq->lq_trans, &hdr, sizeof(TxDataHeader));

	/* all done! clear the table list for the next transaction */
	MemoryContextReset(lq->lq_context);
	lq->lq_tablist = lq->lq_nt_tablist = NIL;
}

/* MCPLocalQueueFinishEmptyTx
 *
 * 	Like MCPLocalQueueFinalize, but doesn't deal with table lists
 * 	and non-transactional files, instead puts a transaction header
 * 	with QUEUE_FLAG_EMPTY flag.
 */
void
MCPLocalQueueFinishEmptyTx(MCPLocalQueue *lq, int flags)
{
	TxDataHeader	hdr;

	/* Check that this is really an empty transaction */
	Assert(!lq->lq_trans_changed && !lq->lq_nontrans_changed);

    elog(DEBUG5, "Writing empty transaction, flags = %d", flags);    

	/* prepare to write the data header */
	MemSet(&hdr, 0, sizeof(TxDataHeader));
	hdr.dh_id = DATA_HEADER_ID;
	hdr.dh_flags = MCP_QUEUE_FLAG_EMPTY | flags; 
	/* no tables in this transaction */
	hdr.dh_listoffset = (off_t) 0;
	/* total file length */
	hdr.dh_len = MCPFileSeek(lq->lq_trans, 0, SEEK_END);

	/* write the header at the start of the file */
	MCPFileSeek(lq->lq_trans, 0, SEEK_SET);
	MCPFileWrite(lq->lq_trans, &hdr, sizeof(TxDataHeader));
}

bool
MCPLocalQueueIsEmpty(MCPLocalQueue *lq, bool is_commit)
{
	/* 
	 * either transactional data was not changed or this is not commit and
	 * nontransaction data is unchanged.
	 */
	return (!lq->lq_trans_changed || !is_commit) && !lq->lq_nontrans_changed;
}

MCPFile *
MCPLocalQueueGetFile(MCPLocalQueue *lq)
{
	return lq->lq_trans;
}

/*
 * MCPQueueTxReadPacket
 *
 * Read the next packet from the given MCP queue.
 *
 * A packet is some data prepended by a TxlDataHeader. We don't interpret the
 * data here, leaving this task to a caller -- except when we detect a
 * COMMAND_END flag, in which case we return NULL.  On EOF, we return NULL.
 * This is only allowed if the caller passes allow_end as true.  If it doesn't,
 * any of these conditions causes an ERROR.
 *
 * If no valid packet is found, we elog(ERROR).
 */
StringInfo
MCPQueueTxReadPacket(MCPQueue *q, bool allow_end)
{
	MCPQueueTxlDataHeader dh;
	StringInfo	str;

	/* Make sure there is an active transaction in the queue */
	Assert(MCPQueueGetDatafile(q) != NULL);

	/*
	 * Read a first txl header corresponding to non-empty data part.  If it
	 * can't be read, it's because we're at EOF; return NULL in that case,
	 * but only if allowed by the caller.  (MCPQueueReadData will error out
	 * if EOF is not allowed here.)
	 */
	if (!MCPQueueReadData(q, &dh, sizeof(MCPQueueTxlDataHeader), allow_end))
		return NULL;

	/* check if this is a packet final packet for some command */
	if (dh.tdh_flags & TXL_FLAG_COMMAND_END)
	{
		if (allow_end)
		{
			elog(DEBUG5, "command end marker found");
			return NULL;
		}
		else
			elog(ERROR, "unexpected TXL_FLAG COMMAND_END detected");
	}

	if (dh.tdh_datalen == 0)
		elog(ERROR, "unexpected 0-length data packet");

	/* allocate our result struct */
	str = makeStringInfo();
	str->len = dh.tdh_datalen;
	str->data = palloc(str->len);

	/* and read the data */
	MCPQueueReadData(q, str->data, str->len, false);

	/* Verify the CRC checksum, if any */
	if (dh.tdh_flags & TXL_FLAG_CHECKSUMMED)
	{
		pg_crc32 	rcvd,
					cur;

		/* 
		 * Reset crc32 field to match the data on master at the moment 
		 * of calculating CRC
		 */
		rcvd = dh.tdh_crc32;
		dh.tdh_crc32 = 0;

		INIT_CRC32(cur);
		COMP_CRC32(cur, &dh, sizeof(MCPQueueTxlDataHeader));
		COMP_CRC32(cur, str->data, str->len);
		FIN_CRC32(cur);

		if (!EQ_CRC32(rcvd, cur))
			ereport(ERROR,
					(errmsg("CRC check failed"),
					 errdetail("Data corruption detected for transaction "
							   UNI_LLU, MCPQueueGetFirstRecno(q))));
	}

	/* Possibly decompress it */
	return MCPDecompressData(str, &dh);
}

/* Add slave id to the transaction's bitmapset */
void
LocalQueueAddSlaveToBitmapset(MCPLocalQueue *lq, int slaveno)
{
	lq->lq_bms = bms_add_member(lq->lq_bms, slaveno);
}
