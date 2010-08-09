/*
 * mcp_api.c
 * 		Common high level routines for replicating data between the nodes.
 *
 * This module relies on MCPQueue and MCPConnection. Functions defined here
 * should deal with sending/receiving queue data. General rule is that if
 * function requires both MCPQueue and MCPConnection as arguments and it is
 * not node-specific - it should be placed here.
 *
 * $Id: mcp_api.c 2227 2009-09-22 07:04:32Z alexk $
 */

#include "postgres.h"

#include "mammoth_r/backend_tables.h"
#include "mammoth_r/mcp_api.h"
#include "mammoth_r/mcp_connection.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/txlog.h"
#include "mammoth_r/mcp_lists.h"
#include "nodes/pg_list.h"


static List *TxReadTableList(MCPQueue *q, ullong recno, TxDataHeader *hdr);

/*
 * Read the given transaction from the queue and send it.
 *
 * The tablelist_hook is a function to determine whether to send the
 * transaction (return true) or not (returns false).
 *
 * The message_hook is a function to preprocess the message before sending it.
 */
void
SendQueueTransaction(MCPQueue *q, 
					 ullong recno,
					 bool (*tablelist_hook)(TxDataHeader *, List *, void *), 
					 void *tablelist_arg,
					 off_t (*message_hook)(TxDataHeader *, void *, ullong),
					 void *message_arg)
{
	off_t		read;
	ssize_t		toread,
				offset;
	off_t		txsize;
	int			flags;
	MCPMsg	   *sm;
	TxDataHeader	*hdr;
	bool		skip = false;

	/* Make sure we are not trying to send something not in the queue */
#ifdef ASSERT_ENABLED
	if (assert_enabled)
	{
		LockReplicationQueue(q, LW_SHARED);
		Assert(recno >= MCPQueueGetInitialRecno(q) &&
			   recno <= MCPQueueGetLastRecno(q));
		UnlockReplicationQueue(q);
	}
#endif

	/* Make sure we are not trying to send uncommitted transactions */
	Assert(TXLOGIsCommitted(recno));

	/* Open a transaction data file */
	MCPQueueTxOpen(q, recno);

	sm = palloc(sizeof(MCPMsg) + MCP_MAX_MSG_SIZE - 1);

	/* XXX: Should use MCPQueueReadData with sizeof(TxDataHeader) instead */
	MCPQueueReadDataHeader(q, (TxDataHeader *) sm->data);

	hdr = (TxDataHeader *) sm->data;
	txsize = hdr->dh_len;
	flags = hdr->dh_flags;

	if (txsize < sizeof(TxDataHeader))
		ereport(ERROR,
				(errmsg("invalid transaction file size"),
				 errdetail("Transaction "UNI_LLU" has size "UINT64_FORMAT,
						   recno, (ullong) txsize)));

	/* call the tablelist hook if the caller specified one */
	if (tablelist_hook != NULL)
	{
		List   *TxTableList;
		MCPFile *file = MCPQueueGetDatafile(q);
		off_t	pos = MCPFileSeek(file, 0, SEEK_CUR);

		TxTableList = TxReadTableList(q, recno, hdr);

		/* 
		 * HACK: call table list hook for full dump even if table list
		 * is NULL. DUMP messages should be examined as early as possible
		 * to determine whether we should skip them. These checks should
		 * be moved out of table list hook, probably to the new dump
		 * messages hook.
		 */
		if (TxTableList != NIL || MessageTypeBelongsToFullDump(flags))
		{
			/* Check if we don't have to send current transaction */
			skip = ! tablelist_hook(hdr, TxTableList, tablelist_arg);

			list_free_deep(TxTableList);

			/* Return to the original position in data file */
			MCPFileSeek(file, pos, SEEK_SET);
		} 
		else 
		{
			ereport(DEBUG2,
					(errmsg("table list hook not called"),
					 errdetail("Transaction does not have a table list.")));
		}
	}

	/* Call the message hook if the caller specified one */
	if (message_hook != NULL && !skip)
		txsize = message_hook(hdr, message_arg, recno);

	if (!skip)
		elog(DEBUG3, "sending transaction "UNI_LLU" size "UINT64_FORMAT,
		 		 	 recno, (ullong) txsize);
	else
		elog(DEBUG3, "skipping transaction "UNI_LLU" size "UINT64_FORMAT,
 		 			 recno, (ullong) txsize);

	read = (off_t) sizeof(TxDataHeader);
	offset = sizeof(TxDataHeader);
	
	/* 
	 * In case, when the transactions should be skipped, send an empty
	 * message instead.
	 */
	if (skip)
	{
		toread = 0;
		flags = (hdr->dh_flags = MCP_QUEUE_FLAG_EMPTY);
		txsize = read;
		hdr->dh_len = txsize;
	} else
		toread = Min(txsize, MCP_MAX_MSG_SIZE) - sizeof(TxDataHeader);

	/*
	 * The only case when read == txsize is when transaction is empty
	 * and its data file contains only TxDataHeader (offset is not 0)
	 */
	while (toread > 0 || offset)
	{
		/* Read data from queue */
		if (toread > 0)
		{
			MCPQueueReadData(q, sm->data + offset, toread, false);
			read += (off_t) toread;
		}

		/* Actions to prepare and send message */
		sm->flags = flags;
		sm->recno = recno;
		sm->datalen = toread + offset;

		if (sm->datalen <= 0)
			ereport(ERROR,
					(errmsg("%s", "Attempted to send zero length message"),
					 errdetail("transaction "UNI_LLU, sm->recno)));

		/* Flush after reading the complete transaction */ 
		MCPSendMsg(sm, read == txsize);

		elog(DEBUG3, "%s", MCPMsgAsString("Send", sm));

		/* Determine the length of data to read */
		offset = 0;
		toread = Min(txsize - read, (off_t) MCP_MAX_MSG_SIZE);
	}
	
	pfree(sm);
	MCPQueueTxClose(q);
}

/*
 * Receive a transaction from the peer.  The first message from the transaction
 * is returned.
 *
 * It is not expected that different processes call this function
 * simultaneously.
 *
 * To execute some actions depending on the transaction's tablelist (e.g.
 * setting flag to request a full dump), pass a function as a tablelist_hook.
 */
MCPMsg *
ReceiveQueueTransaction(MCPQueue *q, 
						void (*tablelist_hook)(List *, TxDataHeader *hdr, 
											   ullong recno, void *), 
						void *tablelist_arg,
						bool (*message_hook)(bool, MCPMsg *, void *), 
						void *message_arg)
{
	MCPMsg 			*msg;
	TxDataHeader	*hdr;
	ullong			txrecno;
	off_t			txsize;
	off_t			written;

	/* Make sure we don't have active transactions */
	Assert(MCPQueueGetDatafile(q) == NULL);
	
	msg = MCPRecvMsg();
	MCPMsgPrint(DEBUG3, "Recv", msg);

	/* Get recno for the coming transaction */
	txrecno = msg->recno;

	/* 
	 * Sanity checks. Either we should receive a record equal to the first
	 * recno if an empty queue or a record that goes next to the queue tail
	 */
#ifdef USE_ASSERT_CHECKING
	if (assert_enabled && txrecno != InvalidRecno)
	{
		LockReplicationQueue(q, LW_SHARED);
		Assert(txrecno > MCPQueueGetLastRecno(q) || 
			   (MCPQueueIsEmpty(q) && txrecno == MCPQueueGetInitialRecno(q)));
		UnlockReplicationQueue(q);
	}
#endif

	/* Read data header from the start of the first message data */
	if (txrecno != InvalidRecno)
	{
		hdr = (TxDataHeader *) msg->data;

		txsize = hdr->dh_len;

		if (txsize < sizeof(TxDataHeader))
			ereport(ERROR,
					(errmsg("Queue transaction size is invalid"),
					 errdetail("Transaction "UNI_LLU", size "UINT64_FORMAT,
							   txrecno, (ullong) txsize)));
	}
	else
	{
		/* Silence compiler warnings */
		hdr = NULL;
		txsize = (off_t) 0;
	}

	/* Call a pre-commit message hook if supplied by a caller */
	if (message_hook != NULL)
	{
		bool	skip;
		
		/* 
		 * If message hook returns false then a transaction shouldn't be
		 * placed in a queue.
		 */
		skip = message_hook(false, msg, message_arg);
		if (skip)
			goto finish;
	}

	/* InvalidRecno is used to mark non-queue messages */
	if (msg->recno == InvalidRecno)
		goto finish;

	if (msg->datalen <= 0)
		ereport(ERROR,
				(errmsg("zero length message received"),
				 errdetail("transaction "UNI_LLU, msg->recno)));

	/* Open transaction file */
	MCPQueueTxOpen(q, txrecno);

	/* Put data from the first message into the queue */
	MCPQueuePutData(q, msg->data, msg->datalen);
	written = (off_t) msg->datalen;

	/* Read further messages from current transaction */
	while (written < txsize)
	{
		MCPMsg *rm = MCPRecvMsg();

		MCPMsgPrint(DEBUG3, "Recv", rm);

		if (msg->datalen == 0)
			ereport(ERROR,
					(errmsg("zero length message received"),
					 errdetail("transaction "UNI_LLU, msg->recno)));

		Assert(rm->recno == txrecno);
		MCPQueuePutData(q, rm->data, rm->datalen);
		written += (off_t) rm->datalen;

		MCPReleaseMsg(rm);
	}

	/* Call tablelist hook if needed */
	if (tablelist_hook != NULL)
	{
		List	*tablist;

		tablist = TxReadTableList(q, txrecno, hdr);

		if (tablist != NIL)
		{
			/* Call tablelist hook */
			tablelist_hook(tablist, hdr, txrecno, tablelist_arg);

			list_free_deep(tablist);
		}
	}

	/* Close current transaction and register it in the queue */
	MCPQueueTxClose(q);

	LockReplicationQueue(q, LW_EXCLUSIVE);
	MCPQueueSetLastRecno(q, txrecno);
	MCPQueueSetEnqueueTimestamp(q);
	UnlockReplicationQueue(q);

	/* Call a post-commit message hook */
	if (message_hook != false)
		message_hook(true, msg, message_arg);

finish:
	return msg;
}

/*
 * Read tablelist of current transaction. Caller is responsible for
 * releasing memory occupied by the resulting list. Transaction data
 * file should be opened at the moment of calling this function. 
 */
static List *
TxReadTableList(MCPQueue *q, ullong recno, TxDataHeader *hdr)
{
	char	sign[2];
	int		total,
			i;
	List   *tablist;
	MCPFile *file = MCPQueueGetDatafile(q);

	Assert(file != NULL);

	/* Return if transaction doesn't contain any tables (e.g. promote) */
	if (hdr->dh_listoffset == (off_t) 0)	
		return NIL;

	/* Seek to the start of the tablelist data */
	MCPFileSeek(file, hdr->dh_listoffset, SEEK_SET);

	/* Read initial guardian word */
	MCPQueueReadData(q, sign, 2, false);
	if (memcmp(sign, "TS", 2) != 0)
		ereport(ERROR,
				(errmsg("tablelist start marker missing"),
				 errdetail("transaction "UNI_LLU" offset "UINT64_FORMAT,
						   recno, (ullong) hdr->dh_listoffset)));

	/* read number of tables in list */
	MCPQueueReadData(q, &total, sizeof(uint32), false);
	Assert(total > 0);

	/* initially, the list is empty */
	tablist = NIL;

	/* Read table names and raise_dump flags.  See MCPLocalQueueFinalize */
	for (i = 0; i < total; i++)
	{
		BackendTable	tab;

		/* Read a single table from the queue */
		tab = (BackendTable) ReadTable(file);

		/* link the new table into the list */
		tablist = lappend(tablist, tab);
	}

	/* Check final guardian word */
	MCPQueueReadData(q, sign, 2, false);
	if (memcmp(sign, "TE", 2) != 0)
		ereport(ERROR,
				(errmsg("tablelist end marker missing"),
				 errdetail("transaction "UNI_LLU" offset %ld",
						   recno, (long) MCPFileSeek(file, 0, SEEK_CUR))));

	return tablist;
}
