/*-----------------------
 * mcp_queue.h
 * 		MCP Queue header definitions
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_queue.h 2109 2009-04-23 16:46:20Z alvherre $
 * ------------------------
 */
#ifndef MCP_QUEUE_H
#define MCP_QUEUE_H

#include "mammoth_r/mcp_file.h"
#include "storage/lwlock.h"


#define InvalidRecno	(UINT64CONST(0))

typedef enum
{
	MCPQUnsynced,
	MCPQSynced
} MCPQSync;

#define	MCPQSyncAsString(sync) \
	((sync == MCPQUnsynced) ? "Unsynced" : \
	(sync == MCPQSynced) ? "Synced" : \
	"Unknown Sync Value")

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

typedef struct
{
	uint16		dh_id;			/* Must be equal to DATA_HEADER_ID */
	uint32		dh_flags;		/* per-transaction flags, like dump/data etc. */
	off_t		dh_listoffset;	/* starting offset of the tablelist data */
	off_t		dh_len;			/* length of transaction data, incl. header */
} TxDataHeader;

#define DATA_HEADER_ID 0x5458
 
/* struct definition appears in mcp_queue.c */
typedef struct MCPQueue MCPQueue;

/* queue initialization, open and close functions */
extern void		BootStrapMCPQueue(void);
extern Size		MCPQueueShmemSize(void);
extern void		MCPQueueShmemInit(void);
extern MCPQueue *MCPQueueInit(bool forwarder);
extern void		MCPQueueDestroy(MCPQueue *q);

/* queue read-write functions */
extern void		MCPQueueReadDataHeader(MCPQueue *q, TxDataHeader *hdr);
extern bool		MCPQueueReadData(MCPQueue *q, void  *buf, ssize_t len,
								 bool eof_allowed);
extern void		MCPQueuePutData(MCPQueue *q, void *data, ssize_t len);

/* queue optimization functions */
extern ssize_t	MCPQueueCalculateSize(MCPQueue *q, ullong recno);
extern void		MCPQueuePrune(MCPQueue *q);
extern void		MCPQueueCleanup(MCPQueue *q, ullong recno);

/* queue status, get and set */
extern MCPFile *MCPQueueGetDatafile(MCPQueue *q);
extern void		MCPQueueSetFirstRecno(MCPQueue *q, ullong recno);
extern ullong	MCPQueueGetFirstRecno(MCPQueue *q);
extern void 	MCPQueueSetLastRecno(MCPQueue *q, ullong lrecno);
extern ullong	MCPQueueGetLastRecno(MCPQueue *q);
extern ullong	MCPQueueGetAckRecno(MCPQueue *q);
extern void 	MCPQueueSetAckRecno(MCPQueue *q, ullong vrecno);
extern ullong	MCPQueueGetInitialRecno(MCPQueue *q);
extern void 	MCPQueueSetInitialRecno(MCPQueue *q, ullong brecno);
extern bool		MCPQueueIsEmpty(MCPQueue *q);
extern MCPQSync	MCPQueueGetSync(MCPQueue *q);
extern void		MCPQueueSetSync(MCPQueue *q, MCPQSync sync);
extern time_t 	MCPQueueGetEnqueueTimestamp(MCPQueue *q);
extern void 	MCPQueueSetEnqueueTimestamp(MCPQueue *q);
extern time_t 	MCPQueueGetDequeueTimestamp(MCPQueue *q);
extern void 	MCPQueueSetDequeueTimestamp(MCPQueue *q);

/* misc functions */
extern void		MCPQueueLogHdrStatus(int elevel, MCPQueue *queue, char *prefix);
extern char	   *MCPQueueGetLocalFilename(MCPQueue *q, char *base);

/* functions dealing with invidual transactions */
extern void		MCPQueueNextTx(MCPQueue *q);
extern void		MCPQueueTxOpen(MCPQueue *q, ullong recno);
extern void		MCPQueueTxClose(MCPQueue *q);
extern void		MCPQueueTxCommit(MCPQueue *q, ullong recno);
extern ullong	MCPQueueCommit(MCPQueue *q, MCPFile *txdata, ullong recno);

/* functions to lock/unlock the queue */
extern void 	LockReplicationQueue(MCPQueue *q, LWLockMode mode);
extern void 	UnlockReplicationQueue(MCPQueue *q);
extern bool 	QueueLockHeldByMe(MCPQueue *q);

#endif   /* MCP_QUEUE_H */
