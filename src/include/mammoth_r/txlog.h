/*
 * txlog.h
 *
 * Mammoth Replicator transaction log.
 *
 * $Id: txlog.h 2093 2009-04-09 19:05:33Z alvherre $
 */

#ifndef TXLOG_H
#define TXLOG_H

#include "access/xlog.h"

#define NUM_TXLOG_BUFFERS 8

/* Xlog flags */
#define RLOG_ZEROPAGE 0x00
#define RLOG_TRUNCATE 0x10
#define RLOG_COMMIT   0x20

extern void BootstrapTXLOG(void);

extern Size TXLOGShmemSize(void);
extern void TXLOGShmemInit(void);
extern void SelectActiveTxlog(bool forwarder);

extern void TXLOGStartup(ullong recno);
extern void TXLOGShutdown(void);
extern void CheckPointTXLOG(void);

extern void TXLOGTruncate(ullong oldestRecno);
extern void TXLOGZeroPageByRecno(ullong recno);

extern void TXLOGSetCommitted(ullong recno);
extern void TXLOGClearCommitted(ullong recno);
extern bool TXLOGIsCommitted(ullong recno);

extern bool	TXLOGIsForwarder(void);

/* Xlog-related functions */
extern void rlog_redo(XLogRecPtr lsn, XLogRecord *rec);
extern void rlog_desc(StringInfo buf, uint8 xl_info, char *rec);


#endif /* TXLOG_H */
