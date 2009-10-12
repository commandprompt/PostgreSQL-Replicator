/*
 * txlog.h
 *
 * Mammoth Replicator transaction log.
 *
 * $Id: txlog.h 2093 2009-04-09 19:05:33Z alvherre $
 */

#ifndef TXLOG_H
#define TXLOG_H

#define NUM_TXLOG_BUFFERS 8

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

#endif /* TXLOG_H */
