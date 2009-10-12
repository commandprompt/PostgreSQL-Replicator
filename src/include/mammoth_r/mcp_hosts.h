/*-----------------------
 * mcp_hosts.h
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_hosts.h 2186 2009-06-25 12:14:51Z alexk $
 * ------------------------
 */
#ifndef MCP_HOSTS_H
#define MCP_HOSTS_H

#include "mammoth_r/mcp_queue.h"
#include "mb/pg_wchar.h"

extern int mcp_max_slaves;

/* struct definition appears in mcp_hosts.c */
typedef struct MCPHosts MCPHosts;

extern MCPHosts *MCPHostsInit(void);
extern void 	MCPHostsShmemInit(void);
extern void		MCPHostsClose(MCPHosts *h);
extern void		MCPHostsNextTx(MCPHosts *h, MCPQueue *q, int hostno,
							   ullong last_recno);
extern MCPQSync	MCPHostsGetSync(MCPHosts *h, int hostno);
extern void		MCPHostsSetSync(MCPHosts *h, int hostno, MCPQSync sync);
extern void		MCPHostsSetFirstRecno(MCPHosts *h, int hostno,
									  ullong new_frecno);
extern ullong	MCPHostsGetFirstRecno(MCPHosts *h, int hostno);
extern void		MCPHostsSetAckedRecno(MCPHosts *h, int hostno,
									  ullong new_vrecno);
extern ullong	MCPHostsGetAckedRecno(MCPHosts *h, int hostno);
extern ullong	MCPHostsGetMinAckedRecno(MCPHosts *h, pid_t *node_pid);

extern ullong	MCPHostsGetPruningRecno(MCPHosts *h, 
										MCPQueue *q, 
										ullong vacrecno,
										ullong dump_start_recno,
										ullong dump_end_recno, 
										off_t max_queue_size,
										pid_t *node_pid);

extern void		MCPHostsCleanup(MCPHosts *h, MCPQueue *q, ullong recno);

extern void		MCPHostsLogTabStatus(int elevel, MCPHosts *h, int hostno,
									 char *prefix, pid_t *node_pid);
extern int		MCPHostsGetMaxHosts(MCPHosts *h);
extern time_t	MCPHostsGetTimestamp(MCPHosts *h, int hostno);
extern void		MCPHostLock(MCPHosts *h, int hostno, LWLockMode mode);
extern void		MCPHostUnlock(MCPHosts *h, int hostno);
extern void 	MCPHostsLockAll(MCPHosts *h, LWLockMode mode);
extern void 	MCPHostsUnlockAll(MCPHosts *h);
/* encoding-specific functions */
extern void		MCPHostsSetEncoding(MCPHosts *h, pg_enc new_encoding);
extern pg_enc 	MCPHostsGetEncoding(MCPHosts *h);

#endif /* MCP_HOSTS_H */
