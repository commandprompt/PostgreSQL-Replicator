/*-----------------------
 * mcp_connection.h
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: mcp_connection.h 2092 2009-04-08 17:32:24Z alexk $
 * ------------------------
 */
#ifndef MCP_CONNECTION_H
#define MCP_CONNECTION_H

#include "libpq/libpq-be.h"

#define MAMMOTH_PROTOCOL_VERSION	7237

#define MCP_TYPE_MASTER 'M'
#define MCP_TYPE_SLAVE  'S'

#define MAX_AUTHKEY_SIZE 32

#define MCP_MAX_MSG_SIZE	(BLCKSZ * 2)

#define ActOnErrorMessage(elevel) \
	ActOnErrorMessageFn(elevel, __FILE__, __LINE__, PG_FUNCNAME_MACRO)

/* in clientconn.c */
typedef enum
{
	CLIENT_ROLE_MASTER = 0,
	CLIENT_ROLE_SLAVE = 1,
} client_role;

/*
 * Variable-length struct: the data array is actually "datalen" bytes long.
 *
 * NB -- if you change this struct, be sure to change MCPRecvMsg as well
 */
typedef struct MCPMsg
{
	uint32		datalen;
	uint32		flags;
	ullong		recno;
	char		data[1];
	/* VARIABLE LENGTH DATA FOLLOWS AT END OF STRUCT */
} MCPMsg;

/*  MCP Message flags */
/* 1 unused */
#define MCP_QUEUE_FLAG_DATA			(1<<2)	/* normal transaction data */
#define MCP_QUEUE_FLAG_TABLE_DUMP	(1<<3)	/* transaction is a table dump */
#define MCP_QUEUE_FLAG_CATALOG_DUMP	(1<<4)	/* transaction is a catalog dump */
#define MCP_QUEUE_FLAG_DUMP_START	(1<<5)	/* start of full dump */
#define MCP_QUEUE_FLAG_DUMP_END		(1<<6)	/* end of full dump */
#define MCP_QUEUE_FLAG_EMPTY		(1<<7)	/* transaction is empty */
#define MCP_QUEUE_FLAG_FULL_DUMP	(1<<8)	/* part of a full dump */
/* 8 unused */
/* 9 unused */
#define MCP_QUEUE_FLAG_TRUNC		(1<<10)	/* truncate the MCP queue */
#define MCP_MSG_FLAG_AUTH			(1<<11)	/* Auth-related message */
#define MCP_MSG_FLAG_ECHO			(1<<12)	/* mcp<->master: keepalive */
#define MCP_MSG_FLAG_REQTABLE		(1<<13)	/* dump request for a single table */
#define MCP_MSG_FLAG_REQFULL		(1<<14)	/* Full dump request */
#define MCP_MSG_FLAG_ACK			(1<<16)	/* ACK previous message */
/* 17 unused */
/* 18 unused */

#define MCP_MSG_FLAG_TABLE_LIST		(1<<19)	/* table list element */
#define MCP_MSG_FLAG_TABLE_LIST_END (1<<20)	/* end of table list */
/* 21 unused */
#define MCP_MSG_FLAG_PROMOTE		(1<<22)	/* slave->mcp: promotion request */
											/* mcp->master: promotion request */
#define MCP_MSG_FLAG_PROMOTE_READY	(1<<23)	/* master->mcp: promotion accepted */
#define MCP_MSG_FLAG_PROMOTE_MAKE	(1<<24)	/* mcp->master: promotion accepted */
#define MCP_MSG_FLAG_PROMOTE_FORCE	(1<<25)	/* mcp->master: promotion force request */
#define MCP_MSG_FLAG_PROMOTE_NOTIFY	(1<<26)	/* slave->mcp: user requested promotion */
#define MCP_MSG_FLAG_PROMOTE_BACK	(1<<27)	/* slave->mcp: user requested back promotion */
#define MCP_MSG_FLAG_PROMOTE_SLAVE_READY (1<<28) /* slave->mcp: ready to promote */
#define MCP_MSG_FLAG_PROMOTE_CANCEL	(1<<29)	/* inform a peer about promote cancellation */
#define MCP_MSG_FLAG_STAT (1<<30) /* MCP -> mcp_stat data */
/* 30 unused */
/* 31 unused */

#define MessageTypeIsDump(flags) (((flags) & MCP_QUEUE_FLAG_TABLE_DUMP) || \
								((flags) & MCP_QUEUE_FLAG_CATALOG_DUMP))
#define MessageTypeBelongsToFullDump(flags) ((flags) & MCP_QUEUE_FLAG_FULL_DUMP)
#define MessageTypeIsData(flags) ((flags) & MCP_QUEUE_FLAG_DATA)


/* mcp_connection.c (server routines) */
extern void MCPConnectionInit(Port *port);
extern bool MCPMsgAvailable(void);
extern MCPMsg *MCPRecvMsg(void);
extern void	MCPSendMsg(MCPMsg *msg, bool flush);
extern void MCPFlush(void);
extern ullong MCPRecvInitialRecno(void);
extern void MCPSendInitialRecno(ullong initial_recno);
extern void	MCPReleaseMsg(MCPMsg *msg);
extern void	MCPMsgPrint(int lev, char *prefix, MCPMsg *m);
extern char *MCPMsgAsString(char *prefix, MCPMsg *m);
extern void ActOnErrorMessageFn(int elevel, const char *filename, int lineno,
								const char *funcname);
extern int mcpWaitTimed(Port *port, int forRead, int forWrite,
                        time_t finish_time);

/* clientconn.c (client routines) */
extern int		WaitOnMcpConnection(Port *port, int timeout, bool for_read,
									bool for_write);
extern Port	   *OpenMcpConnection(char *name, char *host, int port);
extern int		SendMcpStartupPacket(Port *port, bool ssl, client_role role,
									 int slave_no);
extern int 		McpAuthenticateToServer(uint64 sysid, char *authkey, 
										char *encoding);

#endif /* MCP_CONNECTION_H */
