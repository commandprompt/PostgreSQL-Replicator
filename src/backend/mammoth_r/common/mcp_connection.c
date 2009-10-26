/*-------
 * mcp_connection.c
 * 		Low-level MCP operations, abstracting socket stuff into a
 * 		message-passing interface.
 *
 * $Id: mcp_connection.c 2227 2009-09-22 07:04:32Z alexk $
 *-------
 */
#include "postgres.h"

#include <errno.h>
#include <signal.h>
#include <time.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mammoth_r/mcp_connection.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "tcop/tcopprot.h"


static int mcpSocketPoll(int sock, int forRead, int forWrite, time_t end_time);

/*
 * mcpWaitTimed: wait until we can read or write the connection socket, but not
 * past finish_time.
 *
 * If SSL enabled and used and forRead, buffered bytes short-circuit the
 * call to select().
 *
 * We also stop waiting and return if the kernel flags an exception condition
 * on the socket.  The actual error condition will be detected and reported
 * when the caller tries to read or write the socket.
*
 * If the socket is ready, return >0.  If timeout, return 0.  If an error
 * occurred, return EOF.
 *
 * finish_time = ((time_t) -1) disables the wait limit.
 *
 * If SSL is in use, the SSL buffer is checked prior to checking the socket
 * for read data directly.
 */
int
mcpWaitTimed(Port *port, int forRead, int forWrite, time_t finish_time)
{
	int			result;

	if (port->sock < 0)
		elog(ERROR, "socket not open");

#ifdef USE_SSL
	/* Check for SSL library buffering read bytes */
	if (forRead && port->ssl && SSL_pending(port->ssl) > 0)
	{
		/* short-circuit the select */
		return 1;
	}
#endif

	prepare_for_client_read();
	result = mcpSocketPoll(port->sock, forRead, forWrite, finish_time);
	client_read_ended();

	if (result < 0 && errno != EINTR)
		elog(COMMERROR, "select() failed: %m");

	return result;
}

void
MCPConnectionInit(Port *port)
{
	char		remote_host[NI_MAXHOST];
	char		remote_port[NI_MAXSERV];

	/* Save port etc. for ps status */
	MyProcPort = port;

	/* set these to empty in case they are needed before we set them up */
	port->remote_host = "";
	port->remote_port = "";

	/*
	 * Initialize libpq and enable reporting of ereport errors to the client.
	 * Must do this now because authentication uses libpq to send messages.
	 */
	pq_init();
	whereToSendOutput = DestRemote;

	/*
	 * Get the remote host name and port for logging and status display.
	 */
	remote_host[0] = '\0';
	remote_port[0] = '\0';
	if (pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
						   remote_host, sizeof(remote_host),
						   remote_port, sizeof(remote_port),
						   NI_NUMERICSERV))
	{
		int			ret;

		ret = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
								 remote_host, sizeof(remote_host),
								 remote_port, sizeof(remote_port),
								 NI_NUMERICHOST | NI_NUMERICSERV);

		if (ret)
			ereport(WARNING,
					(errmsg_internal("pg_getnameinfo_all() failed: %s",
									 gai_strerror(ret))));
	}

	if (Log_connections)
		ereport(LOG,
				(errmsg("forwarder connection received: host=%s%s%s",
						remote_host, remote_port[0] ? " port=" : "",
						remote_port)));

	/*
	 * save remote_host and remote_port in port stucture
	 */
	port->remote_host = strdup(remote_host);
	port->remote_port = strdup(remote_port);
}

/*
 * Put a message in the buffer, and possibly flush the buffer.
 */
void
MCPSendMsg(MCPMsg *msg, bool flush)
{
	if (pq_putmessage('M', (char *) msg, sizeof(MCPMsg) + msg->datalen - 1) == EOF)
		ereport(ERROR, 
				(errcode(ERRCODE_CONNECTION_FAILURE), 
				(errmsg("connection closed"))));

	if (flush)
		MCPFlush();
}

void
MCPFlush(void)
{
	if (pq_flush() == EOF)
		ereport(ERROR, 
				(errcode(ERRCODE_CONNECTION_FAILURE), 
				(errmsg("connection closed"))));
}

bool
MCPMsgAvailable(void)
{
	return pq_arebytes();
}

/*
 * The whole message is allocated in single chunk (including data)
 */
MCPMsg *
MCPRecvMsg(void)
{
	char	byte;

	byte = pq_getbyte();
	if (byte == EOF)
		ereport(ERROR, 
				(errcode(ERRCODE_CONNECTION_FAILURE), 
				(errmsg("connection closed"))));
	else if (byte == 'M')
	{
		StringInfoData	s;

		initStringInfo(&s);
		if (pq_getmessage(&s, 0) == EOF)
			ereport(ERROR, 
					(errcode(ERRCODE_CONNECTION_FAILURE), 
					(errmsg("connection closed"))));

		return (MCPMsg *) s.data;
	}
	else if (byte == 'E')
		ActOnErrorMessage(ERROR);
	else
		elog(ERROR, "unrecognized message type '%c'", byte);

	return NULL;	/* keep compiler quiet */
}

/* Receive a message of type "initial recno" */
ullong
MCPRecvInitialRecno(void)
{
	StringInfoData	msg;
	ullong		recno;
	char		byte;

	byte = pq_getbyte();
	if (byte == EOF)
		ereport(ERROR, 
				(errcode(ERRCODE_CONNECTION_FAILURE), 
				(errmsg("connection closed"))));
	else if (byte == 'E')
		ActOnErrorMessage(ERROR);
	else if (byte != 'I')
		elog(ERROR, "expected message type 'I', got %d instead", byte);

	initStringInfo(&msg);

	pq_getmessage(&msg, 0);
	recno = pq_getmsgint64(&msg);
	pq_getmsgend(&msg);
	pfree(msg.data);

	return recno;
}

/* Send an "initial recno" message */
void
MCPSendInitialRecno(ullong initial_recno)
{
	StringInfoData	msg;

	pq_beginmessage(&msg, 'I');
	pq_sendint64(&msg, initial_recno);
	pq_endmessage(&msg);

	MCPFlush();
}

/* 
 * Actions performed when receiving error type message. Commonly they are
 * get the error message and ereport it with ERROR errorlevel.
 */
void
ActOnErrorMessageFn(int elevel, const char *filename, int lineno,
					const char *funcname)
{
	StringInfoData 	msg;
	char 	*errormsg = NULL;
	char	*errordetail = NULL;
	int		errorcode = -1;

	initStringInfo(&msg);

	pq_getmessage(&msg, 0);
	do
	{
		char	field = pq_getmsgbyte(&msg);
		char   *recvs;

		if (field == '\0')
			break;

		recvs = (char *) pq_getmsgstring(&msg);

		switch (field)
		{
			case PG_DIAG_SQLSTATE:
				errorcode = MAKE_SQLSTATE(recvs[0], recvs[1], recvs[2], 
										  recvs[3], recvs[4]);
				break;

			case PG_DIAG_MESSAGE_PRIMARY:
				errormsg = recvs;
				break;

			case PG_DIAG_MESSAGE_DETAIL:
				errordetail = recvs;
				break;

			default:
				/* do nothing here */
				;
		}
	} while (1);
	pq_getmsgend(&msg);

	errstart(elevel, filename, lineno, funcname, PG_TEXTDOMAIN("postgres"));
	if (errormsg != NULL)
		errmsg("remote error: %s", errormsg);
	if (errordetail != NULL)
		errdetail("remote error detail: %s", errordetail);
	if (errorcode != -1)
		errcode(errorcode);
	errfinish(0);
}

void
MCPReleaseMsg(MCPMsg *msg)
{
	pfree(msg);
}

char *
MCPMsgAsString(char *prefix, MCPMsg *m)
{
	char            flag_buf[512] = "";
	char            log_str[1024] = "";

	if (m->flags & MCP_QUEUE_FLAG_TRUNC)
		strcat(flag_buf, "TRUNC ");
	if (m->flags & MCP_QUEUE_FLAG_DATA)
		strcat(flag_buf, "DATA ");
	if (m->flags & MCP_QUEUE_FLAG_DUMP_START)
		strcat(flag_buf, "DUMP START ");
	if (m->flags & MCP_QUEUE_FLAG_DUMP_END)
		strcat(flag_buf, "DUMP END ");
	if (m->flags & MCP_QUEUE_FLAG_CATALOG_DUMP)
		strcat(flag_buf, "CATALOG DUMP ");
	if (m->flags & MCP_QUEUE_FLAG_TABLE_DUMP)
		strcat(flag_buf, "TABLE DUMP ");
	if (m->flags & MCP_MSG_FLAG_REQTABLE)
		strcat(flag_buf, "REQTABLE ");
	if (m->flags & MCP_MSG_FLAG_REQFULL)
		strcat(flag_buf, "REQFULL ");
	if (m->flags & MCP_MSG_FLAG_ACK)
		strcat(flag_buf, "ACK ");
	if (m->flags & MCP_MSG_FLAG_TABLE_LIST_BEGIN)
		strcat(flag_buf, "TABLE_BEGIN ");
	if (m->flags & MCP_MSG_FLAG_TABLE_LIST)
		strcat(flag_buf, "TABLE_LIST ");
	if (m->flags & MCP_MSG_FLAG_TABLE_LIST_END)
		strcat(flag_buf, "TABLE_END ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_READY)
		strcat(flag_buf, "PROMOTE_READY ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_SLAVE_READY)
		strcat(flag_buf, "PROMOTE_SLAVE_READY ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_MAKE)
		strcat(flag_buf, "PROMOTE_MAKE ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE)
		strcat(flag_buf, "PROMOTE ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_FORCE)
		strcat(flag_buf, "FORCE PROMOTE ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_NOTIFY)
		strcat(flag_buf, "PROMOTE NOTIFY ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_BACK)
		strcat(flag_buf, "PROMOTE BACK ");
	if (m->flags & MCP_MSG_FLAG_PROMOTE_CANCEL)
		strcat(flag_buf, "PROMOTE CANCEL ");
	if (m->flags & MCP_MSG_FLAG_ECHO)
		strcat(flag_buf, "ECHO");

	if (*prefix != '\0')
		snprintf(log_str, sizeof(log_str),
			 	"%10s| recno: "UNI_LLU" len: %3d flags: %s",
			 	prefix, m->recno, m->datalen, flag_buf);
	else
		snprintf(log_str, sizeof(log_str),
			 	"recno: "UNI_LLU" len: %3d flags: %s",
			 	m->recno, m->datalen, flag_buf);

	return pstrdup(log_str);
}

void
MCPMsgPrint(int lev, char *prefix, MCPMsg *m)
{
	char *string = MCPMsgAsString(prefix, m);

	elog(lev, "%s", string);

	pfree(string);
}

/*
 * Check a file descriptor for read and/or write data, possibly waiting.
 * If neither forRead nor forWrite are set, immediately return a timeout
 * condition (without waiting).  Return >0 if condition is met, 0
 * if a timeout occurred, -1 if an error or interrupt occurred.
 *
 * Timeout is infinite if end_time is -1.  Timeout is immediate (no blocking)
 * if end_time is 0 (or indeed, any time before now).
 */
static int
mcpSocketPoll(int sock, int forRead, int forWrite, time_t end_time)
{
	/* We use poll(2) if available, otherwise select(2) */
#ifdef HAVE_POLL
	struct pollfd input_fd;
	int			timeout_ms;
	int			ret;

	if (!forRead && !forWrite)
		return 0;

	input_fd.fd = sock;
	input_fd.events = POLLERR;
	input_fd.revents = 0;

	if (forRead)
		input_fd.events |= POLLIN;
	if (forWrite)
		input_fd.events |= POLLOUT;

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		timeout_ms = -1;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout_ms = (end_time - now) * 1000;
		else
			timeout_ms = 0;
	}

	ret = poll(&input_fd, 1, timeout_ms);

	return ret;
#else							/* !HAVE_POLL */

	fd_set		input_mask;
	fd_set		output_mask;
	fd_set		except_mask;
	struct timeval timeout;
	struct timeval *ptr_timeout;
	int			ret;

	if (!forRead && !forWrite)
		return 0;

	FD_ZERO(&input_mask);
	FD_ZERO(&output_mask);
	FD_ZERO(&except_mask);
	if (forRead)
		FD_SET(sock, &input_mask);
	if (forWrite)
		FD_SET(sock, &output_mask);
	FD_SET(sock, &except_mask);

	/* Compute appropriate timeout interval */
	if (end_time == ((time_t) -1))
		ptr_timeout = NULL;
	else
	{
		time_t		now = time(NULL);

		if (end_time > now)
			timeout.tv_sec = end_time - now;
		else
			timeout.tv_sec = 0;
		timeout.tv_usec = 0;
		ptr_timeout = &timeout;
	}

	ret = select(sock + 1, &input_mask, &output_mask,
				  &except_mask, ptr_timeout);
	return ret;
#endif   /* HAVE_POLL */
}
