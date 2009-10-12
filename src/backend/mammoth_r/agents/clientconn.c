/*
 * clientconn.c
 *		Mammoth Replicator backend-side code to connect to MCP server
 * 
 * This code is used to establish a connection, send the initial packet and
 * do the authentication.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: clientconn.c 2092 2009-04-08 17:32:24Z alexk $
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#include <time.h>

#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "mammoth_r/mcp_connection.h"
#include "miscadmin.h"
#include "utils/guc.h"


/*
 * Open a connection to the given forwarder.  If the connection cannot be
 * established, raises an error.
 */
Port *
OpenMcpConnection(char *name, char *host, int prt)
{
	Port	*port;
	struct addrinfo hint;
	struct addrinfo *addrs;
	struct addrinfo *addr_cur;
	char	portstr[16];
	int		ret;
	char   *edetail = NULL;

	ereport(LOG,
			(errmsg("attempting connection to forwarder \"%s\" (%s:%d)", name, host, prt)));

	/* initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_NUMERICHOST;

	/* Use pg_getaddrinfo_all() to resolve the address */
	sprintf(portstr, "%d", prt);
	ret = pg_getaddrinfo_all(host, portstr, &hint, &addrs);
	if (ret || !addrs)
	{
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		ereport(ERROR,
				(errmsg("could not translate host name \"%s\" to address: %s",
						host, gai_strerror(ret)),
				 errhint("Only numeric IP addresses are supported.")));
	}
	addr_cur = addrs;

	port = palloc0(sizeof(Port));

	while (addr_cur != NULL)
	{
		int		on = 1;

		/* remember current address for possible error messages */
		memcpy(&port->raddr.addr, addr_cur->ai_addr, addr_cur->ai_addrlen);
		port->raddr.salen = addr_cur->ai_addrlen;

		/* open a socket */
		port->sock = socket(addr_cur->ai_family, SOCK_STREAM, 0);
		if (port->sock < 0)
		{
			edetail = _("could not create socket");
			addr_cur = addr_cur->ai_next;
			continue;
		}

		if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY, (char *) &on,
					   sizeof(on)) < 0)
		{
			edetail = _("could not set socket to TCP no delay mode");
			closesocket(port->sock);
			port->sock = -1;
			addr_cur = addr_cur->ai_next;
			continue;
		}
		if (fcntl(port->sock, F_SETFD, FD_CLOEXEC) == -1)
		{
			edetail = _("could not set socket to close-on-exec mode");
			closesocket(port->sock);
			port->sock = -1;
			addr_cur = addr_cur->ai_next;
			continue;
		}

		/* FIXME this could block indefinitely */
		if (connect(port->sock, addr_cur->ai_addr, addr_cur->ai_addrlen) < 0)
		{
			edetail = _("could not connect to server");
			closesocket(port->sock);
			port->sock = -1;
			addr_cur = addr_cur->ai_next;
			continue;
		}

		/* fill in the client address */
		port->laddr.salen = sizeof(port->laddr.addr);
		if (getsockname(port->sock,
						(struct sockaddr *) &port->laddr.addr,
						&port->laddr.salen) < 0)
		{
			edetail = _("could not get client address from socket");
			closesocket(port->sock);
			port->sock = -1;
			addr_cur = addr_cur->ai_next;
			continue;
		}

		/* if we're here, we have a good socket */
		break;
	}

	if (port->sock == -1)
	{
		pfree(port);
		if (edetail == NULL)
			edetail = _("unexpected error");
		ereport(ERROR,
				(errmsg("%s: %m", _(edetail))));
	}
	
	/* set keepalive-idle settings for this connection */
	(void) pq_setkeepalivesidle(tcp_keepalives_idle, port);
	(void) pq_setkeepalivesinterval(tcp_keepalives_interval, port);
	(void) pq_setkeepalivescount(tcp_keepalives_count, port);

	/* all OK */
	return port;
}

int
SendMcpStartupPacket(Port *port, bool ssl, client_role role, int slave_no)
{
	StringInfoData	buf;

	pq_beginmessage(&buf, '\0');
	pq_sendbyte(&buf, 'M');
	pq_sendbyte(&buf, 'R');
	pq_sendint(&buf, MAMMOTH_PROTOCOL_VERSION, 2);

#ifndef USE_SSL
	if (ssl)
		elog(FATAL, "SSL mode requested but this installation does not support it");
#endif

#ifdef USE_SSL
	/* enter SSL mode if required */
	if (ssl)
	{
		int		ret;
		char	byte;

		pq_sendbyte(&buf, 'S');
		pq_endmessage(&buf);
		ret = pq_flush();
		if (ret)
			return ret;

		byte = pq_getbyte();
		if (byte == 'J')
		{
			/* this message should be empty -- merely getting it means we're OK */
			initStringInfo(&buf);
			pq_getmessage(&buf, 0);
			pq_getmsgend(&buf);
		}
		else if (byte == 'E')
			ActOnErrorMessage(ERROR);

		/* do the SSL magic */
		if (secure_open_client(port) == -1)
			elog(ERROR, "could not establish SSL connection");

		/* and finally send the new startup packet */
		return SendMcpStartupPacket(port, false, role, slave_no);
	}
	else
#endif /* USE_SSL */
		pq_sendbyte(&buf, 'N');		/* SSL not desired or not supported */
	pq_sendbyte(&buf, role == (CLIENT_ROLE_MASTER) ? MCP_TYPE_MASTER : 
				MCP_TYPE_SLAVE);
	if (role == CLIENT_ROLE_SLAVE)
		pq_sendint(&buf, slave_no, 2);

	pq_endmessage(&buf);

	/* total packet length: 8 bytes if slave, 6 bytes if master */

	return pq_flush();
}

int
McpAuthenticateToServer(uint64 nodeid, char *authkey, char *encoding)
{
	StringInfoData	msg;
	StringInfoData	authkey_str,
					encoding_str;
	char			byte;

	pq_beginmessage(&msg, '\0');
	pq_sendint64(&msg, nodeid);	/* sysid */
	pq_sendint(&msg, 2, 4);		/* number of key=value pairs we're gonna send */

	initStringInfo(&authkey_str);
	appendStringInfo(&authkey_str, "%s=%s", "authkey", authkey);
	pq_sendstring(&msg, authkey_str.data);	/* BEWARE of encoding conversion */
	pfree(authkey_str.data);
	initStringInfo(&encoding_str);
	appendStringInfo(&encoding_str, "%s=%s", "encoding", encoding);
	pq_sendstring(&msg, encoding_str.data);
	pfree(encoding_str.data);
	pq_endmessage(&msg);

	if (pq_flush() == EOF)
		elog(ERROR, "unable to send authentication packet");

	mcpWaitTimed(MyProcPort, true, false, time(NULL) + 60);

	byte = pq_getbyte();
	if (byte == EOF)
		elog(ERROR, "got EOF byte");
	if (byte == 'J')	/* message type jack */
	{
		initStringInfo(&msg);

		pq_getmessage(&msg, 0);
		/* just getting an empty message here means we're OK */
		pq_getmsgend(&msg);

		return STATUS_OK;
	}
	else if (byte == 'E')	/* message type error */
		ActOnErrorMessage(ERROR);
	else
		elog(LOG, "got unrecognized byte %d", byte);

	return STATUS_ERROR;
}
