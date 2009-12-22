/*
 * mcp_auth.c
 * 		The Mammoth Control Processor (MCP) authentication routines
 *
 * $Id: mcp_auth.c 2117 2009-04-30 22:32:24Z alvherre $
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "mammoth_r/forwarder.h"
#include "mammoth_r/mcp_hosts.h"
#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_promotion.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"


/*
 * Wait for and receive the startup packet from the client connection.
 *
 * This routine is in charge of:
 * o  Setting up SSL mode as required by configuration.
 * o  Checking whether the slave number is within implementation limits (an
 *    error is raised if it isn't).
 * o  Checking canAcceptConnection state, and close the connection with a
 *    suitable error message if not.
 *
 * After this routine returns, the socket is already using SSL (or not).
 *
 * "mode" is an output parameter for signalling whether this is slave or
 * master.  In the former case, "slave_no" is the slave number received;
 * otherwise it is not touched.
 *
 * If the initial packet cannot be received in 30 seconds, raise an error.
 */
int
ProcessMcpStartupPacket(Port *port, bool require_ssl, char *mode, int *slave_no)
{
	StringInfoData	msg;
	char		byte;
	int			version;
	char		ssl;
	int			ret;
#ifdef USE_SSL
	bool		ssl_desired = false;
#endif

	/* wait at most 30 seconds to get the startup packet */
	ret = mcpWaitTimed(port, true, false, time(NULL) + 30);
		
	if (ret == EOF)
		elog(ERROR, "timeout expired");

	initStringInfo(&msg);
	pq_getmessage(&msg, 0);
	byte = pq_getmsgbyte(&msg);
	if (byte != 'M')
		elog(ERROR, "expected '%c', got '%c'", 'M', byte);
	byte = pq_getmsgbyte(&msg);
	if (byte != 'R')
		elog(ERROR, "expected '%c', got '%c'", 'R', byte);

	version = pq_getmsgint(&msg, 2);
	if (version != MAMMOTH_PROTOCOL_VERSION)
		elog(ERROR, "unsupported protocol version %d, expected %d",
			 version, MAMMOTH_PROTOCOL_VERSION);

	ssl = pq_getmsgbyte(&msg);
	if (ssl == 'S')
	{
		if (require_ssl)
		{
			/*
			 * message ends here -- do SSL initialization and receive the
			 * second startup packet, which contains the rest of the
			 * stuff we're supposed to set up.
			 */
			pq_getmsgend(&msg);

#ifdef USE_SSL
			ssl_desired = true;
#else
			elog(ERROR, "we do not talk SSL");
#endif
		}
		else
			elog(ERROR, "SSL mode requested but not allowed");
	}
	else if (ssl != 'N')
		elog(ERROR, "unrecognized SSL mode byte %d", ssl);

#ifdef USE_SSL
	if (ssl_desired)
	{
		StringInfoData	msg;

		pq_beginmessage(&msg, 'J');	/* an empty message of type 'J'ack */
		pq_endmessage(&msg);
		if (pq_flush())
			elog(ERROR, "error sending J message");
		if (secure_open_server(port) == -1)
			elog(ERROR, "SSL connection not established");
		else
			return ProcessMcpStartupPacket(port, false, mode, slave_no);
	}
#endif

	*mode = pq_getmsgbyte(&msg);
	if (*mode != MCP_TYPE_MASTER && 
		*mode != MCP_TYPE_SLAVE)
		elog(ERROR, "unsupported mode byte %c", *mode);

	if (*mode == MCP_TYPE_SLAVE)
		*slave_no = pq_getmsgint(&msg, 2);

	if (*mode == MCP_TYPE_SLAVE &&
		(*slave_no < 0 || *slave_no >= MCP_MAX_SLAVES))
		elog(ERROR, "unsupported slave no");

	pq_getmsgend(&msg);
	pfree(msg.data);

	/*
	 * If we're going to reject the connection due to database state, say so
	 * now instead of wasting cycles on an authentication exchange. (This also
	 * allows a pg_ping utility to be written.)
	 */
	switch (port->canAcceptConnections)
	{
		case CAC_STARTUP:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errmsg("the database system is starting up")));
			break;
		case CAC_SHUTDOWN:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errmsg("the database system is shutting down")));
			break;
		case CAC_RECOVERY:
			ereport(FATAL,
					(errcode(ERRCODE_CANNOT_CONNECT_NOW),
					 errmsg("the database system is in recovery mode")));
			break;
		case CAC_TOOMANY:
			ereport(FATAL,
					(errcode(ERRCODE_TOO_MANY_CONNECTIONS),
					 errmsg("sorry, too many clients already")));
			break;
		case CAC_WAITBACKUP:
			/* OK for now, will check in InitPostgres */
			break;
		case CAC_OK:
			break;
	}

	return STATUS_OK;
}

/*
 * Receive the authentication packet, and send the ack message.
 */
int
McpReceiveClientAuth(uint64 *nodeid, pg_enc *peer_encoding)
{
	StringInfoData	msg;
	char	*authkey,
			*encoding;
	int		total;

	initStringInfo(&msg);
	pq_getmessage(&msg, 0);
	*nodeid = pq_getmsgint64(&msg);
	total = pq_getmsgint(&msg, 4);

	authkey = encoding = NULL;

	/* look for authkey inside the incoming packet */
	for (; total > 0; total--)
	{
		const char	*key;
		char	*sep;
		char	*val;

		key = pq_getmsgstring(&msg);	/* XXX may leak memory */

		sep = strchr(key, '=');
		val = sep + 1;
		sep[0] = '\0';
		if (strcmp(key, "authkey") == 0)
		{
			authkey = strdup(val);
			if (authkey == NULL)
				elog(ERROR, "out of memory");
		}
		if (strcmp(key, "encoding") == 0)
		{
			encoding = strdup(val);
			if (encoding == NULL)
				elog(ERROR, "out of memory");
		}
	}

	pq_getmsgend(&msg);
	pfree(msg.data);

	if (authkey == NULL)
		elog(ERROR, "authkey was not found in the authentication packet");

	if (strcmp(authkey, ForwarderAuthKey) != 0)
		elog(ERROR, "invalid authkey");

	if (encoding == NULL)
		elog(ERROR, "peer database encoding was not found in the authentication packet");
		
	*peer_encoding = pg_char_to_encoding(encoding);
	
	if (!PG_VALID_BE_ENCODING(*peer_encoding))
		elog(ERROR, "\"%s\" is not a valid backend encoding", encoding);
		
	/* release unused memory */
	free(authkey);
	free(encoding);
	
	pq_beginmessage(&msg, 'J');	/* empty message of type 'J'ack */
	pq_endmessage(&msg);
	return pq_flush();
}

/*
 * MCPMasterConnectionAllowed
 *
 * Check if master ip address matches the legal master address in
 * mcp_server.conf.
 */
int
MCPMasterConnectionAllowed(SockAddr *remote_addr, bool check_promoted)
{
	int		ret;
	char	remote_ip[NI_MAXHOST];

	/* if ACL checks are disabled - always return success */
	if (ForwarderACLenable == 0)
		return STATUS_OK;

	if (ForwarderMasterAddress == NULL)
		return STATUS_ERROR;

	ret = pg_getnameinfo_all(&remote_addr->addr, remote_addr->salen,
							 remote_ip, sizeof(remote_ip),
							 NULL, 0,
							 NI_NUMERICHOST | NI_NUMERICSERV);
	if (ret)
	{
		ereport(WARNING,
				(errmsg_internal("pg_getnameinfo_all() failed: %s",
								 gai_strerror(ret))));
		return STATUS_ERROR;
	}

	/* Check remote_ip against ForwarderMasterAddress config parameter */
	if (strcmp(remote_ip, ForwarderMasterAddress) == 0)
		return STATUS_OK;

	if (check_promoted)
	{
		ListCell *c;

		foreach(c, ParsedForwarderPromoteAcl)
		{
			int slaveno = lfirst_int(c);

			if (MCPSlaveConnectionAllowed(remote_addr, slaveno) == STATUS_OK)
			{
				elog(LOG, "accepted promoted slave ip address");
				return STATUS_OK;
			}
		}
	}
	return STATUS_ERROR;
}


/*
 * MCPSlaveConnectionAllowed
 *
 * Check if slave's ip address matches the legal master address in
 * mcp_server.conf
 */
int
MCPSlaveConnectionAllowed(SockAddr *remote_addr, int hostno)
{
	int		result = STATUS_ERROR,
			ret;
	char   *rawstring,
		   remote_ip[NI_MAXHOST];

	List   *elemlist;
	ListCell *c;

	/* if ACL checks are disabled - always return success */
	if (ForwarderACLenable == 0)
		return STATUS_OK;

	if (ForwarderSlaveAddresses == NULL)
		return STATUS_ERROR;

	ret = pg_getnameinfo_all(&remote_addr->addr, remote_addr->salen,
							 remote_ip, sizeof(remote_ip),
							 NULL, 0,
							 NI_NUMERICHOST | NI_NUMERICSERV);
	if (ret)
	{
		ereport(WARNING,
				(errmsg_internal("pg_getnameinfo_all() failed: %s",
								 gai_strerror(ret))));
		return STATUS_ERROR;
	}
	
	rawstring = pstrdup(ForwarderSlaveAddresses);
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
		ereport(ERROR, (errmsg("Invalid list syntax for \"forwarder_slave_addresses\"")));

	foreach(c, elemlist)
	{
		int slaveno;
		char *endptr;
		char *address_item = lfirst(c);

		/* parse number:address pair, first get the slave number */
		slaveno = strtol(address_item, &endptr, 10);

		if (endptr != address_item && slaveno == hostno)
		{
			/* If no errors and the slave number matches hostno
			 * skip space characters until the we found '\0' or digit
			 */
			while (isspace(*endptr) || (*endptr == ':'))
				endptr++;

			if (*endptr != '\0')
			{
				/* Compare addresses */
				if (strcmp(endptr, remote_ip) == 0)
				{
					result = STATUS_OK;
					break;
				}
			}
		}
	}

	list_free(elemlist);
	pfree(rawstring);

	return result;
}
