/*
 * forwarder.h
 * 		Replicator Forwarder configuration declarations
 *
 * $Id$
 */
#ifndef FORWARDER_H
#define FORWARDER_H

#include "libpq/libpq-be.h"
#include "mammoth_r/mcp_queue.h"
#include "mammoth_r/mcp_hosts.h"

/* GUC variables */

extern bool ForwarderEnable;
extern bool ForwarderRequireSSL;
extern bool ForwarderACLenable;
extern int ForwarderPortNumber;
extern int ForwarderEchoTimeout;
extern int ForwarderDumpCacheMaxSize;
extern int ForwarderOptimizerRounds;
extern int ForwarderOptimizerNaptime;
extern char *ForwarderListenAddresses;
extern char *ForwarderSlaveAddresses;
extern char *ForwarderMasterAddress;
extern char *ForwarderDataPath;
extern char *ForwarderPromoteACL;
extern char *ForwarderAuthKey;

extern MCPQueue    *ForwarderQueue;
extern MCPHosts    *ForwarderHosts;

/* forwarder stuff */
extern void ForwarderInitialize(void);
extern void forwarder_reset_shared(void);
extern int MCPServerHandleConnection(Port *port);

/* forwarder helper stuff */
extern void ForwarderHelperMain(int argc, char *argv);
extern void forwarder_helper_stopped(void);

#endif /* FORWARDER_H */
