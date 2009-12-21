/*
 * mcp_proceses
 *
 * Common declaration for MCP server subprocesses.
 *
 * $Id: mcp_processes.h 2185 2009-06-23 20:46:10Z alexk $
 */
#ifndef MCP_PROCESSES_H
#define MCP_PROCESSES_H

#include "libpq/pqcomm.h"
#include "libpq/libpq-be.h"
#include "mammoth_r/mcp_api.h"
#include "mammoth_r/mcp_connection.h"
#include "mb/pg_wchar.h"
#include "mcp_hosts.h"
#include "mcp_tables.h"

/*
 * Macro to verify in shmem whether a peer is connected
 */
#define PEER_CONNECTED(x) \
	(ServerCtl->node_pid[x] != 0)

typedef enum
{
	master_check_none,
	master_check_started,
	master_check_inprogress,
	master_check_finished
} MasterCheckState;

#define MASTER_LIST_SUFFIX	"master"
#define MCP_LIST_SUFFIX		"mcp"

/* In-place promotion stack. Since it should be placed into the shared memory
 * we can't use pg list API. A simple fixed-size array stores stack elements.
 * To avoid overflow the element after the last in array is stored as at
 * position 0, and the previous to 0 element is the last element of the array.
 * The tail is always increased during push operations (with a proper wrap-around
 * to 0 after reaching max stack depth). If tail becomes equal to head after
 * push the head is moved to the position of the next element. If tail becomes
 * equal to head after pop - the stack is declared empty and head = tail = 0.
 */
#define PROMOTION_STACK_DEPTH 100
typedef struct MCPPromotionStack
{
	int		head;
	int		tail;
	bool	empty;
	int		storage[PROMOTION_STACK_DEPTH];
} MCPPromotionStack;

typedef struct MCPDumpVars
{
	FullDumpProgress   mcp_dump_progress; /* need to send DESYNC to master */
	ullong	mcp_dump_start_recno; /* start latest dump stored in the forwarder queue */
	ullong	mcp_dump_end_recno;	  /* end of the latest dump stored in the forwarer queue */
	bool	table_dump_request;   /* true if a slave requested a dump of the table */
} MCPDumpVars;

typedef struct MCPControlVars
{
	bool			mcp_cancel; /* Inform MCP threads about server shutdown */
	bool			num_slaves_changed; /* Tells master thread that it should
										 * recheck number of slaves connected */

	uint32			latest_tablelist_rev; /* Latest table list revision number */

	/* PID of master/slave processes (0=master, 1..N=slaves) */
	pid_t			node_pid[MCP_MAX_SLAVES + 1];	
	uint64			node_sysid[MCP_MAX_SLAVES + 1];
} MCPControlVars;

extern MCPControlVars *ServerCtl;
extern bool sigusr1_rcvd;
extern bool promotion_signal;
extern bool run_encoding_check;

/* in mcp_server.c */
extern char *MCPGetTableListFilename(char *suffix);
extern void MCPSetInitialPromotionState(short type, int slaveno);
extern bool MCPDoBackPromotion(bool master, int hostno);
extern void prepare_for_client_read(void);
extern void client_read_ended(void);

/* in mcp_master.c */
extern void HandleMasterConnection(MCPQueue *q, MCPHosts *h);

/* in mcp_slave.c */
extern void HandleSlaveConnection(MCPQueue *q, MCPHosts *h, 
								  int peerid, pg_enc encoding);
extern bool SlaveProcessPromoteNotify(int hostno, bool force);

/* in mcp_stat_process.c */
extern void HandleStatConnection(MCPQueue *q, MCPHosts *h);

/* in mcp_auth.c */
extern void MCPServerCheckAuthentication(MCPQueue *q, MCPHosts *h);
extern int ProcessMcpStartupPacket(Port *port, bool require_ssl, char *mode,
								   int *slave_no);
extern int McpReceiveClientAuth(uint64 *nodeid, pg_enc *peer_encoding);
extern void MasterSendInitialRecno(ullong initial_recno);
extern void SlaveRecvInitialRecno(ullong *initial_recno);
extern int MCPMasterConnectionAllowed(SockAddr *addr, bool check_promoted);
extern int MCPSlaveConnectionAllowed(SockAddr *addr, int hostno);

/* in mcp_processes.c */
extern void MCPInitVars(bool allocate);

extern FullDumpProgress FullDumpGetProgress(void);
extern void FullDumpSetProgress(FullDumpProgress val);
extern bool TableDumpIsRequested(void);
extern void TableDumpSetRequest(bool request);

extern void RegisterFullDumpQueueCallback(MCPQueue *q);
extern ullong FullDumpGetStartRecno(void);
extern ullong FullDumpGetEndRecno(void);
extern void FullDumpSetStartRecno(ullong recno);
extern void FullDumpSetEndRecno(ullong recno);
 
extern void WriteForwarderStateFile(void);
extern void ReadForwarderStateFile(void);
extern void RemoveForwarderStateFile(void);

#endif /* MCP_PROCESSES_H */
