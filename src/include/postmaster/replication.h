/*------------------------------
 * replication.h
 * 		Exports from postmaster/replication.c.
 *
 * Portions Copyright (c) 1996-2005, PostgreSQL Global Development Group
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $CommandPrompt$
 *
 *------------------------------
 */
#ifndef POSTMASTER_REPLICATION_H
#define POSTMASTER_REPLICATION_H

#include "nodes/parsenodes.h"

typedef enum
{
	REPLICATION_MODE_MASTER,
	REPLICATION_MODE_SLAVE
} ReplicationMode;

/* GUC variables */
extern PGDLLIMPORT bool replication_enable;
extern bool replication_slave_batch_mode;
extern bool replication_compression;
extern bool replication_use_utf8_encoding;
extern int replication_slave_no;
extern int replication_slave_batch_mode_timeout;
extern char *replication_database;
extern char *replication_data_path;
extern ReplicationMode replication_mode;

/* A flag to enable/disable forwarder agent process */
extern bool replication_process_enable;
extern bool replication_encoding_conversion_enable;

extern int	ReplicationChildSlot;	/* PMChildSlot stuff (see pmsignal.c) */

#define replication_master (replication_mode == REPLICATION_MODE_MASTER)
#define replication_slave (replication_mode == REPLICATION_MODE_SLAVE)

#define replication_perform_encoding_conversion \
	(replication_use_utf8_encoding && replication_encoding_conversion_enable)

/* Status inquiry functions */
extern bool ReplicationActive(void);
extern bool IsReplicatorProcess(void);

/* Functions to start a replication process, called from postmaster */
extern void replication_init(void);
extern int replication_start(void);
extern void replication_stopped(bool cause_delay);
extern void InitializeMCPQueue(void);
extern int ReplicationShmemSize(void);
extern void ReplicationShmemInit(void);
extern void DestroyMCPConnection(int arg, Datum code);
extern int forwarder_helper_start(void);

#ifdef EXEC_BACKEND
extern void ReplicationMain(int argc, char *argv[]);
#endif

/* Functions to act on user commands */
void ProcessMcpStmt(McpStmt *stmt);
void ProcessPromoteStmt(PromoteStmt *stmt);
void ProcessAlterSlaveRequestDump(void);
void ProcessAlterSlaveCommand(AlterSlaveStmt *stmt);

/*
 * Known exit codes
 */
#define REPLICATOR_EXIT_WRONG_SCHEMA	91
#define REPLICATOR_EXIT_SLAVE_PROMOTION	92
#define REPLICATOR_EXIT_MASTER_DEMOTION	93

#endif /* POSTMASTER_REPLICATION_H */
