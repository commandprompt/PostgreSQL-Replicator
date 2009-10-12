/*
 * $Id: pgr.h 2209 2009-07-10 19:31:43Z alexk $
 */
#ifndef PGR_H
#define PGR_H

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"
#include "access/htup.h"
#include "mammoth_r/mcp_file.h"
#include "mammoth_r/mcp_local_queue.h"
#include "storage/lock.h"

extern List *MasterTableList;

extern bool    initial_is_tablecmd;
extern bool    current_is_tablecmd;

typedef enum
{
	PGR_CMD_INSERT = CMD_INSERT,
	PGR_CMD_UPDATE = CMD_UPDATE,
	PGR_CMD_DELETE = CMD_DELETE,
	PGR_CMD_NEW_LO = 200,
	PGR_CMD_TRUNCATE,
	PGR_CMD_BEGIN,
	PGR_CMD_COMMIT,
	PGR_CMD_ABORT,
	PGR_CMD_DUMP_START,
	PGR_CMD_DUMP_END,
	PGR_CMD_SEQUENCE,
	PGR_CMD_WRITE_LO,
	PGR_CMD_DROP_LO,
	PGR_CMD_TRUNCATE_LO,
	PGR_CMD_CREATE_ROLE,
	PGR_CMD_ALTER_ROLE,
	PGR_CMD_DROP_ROLE,
	PGR_CMD_GRANT_MEMBERS,
	PGR_CMD_REVOKE_MEMBERS,
	UNDEFINED
} CommandType;

#define CmdTypeAsStr(cmd) \
	((cmd == PGR_CMD_INSERT) ? "PGR_CMD_INSERT" : \
	 (cmd == PGR_CMD_UPDATE) ? "PGR_CMD_UPDATE" : \
	 (cmd == PGR_CMD_DELETE) ? "PGR_CMD_DELETE" : \
	 (cmd == PGR_CMD_NEW_LO) ? "PGR_CMD_NEW_LO" : \
	 (cmd == PGR_CMD_TRUNCATE) ? "PGR_CMD_TRUNCATE" : \
	 (cmd == PGR_CMD_BEGIN) ? "PGR_CMD_BEGIN" : \
	 (cmd == PGR_CMD_COMMIT) ? "PGR_CMD_COMMIT" : \
	 (cmd == PGR_CMD_ABORT) ? "PGR_CMD_ABORT" : \
	 (cmd == PGR_CMD_DUMP_START) ? "PGR_CMD_DUMP_START" : \
	 (cmd == PGR_CMD_DUMP_END) ? "PGR_CMD_DUMP_END" : \
	 (cmd == PGR_CMD_SEQUENCE) ? "PGR_CMD_SEQUENCE" : \
	 (cmd == PGR_CMD_WRITE_LO) ? "PGR_CMD_WRITE_LO" : \
	 (cmd == PGR_CMD_DROP_LO) ? "PGR_CMD_DROP_LO" : \
	 (cmd == PGR_CMD_TRUNCATE_LO) ? "PGR_CMD_TRUNCATE_LO" : \
	 (cmd == PGR_CMD_CREATE_ROLE) ? "PGR_CMD_CREATE_ROLE" : \
	 (cmd == PGR_CMD_ALTER_ROLE) ? "PGR_CMD_ALTER_ROLE" : \
	 (cmd == PGR_CMD_DROP_ROLE) ? "PGR_CMD_DROP_ROLE" : \
	 (cmd == PGR_CMD_GRANT_MEMBERS) ? "PGR_CMD_GRANT_MEMBERS" : \
	 (cmd == PGR_CMD_REVOKE_MEMBERS) ? "PGR_CMD_REVOKE_MEMBERS" : \
	 (cmd == UNDEFINED) ? "UNDEFINED" : "UNKNOWN CMDTYPE")

typedef enum
{
	CommitNoDump = 0,
	CommitFullDump = 1,
	CommitCatalogDump = 2,
	CommitTableDump = 4,
} CommitDumpMode;


extern void   PGRCollectDumpStart(void);
extern void   PGRCollectDumpEnd(void);
extern void   PGRCollectTxBegin(void);
extern void   PGRCollectTxAbort(void);
extern void   PGRCollectTxCommit(CommitDumpMode dump_mode);
extern void   PGRCollectEmptyTx(int flags);

extern void PGRDumpCommit(MCPLocalQueue *lq, int flags);

extern void	PGRCollectUpdate(Relation relation, HeapTuple newtuple,
                                 HeapTuple oldtuple, CommandId command_id);
extern void PGRCollectInsert(Relation relation, HeapTuple tuple,
                             CommandId command_id, bool lo_tabledump_mode);
extern void PGRCollectDelete(Relation relation, HeapTuple oldtuple,
                             CommandId command_id);

extern void PGRCollectSequence(Relation relation, int64 value, CommandId command_id);
extern void PGRCollectTruncate(Relation relation, CommandId command_id);

/* large object routines */
extern void PGRCollectNewLO(HeapTuple tuple, Oid loid, int colNo,
							CommandId cid);
extern void PGRCollectWriteLO(HeapTuple tuple, Oid loid, CommandId cid);
extern void PGRCollectDropLO(Oid loid, CommandId cid);

extern void AddRelationToMasterTableList(Relation rel, bool enable);

/* role replication functions */
extern void CollectNewRole(Relation authidRel, HeapTuple roleTuple, 
						   CommandId cid);
extern void CollectGrantRole(Relation authidRel, HeapTuple membersTuple,
							 CommandId cid);
extern void CollectRevokeRole(Relation authMembersRel, HeapTuple membersTuple,
							  CommandId cid, bool revoke_admin);
extern void CollectAlterRole(Relation authMembersRel, HeapTuple oldTuple,
							 HeapTuple newTuple, CommandId cid);
extern int 
non_dropped_to_absolute_column_number(Relation rel, int non_dropped_colno);
							
#endif /* PGR_H */
