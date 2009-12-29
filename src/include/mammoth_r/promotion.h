/*-----------------------
 * promotion.h
 * 		The Mammoth Replication promotion header
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 *
 * $Id: promotion.h 1745 2008-06-18 14:44:07Z alvherre $
 * ------------------------
 */

#include "mammoth_r/mcp_connection.h"
#include "nodes/parsenodes.h"

/*
 * File name to be written on promotion, to make the information about the
 * promotion stack persistent.  Thus if the promoted slave goes down and then
 * back up again, it knows that it should start as a promoted node.
 */
#define PROMOTION_FILE_NAME "mammoth.promoted"

extern void BackendInitiatePromotion(PromoteStmt *stmt);
extern void SlaveInitiatePromotion(bool force);
extern void InitiateBackPromotion(void);
extern void SlaveCompletePromotion(bool force);
extern void MasterCompletePromotion(void);
extern void process_promotion_file(void);

/*
 * Shared memory structs and pointers.
 *
 * We use these to implement communication between a backend and a replicator
 * process (for example when the user issues a PROMOTE command), and between
 * two replicator processes (for example between the dying slave replication
 * process and the new master queue monitor process, during a promotion.)
 *
 * To implement the commands which require the replication process
 * intervention (PROMOTE, BATCHUPDATE), the backend sets a flag in
 * shared memory and signals the postmaster.  The postmaster signals the
 * replication process, and the latter is in charge of checking the flags the
 * backend may have set, unsetting it and taking the corresponding action.
 *
 * The backend must not signal the replicator process directly, because it
 * doesn't know the PID.
 */

typedef struct ReplicationPromotionData
{
   bool		force_promotion;
   bool		back_promotion;
   bool		promotion_in_progress;
   bool		promotion_block_relations;
} ReplicationPromotionData;

extern ReplicationPromotionData	*ReplPromotionData;
