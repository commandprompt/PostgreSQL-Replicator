/*
 * signals.c
 * 			Mammoth Replicator signal data.
 *
 * Shared signal data is stored in the shared memory and used to send signals
 * from the backend to replication processes, while local signal data is where
 * the replication process saves the value of shared 'signals.
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2006, Command Prompt, Inc.
 * 
 * $Id$
 */

#include "postgres.h"

#include "mammoth_r/signals.h"

/* shared memory data */
ReplicationSignalData	   *ReplSharedSignalData;

/* local per backend data */
ReplicationSignalData		ReplLocalSignalData = 
{
	false, 
	false, 
	false, 
	false
};