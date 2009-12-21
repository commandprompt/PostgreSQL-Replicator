/*
 * signals.h
 *		Data structures for storing information passed
 *		with a signal from backend to the replication
 *		process.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group,
 * Copyright (c) 2009, Command Prompt, Inc..
 * 
 * $Id$
 */

#ifndef _SIGNALS_H_
#define _SIGNALS_H_

/* 
 * If you add members to this structure better add
 * corresponding lines to sigusr1_handler in replication.c
 */
typedef struct ReplicationSignalData
{
	bool		promotion;
	bool		batchupdate;
	bool		reqdump;
	bool		resume;
} ReplicationSignalData;

extern ReplicationSignalData	*ReplSharedSignalData;
extern ReplicationSignalData	ReplLocalSignalData;

#endif /* _SIGNALS_H_ */