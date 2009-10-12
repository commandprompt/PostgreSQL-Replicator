/*-----------------------
 * forwcmds.h
 * 		forwarder command definitions
 *
 * Portions Copyright (c) 1996-2006, PostgreSQL Global Development Group,
 * Copyright (c) 2009, Command Prompt, Inc.
 *
 * $Id: forwcmds.h 2042 2009-03-12 20:54:32Z alvherre $
 * ------------------------
 */
#ifndef FORWCMDS_H
#define FORWCMDS_H

#include "nodes/parsenodes.h"

extern void CreateForwarder(CreateForwarderStmt *stmt);
extern void DropForwarder(DropForwarderStmt *stmt);
extern void AlterForwarder(AlterForwarderStmt *stmt);
extern void init_forwarder_config(char **name, char **address, int *port,
								  char **key, bool *ssl);
extern void check_forwarder_config(void);

#endif /* FORWCMDS_H */
