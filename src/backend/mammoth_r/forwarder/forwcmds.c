/*-------------------------------------------------------------------------
 *
 * forwcmds.c
 *	  Contains functions to create forwarder configurations.
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2009, Command Prompt, Inc.
 *
 *
 * $Id: forwcmds.c 2063 2009-03-20 18:45:19Z alvherre $
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "catalog/indexing.h"
#include "catalog/mammoth_indexing.h"
#include "catalog/repl_forwarder.h"
#include "commands/defrem.h"
#include "libpq/hba.h"
#include "mammoth_r/forwcmds.h"
#include "miscadmin.h"
#include "postmaster/replication.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/relcache.h"


void
CreateForwarder(CreateForwarderStmt *stmt)
{
	Datum	values[Natts_repl_forwarder];
	bool	nulls[Natts_repl_forwarder];
	NameData pname;
	Relation frwrel;
	HeapTuple tup;
	ListCell *pl;

	/* initialize tuple fields */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	namestrcpy(&pname, stmt->name);

	values[Anum_repl_forwarder_name - 1] = NameGetDatum(&pname);

	foreach (pl, stmt->parameters)
	{
		DefElem		*defel = (DefElem *) lfirst(pl);

		if (pg_strcasecmp(defel->defname, "host") == 0)
		{
			values[Anum_repl_forwarder_host - 1] =
				DirectFunctionCall1(inet_in, CStringGetDatum(defGetString(defel)));
		}
		else if (pg_strcasecmp(defel->defname, "port") == 0)
		{
			int64	ival;
			Datum	value;

			ival = defGetInt64(defel);
			value = Int32GetDatum((int32) ival);
			/* FIXME --- what's the right limit for this? */
			if (value < 0 || value > 65535)
				ereport(ERROR,
						(errmsg("port value "INT64_FORMAT" out of range",
								ival)));
			values[Anum_repl_forwarder_port - 1] = value;
		}
		else if (pg_strcasecmp(defel->defname, "authkey") == 0)
		{
			char	*key;

			key = defGetString(defel);
			values[Anum_repl_forwarder_authkey - 1] = 
				DirectFunctionCall1(textin, CStringGetDatum(key));
		}
		else if (pg_strcasecmp(defel->defname, "ssl") == 0)
		{
			bool	boolval = defGetBoolean(defel);

			values[Anum_repl_forwarder_ssl - 1] = BoolGetDatum(boolval);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("forwarder parameter \"%s\" not recognized",
							defel->defname)));
	}

	if (values[Anum_repl_forwarder_host - 1] == 0)
		ereport(ERROR,
				(errmsg("mandatory host parameter not specified")));
	if (values[Anum_repl_forwarder_port - 1] == 0)
		ereport(ERROR,
				(errmsg("mandatory port parameter not specified")));
	if (values[Anum_repl_forwarder_authkey - 1] == 0)
		ereport(ERROR,
				(errmsg("mandatory authorization key parameter not specified")));
	/* SSL defaults to false */

	frwrel = heap_open(ReplForwarderId, RowExclusiveLock);
	tup = heap_form_tuple(frwrel->rd_att, values, nulls);

	simple_heap_insert(frwrel, tup);

	CatalogUpdateIndexes(frwrel, tup);

	heap_freetuple(tup);

	heap_close(frwrel, RowExclusiveLock);

	/* mark the flatfile for update at transaction commit */
	forw_file_update_needed();
}

void
DropForwarder(DropForwarderStmt *stmt)
{
	Relation		forwrel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tup;

	forwrel = heap_open(ReplForwarderId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repl_forwarder_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	scan = systable_beginscan(forwrel, ReplForwarderNameIndexId, true,
			   				  SnapshotNow, 1, key);

	/* we expect a single tuple due to unique index on forwarder name */
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		simple_heap_delete(forwrel, &tup->t_self);

		systable_endscan(scan);
		heap_close(forwrel, RowExclusiveLock);

		/* mark the flatfile for update at transaction commit */
		forw_file_update_needed();

		return;
	}

	ereport(ERROR,
		   (errmsg("forwarder \"%s\" does not exist", stmt->name)));
}

void
AlterForwarder(AlterForwarderStmt *stmt)
{
	Relation		forwrel;
	ScanKeyData		key[1];
	SysScanDesc		scan;
	HeapTuple		tup;
	ListCell	   *option;
	DefElem		   *dhost = NULL;
	DefElem		   *dport = NULL;
	DefElem		   *dkey = NULL;
	DefElem		   *dssl = NULL;

	/* Extract options from the statement node tree */
	foreach(option, stmt->options)
	{
		DefElem	   *defel = (DefElem *) lfirst(option);
		
		if (strcmp(defel->defname, "host") == 0)
		{
			if (dhost)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dhost = defel;
		}
		else if (strcmp(defel->defname, "port") == 0)
		{
			if (dport)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dport = defel;
		}
		else if (strcmp(defel->defname, "authkey") == 0)
		{
			if (dkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dkey = defel;
		}
		else if (strcmp(defel->defname, "ssl") == 0)
		{
			if (dssl)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			dssl = defel;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized", defel->defname)));
	}

	forwrel = heap_open(ReplForwarderId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_repl_forwarder_name,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->name));

	scan = systable_beginscan(forwrel, ReplForwarderNameIndexId, true,
							  SnapshotNow, 1, key);

	/* we expect a single tuple due to unique index on forwarder name */
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		Datum		values[Natts_repl_forwarder];
		bool		nulls[Natts_repl_forwarder];
		bool		replace[Natts_repl_forwarder];
		HeapTuple	newtuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));
		MemSet(replace, 0, sizeof(replace));
		
		if (dhost)
		{
			values[Anum_repl_forwarder_host - 1] =
				DirectFunctionCall1(inet_in, CStringGetDatum(defGetString(dhost)));
			replace[Anum_repl_forwarder_host - 1] = true;
		}
		if (dport)
		{
			int64	ival;
			Datum	value;

			ival = defGetInt64(dport);
			value = Int32GetDatum((int32) ival);
			/* FIXME --- what's the right limit for this? */
			if (value < 0 || value > 65535)
				ereport(ERROR,
						(errmsg("port value "INT64_FORMAT" out of range",
								ival)));
			values[Anum_repl_forwarder_port - 1] = value;
			replace[Anum_repl_forwarder_port - 1] = true;
		}
		if (dkey)
		{
			values[Anum_repl_forwarder_authkey - 1] =
				DirectFunctionCall1(textin, CStringGetDatum(defGetString(dkey)));
			replace[Anum_repl_forwarder_authkey - 1] = true;
		}
		if (dssl)
		{
			values[Anum_repl_forwarder_ssl - 1] =
				BoolGetDatum(defGetBoolean(dssl));
			replace[Anum_repl_forwarder_ssl - 1] = true;
		}

		newtuple = heap_modify_tuple(tup, RelationGetDescr(forwrel),
									 values, nulls, replace);
		simple_heap_update(forwrel, &tup->t_self, newtuple);

		/* Update indexes */
		CatalogUpdateIndexes(forwrel, newtuple);

		systable_endscan(scan);
		heap_close(forwrel, RowExclusiveLock);

		/* mark the flatfile for update at transaction commit */
		forw_file_update_needed();

		return;
	}

	ereport(ERROR,
			(errmsg("forwarder \"%s\" does not exist", stmt->name)));
}

/*
 * Read the forwarder config and return the selected forwarder info.
 *
 * address, port, key and ssl are allowed to be passed as NULL if the caller
 * is not interested in them.
 *
 * This is called by postmaster and by the replication process.
 */
void
init_forwarder_config(char **name, char **address, int *port, char **key, bool *ssl)
{
	char	*filename;
	FILE	*ffile;

	filename = forw_getflatfilename();
	ffile = AllocateFile(filename, "r");
	if (ffile == NULL)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));
	/* XXX we read a single line and just use that */
	read_repl_forwarder_line(ffile, name, address, port, key, ssl);

	FreeFile(ffile);
	pfree(filename);
}

/*
 * Check that a forwarder configuration has been specified.
 *
 * This is called by postmaster to determine whether to start the replication
 * process.
 */
void
check_forwarder_config(void)
{
	char	*name;

	init_forwarder_config(&name, NULL, NULL, NULL, NULL);

	/* If there's no forwarder set up, disable replication */
	if (name == NULL)
	{
		/* note: this is a crock and should be refined somehow */
		Assert(!IsUnderPostmaster);
		ereport(WARNING,
				(errmsg("replication process disabled due to missing forwarder configuration")));
		/* if there's an existing connection, ensure that it's closed */
		if (replication_process_enable)
		{
			/* FIXME send a signal here */
		}
		replication_process_enable = false;
	}
	else
		replication_process_enable = true;

	if (name)
		pfree(name);
}
