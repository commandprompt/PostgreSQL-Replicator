#include "postgres.h"

#include "access/heapam.h"
#include "catalog/repl_slave_roles.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "nodes/nodes.h"
#include "postmaster/replication.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"

/* drop_replicated_role 
 *		Remove a role from the slave */
Datum
drop_replicated_role(PG_FUNCTION_ARGS)
{
	HeapTuple 		tuple;
	TriggerData    *trigdata = (TriggerData *) fcinfo->context;
	const char 	   *rolename;
	Form_repl_slave_roles rolesForm;
	
	elog(DEBUG5, "drop_replicated_role starting");
	
	/* sanity checks */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "must be called by trigger manager");
	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired before row operation");
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "trigger should be fired for each row");
	if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		elog(ERROR, "trigger should be fired on delete only");
	if (RelationGetRelid(trigdata->tg_relation) != ReplSlaveRolesRelationId)
		elog(ERROR, "trigger should be fired for repl_slave_roles relation");
		
	tuple = trigdata->tg_trigtuple;
	
	rolesForm = (Form_repl_slave_roles) GETSTRUCT(tuple);
	rolename = NameStr(rolesForm->rolename);
	
	if (rolesForm->slave == replication_slave_no)
	{
		DropRoleStmt  *stmt;
			
		elog(DEBUG5, "Removing role \"%s\" from slave %d",
					 rolename, replication_slave_no);
		/* Make DropRoleStmt structure before calling DropRole */
		stmt = makeNode(DropRoleStmt);
		stmt->roles = list_make1(makeString((char *)rolename));
		stmt->missing_ok = true;
		
		/* Do actual role removal */
		DropRole(stmt);
		pfree(stmt);
		
		/* flag update of the flat file at transaction commit */
		auth_file_update_needed();
		
	}
	else
		elog(DEBUG5, "Not removing role \"%s\" from slave %d",
					 rolename, replication_slave_no);
					
	return PointerGetDatum(tuple);
}
