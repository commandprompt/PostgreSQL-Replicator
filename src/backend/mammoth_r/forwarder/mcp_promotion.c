/*
 * mcp_promotion.c
 *
 * $Id: mcp_promotion.c 2117 2009-04-30 22:32:24Z alvherre $
 */
#include "postgres.h"

#include "mammoth_r/mcp_processes.h"
#include "mammoth_r/mcp_promotion.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


static List *MCPParsePromoteAllow(char *promote_allow_slaves);
static void	PromotionStackPush(int slaveno);
static int 	PromotionStackPop(void);
int 	PromotionStackPeek(void);
static void	PromotionStackClear(void);


/* GUC vars */
char *ForwarderPromoteACL;

/* struct in shared memory */
MCPPromotionVars	*PromotionCtl = NULL;

/*
 * on_proc_exit callback for setting master_check_state during force promotion.
 */
void
promotion_cleanup(int code, Datum arg)
{
	int 	peerid = DatumGetInt32(arg);
	bool 	in_promotion = true;

	/* Set master_check_state to finished in force promotion */
	if (peerid == 0 && 
		PromotionCtl->master_check_state != master_check_none)
	{
		PromotionCtl->master_check_state = master_check_finished;
	}
	/* 
	 * Clear various promotion shared states. Here we have to synchorinize
	 * between a promoted master and a promoted slave and reset promotion
	 * only at a process involved in promotion that finishes last. We use
	 * slave_sysid and master_sysid for this, clearing them when a 
	 * corresponding process terminates.
	 */
	/* XXX: Can't acquire MCPServerLock because PGPROC is NULL here */

	if (peerid == 0 && PromotionCtl->master_sysid != 0)
		PromotionCtl->master_sysid = 0;
	else if (peerid == PromotionCtl->promotion_slave_no + 1 &&
			 PromotionCtl->slave_sysid != 0)
	   	PromotionCtl->slave_sysid = 0;
	else
		in_promotion = false;

	/* 
	 * If both the master and the slave cleared their sysids - reset promotion
	 * flags. Also do proper actions with a promotion stack.
	 */

	if (PromotionCtl->master_sysid == 0 && 
		PromotionCtl->slave_sysid == 0	&& 
		in_promotion &&
		PromotionCtl->promotion_slave_no >= 0)
	{
		if (PromotionCtl->promotion_type == back_promotion)
			PromotionStackPop();
		else if (PromotionCtl->promotion_type == force_promotion)
			PromotionStackClear();
		else
			PromotionStackPush(PromotionCtl->promotion_slave_no);
		PromotionCtl->promotion_slave_no = -1;
	}
}

void 
MCPInitPromotionVars(bool attach)
{
	bool	found;
	MCPPromotionVars *vars;

	if (attach)
		return;

	vars = (MCPPromotionVars *)
		ShmemInitStruct("PromotionVars", sizeof(MCPPromotionVars), &found);
	Assert(attach == found);

	vars->promotion_type = normal_promotion;
	vars->master_check_state = master_check_none;
	vars->promotion_slave_no = -1;
	vars->PromotionACL = MCPParsePromoteAllow(ForwarderPromoteACL);
	vars->PromotionStack.head = vars->PromotionStack.tail = 0;
	vars->master_sysid = vars->slave_sysid = 0;
	vars->PromotionStack.empty = true;

	PromotionCtl = vars;
}

/* 		MCPParsePromoteAllow
 *
 *	Makes list of slaves allowed to promote from the string representation
 */
static List *
MCPParsePromoteAllow(char *forwarder_promotion_acl)
{
	char *rawstring;
	List *elemlist,
		 *result = NIL;
	ListCell *current;
	MemoryContext	oldcxt;

	if (forwarder_promotion_acl == NULL)
		return NIL;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	rawstring = pstrdup(forwarder_promotion_acl);
	if (!SplitIdentifierString(rawstring, ',', &elemlist))
		ereport(ERROR, 
				(errmsg("Invalid list syntax for \"forwarder_promote_acl\"")));
	
	foreach(current, elemlist)
	{
		int slaveno = atoi(lfirst(current));
		if (slaveno < 0 || slaveno > mcp_max_slaves)
		{
			elog(WARNING, 
				 "invalid slave number %d in \"forwarder_promote_acl\"", 
				 slaveno);
			continue;
		}
		result = lappend_int(result, slaveno);
	}

	list_free(elemlist);
	pfree(rawstring);

	MemoryContextSwitchTo(oldcxt);

	return result;
}

/*
 * PromotionStackPush
 *
 * Pushes the given slaveno to the promotion stack.
 *
 * XXX the caller had better made sure that no other process is possibly
 * executing this function, because it makes use of a shared memory
 * variables, thus being subject to race-condition issues.  Holding the
 * MCPServerLock lock should be enough (for now, that is ...)
 */
static void
PromotionStackPush(int slaveno)
{
	MCPPromotionStack 	*stack = &(PromotionCtl->PromotionStack);

	if (stack->empty)
	{
		Assert(stack->head == stack->tail && stack->head == 0);
		stack->storage[stack->tail] = slaveno;
		stack->empty = false;
	}
	else
	{
		/* Increase stack tail and handle its wrap-around */
		if (++stack->tail >= PROMOTION_STACK_DEPTH)
			stack->tail = 0;
		/* Check if we reached stack head and increase the head
		 * if we did, handling its wrap-around
		 */
		if (stack->tail == stack->head)
			if (++stack->head > PROMOTION_STACK_DEPTH)
				stack->head = 0;
		stack->storage[stack->tail] = slaveno;
	}
}

/*
 * PromotionStackPop
 *
 *	Pops the topmost slaveno from the promotion stack. Same warnings as for
 *	PromotionStackPush.
 */
static int
PromotionStackPop(void)
{
	int 				slaveno;
	MCPPromotionStack		*stack = &(PromotionCtl->PromotionStack);

	Assert(!stack->empty);
	slaveno = stack->storage[stack->tail];
	/* Check if we should declare the stack empty */
	if (stack->tail == stack->head)
	{
		stack->tail = stack->head = 0;
		stack->empty = true;
	}
	else
	{
		/* Decrease stack tail, handling the case of its below-zero value */
		if (--stack->tail < 0)
			stack->tail = PROMOTION_STACK_DEPTH;
	}

	return slaveno;
}

/*
 * PromotionStackClear
 *
 * Removes all promotion history from promotion stack.
 */
static void
PromotionStackClear(void)
{
	MCPPromotionStack *stack = &(PromotionCtl->PromotionStack);

	if (!stack->empty)
	{
		stack->tail = stack->head = 0;
		stack->empty = true;
	}
}


/*
 * PromotionStackPeek
 *
 * Returns the topmost slaveno from the promotion stack, without removing it.
 * Calling it with an empty promotion stack returns -1 and this is the only
 * way to check if no promotions were performed (or they all were reversed).
 */
int
PromotionStackPeek(void)
{
	MCPPromotionStack	*stack = &(PromotionCtl->PromotionStack);
	/* Return -1 if no promotions performed yet */
	if (stack->empty)
		return -1;
	return stack->storage[stack->tail];
}


void 
PromotionResetAtStartup(int peerid)
{
	Assert(LWLockHeldByMe(MCPServerLock));

	/* 
	 * Check if sysid of the process doesn't match the sysid of a master or
	 * a slave that is going to promote. If we have found a match than the
	 * caller process is either a former master or a slave that participated
	 * in promotion but failed to clear a corresponding sysid from PromotionCtl
	 * on exit, which means promotion_cleanup exit handler was never called and
	 * the process terminated abnormally. We have to abandon promotion and also
	 * clear the promotion stack since it might be inconsistent after the 
	 * failed promotion (i.e. if incompleted promotion type was back promotion).
	 */
	if (ServerCtl->node_sysid[peerid] == PromotionCtl->master_sysid ||
		ServerCtl->node_sysid[peerid] == PromotionCtl->slave_sysid)
	{
		PromotionCtl->master_sysid = PromotionCtl->slave_sysid = 0;
		elog(WARNING, "latest promotion was not finished properly, "
			 "we have to clean up promoton stack and loose the promotion undo history");
		PromotionCtl->promotion_slave_no = -1;
		PromotionStackClear();
	}
}

