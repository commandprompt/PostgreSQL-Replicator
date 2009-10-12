/*
 * mcp_promotion.h
 *
 * $Id: mcp_promotion.h 2117 2009-04-30 22:32:24Z alvherre $
 */
#include "nodes/pg_list.h"

typedef enum
{
	normal_promotion,
	force_promotion,
	back_promotion
} MCPPromotionType;

#define PromotionTypeAsString(type) \
	((type == normal_promotion) ? "NORMAL PROMOTION" : \
	(type == force_promotion) ? "FORCE PROMOTION" : \
	(type == back_promotion) ?	 "BACK PROMOTION" : \
	 							 "UKNOWN TYPE" )

typedef struct MCPPromotionVars
{
	/* what kind of promotion is running */
	MCPPromotionType	promotion_type;											

	/* current state of a check of an active master availability */
	MasterCheckState	master_check_state; 

	/* index of the slave to promote */
	int					promotion_slave_no;  	

	/* a flag indicating that an active promotion should be aborted */
	bool				promotion_cancelled;

	/* unique identifiers of a master and a slave to promote */
	uint64				master_sysid;
	uint64				slave_sysid;

	/* Time of a promotion restart for a master and a slave */
	time_t				master_restart_time;
	time_t				slave_restart_time;

	/* Variable length lists, possibly should be somewhere else */

	/* promotion access control for slaves */
	List				*PromotionACL;
	
	/* a storage for a history of already performed promotions */
	MCPPromotionStack	PromotionStack;

} MCPPromotionVars;

extern MCPPromotionVars	*PromotionCtl;

extern void PromotionInitialize(void);
extern void PromotionResetAtStartup(int peerid);
extern void MCPInitPromotionVars(bool attach);
extern void promotion_cleanup(int code, Datum arg);
extern int PromotionStackPeek();
