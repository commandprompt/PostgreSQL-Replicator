#-------------------------------------------------------------------------
#
# Makefile
#    Makefile for mammoth_r/agents
#
# IDENTIFICATION
#    $Id: Makefile 1830 2008-07-16 04:27:11Z alvherre $
#
#-------------------------------------------------------------------------

subdir = src/backend/mammoth_r/agents
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = master_mcp_agent.o slave_mcp_agent.o mcp_local_queue.o clientconn.o report.o

all: SUBSYS.o

SUBSYS.o: $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(OBJS)

clean:
	rm -f SUBSYS.o $(OBJS)
