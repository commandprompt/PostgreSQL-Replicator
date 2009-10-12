#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for mammoth_r/common
#
# IDENTIFICATION
#    $Header: /var/lib/cvs/Mammoth/Replicator/src/backend/mammoth_r/collector/Makefile,v 1.1.1.1.24.2 2005/02/08 12:09:05 weber Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/mammoth_r/collector
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = pgr.o master.o slave.o promotion.o repl_tuples.o common.o

all: SUBSYS.o

SUBSYS.o: $(OBJS)
	$(LD) $(LDREL) $(LDOUT) SUBSYS.o $(OBJS)

dep depend:
	$(CC) -MM $(CFLAGS) *.c >depend

clean:
	rm -f SUBSYS.o $(OBJS)

ifeq (depend,$(wildcard depend))
include depend
endif
