#
# Makefile
# 		Makefile for src/test/replication
#
# Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $Id: Makefile 2017 2009-02-04 21:24:54Z alvherre $
#

subdir = src/test/replication
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

PGROOT=$(prefix)

all:
	PGROOT=$(PGROOT) ./test.pl

clean:
	rm -fr master slave0 slave1 slave2
	rm -fr test*/output
