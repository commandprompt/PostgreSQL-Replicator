#-------------------------------------------------------------------------
#
# Makefile for src/bin (client programs)
#
# Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/bin
top_builddir = ../..
include $(top_builddir)/src/Makefile.global

DIRS = initdb pg_ctl pg_dump \
	psql scripts pg_config pg_controldata pg_resetxlog mammoth
ifeq ($(PORTNAME), win32)
DIRS+=pgevent
endif

all install installdirs uninstall distprep clean distclean maintainer-clean:
	@for dir in $(DIRS); do $(MAKE) -C $$dir $@ || exit; done
