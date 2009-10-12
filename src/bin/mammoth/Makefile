#-------------------------------------------------------------------------
#
# Makefile for src/bin/mammoth
#
# Copyright (c) 2002-2005, Command Prompt Inc.
#
# $Id: Makefile 2093 2009-04-09 19:05:33Z alvherre $
#
#-------------------------------------------------------------------------

top_builddir = ../../..
subdir = src/bin/mammoth
include $(top_builddir)/src/Makefile.global

all:

$(srcdir)/init-mammoth-database: % : %.in $(top_builddir)/src/Makefile.global
	sed -e 's,@EGREP@,$(EGREP),g' -e 's,@LN_S@,$(LN_S),g' $< >$@
	chmod a+x $@

install-scripts: $(srcdir)/init-mammoth-database
	$(INSTALL_SCRIPT) $(top_srcdir)/$(subdir)/init-mammoth-database $(DESTDIR)$(bindir)
	$(INSTALL_DATA) $(top_srcdir)/$(subdir)/replicator-functions.sql $(DESTDIR)$(datadir)/replicator-functions.sql

install: all installdirs install-scripts

installdirs:
	$(mkinstalldirs) $(DESTDIR)$(bindir)

uninstall:
	rm -f $(DESTDIR)$(bindir)/init-mammoth-database
	rm -f $(DESTDIR)$(datadir)/replicator-functions.sql

clean:
	rm -f $(srcdir)/init-mammoth-database

