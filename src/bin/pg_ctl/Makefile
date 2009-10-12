#-------------------------------------------------------------------------
#
# Makefile for src/bin/pg_ctl
#
# Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# $PostgreSQL$
#
#-------------------------------------------------------------------------

PGFILEDESC = "pg_ctl - starts/stops/restarts the PostgreSQL server"
MCPFILEDESC = "mcp_ctl - starts/stops the MCP server"
subdir = src/bin/pg_ctl
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -DDEF_PGPORT=$(DEF_PGPORT) -I$(libpq_srcdir) $(CPPFLAGS)

OBJS= $(WIN32RES)

all: submake-libpq submake-libpgport pg_ctl mcp_ctl

pg_ctl: $(OBJS) pg_ctl.o $(libpq_builddir)/libpq.a
	$(CC) $(CFLAGS) $(OBJS) pg_ctl.o $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)

mcp_ctl: $(OBJS) mcp_ctl.o
	$(CC) $(CFLAGS) $(OBJS) mcp_ctl.o $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_ctl$(X) '$(DESTDIR)$(bindir)/pg_ctl$(X)'
	$(INSTALL_PROGRAM) mcp_ctl$(X) '$(DESTDIR)$(bindir)/mcp_ctl$(X)'

installdirs:
	$(mkinstalldirs) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_ctl$(X)'
	rm -f '$(DESTDIR)$(bindir)/mcp_ctl$(X)'

clean distclean maintainer-clean:
	rm -f pg_ctl$(X) pg_ctl.o $(OBJS)
	rm -f mcp_ctl$(X) mcp_ctl.o $(OBJS)

# ensure that changes in DEF_PGPORT propagate into object file
pg_ctl.o: pg_ctl.c $(top_builddir)/src/Makefile.global
mcp_ctl.o: mcp_ctl.c $(top_builddir)/src/Makefile.global
