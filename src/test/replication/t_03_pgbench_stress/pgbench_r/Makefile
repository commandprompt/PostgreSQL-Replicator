# $PostgreSQL$

PROGRAM = pgbench
OBJS	= pgbench.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

subdir = src/test/replication/test3/pgbench_r
top_builddir = ../../../../..

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
