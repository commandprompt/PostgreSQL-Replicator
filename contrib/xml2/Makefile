# $PostgreSQL$

MODULE_big = pgxml

OBJS = $(if $(filter -lxslt, $(LIBS)), xpath.o xslt_proc.o, xpath.o)

SHLIB_LINK += $(filter -lxslt, $(LIBS)) $(filter -lxml2, $(LIBS))

DATA_built = pgxml.sql
DATA = uninstall_pgxml.sql

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/xml2
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
