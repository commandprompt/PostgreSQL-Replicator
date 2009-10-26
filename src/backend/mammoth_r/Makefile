# Makefile for the Mammoth Replicator modules and binaries
#
# $Id: Makefile 1923 2008-11-24 20:10:53Z alvherre $

subdir = src/backend/mammoth_r
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS	    := common collector agents globals forwarder

include $(top_srcdir)/src/backend/common.mk

all: $(top_srcdir)/$(subdir)/globals/globals-install.sql

install: 	 	 
	$(INSTALL_DATA) $(top_srcdir)/$(subdir)/globals/globals-install.sql $(DESTDIR)/$(datadir)
