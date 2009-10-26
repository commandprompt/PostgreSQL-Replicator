#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for mammoth_r/forwarder
#
# IDENTIFICATION
#    $Header$
#
#-------------------------------------------------------------------------

subdir = src/backend/mammoth_r/forwarder
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = mcp_server.o mcp_slave.o mcp_master.o mcp_promotion.o \
	mcp_auth.o mcp_hosts.o mcp_processes.o mcp_tables.o forwcmds.o \
	mcp_helper.o fwsignals.o

include $(top_srcdir)/src/backend/common.mk
