# Makefile for the Mammoth Replicator modules and binaries
#
# $Id: Makefile 1923 2008-11-24 20:10:53Z alvherre $

subdir = src/backend/mammoth_r
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS	    := common collector agents globals forwarder
SUBDIROBJS  := $(SUBDIRS:%=%/SUBSYS.o)

all: SUBSYS.o

SUBSYS.o: $(SUBDIROBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^

$(SUBDIROBJS): $(SUBDIRS:%=%-recursive) ;

.PHONY: $(SUBDIRS:%=%-recursive)
$(SUBDIRS:%=%-recursive):
	$(MAKE) -C $(subst -recursive,,$@) SUBSYS.o

install: 	 	 
	$(INSTALL_DATA) $(top_srcdir)/$(subdir)/globals/globals-install.sql $(DESTDIR)/$(datadir)

clean:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@ || exit; done
	rm -f SUBSYS.o

dep depend:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@ || exit; done
