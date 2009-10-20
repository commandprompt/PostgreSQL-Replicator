AROPT = cr
export_dynamic = -Wl,-Bexport

DLSUFFIX = .so
ifeq ($(GCC), yes)
CFLAGS_SL = -fpic
else
CFLAGS_SL = -K PIC
endif

%.so: %.o
	$(LD) -G -Bdynamic -o $@ $<
