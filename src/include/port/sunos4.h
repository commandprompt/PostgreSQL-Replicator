/*
 * $PostgreSQL$
 *
 * sprintf() returns char *, not int, on SunOS 4.1.x */
#define SPRINTF_CHAR

#include <unistd.h>
