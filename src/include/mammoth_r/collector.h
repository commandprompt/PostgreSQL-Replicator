/*
 * $Id: collector.h 1767 2008-06-25 21:42:44Z alvherre $
 */
#ifndef COLLECTOR_H
#define COLLECTOR_H

typedef struct
{
	int         cmd_type;
} PlainHeader;

typedef struct
{
	CommandId   cid;
	uint16      natts;
	int         rel_path_len;
	char        rel_path;
} CommandHeader;

typedef struct
{
	int         data_size;
	Oid         master_oid;
	int         col_num;
}           LOData;

#endif /* COLLECTOR_H */
