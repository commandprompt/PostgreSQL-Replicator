/*
 * $PostgreSQL$
 */
#ifndef __HSTORE_H__
#define __HSTORE_H__

#include "fmgr.h"


typedef struct
{
	uint16		keylen;
	uint16		vallen;
	uint32
				valisnull:1,
				pos:31;
} HEntry;

/* these are determined by the sizes of the keylen and vallen fields */
/* in struct HEntry and struct Pairs */
#define HSTORE_MAX_KEY_LEN 65535
#define HSTORE_MAX_VALUE_LEN 65535

/* these are determined by the sizes of the keylen and vallen fields */
/* in struct HEntry and struct Pairs */
#define HSTORE_MAX_KEY_LEN 65535
#define HSTORE_MAX_VALUE_LEN 65535


typedef struct
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int4		size;
	char		data[1];
} HStore;

#define HSHRDSIZE	(VARHDRSZ + sizeof(int4))
#define CALCDATASIZE(x, lenstr) ( (x) * sizeof(HEntry) + HSHRDSIZE + (lenstr) )
#define ARRPTR(x)		( (HEntry*) ( (char*)(x) + HSHRDSIZE ) )
#define STRPTR(x)		( (char*)(x) + HSHRDSIZE + ( sizeof(HEntry) * ((HStore*)x)->size ) )


#define PG_GETARG_HS(x) ((HStore*)PG_DETOAST_DATUM(PG_GETARG_DATUM(x)))

typedef struct
{
	char	   *key;
	char	   *val;
	uint16		keylen;
	uint16		vallen;
	bool		isnull;
	bool		needfree;
} Pairs;

int			comparePairs(const void *a, const void *b);
int			uniquePairs(Pairs *a, int4 l, int4 *buflen);

size_t		hstoreCheckKeyLen(size_t len);
size_t		hstoreCheckValLen(size_t len);

size_t      hstoreCheckKeyLen(size_t len);
size_t      hstoreCheckValLen(size_t len);

#define HStoreContainsStrategyNumber	7
#define HStoreExistsStrategyNumber		9

#endif   /* __HSTORE_H__ */
