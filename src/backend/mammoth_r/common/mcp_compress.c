/*
 * mcp_compress.c
 * 		Compression/decompression routines for txl data.
 *
 * $Id: mcp_compress.c 1899 2008-09-30 15:21:34Z alexk $
 */
#include "postgres.h"

#include "mammoth_r/mcp_compress.h"
#include "mammoth_r/mcp_local_queue.h"
#include "utils/pg_lzcompress.h"

/* MCPCompressData
 *
 * Try to compress data passed as a first argument, return either a palloced
 * memory with compressed data or original pointer to uncompressed data.
 */
void *
MCPCompressData(void *data, MCPQueueTxlDataHeader *dh)
{
	PGLZ_Header	   *compdata;
	int32			compsize;
	bool			compressed;

	Assert(data != NULL);
	Assert(dh != NULL);

	compressed = dh->tdh_flags & TXL_FLAG_COMPRESSED;

	/* 
	 * If already compressed or can't compress due to out of signed int
	 * boundaries or too small size, do nothing 
	 */
	if (compressed || dh->tdh_datalen < 8 || dh->tdh_datalen & 0xF0000000)
		return data;

	compdata = (PGLZ_Header *) palloc(PGLZ_MAX_OUTPUT(dh->tdh_datalen));

	/* compress data using a default strategy */
	compressed = pglz_compress(data, dh->tdh_datalen, compdata, PGLZ_strategy_default);

	if (compressed)
	{
		compsize =  VARSIZE(compdata);
		Assert(compsize != 0);
	}
	else
		compsize = dh->tdh_datalen;
	
	if (compsize < dh->tdh_datalen)
	{
		/* Set compressed bit of signature */
		dh->tdh_flags |= TXL_FLAG_COMPRESSED;
		dh->tdh_datalen = compsize;
	}
	else
	{
		pfree(compdata);
		compdata = data;
	}

	return compdata;
}

/* 
 * Try to decompress data passed as a first argument. Return either a palloc'ed
 * pointer to the uncompressed data or a pointer to the original data if it is
 * compressed.  In case it is compressed, the original is freed.
 */
StringInfo
MCPDecompressData(StringInfo compressed, MCPQueueTxlDataHeader *dh)
{
	StringInfo	decomp;
	uint32		decompsize;

	/* stop if data is not compressed */
	if (!(dh->tdh_flags & TXL_FLAG_COMPRESSED))
		return compressed;

	decomp = makeStringInfo();

	/* Determine size of decomp data */
	decompsize = PGLZ_RAW_SIZE((PGLZ_Header *) (compressed->data));
	decomp->data = palloc(decompsize);
	decomp->len = decompsize;

	/* Decompress data */
	pglz_decompress((PGLZ_Header *) compressed->data,
					decomp->data);
	/* free the original */
	pfree(compressed->data);
	pfree(compressed);

	return decomp;
}
