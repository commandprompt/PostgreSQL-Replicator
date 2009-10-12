#include <zlib.h>
/* -------------------------------------------------------------------------------
 * zlib compression routines
 */
int zCompressStreamInit( z_streamp z_strmp, int buffer_size );
int zCompressStreamClose( z_streamp z_strmp );
int zCompress(z_streamp z_strmp);
int zCompressFlush(z_streamp z_strmp);
int zCompressFinish(z_streamp z_strmp);

/* -------------------------------------------------------------------------------
 * zlib decompression routines
 */
int zDecompressStreamInit( z_streamp z_strmp, char* nextOut, int outSize );
int zDecompressStreamClose( z_streamp z_strmp );

/* returns 0 if data inflated successfully (avail_in = 0)
 * returns 1 if no progress could be made, needs more output space
 * in either case, zstrmp->avail_out is reduced by the amount of 
 * 		decompressed data
 * 
 * returns zerror (negative) on error
 * 
 */
int zDecompress( z_streamp z_strmp );

