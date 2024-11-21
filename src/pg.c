/*-------------------------------------------------------------------------
 *
 * src/bgz.c
 *
 * Implementation of block-gzip:
 * https://samtools.github.io/hts-specs/SAMv1.pdf#subsection.4.1
 * https://www.htslib.org/doc/bgzip.html
 *
 * Inspired from:
 * https://github.com/samtools/htslib/blob/master/bgzip.c
 * https://github.com/pramsey/pgsql-gzip/blob/master/pg_gzip.c
 *
 * Note: no need to test if palloc retruns NULL
 *       https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/utils/mmgr/README
 *
 *-------------------------------------------------------------------------
 */

#include <assert.h>
#include <errno.h>
#include <unistd.h>

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "funcapi.h"

#include <libdeflate.h>

/* logging */
#define F(fmt, ...)  elog(FATAL,  "============ " fmt, ##__VA_ARGS__)
#define E(fmt, ...)  elog(ERROR,  "============ " fmt, ##__VA_ARGS__)
#define W(fmt, ...)  elog(WARNING,"============ " fmt, ##__VA_ARGS__)
#define N(fmt, ...)  elog(NOTICE, "| " fmt, ##__VA_ARGS__)
#define L(fmt, ...)  elog(LOG,    "============ " fmt, ##__VA_ARGS__)
#define D1(fmt, ...) elog(DEBUG1, "============ " fmt, ##__VA_ARGS__)
#define D2(fmt, ...) elog(DEBUG2, "============ " fmt, ##__VA_ARGS__)
#define D3(fmt, ...) elog(DEBUG3, "============ " fmt, ##__VA_ARGS__)
#define D4(fmt, ...) elog(DEBUG4, "============ " fmt, ##__VA_ARGS__)
#define D5(fmt, ...) elog(DEBUG5, "============ " fmt, ##__VA_ARGS__)

PG_MODULE_MAGIC;

#define BGZIP_BLOCK_SIZE     0xff00 // make sure compressBound(BGZIP_BLOCK_SIZE) < BGZIP_MAX_BLOCK_SIZE
#define BGZIP_MAX_BLOCK_SIZE 0x10000
#define BLOCK_HEADER_LENGTH 18
#define BLOCK_FOOTER_LENGTH 8

/* BGZIP header (specialized from RFC 1952; little endian):
 +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 | 31|139|  8|  4|              0|  0|255|      6| 66| 67|      2|BLK_LEN|
 +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
  BGZIP extension:
                ^                              ^   ^   ^
                |                              |   |   |
               FLG.EXTRA                     XLEN  B   C

  BGZIP format is compatible with GZIP. It limits the size of each compressed
  block to 2^16 bytes and adds and an extra "BC" field in the gzip header which
  records the size.
*/
static const uint8_t g_magic[19] =    "\037\213\010\4\0\0\0\0\0\377\6\0\102\103\2\0\0\0";
static const uint8_t eof_marker[28] = "\037\213\010\4\0\0\0\0\0\377\6\0\102\103\2\0\033\0\3\0\0\0\0\0\0\0\0\0";

static inline void packInt16(uint8_t *buffer, uint16_t value)
{
  uint32_t value_le = htole16(value);
  memcpy(buffer, &value_le, 2);
  /*
    buffer[0] = value;
    buffer[1] = value >> 8;
  */
}

static inline void packInt32(uint8_t *buffer, uint32_t value)
{
  uint32_t value_le = htole32(value);
  memcpy(buffer, &value_le, 4);
  /*
    buffer[0] = value;
    buffer[1] = value >> 8;
    buffer[2] = value >> 16;
    buffer[3] = value >> 24;
  */
}

static struct libdeflate_options libdeflate_options = {
  .sizeof_options = sizeof(struct libdeflate_options),
  .malloc_func = palloc0,
  .free_func = pfree,
};

static int
bgzip_compress_block(uint8_t *dst, size_t *dlen,
		     const uint8_t *src, size_t slen,
		     int level)
//__attribute__((non-null(1,2,3,4)))
{
    struct libdeflate_options options;
    struct libdeflate_compressor *z = NULL;
    size_t clen;
    uint32_t crc;

    if (slen == 0) { // EOF block
        if (*dlen < 28) return -1;
        //memcpy(dst, "\037\213\010\4\0\0\0\0\0\377\6\0\102\103\2\0\033\0\3\0\0\0\0\0\0\0\0\0", 28);
        memcpy(dst, g_magic, 16); // not the last bytes
        memcpy(dst+16, "\033\0\3\0\0\0\0\0\0\0\0\0", 12);
        *dlen = 28;
        return 0;
    }

    memset(&options, 0, sizeof(options));
    options.sizeof_options = sizeof(options);
    options.malloc_func = palloc0;
    options.free_func = pfree;

    z = libdeflate_alloc_compressor_ex(level, &options);
    if (!z) return -1;

    // Raw deflate
    clen = libdeflate_deflate_compress(z, (const void *)src, slen,
				       (void *)(dst + BLOCK_HEADER_LENGTH),
				       *dlen - BLOCK_HEADER_LENGTH - BLOCK_FOOTER_LENGTH);

    if (clen <= 0) {
      W("libdeflate_deflate_compress failed");
      libdeflate_free_compressor(z);
      return -1;
    }

    *dlen = clen + BLOCK_HEADER_LENGTH + BLOCK_FOOTER_LENGTH;
    
    libdeflate_free_compressor(z);

    // write the header
    memcpy(dst, g_magic, BLOCK_HEADER_LENGTH); // the last two bytes are a place holder for the length of the block
    packInt16(&dst[16], *dlen - 1); // write the compressed length; -1 to fit 2 bytes

    // write the footer
    crc = libdeflate_crc32(0, src, slen);
    packInt32((uint8_t*)&dst[*dlen - 8], crc);  // CRC
    packInt32((uint8_t*)&dst[*dlen - 4], slen); // ISIZE
    return 0;
}

PG_FUNCTION_INFO_V1(pg_bgzip_compress);
Datum pg_bgzip_compress(PG_FUNCTION_ARGS)
{
	bytea* compressed;
	size_t compressed_size = 0;

	bytea* uncompressed = NULL;
	int32 compression_level = -1;
	uint8_t* in = NULL;
	size_t in_size = 0;

	bool with_eof = false;

	if(PG_NARGS() != 2 && PG_NARGS() != 3){
	  E("Invalid number of arguments: expected 2 or 3, got %d", PG_NARGS());
	  PG_RETURN_NULL();
	}

	if(PG_ARGISNULL(0) || PG_ARGISNULL(1)){
	  E("Null arguments not accepted");
	  PG_RETURN_NULL();
	}

	if(PG_NARGS() == 3 && !PG_ARGISNULL(2))
	  with_eof = PG_GETARG_BOOL(2);

	uncompressed = PG_GETARG_BYTEA_PP(0);
	compression_level = PG_GETARG_INT32(1);
	in = (uint8_t*)(VARDATA_ANY(uncompressed));
	in_size = VARSIZE_ANY_EXHDR(uncompressed);

	/* compression level -1 is default best effort (approx 6) */
	/* level 0 is no compression, 1-9 are lowest to highest */
	if (compression_level < -1 || compression_level > 9)
		elog(ERROR, "invalid compression level: %d", compression_level);

	compressed = (bytea *)palloc(VARHDRSZ); // start empty

	/* Loop through the blocks */
	while (in_size > 0){

	  size_t isize = (in_size < BGZIP_BLOCK_SIZE) ? in_size : BGZIP_BLOCK_SIZE;
	  size_t dlen = BGZIP_MAX_BLOCK_SIZE;

	  compressed = (bytea *)repalloc(compressed, compressed_size + dlen + VARHDRSZ);

	  if(bgzip_compress_block(VARDATA(compressed) + compressed_size, &dlen,
				  in, isize,
				  compression_level))
	    E("Error compressing the block at position %zu", in_size);

	  in += isize;
	  in_size -= isize;
	  compressed_size += dlen;
	}

	if(with_eof){
	  N("bgzip_compress with EOF!");
	  /* Add the EOF marker */
	  compressed = (bytea *)repalloc(compressed, compressed_size + 28 + VARHDRSZ); // sizeof(marker)
	  memcpy(VARDATA(compressed) + compressed_size, eof_marker, 28);
	  compressed_size += 28;
	}

	SET_VARSIZE(compressed, compressed_size + VARHDRSZ);

	PG_RETURN_BYTEA_P(compressed);
}


PG_FUNCTION_INFO_V1(pg_bgzip_gzip_compress);
Datum pg_bgzip_gzip_compress(PG_FUNCTION_ARGS)
{
	bytea* compressed;
	size_t compressed_size = 0;
	bytea* uncompressed = NULL;
	int32 compression_level = 0;
	const void* in;
	size_t ilen = 0;
	size_t dlen = 0;
	struct libdeflate_options options;
	struct libdeflate_compressor *z = NULL;

	if(PG_ARGISNULL(0) || PG_ARGISNULL(1)){
	  E("Null arguments not accepted");
	  PG_RETURN_NULL();
	}

	compression_level = PG_GETARG_INT32(1);

	/* compression level -1 is default best effort (approx 6) */
	/* level 0 is no compression, 1-9 are lowest to highest */
	if (compression_level < -1 || compression_level > 9)
		elog(ERROR, "invalid compression level: %d", compression_level);

	uncompressed = PG_GETARG_BYTEA_P(0);
	in = (const void*)(VARDATA_ANY(uncompressed));
	ilen = VARSIZE_ANY_EXHDR(uncompressed);

	dlen = ilen + BLOCK_HEADER_LENGTH + BLOCK_FOOTER_LENGTH; // yup, bigger, that'll fit them all
	compressed = (bytea *)palloc(dlen + VARHDRSZ); 

	memset(&options, 0, sizeof(options));
	options.sizeof_options = sizeof(options);
	options.malloc_func = palloc0;
	options.free_func = pfree;
	
	z = libdeflate_alloc_compressor_ex(compression_level, &options);
	if (!z)
	  goto bailout;

	// Raw deflate-gzip
	if ( (dlen = libdeflate_gzip_compress(z, in, ilen, VARDATA(compressed), dlen)) <= 0) {
	  W("libdeflate_gzip_compress failed");
	  goto bailout;
	}

	libdeflate_free_compressor(z);

	SET_VARSIZE(compressed, dlen + VARHDRSZ);
	PG_RETURN_BYTEA_P(compressed);

bailout:

	if(compressed) pfree(compressed);

	if(z) libdeflate_free_compressor(z);
	PG_RETURN_NULL();
}
