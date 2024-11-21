-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_bgzip" to load this file. \quit

CREATE SCHEMA bgzip;

CREATE FUNCTION bgzip.compress(content bytea, level integer DEFAULT 9, eof boolean DEFAULT FALSE)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_bgzip_compress'
LANGUAGE C STABLE PARALLEL SAFE -- IMMUTABLE -- STRICT
--COST 1000
; 
COMMENT ON FUNCTION bgzip.compress(bytea,integer,boolean) IS 'compress the given content';

-- CREATE FUNCTION bgzip.uncompress(content bytea)
-- RETURNS bytea
-- AS 'MODULE_PATHNAME', 'pg_bgzip_uncompress'
-- LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT
-- COST 1000; 
-- COMMENT ON FUNCTION bgzip.uncompress(bytea,integer) IS 'uncompress the given content';


CREATE FUNCTION bgzip.gzip_compress(content bytea, level integer DEFAULT 9)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pg_bgzip_gzip_compress'
LANGUAGE C STABLE PARALLEL SAFE -- IMMUTABLE -- STRICT
--COST 1000
; 
COMMENT ON FUNCTION bgzip.gzip_compress(bytea,integer) IS 'gzip-compress the given content';
