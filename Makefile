# Makefile to build the pg_amqp extension

EXTENSION = pg_bgzip

DATA_built = $(EXTENSION)--1.0.sql
DATA = $(wildcard $(EXTENSION)--*--*.sql)

# compilation configuration
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))

#PG_CPPFLAGS = -std=gnu18

#PG_CPPFLAGS += -Wall -Wextra -Werror -Wno-unused-parameter -Wno-maybe-uninitialized -Wno-implicit-fallthrough 
PG_CPPFLAGS += -Isrc -I$(libpq_srcdir) $(shell pkg-config --cflags libdeflate)
SHLIB_LINK = $(libpq) $(shell pkg-config --libs libdeflate)
#EXTRA_CLEAN += $(addprefix src/,*.gcno *.gcda) # clean up after profiling runs

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@
