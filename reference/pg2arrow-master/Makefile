PG_CONFIG := pg_config
PROGRAM    = pg2arrow

OBJS = pg2arrow.o query.o arrow_types.o arrow_read.o arrow_write.o arrow_dump.o
PG_CPPFLAGS = -I$(shell $(PG_CONFIG) --includedir)
PG_LIBS = -lpq

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
