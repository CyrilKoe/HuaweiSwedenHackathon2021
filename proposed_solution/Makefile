NAME = edge-impulse-standalone

CC ?= gcc

CFLAGS += -Wall -g
LDFLAGS += -ljson-c -lm

CSOURCES = $(wildcard proposed_solution/*.c)
COBJECTS := $(patsubst %.c,%.o,$(CSOURCES))

app: ${COBJECTS}
	$(C) $(COBJECTS) -o app.out $(LDFLAGS)