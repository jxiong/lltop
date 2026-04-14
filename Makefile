CC = gcc
CPPFLAGS = $(CDEBUG)
CFLAGS = -Wall 
lltop_objects = main.o hooks.o rbtree.o

all: lltop

lltop: $(lltop_objects)
	$(CC) $(CFLAGS) $^ -o $@ 

clean:
	rm -f lltop $(lltop_objects)
