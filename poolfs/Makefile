all: poolfs.o libpoolfs.so test bw_test rw_test poolfs_fuse

libpoolfs.so:
	gcc -g -o libpoolfs.so -lflexalloc poolfs.o -shared

poolfs.o:
	gcc -g -lflexalloc -fPIC -Wall -c poolfs.c

test:
	gcc -g -Wall test.c -o poolfs_test -L. -lpoolfs -lflexalloc

rw_test:
	gcc -g -Wall rw_test.c -o poolfs_rwtest -L. -lpoolfs -lflexalloc

bw_test:
	gcc -g -Wall bw_test.c -o poolfs_bwtest -L. -lpoolfs -lflexalloc

poolfs_fuse:
	gcc -g -Wall poolfs_fuse.c `pkg-config fuse3 --cflags --libs` -L. -lpoolfs -lflexalloc -o poolfs_fuse

clean:
	rm -f libpoolfs.so poolfs.o poolfs_test poolfs_bwtest poolfs_rwtest poolfs_fuse

install:
	cp libpoolfs.so /usr/local/lib/x86_64-linux-gnu/
	cp poolfs.h /usr/local/include/
