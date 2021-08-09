# Notes

I am currently using a 4MiB loopback device with a blocksize of 4K.

```
losetup -fP -b 4096 ~/md.img
```

# Commands to run

```
make; make test; make poolfs_fuse
mkfs.flexalloc -s 64 /dev/loop0
LD_LIBRARY_PATH=. POOLFS_TEST_DEV=/dev/loop0 ./poolfs_test
modprobe fuse
LD_LIBRARY_PATH=. ./poolfs_fuse --dev_uri=/dev/loop0 --poolname=TEST --obj_size=8192 /mnt/
ls /mnt
umount /mnt
```
