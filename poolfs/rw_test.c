// Copyright (C) 2021 Adam Manzanares <a.manzanares@samsung.com>

#include <libflexalloc.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "poolfs.h"

#define POOL_NAME "TEST"
#define OBJ_NAME "TEST_OBJ"
#define USAGE "./test OBJ_SZ"

size_t ap_pos;

int poolfs_buf_verify(char *obj_buf, unsigned int len)
{
  for (int i = 0; i < len; i++)
  {
    if (obj_buf[i] != (char)i)
    {
      printf("Mismatch at offset:%u\n", i);
      return 1;
    }
  }

  return 0;
}

int verify(uint64_t oh, char *buf, unsigned int obj_sz, struct poolfs_handle *pfsh)
{
  int ret;

  memset(buf, 0, obj_sz);
  ret = poolfs_object_read(oh, buf, 0, ap_pos, pfsh);
  if (ret != ap_pos)
  {
    printf("Error reading %luB from start of object\n", ap_pos);
    goto out;
  }

  ret = poolfs_buf_verify(buf, ap_pos);
  if (ret)
    printf("Error verifying first %luB\n", ap_pos);

out:
  return ret;
}

int append_and_verify(uint64_t oh, char *buf, size_t len, unsigned int obj_sz,
                      struct poolfs_handle *pfsh)
{
  int ret;

  for (size_t i = 0; i < len; i++)
    buf[i + ap_pos] = (char)(i+ap_pos);

  ret = poolfs_object_write(oh, buf + ap_pos, ap_pos, len, pfsh);
  if (ret)
  {
    printf("Error writing:%luB off:%lu\n", len, ap_pos);
    goto out;
  }

  ap_pos += len;
  ret = verify(oh, buf, obj_sz, pfsh);

out:
  return ret;
}

int main(int argc, char **argv)
{
  struct poolfs_handle *pfsh;
  uint64_t oh;
  int ret = 0;
  char *dev_uri = getenv("POOLFS_TEST_DEV");
  char *md_dev_uri = getenv("POOLFS_TEST_MD_DEV");
  unsigned int obj_sz;
  char *endofarg, *obj_buf = NULL;

  if (!dev_uri)
  {
    printf("Set env var POOLFS_TEST_DEV in order to run test\n");
    return 0;
  }

  if (argc != 2)
  {
    printf("Object size not supplied, Usasge:\n%s\n", USAGE);
    return 0;
  }

  errno = 0;
  obj_sz = strtoul(argv[1], &endofarg, 0);
  if (endofarg == argv[1])
  {
    printf("Number not found in argument\n");
    return 0;
  }

  if (errno)
  {
    perror("strotoul");
    return 0;
  }

  // Open flexalloc device, create pool, create named object and close
  printf("Opening poolfs, and creating \"test\" objects\n");
  if (md_dev_uri)
    ret = poolfs_init(dev_uri, md_dev_uri, POOL_NAME, obj_sz, &pfsh);
  else
    ret = poolfs_init(dev_uri, NULL, POOL_NAME, obj_sz, &pfsh);

  if (ret)
    goto out;

  obj_buf = poolfs_buf_alloc(obj_sz, pfsh);
  if (!obj_buf)
    goto out_close;

  ret = poolfs_object_open(OBJ_NAME, pfsh, &oh, POOLFS_OPEN_FLAG_CREATE | POOLFS_OPEN_FLAG_WRITE);
  if (ret)
  {
    printf("Error opening object:%s\n", OBJ_NAME);
    goto out_close;
  }

  ap_pos = 0;
  if (append_and_verify(oh, obj_buf, 2048, obj_sz, pfsh)) // AL START, UNAL END, Single Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 4096, obj_sz, pfsh)) // UNAL START, UNAL END, Spans two blocks
    goto out_close;

  if (append_and_verify(oh, obj_buf, 2048, obj_sz, pfsh)) // UNAL START, AL END, Single Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 9192, obj_sz, pfsh)) // AL START, UNAL END, Multi Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 8192, obj_sz, pfsh)) // UNAL START, UNAL END, Multi Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 11288, obj_sz, pfsh)) // UNAL START, AL END, Multi Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 4096, obj_sz, pfsh)) // AL START, AL END, Single Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 4096, obj_sz, pfsh)) // AL START, AL END, Single Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 6, obj_sz, pfsh)) // AL START, UNAL END, Single Block
    goto out_close;

  if (append_and_verify(oh, obj_buf, 12276, obj_sz, pfsh)) // UNAL START, UNAL END, Spans multiple blocks
    goto out_close;

  printf("Total data written:%luB\n", ap_pos);
  poolfs_object_close(oh, pfsh);
  printf("About to poolfs close\n");
  poolfs_close(pfsh);
  // Re open poolfs
  printf("Re opening poolfs\n");
  if (md_dev_uri)
    ret = poolfs_init(dev_uri, md_dev_uri, POOL_NAME, obj_sz, &pfsh);
  else
    ret = poolfs_init(dev_uri, NULL, POOL_NAME, obj_sz, &pfsh);

  if (ret)
    goto out;

  ret = poolfs_object_open(OBJ_NAME, pfsh, &oh, POOLFS_OPEN_FLAG_READ);
  if (ret)
  {
    printf("Error opening object:%s\n", OBJ_NAME);
    goto out_close;
  }

  ret = verify(oh, obj_buf, obj_sz, pfsh);

out_close:
  printf("About to poolfs close\n");
  poolfs_close(pfsh);
  free(obj_buf);

out:
  return ret;
}
