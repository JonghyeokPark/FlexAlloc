// Copyright (C) 2021 Adam Manzanares <a.manzanares@samsung.com>

#include <libflexalloc.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "poolfs.h"

#define POOL_NAME "TEST"
#define USAGE "./test OBJ_SZ"

int main(int argc, char **argv)
{
  struct poolfs_handle *pfsh;
  uint64_t oh;
  int ret = 0, count = 0, rdcount = 0;
  char *dev_uri = getenv("POOLFS_TEST_DEV"), *obj_name = NULL;
  char *md_dev_uri = getenv("POOLFS_TEST_MD_DEV");
  unsigned int obj_sz;
  unsigned int mid;
  char *endofarg;

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

  mid = obj_sz / 2;
  // Open flexalloc device, create pool, create named object and close
  printf("Opening poolfs, and creating \"test\" objects\n");
  if (md_dev_uri)
    ret = poolfs_init(dev_uri, md_dev_uri, POOL_NAME, obj_sz, &pfsh);
  else
    ret = poolfs_init(dev_uri, NULL, POOL_NAME, obj_sz, &pfsh);

  if (ret)
    goto out;

  obj_name = poolfs_buf_alloc(obj_sz, pfsh);
  if (!obj_name)
    goto out_close;

  do
  {
    snprintf(obj_name, POOLFS_OBJ_NAME_LEN_MAX, "%s.%d", POOL_NAME, count);
    ret = poolfs_object_open(obj_name, pfsh, &oh, POOLFS_OPEN_FLAG_CREATE | POOLFS_OPEN_FLAG_WRITE);
    if (ret)
    {
      break;
    }

    count++;
	printf("Created:%d objects\n", count);

    ret = poolfs_object_write(oh, obj_name, 0, mid - 1, pfsh);
    ret = poolfs_object_read(oh, obj_name, 0, mid - 1, pfsh);
    ret = poolfs_object_write(oh, obj_name + mid - 1, mid - 1, mid + 1, pfsh);
    poolfs_object_close(oh, pfsh);
  } while (!ret);

  printf("Created %d objects\n", count);
  printf("Listing all objects in the pool\n");
  for (rdcount = 0; rdcount < count; rdcount++)
  {
    snprintf(obj_name, POOLFS_OBJ_NAME_LEN_MAX, "%s.%d", POOL_NAME, rdcount);
    if (!(rdcount % 1024))
      printf("Objects found so far:%d\n",rdcount);

    ret = poolfs_object_open(obj_name, pfsh, &oh, POOLFS_OPEN_FLAG_READ);
    if (ret)
      break;

    ret = poolfs_object_read(oh, obj_name, 0, mid - 1, pfsh);
    ret = poolfs_object_read(oh, obj_name, mid - 1, mid + 1, pfsh);
    if (ret != mid + 1)
      break;

    poolfs_object_close(oh, pfsh);
  }

  printf("\nFound %d objects in the pool\n", rdcount);
  printf("Closing poolfs and flexalloc\n");
  poolfs_close(pfsh);
  printf("Reopening poolfs\n");
  if (md_dev_uri)
    ret = poolfs_init(dev_uri, md_dev_uri, POOL_NAME, obj_sz, &pfsh);
  else
    ret = poolfs_init(dev_uri, NULL, POOL_NAME, obj_sz, &pfsh);

  if (ret)
    goto out;
  printf("Listing all objects in the pool\n");
  for (rdcount = 0; rdcount < count; rdcount++)
  {
    snprintf(obj_name, POOLFS_OBJ_NAME_LEN_MAX, "%s.%d", POOL_NAME, rdcount);
    if (!(rdcount % 1024))
      printf("Objects found so far:%d\n", rdcount);

    ret = poolfs_object_open(obj_name, pfsh, &oh, POOLFS_OPEN_FLAG_READ);
    if (ret)
      break;

    ret = poolfs_object_read(oh, obj_name, 0, obj_sz, pfsh);
    if (ret != obj_sz)
      break;

    poolfs_object_close(oh, pfsh);
  }

  printf("\n");
  printf("\nFound %d objects in the pool\n", rdcount);

out_close:
  poolfs_close(pfsh);

out:
  if (obj_name)
    free(obj_name);

  return ret;
}
