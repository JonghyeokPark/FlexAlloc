// Copyright (C) 2021 Adam Manzanares <a.manzanares@samsung.com>

#ifndef __LIBPOOLFS_H_
#define __LIBPOOLFS_H_
#include <limits.h>
#include <libflexalloc.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define POOLFS_OBJ_NAME_LEN_MAX 112
#define POOLFS_DHANDLE_NONE UINT_MAX
#define POOLFS_OPEN_FLAG_CREATE 0x1
#define POOLFS_OPEN_FLAG_READ 0x2
#define POOLFS_OPEN_FLAG_WRITE 0x4
#define POOLFS_MAX_OPEN_OBJECTS 16384
#define POOLFS_APPEND_SIZE 2097152

struct pfs_oinfo
{
  uint64_t size;
  struct fla_object fla_oh;
  char name[POOLFS_OBJ_NAME_LEN_MAX];
};

struct pfs_dirhandle
{
  char *buf;
  unsigned int cur;
  struct fla_object fla_oh;
};

struct poolfs_handle
{
  struct flexalloc *fs;
  struct fla_pool *ph;
  uint32_t append_sz;
  bool is_zns;
  bool is_dirty;
};

struct poolfs_ohandle
{
	struct pfs_oinfo oinfo;
	struct fla_object pfs_oh;
	uint32_t oinfo_off;
	char *append_buf;
	char *read_buf;
	uint64_t append_off;
	uint64_t read_buf_off;
	uint32_t use_count;
	uint32_t o_flags;
	bool frozen;
};


int poolfs_init(const char *dev_uri, const char *mddev_uri, const char *pool_name, uint64_t obj_sz,
                struct poolfs_handle **pfsh);
void poolfs_reset_pool_dir();
struct pfs_oinfo* poolfs_get_oinfo(struct poolfs_handle *pfsh, bool create);
int poolfs_object_open(const char *filename, struct poolfs_handle *pfsh, uint64_t *oh, int flags);
int poolfs_object_delete(const char *filename, struct poolfs_handle *pfsh);
ssize_t poolfs_object_read(uint64_t oh, void *buf, size_t offset, size_t len, struct poolfs_handle *pfsh);
int poolfs_object_write(uint64_t oh, void *buf, size_t offset, size_t len, struct poolfs_handle *pfsh);
int poolfs_object_close(uint64_t oh, struct poolfs_handle *pfsh);
void poolfs_close(struct poolfs_handle *pfsh);
void *poolfs_buf_alloc(size_t nbytes, struct poolfs_handle *pfsh);
uint32_t poolfs_dev_bs(struct poolfs_handle *pfsh);
int poolfs_object_rename(const char *oldname, const char *newname, struct poolfs_handle *pfsh);
int poolfs_sync(struct poolfs_handle *pfsh);
bool poolfs_is_zns(struct poolfs_handle *pfsh);
struct pfs_oinfo *poolfs_find_oinfo(struct poolfs_handle *pfsh, const char *name);
int poolfs_pool_set_strp(struct poolfs_handle *pfsh, uint32_t num_strp, uint32_t strp_sz);
#ifdef __cplusplus
}
#endif

#endif // __LIBPOOLFS_H_

