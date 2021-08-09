// Copyright (C) 2021 Adam Manzanares <a.manzanares@samsung.com>

#define FUSE_USE_VERSION 39
#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <assert.h>
#include <libgen.h>

#include "poolfs.h"


static struct poolfs_fuse_info {
	const char *dev_uri;
	const char *mddev_uri;
	const char *poolname;
	uint64_t obj_sz;
	int usage;
} pf_info;

static const struct fuse_opt cmdln_opts[] = {
	{ "--dev_uri=%s", offsetof(struct poolfs_fuse_info, dev_uri), 1},
	{ "--mddev_uri=%s", offsetof(struct poolfs_fuse_info, mddev_uri), 1},
	{ "--poolname=%s", offsetof(struct poolfs_fuse_info, poolname), 1},
	{ "--obj_size=%lu", offsetof(struct poolfs_fuse_info, obj_sz), 1},
	{ "-h", offsetof(struct poolfs_fuse_info, usage), 1},
	{ "--help", offsetof(struct poolfs_fuse_info, usage), 1},
	FUSE_OPT_END
};

struct poolfs_handle *pfsh;

static void *poolfs_fuse_init(struct fuse_conn_info *conn,
			struct fuse_config *cfg)
{
	int ret;
	(void) conn;
	cfg->kernel_cache = 1;

	ret = poolfs_init(pf_info.dev_uri, pf_info.mddev_uri, pf_info.poolname,
			  pf_info.obj_sz, &pfsh);
	if (ret)
		printf("Something went wrong during init\n");

	return NULL;
}

static int poolfs_fuse_getattr(const char *path, struct stat *stbuf,
			       struct fuse_file_info *fi)
{
	int res = 0;
	struct pfs_oinfo *oinfo;
	(void) fi;
	char *path_dup = strdup(path);

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
		goto out;
	}

	oinfo = poolfs_find_oinfo(pfsh, basename(path_dup));
	if (oinfo) {
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
		stbuf->st_size = oinfo->size;
		stbuf->st_blocks = (oinfo->size / 512) + 1;
		goto out;
	}

	res = -ENOENT;
out:
	if (path_dup)
		free(path_dup);
	return res;
}

static int poolfs_fuse_readdir(const char *path, void *buf,
			       fuse_fill_dir_t filler, off_t offset,
			       struct fuse_file_info *fi,
			       enum fuse_readdir_flags flags)
{
	struct pfs_oinfo *oinfo;
	(void) offset;
	(void) fi;
	(void) flags;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	poolfs_reset_pool_dir();
	while ((oinfo = poolfs_get_oinfo(pfsh, false)) != NULL)
	{
		if (strlen(oinfo->name) > 0) {
			if (filler(buf, basename(oinfo->name), NULL, 0, 0))
				printf ("Filler had an issue\n");
		}
	}

	return 0;
}

static int poolfs_fuse_open(const char *path, struct fuse_file_info *fi)
{
	int ret = 0;

	if ((fi->flags & O_ACCMODE) != O_RDONLY)
		return -EACCES;

	ret = poolfs_object_open(path, pfsh, &(fi->fh), 0);
	if (ret)
		return -EINVAL;

	return ret;
}

static int poolfs_fuse_read(const char *path, char *buf, size_t size,
			    off_t offset, struct fuse_file_info *fi)
{
	ssize_t len;

	if (offset + size > pf_info.obj_sz)
		size = pf_info.obj_sz - offset;

	len = poolfs_object_read(fi->fh, buf, offset, size, pfsh);
	if (len < 0)
		return 0;

	return len;
}

static int poolfs_fuse_release(const char *path, struct fuse_file_info *fi)
{
	poolfs_object_close(fi->fh, pfsh);
	return 0;
}

static const struct fuse_operations poolfs_oper = {
	.init		= poolfs_fuse_init,
	.getattr	= poolfs_fuse_getattr,
	.readdir	= poolfs_fuse_readdir,
	.open		= poolfs_fuse_open,
	.read		= poolfs_fuse_read,
	.release	= poolfs_fuse_release,
};

static void usage(const char *progname)
{
	printf("usage: %s [options] <mountpoint>\n\n", progname);
	printf("File-system specific options:\n"
	       "    --dev_uri=<s>	Name of the poolfs dev file\n"
	       "			(default: \"NULL\")\n"
	       "    --mddev_uri=<s>	Name of the poolfs md dev file\n"
	       "			(default: \"NULL\")\n"
	       "    --poolname=<s>	Name of the poolfs pool\n"
	       "			(default: \"NULL\")\n"
	       "    --obj_sz=<uint64>	Number of objects in the poolfs pool\n"
	       "			(default: \"NULL\")\n"
	       "\n");
}

int main(int argc, char *argv[])
{
	int ret;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	if (fuse_opt_parse(&args, &pf_info, cmdln_opts, NULL) == -1)
		return 1;

	if (pf_info.usage) {
		usage(argv[0]);
		assert(fuse_opt_add_arg(&args, "--help") == 0);
		args.argv[0][0] = '\0';
	}

	ret = fuse_main(args.argc, args.argv, &poolfs_oper, NULL);
	fuse_opt_free_args(&args);
	return ret;
}
