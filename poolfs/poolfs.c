// Copyright (C) 2021 Adam Manzanares <a.manzanares@samsung.com>

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <libgen.h>

#include "poolfs.h"

struct pfs_dirhandle pfs_dir;
unsigned int num_objects;
unsigned int pfs_obj_sz;
unsigned int pfs_oi_ob;
struct poolfs_handle *root;

struct poolfs_ohandle pfs_otable[POOLFS_MAX_OPEN_OBJECTS];

void poolfs_cleanup(void)
{
  poolfs_close(root);
}

int poolfs_pool_set_strp(struct poolfs_handle *pfsh, uint32_t num_strp, uint32_t strp_sz)
{
  return fla_pool_set_strp(pfsh->fs, pfsh->ph, num_strp, strp_sz);
}

int poolfs_init(const char *dev_uri, const char *mddev_uri, const char *pool_name, uint64_t objsz,
                struct poolfs_handle **pfsh)
{
  struct fla_object robj;
  int ret = 0;
  uint32_t obj_nlb;

  *pfsh = malloc(sizeof(struct poolfs_handle));
  memset(*pfsh, 0, sizeof(struct poolfs_handle));
  memset(pfs_otable, 0, sizeof(struct poolfs_ohandle) * POOLFS_MAX_OPEN_OBJECTS);

  if (mddev_uri)
    ret = fla_md_open(dev_uri, mddev_uri, &((*pfsh)->fs));
  else
    ret = fla_open(dev_uri, &((*pfsh)->fs));

  if (ret)
  {
    printf("Error fla opening dev:%s,md_dev:%s\n", dev_uri, mddev_uri);
    goto out_free;
  }

  (*pfsh)->is_zns = fla_fs_zns((*pfsh)->fs);
  if (objsz > POOLFS_APPEND_SIZE)
    (*pfsh)->append_sz = POOLFS_APPEND_SIZE;
  else
    (*pfsh)->append_sz = objsz;

  obj_nlb = objsz / fla_fs_lb_nbytes((*pfsh)->fs);
  ret = fla_pool_open((*pfsh)->fs, pool_name, &((*pfsh)->ph));
  if (ret == -1)
	  ret = fla_pool_create((*pfsh)->fs, pool_name, strlen(pool_name), obj_nlb, &((*pfsh)->ph));

  if (ret)
  {
    printf("Error opening fla pool:%s\n", pool_name);
    goto out_free;
  }

  pfs_obj_sz = obj_nlb * fla_fs_lb_nbytes((*pfsh)->fs);
  pfs_oi_ob = pfs_obj_sz / sizeof(struct pfs_oinfo);
  pfs_dir.buf = fla_buf_alloc((*pfsh)->fs, pfs_obj_sz);
  if (!pfs_dir.buf)
  {
    printf("fla_buf_alloc\n");
    goto out_free;
  }

  memset((void *)pfs_dir.buf, 0, pfs_obj_sz);
  pfs_dir.cur = POOLFS_DHANDLE_NONE;
  num_objects = 0;

  ret = fla_pool_get_root_object((*pfsh)->fs, (*pfsh)->ph, &robj);
  pfs_dir.fla_oh = robj;
  if (ret) // Root object not set
  {
    // Grab a object from the pool, zero it out, and set it to root
    ret = fla_object_create((*pfsh)->fs, (*pfsh)->ph, &robj);
    if (ret)
    {
      printf("Error allocating root object\n");
      goto out_free_buf;
    }

    if (!(*pfsh)->is_zns)
    {
      ret = fla_object_write((*pfsh)->fs, (*pfsh)->ph, &robj, pfs_dir.buf, 0, pfs_obj_sz);
      if (ret)
      {
        printf("Error initial write md object \n");
        goto out_free_buf;
      }
    }

    // Set the acquired object to the root object
    ret =  fla_pool_set_root_object((*pfsh)->fs, (*pfsh)->ph, &robj, false);
    pfs_dir.fla_oh = robj;
    (*pfsh)->is_dirty = false; // Metadata is currently clean
  }
  else
  {
    ret = fla_object_read((*pfsh)->fs, (*pfsh)->ph, &robj, pfs_dir.buf, 0, pfs_obj_sz);
  }

  pfs_dir.cur = 0;
  root = *pfsh;
  atexit(poolfs_cleanup);
  return ret;

out_free_buf:
  free(pfs_dir.buf);
out_free:
  free(*pfsh);

  return ret;
}

void poolfs_reset_pool_dir()
{
  pfs_dir.cur = POOLFS_DHANDLE_NONE;
}

int poolfs_init_dirhandle(struct poolfs_handle *pfsh)
{
  struct fla_object robj;
  int ret = 0;

  if (pfs_dir.cur == POOLFS_DHANDLE_NONE)
  {
    pfs_dir.cur = 0;
    if (!pfsh->is_zns)
    {
      ret = fla_pool_get_root_object(pfsh->fs, pfsh->ph, &robj);
      if (ret)
      {
        printf("Error getting root object\n");
        goto out;
      }

      ret = fla_object_read(pfsh->fs, pfsh->ph, &robj, pfs_dir.buf, 0, pfs_obj_sz);
      if (ret)
      {
        printf("Error reading root object\n");
        goto out;
      }

      pfs_dir.fla_oh = robj;
    }
  }

out:
  return ret;

}

struct pfs_oinfo* poolfs_get_oinfo(struct poolfs_handle *pfsh, bool create)
{
  int ret = 0;
  struct pfs_oinfo *oinfo;

  ret = poolfs_init_dirhandle(pfsh);
  if (ret)
  {
    printf("Error initializing dirhandle\n");
    return NULL;
  }

  // We ran out of md objects
  if(pfs_dir.cur && !(pfs_dir.cur % pfs_oi_ob))
    return NULL;

  if (!((pfs_dir.cur + 1) % pfs_oi_ob))
  {
    oinfo = ((struct pfs_oinfo *)pfs_dir.buf + (pfs_oi_ob - 1));
    // Check if next entry is a pointer to another object
    if (oinfo->size == UINT64_MAX)
    {
      pfs_dir.fla_oh = oinfo->fla_oh;
      fla_object_read(pfsh->fs, pfsh->ph, &oinfo->fla_oh, pfs_dir.buf, 0, pfs_obj_sz);
      // Skip over the oinfo object which points to the next object used for MD
      pfs_dir.cur++;
    }
    else if(create)// Lets try to allocate a new object
    {
      struct fla_object new_md_obj;
      // Grab a object from the pool, zero it out, and set it to root
      ret = fla_object_create(pfsh->fs, pfsh->ph, &new_md_obj);
      if (!ret)
      {
        oinfo->size = UINT64_MAX;
        oinfo->fla_oh = new_md_obj;
        fla_object_write(pfsh->fs, pfsh->ph, &pfs_dir.fla_oh, pfs_dir.buf, 0, pfs_obj_sz);
        memset((void *)pfs_dir.buf, 0, pfs_obj_sz);
        pfs_dir.fla_oh = new_md_obj;
        // Skip over the oinfo object which points to the next MD object
        pfs_dir.cur++;
      }
      else
      {
        printf("Error allocating MD object bailing\n");
        exit(0);
      }
    }
  }

  oinfo = ((struct pfs_oinfo *)pfs_dir.buf + (pfs_dir.cur % pfs_oi_ob));
  pfs_dir.cur++;
  return oinfo;
}

int poolfs_object_create(const char *name, struct poolfs_handle *pfsh, struct pfs_oinfo *oinfo)
{
  int ret;
  unsigned int namelen = strlen(name);
  struct fla_object fla_obj;

  if (namelen > POOLFS_OBJ_NAME_LEN_MAX)
  {
    printf("poolfs obj create Name:%s longer than max len of:%d\n", name,  POOLFS_OBJ_NAME_LEN_MAX);
    return -EINVAL;
  }

  ret = fla_object_create(pfsh->fs, pfsh->ph, &fla_obj);
  if (ret)
  {
    printf("Error allocating fla object\n");
    return ret;
  }

  memcpy(oinfo->name, name, namelen + 1);
  oinfo->size = 0;
  oinfo->fla_oh = fla_obj;

  if (!pfsh->is_zns)
  {
    // TOOD make sure this is safe with object handle table now having private copies
    ret = fla_object_write(pfsh->fs, pfsh->ph, &pfs_dir.fla_oh, pfs_dir.buf, 0, pfs_obj_sz);
    if (ret)
    {
      printf("Error writing md for object :%d\n", num_objects);
      exit(0);
    }
  }
  else
    pfsh->is_dirty = true;

  num_objects++;

  return ret;

}

struct pfs_oinfo *poolfs_find_oinfo(struct poolfs_handle *pfsh, const char *name)
{
  struct pfs_oinfo *oinfo;

  poolfs_reset_pool_dir();
  while (((oinfo = poolfs_get_oinfo(pfsh, false)) != NULL) &&
         strncmp(oinfo->name, name, POOLFS_OBJ_NAME_LEN_MAX));

  return oinfo;
}

struct pfs_oinfo *poolfs_find_first_free_oinfo(struct poolfs_handle *pfsh)
{
  struct pfs_oinfo *oinfo;

  poolfs_reset_pool_dir();
  while (((oinfo = poolfs_get_oinfo(pfsh, true)) != NULL) &&
         strlen(oinfo->name) > 0);

  return oinfo;
}

uint64_t poolfs_otable_search(const char *name, uint64_t *ff)
{
  bool oh_set = false;
  uint64_t oh_num = POOLFS_MAX_OPEN_OBJECTS;

  // Seach through the open objects table
  for (oh_num = 0; oh_num < POOLFS_MAX_OPEN_OBJECTS; oh_num++)
  {
    // Blank entry
    if (!strlen(pfs_otable[oh_num].oinfo.name))
    {
      if (!oh_set)
      {
        if (ff)
          *ff = oh_num;
        
        oh_set = true;
      }

      continue;

    }

    // Entry with a matching name return the entry
    if (!strncmp(name, pfs_otable[oh_num].oinfo.name, POOLFS_OBJ_NAME_LEN_MAX))
    {
      return oh_num;
    }
  }

  if (!oh_set)
    *ff = POOLFS_MAX_OPEN_OBJECTS;

  return oh_num;
}

int poolfs_object_open(const char *name, struct poolfs_handle *pfsh, uint64_t *oh, int flags)
{
  struct pfs_oinfo *oinfo;
  int ret = 0;
  uint64_t oh_num;
  uint64_t ff_oh;
  struct fla_object *noh;
  uint32_t bs = pfsh->append_sz;

  oh_num = poolfs_otable_search(basename((char *)name), &ff_oh);
 
  // The object is already open increase ref count and return object handle
  if (oh_num < POOLFS_MAX_OPEN_OBJECTS)
  {
    *oh = oh_num;
    pfs_otable[oh_num].use_count++;
    return 0;
  }

  if (ff_oh == POOLFS_MAX_OPEN_OBJECTS)
  {
    printf("Open objects hit limit of %d\n", POOLFS_MAX_OPEN_OBJECTS);
    return -EINVAL;
  }

  // Search through all the on disk MD
  oinfo = poolfs_find_oinfo(pfsh, basename((char *)name));
  if (!oinfo && (flags & POOLFS_OPEN_FLAG_CREATE))
    oinfo = poolfs_find_first_free_oinfo(pfsh);

  if (!oinfo)
    return -EINVAL;

  if ((strlen(oinfo->name) == 0) && (flags & POOLFS_OPEN_FLAG_CREATE))
  {
    ret = poolfs_object_create(basename((char *)name), pfsh, oinfo);
    if (ret)
    {
      printf("poolfs object open: object create error\n");
      return ret;
    }
  }

  memcpy(&pfs_otable[ff_oh].oinfo, oinfo, sizeof(struct pfs_oinfo));
  pfs_otable[ff_oh].pfs_oh = pfs_dir.fla_oh;
  pfs_otable[ff_oh].oinfo_off = (pfs_dir.cur - 1) % pfs_oi_ob; // Convert to byte offset for update
  if (flags & POOLFS_OPEN_FLAG_WRITE)
    pfs_otable[ff_oh].append_off = pfs_otable[ff_oh].oinfo.size; // Verify zero on new entry
  else if (flags & POOLFS_OPEN_FLAG_READ)
    pfs_otable[ff_oh].read_buf_off = UINT64_MAX;

  pfs_otable[ff_oh].use_count++;
  pfs_otable[ff_oh].append_buf = fla_buf_alloc(pfsh->fs, pfsh->append_sz);

  if (flags & POOLFS_OPEN_FLAG_READ)
    pfs_otable[ff_oh].read_buf = pfs_otable[ff_oh].append_buf;
  
  pfs_otable[ff_oh].o_flags = flags;

  memset(pfs_otable[ff_oh].append_buf, 0, bs);

  if (!pfs_otable[ff_oh].append_buf)
  {
    printf("Object open unable to allocate append buf\n");
    return -EINVAL;
  }

  // Read the data into the append buffer
  if (pfs_otable[ff_oh].append_off < pfs_obj_sz * 64 && flags && POOLFS_OPEN_FLAG_WRITE)
  {
    noh = &oinfo->fla_oh;
    fla_object_read(pfsh->fs, pfsh->ph, noh, pfs_otable[ff_oh].append_buf,
                    (pfs_otable[ff_oh].append_off / bs) * bs, bs);
  }

  // Freeze a zns file that has been previously appended
  if (pfs_otable[ff_oh].append_off % pfsh->append_sz && pfsh->is_zns)
    pfs_otable[ff_oh].frozen = true;

  *oh = ff_oh;
  return ret;
}

int poolfs_object_delete(const char *name, struct poolfs_handle *pfsh)
{
  uint64_t oh = poolfs_otable_search(basename((char *)name), NULL);
  struct pfs_oinfo *oinfo = poolfs_find_oinfo(pfsh, basename((char *)name));
  
  // Invalidate any open handles, but keep use count to free ohandle when all openers have closed
  if (oh != POOLFS_MAX_OPEN_OBJECTS)
  {
    free(pfs_otable[oh].append_buf);
    memset(&pfs_otable[oh], 0, sizeof(struct poolfs_ohandle) - sizeof(uint32_t));
  }

  if (!oinfo)
  {
    return -EINVAL;
  }

  memset(oinfo->name, 0, POOLFS_OBJ_NAME_LEN_MAX);
  oinfo->size = 0;
  oinfo->fla_oh.slab_id = UINT32_MAX;
  oinfo->fla_oh.entry_ndx = UINT32_MAX;
  num_objects--;

  if (!pfsh->is_zns)
  {
    if (fla_object_write(pfsh->fs, pfsh->ph, &pfs_dir.fla_oh, pfs_dir.buf, 0, pfs_obj_sz))
    {
      printf("Error writing md during delete for object:%s\n", name);
      exit(0);
    }
  }
  else
    pfsh->is_dirty = true;

  return 0;
}

void poolfs_otable_close(struct poolfs_handle *pfsh)
{
  uint64_t oh_num;

  for (oh_num = 0; oh_num < POOLFS_MAX_OPEN_OBJECTS; oh_num++)
  {
    if (strlen(pfs_otable[oh_num].oinfo.name))
      poolfs_object_close(oh_num, pfsh);
  }
}

void poolfs_close(struct poolfs_handle *pfsh)
{
  if (!pfsh)
    return;

  poolfs_otable_close(pfsh);

  if (pfsh->is_zns && !pfsh->is_dirty) // Also clean case, TODO make this independent of ZNS
    goto free;

  if (fla_object_write(pfsh->fs, pfsh->ph, &pfs_dir.fla_oh, pfs_dir.buf, 0, pfs_obj_sz))
  {
    printf("Error writing md during close\n");
    exit(0);
  }

free:
  if (pfs_dir.buf)
    fla_buf_free(pfsh->fs, pfs_dir.buf);
  
  fla_pool_close(pfsh->fs, pfsh->ph);
  fla_close(pfsh->fs);
  free(pfsh);
  root = NULL;
}

int poolfs_object_unaligned_read(uint64_t oh, struct fla_object *noh, void *buf,
                                 size_t offset, size_t len, struct poolfs_handle *pfsh)
{
  void *al_buf;
  size_t al_offset, al_len, tail_len = 0, tail_start = 0;
  int ret = 0;
  uint32_t bs = pfsh->append_sz;

  al_offset = offset - (offset % bs);
  al_len = len + (bs - (len % bs));
  if (al_offset + al_len < offset + len)
	  al_len += bs;

  al_buf = poolfs_buf_alloc(al_len, pfsh);
  if (!al_buf)
    return -ENOMEM;

  if (al_offset + al_len > pfs_otable[oh].append_off && pfsh->is_zns)
  {
    al_len -= bs;
    if (al_len)
      tail_len = (offset + len) - (al_offset + al_len);
    else
      tail_len = len;
  }

  if (al_len)
    ret = fla_object_read(pfsh->fs, pfsh->ph, noh, al_buf, al_offset, al_len);

  if (ret)
  {
    printf("Whoops unaligned object read went wrong\n");
    goto out_free;
  }

  if (al_offset / bs == pfs_otable[oh].append_off / bs)
    tail_start = offset % bs;

  if (tail_len)
  {
    if (al_len)
      memcpy(buf, al_buf + (offset % bs), al_len - (offset % bs));

    if (al_len)
      memcpy(buf + al_len - (offset % bs), pfs_otable[oh].append_buf + tail_start, tail_len);
    else
      memcpy(buf, pfs_otable[oh].append_buf + tail_start, tail_len);
  }
  else
  {
    memcpy(buf, al_buf + (offset % bs), len);
  }

out_free:
  free(al_buf);
  return ret;
}

ssize_t poolfs_object_read_r(uint64_t oh, void *buf, size_t offset, size_t len,
                             struct poolfs_handle *pfsh, struct pfs_oinfo *oinfo)
{
  uint64_t *rb_off = &pfs_otable[oh].read_buf_off;
  char *rb = pfs_otable[oh].read_buf;
  char *bufpos = buf;
  struct fla_object *flaobj = &oinfo->fla_oh;
  size_t from_buffer = 0, toRead = len, append_sz = POOLFS_APPEND_SIZE;

  // Read in the correct block if the starting address does not fall within the buffer
  if (offset < *rb_off || offset >= (*rb_off + POOLFS_APPEND_SIZE))
  {
    *rb_off = (offset / POOLFS_APPEND_SIZE) * POOLFS_APPEND_SIZE;
    fla_object_read(pfsh->fs, pfsh->ph, flaobj, rb, *rb_off, POOLFS_APPEND_SIZE);
  }

  // Read completely contained in buffer
  if (offset >= *rb_off && offset + len <= *rb_off + POOLFS_APPEND_SIZE)
  {
    memcpy(bufpos, rb + offset % POOLFS_APPEND_SIZE, len);
    return len;
  }

  from_buffer = POOLFS_APPEND_SIZE - (offset % POOLFS_APPEND_SIZE);
  memcpy(bufpos, rb + offset % POOLFS_APPEND_SIZE, from_buffer);

  toRead -= from_buffer;
  bufpos += from_buffer;
  offset += from_buffer;

  // TODO set the rb_off if it hasn't been initialized
  while (toRead > append_sz)
  {
    fla_object_read(pfsh->fs, pfsh->ph, flaobj, rb, *rb_off + POOLFS_APPEND_SIZE, POOLFS_APPEND_SIZE);
    *rb_off += POOLFS_APPEND_SIZE;
    memcpy(bufpos, rb, POOLFS_APPEND_SIZE);
    toRead -= POOLFS_APPEND_SIZE;
    bufpos += POOLFS_APPEND_SIZE;
    offset += POOLFS_APPEND_SIZE;
  }

  fla_object_read(pfsh->fs, pfsh->ph, flaobj, rb, *rb_off + POOLFS_APPEND_SIZE, POOLFS_APPEND_SIZE);
  *rb_off += POOLFS_APPEND_SIZE;
  memcpy(bufpos, rb + offset % POOLFS_APPEND_SIZE, toRead);

  return len;

}

ssize_t poolfs_object_read_rw(uint64_t oh, void *buf, size_t offset, size_t len,
                              struct poolfs_handle *pfsh, struct pfs_oinfo *oinfo)
{
  struct fla_object *nfsobj = &oinfo->fla_oh;
  uint32_t bs = pfsh->append_sz;
  int ret;

  // Cant read past end of file
  if (offset + len > oinfo->size)
    len = oinfo->size - offset;

  // Aligned start and end go straight to the underlying storage if buffer is aligned
  if (!(offset % bs) && !(len % bs))
  {
    if (!((uintptr_t)buf % bs))
    {
      ret =  fla_object_read(pfsh->fs, pfsh->ph, nfsobj, buf, offset, len);
      if (ret)
        return ret;

      return len;
    }
  }

  ret = poolfs_object_unaligned_read(oh, nfsobj, buf, offset, len, pfsh);
  if (ret)
    return ret;

  return len;
}

ssize_t poolfs_object_read(uint64_t oh, void *buf, size_t offset, size_t len,
                           struct poolfs_handle *pfsh)
{
  struct pfs_oinfo *oinfo = &pfs_otable[oh].oinfo;
  // Cant read past end of file
  if (offset + len > oinfo->size)
    len = oinfo->size - offset;

  if (pfs_otable[oh].o_flags & POOLFS_OPEN_FLAG_WRITE)
    return poolfs_object_read_rw(oh, buf, offset, len, pfsh, oinfo);

  return poolfs_object_read_r(oh, buf, offset, len, pfsh, oinfo);
}
int poolfs_update_md(struct poolfs_ohandle *oh, struct poolfs_handle *pfsh)
{
  int ret = 0;
  struct fla_object *nfsobj = &oh->pfs_oh;
  char *tbuf = fla_buf_alloc(pfsh->fs, pfs_obj_sz); // On disk MD buf
  struct pfs_oinfo *oinfo;

  ret = fla_object_read(pfsh->fs, pfsh->ph, nfsobj, tbuf, 0, pfs_obj_sz);
  if (ret)
  {
    printf("Object read in update md fails\n");
    free(tbuf);
    return -EIO;
  }

  oinfo = ((struct pfs_oinfo *)tbuf) + oh->oinfo_off;
  memcpy(oinfo, &oh->oinfo, sizeof(struct pfs_oinfo));
  ret = fla_object_write(pfsh->fs, pfsh->ph, nfsobj, tbuf, 0, pfs_obj_sz);
  if (ret)
  {
    printf("Object write in update md fails\n");
    free(tbuf);
    return -EIO;
  }

  free(tbuf);
  return ret;
}



int poolfs_conv_object_write(struct fla_object *nfs_oh, void *buf, size_t offset,
    size_t len, struct poolfs_handle *pfsh)
{
  int ret;

  if (len % poolfs_dev_bs(pfsh))
    ret = fla_object_unaligned_write(pfsh->fs, pfsh->ph, nfs_oh, buf, offset, len);
  else
    ret = fla_object_write(pfsh->fs, pfsh->ph, nfs_oh, buf, offset, len);

  return ret;
}

// This is currently only going to support append, force this later
int poolfs_zns_object_write(struct fla_object *nfs_oh, void *buf, size_t offset,
    size_t len, uint64_t oh, struct poolfs_handle *pfsh)
{
  size_t al_start = 0, al_end = 0, al_len = 0, tail_len = 0;
  int ret = 0;
  char *bufpos = (char *)buf;
  char *al_buf;
  uint32_t bs = pfsh->append_sz;

  // Unaligned start offset
  if (offset % bs)
    al_start = offset + (bs - (offset % bs));
  else
    al_start = offset;

  // Unaligned tail
  if ((offset + len) % bs)
    al_end = ((offset + len) / bs) * bs;
  else
      al_end = offset + len;

  al_len = al_end - al_start;
  tail_len = len - ((al_start - offset) + al_len);

  // We fit into the buffer so just fill buffer up
  if (offset + len < al_start)
  {
    memcpy(pfs_otable[oh].append_buf + (offset % bs), buf, len);
    pfs_otable[oh].append_off += len;
    return ret;
  }

  size_t buf_offset = (offset / bs) * bs;
  // Going to cross block boundary, copy up to the block boundary
  if (al_start != offset)
  {
    memcpy(pfs_otable[oh].append_buf + (offset % bs), buf, al_start - offset);
    ret = fla_object_write(pfsh->fs, pfsh->ph, nfs_oh, pfs_otable[oh].append_buf, buf_offset,
                             bs);
    if (ret)
    {
      printf("Error writing append buffer\n");
      return ret;
    }
  }
  // Write out all of the aligned data
  bufpos += al_start - offset;
  if (al_len) {
    al_buf = poolfs_buf_alloc(al_len, pfsh);
    memcpy(al_buf, bufpos, al_len);
	  ret = fla_object_write(pfsh->fs, pfsh->ph, nfs_oh, al_buf, al_start, al_len);
    free(al_buf);
  }

  if (ret)
  {
    printf("ZNS write of the aligned portion of append data fails\n");
    return ret;
  }

  bufpos += al_len;
  // Copy the unaligned tail to the buffer
  memcpy(pfs_otable[oh].append_buf, bufpos, tail_len);
  pfs_otable[oh].append_off += len;

  return ret;
}

int poolfs_object_write(uint64_t oh, void *buf, size_t offset, size_t len,
                        struct poolfs_handle *pfsh)
{
  struct pfs_oinfo *oinfo = &pfs_otable[oh].oinfo;
  struct fla_object *nfsobj = &oinfo->fla_oh;
  int ret = 0;

  if (pfsh->is_zns)
    ret = poolfs_zns_object_write(nfsobj, buf, offset, len, oh, pfsh);
  else
    ret = poolfs_conv_object_write(nfsobj, buf, offset, len, pfsh);

  if (ret)
  {
    printf("poolfs_object_write fla object write fails\n");
    return -EIO;
  }

  if (offset + len > oinfo->size)
  {
    oinfo->size = offset + len;

    if (!pfsh->is_zns)
    {
      ret = poolfs_update_md(&pfs_otable[oh], pfsh);
      if (ret)
        exit(0);
    }
  }

  return ret;
}

void *poolfs_buf_alloc(size_t nbytes, struct poolfs_handle *pfsh)
{
  return fla_buf_alloc(pfsh->fs, nbytes);
}

uint32_t poolfs_dev_bs(struct poolfs_handle *pfsh)
{
  return fla_fs_lb_nbytes(pfsh->fs);
}

// TODO update to reflect object table
int poolfs_object_rename(const char *oldname, const char *newname, struct poolfs_handle *pfsh)
{
  struct pfs_oinfo *oinfo = poolfs_find_oinfo(pfsh, basename((char *)oldname));
  unsigned int namelen = strlen(basename((char *)newname));
  uint64_t oh;

  oh = poolfs_otable_search(basename((char *)oldname), NULL);
  poolfs_object_delete(basename((char *)newname), pfsh); // TODO clear out even if we have the file currently open

  if (!oinfo)
  {
    return -EINVAL;
  }

  if (oh != POOLFS_MAX_OPEN_OBJECTS) {
    memset(pfs_otable[oh].oinfo.name, 0, POOLFS_OBJ_NAME_LEN_MAX);  
    memcpy(pfs_otable[oh].oinfo.name, basename((char *)newname), namelen + 1);
  }
  
  memset(oinfo->name, 0, POOLFS_OBJ_NAME_LEN_MAX);
  memcpy(oinfo->name, basename((char *)newname), namelen + 1);

  if (!pfsh->is_zns)
  {
    if (fla_object_write(pfsh->fs, pfsh->ph, &pfs_dir.fla_oh, pfs_dir.buf, 0, pfs_obj_sz))
    {
      printf("Error writing md during rename for object:%s\n", oinfo->name);
      exit(0);
    }
  }

  return 0;
}

// TODO assumes this won't fail, revisit me
int poolfs_object_close(uint64_t oh, struct poolfs_handle *pfsh)
{
  struct fla_object *noh = &pfs_otable[oh].oinfo.fla_oh;
  size_t append_off = pfs_otable[oh].append_off;
  uint32_t bs = pfsh->append_sz;
  int ret = 0;

  // File is no longer open
  if (!strlen(pfs_otable[oh].oinfo.name))
    return 0;

  pfs_otable[oh].use_count--;
  // Use count drops to zero lets free
  if (!pfs_otable[oh].use_count)
  {
    if (append_off % bs && !pfs_otable[oh].frozen)
    {
      // Append the last block
      ret = fla_object_write(pfsh->fs, pfsh->ph, noh, pfs_otable[oh].append_buf,
                             (append_off / bs) * bs, bs);
      if (ret)
      {
        printf("Error writing last block, corruption abound\n");
        return ret;
      }
    }

    // Seal the object
    if (append_off && pfsh->is_zns)
      fla_object_seal(pfsh->fs, pfsh->ph, noh);

    // TODO fix this issue
    if (pfs_otable[oh].pfs_oh.slab_id != pfs_dir.fla_oh.slab_id &&
		    pfs_otable[oh].pfs_oh.entry_ndx != pfs_dir.fla_oh.entry_ndx)
    {
      printf("MD will be inconsistent\n");
    }
    else
    {
      memcpy(((struct pfs_oinfo *)pfs_dir.buf) + pfs_otable[oh].oinfo_off, &pfs_otable[oh].oinfo,
          sizeof(struct pfs_oinfo));
    }

    free(pfs_otable[oh].append_buf);
    memset(&pfs_otable[oh], 0, sizeof(struct poolfs_ohandle));

  }

  return 0;
}

int poolfs_sync(struct poolfs_handle *pfsh)
{
  return fla_sync(pfsh->fs);
}

bool poolfs_is_zns(struct poolfs_handle *pfsh)
{
  return pfsh->is_zns;
}
