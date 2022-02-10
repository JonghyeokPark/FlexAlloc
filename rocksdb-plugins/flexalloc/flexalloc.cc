#include <bits/stdint-uintn.h>
#include <libflexalloc.h>
#include <flexalloc_daemon_base.h>
#include <exception>
#include <memory>
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "flexalloc.h"
#include <rocksdb/utilities/object_registry.h>
#include <sstream>
#include <unistd.h>

namespace ROCKSDB_NAMESPACE {

FlexAllocEnv::FlexAllocEnv(Env * env)
  : EnvWrapper(env), dev_uri("/dev/nvme1n1"), options(new FlexAllocEnv_Options())
{
  uint64_t sstable_nbytes, max_manifest_file_size;
  uint32_t dev_lbsize;
  struct fla_pool * l0h, * metah, * manifesth;
  struct flexalloc * flexalloc_handle;
  std::string l0_name, meta_name, manifest_name;
  char * dev_uri_env = NULL;
  std::stringstream ss;

  dev_uri_env = std::getenv("FLEXALLOC_DEV_URI");
  if(!dev_uri_env)
    throw "You must define FLEXALLOC_DEV_URI environment variable to use flexalloc";
  dev_uri = dev_uri_env;

  memset(&fla_handle, 0, sizeof(struct fla_daemon_client));
  if(dev_uri.rfind("/dev/", 0) == 0)
  {
    if(fla_open(dev_uri.c_str(), &flexalloc_handle) != 0)
      throw "Could not open flexalloc handle.";
  } else { // its a socket
    if(fla_daemon_open(dev_uri.c_str(), &fla_handle))
        throw "Could not open flexalloc daemon";
  }
  flexalloc_handle = fla_handle.flexalloc;

  dev_lbsize = fla_fs_lb_nbytes(flexalloc_handle);

  options->pools.reserve(4);
  options->objects.reserve(1048576);

  //TODO get this from exported variable
  sstable_nbytes = 268435456;
  std::cout << "Calculated sstable object size : " << sstable_nbytes << std::endl;

  max_manifest_file_size = 1024 * 1024 * 1024;
  std::cout << "Calculated max manifest file size : " << max_manifest_file_size << std::endl;

  ss.str(""); ss << "l0_" << getpid();
  l0_name = ss.str();
  if(fla_pool_open(flexalloc_handle, l0_name.c_str(), &l0h))
  {
    if(fla_pool_create(flexalloc_handle, l0_name.c_str(), l0_name.length(),
          sstable_nbytes / dev_lbsize, &l0h))
      throw "Could not open l0 pool";
  }

  ss.str(""); ss << "meta_" << getpid();
  meta_name = ss.str();
  if(fla_pool_open(flexalloc_handle, meta_name.c_str(), &metah))
  {
    if(fla_pool_create(flexalloc_handle, meta_name.c_str(), meta_name.length(), 230, &metah))
      throw "Could not open meta pool";
  }

  ss.str(""); ss << "manifest_" << getpid();
  manifest_name = ss.str();
  if(fla_pool_open(flexalloc_handle, manifest_name.c_str(), &manifesth))
  {
    if(fla_pool_create(flexalloc_handle, manifest_name.c_str(), manifest_name.length(),
          max_manifest_file_size / dev_lbsize, &manifesth))
      throw "Could not open manifest pool";
  }

  options->pools = {{"l0", l0h}, {"meta", metah}, {"manifest", manifesth}};
}

FlexAllocEnv::~FlexAllocEnv()
{
  std::cout << "Executing the close function ===================================" << std::endl;
  //close();
}

Status
FlexAllocEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result, const EnvOptions& options_)
{
  result->reset(new FlexAllocEnvSeqAccessFile(fname, options, &fla_handle));
  return Status::OK();
}

Status
FlexAllocEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result, const EnvOptions& options_)
{
  result->reset(new FlexAllocEnvRandAccessFile(fname, options, &fla_handle));
  return Status::OK();
}

Status
FlexAllocEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result, const EnvOptions& options_)
{
  result->reset(new FlexAllocEnvWriteableFile(fname, options, &fla_handle));
  return Status::OK();
}

Status
FlexAllocEnv::NewDirectory(const std::string& name, std::unique_ptr<Directory>* result)
{
  result->reset(new FlexAllocEnvDirectory());
  return Status::OK();
}

Status
FlexAllocEnv::FileExists(const std::string& fname)
{
  if(options->objects.find(fname) != options->objects.end())
    return Status::OK();
  else
    return Status::NotFound();
}
Status
FlexAllocEnv::GetChildren(const std::string& path, std::vector<std::string>* result)
{
  return Status::OK();
}
Status
FlexAllocEnv::DeleteFile(const std::string& fname)
{
  auto obj_it = options->objects.find(fname);
  if(obj_it != options->objects.end())
  {
    struct fla_pool * pool = obj_it->second->pool_handle;
    struct fla_object * object = &obj_it->second->object_handle;
    if(fla_object_destroy(fla_handle.flexalloc, pool, object) != 0)
      return Status::NotFound();
  }
  return Status::OK(); // No matter what happens we return that the file is not there
}
Status
FlexAllocEnv::CreateDir(const std::string& name)
{
  throw;
}
Status
FlexAllocEnv::CreateDirIfMissing(const std::string& name)
{
  //std::cout << "Calling " << __func__ << ", fname " << name << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::DeleteDir(const std::string& name)
{
  //std::cout << "Calling " << __func__ << ", fname " << name << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
  auto obj_it = options->objects.find(fname);
  if(obj_it == options->objects.end())
    return Status::NotFound();

  *size = obj_it->second->e_o_f_;
  return Status::OK();
}
Status
FlexAllocEnv::GetFileModificationTime(const std::string& fname, uint64_t* file_mtime)
{
  throw;
}
Status
FlexAllocEnv::RenameFile(const std::string& src, const std::string& target)
{
  //std::cout << "Calling " << __func__ << ", src " << src << ", target " << target << std::endl;
  if(options->objects.find(src) == options->objects.end())
    return Status::NotFound();

  if(options->objects.find(target) != options->objects.end())
    options->objects.erase(target);

  options->objects.insert(std::make_pair(target, options->objects[src]));
  options->objects.erase(src);
  return Status::OK();
}
Status
FlexAllocEnv::LinkFile(const std::string& src, const std::string& target)
{
  throw;
}
Status
FlexAllocEnv::LockFile(const std::string& fname, FileLock** lock)
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::UnlockFile(FileLock* lock)
{
  //std::cout << "Calling " << __func__ << ", " << __FILE__ << __LINE__ << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::GetAbsolutePath(const std::string& db_path, std::string* output_path)
{
  //std::cout << "Calling " << __func__ << ", fname " << db_path << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::GetTestDirectory(std::string* path)
{
  //std::cout << __FUNCTION__ << std::endl;
  return Status::OK();
}
Status
FlexAllocEnv::IsDirectory(const std::string& path, bool* is_dir)
{
  //std::cout << __FUNCTION__ << std::endl;
  throw;
}
Status
FlexAllocEnv::NewLogger(const std::string& fname, std::shared_ptr<Logger>* result)
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  return Status::OK();
}

Status
FlexAllocEnv::close()
{
  std::cout << "BIG flush .... " << std::endl;
  if(fla_close(this->fla_handle.flexalloc) != 0)
  {
    std::cerr << "Could not close fla handle." << std::endl;
    return Status::Aborted();
  }
  std::cout << "returning without an error" << std::endl;
  return Status::OK();
}

Status
FlexAllocEnv::fssync()
{
  std::cout << "Flushing ... " << std::endl;
  if(fla_sync(this->fla_handle.flexalloc) != 0)
  {
    std::cerr << "Aborting flushing ... " << std::endl;
    return Status::Aborted();
  }
  return Status::OK();
}

struct fla_pool *
FlexAllocEnv::get_pool_handle_from_file_name(const std::string& fname,
    std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options)
{
  if (fname.find(".sst") != std::string::npos)
    return options->pools["l0"];
  else if (fname.find("MANIFEST") != std::string::npos)
    return options->pools["manifest"];
  return options->pools["meta"];
}


FlexAllocEnvSeqAccessFile::FlexAllocEnvSeqAccessFile(
    const std::string & fname_, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
    struct fla_daemon_client * flexalloc_handle_)
  :fname(fname_), flexalloc_handle(flexalloc_handle_), fh(NULL)
{
  //std::cout << "seq access " << fname << std::endl;
  auto object_it = options->objects.find(fname);
  if(object_it != options->objects.end())
  {
    fh = object_it->second;
    fh->offset_ = 0;
  }
  else
  {
    fh = std::make_shared<FlexAllocEnv::FlexAllocEnv_F>();
    fh->pool_handle = FlexAllocEnv::get_pool_handle_from_file_name(fname, options);
    if(fla_object_create(flexalloc_handle_->flexalloc, fh->pool_handle, &fh->object_handle) == 2001)
      throw "No more available slabs";
    options->objects.insert(std::make_pair(fname, fh));
  }
}
FlexAllocEnvSeqAccessFile::~FlexAllocEnvSeqAccessFile()
{
}

Status
FlexAllocEnvSeqAccessFile::Read(size_t n, Slice* result, char* scratch)
{
  //return PositionedRead(fh->offset_, n, result, scratch);
  if((uint64_t)fh->offset_ == fh->e_o_f_)
  {
    result->size_ = 0;
    result->data_ = scratch;
  }
  else
  {
    char * buf = (char *)fla_buf_alloc(flexalloc_handle->flexalloc, n);

    if(fla_object_read(flexalloc_handle->flexalloc, fh->pool_handle, &fh->object_handle,
          buf, fh->offset_, n))
      return Status::IOError();

    memcpy(scratch, buf, n);
    free(buf);
    result->size_ = fh->offset_ + n > fh->e_o_f_ ? fh->e_o_f_ - fh->offset_ : n;
    result->data_ = scratch;
    fh->offset_ += result->size_;
    //std::cout << "buf : " << buf << "result->data " << result->data_ << std::endl;
  }
  //fla_print_object_handle(fh->object_handle);
  return Status::OK();
}

Status
FlexAllocEnvSeqAccessFile::PositionedRead(uint64_t offset, size_t n, Slice* result, char* scratch)
{
  //std::cout << fh->offset_ << "," << fh->e_o_f_ << std::endl;
  if((uint64_t)fh->offset_ == fh->e_o_f_)
  {
    result->size_ = 0;
    result->data_ = scratch;
  }
  else
  {
    const size_t n_ = std::min(n, fh->e_o_f_);
    fh->offset_ = offset;
    char * buf = (char *)fla_buf_alloc(flexalloc_handle->flexalloc, n_);

    if(fla_object_read(flexalloc_handle->flexalloc, fh->pool_handle, &fh->object_handle,
          buf, fh->offset_, n_))
      return Status::IOError();

    memcpy(scratch, buf, n_);
    free(buf);
    result->size_ = fh->offset_ + n_ > fh->e_o_f_ ? fh->e_o_f_ - fh->offset_ : n_;
    result->data_ = scratch;
    fh->offset_ += result->size_;
    //std::cout << "buf : " << buf << "result->data " << result->data_ << std::endl;
  }
  //fla_print_object_handle(fh->object_handle);
  return Status::OK();
}
bool
FlexAllocEnvSeqAccessFile::use_direct_io() const
{
  return true;
}

Status
FlexAllocEnvSeqAccessFile::Skip(uint64_t n)
{
  if(fh->offset_ + n > fh->e_o_f_)
    return Status::IOError();
  fh->offset_ += n;
  return Status::OK();
}

FlexAllocEnvRandAccessFile::FlexAllocEnvRandAccessFile(
    const std::string & fname_, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
    struct fla_daemon_client * flexalloc_handle_)
  :fname(fname_), flexalloc_handle(flexalloc_handle_), fh(NULL)
{
  //std::cout << "Random read construct " << std::endl;
  auto object_it = options->objects.find(fname);
  if(object_it != options->objects.end())
  {
    fh = object_it->second;
    fh->offset_ = 0;
  }
  else
  {
    fh = std::make_shared<FlexAllocEnv::FlexAllocEnv_F>();
    fh->pool_handle = FlexAllocEnv::get_pool_handle_from_file_name(fname, options);
    if(fla_object_create(flexalloc_handle_->flexalloc, fh->pool_handle, &fh->object_handle) == 2001)
      throw "No more available slabs";
    options->objects.insert(std::make_pair(fname, fh));
  }
}
FlexAllocEnvRandAccessFile::~FlexAllocEnvRandAccessFile()
{
}

Status
FlexAllocEnvRandAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
  //std::cout << "Calling a rand read offset : " << offset << " Size : "<< n << std::endl;
  //fla_print_object_handle(fh->object_handle);
  //std::cout << "Rand Read : Object id : " << fh->object_handle.entry_ndx
  //  << " slab id : " << fh->object_handle.slab_id << std::endl;
  fh->offset_ = offset;
  char * buf = (char *)fla_buf_alloc(flexalloc_handle->flexalloc, n);

  if(fla_object_read(flexalloc_handle->flexalloc, fh->pool_handle, &fh->object_handle, buf, fh->offset_, n))
    return Status::IOError();

  memcpy(scratch, buf, n);
  free(buf);
  result->size_ = fh->offset_ + n > fh->e_o_f_ ? fh->e_o_f_ - fh->offset_ : n;
  result->data_ = scratch;
  fh->offset_ += result->size_;
  return Status::OK();
}
Status
FlexAllocEnvRandAccessFile::Prefetch(uint64_t offset, size_t n)
{
  return Status::OK();
}
Status
FlexAllocEnvRandAccessFile::MultiRead(ReadRequest* reqs, size_t num_reqs)
{
  //std::cout << "Multi read " << std::endl;
  throw;
}
size_t
FlexAllocEnvRandAccessFile::GetUniqueId(char* id, size_t max_size) const
{
  return 0;
};
bool
FlexAllocEnvRandAccessFile::use_direct_io() const
{
  return true;
};
Status
FlexAllocEnvRandAccessFile::InvalidateCache(size_t offset, size_t length)
{
  throw;
};

FlexAllocEnvWriteableFile::FlexAllocEnvWriteableFile(
    const std::string & fname_, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
    struct fla_daemon_client * flexalloc_handle_)
  :fname(fname_), flexalloc_handle(flexalloc_handle_), fh(NULL)
{
  //std::cout << "write access " << fname << std::endl;
  auto object_it = options->objects.find(fname);
  if(object_it != options->objects.end())
  {
    fh = object_it->second;
    fh->offset_ = 0;
  }
  else
  {
    fh = std::make_shared<FlexAllocEnv::FlexAllocEnv_F>();
    fh->pool_handle = FlexAllocEnv::get_pool_handle_from_file_name(fname, options);
    if(fla_object_create(flexalloc_handle_->flexalloc, fh->pool_handle, &fh->object_handle) == 2001)
      throw "no more available slabs";
    options->objects.insert(std::make_pair(fname, fh));
  }
  //fla_print_object_handle(fh->object_handle);
}
FlexAllocEnvWriteableFile::~FlexAllocEnvWriteableFile()
{
}

Status
FlexAllocEnvWriteableFile::Append(const Slice& slice)
{
  if(fla_object_unaligned_write(flexalloc_handle->flexalloc, fh->pool_handle, &fh->object_handle,
        (void*)slice.data_, fh->offset_, slice.size_))
    throw "Error appending to file";
  fh->e_o_f_ += slice.size_;
  fh->offset_ += slice.size_;
  //fla_print_object_handle(fh->object_handle);
  //std::cout << "buf : " << slice.data_ << " size : " << slice.data_ << std::endl;
  return Status::OK();
}

Status
FlexAllocEnvWriteableFile::PositionedAppend(const Slice& slice, uint64_t offset)
{
  if(fla_object_write(flexalloc_handle->flexalloc, fh->pool_handle, &fh->object_handle,
        (void*)slice.data_, offset, slice.size_))
    throw "Error position appending to file";

  fh->e_o_f_ = offset + slice.size_ > fh->e_o_f_ ? offset + slice.size_ : fh->e_o_f_;
  fh->offset_ = offset + slice.size_;
  //fla_print_object_handle(fh->object_handle);
  return Status::OK();
}

Status
FlexAllocEnvWriteableFile::PositionedAppend(const Slice& s, uint64_t o, const DataVerificationInfo&)
{
  return PositionedAppend(s, o);
}

Status
FlexAllocEnvWriteableFile::Append(const Slice& s, const DataVerificationInfo&)
{
  return Append(s);
}
bool
FlexAllocEnvWriteableFile::use_direct_io() const
{
  return true;
}
Status
FlexAllocEnvWriteableFile::Flush()
{
  return Status::OK();
}
Status
FlexAllocEnvWriteableFile::Sync()
{
  return Status::OK();
}
Status
FlexAllocEnvWriteableFile::Close()
{
  return Status::OK();
}

FlexAllocEnvDirectory::FlexAllocEnvDirectory(){}
FlexAllocEnvDirectory::~FlexAllocEnvDirectory(){}
Status FlexAllocEnvDirectory::Fsync(){return Status::OK();}

class FlexAllocEnvTest : public EnvWrapper
{
  public:
    FlexAllocEnvTest(Env* t) : EnvWrapper(t){};
};

extern "C" FactoryFunc<Env> flexalloc_reg;

FactoryFunc<Env> flexalloc_reg =
  ObjectLibrary::Default()->Register<Env>
  (
    "flexalloc",
    [](const std::string& uri, std::unique_ptr<Env>* f, std::string* errmsg)
    {
      std::cerr << "URI : " << uri << ", err msg : " << errmsg << std::endl;
      *f = std::unique_ptr<Env>(new FlexAllocEnv(Env::Default()));
      return f->get();
    }
  );

}
