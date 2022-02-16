// Copyright (c) 2022-present, Adam Manzanares <a.manzanares@samsung.com>
// Copyright (c) 2022-present, Joel Granados <j.granados@samsung.com>
// Copyright (c) 2022-present, Jesper Devantier <j.devantier@samsung.com>

#pragma GCC diagnostic ignored "-Wunused-parameter"
#include "env_poolfs.h"

#include <bits/stdint-uintn.h>
#include <memory>
#include <chrono>
#include <cstring>
#include <sstream>
#include <rocksdb/utilities/object_registry.h>


namespace ROCKSDB_NAMESPACE {
#define ROCKSDB_POOLNAME "ROCKSDB_POOL"

#ifndef ROCKSDB_LITE

extern "C" FactoryFunc<Env> poolfs_reg;

FactoryFunc<Env> poolfs_reg =
  ObjectLibrary::Default()->Register<Env>("poolfs:.*",
      [](const std::string &poolfs_opts, std::unique_ptr<Env>* env, std::string *) {
      *env = NewPoolFSEnv(poolfs_opts);
      return env->get();
      });

#endif // ROCKSDB_LITE

void split(const std::string &in, char delim, std::vector<std::string> &pieces)
{
  pieces.clear();
  size_t p_start = 0;
  size_t p_end = in.find(delim);
  while (p_end != std::string::npos) {
    std::string subpiece = in.substr(p_start, p_end - p_start);
    pieces.push_back(subpiece);
    p_start = p_end + 1;
    p_end = in.find(delim, p_start);
  }

  pieces.push_back(in.substr(p_start, in.size() - p_start));

}

std::unique_ptr<Env>
NewPoolFSEnv(const std::string &poolfs_opts) {
  return std::unique_ptr<Env>(new LibPoolFSEnv(Env::Default(), poolfs_opts));
}

LibPoolFSEnv::LibPoolFSEnv(Env *env, const std::string &poolfs_opts)
  : EnvWrapper(env), options(new LibPoolFSEnv_Options())
{
  uint64_t target_file_size_base;
  std::vector<std::string> opts;
  
  options->pfs_mut = new std::mutex();
  // dev:target_file_size_base:md_dev (opt)
  split(poolfs_opts, ':', opts);


  if (opts.size() < 3 || opts.size() > 4 )
    throw std::runtime_error(std::string("poolfs params must be 2 or 3 colon separated values"));

  dev_uri = opts[1];
  std::istringstream conv(opts[2]);
  conv >> target_file_size_base;

  if (opts.size() == 4)
    md_dev_uri = opts[3];

  if (poolfs_init(dev_uri.c_str(), md_dev_uri.c_str(), ROCKSDB_POOLNAME, target_file_size_base / 64,
        &pfsh))
      throw std::runtime_error(std::string("Error initializing poolfs:"));

  if (poolfs_pool_set_strp(pfsh, 64, 2097152/64))
    throw std::runtime_error(std::string("Error setting poolfs striping parameters"));

}

LibPoolFSEnv::~LibPoolFSEnv()
{
  std::cout << "Executing the close function ===================================" << std::endl;
  //close();
}

Status
LibPoolFSEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result, const EnvOptions& options_)
{
  std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  result->reset(new LibPoolFSEnvSeqAccessFile(fname, options, pfsh));
  return Status::OK();
}

Status
LibPoolFSEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result, const EnvOptions& options_)
{
  std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  result->reset(new LibPoolFSEnvRandAccessFile(fname, options, pfsh));
  return Status::OK();
}

Status
LibPoolFSEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result, const EnvOptions& options_)
{
  std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  result->reset(new LibPoolFSEnvWriteableFile(fname, options, pfsh));
  return Status::OK();
}

Status
LibPoolFSEnv::NewDirectory(const std::string& name, std::unique_ptr<Directory>* result)
{
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  result->reset(new LibPoolFSEnvDirectory());
  return Status::OK();
}

Status
LibPoolFSEnv::FileExists(const std::string& fname)
{
  uint64_t oh;
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  if(!poolfs_object_open(fname.c_str(), pfsh, &oh, POOLFS_OPEN_FLAG_READ)) {
    poolfs_object_close(oh, pfsh);
    return Status::OK();
  }
  else
    return Status::NotFound();
}
Status
LibPoolFSEnv::GetChildren(const std::string& path, std::vector<std::string>* result)
{
  return Status::OK();
}

Status
LibPoolFSEnv::DeleteFile(const std::string& fname)
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  if(poolfs_object_delete(fname.c_str(), pfsh))
    return Status::NotFound();

  return Status::OK(); // No matter what happens we return that the file is not there
}

Status
LibPoolFSEnv::CreateDir(const std::string& name)
{
  throw;
}

Status
LibPoolFSEnv::CreateDirIfMissing(const std::string& name)
{
  //std::cout << "Calling " << __func__ << ", fname " << name << std::endl;
  return Status::OK();
}

  Status
LibPoolFSEnv::DeleteDir(const std::string& name)
{
  //std::cout << "Calling " << __func__ << ", fname " << name << std::endl;
  return Status::OK();
}

  Status
LibPoolFSEnv::GetFileSize(const std::string& fname, uint64_t* size)
{
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  struct pfs_oinfo *oinfo = poolfs_find_oinfo(pfsh, fname.c_str());
  
  if(!oinfo)
    return Status::NotFound();

  *size = oinfo->size;
  return Status::OK();
}

  Status
LibPoolFSEnv::GetFileModificationTime(const std::string& fname, uint64_t* file_mtime)
{
  throw;
}

Status
LibPoolFSEnv::RenameFile(const std::string& src, const std::string& target)
{
  //std::cout << "Calling " << __func__ << ", src " << src << ", target " << target << std::endl;
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  
  if (poolfs_object_rename(src.c_str(), target.c_str(), pfsh))
    return Status::NotFound();

  return Status::OK();
}

Status
LibPoolFSEnv::LinkFile(const std::string& src, const std::string& target)
{
  throw;
}
Status
LibPoolFSEnv::LockFile(const std::string& fname, FileLock** lock)
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  return Status::OK();
}
Status
LibPoolFSEnv::UnlockFile(FileLock* lock)
{
  //std::cout << "Calling " << __func__ << ", " << __FILE__ << __LINE__ << std::endl;
  return Status::OK();
}
Status
LibPoolFSEnv::GetAbsolutePath(const std::string& db_path, std::string* output_path)
{
  //std::cout << "Calling " << __func__ << ", fname " << db_path << std::endl;
  return Status::OK();
}
Status
LibPoolFSEnv::GetTestDirectory(std::string* path)
{
  //std::cout << __FUNCTION__ << std::endl;
  return Status::OK();
}
Status
LibPoolFSEnv::IsDirectory(const std::string& path, bool* is_dir)
{
  //std::cout << __FUNCTION__ << std::endl;
  throw;
}
Status
LibPoolFSEnv::NewLogger(const std::string& fname, std::shared_ptr<Logger>* result)
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  return Status::OK();
}

Status
LibPoolFSEnv::close()
{
  //std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  //std::cout << "BIG flush .... " << std::endl;
  poolfs_close(this->pfsh); // TODO poolfs close returns status so should poolfs_close

  //if(poolfs_close(this->poolfs_handle) != 0)
  //{
  //  std::cerr << "Could not close poolfs handle." << std::endl;
  //  return Status::Aborted();
  //}
  //std::cout << "returning without an error" << std::endl;
  return Status::OK();
}

Status
LibPoolFSEnv::fssync()
{
  std::lock_guard<std::mutex> guard(*(options->pfs_mut));
  //std::cout << "Flush has to wait until close " << std::endl;
  if(poolfs_sync(pfsh) != 0)
  {
    std::cerr << "Aborting flushing ... " << std::endl;
	  return Status::Aborted();
  }
  return Status::OK();
}

LibPoolFSEnvSeqAccessFile::LibPoolFSEnvSeqAccessFile(
    const std::string & fname_, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh_)
  :fname(fname_), pfsh(pfsh_), fh(NULL)
{
  //std::cout << "Seq Access File:" << fname << std::endl;
  lnfs_mut = options->pfs_mut;
  fh = std::make_shared<LibPoolFSEnv::LibPoolFSEnv_F>();
  fh->pfsh = pfsh;
  if(poolfs_object_open(fname.c_str(), pfsh, &fh->object_handle, POOLFS_OPEN_FLAG_READ))
    throw std::runtime_error(std::string("Error finding file:") + fname);

  fh->opens++;
}

LibPoolFSEnvSeqAccessFile::~LibPoolFSEnvSeqAccessFile(){}

Status
LibPoolFSEnvSeqAccessFile::Read(size_t n, Slice* result, char* scratch)
{
  //std::cout << "Seq Access read:" << fname << " off:" << fh->roffset_ << " len:"  << n << std::endl;
  ssize_t len;
  std::lock_guard<std::mutex> guard(*lnfs_mut);

  len = poolfs_object_read(fh->object_handle, scratch, fh->roffset_, n, fh->pfsh);
  if (len < 0)
    return Status::IOError();

  *result = Slice(scratch, len);
  fh->roffset_ += len;

  return Status::OK();
}

Status
LibPoolFSEnvSeqAccessFile::PositionedRead(uint64_t offset, size_t n, Slice* result, char* scratch)
{
  //std::cout << fh->offset_ << "," << fh->e_o_f_ << std::endl;
  //std::cout << "Seq Acc Pos read:" << fname << " off:" << offset << " len:"  << n << std::endl;
  ssize_t len;
  std::lock_guard<std::mutex> guard(*lnfs_mut);
  
  len = poolfs_object_read(fh->object_handle, scratch, offset, n, fh->pfsh);
  if (len < 0)
    return Status::IOError();

  result->size_ = len;
  result->data_ = scratch;
  return Status::OK();
}

/*bool
LibPoolFSEnvSeqAccessFile::use_direct_io() const
{
  return true;
}*/

Status
LibPoolFSEnvSeqAccessFile::Skip(uint64_t n)
{
  if(fh->roffset_ + n > fh->e_o_f_)
    return Status::IOError();
  fh->roffset_ += n;
  return Status::OK();
}

LibPoolFSEnvRandAccessFile::LibPoolFSEnvRandAccessFile(
    const std::string & fname_, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh_)
  :fname(fname_), pfsh(pfsh_), fh(NULL)
{
  //std::cout << "Rand Access File:" << fname << std::endl;
  lnfs_mut = options->pfs_mut;
  fh = std::make_shared<LibPoolFSEnv::LibPoolFSEnv_F>();
  fh->pfsh = pfsh;
  if(poolfs_object_open(fname.c_str(), pfsh, &fh->object_handle, POOLFS_OPEN_FLAG_READ))
    throw std::runtime_error(std::string("Error allocating object for file:") + fname);
    
  fh->opens++;
}
LibPoolFSEnvRandAccessFile::~LibPoolFSEnvRandAccessFile(){}

Status
LibPoolFSEnvRandAccessFile::Read(uint64_t offset, size_t n, Slice* result, char* scratch) const
{
  //std::cout << "Calling a rand read offset : " << offset << " Size : "<< n << std::endl;
  //std::cout << "Ran Acc read:" << fname << " off:" << offset << " len"  << n << std::endl;
  ssize_t len;
  std::lock_guard<std::mutex> guard(*lnfs_mut);

  len = poolfs_object_read(fh->object_handle, scratch, offset, n, fh->pfsh);
  if (len < 0)
    return Status::IOError();

  *result = Slice(scratch, len);
  return Status::OK();
}
Status
LibPoolFSEnvRandAccessFile::Prefetch(uint64_t offset, size_t n)
{
  return Status::OK();
}
Status
LibPoolFSEnvRandAccessFile::MultiRead(ReadRequest* reqs, size_t num_reqs)
{
  //std::cout << "Multi read " << std::endl;
  throw;
}
size_t
LibPoolFSEnvRandAccessFile::GetUniqueId(char* id, size_t max_size) const
{
  return 0;
};
/*bool
LibPoolFSEnvRandAccessFile::use_direct_io() const
{
  return true;
};*/
Status
LibPoolFSEnvRandAccessFile::InvalidateCache(size_t offset, size_t length)
{
  throw;
};

LibPoolFSEnvWriteableFile::LibPoolFSEnvWriteableFile(
    const std::string & fname_, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh_)
  :fname(fname_), pfsh(pfsh_), fh(NULL), _prevwr(0)
{

  lnfs_mut = options->pfs_mut;
  fh = std::make_shared<LibPoolFSEnv::LibPoolFSEnv_F>();
  fh->pfsh = pfsh;
  //auto start = std::chrono::high_resolution_clock::now();
  if(poolfs_object_open(fname.c_str(), pfsh, &fh->object_handle, POOLFS_OPEN_FLAG_CREATE | 
	  POOLFS_OPEN_FLAG_WRITE))
    throw std::runtime_error(std::string("Error allocating object for file:") + fname);
    
  //auto end = std::chrono::high_resolution_clock::now();
  //auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  //std::cout << "Writeable File:" << fname << " : creation time(us):"<< duration.count() << std::endl;
  fh->opens++;
}

LibPoolFSEnvWriteableFile::~LibPoolFSEnvWriteableFile()
{
}

Status
LibPoolFSEnvWriteableFile::Append(const Slice& slice)
{
  std::lock_guard<std::mutex> guard(*lnfs_mut);
  //Env::num_written_bytes += slice.size_;

  //auto start = std::chrono::high_resolution_clock::now();
  if(poolfs_object_write(fh->object_handle, (void*)slice.data_, fh->woffset_, slice.size_, fh->pfsh))
    throw std::runtime_error(std::string("Error appending to file"));

  //auto end = std::chrono::high_resolution_clock::now();
  //auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  //std::cout << "File:" << fname << " append size:" << slice.size_ << " wof:" << fh->woffset_ << std::endl;
  //std::cout << "Time(us)):" << duration.count() << std::endl;
  fh->e_o_f_ += slice.size_;
  fh->woffset_ += slice.size_;
  //std::cout << "buf : " << slice.data_ << " size : " << slice.data_ << std::endl;
  return Status::OK();
}

Status
LibPoolFSEnvWriteableFile::PositionedAppend(const Slice& slice, uint64_t offset)
{
  //std::cout << "File:" << fname << " pos append size:" << slice.size_<<  " pos:"<< offset<< std::endl;
  std::lock_guard<std::mutex> guard(*lnfs_mut);
  //Env::num_written_bytes += slice.size_;

  if(poolfs_object_write(fh->object_handle, (void*)slice.data_, offset, slice.size_, fh->pfsh))
    throw std::runtime_error(std::string("Error pos appending to file"));

  fh->e_o_f_ = offset + slice.size_ > fh->e_o_f_ ? offset + slice.size_ : fh->e_o_f_;
  fh->woffset_ = offset + slice.size_;
  return Status::OK();
}

Status
LibPoolFSEnvWriteableFile::PositionedAppend(const Slice& s, uint64_t o, const DataVerificationInfo&)
{
  return PositionedAppend(s, o);
}

Status
LibPoolFSEnvWriteableFile::Append(const Slice& s, const DataVerificationInfo&)
{
  return Append(s);
}
/*
bool
LibPoolFSEnvWriteableFile::use_direct_io() const
{
  return true;
}*/

Status
LibPoolFSEnvWriteableFile::Flush()
{
  return Status::OK();
}
Status
LibPoolFSEnvWriteableFile::Sync()
{
  return Status::OK();
}
Status
LibPoolFSEnvWriteableFile::Close()
{
  //std::cout << "Calling " << __func__ << ", fname " << fname << std::endl;
  //std::lock_guard<std::mutex> guard(*lnfs_mut);
  poolfs_object_close(fh->object_handle, fh->pfsh);
  return Status::OK();
}

LibPoolFSEnvDirectory::LibPoolFSEnvDirectory(){}
LibPoolFSEnvDirectory::~LibPoolFSEnvDirectory(){}
Status LibPoolFSEnvDirectory::Fsync(){return Status::OK();}

}
