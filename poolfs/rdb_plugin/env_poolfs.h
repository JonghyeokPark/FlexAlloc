// Copyright (c) 2022-present, Adam Manzanares <a.manzanares@samsung.com>
// Copyright (c) 2022-present, Joel Granados <j.granados@samsung.com>
// Copyright (c) 2022-present, Jesper Devantier <j.devantier@samsung.com>

#pragma once
#include <iostream>
#include <mutex>
#include <poolfs.h>
#include <stdint.h>
#include "rocksdb/env.h"
#include "rocksdb/options.h"

#define ZNS_BLK_SZ 4096

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<ROCKSDB_NAMESPACE::Env> NewPoolFSEnv(const std::string &poolfs_opts);

class LibPoolFSEnv : public EnvWrapper
{
  public:
    struct LibPoolFSEnv_F
    {
      LibPoolFSEnv_F():pfsh(NULL), object_handle(0), e_o_f_(0), woffset_(0),
      roffset_(0), opens(0) {}
      struct poolfs_handle* pfsh;
      uint64_t object_handle;
      uint64_t e_o_f_;
      off_t woffset_;
      off_t roffset_;
      uint32_t opens;
    };
    struct LibPoolFSEnv_Options
    {
      std::mutex *pfs_mut;
    };

  private:
    std::string dev_uri = "";
    std::string md_dev_uri = "";
    struct poolfs_handle *pfsh;
    std::shared_ptr<LibPoolFSEnv_Options> options;

  public:
    //explicit LibPoolFSEnv(const uint64_t target_file_size_base,
    //    const uint64_t max_manifest_file_size, const std::string &dev, const std::string &md_dev);
    explicit LibPoolFSEnv(Env *env, const std::string &poolfs_opts_file);

    virtual ~LibPoolFSEnv();

  public:
    Status NewSequentialFile(const std::string& fname, std::unique_ptr<SequentialFile>* result,
        const EnvOptions& options) override;
    Status NewRandomAccessFile(const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
        const EnvOptions& options) override;
    Status NewWritableFile(const std::string& fname, std::unique_ptr<WritableFile>* result,
        const EnvOptions& options) override;

    Status NewDirectory(const std::string& name, std::unique_ptr<Directory>* result) override;
    Status FileExists(const std::string& fname) override;
    Status GetChildren(const std::string& path, std::vector<std::string>* result) override;
    Status DeleteFile(const std::string& fname) override;
    Status CreateDir(const std::string& name) override;
    Status CreateDirIfMissing(const std::string& name) override;
    Status DeleteDir(const std::string& name) override;
    Status GetFileSize(const std::string& fname, uint64_t* size) override;
    Status GetFileModificationTime(const std::string& fname, uint64_t* file_mtime) override;
    Status RenameFile(const std::string& src, const std::string& target) override;
    Status LinkFile(const std::string& src, const std::string& target) override;
    Status LockFile(const std::string& fname, FileLock** lock) override;
    Status UnlockFile(FileLock* lock) override;
    Status GetAbsolutePath(const std::string& db_path, std::string* output_path) override;
    Status GetTestDirectory(std::string* path) override;
    Status IsDirectory(const std::string& path, bool* is_dir) override;
    Status NewLogger(const std::string& fname, std::shared_ptr<Logger>* result) override;
    Status close();
    Status fssync();

};

class LibPoolFSEnvSeqAccessFile : public SequentialFile
{
private:
  std::string fname;
  struct poolfs_handle *pfsh;
  std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_F> fh;
  std::mutex *lnfs_mut;

  Status ZNSRead(size_t n, Slice *result, char *scratch);

public:
  LibPoolFSEnvSeqAccessFile(
      const std::string & fname, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh);
  virtual ~LibPoolFSEnvSeqAccessFile();

public:
  virtual Status Read(size_t n, Slice* result, char* scratch) override;
  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result, char* scratch) override;
  virtual Status Skip(uint64_t n) override;
  //virtual bool use_direct_io() const override;
  //virtual size_t GetRequiredBufferAlignment() const override;
};

class LibPoolFSEnvRandAccessFile : public RandomAccessFile
{
private:
  std::string fname;
  struct poolfs_handle *pfsh;
  std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_F> fh;
  std::mutex *lnfs_mut;

public:
  LibPoolFSEnvRandAccessFile(
      const std::string & fname, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh);
  virtual ~LibPoolFSEnvRandAccessFile();

public:
  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;
  virtual Status Prefetch(uint64_t offset, size_t n) override;
  virtual Status MultiRead(ReadRequest* reqs, size_t num_reqs) override;
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
  //virtual bool use_direct_io() const override;
  //virtual size_t GetRequiredBufferAlignment() const override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class LibPoolFSEnvWriteableFile : public WritableFile
{
private:
  std::string fname;
  struct poolfs_handle *pfsh;
  std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_F> fh;
  uint64_t _prevwr;
  std::mutex *lnfs_mut;

  Status ZNSAppend(const Slice& data);
  Status ZNSPositionedAppend(const Slice &data, uint64_t offset);

public:
  LibPoolFSEnvWriteableFile(
      const std::string & fname, std::shared_ptr<LibPoolFSEnv::LibPoolFSEnv_Options> options, struct poolfs_handle *pfsh);
  virtual ~LibPoolFSEnvWriteableFile();

public:
  //virtual bool use_direct_io() const override;
  virtual Status Append(const Slice& data) override;
  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override;
  virtual Status PositionedAppend(const Slice& s, uint64_t o, const DataVerificationInfo&) override;
  virtual Status Append(const Slice& data, const DataVerificationInfo&) override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Close() override;
};

class LibPoolFSEnvDirectory : public Directory
{
public:
  LibPoolFSEnvDirectory();
  virtual ~LibPoolFSEnvDirectory();

public:
  virtual Status Fsync() override;

};

}
