#pragma once
#include <iostream>
#include <libflexalloc.h>
#include <stdint.h>
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include <rocksdb/file_system.h>
#include <flexalloc_daemon_base.h>

namespace ROCKSDB_NAMESPACE {

class FlexAllocEnv : public EnvWrapper
{
  public:
    struct FlexAllocEnv_F
    {
      FlexAllocEnv_F():pool_handle(NULL), object_handle({0}), e_o_f_(0), offset_(0){}
      struct fla_pool * pool_handle;
      struct fla_object object_handle;
      uint64_t e_o_f_;
      off_t offset_;
    };
    struct FlexAllocEnv_Options
    {
      std::unordered_map<std::string, struct fla_pool *> pools;
      std::unordered_map<std::string, std::shared_ptr<FlexAllocEnv_F>> objects;
    };

  private:
    std::string dev_uri = "";
    struct fla_daemon_client fla_handle;
    std::shared_ptr<FlexAllocEnv_Options> options;

  public:
    explicit FlexAllocEnv(Env * env);
    virtual ~FlexAllocEnv();

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

    static struct fla_pool * get_pool_handle_from_file_name(const std::string& fname,
        std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options);

};

class FlexAllocEnvSeqAccessFile : public SequentialFile
{
private:
  std::string fname;
  struct fla_daemon_client * flexalloc_handle;
  std::shared_ptr<FlexAllocEnv::FlexAllocEnv_F> fh;

public:
  FlexAllocEnvSeqAccessFile(
      const std::string & fname, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
      struct fla_daemon_client * flexalloc_handle);
  virtual ~FlexAllocEnvSeqAccessFile();

public:
  virtual Status Read(size_t n, Slice* result, char* scratch) override;
  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result, char* scratch) override;
  virtual Status Skip(uint64_t n) override;
  virtual bool use_direct_io() const override;
  //virtual size_t GetRequiredBufferAlignment() const override;
};

class FlexAllocEnvRandAccessFile : public RandomAccessFile
{
private:
  std::string fname;
  struct fla_daemon_client * flexalloc_handle;
  std::shared_ptr<FlexAllocEnv::FlexAllocEnv_F> fh;

public:
  FlexAllocEnvRandAccessFile(
      const std::string & fname, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
      struct fla_daemon_client * flexalloc_handle);
  virtual ~FlexAllocEnvRandAccessFile();

public:
  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const override;
  virtual Status Prefetch(uint64_t offset, size_t n) override;
  virtual Status MultiRead(ReadRequest* reqs, size_t num_reqs) override;
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
  virtual bool use_direct_io() const override;
  //virtual size_t GetRequiredBufferAlignment() const override;
  virtual Status InvalidateCache(size_t offset, size_t length) override;
};

class FlexAllocEnvWriteableFile : public WritableFile
{
private:
  std::string fname;
  struct fla_daemon_client * flexalloc_handle;
  std::shared_ptr<FlexAllocEnv::FlexAllocEnv_F> fh;

public:
  FlexAllocEnvWriteableFile(
      const std::string & fname, std::shared_ptr<FlexAllocEnv::FlexAllocEnv_Options> options,
      struct fla_daemon_client * flexalloc_handle);
  virtual ~FlexAllocEnvWriteableFile();

public:
  virtual bool use_direct_io() const override;
  virtual Status Append(const Slice& data) override;
  virtual Status PositionedAppend(const Slice& data, uint64_t offset) override;
  virtual Status PositionedAppend(const Slice& s, uint64_t o, const DataVerificationInfo&) override;
  virtual Status Append(const Slice& data, const DataVerificationInfo&) override;
  virtual Status Flush() override;
  virtual Status Sync() override;
  virtual Status Close() override;
};

class FlexAllocEnvDirectory : public Directory
{
public:
  FlexAllocEnvDirectory();
  virtual ~FlexAllocEnvDirectory();

public:
  virtual Status Fsync() override;

};

}
