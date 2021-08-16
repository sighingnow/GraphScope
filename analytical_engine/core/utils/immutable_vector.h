#ifndef ANALYTICAL_ENGINE_CORE_UTILS_IMMUTABLE_VECTOR_H_
#define ANALYTICAL_ENGINE_CORE_UTILS_IMMUTABLE_VECTOR_H_

#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "glog/logging.h"

namespace grape {

template <typename T>
inline void dump_vector(const std::vector<T>& vec, const std::string& path) {
  int md = shm_open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (md == -1) {
    LOG(ERROR) << "Failed to dump to file-[" << path << "]";
    perror("open");
    return;
  }

  size_t size_in_bytes = vec.size() * sizeof(T);
  int ret = ftruncate(md, size_in_bytes);
  if (ret == -1) {
    LOG(ERROR) << "Failed to truncate file-[" << path << "]";
    perror("truncate");
    return;
  }

  void* addr = mmap(NULL, size_in_bytes, PROT_WRITE, MAP_SHARED, md, 0);
  if (addr == MAP_FAILED) {
    LOG(ERROR) << "Failed to mmap file-[" << path << "]";
    perror("mmap");
    return;
  }

  memcpy(addr, vec.data(), size_in_bytes);
}

template <typename T>
class PodVector {
 public:
  PodVector() : data_(NULL), size_(0), md_(-1) {}

  void load(const std::string& path) {
    md_ = shm_open(path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR);
    if (md_ == -1) {
      LOG(ERROR) << "Failed to open file-[" << path << "]";
      perror("open");
      return;
    }

    struct stat buf;
    if (fstat(md_, &buf) != 0) {
      LOG(ERROR) << "Failed to fstat file-[" << path << "]";
      perror("stat");
      return;
    }

    size_t size_in_bytes = buf.st_size;
    size_ = size_in_bytes / sizeof(T);

    data_ = static_cast<T*>(
        mmap(NULL, size_in_bytes, PROT_READ, MAP_SHARED, md_, 0));
  }

  void unload() {
    if (md_ == -1) {
      return;
    }
    munmap(static_cast<void*>(data_), size_ * sizeof(T));

    md_ = -1;
  }

  const T* data() const { return data_; }

  T* data() { return data_; }

  size_t size() const { return size_; }

  const T& operator[](size_t index) const { return data_[index]; }

 private:
  T* data_;
  size_t size_;
  int md_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_UTILS_IMMUTABLE_VECTOR_H_
