/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_MAPPED_BUFFER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_MAPPED_BUFFER_H_

#ifdef ENABLE_JAVA_SDK

#include <string>

#include <fcntl.h>
#include <jni.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "glog/logging.h"

namespace gs {

/**
 * @brief Representing memory mapped files.
 */
class MemoryMappedBuffer {
 public:
  MemoryMappedBuffer(std::string memory_mapped_file,int64_t mapped_size_in)
      : filePath(std::move(memory_mapped_file)), fd(-1), addr(MAP_FAILED), mapped_size(mapped_size_in) {}
  void Map() {
    if (fd != -1 || addr != MAP_FAILED) {
      LOG(ERROR) << "Already mapped!";
      perror("Already mapped");
      return;
    }
    if (mapped_size < 0) {
      LOG(ERROR) << "mapped size: " << mapped_size;
      perror("mapped size wrong");
    }
    fd = shm_open(filePath.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd == -1) {
      LOG(ERROR) << "Failed to open to file-[" << filePath << "]";
      perror("open");
      return 0;
    }
    int ret = ftruncate(fd, mapped_size);
    if (ret == -1) {
      LOG(ERROR) << "Failed to truncate file-[" << filePath << "]";
      perror("truncate");
      return 0;
    }
    if (mapped_size != 0) {
      addr = mmap(NULL, mapped_size, PROT_WRITE, MAP_SHARED, fd, 0);
      if (addr == MAP_FAILED) {
        LOG(ERROR) << "Failed to mmap file-[" << filePath << "], "
                   << mapped_size;
        perror("mmap");
      }
    }
    LOG(INFO) << "Successfully mapped: " << filePath
              << " with size: " << mapped_size;
  }

  void Unmap() {
    if (fd != -1) {
      munmap(addr, mapped_size);
      close(fd);
    }
    fd = -1;
    addr = MAP_FAILED;
    mapped_size = -1;
  }
  int64_t GetAddr() {
    if (addr == MAP_FAILED) {
      return -1;
    }
    return reinterpret_cast<int64_t>(addr);
  }

 private:
 int64_t mapped_size;
  std::string filePath;
  int fd;
  void* addr;
};
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_MAPPED_BUFFER_H_
