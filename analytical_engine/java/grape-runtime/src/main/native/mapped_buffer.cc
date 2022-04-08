
#include <fcntl.h>
#include <jni.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "core/java/javasdk.h"

#include <string>
#include <vector>

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL Java_com_alibaba_graphscope_utils_MappedBuffer_create(
    JNIEnv* env, jclass clz, jstring path, jlong mapped_size) {
  std::string mmap_file_path = gs::JString2String(env, path);
  int md =
      shm_open(mmap_file_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (md == -1) {
    LOG(ERROR) << "Failed to open to file-[" << mmap_file_path << "]";
    perror("open");
    return 0;
  }

  int ret = ftruncate(md, mapped_size);
  if (ret == -1) {
    LOG(ERROR) << "Failed to truncate file-[" << path << "]";
    perror("truncate");
    return 0;
  }

  if (mapped_size != 0) {
    void* addr = mmap(NULL, mapped_size, PROT_WRITE, MAP_SHARED, md, 0);
    if (addr == MAP_FAILED) {
      LOG(ERROR) << "Failed to mmap file-[" << mmap_file_path << "], "
                 << mapped_size;
      perror("mmap");
    }
    return reinterpret_cast<jlong>(addr);
  }
  return 0;
}

#ifdef __cplusplus
}
#endif
