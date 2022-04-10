
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_

#ifdef ENABLE_JAVA_SDK

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/builder.h"

#include "grape/communication/communicator.h"
#include "grape/grape.h"
#include "grape/util.h"

#include "core/java/javasdk.h"
#include "vineyard/basic/ds/arrow_utils.h"
//#include "core/java/utils.h"

namespace gs {

static constexpr const char* OFFSET_VECTOR_VECTOR =
    "std::vector<std::vector<int>>";
static constexpr const char* DATA_VECTOR_VECTOR =
    "std::vector<std::vector<char>>";
// consistent with vineyard::TypeToInt
// 2=int32_t
// 4=int64_t
// 6=float
// 7=double
// 9=std::string(udf)

template <typename T,
          typename std::enable_if<std::is_same<T, grape::EmptyType>::value,
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building pod array with null builder";
  arrow::NullBuilder array_builder;
  int64_t total_length = 0;
  for (size_t i = 0; i < offset_arr.size(); ++i) {
    total_length += offset_arr[i].size();
  }
  array_builder.AppendNulls(total_length);

  array_builder.Finish(&array);
}
template <typename T,
          typename std::enable_if<(!std::is_same<T, std::string>::value &&
                                   !std::is_same<T, grape::EmptyType>::value),
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building pod array with pod builder";
  using elementType = T;
  using builderType = typename vineyard::ConvertToArrowType<T>::BuilderType;
  builderType array_builder;
  int64_t total_length = 0;
  for (size_t i = 0; i < offset_arr.size(); ++i) {
    total_length += offset_arr[i].size();
  }
  array_builder.Reserve(total_length);  // the number of elements

  for (size_t i = 0; i < data_arr.size(); ++i) {
    auto ptr = reinterpret_cast<const elementType*>(data_arr[i].data());
    auto cur_offset = offset_arr[i];

    for (size_t j = 0; j < cur_offset.size(); ++j) {
      array_builder.UnsafeAppend(*ptr);
      CHECK(sizeof(*ptr) == cur_offset[j]);
      ptr += 1;  // We have convert to T*, so plus 1 is ok.
    }
  }
  array_builder.Finish(&array);
}

template <typename T,
          typename std::enable_if<std::is_same<T, std::string>::value,
                                  T>::type* = nullptr>
void BuildArray(std::shared_ptr<arrow::Array>& array,
                const std::vector<std::vector<char>>& data_arr,
                const std::vector<std::vector<int>>& offset_arr) {
  VLOG(10) << "Building utf array with string builder";
  arrow::LargeStringBuilder array_builder;
  int64_t total_length = 0, total_bytes = 0;
  for (size_t i = 0; i < data_arr.size(); ++i) {
    total_bytes += data_arr[i].size();
    total_length += offset_arr[i].size();
  }
  array_builder.Reserve(total_length);  // the number of elements
  array_builder.ReserveData(total_bytes);

  for (size_t i = 0; i < data_arr.size(); ++i) {
    const char* ptr = data_arr[i].data();
    auto cur_offset = offset_arr[i];

    for (size_t j = 0; j < cur_offset.size(); ++j) {
      // for appending data to arrow_binary_builder, we use raw pointer to
      // avoid copy.
      array_builder.UnsafeAppend(ptr, cur_offset[j]);
      ptr += cur_offset[j];
    }
  }
  array_builder.Finish(&array);
}

static constexpr const char* JAVA_LOADER_CLASS =
    "com/alibaba/graphscope/loader/impl/FileLoader";
static constexpr const char* JAVA_LOADER_CREATE_METHOD = "create";
static constexpr const char* JAVA_LOADER_CREATE_SIG =
    "(Ljava/net/URLClassLoader;)Lcom/alibaba/graphscope/loader/impl/"
    "FileLoader;";
static constexpr const char* JAVA_LOADER_LOAD_VE_METHOD =
    "loadVerticesAndEdges";
static constexpr const char* JAVA_LOADER_LOAD_VE_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)I";
static constexpr const char* JAVA_LOADER_LOAD_E_METHOD = "loadEdges";
static constexpr const char* JAVA_LOADER_LOAD_E_SIG =
    "(Ljava/lang/String;Ljava/lang/String;)V";
static constexpr const char* JAVA_LOADER_INIT_METHOD = "init";
static constexpr const char* JAVA_LOADER_INIT_SIG =
    "(IIILcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIByteVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;"
    "Lcom/alibaba/graphscope/stdcxx/FFIIntVecVector;)V";
static constexpr int GIRAPH_TYPE_CODE_LENGTH = 4;
class JavaLoaderInvoker {
 public:
  JavaLoaderInvoker() {
    load_thread_num = 1;
    if (getenv("LOADING_THREAD_NUM")) {
      load_thread_num = atoi(getenv("LOADING_THREAD_NUM"));
    }
    VLOG(1) << "loading thread num: " << load_thread_num;
    oids.resize(load_thread_num);
    vdatas.resize(load_thread_num);
    esrcs.resize(load_thread_num);
    edsts.resize(load_thread_num);
    edatas.resize(load_thread_num);

    oid_offsets.resize(load_thread_num);
    vdata_offsets.resize(load_thread_num);
    esrc_offsets.resize(load_thread_num);
    edst_offsets.resize(load_thread_num);
    edata_offsets.resize(load_thread_num);
    // Construct the FFIPointer

    if (!getenv("USER_JAR_PATH") || !getenv("GRAPE_JVM_OPTS")) {
      LOG(ERROR) << "expect env USER_JAR_PATH and GRAPE_JVM_OPTS set";
    }

    createFFIPointers();

    oid_type = vdata_type = edata_type = msg_type = -1;
  }

  ~JavaLoaderInvoker() { VLOG(1) << "Destructing java loader invoker"; }

  void SetWorkerInfo(int worker_id, int worker_num,
                     const grape::CommSpec& comm_spec) {
    VLOG(2) << "JavaLoaderInvoekr set worker Id, num " << worker_id << ", "
            << worker_num;
    worker_id_ = worker_id;
    worker_num_ = worker_num;
    comm_spec_ = comm_spec;
    communicator.InitCommunicator(comm_spec.comm());
  }
  // load the class and call init method
  // mode = {graphx, giraph}
  void InitJavaLoader(const std::string& mode) {
    if (mode == "giraph") {
      initForGiraph();
    } else if (mode == "graphx") {
      initForGraphx();
    } else {
      LOG(ERROR) << "Expect graphx or giraph";
    }
  }
  // getters use by java
  std::vector<std::vector<char>>& GetOids() { return oids; }
  std::vector<std::vector<char>>& GetVdatas() { return vdatas; }
  std::vector<std::vector<char>>& GetEdgeSrcs() { return esrcs; }
  std::vector<std::vector<char>>& GetEdgeDsts() { return edsts; }
  std::vector<std::vector<char>>& GetEdgeDatas() { return edatas; }

  std::vector<std::vector<int>>& GetOidOffsets() { return oid_offsets; }
  std::vector<std::vector<int>>& GetVdataOffsets() { return vdata_offsets; }
  std::vector<std::vector<int>>& GetEdgeSrcOffsets() { return esrc_offsets; }
  std::vector<std::vector<int>>& GetEdgeDstOffsets() { return edst_offsets; }
  std::vector<std::vector<int>>& GetEdgeDataOffsets() { return edata_offsets; }
  int WorkerId() const { return worker_id_; }
  int WorkerNum() const { return worker_num_; }
  int LoadingThreadNum() const { return load_thread_num; }
  void SetTypeInfoInt(int info_int) { parseGiraphTypeInt(info_int); }

  void load_vertices_and_edges(const std::string& vertex_location,
                               const std::string vformatter) {
    VLOG(2) << "vertex file: " << vertex_location
            << ", formatter: " << vformatter;
    if (vformatter.find("giraph:") == std::string::npos) {
      LOG(ERROR) << "Expect a giraph formatter: giraph:your.class.name";
      return;
    }
    std::string vertex_location_prune = vertex_location;
    if (vertex_location.find("#") != std::string::npos) {
      vertex_location_prune =
          vertex_location.substr(0, vertex_location.find("#"));
    }
    std::string vformatter_class = vformatter.substr(7, std::string::npos);
    int giraph_type_int = callJavaLoaderVertices(vertex_location_prune.c_str(),
                                                 vformatter_class.c_str());
    CHECK_GE(giraph_type_int, 0);

    // fetch giraph graph types infos, so we can optimizing graph store by use
    // primitive types for LongWritable.
    parseGiraphTypeInt(giraph_type_int);
  }

  void readDataFromMMapedFile(const std::string& location_prefix,
                              bool forVertex, int max_partition_id,
                              int mapped_size) {
    int partition_id = 0;
    // FIXME
    int cnt = 0;
    while (partition_id < max_partition_id) {
      std::string file_path = location_prefix + std::to_string(partition_id);
      //      size_t file_size = get_file_size(file_path.c_str());
      //      if (file_size > 0) {
      // VLOG(1) << "opening file " << file_path << ", size " << file_size;
      VLOG(1) << "reading from " << file_path;
      int fd =
          shm_open(file_path.c_str(), O_RDWR, S_IRUSR | S_IWUSR);  // no O_CREAT
      if (fd < 0) {
        LOG(ERROR) << "Not exists " << file_path;
        partition_id += 1;
        continue;
      }

      void* mmapped_data =
          mmap(NULL, mapped_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmapped_data == MAP_FAILED) {
        close(fd);
        VLOG(1) << "Error mmapping the file " << file_path;
        return;
      }

      cnt += 1;
      // first 8 bytes are size in int64_t;
      int64_t data_len = *reinterpret_cast<int64_t*>(mmapped_data);
      CHECK_GT(data_len, 0);
      VLOG(1) << "Reading first 8 bytes, indicating total len: " << data_len;
      char* data_start = (reinterpret_cast<char*>(mmapped_data) + 8);

      if (forVertex) {
        int numVertices =
            digestVerticesFromMapedFile(data_start, data_len, partition_id);
        VLOG(1) << "Finish reading mmaped v file, got " << numVertices
                << " vertices";
      } else {
        int numEdges =
            digestEdgesFromMapedFile(data_start, data_len, partition_id);
        VLOG(1) << "Finish reading mmaped e file, got " << numEdges << " edges";
      }

      //      } else {
      //        VLOG(1) << "file: " << file_path << "size " << file_size
      //                << " doesn't exist";
      //      }
      partition_id += 1;
    }
    if (forVertex) {
      VLOG(1) << " Worker [" << worker_id_
              << "] finish loading vertices for max partition: "
              << max_partition_id << ", success: " << cnt;
    } else {
      VLOG(1) << " Worker [" << worker_id_
              << "] finish loading edges for max partition: "
              << max_partition_id << ", success: " << cnt;
    }
  }

  // Work for graphx graph loading, the input is the prefix for memory mapped
  // file.
  void load_vertices(const std::string& location_prefix, int max_parition_id,
                     int mapped_size) {
    readDataFromMMapedFile(location_prefix, true, max_parition_id, mapped_size);
    // it is possible that on some nodes, there are no graphx data. We broad
    // cast type int, to allow subsequent move.
    MPI_Barrier(comm_spec_.comm());
    int prev_oid_type, prev_vdata_type, prev_edata_type, prev_msg_type;
    prev_oid_type = oid_type;
    prev_vdata_type = vdata_type;
    prev_edata_type = edata_type;
    prev_msg_type = msg_type;
    communicator.Max(prev_oid_type, oid_type);
    communicator.Max(prev_vdata_type, vdata_type);
    communicator.Max(prev_edata_type, edata_type);
    communicator.Max(prev_msg_type, msg_type);
    VLOG(1) << "previous oid: " << prev_oid_type
            << ", after aggre: " << oid_type;
    VLOG(1) << "previous edata: " << prev_edata_type
            << ", after aggre: " << edata_type;
    VLOG(1) << "previous vdata: " << prev_vdata_type
            << ", after aggre: " << vdata_type;
    VLOG(1) << "previous msg: " << prev_msg_type
            << ", after aggre: " << msg_type;
  }

  // load vertices must be called before load edge, since we assume giraph type
  // int has been calculated.
  void load_edges(const std::string& edge_location,
                  const std::string eformatter) {
    VLOG(2) << "edge file: " << edge_location << " eformatter: " << eformatter;
    if (eformatter.find("giraph:") == std::string::npos) {
      LOG(ERROR) << "Expect a giraph formatter: giraph:your.class.name";
      return;
    }
    std::string edge_location_prune = edge_location;
    if (edge_location.find("#") != std::string::npos) {
      edge_location_prune = edge_location.substr(0, edge_location.find("#"));
    }
    std::string eformatter_class = eformatter.substr(7, std::string::npos);

    callJavaLoaderEdges(edge_location_prune.c_str(), eformatter_class.c_str());
  }

  void load_edges(const std::string& location_prefix, int max_parition_id,
                  int mapped_size) {
    readDataFromMMapedFile(location_prefix, false, max_parition_id,
                           mapped_size);
  }

  std::shared_ptr<arrow::Table> get_edge_table() {
    CHECK(oid_type > 0 && edata_type > 0);
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t esrc_total_length = 0, edst_total_length = 0,
            edata_total_length = 0;
    int64_t esrc_total_bytes = 0, edst_total_bytes = 0, edata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      esrc_total_length += esrc_offsets[i].size();
      edst_total_length += edst_offsets[i].size();
      edata_total_length += edata_offsets[i].size();

      esrc_total_bytes += esrcs[i].size();
      edst_total_bytes += edsts[i].size();
      edata_total_bytes += edatas[i].size();
    }

    VLOG(10) << "worker " << worker_id_ << " Building edge table "
             << " esrc len: [" << esrc_total_length << "] esrc total bytes: ["
             << esrc_total_bytes << "] edst len: [" << edst_total_length
             << "] esrc total bytes: [" << edst_total_bytes << "] edata len: ["
             << edata_total_length << "] edata total bytes: ["
             << edata_total_bytes << "]";

    CHECK((esrc_total_length == edst_total_length) &&
          (edst_total_length == edata_total_length));
    double edgeTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> esrc_array, edst_array, edata_array;

    buildArray(oid_type, esrc_array, esrcs, esrc_offsets);
    buildArray(oid_type, edst_array, edsts, edst_offsets);
    buildArray(edata_type, edata_array, edatas, edata_offsets);

    VLOG(10) << "Finish edge array building esrc: " << esrc_array->ToString()
             << " edst: " << edst_array->ToString()
             << " edata: " << edata_array->ToString();

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("src", getArrowDataType(oid_type)),
                       arrow::field("dst", getArrowDataType(oid_type)),
                       arrow::field("data", getArrowDataType(edata_type))});

    auto res =
        arrow::Table::Make(schema, {esrc_array, edst_array, edata_array});
    VLOG(10) << "worker " << worker_id_
             << " generated table, rows:" << res->num_rows()
             << " cols: " << res->num_columns();

    edgeTableBuildingTime += grape::GetCurrentTime();
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table cost: " << edgeTableBuildingTime;
    return res;
  }

  std::shared_ptr<arrow::Table> get_vertex_table() {
    CHECK(oid_type > 0 && vdata_type > 0);
    // copy the data in std::vector<char> to arrowBinary builder.
    int64_t oid_length = 0;
    int64_t oid_total_bytes = 0;
    int64_t vdata_total_length = 0;
    int64_t vdata_total_bytes = 0;
    for (int i = 0; i < load_thread_num; ++i) {
      oid_length += oid_offsets[i].size();
      vdata_total_length += vdata_offsets[i].size();
      oid_total_bytes += oids[i].size();
      vdata_total_bytes += vdatas[i].size();
    }
    CHECK(oid_length == vdata_total_length);
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table from oid array of size [" << oid_length
             << "] oid total bytes: [" << oid_total_bytes << "] vdata size: ["
             << vdata_total_length << "] total bytes: [" << vdata_total_bytes
             << "]";

    double vertexTableBuildingTime = -grape::GetCurrentTime();

    std::shared_ptr<arrow::Array> oid_array;
    std::shared_ptr<arrow::Array> vdata_array;

    buildArray(oid_type, oid_array, oids, oid_offsets);
    buildArray(vdata_type, vdata_array, vdatas, vdata_offsets);

    VLOG(10) << "Finish vertex array building oid array: "
             << oid_array->ToString() << " vdata: " << vdata_array->ToString();

    std::shared_ptr<arrow::Schema> schema =
        arrow::schema({arrow::field("oid", getArrowDataType(oid_type)),
                       arrow::field("vdata", getArrowDataType(vdata_type))});

    auto res = arrow::Table::Make(schema, {oid_array, vdata_array});
    VLOG(10) << "worker " << worker_id_
             << " generated table, rows:" << res->num_rows()
             << " cols: " << res->num_columns();

    vertexTableBuildingTime += grape::GetCurrentTime();
    VLOG(10) << "worker " << worker_id_
             << " Building vertex table cost: " << vertexTableBuildingTime;
    return res;
  }

 private:
  size_t get_file_size(const char* file_name) {
    struct stat st;

    if (stat(file_name, &st) != 0) {
      LOG(ERROR) << "file " << file_name << "doesn't exist";
      return 0;
    }
    return st.st_size;
  }
  void initForGiraph() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);
      // construct java loader obj.
      jmethodID create_method = env->GetStaticMethodID(
          loader_class, JAVA_LOADER_CREATE_METHOD, JAVA_LOADER_CREATE_SIG);
      CHECK(create_method);

      java_loader_obj = env->NewGlobalRef(env->CallStaticObjectMethod(
          loader_class, create_method, gs_class_loader_obj));
      CHECK(java_loader_obj);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_INIT_METHOD, JAVA_LOADER_INIT_SIG);
      CHECK_NOTNULL(loader_method);

      env->CallVoidMethod(java_loader_obj, loader_method, worker_id_,
                          worker_num_, load_thread_num, oids_jobj, vdatas_jobj,
                          esrcs_jobj, edsts_jobj, edatas_jobj, oid_offsets_jobj,
                          vdata_offsets_jobj, esrc_offsets_jobj,
                          edst_offsets_jobj, edata_offsets_jobj);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Init java loader";
        return;
      }
    }
    VLOG(1) << "Successfully init java loader with params ";
  }

  // When loading for graphx, we don't need FileLoader in java.
  void initForGraphx() {}

  // Work for both vdata and edata.
  template <typename T>
  void readData(int num_slot, int src_ptr_step_size, char* src_ptr,
                char* dst_ptr, std::vector<int>& dst_offset) {
    for (auto index = 0; index < num_slot; index++) {
      T vdata = *reinterpret_cast<T*>(src_ptr);
      std::memcpy(dst_ptr, &vdata, sizeof(vdata));
      // VLOG(1) << "Reading vdata: " << vdata << " sizeof " << sizeof(vdata);
      dst_ptr += sizeof(vdata);
      src_ptr += src_ptr_step_size;  // move 1 * 8 bytes
      dst_offset.push_back(static_cast<int>(sizeof(vdata)));
    }
  }

  /* Deserializing from the mmaped file. The layout of is
   |   8bytes | 4 bytes  | 4 bytes  | 4 bytes  | 8 bytes  | 4 or 8 bytes | ...
   | length  |  vd class | ed class | msgClass | oid-0    | vdata        | ...

   do not modify pointer */
  int digestVerticesFromMapedFile(char* data, int64_t chunk_len,
                                  int partition_id) {
    if (chunk_len < 12) {
      LOG(ERROR) << "At least need 16 bytes to read meta";
      return 0;
    }
    int _vdata_type = *reinterpret_cast<int*>(data);
    VLOG(1) << "vdClass " << _vdata_type;
    data += 4;
    int _edata_type = *reinterpret_cast<int*>(data);
    VLOG(1) << "edClass " << _edata_type;
    data += 4;
    int _msg_type = *reinterpret_cast<int*>(data);
    VLOG(1) << "msgClass " << _msg_type;
    data += 4;
    // check consistency.
    updateTypeInfo(_vdata_type, _edata_type, _msg_type);

    // f
    chunk_len -= 12;
    int total_vertices = 0;
    int bytes_gap = 0;
    if (vdata_type == 4 || vdata_type == 7) {
      CHECK_EQ(chunk_len % 16, 0);
      total_vertices = chunk_len / 16;
      bytes_gap = 16;
    } else if (vdata_type == 2) {
      CHECK_EQ(chunk_len % 12, 0);
      total_vertices = chunk_len / 12;
      bytes_gap = 12;
    } else {
      LOG(ERROR) << "unexpected " << vdata_type;
    }
    VLOG(1) << "Total vertices in partition " << partition_id << ": "
            << total_vertices;

    std::vector<char>& dst_oids0 = oids[0];
    std::vector<char>& dst_vdatas0 = vdatas[0];
    std::vector<int>& dst_oids_offsets0 = oid_offsets[0];
    std::vector<int>& dst_vdata_offsets0 = vdata_offsets[0];
    // read acutal data
    // when we start put data in vector<char>, there may be already bytes there.
    // it is safe to start from size. But first of all, we need to resize.
    size_t dst_oid_previous_size = dst_oids0.size();
    size_t dst_vdata_previous_size = dst_vdatas0.size();
    dst_oids0.resize(dst_oid_previous_size + 8 * total_vertices);
    dst_vdatas0.resize(dst_vdata_previous_size +
                       (bytes_gap - 8) * total_vertices);

    VLOG(1) << "bytes_gap: " << bytes_gap
            << "dst oids prev size: " << dst_oid_previous_size
            << "dst oids size: " << dst_oids0.size()
            << "dst vdata prev size: " << dst_vdata_previous_size
            << " dst vdata size: " << dst_vdatas0.size();
    {
      char* src_oid_ptr = data;
      char* dst_oids_ptr = &dst_oids0[dst_oid_previous_size];
      for (auto i = 0; i < total_vertices; ++i) {
        int64_t oid = *reinterpret_cast<int64_t*>(src_oid_ptr);
        std::memcpy(dst_oids_ptr, &oid, sizeof(oid));
        // VLOG(1) << "Reading oid: " << oid << " sizeof " << sizeof(oid);
        dst_oids_ptr += sizeof(oid);
        src_oid_ptr += bytes_gap;  // move 1 * 8 bytes
        dst_oids_offsets0.push_back(8);
      }
    }
    VLOG(1) << "Finish oid putting";
    {
      char* src_vdata_ptr = data + 8;  // shift right to point to vdata
      char* dst_vdata_ptr = &dst_vdatas0[dst_vdata_previous_size];
      if (vdata_type == 4) {
        readData<int64_t>(total_vertices, bytes_gap, src_vdata_ptr,
                          dst_vdata_ptr, dst_vdata_offsets0);
      } else if (vdata_type == 2) {
        readData<int32_t>(total_vertices, bytes_gap, src_vdata_ptr,
                          dst_vdata_ptr, dst_vdata_offsets0);
      } else if (vdata_type == 7) {
        readData<double>(total_vertices, bytes_gap, src_vdata_ptr,
                         dst_vdata_ptr, dst_vdata_offsets0);
      } else {
        LOG(ERROR) << "Unrecognized data type: " << vdata_type;
      }
    }
    VLOG(1) << "Finish reading vertices";
    return total_vertices;
  }

  /* Deserializing from the mmaped file. The layout of is
 |   8bytes | 4 bytes  | 4 bytes  | 4 bytes  | 8 bytes  | 4 or 8 bytes | ...
 | length  |  vd class | ed class | msgClass | oid-0    | vdata        | ...

 do not modify pointer */
  int digestEdgesFromMapedFile(char* data, int64_t chunk_len,
                               int partition_id) {
    // no headers need for edges
    CHECK_GT(oid_type, 0);
    CHECK_GT(vdata_type, 0);
    CHECK_GT(edata_type, 0);
    CHECK_GT(msg_type, 0);
    int edges_num = 0;
    int bytes_gap = 0;
    if (edata_type == 4 || edata_type == 7) {
      CHECK_EQ(chunk_len % 24, 0);
      edges_num = chunk_len / 24;
      bytes_gap = 24;
    } else if (edata_type == 2) {
      CHECK_EQ(chunk_len % 20, 0);
      edges_num = chunk_len / 20;
      bytes_gap = 20;
    } else {
      LOG(ERROR) << "unexpected " << edata_type;
    }
    VLOG(1) << "Total edges in partition " << partition_id << ": " << edges_num;

    std::vector<char>& dst_esrc0 = esrcs[0];
    std::vector<char>& dst_edst0 = edsts[0];
    std::vector<char>& dst_edata0 = edatas[0];
    std::vector<int>& dst_esrc_offset0 = esrc_offsets[0];
    std::vector<int>& dst_edst_offset0 = edst_offsets[0];
    std::vector<int>& dst_edata_offset0 = edata_offsets[0];
    // read acutal data
    // when we start put data in vector<char>, there may be already bytes there.
    // it is safe to start from size. But first of all, we need to resize.
    size_t dst_esrc_previous_size = dst_esrc0.size();
    size_t dst_edst_previous_size = dst_edst0.size();
    size_t dst_edata_previous_size = dst_edata0.size();
    dst_esrc0.resize(dst_esrc_previous_size + 8 * edges_num);
    dst_edst0.resize(dst_edst_previous_size + 8 * edges_num);
    dst_edata0.resize(dst_edata_previous_size + (bytes_gap - 16) * edges_num);

    VLOG(1) << "bytes_gap: " << bytes_gap
            << "dst esrc prev size: " << dst_esrc_previous_size
            << "dst esrc size: " << dst_esrc0.size()
            << "dst edst prev size: " << dst_edst_previous_size
            << " dst edst size: " << dst_edst0.size()
            << "dst edata prev size: " << dst_edata_previous_size
            << "dst edata size: " << dst_edata0.size();
    {
      char* src_esrc_oid_ptr = data;
      char* src_edst_oid_ptr = data + 8;
      char* dst_esrc_oid_ptr = &dst_esrc0[dst_esrc_previous_size];
      char* dst_edst_oid_ptr = &dst_edst0[dst_edst_previous_size];
      for (auto i = 0; i < edges_num; ++i) {
        int64_t src_esrc_oid = *reinterpret_cast<int64_t*>(src_esrc_oid_ptr);
        int64_t src_edst_oid = *reinterpret_cast<int64_t*>(src_edst_oid_ptr);
        std::memcpy(dst_esrc_oid_ptr, &src_esrc_oid, sizeof(src_esrc_oid));
        std::memcpy(dst_edst_oid_ptr, &src_edst_oid, sizeof(src_edst_oid));

        // VLOG(1) << "Reading edge [" << src_esrc_oid << "->" << src_edst_oid
        //         << "] size " << sizeof(src_esrc_oid);
        src_esrc_oid_ptr += bytes_gap;
        src_edst_oid_ptr += bytes_gap;
        dst_esrc_oid_ptr += 8;
        dst_edst_oid_ptr += 8;
        dst_esrc_offset0.push_back(8);
        dst_edst_offset0.push_back(8);
      }
    }
    VLOG(1) << "Finish oid putting";
    {
      char* src_edata_ptr = data + 16;  // shift right to point to vdata
      char* dst_edata_ptr = &dst_edata0[dst_edata_previous_size];
      if (vdata_type == 4) {
        readData<int64_t>(edges_num, bytes_gap, src_edata_ptr, dst_edata_ptr,
                          dst_edata_offset0);
      } else if (vdata_type == 2) {
        readData<int32_t>(edges_num, bytes_gap, src_edata_ptr, dst_edata_ptr,
                          dst_edata_offset0);
      } else if (vdata_type == 7) {
        readData<double>(edges_num, bytes_gap, src_edata_ptr, dst_edata_ptr,
                         dst_edata_offset0);
      } else {
        LOG(ERROR) << "Unrecognized data type: " << vdata_type;
      }
    }
    VLOG(1) << "Finish reading edges";
    return edges_num;
  }

  std::shared_ptr<arrow::DataType> getArrowDataType(int data_type) {
    if (data_type == 2) {
      return vineyard::ConvertToArrowType<int32_t>::TypeValue();
    } else if (data_type == 4) {
      return vineyard::ConvertToArrowType<int64_t>::TypeValue();
    } else if (data_type == 6) {
      return vineyard::ConvertToArrowType<float>::TypeValue();
    } else if (data_type == 7) {
      return vineyard::ConvertToArrowType<double>::TypeValue();
    } else if (data_type == 9) {
      return vineyard::ConvertToArrowType<std::string>::TypeValue();
    } else if (data_type == 1) {
      return arrow::null();
    } else {
      LOG(ERROR) << "Wrong data type: " << data_type;
      return arrow::null();
    }
  }
  void buildArray(int data_type, std::shared_ptr<arrow::Array>& array,
                  const std::vector<std::vector<char>>& data_arr,
                  const std::vector<std::vector<int>>& offset_arr) {
    if (data_type == 2) {
      BuildArray<int32_t>(array, data_arr, offset_arr);
    } else if (data_type == 4) {
      BuildArray<int64_t>(array, data_arr, offset_arr);
    } else if (data_type == 6) {
      BuildArray<float>(array, data_arr, offset_arr);
    } else if (data_type == 7) {
      BuildArray<double>(array, data_arr, offset_arr);
    } else if (data_type == 9) {
      BuildArray<std::string>(array, data_arr, offset_arr);
    } else if (data_type == 1) {
      BuildArray<grape::EmptyType>(array, data_arr, offset_arr);
    } else {
      LOG(ERROR) << "Wrong data type: " << data_type;
    }
  }
  void createFFIPointers() {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      if (!getenv("USER_JAR_PATH")) {
        LOG(ERROR) << "expect env USER_JAR_PATH set";
      }
      std::string user_jar_path = getenv("USER_JAR_PATH");

      gs_class_loader_obj = gs::CreateClassLoader(env, user_jar_path);
      CHECK_NOTNULL(gs_class_loader_obj);
      {
        oids_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oids));
        vdatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdatas));
        esrcs_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrcs));
        edsts_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edsts));
        edatas_jobj =
            gs::CreateFFIPointer(env, DATA_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edatas));
      }
      {
        oid_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&oid_offsets));
        vdata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&vdata_offsets));
        esrc_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&esrc_offsets));
        edst_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edst_offsets));
        edata_offsets_jobj =
            gs::CreateFFIPointer(env, OFFSET_VECTOR_VECTOR, gs_class_loader_obj,
                                 reinterpret_cast<jlong>(&edata_offsets));
      }
    }
    VLOG(1) << "Finish creating ffi wrappers";
  }

  int callJavaLoaderVertices(const char* file_path, const char* java_params) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_LOAD_VE_METHOD, JAVA_LOADER_LOAD_VE_SIG);
      CHECK_NOTNULL(loader_method);

      jstring file_path_jstring = env->NewStringUTF(file_path);
      jstring java_params_jstring = env->NewStringUTF(java_params);
      double javaLoadingTime = -grape::GetCurrentTime();

      jint res = env->CallIntMethod(java_loader_obj, loader_method,
                                    file_path_jstring, java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return -1;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1) << "Successfully Loaded graph vertex data from Java loader, "
                 "duration: "
              << javaLoadingTime;
      return res;
    } else {
      LOG(ERROR) << "Java env not available.";
      return -1;
    }
  }

  int callJavaLoaderEdges(const char* file_path, const char* java_params) {
    gs::JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      jclass loader_class =
          LoadClassWithClassLoader(env, gs_class_loader_obj, JAVA_LOADER_CLASS);
      CHECK_NOTNULL(loader_class);

      jmethodID loader_method = env->GetMethodID(
          loader_class, JAVA_LOADER_LOAD_E_METHOD, JAVA_LOADER_LOAD_E_SIG);
      CHECK_NOTNULL(loader_method);

      jstring file_path_jstring = env->NewStringUTF(file_path);
      jstring java_params_jstring = env->NewStringUTF(java_params);
      double javaLoadingTime = -grape::GetCurrentTime();

      env->CallVoidMethod(java_loader_obj, loader_method, file_path_jstring,
                          java_params_jstring);
      if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        env->ExceptionClear();
        LOG(ERROR) << "Exception in Calling java loader.";
        return -1;
      }

      javaLoadingTime += grape::GetCurrentTime();
      VLOG(1)
          << "Successfully Loaded graph edge data from Java loader, duration: "
          << javaLoadingTime;
      return 0;
    } else {
      LOG(ERROR) << "Java env not available.";
      return -1;
    }
  }

  void parseGiraphTypeInt(int type_int) {
    edata_type = (type_int & 0x000F);
    type_int = type_int >> GIRAPH_TYPE_CODE_LENGTH;
    vdata_type = (type_int & 0x000F);
    type_int = type_int >> GIRAPH_TYPE_CODE_LENGTH;
    oid_type = (type_int & 0x000F);
    type_int = type_int >> GIRAPH_TYPE_CODE_LENGTH;
    CHECK_EQ(type_int, 0);
    VLOG(1) << "giraph types: oid [" << oid_type << "]  vd: [" << vdata_type
            << "]  ed: [" << edata_type << "]";
  }

  void updateTypeInfo(int _vdata_type, int _edata_type, int _msg_type) {
    if (oid_type == -1) {
      oid_type = 4;  // long
    }
    if (vdata_type == -1) {
      vdata_type = _vdata_type;
    } else {
      CHECK_EQ(vdata_type, _vdata_type);
    }

    if (edata_type == -1) {
      edata_type = _edata_type;
    } else {
      CHECK_EQ(edata_type, _edata_type);
    }

    if (msg_type == -1) {
      msg_type = _msg_type;
    } else {
      CHECK_EQ(msg_type, _msg_type);
    }
  }

  int worker_id_, worker_num_, load_thread_num;
  int oid_type, vdata_type, edata_type;
  int msg_type;  // for graphx
  std::vector<std::vector<char>> oids;
  std::vector<std::vector<char>> vdatas;
  std::vector<std::vector<char>> esrcs;
  std::vector<std::vector<char>> edsts;
  std::vector<std::vector<char>> edatas;

  std::vector<std::vector<int>> oid_offsets;
  std::vector<std::vector<int>> vdata_offsets;
  std::vector<std::vector<int>> esrc_offsets;
  std::vector<std::vector<int>> edst_offsets;
  std::vector<std::vector<int>> edata_offsets;

  jobject gs_class_loader_obj;
  jobject java_loader_obj;

  jobject oids_jobj;
  jobject vdatas_jobj;
  jobject esrcs_jobj;
  jobject edsts_jobj;
  jobject edatas_jobj;

  jobject oid_offsets_jobj;
  jobject vdata_offsets_jobj;
  jobject esrc_offsets_jobj;
  jobject edst_offsets_jobj;
  jobject edata_offsets_jobj;

  grape::CommSpec comm_spec_;
  grape::Communicator communicator;
};
};  // namespace gs

#endif

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVA_LOADER_INVOKER_H_
