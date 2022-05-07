
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_EDGE_RDD_H
#define ANALYTICAL_ENGINE_CORE_JAVA_EDGE_RDD_H

#ifdef ENABLE_JAVA_SDK
#include <jni.h>
#endif

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"

#include "grape/graph/adj_list.h"
#include "grape/graph/immutable_csr.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/stream/byte_stream.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/type_alias.h"

/**
 * @brief Defines the RDD of edges. when data is feed into this, we assume it is
 * already shuffle and partitioned.
 *
 */
namespace gs {

template <typename OID_T, typename VID_T, typename ED_T>
class EdgePartition {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using nbr_t = grape::Nbr<vid_t, edata_t>;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using edata_array_builder_t =
      typename vineyard::ConvertToArrowType<edata_t>::BuilderType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  EdgePartition(vineyard::Client& client, bool directed = true)
      : client_(client), directed_(directed){};

  int64_t GetVerticesNum() { return vnum; }

  int64_t GetEdgesNum() { return outEdges.edge_num(); }

  grape::ImmutableCSR<vid_t, nbr_t>& GetInEdges() { return inEdges; }

  grape::ImmutableCSR<vid_t, nbr_t>& GetOutEdges() { return outEdges; }

  graphx::MutableTypedArray<oid_t>& getOidArray() { return oidArray_accessor; }

  void LoadEdges(const std::string& mmFiles, int64_t mapped_size) {
    std::shared_ptr<oid_array_t> edge_src, edge_dst;
    std::shared_ptr<edata_array_t> edge_data;
    readDataFromMMapedFile(mmFiles, mapped_size, edge_src, edge_dst, edge_data);
    LOG(INFO) << "Finish loading edges, edge src nums: " << edge_src->length()
              << " dst nums: " << edge_dst->length()
              << "edge data length: " << edge_data->length();
    // 0.1 Iterate over all edges, to build index, and count how many vertices
    // in this edge partition.
    CHECK_EQ(edge_src->length(), edge_dst->length());
    for (auto ind = 0; ind < edge_src->length(); ++ind) {
      auto srcId = edge_src->Value(ind);
      if (oid2Lid.find(srcId) == oid2Lid.end()) {
        oid2Lid.emplace(srcId, static_cast<vid_t>(oid2Lid.size()));
      }
    }
    for (auto ind = 0; ind < edge_dst->length(); ++ind) {
      auto dstId = edge_dst->Value(ind);
      if (oid2Lid.find(dstId) == oid2Lid.end()) {
        oid2Lid.emplace(dstId, static_cast<vid_t>(oid2Lid.size()));
      }
    }
    vnum = oid2Lid.size();
    LOG(INFO) << "Found " << vnum << " distince vertices from "
              << edge_src->length() << " edges";
    {
      oid_array_builder_t builder;
      builder.Reserve(vnum);
      for (auto iter = oid2Lid.begin(); iter != oid2Lid.end(); ++iter) {
        builder.UnsafeAppend(iter->second);
      }
      builder.Finish(&lid2Oid);
    }
    LOG(INFO) << "Finish lid2oid building, len" << lid2Oid->length();
    oidArray_accessor.Init(lid2Oid);
    LOG(INFO) << "Finish construct accessor: " << oidArray_accessor.GetLength();

    grape::ImmutableCSRBuild<vid_t, nbr_t> ie_builder, oe_builder;
    ie_builder.init(vnum);
    oe_builder.init(vnum);
    // both in and out
    for (auto i = 0; i < edge_src->length(); ++i) {
      oid_t srcId = edge_src->Value(i);
      oid_t dstId = edge_dst->Value(i);
      ie_builder.inc_degree(oid2Lid[dstId]);
      oe_builder.inc_degree(oid2Lid[srcId]);
    }
    ie_builder.build_offsets();
    oe_builder.build_offsets();
    // now add edges
    for (auto i = 0; i < edge_src->length(); ++i) {
      ie_builder.add_edge(edge_dst->Value(i),
                          nbr_t(edge_src->Value(i), edge_data->Value(i)));
      oe_builder.add_edge(edge_src->Value(i),
                          nbr_t(edge_dst->Value(i), edge_data->Value(i)));
    }
    ie_builder.finish(inEdges);
    oe_builder.finish(outEdges);
    LOG(INFO) << "Finish build inEdges and out Edges.";
  }

 private:
  void readDataFromMMapedFile(const std::string& files, int64_t mapped_size,
                              std::shared_ptr<oid_array_t>& edge_src,
                              std::shared_ptr<oid_array_t>& edge_dst,
                              std::shared_ptr<edata_array_t>& edge_data) {
    std::vector<std::string> files_splited;
    boost::split(files_splited, files, boost::is_any_of(":"));
    int success_cnt = 0;
    int64_t numEdges = 0;
    // FIXME
    for (auto file_path : files_splited) {
      LOG(INFO) << "reading from " << file_path;
      int fd =
          shm_open(file_path.c_str(), O_RDWR, S_IRUSR | S_IWUSR);  // no O_CREAT
      if (fd < 0) {
        LOG(ERROR) << " Not exists " << file_path;
        continue;
      }

      void* mmapped_data =
          mmap(NULL, mapped_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmapped_data == MAP_FAILED) {
        close(fd);
        LOG(INFO) << "Error mmapping the file " << file_path;
        return;
      }

      // first 8 bytes are size in int64_t;
      int64_t data_len = *reinterpret_cast<int64_t*>(mmapped_data);
      CHECK_GT(data_len, 0);
      LOG(INFO) << "Reading first 8 bytes, indicating total len: " << data_len;
      char* data_start = (reinterpret_cast<char*>(mmapped_data) + 8);

      int64_t res = digestEdgesFromMapedFile(data_start, data_len, edge_src,
                                             edge_dst, edge_data);
      LOG(INFO) << " Finish reading " << file_path << " got " << numEdges
                << " edges";
      numEdges += res;
      success_cnt += 1;
    }

    LOG(INFO) << "] finish loading edges,  success: " << success_cnt << " / "
              << files_splited.size() << " read: " << numEdges;
  }

  /* Deserializing from the mmaped file. The layout of is
 |   8bytes  | ...     | ...      |   ...
 | length    | srcOids | dstOids  |   edatas

 do not modify pointer */
  int64_t digestEdgesFromMapedFile(char* data, int64_t chunk_len,
                                   std::shared_ptr<oid_array_t>& edge_src,
                                   std::shared_ptr<oid_array_t>& edge_dst,
                                   std::shared_ptr<edata_array_t>& edge_data) {
    oid_array_builder_t src_builder, dst_builder;
    edata_array_builder_t edata_builder;
    oid_t* ptr = reinterpret_cast<oid_t*>(data);
    {
      src_builder.Reserve(chunk_len);
      for (auto i = 0; i < chunk_len; ++i) {
        src_builder.UnsafeAppend(*ptr);
        ptr += 1;
      }
      src_builder.Finish(&edge_src);
      LOG(INFO) << "Finish read src oid of length: " << chunk_len;

      dst_builder.Reserve(chunk_len);
      for (auto i = 0; i < chunk_len; ++i) {
        dst_builder.UnsafeAppend(*ptr);
        ptr += 1;
      }
      dst_builder.Finish(&edge_dst);
      LOG(INFO) << "Finish read dst oid of length: " << chunk_len;
    }

    {
      edata_t* data_ptr = reinterpret_cast<edata_t*>(ptr);
      edata_builder.Reserve(chunk_len);
      for (auto i = 0; i < chunk_len; ++i) {
        edata_builder.UnsafeAppend(*data_ptr);
        data_ptr += 1;
      }
      edata_builder.Finish(&edge_data);
      LOG(INFO) << "Finish read edata of length: " << chunk_len;
    }
    return chunk_len;
  }
  vineyard::Client& client_;
  grape::ImmutableCSR<vid_t, nbr_t> inEdges, outEdges;
  ska::flat_hash_map<oid_t, vid_t> oid2Lid;
  std::shared_ptr<oid_array_t> lid2Oid;
  graphx::MutableTypedArray<oid_t> oidArray_accessor;
  vid_t vnum;
  bool directed_;
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H
