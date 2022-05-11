
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_CSR_H
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_CSR_H

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
class GraphXCSR : public vineyard::Registered<GraphXCSR<OID_T, VID_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using vineyard_offset_array_t =
      typename vineyard::InternalType<int64_t>::vineyard_array_type;
  using vineyard_offset_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;

 public:
  GraphXCSR() {}
  ~GraphXCSR() {}

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    edges.Construct(meta.GetMemberMeta("edges"));

    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("offsets"));
      offsets = array.GetArray();
    }
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas = array.GetArray();
    }

    local_vnum = offset->length() - 1;
    LOG(INFO) << "In constructing graphx csr, length: " << local_vnum;
    edge_ptr = reinterpret_cast<const nbr_t*>(edges->GetValue(0));
    LOG(INFO) << "total edges: " << offsets->Value(local_vnum + 1);
    for (auto i = 0; i < local_vnum; ++i) {
      const nbr_t* start = &edge_ptr[offsets->Value(i)];
      const nbr_t* end = &edge_ptr[offsets->Value(i + 1)];
      while (start != end) {
        LOG(INFO) < "Edge (" + i << ", " << start->vid
                                 << ", eid: " << start->eid
                                 << ", edata:" << edatas->Value(start->eid);
        start++;
      }
    }
    LOG(INFO) << "Finish construct GraphXCSR: ";
  }

 private:
  vid_t local_vnum;
  const nbr_t* edge_ptr;
  vineyard::FixedSizeBinaryArray edges;
  std::shared_ptr<arrow::Int64Array> offsets;
  std::shared_ptr<edata_array_t> edatas;

  template <typename _OID_T, typename _VID_T>
  friend class GraphXCSRBuilder;
}

template <typename OID_T, typename VID_T, typename ED_T>
class GraphXCSRBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using edata_t = ED_T;

 public:
  explicit GraphXCSRBuilder(vineyard::Client& client) : client_(client) {}

  void SetEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this.edges = edges;
  }
  void SetOffsetArray(const vineyard::NumericArray<oid_t>& array) {
    this.offset = array;
  }
  void SetEdataArray(const vineyard::NumericArray<oid_t>& array) {
    this.edatas = array;
  }
  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto graphx_csr = std::make_shared<GraphXCSR<oid_t, vid_t, edata_t>>();
    graphx_csr->meta_.SetTypeName(
        type_name<GraphXCSR<oid_t, vid_t, edata_t>>());

    size_t nBytes = 0;
    graphx_csr->offset = offset.GetArray();
    nBytes += offset.nbytes();
    graphx_csr->edges = edges;
    nBytes += edges.nbytes();
    graphx_csr->edatas = edata.GetArray();
    nBytes += edatas.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;

    graphx_csr->meta_.AddMember("edges", ie.meta());
    graphx_csr->meta_.AddMember("offsets", offset.meta());
    graphx_csr->meta_.AddMember("edatas", edatas.meta());
    graphx_csr->meta_.SetNBytes(nBytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(graphx_csr->meta_, graphx_csr->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(graphx_csr);
  }

 private:
  vineyard::Client& client;
  vineyard::FixedSizeBinaryArray edges;
  vineyard::NumericArray<int64_t> offset;
  vineyard::NumericArray<edata_t> edatas;
}

template <typename OID_T, typename VID_T, typename ED_T>
class BasicGraphXCSRBuilder : public GraphXCSRBuilder<OID_T, VID_T, ED_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using offset_array_builder_t =
      typename vineyard::ConvertToArrowType<int64_t>::BuilderType;
  using vineyard_offset_array_builder_t =
      typename vineyard::InternalType<oid_t>::vineyard_builder_type;
  using offset_array_t =
      typename vineyard::ConvertToArrowType<int64_t>::ArrayType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using edata_array_builder_t =
      typename vineyard::ConvertToArrowType<edata_t>::BuilderType;
  using vineyard_edata_array_builder_t =
      typename vineyard::InternalType<edata_t>::vineyard_builder_type;
  using vid_array_t = typename vineyard::ConvertToArrowType<int64_t>::ArrayType;

 public:
  explicit BasicGraphXCSRBuilder(vineyard::Client& client)
      : GraphXCSRBuilder(client) {}

  void init(VID_T vnum) {
    vnum_ = vnum;
    degree_.clear();
    degree_.resize(vnum, 0);
  }

  void inc_degree(VID_T i) { ++degree_[i]; }

  void build_offsets() {
    edge_num_ = 0;
    for (auto d : degree_) {
      edge_num_ += d;
    }
    edge_builder.Resize(edge_num_);

    offset.resize(vnum + 1);
    int64_t length = 0;
    offset[0] = 0;
    for (VID_T i = 0; i < vnum_; ++i) {
      length += degreee_[i];
      offset[i + 1] = offset[i] + degree_[i];
    }
    CHECK_EQ(offset[vnum_], edge_num_);
    offset_array_builder_t builder;
    builder.AppendValues(offset);
    builder.Finish(&offset_array);
    {
      std::vector<int> tmp;
      tmp.swap(degree_);
    }
  }

  void add_edges(const std::shared_ptr<vid_array_t>& srcLids,
                 const std::shared_ptr<vid_array_t>& dstLids,
                 const std::shared_ptr<edata_array_t>& edatas) {
    CHECK_EQ(srcLids->length(), dstLids->length());
    CHECK_EQ(dstLids.length(), edatas->length());

    edata_array_builder_t edata_bulider;
    edata_builder.Reserve(dstLids->length());
    auto len = srcLids->length();
    for (auto i = 0; i < len; ++i) {
      vid_t srcLid = srcLids->Value(i);
      int dstPos = offset[srcLid]++;
      nbr_t* ptr = edge_builder.MutablePointer(dstPos);
      ptr->vid = dstLids->Value(i);
      ptr->eid = static_cast<eid_t>(i);
      LOG(INFO) << "push nbr(src=" << srcLid << ",dstLid=" << dstLids->Value(i)
                << ", after offset:" << offset[srcLid];
      edata_builder.UnsafeAppend(edatas->Value(i));
    }
    edata_builder.Finish(&edata_array);
    LOG(INFO) << "Finish adding " << len << "edges";
  }

  void sort() {
    const int64_t* offsets_ptr = offset_array->raw_values();
    for (VID_T i = 0; i < vnum; ++i) {
      nbr_t* begin = edge_builder.MutablePointer(offsets_ptr[i]);
      nbr_t* end = edge_builder.MutablePointer(offsets_ptr[i + 1]);
      std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
        return lhs.vid < rhs.vid;
      });
    }
    LOG(INFO) << "After sort";
  }

  vineyard::Status Build(vineyard::Client& client) override {
    {
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      edge_builder.Finish(&edges);

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetEdges(*res);
      LOG(INFO) << "Finish set edges";
    }

    CHECK_EQ(offset_array->length(), vnum);
    {
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           offset_array);
      this->SetOffsetArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
              offset_array_builder.Seal(client)));
      LOG(INFO) << "FINISh set offset array";
    }

    {
      vineyard_edata_array_builder_t edata_array_builder(client, edata_array);
      this->SetEdataArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<edata_t>>(
              offset_array_builder.Seal(client)));
      LOG(INFO) << "FINISh set edata array";
    }

    return vineyard::Status::OK();
  }

 private:
  vid_t vnum;
  size_t edge_num_;

  std::vector<int> degree_;
  vineyard::PodArrayBuilder<nbr_t> edge_builder;
  std::shared_ptr<offset_array_t> offset_array;  // for output
  vector<int64_t> offset;  // used for edge iterate in this
  std::shared_ptr<edata_array_t> edata_array;
};
}  // namespace gs

#endif