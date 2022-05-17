
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
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/graphx_vertex_map.h"
#include "core/java/type_alias.h"
/**
 * @brief Defines the RDD of edges. when data is feed into this, we assume it is
 * already shuffle and partitioned.
 *
 */
namespace gs {

template <typename VID_T, typename ED_T>
class GraphXCSR : public vineyard::Registered<GraphXCSR<VID_T, ED_T>> {
 public:
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using vineyard_offset_array_t =
      typename vineyard::InternalType<int64_t>::vineyard_array_type;
  using vineyard_edata_array_t =
      typename vineyard::InternalType<edata_t>::vineyard_array_type;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using vineyard_edges_array_t = vineyard::FixedSizeBinaryArray;

  GraphXCSR() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXCSR<VID_T, ED_T>>{new GraphXCSR<VID_T, ED_T>()});
  }

  ~GraphXCSR() {}

  int64_t GetDegree(vid_t lid) { return getOffset(lid + 1) - getOffset(lid); }

  bool IsEmpty(vid_t lid) { return getOffset(lid + 1) == getOffset(lid); }

  nbr_t* GetBegin(VID_T i) { return &edge_ptr_[getOffset(i)]; }
  const nbr_t* GetBegin(VID_T i) const { return &edge_ptr_[getOffset(i)]; }

  nbr_t* GetEnd(VID_T i) { return &edge_ptr_[getOffset(i + 1)]; }
  const nbr_t* GetEnd(VID_T i) const { return &edge_ptr_[getOffset(i + 1)]; }

  vid_t VertexNum() const { return local_vnum_; }

  int64_t GetTotalEdgesNum() const { return edges_num_; }

  int64_t GetPartialEdgesNum(vid_t from, vid_t end) const {  //[from,end)
    CHECK_LT(from, end);
    CHECK_LE(end, local_vnum_);
    return offsets_->Value(static_cast<int64_t>(end)) -
           offsets_->Value(static_cast<int64_t>(from));
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    {
      vineyard_edges_array_t v6d_edges;
      v6d_edges.Construct(meta.GetMemberMeta("edges"));
      edges_ = v6d_edges.GetArray();
    }

    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("offsets"));
      offsets_ = array.GetArray();
    }
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
      edatas_accessor_.Init(edatas_);
    }

    local_vnum_ = offsets_->length() - 1;
    CHECK_GT(local_vnum_, 0);
    LOG(INFO) << "In constructing graphx csr, local vnum: " << local_vnum_;
    edge_ptr_ =
        const_cast<nbr_t*>(reinterpret_cast<const nbr_t*>(edges_->GetValue(0)));

    edges_num_ = getOffset(local_vnum_);
    LOG(INFO) << "total edges: " << edges_num_;
    // for (size_t i = 0; i < local_vnum_; ++i) {
    //   nbr_t* start = &edge_ptr_[getOffset(i)];
    //   nbr_t* end = &edge_ptr_[getOffset(i + 1)];
    //   while (start != end) {
    //     LOG(INFO) << "Edge (" << std::to_string(i) << ", " << start->vid
    //               << ", eid: " << start->eid
    //               << ", edata:" << edatas_->Value(start->eid);
    //     start++;
    //   }
    // }
    LOG(INFO) << "Finish construct GraphXCSR: ";
  }
  inline int64_t getOffset(vid_t lid) {
    CHECK_LE(lid, local_vnum_);
    return offsets_->Value(static_cast<int64_t>(lid));
  }
  inline graphx::ImmutableTypedArray<edata_t>& GetEdataArray() {
    return edatas_accessor_;
  }

 private:
  vid_t local_vnum_;
  int64_t edges_num_;
  nbr_t* edge_ptr_;
  std::shared_ptr<arrow::FixedSizeBinaryArray> edges_;
  std::shared_ptr<arrow::Int64Array> offsets_;
  std::shared_ptr<edata_array_t> edatas_;
  graphx::ImmutableTypedArray<edata_t> edatas_accessor_;

  template <typename _VID_T, typename _ED_T>
  friend class GraphXCSRBuilder;
};

template <typename VID_T, typename ED_T>
class GraphXCSRBuilder : public vineyard::ObjectBuilder {
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  explicit GraphXCSRBuilder(vineyard::Client& client) : client_(client) {}

  void SetEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this->edges = edges;
  }
  void SetOffsetArray(const vineyard::NumericArray<int64_t>& array) {
    this->offsets = array;
  }
  void SetEdataArray(const vineyard::NumericArray<edata_t>& array) {
    this->edatas = array;
  }
  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto graphx_csr = std::make_shared<GraphXCSR<vid_t, edata_t>>();
    graphx_csr->meta_.SetTypeName(type_name<GraphXCSR<vid_t, edata_t>>());

    size_t nBytes = 0;
    graphx_csr->offsets_ = offsets.GetArray();
    nBytes += offsets.nbytes();
    graphx_csr->edges_ = edges.GetArray();
    nBytes += edges.nbytes();
    graphx_csr->edatas_ = edatas.GetArray();
    graphx_csr->edatas_accessor_.Init(graphx_csr->edatas_);
    nBytes += edatas.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;

    graphx_csr->edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(graphx_csr->edges_->GetValue(0)));
    graphx_csr->local_vnum_ = graphx_csr->offsets_->length() - 1;
    graphx_csr->edges_num_ = graphx_csr->getOffset(graphx_csr->local_vnum_);

    graphx_csr->meta_.AddMember("edges", edges.meta());
    graphx_csr->meta_.AddMember("offsets", offsets.meta());
    graphx_csr->meta_.AddMember("edatas", edatas.meta());
    graphx_csr->meta_.SetNBytes(nBytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(graphx_csr->meta_, graphx_csr->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(graphx_csr);
  }

 private:
  vineyard::Client& client_;
  vineyard::FixedSizeBinaryArray edges;
  vineyard::NumericArray<int64_t> offsets;
  vineyard::NumericArray<edata_t> edatas;
};

template <typename OID_T, typename VID_T, typename ED_T>
class BasicGraphXCSRBuilder : public GraphXCSRBuilder<VID_T, ED_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;
  using offset_array_builder_t =
      typename vineyard::ConvertToArrowType<int64_t>::BuilderType;
  using vineyard_offset_array_builder_t =
      typename vineyard::InternalType<int64_t>::vineyard_builder_type;
  using offset_array_t =
      typename vineyard::ConvertToArrowType<int64_t>::ArrayType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<edata_t>::ArrayType;
  using edata_array_builder_t =
      typename vineyard::ConvertToArrowType<edata_t>::BuilderType;
  using vineyard_edata_array_builder_t =
      typename vineyard::InternalType<edata_t>::vineyard_builder_type;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;

 public:
  explicit BasicGraphXCSRBuilder(vineyard::Client& client, bool outEdges = true)
      : GraphXCSRBuilder<vid_t, edata_t>(client), outEdges_(outEdges) {}

  void LoadEdges(oid_array_builder_t& srcOidsBuilder,
                 oid_array_builder_t& dstOidsBuilder,
                 edata_array_builder_t& edatasBuilder,
                 GraphXVertexMap<oid_t, vid_t>& graphx_vertex_map) {
    std::shared_ptr<oid_array_t> srcOids, dstOids;
    std::shared_ptr<edata_array_t> edatas;
    {
      srcOidsBuilder.Finish(&srcOids);
      dstOidsBuilder.Finish(&dstOids);
      edatasBuilder.Finish(&edatas);
    }
    CHECK_EQ(srcOids->length(), dstOids->length());
    CHECK_EQ(dstOids->length(), edatas->length());
    vnum_ = graphx_vertex_map.GetInnerVertexSize(graphx_vertex_map.fid());
    LOG(INFO) << "inner vnum : " << vnum_;
    edges_num_ = srcOids->length();
    std::shared_ptr<vid_array_t> srcLids, dstLids;
    auto curFid = graphx_vertex_map.fid();
    LOG(INFO) << "fid: " << curFid;
    {
      vid_array_builder_t srcLidBuilder, dstLidBuilder;
      srcLidBuilder.Reserve(edges_num_);
      dstLidBuilder.Reserve(edges_num_);
      for (auto i = 0; i < edges_num_; ++i) {
        srcLidBuilder.UnsafeAppend(graphx_vertex_map.GetLid(srcOids->Value(i)));
        dstLidBuilder.UnsafeAppend(graphx_vertex_map.GetLid(dstOids->Value(i)));
      }
      srcLidBuilder.Finish(&srcLids);
      dstLidBuilder.Finish(&dstLids);
    }
    LOG(INFO) << "Finish building lid array";
    degree_.clear();
    degree_.resize(vnum_, 0);

    LOG(INFO) << "Loading edges size " << edges_num_
              << "vertices num: " << vnum_;
    for (auto i = 0; i < edges_num_; ++i) {
      inc_degree(srcLids->Value(i));
    }
    build_offsets();
    LOG(INFO) << "finish offset building";
    add_edges(srcLids, dstLids, edatas);
    sort();
    LOG(INFO) << "Finish loading edges";
  }

  vineyard::Status Build(vineyard::Client& client) override {
    {
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      edge_builder_.Finish(&edges);

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetEdges(*res);
      LOG(INFO) << "Finish set edges";
    }

    CHECK_EQ(offset_array_->length(), vnum_ + 1);
    {
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           offset_array_);
      this->SetOffsetArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              offset_array_builder.Seal(client)));
      LOG(INFO) << "FINISh set offset array";
    }

    {
      vineyard_edata_array_builder_t edata_array_builder(client, edata_array_);
      this->SetEdataArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<edata_t>>(
              edata_array_builder.Seal(client)));
      LOG(INFO) << "FINISh set edata array";
    }

    return vineyard::Status::OK();
  }

  std::shared_ptr<GraphXCSR<vid_t, edata_t>> MySeal(vineyard::Client& client) {
    return std::dynamic_pointer_cast<GraphXCSR<vid_t, edata_t>>(
        this->Seal(client));
  }

 private:
  void inc_degree(vid_t i) { ++degree_[i]; }

  void build_offsets() {
    edges_num_ = 0;
    for (auto d : degree_) {
      edges_num_ += d;
    }
    edge_builder_.Resize(edges_num_);

    offsets_.resize(vnum_ + 1);
    int64_t length = 0;
    offsets_[0] = 0;
    for (VID_T i = 0; i < vnum_; ++i) {
      length += degree_[i];
      offsets_[i + 1] = offsets_[i] + degree_[i];
    }
    CHECK_EQ(offsets_[vnum_], edges_num_);
    offset_array_builder_t builder;
    builder.AppendValues(offsets_);
    builder.Finish(&offset_array_);
    // We copy to offset_array since later we will modify inplace in <offset>
    {
      std::vector<int> tmp;
      tmp.swap(degree_);
    }
  }

  void add_edges(const std::shared_ptr<vid_array_t>& srcLids,
                 const std::shared_ptr<vid_array_t>& dstLids,
                 const std::shared_ptr<edata_array_t>& edatas) {
    edata_array_builder_t edata_builder_;
    edata_builder_.Reserve(dstLids->length());
    auto len = srcLids->length();
    for (auto i = 0; i < len; ++i) {
      vid_t srcLid = srcLids->Value(i);
      int dstPos = offsets_[srcLid]++;
      nbr_t* ptr = edge_builder_.MutablePointer(dstPos);
      ptr->vid = dstLids->Value(i);
      ptr->eid = static_cast<eid_t>(i);
      LOG(INFO) << "push nbr(src=" << srcLid << ",dstLid=" << dstLids->Value(i)
                << ", after offset:" << offsets_[srcLid];
      edata_builder_.UnsafeAppend(edatas->Value(i));
    }
    edata_builder_.Finish(&edata_array_);
    LOG(INFO) << "Finish adding " << len << "edges";
  }

  void sort() {
    const int64_t* offsets_ptr = offset_array_->raw_values();
    for (VID_T i = 0; i < vnum_; ++i) {
      nbr_t* begin = edge_builder_.MutablePointer(offsets_ptr[i]);
      nbr_t* end = edge_builder_.MutablePointer(offsets_ptr[i + 1]);
      std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
        return lhs.vid < rhs.vid;
      });
    }
    LOG(INFO) << "After sort";
  }

  vid_t vnum_;
  int64_t edges_num_;
  bool outEdges_;

  std::vector<int> degree_;
  vineyard::PodArrayBuilder<nbr_t> edge_builder_;
  std::shared_ptr<offset_array_t> offset_array_;  // for output
  std::vector<int64_t> offsets_;  // used for edge iterate in this
  std::shared_ptr<edata_array_t> edata_array_;
};
}  // namespace gs

#endif
