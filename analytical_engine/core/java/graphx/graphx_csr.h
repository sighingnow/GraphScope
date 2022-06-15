
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

#define WITH_PROFILING
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

#include "grape/grape.h"
#include "grape/graph/immutable_csr.h"
#include "grape/utils/bitset.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/stream/byte_stream.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"
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
  ~GraphXCSR() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXCSR<VID_T, ED_T>>{new GraphXCSR<VID_T, ED_T>()});
  }

  int64_t GetInDegree(vid_t lid) {
    return GetIEOffset(lid + 1) - GetIEOffset(lid);
  }

  int64_t GetOutDegree(vid_t lid) {
    return GetOEOffset(lid + 1) - GetOEOffset(lid);
  }
  bool IsIEEmpty(vid_t lid) { return GetIEOffset(lid + 1) == GetIEOffset(lid); }
  bool IsOEEmpty(vid_t lid) { return GetOEOffset(lid + 1) == GetOEOffset(lid); }

  nbr_t* GetIEBegin(VID_T i) { return &in_edge_ptr_[GetIEOffset(i)]; }
  nbr_t* GetOEBegin(VID_T i) { return &out_edge_ptr_[GetOEOffset(i)]; }

  nbr_t* GetIEEnd(VID_T i) { return &in_edge_ptr_[GetIEOffset(i + 1)]; }
  nbr_t* GetOEEnd(VID_T i) { return &out_edge_ptr_[GetOEOffset(i + 1)]; }

  // verticesNum
  vid_t VertexNum() const { return local_vnum_; }

  int64_t GetInEdgesNum() const { return in_edges_num_; }

  int64_t GetOutEdgesNum() const { return out_edges_num_; }

  int64_t GetTotalEdgesNum() const { return in_edges_num_ + out_edges_num_; }

  int64_t GetPartialInEdgesNum(vid_t from, vid_t end) const {  //[from,end)
    CHECK_LT(from, end);
    CHECK_LE(end, local_vnum_);
    return ie_offsets_->Value(static_cast<int64_t>(end)) -
           ie_offsets_->Value(static_cast<int64_t>(from));
  }
  int64_t GetPartialOutEdgesNum(vid_t from, vid_t end) const {  //[from,end)
    CHECK_LT(from, end);
    CHECK_LE(end, local_vnum_);
    return oe_offsets_->Value(static_cast<int64_t>(end)) -
           oe_offsets_->Value(static_cast<int64_t>(from));
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    {
      vineyard_edges_array_t v6d_edges;
      v6d_edges.Construct(meta.GetMemberMeta("in_edges"));
      in_edges_ = v6d_edges.GetArray();
    }
    {
      vineyard_edges_array_t v6d_edges;
      v6d_edges.Construct(meta.GetMemberMeta("out_edges"));
      out_edges_ = v6d_edges.GetArray();
    }

    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("ie_offsets"));
      ie_offsets_ = array.GetArray();
      ie_offsets_accessor_.Init(ie_offsets_);
    }
    {
      vineyard_offset_array_t array;
      array.Construct(meta.GetMemberMeta("oe_offsets"));
      oe_offsets_ = array.GetArray();
      oe_offsets_accessor_.Init(oe_offsets_);
    }
    {
      vineyard_edata_array_t array;
      array.Construct(meta.GetMemberMeta("edatas"));
      edatas_ = array.GetArray();
      edatas_accessor_.Init(edatas_);
    }

    local_vnum_ = ie_offsets_->length() - 1;
    CHECK_EQ(ie_offsets_->length(), oe_offsets_->length());
    CHECK_GT(local_vnum_, 0);
    LOG(INFO) << "In constructing graphx csr, local vnum: " << local_vnum_;
    out_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(out_edges_->GetValue(0)));
    in_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(in_edges_->GetValue(0)));
    in_edges_num_ = GetIEOffset(local_vnum_);
    out_edges_num_ = GetOEOffset(local_vnum_);
    LOG(INFO) << "total in edges: " << in_edges_num_
              << " out edges : " << out_edges_num_;
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
  inline int64_t GetIEOffset(vid_t lid) {
    CHECK_LE(lid, local_vnum_);
    return ie_offsets_->Value(static_cast<int64_t>(lid));
  }
  inline int64_t GetOEOffset(vid_t lid) {
    CHECK_LE(lid, local_vnum_);
    return oe_offsets_->Value(static_cast<int64_t>(lid));
  }
  inline graphx::ImmutableTypedArray<edata_t>& GetEdataArray() {
    return edatas_accessor_;
  }

  inline graphx::ImmutableTypedArray<int64_t>& GetIEOffsetArray() {
    return ie_offsets_accessor_;
  }
  inline graphx::ImmutableTypedArray<int64_t>& GetOEOffsetArray() {
    return oe_offsets_accessor_;
  }

 private:
  vid_t local_vnum_;
  int64_t in_edges_num_, out_edges_num_;
  nbr_t *in_edge_ptr_, *out_edge_ptr_;
  std::shared_ptr<arrow::FixedSizeBinaryArray> in_edges_, out_edges_;
  std::shared_ptr<arrow::Int64Array> ie_offsets_, oe_offsets_;
  std::shared_ptr<edata_array_t> edatas_;
  graphx::ImmutableTypedArray<edata_t> edatas_accessor_;
  graphx::ImmutableTypedArray<int64_t> ie_offsets_accessor_,
      oe_offsets_accessor_;

  template <typename _VID_T, typename _ED_T>
  friend class GraphXCSRBuilder;

  template <typename _VID_T, typename _ED_T, typename _NEW_ED_T>
  friend class GraphXCSRMapper;
};

template <typename VID_T, typename OLD_ED_T, typename NEW_ED_T>
class GraphXCSRMapper {
  using vid_t = VID_T;
  using old_edata_t = OLD_ED_T;
  using new_edata_t = NEW_ED_T;
  using new_edata_array_builder_t =
      typename vineyard::ConvertToArrowType<new_edata_t>::BuilderType;
  using new_edata_array_t =
      typename vineyard::ConvertToArrowType<new_edata_t>::ArrayType;
  using new_vineyard_edata_array_builder_t =
      typename vineyard::InternalType<new_edata_t>::vineyard_builder_type;

 public:
  GraphXCSRMapper() {}
  ~GraphXCSRMapper() {}

  std::shared_ptr<GraphXCSR<vid_t, new_edata_t>> Map(
      GraphXCSR<vid_t, old_edata_t> old_csr,
      new_edata_array_builder_t& arrow_array_builder,
      vineyard::Client& client) {
    std::shared_ptr<new_edata_array_t> arrow_edata_array;
    arrow_array_builder.Finish(&arrow_edata_array);
    new_vineyard_edata_array_builder_t edata_array_builder(client,
                                                           arrow_edata_array);
    auto edata_array =
        *std::dynamic_pointer_cast<vineyard::NumericArray<new_edata_t>>(
            edata_array_builder.Seal(client));
    LOG(INFO) << "Sealed new edata array";
    // 1. create new meta, seal and got new graphx csr.
    vineyard::ObjectID new_graphx_csr_id;
    {
      auto graphx_csr = std::make_shared<GraphXCSR<vid_t, new_edata_t>>();
      graphx_csr->meta_.SetTypeName(type_name<GraphXCSR<vid_t, new_edata_t>>());
      graphx_csr->meta_.AddMember("in_edges",
                                  old_csr.meta().GetMemberMeta("in_edges"));
      graphx_csr->meta_.AddMember("out_edges",
                                  old_csr.meta().GetMemberMeta("out_edges"));

      graphx_csr->meta_.AddMember("ie_offsets",
                                  old_csr.meta().GetMemberMeta("ie_offsets"));
      graphx_csr->meta_.AddMember("oe_offsets",
                                  old_csr.meta().GetMemberMeta("oe_offsets"));
      graphx_csr->meta_.AddMember("edatas", edata_array.meta());
      graphx_csr->meta_.SetNBytes(old_csr.meta().GetNBytes());

      VINEYARD_CHECK_OK(
          client.CreateMetaData(graphx_csr->meta_, graphx_csr->id_));
      new_graphx_csr_id = graphx_csr->id_;
    }
    auto new_graphx_csr =
        std::dynamic_pointer_cast<GraphXCSR<vid_t, new_edata_t>>(
            client.GetObject(new_graphx_csr_id));
    return new_graphx_csr;
  }
};

template <typename VID_T, typename ED_T>
class GraphXCSRBuilder : public vineyard::ObjectBuilder {
  using eid_t = vineyard::property_graph_types::EID_TYPE;
  using vid_t = VID_T;
  using edata_t = ED_T;
  using nbr_t = vineyard::property_graph_utils::NbrUnit<vid_t, eid_t>;

 public:
  explicit GraphXCSRBuilder(vineyard::Client& client) : client_(client) {}

  void SetInEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this->in_edges = edges;
  }
  void SetOutEdges(const vineyard::FixedSizeBinaryArray& edges) {
    this->out_edges = edges;
  }
  void SetIEOffsetArray(const vineyard::NumericArray<int64_t>& array) {
    this->ie_offsets = array;
  }
  void SetOEOffsetArray(const vineyard::NumericArray<int64_t>& array) {
    this->oe_offsets = array;
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
    graphx_csr->ie_offsets_ = ie_offsets.GetArray();
    graphx_csr->ie_offsets_accessor_.Init(graphx_csr->ie_offsets_);
    nBytes += ie_offsets.nbytes();
    graphx_csr->oe_offsets_ = oe_offsets.GetArray();
    graphx_csr->oe_offsets_accessor_.Init(graphx_csr->oe_offsets_);
    nBytes += oe_offsets.nbytes();
    graphx_csr->in_edges_ = in_edges.GetArray();
    nBytes += in_edges.nbytes();
    graphx_csr->out_edges_ = out_edges.GetArray();
    nBytes += out_edges.nbytes();
    graphx_csr->edatas_ = edatas.GetArray();
    graphx_csr->edatas_accessor_.Init(graphx_csr->edatas_);
    nBytes += edatas.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;

    graphx_csr->in_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(graphx_csr->in_edges_->GetValue(0)));
    graphx_csr->out_edge_ptr_ = const_cast<nbr_t*>(
        reinterpret_cast<const nbr_t*>(graphx_csr->out_edges_->GetValue(0)));
    graphx_csr->local_vnum_ = graphx_csr->ie_offsets_->length() - 1;
    graphx_csr->in_edges_num_ =
        graphx_csr->GetIEOffset(graphx_csr->local_vnum_);
    graphx_csr->out_edges_num_ =
        graphx_csr->GetOEOffset(graphx_csr->local_vnum_);

    graphx_csr->meta_.AddMember("in_edges", in_edges.meta());
    graphx_csr->meta_.AddMember("out_edges", out_edges.meta());

    graphx_csr->meta_.AddMember("ie_offsets", ie_offsets.meta());
    graphx_csr->meta_.AddMember("oe_offsets", oe_offsets.meta());
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
  vineyard::FixedSizeBinaryArray in_edges, out_edges;
  vineyard::NumericArray<int64_t> ie_offsets, oe_offsets;
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
  explicit BasicGraphXCSRBuilder(vineyard::Client& client)
      : GraphXCSRBuilder<vid_t, edata_t>(client) {}

  boost::leaf::result<void> LoadEdges(
      oid_array_builder_t& srcOidsBuilder, oid_array_builder_t& dstOidsBuilder,
      edata_array_builder_t& edatasBuilder,
      GraphXVertexMap<oid_t, vid_t>& graphx_vertex_map) {
    LOG(INFO) << "start loading edges";
    std::shared_ptr<oid_array_t> srcOids, dstOids;
    // std::shared_ptr<edata_array_t> edatas;
    {
      ARROW_OK_OR_RAISE(srcOidsBuilder.Finish(&srcOids));
      ARROW_OK_OR_RAISE(dstOidsBuilder.Finish(&dstOids));
      ARROW_OK_OR_RAISE(edatasBuilder.Finish(&edata_array_));
    }
    CHECK_EQ(srcOids->length(), dstOids->length());
    CHECK_EQ(dstOids->length(), edata_array_->length());
    vnum_ = graphx_vertex_map.GetInnerVertexSize();
    // LOG(INFO) << "fr vnum : " << vnum_;
    auto edges_num_ = srcOids->length();
    const oid_t* src_oid_ptr = srcOids->raw_values();
    const oid_t* dst_oid_ptr = dstOids->raw_values();
    // std::shared_ptr<vid_array_t> srcLids, dstLids;
    std::vector<vid_t> srcLids, dstLids;
    srcLids.resize(edges_num_);
    dstLids.resize(edges_num_);
    auto curFid = graphx_vertex_map.fid();
    LOG(INFO) << "fid: " << curFid;
    {
      int thread_num = 16;
      std::atomic<int> current_chunk(0);
      int64_t chunkSize = 4096;
      int64_t num_chunks = (edges_num_ + chunkSize - 1) / chunkSize;
      LOG(INFO) << "thread num 4, chunk size: " << chunkSize << "num chunks "
                << num_chunks;
      std::vector<std::thread> work_threads(thread_num);
      std::vector<int> cnt(thread_num);
      for (int tid = 0; tid < thread_num; ++tid) {
        work_threads[tid] = std::thread([&] {
          int got;
          int64_t begin, end;
          while (true) {
            got = current_chunk.fetch_add(1, std::memory_order_relaxed);
            if (got >= num_chunks) {
              break;
            }
            begin = std::min(edges_num_, got * chunkSize);
            end = std::min(edges_num_, begin + chunkSize);
            for (auto cur = begin; cur < end; ++cur) {
              auto src_lid = graphx_vertex_map.GetLid(src_oid_ptr[cur]);
              srcLids[cur] = src_lid;
            }
            for (auto cur = begin; cur < end; ++cur) {
              auto dst_lid = graphx_vertex_map.GetLid(dst_oid_ptr[cur]);
              dstLids[cur] = dst_lid;
            }
            cnt[tid] += (end - begin);
          }
        });
      }
      for (auto& thrd : work_threads) {
        thrd.join();
      }
      for (auto i = 0; i < thread_num; ++i) {
        LOG(INFO) << "Thread " << i << "processed: " << cnt[i] << "vertices";
      }
    }
    LOG(INFO) << "Finish building lid array";

    ie_degree_.clear();
    oe_degree_.clear();
    ie_degree_.resize(vnum_, 0);
    oe_degree_.resize(vnum_, 0);

    LOG(INFO) << "Loading edges size " << edges_num_
              << "vertices num: " << vnum_;
    grape::Bitset in_edge_active, out_edge_active;
    in_edge_active.init(edges_num_);
    out_edge_active.init(edges_num_);
    for (auto i = 0; i < edges_num_; ++i) {
      auto src_lid = srcLids[i];
      if (src_lid < vnum_) {
        ++oe_degree_[src_lid];
        out_edge_active.set_bit(i);
      }
    }
    for (auto i = 0; i < edges_num_; ++i) {
      auto dst_lid = dstLids[i];
      if (dst_lid < vnum_) {
        ++ie_degree_[dst_lid];
        in_edge_active.set_bit(i);
      }
    }

    build_offsets();
    LOG(INFO) << "finish offset building";
    add_edges(srcLids, dstLids, in_edge_active, out_edge_active);
    sort();
    LOG(INFO) << "Finish loading edges";
    return {};
  }

  vineyard::Status Build(vineyard::Client& client) override {
    {
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      CHECK(in_edge_builder_.Finish(&edges).ok());

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetInEdges(*res);
      LOG(INFO) << "Finish set edges";
    }
    {
      std::shared_ptr<arrow::FixedSizeBinaryArray> edges;
      CHECK(out_edge_builder_.Finish(&edges).ok());

      vineyard::FixedSizeBinaryArrayBuilder edge_builder_v6d(client, edges);
      auto res = std::dynamic_pointer_cast<vineyard::FixedSizeBinaryArray>(
          edge_builder_v6d.Seal(client));
      this->SetOutEdges(*res);
      LOG(INFO) << "Finish set edges";
    }

    CHECK_EQ(ie_offset_array_->length(), vnum_ + 1);
    CHECK_EQ(oe_offset_array_->length(), vnum_ + 1);
    {
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           ie_offset_array_);
      this->SetIEOffsetArray(
          *std::dynamic_pointer_cast<vineyard::NumericArray<int64_t>>(
              offset_array_builder.Seal(client)));
      LOG(INFO) << "FINISh set offset array";
    }
    {
      vineyard_offset_array_builder_t offset_array_builder(client,
                                                           oe_offset_array_);
      this->SetOEOffsetArray(
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
  boost::leaf::result<void> build_offsets() {
    in_edges_num_ = 0;
    for (auto d : ie_degree_) {
      in_edges_num_ += d;
    }
    ARROW_OK_OR_RAISE(in_edge_builder_.Resize(in_edges_num_));

    out_edges_num_ = 0;
    for (auto d : oe_degree_) {
      out_edges_num_ += d;
    }
    ARROW_OK_OR_RAISE(out_edge_builder_.Resize(out_edges_num_));

    {
      ie_offsets_.resize(vnum_ + 1);
      ie_offsets_[0] = 0;
      for (VID_T i = 0; i < vnum_; ++i) {
        ie_offsets_[i + 1] = ie_offsets_[i] + ie_degree_[i];
      }
      CHECK_EQ(ie_offsets_[vnum_], in_edges_num_);
      offset_array_builder_t builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(ie_offsets_));
      ARROW_OK_OR_RAISE(builder.Finish(&ie_offset_array_));
    }
    {
      oe_offsets_.resize(vnum_ + 1);
      oe_offsets_[0] = 0;
      for (VID_T i = 0; i < vnum_; ++i) {
        oe_offsets_[i + 1] = oe_offsets_[i] + oe_degree_[i];
      }
      CHECK_EQ(oe_offsets_[vnum_], out_edges_num_);
      offset_array_builder_t builder;
      ARROW_OK_OR_RAISE(builder.AppendValues(oe_offsets_));
      ARROW_OK_OR_RAISE(builder.Finish(&oe_offset_array_));
    }

    // We copy to offset_array since later we will modify inplace in <offset>
    {
      std::vector<int> tmp;
      tmp.swap(ie_degree_);
    }
    {
      std::vector<int> tmp;
      tmp.swap(oe_degree_);
    }
    return {};
  }

  void add_edges(const std::vector<vid_t>& src_accessor,
                 const std::vector<vid_t>& dst_accessor,
                 const grape::Bitset& in_edge_active,
                 const grape::Bitset& out_edge_active) {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    // edata_array_builder_t edata_builder_;
    auto len = src_accessor.size();
    // edata_builder_.Reserve(len);
    // const edata_t* edata_accessor = edatas->raw_values();

    // for (auto i = 0; i < len; ++i) {
    // edata_builder_.UnsafeAppend(edata_accessor[i]);
    // }
    // edata_builder_.Finish(&edata_array_);

    // const vid_t* src_accessor = srcLids->raw_values();
    // const vid_t* dst_accessor = dstLids->raw_values();
    nbr_t* ie_mutable_ptr_begin = in_edge_builder_.MutablePointer(0);
    nbr_t* oe_mutable_ptr_begin = out_edge_builder_.MutablePointer(0);
    {
      // int thread_num = 16;
      // std::atomic<int> current_chunk(0);
      // int64_t chunkSize = 4096;
      // int64_t num_chunks = (len + chunkSize - 1) / chunkSize;
      // LOG(INFO) << "thread num 4, chunk size: " << chunkSize << "num chunks "
      //           << num_chunks;
      std::vector<std::thread> work_threads(2);
      // std::vector<int> cnt(thread_num);
      work_threads[0] = std::thread([&] {
        for (size_t i = 0; i < len; ++i) {
          vid_t srcLid = src_accessor[i];
          vid_t dstLid = dst_accessor[i];
          if (out_edge_active.get_bit(i)) {
            int dstPos = oe_offsets_[srcLid]++;
            nbr_t* ptr = oe_mutable_ptr_begin + dstPos;
            ptr->vid = dstLid;
            ptr->eid = static_cast<eid_t>(i);
          }
        }
      });
      work_threads[1] = std::thread([&] {
        for (size_t i = 0; i < len; ++i) {
          vid_t srcLid = src_accessor[i];
          vid_t dstLid = dst_accessor[i];
          if (in_edge_active.get_bit(i)) {
            int dstPos = ie_offsets_[dstLid]++;
            nbr_t* ptr = ie_mutable_ptr_begin + dstPos;
            ptr->vid = srcLid;
            ptr->eid = static_cast<eid_t>(i);
          }
        }
      });
      for (auto& thrd : work_threads) {
        thrd.join();
      }
    }

    LOG(INFO) << "Finish adding " << len << "edges";

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "adding edges cost" << (finish_seal_ts - start_ts)
              << " seconds";
#endif
  }

  void sort() {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    {
      const int64_t* offsets_ptr = ie_offset_array_->raw_values();
      for (VID_T i = 0; i < vnum_; ++i) {
        nbr_t* begin = in_edge_builder_.MutablePointer(offsets_ptr[i]);
        nbr_t* end = in_edge_builder_.MutablePointer(offsets_ptr[i + 1]);
        std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
          return lhs.vid < rhs.vid;
        });
      }
    }
    {
      const int64_t* offsets_ptr = oe_offset_array_->raw_values();
      for (VID_T i = 0; i < vnum_; ++i) {
        nbr_t* begin = out_edge_builder_.MutablePointer(offsets_ptr[i]);
        nbr_t* end = out_edge_builder_.MutablePointer(offsets_ptr[i + 1]);
        std::sort(begin, end, [](const nbr_t& lhs, const nbr_t& rhs) {
          return lhs.vid < rhs.vid;
        });
      }
    }
#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Sort edges cost" << (finish_seal_ts - start_ts) << " seconds";
#endif
  }

  vid_t vnum_;
  int64_t in_edges_num_, out_edges_num_;

  std::vector<int> ie_degree_, oe_degree_;
  vineyard::PodArrayBuilder<nbr_t> in_edge_builder_, out_edge_builder_;
  std::shared_ptr<offset_array_t> ie_offset_array_,
      oe_offset_array_;  // for output
  std::vector<int64_t> ie_offsets_,
      oe_offsets_;  // used for edge iterate in this
  std::shared_ptr<edata_array_t> edata_array_;
};
}  // namespace gs

#endif
