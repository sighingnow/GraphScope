
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_MAP_H
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_MAP_H

#define WITH_PROFILING

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
#include "grape/util.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/graph/utils/table_shuffler.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/type_alias.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T>
class GraphXVertexMap
    : public vineyard::Registered<GraphXVertexMap<OID_T, VID_T>> {
 public:
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_vid_array_t =
      typename vineyard::InternalType<vid_t>::vineyard_array_type;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using vertex_t = grape::Vertex<VID_T>;

  GraphXVertexMap() {}
  ~GraphXVertexMap() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXVertexMap<OID_T, VID_T>>{
            new GraphXVertexMap<OID_T, VID_T>()});
  }

  // FIXME: we have to call initOuterGids after construct!
  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->fid_ = meta.GetKeyValue<fid_t>("fid");
    this->graphx_pid_ = meta.GetKeyValue<int>("graphx_pid");

    id_parser_.init(fnum_);

    lid2Oids_.resize(fnum_);
    lid2Oids_accessor_.resize(fnum_);
    oid2Lids_.resize(fnum_);
    for (fid_t i = 0; i < fnum_; ++i) {
      vineyard_array_t array;
      array.Construct(meta.GetMemberMeta("lid2Oids_" + std::to_string(i)));
      lid2Oids_[i] = array.GetArray();
      lid2Oids_accessor_[i] = lid2Oids_[i]->raw_values();

      oid2Lids_[i].Construct(
          meta.GetMemberMeta("oid2Lids_" + std::to_string(i)));
    }
    {
      vineyard::NumericArray<int32_t> array;
      array.Construct(meta.GetMemberMeta("graphx_pids_array"));
      graphx_pids_array_ = array.GetArray();
    }
    {
      vineyard_vid_array_t array;
      array.Construct(meta.GetMemberMeta("outerLid2Gids"));
      outer_lid2Gids_ = array.GetArray();
      outer_lid2Gids_accessor_ = outer_lid2Gids_->raw_values();
    }

    this->ivnum_ = lid2Oids_[fid_]->length();
    this->ovnum_ = outer_lid2Gids_->length();
    this->tvnum_ = this->ivnum_ + this->ovnum_;

    outer_gid2Lids_.Construct(meta.GetMemberMeta("outerGid2Lids"));

    LOG(INFO) << "Finish constructing global vertex map, ivnum: " << ivnum_
              << "ovnum: " << ovnum_ << " tvnum: " << tvnum_;
  }
  fid_t fid() const { return fid_; }
  fid_t fnum() const { return fnum_; }

  int32_t Fid2GraphxPid(fid_t fid) {
    CHECK_LT(fid, fnum_);
    return graphx_pids_array_->Value(fid);
  }

  inline fid_t GetFragId(const vertex_t& v) const {
    if (v.GetValue() >= ivnum_) {
      auto gid = outer_lid2Gids_accessor_[v.GetValue() - ivnum_];
      return id_parser_.get_fragment_id(gid);
    }
    return fid_;
  }

  inline fid_t GetFragId(vid_t lid) const {
    if (lid >= ivnum_) {
      auto gid = outer_lid2Gids_accessor_[lid - ivnum_];
      return id_parser_.get_fragment_id(gid);
    }
    return fid_;
  }

  inline VID_T GetTotalVertexSize() const {
    size_t size = 0;
    for (const auto& v : oid2Lids_) {
      size += v.size();
    }
    return size;
  }

  VID_T GetInnerVertexSize(fid_t fid) const { return oid2Lids_[fid].size(); }

  VID_T GetInnerVertexSize() const { return ivnum_; }

  VID_T GetOuterVertexSize() const { return ovnum_; }

  VID_T GetVertexSize() const { return tvnum_; }

  bool GetVertex(const oid_t& oid, vertex_t& v) {
    vid_t gid;
    if (!GetGid(oid, gid)) {
      LOG(ERROR) << "worker " << fid_ << "Get gid from oid faild: oid" << oid;
      return false;
    }
    return Gid2Vertex(gid, v);
  }

  bool GetInnerVertex(const oid_t& oid, vertex_t& v) {
    auto iter = oid2Lids_[fid_].find(oid);
    if (iter == oid2Lids_[fid_].end()) {
      LOG(ERROR) << "No match for oid " << oid << "found in frag: " << fid_;
      return false;
    }
    v.SetValue(iter->second);
    return true;
  }

  bool GetOuterVertex(const oid_t& oid, vertex_t& v) {
    vid_t gid;
    assert(GetGid(oid, gid));
    auto iter = outer_gid2Lids_.find(gid);
    if (iter == outer_gid2Lids_.end()) {
      LOG(ERROR) << "No outer vertex with oid: " << oid << "found in frag "
                 << fid_;
      return false;
    }
    v.SetValue(iter->second);
    return true;
  }

  bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    return IsInnerVertexGid(gid) ? InnerVertexGid2Vertex(gid, v)
                                 : OuterVertexGid2Vertex(gid, v);
  }
  inline bool IsInnerVertexGid(const VID_T& gid) const {
    return id_parser_.get_fragment_id(gid) == fid();
  }

  inline bool InnerVertexGid2Vertex(const VID_T& gid, vertex_t& v) const {
    v.SetValue(id_parser_.get_local_id(gid));
    return true;
  }

  inline bool OuterVertexGid2Vertex(const VID_T& gid, vertex_t& v) const {
    vid_t lid;
    if (OuterVertexGid2Lid(gid, lid)) {
      v.SetValue(lid);
      return true;
    }
    return false;
  }

  inline bool OuterVertexGid2Lid(const VID_T gid, VID_T& lid) const {
    auto iter = outer_gid2Lids_.find(gid);
    if (iter == outer_gid2Lids_.end()) {
      LOG(ERROR) << "worker [" << fid_ << "find no lid for outer gid" << gid;
      return false;
    }
    lid = iter->second;
    return true;
  }

  inline VID_T Vertex2Gid(const vertex_t& v) {
    return IsInnerVertex(v) ? GetInnerVertexGid(v) : GetOuterVertexGid(v);
  }
  inline VID_T GetInnerVertexGid(const vertex_t& v) const {
    return id_parser_.generate_global_id(fid(), v.GetValue());
  }
  inline VID_T GetOuterVertexGid(const vertex_t& v) const {
    return outer_lid2Gids_->Value(v.GetValue() - ivnum_);
  }
  inline VID_T GetOuterVertexGid(const VID_T& lid) const {
    CHECK_GE(lid, ivnum_);
    return outer_lid2Gids_accessor_[lid - ivnum_];
  }

  inline OID_T GetInnerVertexId(const vertex_t& v) const {
    assert(v.GetValue() < ivnum_);
    // return lid2Oids_[fid_]->Value(v.GetValue());
    return lid2Oids_accessor_[fid_][v.GetValue()];
  }
  inline OID_T GetOuterVertexId(const vertex_t& v) const {
    assert(v.GetValue() >= ivnum_);
    return OuterVertexLid2Oid(v);
  }

  inline bool IsInnerVertex(const vertex_t& v) { return v.GetValue() < ivnum_; }

  inline bool InnerVertexGid2Lid(VID_T gid, VID_T& lid) const {
    lid = id_parser_.get_local_id(gid);
    return true;
  }

  inline VID_T GetInnererVertexGid(const vertex_t& v) {
    assert(v.GetValue() < ivnum_);
    return id_parser_.generate_global_id(fid_, v.GetValue());
  }

  OID_T GetId(const vertex_t& v) const {
    if (v.GetValue() >= ivnum_) {
      return OuterVertexLid2Oid(v.GetValue());
    } else {
      return InnerVertexLid2Oid(v.GetValue());
    }
  }
  OID_T GetId(const vid_t lid) const {
    if (lid >= ivnum_) {
      return OuterVertexLid2Oid(lid);
    } else {
      return InnerVertexLid2Oid(lid);
    }
  }

  inline bool GetOid(const VID_T& gid, OID_T& oid) const {
    fid_t fid = GetFidFromGid(gid);
    VID_T lid = GetLidFromGid(gid);
    return GetOid(fid, lid, oid);
  }

  bool GetOid(fid_t fid, const VID_T& lid, OID_T& oid) const {
    if (lid >= lid2Oids_[fid]->length()) {
      return false;
    }
    oid = lid2Oids_accessor_[fid][lid];
    return true;
  }

  /**
   * @brief For a oid, get the lid in this frag.
   *
   * @param oid
   * @return VID_T
   */
  inline VID_T GetLid(const OID_T& oid) const {
    vid_t gid;
    CHECK(GetGid(oid, gid));
    if (GetFidFromGid(gid) == fid_) {
      return id_parser_.get_local_id(gid);
    } else {
      auto iter = outer_gid2Lids_.find(gid);
      if (iter == outer_gid2Lids_.end()) {
        LOG(ERROR) << "worker [" << fid_ << "find no lid for outer gid" << gid;
        return -1;
      }
      return iter->second;
    }
  }

  inline OID_T InnerVertexLid2Oid(const VID_T& lid) const {
    CHECK_LT(lid, ivnum_);
    return lid2Oids_accessor_[fid_][lid];
  }
  inline OID_T OuterVertexLid2Oid(const VID_T& lid) const {
    auto gid = outer_lid2Gids_accessor_[lid];
    oid_t oid;
    CHECK(GetOid(gid, oid));
    return oid;
  }

  bool GetGid(fid_t fid, const OID_T& oid, VID_T& gid) const {
    auto& rm = oid2Lids_[fid];
    auto iter = rm.find(oid);
    if (iter == rm.end()) {
      return false;
    } else {
      gid = Lid2Gid(fid, iter->second);
      return true;
    }
  }

  VID_T InnerOid2Gid(const OID_T& oid) const {
    VID_T gid;
    CHECK(GetGid(fid_, oid, gid));
    return gid;
  }

  inline bool GetGid(const OID_T& oid, VID_T& gid) const {
    fid_t fid = static_cast<fid_t>(0);
    while (fid < fnum_ && !GetGid(fid, oid, gid)) {
      fid++;
    }
    if (fid == fnum_) {
      return false;
    }
    return true;
  }

  inline fid_t GetFidFromGid(const VID_T& gid) const {
    return id_parser_.get_fragment_id(gid);
  }
  inline VID_T Lid2Gid(fid_t fid, const VID_T& lid) const {
    return id_parser_.generate_global_id(fid, lid);
  }

 private:
  grape::fid_t fnum_, fid_;
  int graphx_pid_;
  vid_t ivnum_, ovnum_, tvnum_;
  grape::IdParser<vid_t> id_parser_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> oid2Lids_;
  std::vector<std::shared_ptr<oid_array_t>> lid2Oids_;
  std::vector<const oid_t*> lid2Oids_accessor_;
  const vid_t* outer_lid2Gids_accessor_;
  std::shared_ptr<vid_array_t> outer_lid2Gids_;
  vineyard::Hashmap<vid_t, vid_t> outer_gid2Lids_;
  std::shared_ptr<arrow::Int32Array> graphx_pids_array_;

  template <typename _OID_T, typename _VID_T>
  friend class GraphXVertexMapBuilder;
};

template <typename OID_T, typename VID_T>
class GraphXVertexMapBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_vid_array_t =
      typename vineyard::InternalType<vid_t>::vineyard_array_type;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  explicit GraphXVertexMapBuilder(vineyard::Client& client, grape::fid_t fnum,
                                  grape::fid_t fid, int graphx_pid,
                                  int32_t local_num,
                                  oid_array_builder_t& outer_oids_builder)
      : client_(client) {
    lid2Oids_.resize(fnum);
    oid2Lids_.resize(fnum);
    fnum_ = fnum;
    fid_ = fid;
    graphx_pid_ = graphx_pid;
    local_num_ = local_num;
    id_parser_.init(fnum);
    CHECK(outer_oids_builder.Finish(&outer_oids_array_).ok());
  };
  void SetGraphXPids(const vineyard::NumericArray<int32_t>& graphx_pids_array) {
    this->graphx_pids_array_ = graphx_pids_array;
  }

  void SetOidArray(grape::fid_t fid, const vineyard_oid_array_t& oid_arrays) {
    this->lid2Oids_[fid] = oid_arrays;
  }

  void SetOid2Lid(grape::fid_t fid, const vineyard::Hashmap<oid_t, vid_t>& rm) {
    this->oid2Lids_[fid] = rm;
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif

    auto vertex_map = std::make_shared<GraphXVertexMap<oid_t, vid_t>>();
    vertex_map->fnum_ = fnum_;
    vertex_map->fid_ = fid_;
    vertex_map->id_parser_.init(fnum_);
    vertex_map->graphx_pid_ = graphx_pid_;
    vertex_map->ivnum_ = lid2Oids_[fid_]->length();
    vertex_map->ovnum_ = outer_oids_array_->length();
    vertex_map->tvnum_ = vertex_map->ivnum_ + vertex_map->ovnum_;

    vertex_map->lid2Oids_.resize(fnum_);
    vertex_map->lid2Oids_accessor_.resize(fnum_);
    for (grape::fid_t i = 0; i < fnum_; ++i) {
      auto& array = vertex_map->lid2Oids_[i];
      array = lid2Oids_[i].GetArray();
      vertex_map->lid2Oids_accessor_[i] = array->raw_values();
    }

    vertex_map->oid2Lids_ = oid2Lids_;
    size_t nbytes = 0;
    // Builder ovg2l and ovgids
    int64_t ovnum = vertex_map->ovnum_;
    {
#if defined(WITH_PROFILING)
      auto time0 = grape::GetCurrentTime();
#endif
      std::atomic<int64_t> current_ind(0);
      int thread_num =
          (std::thread::hardware_concurrency() + local_num_ - 1) / local_num_;
      std::vector<std::thread> threads(thread_num);
      vid_array_builder_t gid_builder;
      gid_builder.resize(ovnum);
      const oid_t* outer_lid2Oids_accessor_ = outer_oids_arrays_->raw_values;
      for (int i = 0; i < thread_num; ++i) {
        threads[i] = std::thread(
            [&](int fid) {
              int64_t begin, end;
              while (true) {
                begin = min(ovnum, current_ind.fetch_add(
                                       4096, std::memory_order_relaxed));
                end = min(begin + 4096, ovnum);
                if (begin >= end) {
                  break;
                }
                vid_t gid;
                for (int64_t j = begin; j < end; ++j) {
                  CHECK(getGid(outer_lid2Oids_accessor_[j], gid));
                  gid_builder[j] = gid;
                }
              }
            },
            i)
      }
      for (int i = 0; i < thread_num; ++i) {
        threads[i].join();
      }
      gid_builder.Finish(&vertex_map->outer_lid2Gids_);
      vertex_map->outer_lid2Gids_accessor_ =
          vertex_map->outer_lid2Gids_->raw_values();
#if defined(WITH_PROFILING)
      auto time1 = grape::GetCurrentTime();
      LOG(INFO) << "Build gid array len: "
                << vertex_map->outer_lid2Gids_->length() << " cost"
                << (time1 - time0) << " seconds";
#endif
    }
    {
      typename vineyard::InternalType<vid_t>::vineyard_builder_type
          array_builder(client, vertex_map->outer_lid2Gids_);
      auto vineyard_gid_array =
          *std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
              array_builder.Seal(client));
      nBytes += vineyard_gid_array.nbytes();
      vertex_map->meta_.AddMember("outerLid2Gids", vineyard_gid_array.meta());
    }
    {
      vineyard::HashmapBuilder<vid_t, vid_t> builder(client);
      auto& gid_accessor = vertex_map->outer_lid2Gids_accessor_ auto ivnum =
          vertex_map->ivnum_;
      for (int i = 0; i < ovnum; ++i) {
        builder.emplace(gid_accessor[i], i + ivnum);
      }
      vertex_map->outer_gid2Lids_ =
          *std::dynamic_pointer_cast<vineyard::Hashmap<vid_t, vid_t>>(
              builder.Seal(client));
      nBytes += vertex_map->outer_gid2Lids_.nbytes();
      vertex_map->meta_.AddMember("outerGid2Lids",
                                  vertex_map->outer_gid2Lids_.meta());
#if defined(WITH_PROFILING)
      auto time2 = grape::GetCurrentTime();
      LOG(INFO) << "building gid2lid cost" << (time2 - time1) << " seconds";
#endif
    }
    vertex_map->graphx_pids_array_ = graphx_pids_array_.GetArray();

    // sealing outer gid mapping

    // vertex_map->outer_lid2Gids_ = outer_gid_array_.GetArray();

    vertex_map->meta_.SetTypeName(type_name<GraphXVertexMap<oid_t, vid_t>>());

    vertex_map->meta_.AddKeyValue("fnum", fnum_);
    vertex_map->meta_.AddKeyValue("fid", fid_);
    vertex_map->meta_.AddKeyValue("graphx_pid", graphx_pid_);

    // nbytes += initOuterGids(vertex_map);

    for (grape::fid_t i = 0; i < fnum_; ++i) {
      vertex_map->meta_.AddMember("oid2Lids_" + std::to_string(i),
                                  oid2Lids_[i].meta());
      nbytes += oid2Lids_[i].nbytes();

      vertex_map->meta_.AddMember("lid2Oids_" + std::to_string(i),
                                  lid2Oids_[i].meta());
      nbytes += lid2Oids_[i].nbytes();
    }
    vertex_map->meta_.AddMember("graphx_pids_array", graphx_pids_array_.meta());
    nbytes += graphx_pids_array_.nbytes();

    vertex_map->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_map->meta_, vertex_map->id_));
    // mark the builder as sealed
    this->set_sealed(true);
#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Sealing GraphX vertex map cost" << (finish_seal_ts - start_ts)
              << " seconds";
#endif

    return std::static_pointer_cast<vineyard::Object>(vertex_map);
  }

 protected:
  int graphx_pid_;

 private:
  // size_t initOuterGids(
  //     std::shared_ptr<GraphXVertexMap<oid_t, vid_t>>& vertex_map) {
  //   vid_array_builder_t gid_builder;
  //   size_t nbytes = 0;
  //   auto ovnum = vertex_map->outer_lid2Oids_->length();
  //   gid_builder.Reserve(ovnum);
  //   vid_t gid;
  //   for (auto i = 0; i < ovnum; ++i) {
  //     CHECK(getGid(vertex_map->outer_lid2Oids_accessor_[i], gid));
  //     gid_builder.UnsafeAppend(gid);
  //   }
  //   gid_builder.Finish(&(vertex_map->outer_lid2Gids_));
  //   vertex_map->outer_lid2Gids_accessor_ =
  //       vertex_map->outer_lid2Gids_->raw_values();
  //   {
  //     // build and seal.
  //     typename vineyard::InternalType<vid_t>::vineyard_builder_type
  //         array_builder(client_, vertex_map->outer_lid2Gids_);
  //     auto vineyard_array =
  //         *std::dynamic_pointer_cast<vineyard::NumericArray<vid_t>>(
  //             array_builder.Seal(client_));
  //     vertex_map->meta_.AddMember("outerLid2Gids", vineyard_array.meta());
  //     nbytes += vineyard_array.meta().GetNBytes();
  //   }

  //   vineyard::HashmapBuilder<vid_t, vid_t> builder(client_);
  //   vid_t lid = vertex_map->lid2Oids_[fid_]->length();
  //   for (int64_t i = 0; i < ovnum; ++i) {
  //     builder.emplace(vertex_map->outer_lid2Gids_->Value(i), lid);
  //     lid++;
  //   }

  //   {
  //     vertex_map->outer_gid2Lids_ =
  //         *std::dynamic_pointer_cast<vineyard::Hashmap<vid_t, vid_t>>(
  //             builder.Seal(client_));
  //     vertex_map->meta_.AddMember("outerGid2Lids",
  //                                 vertex_map->outer_gid2Lids_.meta());
  //     nbytes += vertex_map->outer_gid2Lids_.meta().GetNBytes();
  //   }
  //   return nbytes;
  // }
  inline bool getGid(const oid_t& oid, vid_t& gid) const {
    fid_t fid = static_cast<fid_t>(0);
    while (fid < fnum_ && !getGid(fid, oid, gid)) {
      fid++;
    }
    if (fid == fnum_) {
      return false;
    }
    return true;
  }

  inline bool getGid(fid_t fid, const oid_t& oid, vid_t& gid) const {
    auto& rm = oid2Lids_[fid];
    auto iter = rm.find(oid);
    if (iter == rm.end()) {
      return false;
    } else {
      gid = lid2Gid(fid, iter->second);
      return true;
    }
  }
  inline vid_t lid2Gid(fid_t fid, const vid_t& lid) const {
    return id_parser_.generate_global_id(fid, lid);
  }

  grape::fid_t fnum_, fid_;
  int32_t local_num_;
  grape::IdParser<vid_t> id_parser_;
  vineyard::Client& client_;
  std::vector<vineyard_oid_array_t> lid2Oids_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> oid2Lids_;
  std::shared_ptr<oid_array_t> outer_oids_array_;
  vineyard::NumericArray<int32_t> graphx_pids_array_;
  // vineyard_vid_array_t outer_gid_array_;
};

template <typename OID_T, typename VID_T>
class BasicGraphXVertexMapBuilder
    : public GraphXVertexMapBuilder<OID_T, VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using base_t = GraphXVertexMapBuilder<oid_t, vid_t>;

 public:
  BasicGraphXVertexMapBuilder(vineyard::Client& client,
                              grape::CommSpec& comm_spec, int graphx_pid,
                              oid_array_builder_t& inner_oids_builder,
                              oid_array_builder_t& outer_oids_builder)
      : GraphXVertexMapBuilder<oid_t, vid_t>(client, comm_spec.worker_num(),
                                             comm_spec.worker_id(), graphx_pid,
                                             comm_spec.local_num();
                                             outer_oids_builder),
        comm_spec_(comm_spec) {
    comm_spec_.Dup();
    CHECK(inner_oids_builder.Finish(&inner_oids_).ok());
    LOG(INFO) << "Worer [" << comm_spec.worker_id()
              << ", local vnum: " << inner_oids_->length()
              << ", graphx pid: " << graphx_pid;
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    std::vector<std::shared_ptr<oid_array_t>> collected_oids;

    CHECK(vineyard::FragmentAllGatherArray<oid_t>(comm_spec_, inner_oids_,
                                                  collected_oids)
              .ok());
    CHECK_EQ(collected_oids.size(), comm_spec_.worker_num());
    // for (auto i = 0; i < comm_spec_.worker_num(); ++i) {
    //   auto array = collected_oids[i];
    //   LOG(INFO) << "Worker [" << comm_spec_.worker_id() << " Receives "
    //             << array->length() << "from worker: " << i;
    // }

    grape::fid_t curFid = comm_spec_.fid();
    std::atomic<grape::fid_t> current_fid(0);
    int thread_num =
        (std::thread::hardware_concurrency() + comm_spec_.local_num() - 1) /
        comm_spec_.local_num();
    std::vector<std::thread> threads(thread_num);
    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int fid) {
            grape::fid_t cur_fid;
            while (true) {
              cur_fid = current_fid.fetch_add(1, std::memory_order_relaxed);
              if (cur_fid >= comm_spec_.fnum()) {
                break;
              }
              vineyard::HashmapBuilder<oid_t, vid_t> builder(client);
              auto array = collected_oids[cur_fid];
              {
                vid_t cur_lid = 0;
                int64_t vnum = array->length();
                // builder.reserve(static_cast<size_t>(vnum));
                for (int64_t k = 0; k < vnum; ++k) {
                  builder.emplace(array->GetView(k), cur_lid);
                  ++cur_lid;
                }
              }
              // may be reuse local vm.
              {
                typename vineyard::InternalType<oid_t>::vineyard_builder_type
                    array_builder(client, array);
                this->SetOidArray(
                    cur_fid,
                    *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
                        array_builder.Seal(client)));

                this->SetOid2Lid(
                    cur_fid,
                    *std::dynamic_pointer_cast<vineyard::Hashmap<oid_t, vid_t>>(
                        builder.Seal(client)));
              }
            }
          },
          i);
    }
    for (auto& thrd : threads) {
      thrd.join();
    }

    {
      // gather grape pid <-> graphx pid matching.
      std::vector<int32_t> graphx_pids;
      graphx_pids.resize(comm_spec_.fnum());
      int32_t tmp_graphx_pid = base_t::graphx_pid_;
      MPI_Allgather(&tmp_graphx_pid, 1, MPI_INT, graphx_pids.data(), 1, MPI_INT,
                    comm_spec_.comm());

      arrow::Int32Builder builder;
      CHECK(builder.AppendValues(graphx_pids).ok());
      std::shared_ptr<arrow::Int32Array> graphx_pids_array;
      CHECK(builder.Finish(&graphx_pids_array).ok());
      vineyard::NumericArrayBuilder<int32_t> v6d_graphx_pids_builder(
          client, graphx_pids_array);
      this->SetGraphXPids(
          *std::dynamic_pointer_cast<vineyard::NumericArray<int32_t>>(
              v6d_graphx_pids_builder.Seal(client)));
    }

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Buillding GraphX vertex map cost"
              << (finish_seal_ts - start_ts) << " seconds";
#endif
    return vineyard::Status::OK();
  }

 private:
  grape::CommSpec comm_spec_;
  std::shared_ptr<oid_array_t> inner_oids_;  // outer_oids;
  //   grape::IdParser id_parser_;
};

template <typename OID_T, typename VID_T>
class GraphXVertexMapGetter {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  GraphXVertexMapGetter() {}
  ~GraphXVertexMapGetter() {}
  std::shared_ptr<GraphXVertexMap<oid_t, vid_t>> Get(
      vineyard::Client& client, vineyard::ObjectID globalVMID) {
    auto globalVM = std::dynamic_pointer_cast<GraphXVertexMap<oid_t, vid_t>>(
        client.GetObject(globalVMID));
    LOG(INFO) << "Got global vm: " << globalVMID
              << " total vnum: " << globalVM->GetTotalVertexSize();
    return globalVM;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_MAP_H
