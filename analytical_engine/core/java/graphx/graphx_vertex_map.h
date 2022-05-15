
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
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/type_alias.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T>
class GraphXVertexMap
    : public vineyard::Registered<GraphXVertexMap<OID_T, VID_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vid_array_builder_t =
      typename vineyard::ConvertToArrowType<vid_t>::BuilderType;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  GraphXVertexMap() {}
  ~GraphXVertexMap() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXVertexMap<OID_T, VID_T>>{
            new GraphXVertexMap<OID_T, VID_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->fid_ = meta.GetKeyValue<fid_t>("fid");

    id_parser_.init(fnum_);

    lid2Oids_.resize(fnum_);
    oid2Lids_.resize(fnum_);
    for (fid_t i = 0; i < fnum_; ++i) {
      vineyard_array_t array;
      array.Construct(meta.GetMemberMeta("lid2Oids_" + std::to_string(i)));
      lid2Oids_[i] = array.GetArray();

      oid2Lids_[i].Construct(
          meta.GetMemberMeta("oid2Lids_" + std::to_string(i)));
    }
    {
      vineyard_array_t array;
      array.Construct(meta.GetMemberMeta("outerLid2Oids"));
      outer_lid2Oids_ = array.GetArray();
    }
    InitOuterGids();
    this->ivnum_ = lid2Oids_[fid_]->length();
    this->ovnum_ = outer_lid2Oids_->length();
    this->tvnum_ = this->ivnum_ + this->ovnum_;
    // {
    //   vineyard_array_t array;
    //   array.Construct(meta.GetMemberMeta("outerLid2Gids"));
    //   outer_lid2Gids_ = array.GetArray();
    // }

    LOG(INFO) << "Finish constructing global vertex map, ivnum: " << ivnum_
              << "ovnum: " << ovnum_ << " tvnum: " << tvnum_;

    for (size_t i = 0; i < oid2Lids_.size(); ++i) {
      LOG(INFO) << "oid2Lids_" << i << ", size " << oid2Lids_[i].size();
    }
    for (size_t i = 0; i < lid2Oids_.size(); ++i) {
      LOG(INFO) << "lid2Oids_" << i << ", size " << lid2Oids_[i]->length();
    }
  }
  fid_t fid() { return fid_; }
  fid_t fnum() { return fnum_; }

  inline fid_t GetFragId(vertex_t& v) {
    if (v.GetValue() > ivnum_) {
      auto gid = outer_lid2Gids_->Value(v.GetValue());
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
    Gid2Vertex(gid, v);
    return true;
  }

  bool GetInnerVertex(const oid_t& oid, vertex_t& v) {
    auto iter = lid2Oids_[fid_].find(oid);
    if (iter == lid2Oids_[fid_].end()) {
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
    return outer_lid2Gids_[v.GetValue() - ivnum_];
  }

  OID_T GetInnerVertexId(const vertex_t& v) const {
    assert(v.GetValue() < ivnum_);
    return lid2Oids_[fid_]->Value(v.GetValue());
  }
  OID_T GetOuterVertexId(const vertex_t& v) const {
    assert(v.GetValue() >= ivnum_);
    assert(v.GetValue() < tvnum_);
    return outer_lid2Oids_->Value(v.GetValue() - ivnum_);
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

  inline bool GetOid(const VID_T& gid, OID_T& oid) const {
    fid_t fid = GetFidFromGid(gid);
    VID_T lid = GetLidFromGid(gid);
    return GetOid(fid, lid, oid);
  }

  bool GetOid(fid_t fid, const VID_T& lid, OID_T& oid) const {
    if (lid >= lid2Oids_[fid]->length()) {
      return false;
    }
    oid = lid2Oids_[fid]->Value(lid);
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
      if (iter == outer_gid2Lids_.end()){
          LOG(ERROR) << "worker [" << fid_ << "find no lid for outer gid" << gid;
          return -1;
      }
      return iter->second;
    }
  }

  OID_T InnerVertexLid2Oid(const VID_T& lid) const {
    CHECK_LT(lid, ivnum_);
    return lid2Oids_[fid_]->Value(lid);
  }
  OID_T OuterVertexLid2Oid(const VID_T& lid) const {
    CHECK_GE(lid, ivnum_);
    CHECK_LT(lid, tvnum_);
    return outer_lid2Oids_->Value(lid - ivnum_);
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

  bool GetGid(const OID_T& oid, VID_T& gid) const {
    fid_t fid = static_cast<fid_t>(0);
    while (fid < fnum_ && !GetGid(fid, oid, gid)) {
      fid++;
    }
    if (fid == fnum_) {
      return false;
    }
    return true;
  }

  fid_t GetFidFromGid(const VID_T& gid) const {
    return id_parser_.get_fragment_id(gid);
  }
  VID_T Lid2Gid(fid_t fid, const VID_T& lid) const {
    return id_parser_.generate_global_id(fid, lid);
  }

  void InitOuterGids() {
    vid_array_builder_t gid_builder;

    auto ovnum = outer_lid2Oids_->length();
    LOG(INFO) << "ovnum: " << ovnum;
    gid_builder.Reserve(ovnum);
    vid_t gid;
    for (auto i = 0; i < ovnum; ++i) {
      CHECK(GetGid(outer_lid2Oids_->Value(i), gid));
      gid_builder.UnsafeAppend(gid);
      LOG(INFO) << "outer oid: " << outer_lid2Oids_->Value(i)
                << " gid: " << gid;
    }
    gid_builder.Finish(&outer_lid2Gids_);

    vid_t lid = lid2Oids_[fid_]->length();
    for (int64_t i = 0; i < ovnum; ++i) {
      outer_gid2Lids_.emplace(outer_lid2Gids_->Value(i), lid);
      lid++;
    }
  }

 private:
  grape::fid_t fnum_, fid_;
  vid_t ivnum_, ovnum_, tvnum_;
  grape::IdParser<vid_t> id_parser_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> oid2Lids_;
  std::vector<std::shared_ptr<oid_array_t>> lid2Oids_;
  std::shared_ptr<oid_array_t> outer_lid2Oids_;
  std::shared_ptr<vid_array_t> outer_lid2Gids_;
  ska::flat_hash_map<vid_t, vid_t> outer_gid2Lids_;

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

 public:
  explicit GraphXVertexMapBuilder(vineyard::Client& client, grape::fid_t fnum,
                                  grape::fid_t fid)
      : client_(client) {
    lid2Oids_.resize(fnum);
    oid2Lids_.resize(fnum);
    fnum_ = fnum;
    fid_ = fid;
  };

  void SetOidArray(grape::fid_t fid, const vineyard_oid_array_t& oid_arrays) {
    this->lid2Oids_[fid] = oid_arrays;
  }

  void SetOid2Lid(grape::fid_t fid, const vineyard::Hashmap<oid_t, vid_t>& rm) {
    this->oid2Lids_[fid] = rm;
  }

  void SetSelfOuterOidArray(grape::fid_t fid,
                            vineyard_oid_array_t& outer_oid_array) {
    CHECK_EQ(fid, fid_);
    this->outer_oid_array_ = outer_oid_array;
  }
  // void SetSelfOuterGidArray(grape::fid_t fid,
  //                           vineyard_vid_array_t& outer_gid_array) {
  //   CHECK_EQ(fid, fid_);
  //   this->outer_gid_array_ = outer_gid_array;
  // }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto vertex_map = std::make_shared<GraphXVertexMap<oid_t, vid_t>>();
    vertex_map->fnum_ = fnum_;
    vertex_map->fid_ = fid_;
    vertex_map->id_parser_.init(fnum_);

    vertex_map->lid2Oids_.resize(fnum_);
    for (grape::fid_t i = 0; i < fnum_; ++i) {
      auto& array = vertex_map->lid2Oids_[i];
      array = lid2Oids_[i].GetArray();
    }

    vertex_map->oid2Lids_ = oid2Lids_;
    vertex_map->outer_lid2Oids_ = outer_oid_array_.GetArray();
    // Initiate outer gids rather than sealing them
    vertex_map->InitOuterGids();
    // vertex_map->outer_lid2Gids_ = outer_gid_array_.GetArray();
    vertex_map->ivnum_ = vertex_map->lid2Oids_[fid_]->length();
    vertex_map->ovnum_ = vertex_map->outer_lid2Oids_->length();
    vertex_map->tvnum_ = vertex_map->ivnum_ + vertex_map->ovnum_;

    vertex_map->meta_.SetTypeName(type_name<GraphXVertexMap<oid_t, vid_t>>());

    vertex_map->meta_.AddKeyValue("fnum", fnum_);
    vertex_map->meta_.AddKeyValue("fid", fid_);

    size_t nbytes = 0;
    for (grape::fid_t i = 0; i < fnum_; ++i) {
      if (i == fid_) {
        vertex_map->meta_.AddMember("outerLid2Oids", outer_oid_array_.meta());
        nbytes += outer_oid_array_.meta().GetNBytes();
        // vertex_map->meta_.AddMember("outerLid2Gids",
        // outer_gid_array_.meta()); nbytes +=
        // outer_gid_array_.meta().GetNBytes();
      }
      vertex_map->meta_.AddMember("oid2Lids_" + std::to_string(i),
                                  oid2Lids_[i].meta());
      nbytes += oid2Lids_[i].nbytes();

      vertex_map->meta_.AddMember("lid2Oids_" + std::to_string(i),
                                  lid2Oids_[i].meta());
      nbytes += lid2Oids_[i].nbytes();
    }

    vertex_map->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_map->meta_, vertex_map->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(vertex_map);
  }

 private:
  grape::fid_t fnum_, fid_;
  vineyard::Client& client_;
  std::vector<vineyard_oid_array_t> lid2Oids_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> oid2Lids_;
  vineyard_oid_array_t outer_oid_array_;
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

 public:
  BasicGraphXVertexMapBuilder(vineyard::Client& client,
                              grape::CommSpec& comm_spec,
                              vineyard::ObjectID localVertexMapID)
      : GraphXVertexMapBuilder<oid_t, vid_t>(client, comm_spec.worker_num(),
                                             comm_spec.worker_id()),
        comm_spec_(comm_spec) {
    comm_spec_.Dup();
    partial_vmap = std::dynamic_pointer_cast<LocalVertexMap<oid_t, vid_t>>(
        client.GetObject(localVertexMapID));
    LOG(INFO) << "Worer [" << comm_spec.worker_id() << " got partial vmap id "
              << localVertexMapID
              << ", local vnum: " << partial_vmap->GetInnerVerticesNum();
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    std::vector<std::shared_ptr<oid_array_t>> collected_oids;
    std::shared_ptr<oid_array_t> our_oids =
        partial_vmap->GetInnerLid2Oid().GetArray();

    vineyard::FragmentAllGatherArray<oid_t>(comm_spec_, our_oids,
                                            collected_oids);
    CHECK_EQ(collected_oids.size(), comm_spec_.worker_num());
    for (auto i = 0; i < comm_spec_.worker_num(); ++i) {
      auto array = collected_oids[i];
      LOG(INFO) << "Worker [" << comm_spec_.worker_id() << " Receives "
                << array->length() << "from worker: " << i;
    }

    grape::fid_t curFid = comm_spec_.fid();
    int thread_num = comm_spec_.worker_num();
    std::vector<std::thread> threads(thread_num);
    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int fid) {
            grape::fid_t cur_fid = static_cast<grape::fid_t>(fid);
            if (cur_fid == curFid) {
              this->SetOidArray(cur_fid, partial_vmap->GetInnerLid2Oid());
              this->SetSelfOuterOidArray(cur_fid,
                                         partial_vmap->GetOuterLid2Oid());
              this->SetOid2Lid(cur_fid, partial_vmap->GetInnerOid2Lid());
              // builder for outer lid 2 gid
            } else {
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

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Buillding GraphX vertex map cost"
              << (finish_seal_ts - start_ts) << " seconds";
#endif
    return vineyard::Status::OK();
  }

 private:
  grape::CommSpec comm_spec_;
  std::shared_ptr<LocalVertexMap<OID_T, VID_T>> partial_vmap;
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
