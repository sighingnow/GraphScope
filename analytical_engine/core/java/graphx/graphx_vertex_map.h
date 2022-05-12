
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

#include "grape/fragment/partitioner.h"
#include "grape/grape.h"
#include "grape/graph/adj_list.h"
#include "grape/graph/immutable_csr.h"
#include "grape/vertex_map/vertex_map_base.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/basic/stream/byte_stream.h"
#include "vineyard/basic/stream/dataframe_stream.h"
#include "vineyard/basic/stream/parallel_stream.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/loader/arrow_fragment_loader.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/graph/utils/table_shuffler.h"
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

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

 public:
  GraphXVertexMap(){}
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
    for (fid_t i = 0; i < fnum_; ++i) {
      vineyard_array_t array;
      array.Construct(meta.GetMemberMeta("lid2Oids_" + std::to_string(i)));
      lid2Oids_[i] = array.GetArray();

      oid2Lids_[i].Construct(
          meta.GetMemberMeta("oid2Lids_" + std::to_string(i)));
    }
    LOG(INFO) << "Finish constructing global vertex map";
    for (auto i = 0; i < oid2Lids_.size(); ++i) {
      LOG(INFO) << "oid2Lids_" << i << ", size " << oid2Lids_[i].size();
    }
    for (auto i = 0; i < lid2Oids_.size(); ++i) {
      LOG(INFO) << "lid2Oids_" << i << ", size " << lid2Oids_[i]->length();
    }
  }

  size_t GetTotalVertexSize() const {
    size_t size = 0;
    for (const auto& v : oid2Lids_) {
      size += v.size();
    }
    return size;
  }

  size_t GetInnerVertexSize(fid_t fid) const { return l2o_[fid]->length(); }

  bool GetOid(const VID_T& gid, OID_T& oid) const {
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

 private:
  grape::fid_t fnum_, fid_;
  grape::IdParser<vid_t> id_parser_;
  std::vector<vineyard::Hashmap<oid_t, vid_t>> oid2Lids_;
  std::vector<std::shared_ptr<oid_array_t>> lid2Oids_;

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

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto vertex_map = std::make_shared<GraphXVertexMap<oid_t, vid_t>>();
    vertex_map->fnum_ = fnum_;
    vertex_map->id_parser_.init(fnum_);

    vertex_map->lid2Oids_.resize(fnum_);
    for (grape::fid_t i = 0; i < fnum_; ++i) {
      auto& array = vertex_map->lid2Oids_[i];
      array = lid2Oids_[i].GetArray();
    }

    vertex_map->oid2Lids_ = oid2Lids_;

    vertex_map->meta_.SetTypeName(type_name<GraphXVertexMap<oid_t, vid_t>>());

    vertex_map->meta_.AddKeyValue("fnum", fnum_);
    vertex_map->meta_.AddKeyValue("fid", fid_);

    size_t nbytes = 0;
    for (grape::fid_t i = 0; i < fnum_; ++i) {
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
    comm_spec.Dup();
    partial_vmap = std::dynamic_pointer_cast<LocalVertexMap<oid_t, vid_t>>(
        client.GetObject(localVertexMapID));
    LOG(INFO) << "Worer [" << comm_spec.worker_id() << " got partial vmap id "
              << localVertexMapID
              << ", local vnum: " << partial_vmap->GetVerticesNum();
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    std::vector<std::shared_ptr<oid_array_t>> collected_oids;
    std::shared_ptr<oid_array_t> our_oids = partial_vmap->GetLid2Oid();

    vineyard::FragmentAllGatherArray<oid_t>(comm_spec_, our_oids, collected_oids);
    CHECK_EQ(collected_oids.size(), comm_spec_.worker_num());
    for (auto i = 0; i < comm_spec_.worker_num(); ++i) {
      auto array = collected_oids[i];
      LOG(INFO) << "Worker [" << comm_spec_.worker_id() << " Receives "
                << array->length() << "from worker: " << i;
    }

    int thread_num = comm_spec_.worker_num();
    std::vector<std::thread> threads(thread_num);
    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int fid) {
            grape::fid_t cur_fid = static_cast<grape::fid_t>(fid);
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
              typename vineyard::InternalType<oid_t>::vineyard_builder_type array_builder(
                  client, array);
              this->SetOidArray(
                  cur_fid,
                  *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
                      array_builder.Seal(client)));

              this->SetOid2Gid(
                  cur_fid,
                  *std::dynamic_pointer_cast<vineyard::Hashmap<oid_t, vid_t>>(
                      builder.Seal(client)));
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

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_MAP_H
