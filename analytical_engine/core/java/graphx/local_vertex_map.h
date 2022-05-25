
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_LOCAL_VERTEX_MAP_H
#define ANALYTICAL_ENGINE_CORE_JAVA_LOCAL_VERTEX_MAP_H

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

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/type_alias.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T>
class LocalVertexMap
    : public vineyard::Registered<LocalVertexMap<OID_T, VID_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;

 public:
  LocalVertexMap() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<LocalVertexMap<OID_T, VID_T>>{
            new LocalVertexMap<OID_T, VID_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->ivnum_ = meta.GetKeyValue<fid_t>("ivnum");
    this->ovnum_ = meta.GetKeyValue<fid_t>("ovnum");
    LOG(INFO) << "ivnum: " << ivnum_ << "ovnum: " << ovnum_;
    inner_oid2Lid_.Construct(meta.GetMemberMeta("inner_oid2Lid"));
    // outer_oid2Lid_.Construct(meta.GetMemberMeta("outer_oid2Lid"));

    inner_lid2Oid_.Construct(meta.GetMemberMeta("inner_lid2Oid"));

    outer_lid2Oid_.Construct(meta.GetMemberMeta("outer_lid2Oid"));

    CHECK_EQ(inner_oid2Lid_.size(), inner_lid2Oid_.GetArray()->length());
    // CHECK_EQ(outer_oid2Lid_.size(), outer_lid2Oid_->length());

    // inner_oidArray_accessor.Init(inner_lid2Oid_);
    // inner_oidArray_accessor.Init(outer_lid2Oid_);
    LOG(INFO) << "Finish construct local_vertex_map,  ivnum" << ivnum_;
    // << " ovnum: " << ovnum_;
  }

  // int64_t GetVerticesNum() { return ivnum_ + ovnum_; }
  int64_t GetInnerVerticesNum() { return ivnum_; }
  // int64_t GetOuterVerticesNum() { return ovnum_; }

  // graphx::ImmutableTypedArray<oid_t>& GetInnerOidArray() {
  //   return inner_oidArray_accessor;
  // }
  // graphx::ImmutableTypedArray<oid_t>& GetOuterOidArray() {
  //   return outer_oidArray_accessor;
  // }

  // std::shared_ptr<oid_array_t> GetInnerLid2Oid() {
  //   return inner_lid2Oid_.GetArray();
  // }
  vineyard_oid_array_t& GetInnerLid2Oid() { return inner_lid2Oid_; }

  vineyard_oid_array_t& GetOuterLid2Oid() { return outer_lid2Oid_; }
  vineyard::Hashmap<oid_t, vid_t>& GetInnerOid2Lid() { return inner_oid2Lid_; }

 private:
  vid_t ivnum_, ovnum_;
  vineyard::Hashmap<oid_t, vid_t> inner_oid2Lid_;
  // outer_oid2Lid_;
  // std::shared_ptr<oid_array_t> inner_lid2Oid_, outer_lid2Oid_;
  vineyard_oid_array_t inner_lid2Oid_, outer_lid2Oid_;
  // graphx::ImmutableTypedArray<oid_t> inner_oidArray_accessor;
  // graphx::ImmutableTypedArray<oid_t> outer_oidArray_accessor;

  template <typename _OID_T, typename _VID_T>
  friend class LocalVertexMapBuilder;
};

template <typename OID_T, typename VID_T>
class LocalVertexMapBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vineyard_oid_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;

 public:
  explicit LocalVertexMapBuilder(vineyard::Client& client) : client_(client){};

  void SetInnerOidArray(const vineyard_oid_array_t& oid_array) {
    this->inner_lid2Oid_ = oid_array;
  }
  void SetOuterOidArray(const vineyard_oid_array_t& oid_array) {
    this->outer_lid2Oid_ = oid_array;
  }

  void SetInnerOid2Lid(const vineyard::Hashmap<oid_t, vid_t>& rm) {
    this->inner_oid2Lid_ = rm;
  }
  // void SetOuterOid2Lid(const vineyard::Hashmap<oid_t, vid_t>& rm) {
  //   this->outer_oid2Lid_ = rm;
  // }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);

    VINEYARD_CHECK_OK(this->Build(client));

    auto vertex_map = std::make_shared<LocalVertexMap<oid_t, vid_t>>();
    vertex_map->meta_.SetTypeName(type_name<LocalVertexMap<oid_t, vid_t>>());
    vertex_map->inner_oid2Lid_ = inner_oid2Lid_;
    // vertex_map->outer_oid2Lid_ = outer_oid2Lid_;

    {
      vertex_map->inner_lid2Oid_ = inner_lid2Oid_;
      vertex_map->ivnum_ = inner_lid2Oid_.GetArray()->length();
      vertex_map->meta_.AddKeyValue("ivnum", vertex_map->ivnum_);

      vertex_map->outer_lid2Oid_ = outer_lid2Oid_;
      vertex_map->ovnum_ = outer_lid2Oid_.GetArray()->length();
      vertex_map->meta_.AddKeyValue("ovnum", vertex_map->ovnum_);
    }
    size_t nbytes = 0;

    vertex_map->meta_.AddMember("inner_lid2Oid", inner_lid2Oid_.meta());
    nbytes += inner_lid2Oid_.nbytes();
    vertex_map->meta_.AddMember("outer_lid2Oid", outer_lid2Oid_.meta());
    nbytes += outer_lid2Oid_.nbytes();
    vertex_map->meta_.AddMember("inner_oid2Lid", inner_oid2Lid_.meta());
    nbytes += inner_oid2Lid_.nbytes();
    //    vertex_map->meta_.AddMember("outer_oid2Lid", outer_oid2Lid_.meta());
    //    nbytes += outer_oid2Lid_.nbytes();

    LOG(INFO) << "total bytes: " << nbytes;
    vertex_map->meta_.SetNBytes(nbytes);

    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_map->meta_, vertex_map->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(vertex_map);
  }

 private:
  vineyard::Client& client_;
  vineyard_oid_array_t inner_lid2Oid_, outer_lid2Oid_;
  vineyard::Hashmap<oid_t, vid_t> inner_oid2Lid_;
  //  outer_oid2Lid_;
};

template <typename OID_T, typename VID_T>
class BasicLocalVertexMapBuilder : public LocalVertexMapBuilder<OID_T, VID_T> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;

 public:
  BasicLocalVertexMapBuilder(vineyard::Client& client,
                             oid_array_builder_t& inner_oids_builder,
                             oid_array_builder_t& outer_oids_builder)
      : LocalVertexMapBuilder<oid_t, vid_t>(client) {
    ARROW_OK_OR_RAISE(inner_oids_builder.Finish(&inner_oids));
    ARROW_OK_OR_RAISE(outer_oids_builder.Finish(&outer_oids));
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    vid_t lid = static_cast<vid_t>(0);
    {
      vineyard::HashmapBuilder<oid_t, vid_t> innerBuilder(client);
      auto vnum = inner_oids->length();
      for (int64_t k = 0; k < vnum; ++k) {
        auto oid = inner_oids->GetView(k);
        if (innerBuilder.find(oid) == innerBuilder.end()) {
          innerBuilder.emplace(oid, lid++);
        }
      }
      LOG(INFO) << "inner vertices: " << lid;
      this->SetInnerOid2Lid(
          *std::dynamic_pointer_cast<vineyard::Hashmap<oid_t, vid_t>>(
              innerBuilder.Seal(client)));
    }

    // {
    //   vineyard::HashmapBuilder<oid_t, vid_t> outerBuilder(client);
    //   auto vnum = outer_oids->length();
    //   for (int64_t k = 0; k < vnum; ++k) {
    //     auto oid = outer_oids->GetView(k);
    //     if (outerBuilder.find(oid) == outerBuilder.end()) {
    //       outerBuilder.emplace(oid, lid++);
    //     }
    //   }
    //   LOG(INFO) << "outer vertices: " << vnum;
    //   this->SetOuterOid2Lid(
    //       *std::dynamic_pointer_cast<vineyard::Hashmap<oid_t, vid_t>>(
    //           outerBuilder.Seal(client)));
    // }

    typename vineyard::InternalType<oid_t>::vineyard_builder_type
        inner_array_builder(client, inner_oids);
    this->SetInnerOidArray(
        *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
            inner_array_builder.Seal(client)));
    typename vineyard::InternalType<oid_t>::vineyard_builder_type
        outer_array_builder(client, outer_oids);
    this->SetOuterOidArray(
        *std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
            outer_array_builder.Seal(client)));
    LOG(INFO) << "Finish setting inner and outer oids";

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Seal hashmaps uses " << (finish_seal_ts - start_ts)
              << " seconds";
#endif
    return vineyard::Status::OK();
  }
  std::shared_ptr<LocalVertexMap<oid_t, vid_t>> MySeal(
      vineyard::Client& client) {
    return std::dynamic_pointer_cast<LocalVertexMap<oid_t, vid_t>>(
        this->Seal(client));
  }

 private:
  std::shared_ptr<oid_array_t> inner_oids, outer_oids;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H
