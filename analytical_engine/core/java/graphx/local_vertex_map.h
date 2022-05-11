
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
#include "grape/graph/adj_list.h"
#include "grape/graph/immutable_csr.h"
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
#include "vineyard/io/io/i_io_adaptor.h"
#include "vineyard/io/io/io_factory.h"

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

    oid2Lid.Construct(meta.GetMemberMeta("oid2Lid"));

    vineyard_array_t array;
    array.Construct(meta.GetMemberMeta("lid2Oid"));
    lid2Oid = array.GetArray();

    CHECK_EQ(oid2Lid.size(), lid2Oid->length());
    vnum = oid2Lid.size();

    oidArray_accessor.Init(lid2Oid);
    LOG(INFO) << "Finish construct accessor: " << oidArray_accessor.GetLength();
  }

  int64_t GetVerticesNum() { return vnum; }

  graphx::MutableTypedArray<oid_t>& GetOidArray() { return oidArray_accessor; }

 private:
  vid_t vnum;
  vineyard::Hashmap<oid_t, vid_t> oid2Lid;
  std::shared_ptr<oid_array_t> lid2Oid;
  graphx::MutableTypedArray<oid_t> oidArray_accessor;

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

  void SetOidArray(const vineyard_oid_array_t& oid_array) {
    this->lid2Oid = oid_array;
  }

  void SetOid2Lid(const vineyard::Hashmap<oid_t, vid_t>& rm) {
    this->oid2Lid = rm;
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);

    VINEYARD_CHECK_OK(this->Build(client));

    auto vertex_map = std::make_shared<LocalVertexMap<oid_t, vid_t>>();
    vertex_map->meta_.SetTypeName(type_name<LocalVertexMap<oid_t, vid_t>>());
    vertex_map->oid2Lid = oid2Lid;

    auto& array = vertex_map->lid2Oid;
    array = lid2Oid.GetArray();

    size_t nbytes = 0;

    vertex_map->meta_.AddMember("lid2Oid", lid2Oid.meta());
    nbytes += lid2Oid.nbytes();
    vertex_map->meta_.AddMember("oid2Lid", oid2Lid.meta());
    nbytes += oid2Lid.nbytes();

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
  vineyard_oid_array_t lid2Oid;
  vineyard::Hashmap<oid_t, vid_t> oid2Lid;
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
      : LocalVertexMapBuilder<oid_t, vid_t>(client){
    inner_oids_builder.Finish(&inner_oids);
    outer_oids_builder.Finish(&outer_oids);
  }

  vineyard::Status Build(vineyard::Client& client) override {
#if defined(WITH_PROFILING)
    auto start_ts = grape::GetCurrentTime();
#endif
    vineyard::HashmapBuilder<oid_t, vid_t> builder(client);
    vid_t lid = static_cast<vid_t>(0);
    {
      auto vnum = inner_oids->length();
      for (int64_t k = 0; k < vnum; ++k) {
        auto oid = inner_oids->GetView(k);
        if (builder.find(oid) == builder.end()) {
          builder.emplace(oid, lid++);
        }
      }
      LOG(INFO) << "inner vertices: " << lid;

      vnum = outer_oids->length();
      for (int64_t k = 0; k < vnum; ++k) {
        auto oid = outer_oids->GetView(k);
        if (builder.find(oid) == builder.end()) {
          builder.emplace(oid, lid++);
        }
      }
    }
    LOG(INFO) << "Finish building oid2Lid, distince vertices : "
              << builder.size()
              << " total vertices encoutered: " << lid;
    this->SetOid2Lid(
        *std::dynamic_pointer_cast<vineyard::Hashmap<oid_t, vid_t>>(
            builder.Seal(client)));

    // get distinct oids
    std::shared_ptr<oid_array_t> distinct_oids;
    {
      oid_array_builder_t distinct_oids_builder;
      distinct_oids_builder.Reserve(builder.size());
      CHECK_EQ(inner_oids->length() + outer_oids->length(), builder.size());
      distinct_oids_builder.AppendValues(inner_oids->raw_values(),
                                         inner_oids->length());
      distinct_oids_builder.AppendValues(outer_oids->raw_values(),
                                         outer_oids->length());
      distinct_oids_builder.Finish(&distinct_oids);
      CHECK_EQ(distinct_oids->length(), builder.size());
      LOG(INFO) << "distinct oids: " << distinct_oids->ToString();
    }
    typename vineyard::InternalType<oid_t>::vineyard_builder_type array_builder(
        client, distinct_oids);
    this->SetOidArray(*std::dynamic_pointer_cast<vineyard::NumericArray<oid_t>>(
        array_builder.Seal(client)));
    LOG(INFO) << "Finish setting distinct oids";

#if defined(WITH_PROFILING)
    auto finish_seal_ts = grape::GetCurrentTime();
    LOG(INFO) << "Seal hashmaps uses " << (finish_seal_ts - start_ts)
              << " seconds";
#endif
    return vineyard::Status::OK();
  }

 private:
    std::shared_ptr<oid_array_t> inner_oids, outer_oids;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H
