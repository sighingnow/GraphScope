
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H
#define ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H

#include "core/java/type_alias.h"

namespace gs {
template <typename OID_T, typename VID_T, typename VD_T>
class VertexPartition {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using oid_array_t = typename vineyard::ConvertToArrowType<oid_t>::ArrayType;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;

 public:
  VertexPartition() : vnums(0) {}

  vid_t VerticesNum() { return vnums; }

  vid_t Oid2Lid(oid_t oid) { return oid2Lid[oid]; }

  oid_t Lid2Oid(vid_t lid) { return oids->Value(lid); }

  graphx::MutableTypedArray<vdata_t>& GetVdataArray() {
    return vdatas_accessor;
  }

 private:
  std::shared_ptr<oid_array_t> oids;
  std::shared_ptr<vdata_array_t> vdatas;
  ska::flat_hash_map<oid_t, vid_t> oid2Lid;
  vid_t vnums;
  graphx::MutableTypedArray<vdata_t> vdatas_accessor;
  std::vector<std::vector<int>> lid2Pids;

  template <typename _OID_T, typename _VID_T, typename _VD_T>
  friend class VertexPartitionBuilder;
};

template <typename OID_T, typename VID_T, typename VD_T>
class VertexPartitionBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using oid_array_builder_t =
      typename vineyard::ConvertToArrowType<oid_t>::BuilderType;
  using vdata_array_builder_t =
      typename vineyard::ConvertToArrowType<vdata_t>::BuilderType;

 public:
  VertexPartitionBuilder() {}

  /**
   * @brief Add vertices which receives from certain partition.
   * //FIXME: more efficient
   * @param oids
   * @param pids
   */
  void AddVertex(std::vector<oid_t>& oids, int fromPid) {
    LOG(INFO) << "Add vertices received from partition " << fromPid
              << " size: " << oids.size();
    oids_builder.Reserve(oids.size());
    for (auto oid : oids) {
      // Theoretically, no duplicate ids should appeal
      if (oid2Lid.find(oid) == oid2Lid.end()) {
        oids_builder.UnsafeAppend(oid);
        oid2Lid.emplace(oid, static_cast<vid_t>(oid2Lid.size()));
      }
    }
    lid2Pids.resize(oid2Lid.size());
    for (auto oid : oids) {
      auto lid = oid2Lid[oid];
      lid2Pids[lid].push_back(fromPid);
    }
  }

  void Build(VertexPartition<oid_t, vid_t, vdata_t>& partition,
             vdata_t defaultVal) {
    // get vnums
    LOG(INFO) << "receives totally: " << oid2Lid.size() << " vertices,";
    for (auto i = 0; i < lid2Pids.size(); ++i) {
      LOG(INFO) << "lid " << i << " is reference in " << lid2Pids[i].size()
                << " parts";
    }
    vid_t vnums = oids_builder.length();
    CHECK_EQ(oid2Lid.size(), vnums);

    oids_builder.Finish(&partition.oids);

    vdata_builder.Reserve(vnums);
    for (auto i = 0; i < vnums; ++i) {
      vdata_builder.UnsafeAppend(defaultVal);
    }
    vdata_builder.Finish(&partition.vdatas);

    partition.oid2Lid = std::move(oid2Lid);
    partition.lid2Pids = std::move(lid2Pids);
    partition.vdatas_accessor.Init(partition.vdatas);
    partition.vnums = vnums;
    LOG(INFO) << "Finish constructing vertex partition vertices : "
              << partition.vnums << ", oid2Lid: " << partition.oid2Lid.size()
              << "lid2Pids size: " << partition.lid2Pids.size();
  }

 private:
  oid_array_builder_t oids_builder;
  vdata_array_builder_t vdata_builder;
  ska::flat_hash_map<oid_t, vid_t> oid2Lid;
  std::vector<std::vector<int>> lid2Pids;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H
