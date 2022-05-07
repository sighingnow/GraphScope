
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
  VertexPartition() : vnums(0) oids(_oids) {}

  void Init(vid_t _vnums, std::shared_ptr<oid_array_t>& _oids,
            std::shared_ptr<vdata_array_t>& _vdatas,
            ska::flat_hash_map<oid_t, vid_t>& _oid2Lid,
            graphx::MutableTypedArray<vdata_t>& _vdatas_accessor,
            std::vector<std::vector<int>>& _lid2pids) {
    vnums = _vnums;
    oids = _oids;
    vdatas = _vdatas;
    oid2Lid = _oid2Lid;
    vdatas_accessor = _vdatas_accessor;
    lid2pids = _lid2pids;
  }

  vid_t VerticesNum() { return vnums; }

  vid_t Oid2Lid(oid_t oid) { return oid2Lid[oid]; }

  oid_t Lid2Oid(vid_t lid) { return oids[lid]; }

  graphx::MutableTypedArray<vdata_t>& GetVdataArray() {
    return vdatas_accessor;
  }

 private:
  std::shared_ptr<oid_array_t> oids;
  std::shared_ptr<vdata_array_t> vdatas;
  ska::flat_hash_map<oid_t, vid_t> oid2Lid;
  vid_t vnums;
  graphx::MutableTypedArray<vdata_t> vdatas_accessor;
  std::vector<std::vector<int>> lid2pids;
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
  VertexPartitionBuilder() {
    vnums = 0;
    defaultVal = static_cast<vdata_t>(0);
  }

  VertexPartitionBuilder(vdata_t defaultValue) {
    vnums = 0;
    defaultVal = defaultValue;
  }

  //   void Init(vid_t numVertices, vdata_t value) {
  //     this->vnums = numVertices;
  //     defaultVal = value;
  //     vdata_builder.Reserve(vnums);
  //     oids_builder.Reserve(vnums);
  //   }

  /**
   * @brief Add vertices which receives from certain partition.
   * @param oids
   * @param pids
   */
  void AddVertex(std::vector<oid_t>& oids, int fromPid) {}

  void Build(VertexPartition<oid_t, vid_t, vdata_t>& partition) {}

 private:
  oid_array_builder_t oids_builder;
  vdata_array_builder_t vdata_builder;
  ska::flat_hash_map<oid_t, vid_t> oid2Lid;
  vid_t vnums;
  vdata_t defaultVal;
  std::vector<std::vector<int>> lid2pids;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_VERTEX_RDD_H