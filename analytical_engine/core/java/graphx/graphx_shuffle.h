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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_SHUFFLE_H
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_SHUFFLE_H

#include <string>
#include <vector>

namespace gs {
template <typename OID_T>
class GraphXShuffle : public vineyard::Registered<GraphXShuffle<OID_T>> {
  using oid_t = OID_T;

 public:
  GraphXShuffle() {}
  ~GraphXShuffle() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXCSR<VID_T>>{new GraphXCSR<VID_T>()});
  }

 private:
  std::shared_ptr<arrow::Int64Array> src_oids;
  std::shared_ptr<arrow::Int64Array> dst_oids;
  template <typename _VID_T>
  friend class GraphXShuffleBuilder;
};

template <typename OID_T>
class GraphXShuffleBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vineyard_array_t =
      typename vineyard::InternalType<oid_t>::vineyard_array_type;
  using vineyard_array_builder_t =
      typename vineyard::InternalType<oid_t>::vineyard_builder_type;

 public:
  explicit GraphXShuffleBuilder(std::vector<OID_T> src_oids,
                                std::vector<OID_T> dst_oids,
                                vineyard::Client& client) {
    vineyard_array_builder_t src_builder(client, src_oids);
    vineyard_array_builder_t dst_builder(client, dst_oids);
    src_builder.Seal(client);
  }

  vineyard::Status Build(vineyard::Client& client) override {}

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {}

 private:
  vineyard_array_t src_oid_array, dst_oid_array;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_SHUFFLE_H