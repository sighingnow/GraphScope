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
#ifndef ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H
#define ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H

#include "grape/utils/vertex_array.h"
#include "core/fragment/arrow_projected_fragment.h"

namespace gs {
// Type alias for ease of use in Java.
template <typename DATA_T>
using VertexArrayDefault = grape::VertexArray<DATA_T, uint64_t>;

// Type alias for ease of use in Java.
template <typename VID_T, typename EDATA_T>
using NbrDefault =
    Nbr<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;

// Type alias for ease of use in Java.
template <typename VID_T, typename EDATA_T>
using AdjListDefault =
    AdjList<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H