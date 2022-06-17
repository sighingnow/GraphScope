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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_MAPPER_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_MAPPER_H_

#include <limits>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/asio.hpp>
#include "arrow/array.h"
#include "boost/lexical_cast.hpp"

#include "grape/fragment/fragment_base.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/common/util/version.h"
#include "vineyard/graph/fragment/property_graph_types.h"

#include "core/fragment/arrow_projected_fragment.h"

namespace gs {

/**
 * @brief Create a new arrowProjectedFragment with new vdata and new edata.
 *
 * @tparam OID_T OID type
 * @tparam VID_T VID type
 */
template <typename OID_T, typename VID_T, typename OLD_VDATA_T,
          typename NEW_VDATA_T, typename OLD_EDATA_T, typename NEW_EDATA_T>
class ArrowProjectedFragmentMapper {
 public:
  using label_id_t = vineyard::property_graph_types::LABEL_ID_TYPE;
  using prop_id_t = vineyard::property_graph_types::PROP_ID_TYPE;
  using vid_t = VID_T;
  using oid_t = OID_T;
  using old_vdata_t = OLD_VDATA_T;
  using new_vdata_t = NEW_VDATA_T;
  using old_edata_t = OLD_EDATA_T;
  using new_edata_t = NEW_EDATA_T;

  using edata_array_builder_t =
      typename vineyard::ConvertToArrowType<new_edata_t>::BuilderType;
  using edata_array_t =
      typename vineyard::ConvertToArrowType<new_edata_t>::ArrayType;
  using vineyard_edata_array_builder_t =
      typename vineyard::InternalType<new_edata_t>::vineyard_builder_type;

  using vdata_array_builder_t =
      typename vineyard::ConvertToArrowType<new_vdata_t>::BuilderType;
  using vdata_array_t =
      typename vineyard::ConvertToArrowType<new_vdata_t>::ArrayType;
  using vineyard_vdata_array_builder_t =
      typename vineyard::InternalType<new_vdata_t>::vineyard_builder_type;

  using old_frag_t =
      ArrowProjectedFragment<oid_t, vid_t, old_vdata_t, old_edata_t>;
  using new_frag_t =
      ArrowProjectedFragment<oid_t, vid_t, new_vdata_t, new_edata_t>;

  ArrowProjectedFragmentMapper() {}
  ~ArrowProjectedFragmentMapper() {}

  std::shared_ptr<new_frag_t> Map(old_frag_t& old_fragment,
                                  vdata_array_builder_t& vdata_array_builder,
                                  edata_array_builder_t& edata_array_builder,
                                  vineyard::Client& client) {
    std::shared_ptr<edata_array_t> arrow_edata_array;
    edata_array_builder.Finish(&arrow_edata_array);
    std::shared_ptr<vdata_array_t> arrow_vdata_array;
    vdata_array_builder.Finish(&arrow_vdata_array);
    vineyard::NumericArray<new_edata_t> edata_array;
    vineyard::NumericArray<new_vdata_t> vdata_array;
    {
      vineyard_edata_array_builder_t builder(client, arrow_edata_array);
      edata_array =
          *std::dynamic_pointer_cast<vineyard::NumericArray<new_edata_t>>(
              builder.Seal(client));
      LOG(INFO) << "Sealed new edata array";
    }
    {
      vineyard_vdata_array_builder_t builder(client, arrow_vdata_array);
      vdata_array =
          *std::dynamic_pointer_cast<vineyard::NumericArray<new_vdata_t>>(
              builder.Seal(client));
      LOG(INFO) << "Sealed new vdata array";
    }

    vineyard::ObjectID new_frag_id;
    const vineyard::ObjectMeta& old_meta = old_fragment.meta();
    auto v_label = old_meta.GetKeyValue<int>("projected_v_label");
    {
      auto new_frag = std::make_shared<new_frag_t>();
      new_frag->meta_.SetTypeName(
          type_name<ArrowProjectedFragment<oid_t, vid_t, new_vdata_t,
                                           new_edata_t>>());
      new_frag->meta_.AddKeyValue("projected_v_label", v_label);
      new_frag->meta_.AddKeyValue(
          "projected_e_label", old_meta.GetKeyValue<int>("projected_e_label"));
      new_frag->meta_.AddKeyValue(
          "projected_v_property",
          old_meta.GetKeyValue<int>("projected_v_property"));
      new_frag->meta_.AddKeyValue(
          "projected_e_property",
          old_meta.GetKeyValue<int>("projected_e_property"));

      new_frag->meta_.AddMember("arrow_fragment",
                                old_meta.GetMemberMeta("arrow_fragment"));
      if (old_fragment.directed()) {
        new_frag->meta_.AddMember("ie_offsets_begin",
                                  old_meta.GetMemberMeta("ie_offsets_begin"));
        new_frag->meta_.AddMember("ie_offsets_end",
                                  old_meta.GetMemberMeta("ie_offsets_end"));
      }

      new_frag->meta_.AddMember("oe_offsets_begin",
                                old_meta.GetMemberMeta("oe_offsets_begin"));
      new_frag->meta_.AddMember("oe_offsets_end",
                                old_meta.GetMemberMeta("oe_offsets_end"));
      new_frag->meta_.AddMember(
          "arrow_projected_vertex_map",
          old_meta.GetMemberMeta("arrow_projected_vertex_map"));

      new_frag->meta_.AddMember("new_vdata_array", vdata_array.meta());
      new_frag->meta_.AddMember("new_edata_array", edata_array.meta());

      VINEYARD_CHECK_OK(client.CreateMetaData(new_frag->meta_, new_frag->id_));
      new_frag_id = new_frag->id_;
    }
    auto new_frag =
        std::dynamic_pointer_cast<new_frag_t>(client.GetObject(new_frag_id));
    return new_frag;
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_PROJECTED_FRAGMENT_MAPPER_H_
