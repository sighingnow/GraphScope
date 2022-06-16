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

#include <map>
#include <memory>

#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

#include "core/config.h"
#include "core/error.h"
#include "core/io/property_parser.h"

#include "core/fragment/arrow_projected_fragment.h"
#include "core/object/fragment_wrapper.h"
#include "core/server/rpc_utils.h"
#include "core/utils/fragment_traits.h"
#include "proto/graphscope/proto/attr_value.pb.h"
#include "proto/graphscope/proto/graph_def.pb.h"

#if !defined(_GRAPH_TYPE)
#error Missing _GRAPH_TYPE
#endif

/**
 * projected_graph_frame.cc serves as a frame to be compiled with
 * ArrowProjectedFragment. LoadGraph function is provided to proceed with
 * corresponding operations. The frame only needs one macro _GRAPH_TYPE to
 * present which specialized ArrowFragment type will be injected into the frame.
 */
extern "C" {

void LoadGraph(
    const grape::CommSpec& comm_spec, vineyard::Client& client,
    const std::string& graph_name, const gs::rpc::GSParams& params,
    gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>>& fragment_wrapper) {
  using oid_t = typename _GRAPH_TYPE::oid_t;
  using vid_t = typename _GRAPH_TYPE::vid_t;

  fragment_wrapper = gs::bl::try_handle_some(
      [&]() -> gs::bl::result<std::shared_ptr<gs::IFragmentWrapper>> {
        BOOST_LEAF_AUTO(from_vineyard_id,
                        params.Get<bool>(gs::rpc::IS_FROM_VINEYARD_ID));

        if (from_vineyard_id) {
          vineyard::ObjectID frag_group_id = vineyard::InvalidObjectID();
          if (params.HasKey(gs::rpc::VINEYARD_ID)) {
            frag_group_id = params.Get<int64_t>(gs::rpc::VINEYARD_ID).value();
          } else {
            RETURN_GS_ERROR(vineyard::ErrorCode::kInvalidValueError,
                            "Missing param: VINEYARD_ID");
          }
          auto fg = std::dynamic_pointer_cast<gs::ArrowProjectedFragmentGroup>(
              client.GetObject(frag_group_id));
          auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
          auto frag_id = fg->Fragments().at(fid);
          auto frag =
              std::static_pointer_cast<_GRAPH_TYPE>(client.GetObject(frag_id));

          BOOST_LEAF_AUTO(new_frag_group_id, gs::ConstructProjectedFragmentGroup(
                                                 client, frag_id, comm_spec));
          gs::rpc::graph::GraphDefPb graph_def;

          graph_def.set_key(graph_name);
          gs::rpc::graph::VineyardInfoPb vy_info;
          if (graph_def.has_extension()) {
            graph_def.extension().UnpackTo(&vy_info);
          }
          vy_info.set_vineyard_id(new_frag_group_id);
          gs::set_graph_def(frag, graph_def);

          auto wrapper = std::make_shared<gs::FragmentWrapper<_GRAPH_TYPE>>(
              graph_name, graph_def, frag);
          return std::dynamic_pointer_cast<gs::IFragmentWrapper>(wrapper);
        } else {
          RETURN_GS_ERROR(
              vineyard::ErrorCode::kInvalidValueError,
              "Only support load a already loaded projected fragment");
        }
      });
}
}
