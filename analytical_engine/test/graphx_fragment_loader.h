/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_TEST_GRAPHX_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_TEST_GRAPHX_FRAGMENT_LOADER_H_

#ifdef ENABLE_JAVA_SDK

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/fragment/immutable_edgecut_fragment.h"
#include "grape/fragment/loader.h"
#include "grape/grape.h"

#include "apps/java_pie/java_pie_projected_default_app.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/io/property_parser.h"
// #include "core/java/utils.h"
#include "core/java/javasdk.h"
#include "core/loader/arrow_fragment_loader.h"

DECLARE_string(ipc_socket);
DECLARE_string(vertex_files);
DECLARE_string(edge_files);
DECLARE_int64(vertex_mapped_size);
DECLARE_int64(edge_mapped_size);
DECLARE_bool(directed);
DECLARE_bool(vd_type);
DECLARE_bool(ed_type);

namespace gs {

using FragmentType =
    vineyard::ArrowFragment<int64_t, vineyard::property_graph_types::VID_TYPE>;
using FragmentLoaderType =
    ArrowFragmentLoader<int64_t, vineyard::property_graph_types::VID_TYPE>;
template <typename ProjectedFragmentT>
vineyard::ObjectID LoadFragment(const grape::CommSpec& comm_spec,
                                vineyard::Client& client, bool directed,
                                const std::string& vertex_mm_files,
                                const std::string& edge_mm_files,
                                int64_t vertex_mapped_size,
                                int64_t edge_mapped_size) {
  vineyard::ObjectID fragment_id;
  {
    auto graph = std::make_shared<gs::detail::Graph>();
    graph->directed = directed;
    graph->generate_eid = false;

    auto vertex = std::make_shared<gs::detail::Vertex>();
    vertex->label = "label1";
    vertex->vid = "0";
    vertex->protocol = "graphx";
    std::stringstream ss1;
    ss1 << vertex_mm_files << "&" << vertex_mapped_size;
    vertex->values = ss1.str();
    graph->vertices.push_back(vertex);

    auto edge = std::make_shared<gs::detail::Edge>();
    edge->label = "label2";
    auto subLabel = std::make_shared<gs::detail::Edge::SubLabel>();
    subLabel->src_label = "label1";
    subLabel->src_vid = "0";
    subLabel->dst_label = "label1";
    subLabel->dst_vid = "0";
    subLabel->protocol = "graphx";
    std::stringstream ss2;
    ss2 << edge_mm_files << "&" << edge_mapped_size;
    subLabel->values = ss2.str();
    // subLabel->values = efile;
    // subLabel->eformat += edge_input_format_class;  // eif
    edge->sub_labels.push_back(*subLabel.get());

    graph->edges.push_back(edge);

    // create arrowFragmentLoader and return
    auto loader =
        std::make_unique<FragmentLoaderType>(client, comm_spec, graph);
    fragment_id = boost::leaf::try_handle_all(
        [&loader]() { return loader->LoadFragment(); },
        [](const vineyard::GSError& e) {
          LOG(FATAL) << e.error_msg;
          return 0;
        },
        [](const boost::leaf::error_info& unmatched) {
          LOG(FATAL) << "Unmatched error " << unmatched;
          return 0;
        });
  }
  VLOG(10) << "[worker " << comm_spec.worker_id()
           << "] loaded frag id: " << fragment_id;

  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

  VLOG(10) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
           << "v label num: " << fragment->vertex_label_num()
           << "e label num: " << fragment->edge_label_num()
           << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0)
          << "outer vertices: " << fragment->GetOuterVerticesNum(0);

  // Project
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, "0", "0", "0", "0");

  VLOG(10) << "Worker[ " << comm_spec.worker_id() << "] "
           << projected_fragment->id();
  return projected_fragment->id();
}

void Run() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }

  VLOG(10) << "vertex files " << FLAGS_vertex_files;
  VLOG(10) << "edge files: " << FLAGS_edge_files;
  VLOG(10) << "vertex mapped size: " << FLAGS_vertex_mapped_size;
  VLOG(10) << "edge mapped size: " << FLAGS_edge_mapped_size;
  VLOG(10) << "directed: " << FLAGS_directed;
  VLOG(10) << "vd: " << FLAGS_vd_type << " ed: " << FLAGS_ed_type;

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(FLAGS_ipc_socket));
  VLOG(1) << "Connected to IPCServer: " << FLAGS_ipc_socket;

  double t0 = grape::GetCurrentTime();
  vineyard::ObjectID projected_frag_id = vineyard::InvalidObjectID();
  if (FLAGS_vd_type == "int64_t" && FLAGS_ed_type == "int64_t") {
    using ProjectedFragmentType =
        ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t>;
    using APP_TYPE = JavaPIEProjectedDefaultApp<ProjectedFragmentType>;

    projected_frag_id = LoadFragment<ProjectedFragmentType>(
        comm_spec, client, FLAGS_directed, FLAGS_vertex_files, FLAGS_edge_files,
        FLAGS_vertex_mapped_size, FLAGS_edge_mapped_size);
  } else if (FLAGS_vd_type == "double" && FLAGS_ed_type == "double") {
    using ProjectedFragmentType =
        ArrowProjectedFragment<int64_t, uint64_t, double, double>;
    using APP_TYPE = JavaPIEProjectedDefaultApp<ProjectedFragmentType>;

    projected_frag_id = LoadFragment<ProjectedFragmentType>(
        comm_spec, client, FLAGS_directed, FLAGS_vertex_files, FLAGS_edge_files,
        FLAGS_vertex_mapped_size, FLAGS_edge_mapped_size);
  } else {
    LOG(ERROR) << "Not supported: " << FLAGS_vd_type
               << ",ed: " << FLAGS_ed_type;
  }

  double t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Load fragment cost:" << (t1 - t0) << "s";
  }
  if (projected_frag_id != vineyard::InvalidObjectID()) {
    grape::Communicator comm;
    comm.InitCommunicator(comm_spec.comm());
    std::vector<vineyard::ObjectID> ids;
    comm.AllGather(projected_frag_id, ids);
    VLOG(1) << "[FragIds]:" << fragIdsToStr(ids);
  }
}

std::string fragidsToStr(std::vector<vineyard::ObjectID>& ids) {
  stringstream ss;
  bool first = true;
  for (auto id : id) {
    if (!first) {
      ss << ","
    }
    ss << id;
    first = false;
  }
  return ss.str();
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_TEST_GRAPHX_FRAGMENT_LOADER_H_
