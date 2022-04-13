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

#ifndef ANALYTICAL_ENGINE_TEST_GRAPHX_RUNNER_H_
#define ANALYTICAL_ENGINE_TEST_GRAPHX_RUNNER_H_

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

namespace gs {

static constexpr const char* IPC_SOCKET = "ipc_socket";
static constexpr const char* EFILE = "efile";
static constexpr const char* VFILE = "vfile";
static constexpr const char* DIRECTED = "directed";
static constexpr const char* USER_LIB_PATH = "user_lib_path";
static constexpr const char* VERTEX_MM_FILE_PREFIX = "vertex_mm_file_prefix";
static constexpr const char* EDGE_MM_FILE_PREFIX = "edge_mm_file_prefix";
static constexpr const char* MAX_PARTITION_ID = "max_partition_id";
static constexpr const char* MAPPED_SIZE = "mapped_size";
using FragmentType =
    vineyard::ArrowFragment<int64_t, vineyard::property_graph_types::VID_TYPE>;
using ProjectedFragmentType =
    ArrowProjectedFragment<int64_t, uint64_t, double, double>;

using FragmentLoaderType =
    ArrowFragmentLoader<int64_t, vineyard::property_graph_types::VID_TYPE>;
using APP_TYPE = JavaPIEProjectedDefaultApp<ProjectedFragmentType>;

void Init(const std::string& params) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers of libgrape-lite initialized.";
  }
}

vineyard::ObjectID LoadFragment(const grape::CommSpec& comm_spec,
                                vineyard::Client& client, bool directed,
                                const std::string& vertex_mm_file_prefix,
                                const std::string& edge_mm_file_prefix,
                                int max_parition_id, int64_t mapped_size) {
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
    ss1 << vertex_mm_file_prefix << "&" << max_parition_id << "&"
        << mapped_size;
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
    ss2 << edge_mm_file_prefix << "&" << max_parition_id << "&" << mapped_size;
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
  return fragment_id;
}

template <typename FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           const std::string& params_str, const std::string& user_lib_path) {
  auto app = std::make_shared<APP_TYPE>();
  auto worker = APP_TYPE::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());
  double t = -grape::GetCurrentTime();
  worker->Query(params_str, user_lib_path);
  t += grape::GetCurrentTime();
  MPI_Barrier(comm_spec.comm());
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Query time cost: " << t;
  }

  std::ofstream unused_stream;
  unused_stream.open("empty");
  worker->Output(unused_stream);
  unused_stream.close();
}

void CreateAndQuery(std::string params) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  boost::property_tree::ptree pt;
  string2ptree(params, pt);

  std::string ipc_socket = pt.get<std::string>(IPC_SOCKET);
  bool directed = pt.get<bool>(DIRECTED);
  std::string user_lib_path = pt.get<std::string>(USER_LIB_PATH);
  std::string vertex_mm_file_prefix =
      pt.get<std::string>(VERTEX_MM_FILE_PREFIX);
  std::string edge_mm_file_prefix = pt.get<std::string>(EDGE_MM_FILE_PREFIX);
  int max_partition_id = pt.get<int>(MAX_PARTITION_ID);
  int64_t mapped_size = pt.get<int64_t>(MAPPED_SIZE);

  VLOG(10) << "user_lib_path: " << user_lib_path << ", directed: " << directed
           << ", max partition id: " << max_partition_id << ", mapped size"
           << mapped_size;
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  VLOG(1) << "Connected to IPCServer: " << ipc_socket;

  double t0 = grape::GetCurrentTime();

  vineyard::ObjectID fragment_id =
      LoadFragment(comm_spec, client, directed, vertex_mm_file_prefix,
                   edge_mm_file_prefix, max_partition_id, mapped_size);
  VLOG(10) << "[worker " << comm_spec.worker_id()
           << "] loaded frag id: " << fragment_id;

  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));
  double t1 = grape::GetCurrentTime();

  VLOG(10) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
           << "v label num: " << fragment->vertex_label_num()
           << "e label num: " << fragment->edge_label_num()
           << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0)
          << "outer vertices: " << fragment->GetOuterVerticesNum(0);

  std::string frag_name =
      "gs::ArrowProjectedFragment<int64_t,uint64_t,double,double>";
  pt.put("frag_name", frag_name);
  pt.put("total_cores", max_partition_id);

  if (getenv("USER_JAR_PATH")) {
    pt.put("jar_name", getenv("USER_JAR_PATH"));
  } else {
    LOG(ERROR) << "USER_JAR_PATH not set";
    return;
  }

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string new_params = ss.str();

  // Project
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, "0", "0", "0", "0");

  double t2 = grape::GetCurrentTime();

  Query<ProjectedFragmentType>(comm_spec, projected_fragment, new_params,
                               user_lib_path);

  double t3 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "[Loading fragment time]: " << (t1 - t0);
    VLOG(1) << "[Projecting fragment time]: " << (t2 - t1);
    VLOG(1) << "[Total Query time]: " << (t3 - t2);
  }
}  // namespace gs
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_TEST_GRAPHX_RUNNER_H_
