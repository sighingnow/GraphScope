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

DECLARE_string(ipc_socket);
DECLARE_bool(directed);
DECLARE_string(user_lib_path);
DECLARE_string(app_class);  // graphx_driver_class
DECLARE_string(vprog_path);
DECLARE_string(send_msg_path);
DECLARE_string(merge_msg_path);
DECLARE_string(vdata_path);
DECLARE_string(vd_class);
DECLARE_string(ed_class);
DECLARE_string(msg_class);
DECLARE_string(initial_msg);
DECLARE_int64(vdata_size);
DECLARE_int32(max_iterations);
DECLARE_string(frag_ids);

namespace gs {

// static constexpr const char* IPC_SOCKET = "ipc_socket";
// static constexpr const char* DIRECTED = "directed";
// static constexpr const char* USER_LIB_PATH = "user_lib_path";
// static constexpr const char* MAPPED_SIZE = "mapped_size";
using FragmentType =
    vineyard::ArrowFragment<int64_t, vineyard::property_graph_types::VID_TYPE>;

template <typename FRAG_T>
void Query(grape::CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           const std::string& params_str, const std::string& user_lib_path) {
  using APP_TYPE = JavaPIEProjectedDefaultApp<FRAG_T>;
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

template <typename ProjectedFragmentType>
void CreateAndQuery(std::string params, const std::string& frag_name) {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  boost::property_tree::ptree pt;
  string2ptree(params, pt);

  VLOG(10) << "user_lib_path: " << FLAGS_user_lib_path
           << ", directed: " << FLAGS_directed << ", vdata_path "
           << FLAGS_vdata_path << ", vdata size" << FLAGS_vdata_size;
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(FLAGS_ipc_socket));
  VLOG(1) << "Connected to IPCServer: " << FLAGS_ipc_socket;

  std::vector<std::string> frags_splited;
  boost::split(frags_splited, FLAGS_frag_ids, boost::is_any_of(","));

  CHECK_EQ(frags_splited.size(), comm_spec.worker_num());
  auto fragment_id = std::stoull(frags_splited[comm_spec.worker_id()].c_str(),NULL,10);

  VLOG(10) << "[worker " << comm_spec.worker_id()
           << "] loaded frag id: " << fragment_id;

  std::shared_ptr<ProjectedFragmentType> fragment =
      std::dynamic_pointer_cast<ProjectedFragmentType>(
          client.GetObject(fragment_id));

  pt.put("frag_name", frag_name);

  if (getenv("USER_JAR_PATH")) {
    pt.put("jar_name", getenv("USER_JAR_PATH"));
  } else {
    LOG(ERROR) << "USER_JAR_PATH not set";
    return;
  }

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string new_params = ss.str();

  double t0 = grape::GetCurrentTime();

  for (int i = 0; i < 1; ++i) {
    Query<ProjectedFragmentType>(comm_spec, fragment, new_params,
                                 FLAGS_user_lib_path);
  }
  double t1 = grape::GetCurrentTime();
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "[Total Query time]: " << (t1 - t0);
  }
}  // namespace gs
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_TEST_GRAPHX_RUNNER_H_
