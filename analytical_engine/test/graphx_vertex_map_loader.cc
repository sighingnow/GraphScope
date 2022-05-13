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

#include <dlfcn.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <utility>

#include <boost/asio.hpp>
#include <boost/leaf/error.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/config.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"

#include "core/java/graphx/graphx_vertex_map.h"

DEFINE_string(ipc_socket, "/tmp/vineyard.sock", "vineyard socket addr");
DEFINE_string(local_vm_ids, "", "local vm ids");
DEFINE_string(oid_type, "int64_t", "oid type");
DEFINE_string(vid_type, "uint64_t", "vid type");

void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}

std::string getHostName() { return boost::asio::ip::host_name(); }

template <typename OID_T, typename VID_T>
void Load(const std::string local_vm_ids, vineyard::Client& client) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  std::vector<std::string> splited;
  boost::split(splited, local_vm_ids, boost::is_any_of(","));
  CHECK_EQ(splited.size(), comm_spec.worker_num());

  vineyard::ObjectID global_vm_id;
  {
    auto raw = splited[comm_spec.worker_id()];
    if (raw.find(":") != std::string::npos) {
      raw = raw.substr(raw.find(":") + 1, std::string::npos);
      LOG(INFO) << ": found, trimed to " << raw;
    }
    vineyard::ObjectID partial_map = std::stoull(raw);
    LOG(INFO) << "Worker: [" << comm_spec.worker_id()
              << "] local vm: " << partial_map;
    gs::BasicGraphXVertexMapBuilder<int64_t, uint64_t> builder(
        client, comm_spec, partial_map);
    auto graphx_vm =
        std::dynamic_pointer_cast<gs::GraphXVertexMap<int64_t, uint64_t>>(
            builder.Seal(client));

    VINEYARD_CHECK_OK(client.Persist(graphx_vm->id()));
    global_vm_id = graphx_vm->id();
    LOG(INFO) << "Persist csr id: " << graphx_vm->id();
  }
  LOG(INFO) << "GlobalVertexMapID:" << getHostName() << ":" << global_vm_id;
}

void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./graphx_vertex_map_loader [options]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "graphx_vertex_map_loader");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("graphx_vertex_map_loader");
  google::InstallFailureSignalHandler();

  Init();
  vineyard::Client client;
  client.Connect(FLAGS_ipc_socket);
  LOG(INFO) << "Connected to " << FLAGS_ipc_socket;

  if (FLAGS_oid_type == "int64_t" && FLAGS_vid_type == "uint64_t") {
    Load<int64_t, uint64_t>(FLAGS_local_vm_ids, client);
  } else {
    LOG(ERROR) << "Unrecognized " << FLAGS_vid_type << ", " << FLAGS_oid_type;
  }

  Finalize();

  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
