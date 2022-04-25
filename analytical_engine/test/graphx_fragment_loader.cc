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
#include "test/graphx_fragment_loader.h"

#include <dlfcn.h>
#include <unistd.h>
#include <memory>
#include <string>
#include <utility>

#include <boost/leaf/error.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "gflags/gflags.h"
#include "gflags/gflags_declare.h"
#include "glog/logging.h"

#include "grape/config.h"

DEFINE_string(ipc_socket, "/tmp/vineyard.sock", "vineyard socket addr");
DEFINE_string(vertex_files, "", "vertex files concatenated by ;");
DEFINE_string(edge_files, "", "edge files concatenated by ;");
DEFINE_int64(vertex_mapped_size, 512 * 1024 * 1024, "vertex mapped size");
DEFINE_int64(edge_mapped_size, 512 * 1024 * 1024, "edge mapped size");
DEFINE_bool(directed, true, "directed or not");
DEFINE_string(vd_type, "int64_t", "vertex data type");
DEFINE_string(ed_type, "int64_t", "vertex data type");
DEFINE_string(frag_ids_path, "/tmp/graphx-mpi-log", "path for fragids");
// put all flags in a json str

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  grape::gflags::SetUsageMessage(
      "Usage: mpiexec [mpi_opts] ./graphx_runner [options]");
  if (argc == 1) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "graphx-runner");
    exit(1);
  }
  grape::gflags::ParseCommandLineFlags(&argc, &argv, true);
  grape::gflags::ShutDownCommandLineFlags();

  google::InitGoogleLogging("graphx-fragment-loader");
  google::InstallFailureSignalHandler();

  gs::Run();
  gs::Finalize();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
