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
#include "test/graphx_runner.h"

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

DEFINE_string(user_class, "", "graphx user app");
DEFINE_string(efile, "", "edge file");
DEFINE_string(vfile, "", "vertex file");
DEFINE_bool(directed, true, "directed or not");
DEFINE_string(user_lib_path, "", "user jni lib");
DEFINE_string(app_class, "com.alibaba.graphscope.app.GraphxAdaptor", "graphx driver class"); //graphx_driver_class

// put all flags in a json str
std::string flags2JsonStr() {
  boost::property_tree::ptree pt;
  if (FLAGS_user_class.empty()){
      LOG(ERROR) << "user class not set";
  }
  pt.put("user_class", FLAGS_user_class);
  if (FLAGS_efile.empty()){
      LOG(ERROR) << "efile not set";
  }
  pt.put("efile", FLAGS_efile);
  if (FLAGS_vfile.empty()){
      LOG(ERROR) << "vfile not set";
  }
  pt.put("vfile", FLAGS_vfile);
  pt.put("directed", FLAGS_directed);
  if (FLAGS_user_lib_path.empty()){
      LOG(ERROR) << "user jni lib not set";
  }
  pt.put("user_lib_path", FLAGS_user_lib_path);
  pt.put("app_class", FLAGS_app_class);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  return std::move(ss.str());
}

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

  google::InitGoogleLogging("graphx-runner");
  google::InstallFailureSignalHandler();

  VLOG(1) << "Finish option parsing";

  std::string params = flags2JsonStr();
  gs::Init(params);
  gs::CreateAndQuery(params);
  gs::Finalize();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
