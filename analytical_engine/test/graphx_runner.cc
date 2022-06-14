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

DEFINE_string(ipc_socket, "/tmp/vineyard.sock", "vineyard socket addr");
DEFINE_bool(directed, true, "directed or not");
DEFINE_string(user_lib_path, "/opt/graphscope/lib/libgrape-jni.so",
              "user jni lib");
DEFINE_string(app_class, "com.alibaba.graphscope.app.GraphXAdaptor",
              "graphx driver class");  // graphx_driver_class
DEFINE_string(context_class,
              "com.alibaba.graphscope.context.GraphXAdaptorContext",
              "graphx driver context class");  // graphx_driver_class
DEFINE_string(vd_class, "", "int64_t,int32_t,double,std::string");
DEFINE_string(ed_class, "", "int64_t,int32_t,double,std::string");
DEFINE_string(msg_class, "", "int64_t,int32_t,double,std::string");
DEFINE_int32(max_iterations, 100, "max iterations");
DEFINE_string(vm_ids, "", "vertex map ids");
DEFINE_string(csr_ids, "", "csr ids");
DEFINE_string(vdata_ids, "", "vdata ids");
DEFINE_string(serial_path, "", "serial path");

std::string build_generic_class(const std::string& base_class,
                                const std::string& vd_class,
                                const std::string& ed_class,
                                const std::string& msg_class) {
  std::stringstream ss;
  ss << base_class << "<" << vd_class << "," << ed_class << "," << msg_class
     << ">";
  return ss.str();
}
// put all flags in a json str
std::string flags2JsonStr() {
  boost::property_tree::ptree pt;
  pt.put("directed", FLAGS_directed);
  if (FLAGS_user_lib_path.empty()) {
    LOG(ERROR) << "user jni lib not set";
  }
  pt.put("user_lib_path", FLAGS_user_lib_path);
  // Different from other type of apps, we need to specify
  // vd and ed type in app_class for generic class creations
  pt.put("app_class", build_generic_class(FLAGS_app_class, FLAGS_vd_class,
                                          FLAGS_ed_class, FLAGS_msg_class));
  pt.put("context_class",
         build_generic_class(FLAGS_context_class, FLAGS_vd_class,
                             FLAGS_ed_class, FLAGS_msg_class));
  pt.put("msg_class", FLAGS_msg_class);
  pt.put("vd_class", FLAGS_vd_class);
  pt.put("ed_class", FLAGS_ed_class);
  pt.put("max_iterations", FLAGS_max_iterations);
  pt.put("serial_path", FLAGS_serial_path);

  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  return ss.str();
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

  std::string params = flags2JsonStr();
  VLOG(1) << "Finish option parsing" << params;
  //  if (std::strcmp(FLAGS_vd_class.c_str(), "int64_t") == 0 &&
  //      std::strcmp(FLAGS_ed_class.c_str(), "int64_t") == 0) {
  if (std::strcmp(FLAGS_vd_class.c_str(), "int64_t") == 0 &&
      std::strcmp(FLAGS_ed_class.c_str(), "int64_t") == 0) {
    gs::Run<int64_t, uint64_t, int64_t, int64_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "int64_t") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "int32_t") == 0) {
    gs::Run<int64_t, uint64_t, int64_t, int32_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "int64_t") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "double") == 0) {
    gs::Run<int64_t, uint64_t, int64_t, double>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "int32_t") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "int64_t") == 0) {
    gs::Run<int64_t, uint64_t, int32_t, int64_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "int32_t") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "int32_t") == 0) {
    gs::Run<int64_t, uint64_t, int32_t, int32_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "int32_t") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "double") == 0) {
    gs::Run<int64_t, uint64_t, int32_t, double>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "double") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "int64_t") == 0) {
    gs::Run<int64_t, uint64_t, double, int64_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "double") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "int32_t") == 0) {
    gs::Run<int64_t, uint64_t, double, int32_t>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "double") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "double") == 0) {
    gs::Run<int64_t, uint64_t, double, double>(params);
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "std::string") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "double") == 0) {
    gs::Run<int64_t, uint64_t, std::string, double>(params);
  } else {
    LOG(ERROR) << "current not supported: " << FLAGS_vd_class << ", "
               << FLAGS_ed_class;
  }

  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
