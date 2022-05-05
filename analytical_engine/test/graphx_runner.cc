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
DEFINE_string(vprog_path, "/tmp/graphx-vprog",
              "path to the serialization file for vprog");
DEFINE_string(send_msg_path, "/tmp/graphx-sendMsg",
              "path to the serialization file for sendMsg");
DEFINE_string(merge_msg_path, "/tmp/graphx-mergeMsg",
              "path to the serialization file for Merge msg");
DEFINE_string(vdata_path, "/tmp/graphx-vdata",
              "path to serialization for vdata array");
DEFINE_string(vd_class, "", "int64_t,int32_t,double");
DEFINE_string(ed_class, "", "int64_t,int32_t,double");
DEFINE_string(msg_class, "", "int64_t,int32_t,double");
DEFINE_string(initial_msg, "", "the initial msg");
DEFINE_int64(vdata_size, 10 * 1024 * 1024,
             "mapped size fo vdata shared memroy");
DEFINE_int32(max_iterations, 100000, "max iterations");
DEFINE_string(frag_ids, "", "frag ids got, should be in order");

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
  pt.put("context_class", build_generic_class(FLAGS_context_class, FLAGS_vd_class,
                                      FLAGS_ed_class, FLAGS_msg_class));
  pt.put("msg_class", FLAGS_msg_class);
  pt.put("vd_class", FLAGS_vd_class);
  pt.put("ed_class", FLAGS_ed_class);
  pt.put("initial_msg", FLAGS_initial_msg);
  pt.put("max_iterations", FLAGS_max_iterations);
  pt.put("vprog_path", FLAGS_vprog_path);
  pt.put("send_msg_path", FLAGS_send_msg_path);
  pt.put("merge_msg_path", FLAGS_merge_msg_path);
  pt.put("vdata_path", FLAGS_vdata_path);
  pt.put("vdata_size", FLAGS_vdata_size);

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

  std::string params = flags2JsonStr();
  VLOG(1) << "Finish option parsing" << params;
//  if (std::strcmp(FLAGS_vd_class.c_str(), "int64_t") == 0 &&
//      std::strcmp(FLAGS_ed_class.c_str(), "int64_t") == 0) {
  if (true){
    using ProjectedFragmentType =
        gs::ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t>;
    // using APP_TYPE = JavaPIEProjectedDefaultApp<ProjectedFragmentType>;
    std::string frag_name =
        "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>";

    LOG(INFO) << "Running for int64_t, int64_t";
    gs::CreateAndQuery<ProjectedFragmentType>(params, frag_name);
    gs::Finalize();
  } else if (std::strcmp(FLAGS_vd_class.c_str(), "double") == 0 &&
             std::strcmp(FLAGS_ed_class.c_str(), "double") == 0) {
    using ProjectedFragmentType =
        gs::ArrowProjectedFragment<int64_t, uint64_t, double, double>;
    std::string frag_name =
        "gs::ArrowProjectedFragment<int64_t,uint64_t,double,double>";
    gs::CreateAndQuery<ProjectedFragmentType>(params, frag_name);
    gs::Finalize();
  } else {
    LOG(ERROR) << "Unrecognized: " << FLAGS_vd_class << ", " << FLAGS_ed_class;
  }

  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
}
