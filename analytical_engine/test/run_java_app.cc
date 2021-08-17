#include "run_java_app.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

namespace grape {
char const* OID_T_str = "std::vector<std::vector<int64_t>";
char const* VID_T_str = "std::vector<std::vector<uint64_t>>";
char const* VDATA_T_str = "std::vector<std::vector<int>>";
char const* EDATA_T_str = "std::vector<std::vector<double>>";
}  // namespace grape

int main(int argc, char* argv[]) {
  using OID_T = int64_t;
  using VID_T = uint64_t;
  using VDATA_T = int;
  using EDATA_T = double;

  FLAGS_stderrthreshold = 0;

  google::InitGoogleLogging("run_java_pie_app");
  google::InstallFailureSignalHandler();

  grape::Init();

  grape::Run<int64_t, uint64_t, int, double>(argc, argv);
  // step 0: parse the config

  grape::Finalize();

  google::ShutdownGoogleLogging();
}
