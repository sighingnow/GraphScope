#include "run_java_app.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>


int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  google::InitGoogleLogging("run_java_pie_app");
  google::InstallFailureSignalHandler();

  grape::Init();

  grape::Run<int32_t, uint32_t, int, double>(argc, argv);
  // step 0: parse the config

  grape::Finalize();

  google::ShutdownGoogleLogging();
}
