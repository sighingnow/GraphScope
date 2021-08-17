#include "run_java_app_preprocess.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  google::InitGoogleLogging("run_pie_preprocess");
  google::InstallFailureSignalHandler();

  grape::Init();

  grape::preprocess(argc, argv);
  // step 0: call main function to write the config
  // step 1: call tianxiao's method to generate code and cpp files
  // step 2: compile above file to user.so, along  with libgrape-lite.so and
  // libgrape-lite-jin.so, we build run_pie_sdk

  grape::Finalize();

  google::ShutdownGoogleLogging();
}
