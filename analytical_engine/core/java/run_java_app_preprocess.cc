#include "run_java_app_preprocess.h"

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;

  google::InitGoogleLogging("run_pie_preprocess");
  google::InstallFailureSignalHandler();

  gs::Init();

  gs::preprocess(argc, argv);

  gs::Finalize();

  google::ShutdownGoogleLogging();
}
