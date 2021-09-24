#ifndef ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_PREPROCESS_H_
#define ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_PREPROCESS_H_

#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/java/javasdk.h"
#include "grape/grape.h"
#include "grape/util.h"
namespace gs {

void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers are initialized.";
  }
}

jclass getClass(JNIEnv* env, const std::string& main_class_name) {
  char* c_class_name = java_class_name_dash_to_slash(main_class_name);
  jclass main_class = env->FindClass(c_class_name);
  CHECK_NOTNULL(main_class);
  return main_class;
}

void set_codegen_path(std::string code_gen_path, std::string local_file_path) {
  std::ofstream out;
  out.open(local_file_path);
  if (!out) {
    LOG(ERROR) << "open " << local_file_path << " failed";
  }
  out << code_gen_path << std::endl;
  LOG(INFO) << "set code gen path to " << local_file_path;
  return;
}

// arg 0: path of jar
// args 1: output path
// args 2: graph template type
void preprocess(int argc, char** argv) {
  if (argc != 4) {
    LOG(INFO) << "Only 3 args required, jar path,"
                 " output path, and graph_type. Received: "
              << argc;
    return;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double end_to_end = -grape::GetCurrentTime();

  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  SetupEnv(comm_spec.local_num());

  JavaVM* jvm = GetJavaVM(false);
  (void) jvm;

  JNIEnvMark m;

  if (m.env()) {
    jclass system_class = getClass(m.env(), "java/lang/System");
    if (system_class == NULL) {
      LOG(ERROR) << "system class no found";
    }
    jmethodID get_property_method = m.env()->GetStaticMethodID(
        system_class, "getProperty", "(Ljava/lang/String;)Ljava/lang/String;");
    if (get_property_method == NULL) {
      LOG(ERROR) << "get property no found";
    }
    std::string class_path_name = "java.class.path";
    jstring class_path_str = m.env()->NewStringUTF(class_path_name.c_str());
    jstring jjres = (jstring) m.env()->CallStaticObjectMethod(
        system_class, get_property_method, class_path_str);
    std::string res = jstring2string(m.env(), jjres);
    LOG(INFO) << "java.class.path: " << res;

    std::string jar_path = argv[1];
    std::string ffi_output_path = argv[2];
    std::string graphTemplateStr = argv[3];
    LOG(INFO) << "jar path" << jar_path << ", output path: " << ffi_output_path
              << ", grape template str:" << graphTemplateStr;

    // Call grapeAnnotationProcessor here
    // Currently we geneerate graphScope sample and grape sample
    // from the sampe entrence, so for gs, the main class is never used
    // in codegen.
    const char* grape_processor_class_name =
        "com/alibaba/grape/annotation/GrapeAppScanner";
    jclass grape_processor_class =
        m.env()->FindClass(grape_processor_class_name);
    CHECK_NOTNULL(grape_processor_class);
    jmethodID process_method =
        m.env()->GetStaticMethodID(grape_process_class, "scanAppAndGenerate",
                                   "(Ljava/lang/String;Ljava/lang/String;Ljava/"
                                   "lang/String;Z)Ljava/lang/String;");
    CHECK_NOTNULL(process_method);
    jstring jar_path_jstring = m.env()->NewStringUTF(jar_path.c_str());
    jstring ffi_output_path_jstring =
        m.env()->NewStringUTF(ffi_output_path.c_str());
    jstring graphTemplate_jstring =
        m.env()->NewStringUTF(graphTemplateStr.c_str());
    jstring jres = (jstring) m.env()->CallStaticObjectMethod(
        grape_process_class, process_method, jar_path_jstring,
        ffi_output_path_jstring, graphTemplate_jstring, true);
    if (m.env()->ExceptionOccurred()) {
      LOG(ERROR) << "Exception occurred in grape process method";
      m.env()->ExceptionDescribe();
      m.env()->ExceptionClear();
      // env->DeleteLocalRef(main_class);
    }
    LOG(INFO) << "after call static object method";
    std::string file_gen_path = jstring2string(m.env(), jres);
    LOG(INFO) << "generated file wrote to :" << file_gen_path;
    // set the value of code gen path to env for later compilation.
    set_codegen_path(file_gen_path, "codegen_path");
    LOG(INFO) << "Exiting grape processor";

  } else {
    LOG(ERROR) << " java env not available";
  }

  MPI_Barrier(MPI_COMM_WORLD);
  end_to_end += grape::GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "end to end time: " << end_to_end;
  }
}
void Finalize() {
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_PREPROCESS_H_
