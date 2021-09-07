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

#include "grape/grape.h"
#include "grape/util.h"
#include "java_pie/javasdk.h"
namespace gs {

void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == grape::kCoordinatorRank) {
    VLOG(1) << "Workers are initialized.";
  }
}

void SetupEnv(const grape::CommSpec& comm_spec) {
  int systemMemory = getTotalSystemMemory();
  LOG(INFO) << "System Memory = " << systemMemory << " GB";
  int systemMemoryPerWorker = std::max(systemMemory / comm_spec.local_num(), 1);
  LOG(INFO) << "System Memory Per Worker = " << systemMemoryPerWorker << " GB";
  int mnPerWorker = std::max(systemMemoryPerWorker * 7 / 12, 1);

  char kvPair[32000];
  snprintf(kvPair, sizeof(kvPair), "-Xmx%dg -Xms%dg -Xmn%dg",
           systemMemoryPerWorker, systemMemoryPerWorker, mnPerWorker);

  char* jvm_opts = getenv("JVM_OPTS");

  char setStr[320010];
  if (jvm_opts == NULL || *jvm_opts == '\0') {
    snprintf(setStr, sizeof(setStr), "JVM_OPTS=%s", kvPair);
    putenv(setStr);
  } else {
    std::string jvmOptsStr = jvm_opts;
    LOG(INFO) << "jvm opts: " << jvmOptsStr;
    size_t pos = 0;
    std::string token;
    std::string delimiter = " ";
    bool flag = true;
    while ((pos = jvmOptsStr.find(delimiter)) != std::string::npos) {
      token = jvmOptsStr.substr(0, pos);
      jvmOptsStr.erase(0, pos + delimiter.length());
      if (token.length() > 4) {
        std::string prefix = token.substr(0, 4);
        if (prefix == "-Xmx" || prefix == "-Xms" || prefix == "-Xmn") {
          LOG(INFO) << "token = " << token;
          flag = false;
          break;
        }
      }
    }
    if (flag) {
      snprintf(setStr, sizeof(setStr), "JVM_OPTS=%s %s", jvm_opts, kvPair);
      putenv(setStr);
    }
  }
}

jclass getMainClass(JNIEnv* env, const std::string& main_class_name) {
  char* c_class_name = new char[main_class_name.length() + 1];
  c_class_name[main_class_name.length()] = '\0';
  strcpy(c_class_name, main_class_name.c_str());
  char* p = c_class_name;
  while (*p) {
    if (*p == '.')
      *p = '/';
    p++;
  }
  jclass main_class = env->FindClass(c_class_name);
  if (main_class == NULL) {
    LOG(ERROR) << "fail to find main class";
    return NULL;
  }
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

// arg 1: main class to run
// arg 2: path of jar
// arg 3: conf path
// args 4: output path
// args 4.. : other cmdline parameters
void preprocess(int argc, char** argv) {
  if (argc < 4) {
    LOG(INFO) << "at least 4 args required, jar path, main class name,conf "
                 "path, output path, and others received "
              << argc;
    return;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  double end_to_end = -grape::GetCurrentTime();

  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  SetupEnv(comm_spec);

  JavaVM* jvm = GetJavaVM();
  (void) jvm;

  JNIEnvMark m;
  // args 0: the exe itself
  // args 1: mainclass
  // args 2: jar name
  // args 3: conf path

  if (m.env()) {
    jclass system_class = getMainClass(m.env(), "java/lang/System");
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

    std::string main_class_str = argv[1];
    std::string jar_path = argv[2];
    std::string conf_path = argv[3];
    std::string ffi_output_path = argv[4];
    LOG(INFO) << "jar path" << jar_path << ", conf path " << conf_path
              << ", output destination:" << ffi_output_path;

    std::vector<std::string> args;
    // java main class need conf path as input
    args.push_back(conf_path);
    for (int i = 0; i < argc; ++i) {
      std::string tmp = argv[i];
      LOG(INFO) << i << " " << tmp;
      if (i > 4) {
        std::string v = argv[i];
        args.push_back(v);
      }
    }
    jclass main_class = getMainClass(m.env(), main_class_str);
    if (main_class == NULL) {
      LOG(ERROR) << "Fail to find mainclass" + main_class_str;
      return;
    }
    jmethodID main_method = m.env()->GetStaticMethodID(
        main_class, "main", "([Ljava/lang/String;)V");
    if (main_method == NULL) {
      LOG(ERROR) << "Fail to get main method for class " + main_class_str;
      return;
    }

    jobjectArray mainArgs;
    mainArgs =
        m.env()->NewObjectArray(static_cast<int>(args.size()),
                                m.env()->FindClass("java/lang/String"), NULL);
    for (size_t i = 0; i < args.size(); ++i) {
      jstring arg = m.env()->NewStringUTF(args[i].c_str());
      LOG(INFO) << "args " << i << " " << args[i];
      m.env()->SetObjectArrayElement(mainArgs, static_cast<int>(i), arg);
    }
    m.env()->CallStaticVoidMethod(main_class, main_method, mainArgs);
    if (m.env()->ExceptionOccurred()) {
      LOG(ERROR) << "Exception occurred in app main class";
      m.env()->ExceptionDescribe();
      m.env()->ExceptionClear();
      // env->DeleteLocalRef(main_class);
    }

    LOG(INFO) << "Exiting app main function";
    // Call grapeAnnotationProcessor here
    // Currently we geneerate graphScope sample and grape sample
    // from the sampe entrence, so for gs, the main class is never used
    // in codegen.
    std::string grape_process_class_name =
        "com/alibaba/grape/annotation/GrapeAppScanner";
    jclass grape_process_class =
        m.env()->FindClass(grape_process_class_name.c_str());
    if (grape_process_class == NULL) {
      LOG(ERROR) << "fail to find grape process class ";
      return;
    }
    jmethodID process_method =
        m.env()->GetStaticMethodID(grape_process_class, "scanAppAndGenerate",
                                   "(Ljava/lang/String;Ljava/lang/String;Ljava/"
                                   "lang/String;Z)Ljava/lang/String;");
    if (process_method == NULL) {
      LOG(ERROR) << "fail to find process method";
      return;
    }
    jstring jar_path_jstring = m.env()->NewStringUTF(jar_path.c_str());
    jstring conf_path_jstring = m.env()->NewStringUTF(conf_path.c_str());
    jstring ffi_output_path_jstring =
        m.env()->NewStringUTF(ffi_output_path.c_str());
    jstring jres = (jstring) m.env()->CallStaticObjectMethod(
        grape_process_class, process_method, jar_path_jstring,
        conf_path_jstring, ffi_output_path_jstring, true);
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
