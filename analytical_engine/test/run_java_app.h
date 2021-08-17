#ifndef ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_H_
#define ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_H_

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

//#include "alibaba-ffi.h"
#include "core/fragment/java_immutable_edgecut_fragment.h"
#include "core/loader/java_immutable_edgecut_fragment_loader.h"
#include "grape/grape.h"
#include "grape/types.h"
#include "grape/util.h"
#include "java_pie/java_pie_default_app.h"
#include "java_pie/java_pie_parallel_app.h"
#include "java_pie/javasdk.h"
// #define USE_X
namespace grape {

char const* OID_T_str = "std::vector<std::vector<int32_t>";
char const* VID_T_str = "std::vector<std::vector<uint32_t>>";
char const* VDATA_T_str = "std::vector<std::vector<int>>";
char const* EDATA_T_str = "std::vector<std::vector<double>>";

void Init() {
  InitMPIComm();
  CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_id() == kCoordinatorRank) {
    VLOG(1) << "Workers are initialized.";
  }
}
inline uint64_t getTotalSystemMemory() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  uint64_t ret = pages * page_size;
  LOG(INFO) << "---> getTotalSystemMemory() -> " << ret;
  ret = ret / 1024;
  ret = ret / 1024;
  ret = ret / 1024;
  return ret;
}

void SetupEnv(const CommSpec& comm_spec) {
  int systemMemory = getTotalSystemMemory();
  // LOG(INFO) << "System Memory = " << systemMemory << " GB";
  int systemMemoryPerWorker = std::max(systemMemory / comm_spec.local_num(), 1);
  // LOG(INFO) << "System Memory Per Worker = " << systemMemoryPerWorker << "
  // GB";
  int mnPerWorker = std::max(systemMemoryPerWorker * 7 / 12, 1);

  char kvPair[32000];
  snprintf(kvPair, sizeof(kvPair), "-Xmx%dg -Xms%dg -Xmn%dg",
           systemMemoryPerWorker, systemMemoryPerWorker, mnPerWorker);

  char* jvm_opts = getenv("JVM_OPTS");

  char setStr[32010];
  if (jvm_opts == NULL || *jvm_opts == '\0') {
    snprintf(setStr, sizeof(setStr), "JVM_OPTS=%s", kvPair);
    putenv(setStr);
  } else {
    std::string jvmOptsStr = jvm_opts;
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

jclass getClassByJavaPath(JNIEnv* env, const std::string& main_class_name) {
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

std::string generate_cpp_fragment_signature() {
  std::stringstream ss;
  ss << "grape::JavaImmutableEdgecutFragment<";
  std::string oid_t_str(grape::OID_T_str);
  std::string vid_t_str(grape::VID_T_str);
  std::string vdata_t_str(grape::VDATA_T_str);
  std::string edata_t_str(grape::EDATA_T_str);

  ss << oid_t_str.substr(24, oid_t_str.size() - 26) << ",";
  ss << vid_t_str.substr(24, vid_t_str.size() - 26) << ",";
  ss << vdata_t_str.substr(24, vdata_t_str.size() - 26) << ",";
  ss << edata_t_str.substr(24, edata_t_str.size() - 26);
  ss << ">";
  return ss.str();
}

void parseArgs(int argc, char** argv, std::string& app_class_name,
               std::string& app_context_name, bool& serialize,
               bool& deserialize, std::string& serialize_prefix,
               std::vector<std::string>& java_args) {
  if (argc < 4) {
    LOG(FATAL) << "at least 3 args required, received " << argc;
    return;
  }
  std::string tmp = argv[1];
  if (tmp == "serialize") {
    deserialize = false;
    serialize = true;
  } else if (tmp == "deserialize") {
    serialize = false;
    deserialize = true;
  } else if (tmp == "noSerialize") {
    serialize = false;
    deserialize = false;
  } else {
    LOG(FATAL) << "Unrecoginzed args" << argv[1];
  }
  if (serialize || deserialize) {
    serialize_prefix = argv[2];
    app_class_name = argv[3];
    app_context_name = argv[4];
    for (auto i = 5; i < argc; ++i) {
      std::string str = argv[i];
      java_args.push_back(str);
    }
  } else {
    app_class_name = argv[2];
    app_context_name = argv[3];
    for (auto i = 4; i < argc; ++i) {
      std::string str = argv[i];
      java_args.push_back(str);
    }
  }
  VLOG(2) << "serialization: " << tmp;
  VLOG(2) << "serialization prefix" << serialize_prefix;
  VLOG(2) << "app class name" << app_class_name;
  VLOG(2) << "context class name" << app_context_name;
}
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
void LoadingFromJava(JNIEnv* env, const CommSpec& comm_spec,
                     std::string main_class_name,
                     std::vector<std::vector<OID_T>>& vid_buffers,
                     std::vector<std::vector<VDATA_T>>& vdata_buffers,
                     std::vector<std::vector<OID_T>>& esrc_buffers,
                     std::vector<std::vector<OID_T>>& edst_buffers,
                     std::vector<std::vector<EDATA_T>>& edata_buffers) {
  MPI_Barrier(comm_spec.comm());
  double loading_java_duration = -GetCurrentTime();
  // fetch the main class
  jclass main_class = getClassByJavaPath(env, main_class_name);
  if (main_class == NULL) {
    LOG(FATAL) << "get main class failed";
    return;
  }
  // after get class, so should be loaded.

  jmethodID ctor = env->GetMethodID(main_class, "<init>", "()V");
  if (ctor == NULL) {
    LOG(FATAL) << "Cannot find default constructor for main class"
               << main_class_name;
    return;
  }
  jobject main_class_object = env->NewObject(main_class, ctor);
  if (main_class_object == NULL) {
    LOG(FATAL) << "create main object failed";
    return;
  }
  jmethodID loadFragment_method = env->GetMethodID(main_class, "loadFragment",
                                                   "("
                                                   "Lcom/alibaba/ffi/FFIVector;"
                                                   "Lcom/alibaba/ffi/FFIVector;"
                                                   "Lcom/alibaba/ffi/FFIVector;"
                                                   "Lcom/alibaba/ffi/FFIVector;"
                                                   "Lcom/alibaba/ffi/FFIVector;"
                                                   "II)V");
  if (loadFragment_method == NULL) {
    LOG(FATAL) << "get loadFragment method failed";
    return;
  }
  // create ffi objects
  jobject vid_buffers_obj = createFFIVectorObject(
      env, OID_T_str, reinterpret_cast<jlong>(&vid_buffers));
  jobject vdata_buffers_obj = createFFIVectorObject(
      env, VDATA_T_str, reinterpret_cast<jlong>(&vdata_buffers));
  jobject esrc_buffers_obj = createFFIVectorObject(
      env, OID_T_str, reinterpret_cast<jlong>(&esrc_buffers));
  jobject edst_buffers_obj = createFFIVectorObject(
      env, OID_T_str, reinterpret_cast<jlong>(&edst_buffers));
  jobject edata_buffers_obj = createFFIVectorObject(
      env, EDATA_T_str, reinterpret_cast<jlong>(&edata_buffers));

  env->CallVoidMethod(main_class_object, loadFragment_method, vid_buffers_obj,
                      vdata_buffers_obj, esrc_buffers_obj, edst_buffers_obj,
                      edata_buffers_obj, comm_spec.worker_id(),
                      comm_spec.worker_num());
  if (env->ExceptionOccurred()) {
    LOG(ERROR) << std::string("Exception occurred in calling loader function");
    env->ExceptionDescribe();
    env->ExceptionClear();
    // env->DeleteLocalRef(main_class);
    LOG(FATAL) << "exiting since exception occurred";
  }
  MPI_Barrier(comm_spec.comm());
  loading_java_duration += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "Loading_java_duration: [" << loading_java_duration << " ]";
  }
}
template <typename FRAG_T>
std::shared_ptr<FRAG_T> ConstructInCPP(
    const CommSpec& comm_spec,
    std::vector<std::vector<typename FRAG_T::oid_t>>& vid_buffers,
    std::vector<std::vector<typename FRAG_T::vdata_t>>& vdata_buffers,
    std::vector<std::vector<typename FRAG_T::oid_t>>& esrc_buffers,
    std::vector<std::vector<typename FRAG_T::oid_t>>& edst_buffers,
    std::vector<std::vector<typename FRAG_T::edata_t>>& edata_buffers,
    bool serialize, std::string& prefix) {
  std::shared_ptr<JavaImmutableEdgecutFragmentLoader<FRAG_T>> fragment_loader =
      std::make_shared<JavaImmutableEdgecutFragmentLoader<FRAG_T>>();
  fragment_loader->Init();
  MPI_Barrier(comm_spec.comm());
  double loading_cpp_duration = -GetCurrentTime();
  double t0 = -GetCurrentTime();
  fragment_loader->AddVertexBuffersV2(vid_buffers, vdata_buffers);

  MPI_Barrier(comm_spec.comm());
  t0 += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "Add Vertex Buffer time = " << t0;
  }

  MPI_Barrier(comm_spec.comm());
  double t1 = -GetCurrentTime();

  fragment_loader->AddEdgeBuffersV2(esrc_buffers, edst_buffers, edata_buffers);

  MPI_Barrier(comm_spec.comm());
  t1 += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "Add Edge Buffer time = " << t1;
  }

  MPI_Barrier(comm_spec.comm());
  double t2 = -GetCurrentTime();

  auto fragment = fragment_loader->LoadFragment(serialize, prefix);

  MPI_Barrier(comm_spec.comm());
  t2 += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "t2 = " << t2;
  }

  MPI_Barrier(comm_spec.comm());
  loading_cpp_duration += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "loading cpp: " << loading_cpp_duration;
  }
  return fragment;
}
template <typename FRAG_T, typename APP_T>
void Query(const CommSpec& comm_spec, std::shared_ptr<FRAG_T> fragment,
           std::string& app_class_name, std::string& app_context_name,
           std::vector<std::string>& args) {
  // using AppType = JavaAppPIE<FRAG_T>;
  auto app = std::make_shared<APP_T>();
  auto worker = APP_T::CreateWorker(app, fragment);
  auto spec = DefaultParallelEngineSpec();

  worker->Init(comm_spec, spec);

  MPI_Barrier(comm_spec.comm());

  std::string frag_name = generate_cpp_fragment_signature();
  double t = -GetCurrentTime();

  // args contains cmdline arguments, and should be passed to java context
  worker->Query(frag_name, app_class_name, app_context_name, args);

  MPI_Barrier(comm_spec.comm());
  double t1 = GetCurrentTime() + t;

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename("./odps_output/", fragment->fid());
  ostream.open(output_path);
  worker->Output(ostream);

  MPI_Barrier(comm_spec.comm());
  t += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "eval time: including output" << t << " (s), only query" << t1
              << " (s)";
  }
  ostream.close();
  worker->Finalize();
}

template <typename FRAG_T, typename APP_T>
void LoadAndQuery(const CommSpec& comm_spec, JNIEnvMark& m,
                  std::string& app_class_name, std::string& app_context_name,
                  bool& serialize, bool& deserialize,
                  std::string& serialize_prefix,
                  std::vector<std::string>& java_args) {
   using OID_T = typename FRAG_T::oid_t;
   using VID_T = typename FRAG_T::vid_t;
   using VDATA_T = typename FRAG_T::vdata_t;
   using EDATA_T = typename FRAG_T::edata_t;
  // load_jni_library(m.env());
  std::shared_ptr<FRAG_T> fragment(nullptr);
  if (deserialize && (!serialize)) {
    if (serialize_prefix.size() <= 0) {
      LOG(FATAL) << "serialize prefix empty";
    }
    std::shared_ptr<JavaImmutableEdgecutFragmentLoader<FRAG_T>> fragment_loader =
        std::make_shared<JavaImmutableEdgecutFragmentLoader<FRAG_T>>();
    fragment_loader->Init();
    bool deserialized =
        fragment_loader->DeserializeFragment(fragment, serialize_prefix);
    int flag = 0;
    int sum = 0;
    if (!deserialized) {
      flag = 1;
    }
    MPI_Allreduce(&flag, &sum, 1, MPI_INT, MPI_SUM, comm_spec.comm());
    if (sum != 0) {
      fragment.reset();
      if (comm_spec.worker_id() == 0) {
        LOG(FATAL) << "Deserialization failed";
      }
    } else {
      VLOG(1) << "Deserialization success";
    }

  } else {
    // 0. Load fragment from java
    fid_t fnum = comm_spec.fnum();
    std::vector<std::vector<OID_T>> vid_buffers(fnum);
    std::vector<std::vector<VDATA_T>> vdata_buffers(fnum);
    std::vector<std::vector<OID_T>> esrc_buffers(fnum), edst_buffers(fnum);
    std::vector<std::vector<EDATA_T>> edata_buffers(fnum);
    if (m.env()) {
      LoadingFromJava<OID_T,VID_T,VDATA_T,EDATA_T>(m.env(), comm_spec, app_class_name,
				vid_buffers, vdata_buffers, esrc_buffers, edst_buffers,
                      edata_buffers);
      if (m.env()->ExceptionOccurred()) {
        LOG(ERROR) << std::string(
            "Exception occurred in calling "
            "loader function");
        m.env()->ExceptionDescribe();
        m.env()->ExceptionClear();
        // env->DeleteLocalRef(main_class);
        LOG(FATAL) << "Exiting since error occurred in loading data from java";
      }
      for (size_t i = 0; i < fnum; ++i) {
        VLOG(2) << "worker [" << comm_spec.worker_id() << "],frag [" << i
                << "]: oid size" << vid_buffers[i].size() << ",edge size  "
                << esrc_buffers[i].size();
      }
    }
    // 1.Construct graph in c++
    fragment = ConstructInCPP<FRAG_T>(comm_spec, vid_buffers, vdata_buffers,
                                      esrc_buffers, edst_buffers, edata_buffers,
                                      serialize, serialize_prefix);
  }
  if (fragment.get() == nullptr) {
    LOG(FATAL) << "Error in loading fragment, got nullptr fragment";
  }
  VLOG(1) << "[worker " << comm_spec.worker_id()
          << "] finished load fragment, inner_vertices num: "
          << fragment->GetInnerVerticesNum() << ", outer vertices num"
          << fragment->GetOuterVerticesNum() << ", frag vertices "
          << fragment->GetVerticesNum() << ",total vertices "
          << fragment->GetTotalVerticesNum() << ", incoming edge num "
          << fragment->GetIncomingEdgeNum() << ", outgoing edge num "
          << fragment->GetOutgoingEdgeNum();

  Query<FRAG_T, APP_T>(comm_spec, fragment, app_class_name, app_context_name,
                       java_args);
  // run query
}

// args 0: serialize/deserialze/java
// args 1: serialize prefix (available when serialize)
// args 2: app class name
// args 3: app context class name
// args 4...: other params
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
void Run(int argc, char** argv) {
  if (argc < 4) {
    LOG(INFO)
        << "at least 4 args required, app class name and context class name."
        << "    serialize option, serialize prefix, and other params";
    return;
  }
  MPI_Barrier(MPI_COMM_WORLD);
  double end_to_end = -GetCurrentTime();

  CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);

  SetupEnv(comm_spec);

  JavaVM* jvm = GetJavaVM();
  (void) jvm;
  JNIEnvMark m;

  std::string serialize_prefix;
  std::string app_class_name;
  std::string app_context_name;
  bool serialize = false;
  bool deserialize = false;
  std::vector<std::string> java_args;

  parseArgs(argc, argv, app_class_name, app_context_name, serialize,
            deserialize, serialize_prefix, java_args);

  if (app_class_name.find("Parallel") != std::string::npos) {
    if (app_class_name.find("SSSP") != std::string::npos) {
      using FRAG_T =
          JavaImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                       LoadStrategy::kBothOutIn>;
      // sssp should be onlyOut, here use both outin for deserialization
      // consistent
      using APP_T = JavaPIEParallelApp<FRAG_T>;
      LoadAndQuery<FRAG_T, APP_T>(comm_spec, m, app_class_name,
                                  app_context_name, serialize, deserialize,
                                  serialize_prefix, java_args);
    } else {
      using FRAG_T =
          JavaImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                       LoadStrategy::kBothOutIn>;
      using APP_T = JavaPIEParallelApp<FRAG_T>;
      LoadAndQuery<FRAG_T, APP_T>(comm_spec, m, app_class_name,
                                  app_context_name, serialize, deserialize,
                                  serialize_prefix, java_args);
    }
  } else {
    if (app_class_name.find("SSSP") != std::string::npos) {
      using FRAG_T =
          JavaImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                       LoadStrategy::kBothOutIn>;
      // sssp should be onlyOut, here use both outin for deserialization
      // consistent
      using APP_T = JavaPIEDefaultApp<FRAG_T>;
      LoadAndQuery<FRAG_T, APP_T>(comm_spec, m, app_class_name,
                                  app_context_name, serialize, deserialize,
                                  serialize_prefix, java_args);
    } else {
      using FRAG_T =
          JavaImmutableEdgecutFragment<OID_T, VID_T, VDATA_T, EDATA_T,
                                       LoadStrategy::kBothOutIn>;
      using APP_T = JavaPIEDefaultApp<FRAG_T>;
      LoadAndQuery<FRAG_T, APP_T>(comm_spec, m, app_class_name,
                                  app_context_name, serialize, deserialize,
                                  serialize_prefix, java_args);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);
  end_to_end += GetCurrentTime();
  if (comm_spec.worker_id() == 0) {
    LOG(INFO) << "end to end time: " << end_to_end;
  }
}

void Finalize() {
  FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
}

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_TEST_RUN_JAVA_APP_H_
