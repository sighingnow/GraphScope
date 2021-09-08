#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "apps/property/sssp_property.h"
#include "boost/algorithm/string/classification.hpp"  // Include boost::for is_any_of
#include "boost/algorithm/string/split.hpp"  // Include for boost::split
#include "boost/property_tree/exceptions.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/java/javasdk.h"
#include "core/loader/arrow_fragment_loader.h"
#include "core/loader/java_immutable_edgecut_fragment_loader.h"
#include "core/object/fragment_wrapper.h"
#include "glog/logging.h"
#include "grape/grape.h"
#include "grape/types.h"
#include "grape/util.h"
#include "java_pie/java_pie_property_default_app.h"
#include "proto/graph_def.pb.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/arrow_fragment.h"

using FragmentType =
    vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                            vineyard::property_graph_types::VID_TYPE>;
void output_nd_array(const grape::CommSpec& comm_spec,
                     std::unique_ptr<grape::InArchive> arc,
                     const std::string& output_path) {
  if (comm_spec.worker_id() == 0) {
    grape::OutArchive oarc;
    oarc = std::move(*arc);

    int64_t ndim, length1, length2;
    int data_type;
    oarc >> ndim;
    LOG(INFO) << "ndim: " << ndim;
    CHECK_EQ(ndim, 1);
    oarc >> length1;
    oarc >> data_type;
    LOG(INFO) << "length1: " << length1 << ",data type: " << data_type;
    CHECK_EQ(data_type, 7);
    oarc >> length2;
    LOG(INFO) << "length2: " << length2;
    CHECK_EQ(length1, length2);

    std::ofstream assembled_ostream;
    assembled_ostream.open(output_path);
    LOG(INFO) << "osream " << output_path;
    for (int64_t i = 0; i < length1; ++i) {
      double v;
      oarc >> v;
      assembled_ostream << v << std::endl;
    }
    LOG(INFO) << "output complete: " << oarc.Empty() << output_path;
    CHECK(oarc.Empty());

    assembled_ostream.close();
  }
}

void Query(std::shared_ptr<FragmentType> fragment,
           const grape::CommSpec& comm_spec, const std::string& app_name,
           const std::string& out_prefix, const std::string& basic_params) {
  using AppType = gs::JavaPIEPropertyDefaultApp<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);
  worker->Query(basic_params);
  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  std::shared_ptr<gs::JavaPIEPropertyDefaultContext<FragmentType>> ctx =
      worker->GetContext();
  worker->Finalize();

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_graph_type(gs::rpc::graph::ARROW_PROPERTY);

  auto frag_wrapper = std::make_shared<gs::FragmentWrapper<FragmentType>>(
      "graph_123", graph_def, fragment);

  gs::JavaPIEPropertyDefaultContextWrapper<FragmentType> ctx_wrapper(
      "ctx_wrapper_" + vineyard::random_string(8), frag_wrapper, ctx);
  //  auto selector = gs::LabeledSelector::parse("r:label0.property0").value();
  std::string selector_string = "r:label0.property0";
  auto range = std::make_pair("", "");
  std::unique_ptr<grape::InArchive> arc = std::move(
      ctx_wrapper.ToNdArray(comm_spec, selector_string, range).value());
  std::string java_out_prefix = out_prefix + "/java_assembled_ndarray.dat";
  output_nd_array(comm_spec, arc, java_out_prefix);
  LOG(INFO) << "finish query";
}

void RunSSSP(std::shared_ptr<FragmentType> fragment,
             const grape::CommSpec& comm_spec, const std::string& out_prefix) {
  using AppType = gs::SSSPProperty<FragmentType>;
  auto app = std::make_shared<AppType>();
  auto worker = AppType::CreateWorker(app, fragment);
  auto spec = grape::DefaultParallelEngineSpec();
  worker->Init(comm_spec, spec);

  worker->Query(4);

  std::ofstream ostream;
  std::string output_path =
      grape::GetResultFilename(out_prefix, fragment->fid());

  ostream.open(output_path);
  worker->Output(ostream);
  ostream.close();

  worker->Finalize();

  gs::rpc::graph::GraphDefPb graph_def;
  graph_def.set_graph_type(gs::rpc::graph::ARROW_PROPERTY);

  auto frag_wrapper = std::make_shared<gs::FragmentWrapper<FragmentType>>(
      "graph_456", graph_def, fragment);
  gs::LabeledVertexDataContextWrapper<FragmentType> ctx_wrapper(
      "ctx_wrapper_" + vineyard::random_string(8), frag_wrapper, ctx);
  //  auto selector = gs::LabeledSelector::parse("r:label0.property0").value();
  std::string selector_string = "r:label0.property0";
  auto range = std::make_pair("", "");
  std::unique_ptr<grape::InArchive> arc = std::move(
      ctx_wrapper.ToNdArray(comm_spec, selector_string, range).value());
  std::string cpp_out_prefix = out_prefix + "/cpp_assembled_ndarray.dat";
  output_nd_array(comm_spec, arc, cpp_out_prefix);
}

// Running test doesn't require codegen.
void Run(vineyard::Client& client, const grape::CommSpec& comm_spec,
         vineyard::ObjectID id, bool run_projected,
         const std::string& app_name) {
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(id));
  // 0. setup environment
  // gs::SetupEnv(comm_spec.local_num());
  // 1. prepare the running params;
  boost::property_tree::ptree pt;
  pt.put("src", "4");
  pt.put("frag_name", "vineyard::ArrowFragmentDefault<int64_t>");
  pt.put("app_class", app_name);
  // The path to sdk jni library
  pt.put("user_library_name", "vineyard-jni");
  char* jvm_opts = getenv("RUN_JVM_OPTS");

  pt.put("jvm_runtime_opt", std::string(jvm_opts));
  LOG(INFO) << "geted shell env : " << std::string(jvm_opts);
  std::stringstream ss;
  boost::property_tree::json_parser::write_json(ss, pt);
  std::string basic_params = ss.str();
  LOG(INFO) << "basic_params" << basic_params;
  // 1. run java query
  Query(fragment, comm_spec, app_name, "/tmp", basic_params);

  // 2.run c++ query
  RunSSSP(fragment, comm_spec, "/tmp");
}

int main(int argc, char** argv) {
  if (argc < 9) {
    printf(
        "usage: ./run_java_property_app <ipc_socket> <e_label_num> "
        "<efiles...> "
        "<v_label_num> <vfiles...> <run_projected>"
        "[directed] [app_name]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
  }

  int vertex_label_num = atoi(argv[index++]);
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
  }

  int run_projected = atoi(argv[index++]);

  int directed = 1;
  std::string app_name = "";
  // std::string path_pattern = "";
  if (argc > index) {
    directed = atoi(argv[index++]);
  }
  if (argc > index) {
    app_name = argv[index++];
  }
  LOG(INFO) << "app name " << app_name;

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

    vineyard::ObjectID fragment_id;
    {
      auto loader = std::make_unique<
          gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                                  vineyard::property_graph_types::VID_TYPE>>(
          client, comm_spec, efiles, vfiles, directed != 0);
      fragment_id = boost::leaf::try_handle_all(
          [&loader]() { return loader->LoadFragment(); },
          [](const vineyard::GSError& e) {
            LOG(FATAL) << e.error_msg;
            return 0;
          },
          [](const boost::leaf::error_info& unmatched) {
            LOG(FATAL) << "Unmatched error " << unmatched;
            return 0;
          });
    }

    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ...";

    MPI_Barrier(comm_spec.comm());

    Run(client, comm_spec, fragment_id, run_projected, app_name);
    MPI_Barrier(comm_spec.comm());
  }

  grape::FinalizeMPIComm();
  return 0;
}
