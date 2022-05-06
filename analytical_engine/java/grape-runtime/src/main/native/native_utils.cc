#include <jni.h>
#include <string>
#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"
#include "glog/logging.h"
#include "grape/config.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"

#ifdef __cplusplus
extern "C" {
#endif

static vineyard::Client client;
/*
 * Class:     com_alibaba_graphscope_runtime_NativeUtils
 * Method:    createLoader
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_runtime_NativeUtils_createLoader(JNIEnv*, jclass) {
  using FragmentLoaderType =
      gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  // get comm_spec, create vineyard client
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  VLOG(1) << "Created comm_spec";
  std::string ipc_socket = "/tmp/vineyard.sock.lei";
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  auto graph = std::make_shared<gs::detail::Graph>();
  graph->directed = true;
  graph->generate_eid = false;

  auto vertex = std::make_shared<gs::detail::Vertex>();
  vertex->label = "label1";
  vertex->vid = "0";
  vertex->protocol = "graphx";
  // vertex->values = vfile;
  // vertex->vformat = vertex_input_format_class;  // vif

  graph->vertices.push_back(vertex);

  auto edge = std::make_shared<gs::detail::Edge>();
  edge->label = "label2";
  auto subLabel = std::make_shared<gs::detail::Edge::SubLabel>();
  subLabel->src_label = "label1";
  subLabel->src_vid = "0";
  subLabel->dst_label = "label1";
  subLabel->dst_vid = "0";
  subLabel->protocol = "graphx";
  // subLabel->values = efile;
  // subLabel->eformat += edge_input_format_class;  // eif
  edge->sub_labels.push_back(*subLabel.get());

  graph->edges.push_back(edge);

  // create arrowFragmentLoader and return
  static auto loader =
      std::make_shared<FragmentLoaderType>(client, comm_spec, graph);
  VLOG(1) << "Sucessfully create load;";
  return reinterpret_cast<jlong>(loader.get());
}

JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_runtime_NativeUtils_invokeLoadingAndProjection(
    JNIEnv*, jclass, jlong addr, jint vdType, jint edType) {
  using FragmentLoaderType =
      gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  using FragmentType =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  using ProjectedFragmentType =
      gs::ArrowProjectedFragment<int64_t, uint64_t, double, double>;
  auto loader = reinterpret_cast<FragmentLoaderType*>(addr);

  vineyard::ObjectID fragment_id = boost::leaf::try_handle_all(
      [&loader]() { return loader->LoadFragment(); },
      [](const vineyard::GSError& e) {
        LOG(FATAL) << e.error_msg;
        return 0;
      },
      [](const boost::leaf::error_info& unmatched) {
        LOG(FATAL) << "Unmatched error " << unmatched;
        return 0;
      });
  VLOG(1) << "frag id: " << fragment_id;

  static std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

  VLOG(10) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
           << "v label num: " << fragment->vertex_label_num()
           << "e label num: " << fragment->edge_label_num()
           << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0);
  // project
  static std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, "0", "0", "0", "0");
  // return projected fragment pointer.
  return reinterpret_cast<jlong>(projected_fragment.get());
}
JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_runtime_NativeUtils_getArrowProjectedFragment(
    JNIEnv* env, jclass clz, jlong fragId, jstring jfragName) {
  std::string ipc_socket = "/tmp/vineyard.sock";
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  std::string fragName = gs::JString2String(env, jfragName);
  if (std::strcmp(
          fragName.c_str(),
          "gs::ArrowProjectedFragment<int64_t,uint64_t,int64_t,int64_t>") ==
      0) {
    using FragType =
        gs::ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t>;
    static std::shared_ptr<FragType> fragment =
        std::dynamic_pointer_cast<FragType>(client.GetObject(fragId));
    return reinterpret_cast<jlong>(fragment.get());
  } else {
    LOG(ERROR) << "Unrecognized fragname: " << fragName;
  }
  return 0;
}

/*
 * Class:     com_alibaba_graphscope_runtime_NativeUtils
 * Method:    nativeCreateEdgePartition
 * Signature: (Ljava/lang/String;JI)J
 */
JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_runtime_NativeUtils_nativeCreateEdgePartition(
    JNIEnv* env, jclass clz, jstring mmFiles, jlong mapped_size, jint ed_type) {
  google::InitGoogleLogging("NativeUtils");
  google::InstallFailureSignalHandler();

  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  VLOG(1) << "Created comm_spec";
  std::string ipc_socket = "/tmp/vineyard.sock";
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));
  VLOG(1) << "Connected to " << ipc_socket;

  std::string files = gs::JString2String(env, mmFiles);
  if (ed_type == 0) {
    VLOG(1) << "creating EdgePartition for ed = int64_t, mmfiles " << files;
    static auto edge_partition =
        std::make_shared<gs::EdgePartition<int64_t, uint64_t, int64_t>>(
            client, comm_spec);
    edge_partition->LoadEdges(files, mapped_size);
    return reinterpret_cast<jlong>(edge_partition.get());
  } else if (ed_type == 1) {
    VLOG(1) << "creating EdgePartition for ed = double, mmfiles " << files;
    static auto edge_partition =
        std::make_shared<gs::EdgePartition<int64_t, uint64_t, double>>(
            client, comm_spec);
    edge_partition->LoadEdges(files, mapped_size);
    return reinterpret_cast<jlong>(edge_partition.get());
  } else if (ed_type == 2) {
    VLOG(1) << "creating EdgePartition for ed = int32_t, mmfiles " << files;
    static auto edge_partition =
        std::make_shared<gs::EdgePartition<int64_t, uint64_t, int32_t>>(
            client, comm_spec);
    edge_partition->LoadEdges(files, mapped_size);
    return reinterpret_cast<jlong>(edge_partition.get());
  } else {
    LOG(ERROR) << "WRONG ed type: " << ed_type;
    return -1;
  }
}

#ifdef __cplusplus
}
#endif
