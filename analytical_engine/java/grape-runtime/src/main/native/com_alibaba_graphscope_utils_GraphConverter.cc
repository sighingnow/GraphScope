/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifdef ENABLE_JAVA_SDK

#include <jni.h>
#include <string>
#include "core/loader/arrow_fragment_loader.h"
#include "glog/logging.h"
#include "grape/config.h"
#include "grape/grape.h"
#include "vineyard/client/client.h"
/* Header for class com_alibaba_graphscope_utils_GraphConverter */

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_alibaba_graphscope_utils_GraphConverter
 * Method:    createArrowFragmentLoader
 * Signature: ()J;
 */
JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_utils_GraphConverter_createArrowFragmentLoader(
    JNIEnv* env, jclass clz) {
  using FragmentLoaderType =
      gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  // get comm_spec, create vineyard client
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  VLOG(1) << "Created comm_spec";
  static vineyard::client client;
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
  subLabel->protocol = "file";
  // subLabel->values = efile;
  // subLabel->eformat += edge_input_format_class;  // eif
  edge->sub_labels.push_back(*subLabel.get());

  graph->edges.push_back(edge);

  // create arrowFragmentLoader and return
  auto loader = std::make_shared<FragmentLoaderType>(client, comm_spec, graph);
  VLOG(1) << "Sucessfully create load;" return reinterpret_cast<jlong>(
      loader->get());
}

JNIEXPORT jlong JNICALL
Java_com_alibaba_graphscope_utils_GraphConverter_constructFragment(
    JNIEnv* env, jclass clz, jlong addr, jint vd_type, jint ed_type) {
  using FragmentLoaderType =
      gs::ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  using FragmentType =
      vineyard::ArrowFragment<vineyard::property_graph_types::OID_TYPE,
                              vineyard::property_graph_types::VID_TYPE>;
  using ProjectedFragmentType =
      ArrowProjectedFragment<int64_t, uint64_t, int64_t, int64_t>;
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
  std::shared_ptr<FragmentType> fragment =
      std::dynamic_pointer_cast<FragmentType>(client.GetObject(fragment_id));

  VLOG(10) << "fid: " << fragment->fid() << "fnum: " << fragment->fnum()
           << "v label num: " << fragment->vertex_label_num()
           << "e label num: " << fragment->edge_label_num()
           << "total v num: " << fragment->GetTotalVerticesNum();
  VLOG(1) << "inner vertices: " << fragment->GetInnerVerticesNum(0);
  // project
  std::shared_ptr<ProjectedFragmentType> projected_fragment =
      ProjectedFragmentType::Project(fragment, "0", "0", "0", "0");
  // return projected fragment pointer.
  return reinterpret_cast<jlong>(projected_fragment->get());
}

#ifdef __cplusplus
}
#endif
#endif
