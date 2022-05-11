/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <memory>
#include <string>
#include <utility>

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "core/java/graphx/local_vertex_map.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

void Run() {
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect("/tmp/vineyard.sock"));
  LOG(INFO) << "Connected to IPCServer: ";

  arrow::Int64Builder inner, outer;
  inner.Reserve(3);
  outer.Reserve(2);
  inner.UnsafeAppend(1, 2, 3);
  outer.UnsafeAppend(5, 6);
  gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                            outer);
  auto vmap = std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
      builder.Seal(client_));

  VINEYARD_CHECK_OK(client_.Persist(vmap->id()));
  LOG(INFO) << "Persist vmap id: " << vmap->id();
  LOG(INFO) << "vnum: " << vmap->GetVerticesNum();
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("edge_partition");
  google::InstallFailureSignalHandler();

  TestLocalVertexMap();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
  return 0;
}
