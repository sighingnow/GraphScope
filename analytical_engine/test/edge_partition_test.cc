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
#include "core/java/graphx/edge_partition.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

void Run() {
  vineyard::Client client;
  gs::EdgePartition<int64_t, uint64_t, int32_t>* partition =
      new gs::EdgePartition<int64_t, uint64_t, int32_t>(client);
  arrow::Int64Builder srcBuilder, dstBuilder;
  arrow::Int32Builder edBuilder;
  srcBuilder.Reserve(2);
  dstBuilder.Reserve(2);
  edBuilder.Reserve(2);
  srcBuilder.UnsafeAppend(0);
  srcBuilder.UnsafeAppend(1);
  dstBuilder.UnsafeAppend(2);
  dstBuilder.UnsafeAppend(3);
  edBuilder.UnsafeAppend(4);
  edBuilder.UnsafeAppend(5);

  partition->LoadEdges(srcBuilder, dstBuilder, edBuilder);
  LOG(INFO) << "vnum: " << partition->GetVerticesNum() << ", edge num"
            << partition->GetEdgesNum()
            << " inedges : " << partition->GetInEdges().vertex_num();
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("edge_partition");
  google::InstallFailureSignalHandler();

  Run();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
  return 0;
}
