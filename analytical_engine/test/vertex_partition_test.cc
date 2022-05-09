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

#include "core/java/graphx/vertex_partition.h"
#include "glog/logging.h"

void Run() {
  gs::VertexPartitionBuilder<int64_t, uint64_t, int32_t> builder;
  gs::VertexPartition<int64_t, uint64_t, int32_t> partition;
  std::vector<int64_t> oids;
  oids.push_back(1);
  oids.push_back(2);
  builder.AddVertex(oids, 1);
  builder.Build(partition, 1);
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("vertex_partition");
  google::InstallFailureSignalHandler();

  Run();
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
  return 0;
}
