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
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/local_vertex_map.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

void TestLocalVertexMap(vineyard::Client& client;) {
  vineyard::ObjectID vmap_id;
  {
    arrow::Int64Builder inner, outer;
    inner.Reserve(3);
    outer.Reserve(2);
    inner.UnsafeAppend(1);
    inner.UnsafeAppend(2);
    inner.UnsafeAppend(3);
    outer.UnsafeAppend(5);
    outer.UnsafeAppend(6);
    gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                              outer);
    auto vmap =
        std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
            builder.Seal(client));

    VINEYARD_CHECK_OK(client.Persist(vmap->id()));
    vmap_id = vmap->id();
    LOG(INFO) << "Persist vmap id: " << vmap->id();
  }
  std::shared_ptr<gs::LocalVertexMap<int64_t, uint64_t>> vmap =
      std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
          client.GetObject(vmap_id));
  LOG(INFO) << "Got vmap " << vmap->id();
  LOG(INFO) << "num vertices: " << vmap->GetVerticesNum();
}

void TestGraphXCSR(vineyard::Client& client) {
  vineyard::ObjectID csr_id;
  {
    uint64_t vnum = 3;
    std::shared_ptr<arrow::Uint64Array> srcLids, dstLids;
    std::shared_ptr<arrow::Int64Array> edatas;
    generateData(srcLids, dstLids, edatas);

    gs::BasicGraphXCSRBuilder<uint64_t, int64_t> builder(client);
    builder.LoadEdges(vnums, srcBuilder, dstBuilder, edataBuilder);
    auto csr = std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t, int64_t>>(
        builder.Seal(client));

    VINEYARD_CHECK_OK(client.Persist(csr->id()));
    csr_id = csr->id();
    LOG(INFO) << "Persist csr id: " << csr->id();
  }
  std::shared_ptr<gs::GraphXCSR<int64_t, uint64_t>> csr =
      std::dynamic_pointer_cast<gs::GraphXCSR<int64_t, uint64_t>>(
          client.GetObject(csr_id));
  LOG(INFO) << "Got csr " << csr->id();
  LOG(INFO) << "num edges: " << csr->GetTotalEdgesNum() << " vs "
            << csr->GetPartialEdgesNum(0, 3);
  LOG(INFO) << "lid 0 degreee: " << csr->GetDegree(0) << ", "
            << csr->GetPartialEdgesNum(0, 1);
}

void generateData(std::shared_ptr<arrow::Uint64Array>& srcLids,
                  std::shared_ptr<arrow::Uint64Array>& dstLids,
                  std::shared_ptr<arrow::int64Array>& edatas) {
  arrow::Uint64Builder srcBuilder, dstBuilder;
  arrow::Int64Builder edataBuilder;
  srcLids.Reseve(5);
  dstBuilder.Reserve(5);
  edataBuilder.Reserve(5);

  srcBuilder.UnsafeAppend(0);
  srcBuilder.UnsafeAppend(0);
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(2);

  dstBuilder.UnsafeAppend(1);
  dstBuilder.UnsafeAppend(2);
  dstBuilder.UnsafeAppend(0);
  dstBuilder.UnsafeAppend(2);
  dstBuilder.UnsafeAppend(1);

  edataBuilder.UnsafeAppend(1);
  edataBuilder.UnsafeAppend(2);
  edataBuilder.UnsafeAppend(3);
  edataBuilder.UnsafeAppend(4);
  edataBuilder.UnsafeAppend(5);

  srcBuilder.Finish(&srcLids);
  dstBuilder.Finish(&dstLids);
  edataBuilder.Finish(&edatas);
}

int main(int argc, char* argv[]) {
  google::InitGoogleLogging("graphx_test");
  google::InstallFailureSignalHandler();
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect("/tmp/vineyard.sock"));
  LOG(INFO) << "Connected to IPCServer: ";

  TestLocalVertexMap(client);
  TestGraphXCSR(client);
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
  return 0;
}
