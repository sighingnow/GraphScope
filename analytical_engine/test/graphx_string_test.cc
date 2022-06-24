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
#include "core/java/graphx/edge_data.h"
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/graphx_fragment.h"
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/graphx/vertex_data.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

boost::leaf::result<void> generateData(arrow::Int64Builder& srcBuilder,
                                       arrow::Int64Builder& dstBuilder,
                                       arrow::Int64Builder& edataBuilder,
                                       grape::CommSpec& comm_spec);

vineyard::ObjectID getLocalVM(vineyard::Client& client,
                              grape::CommSpec& comm_spec) {
  vineyard::ObjectID vmap_id;
  {
    arrow::Int64Builder inner, outer;
    if (comm_spec.worker_id() == 0) {
      CHECK(inner.Reserve(3).ok());
      CHECK(outer.Reserve(3).ok());
      inner.UnsafeAppend(2);
      inner.UnsafeAppend(4);
      inner.UnsafeAppend(6);
      outer.UnsafeAppend(1);
      outer.UnsafeAppend(3);
      outer.UnsafeAppend(5);
      gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                                outer);
      auto vmap =
          std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
              builder.Seal(client));

      VINEYARD_CHECK_OK(client.Persist(vmap->id()));
      vmap_id = vmap->id();
      LOG(INFO) << "Worker [" << comm_spec.worker_id()
                << "Persist local vmap id: " << vmap->id();
    } else {
      CHECK(inner.Reserve(3).ok());
      CHECK(outer.Reserve(3).ok());
      inner.UnsafeAppend(1);
      inner.UnsafeAppend(3);
      inner.UnsafeAppend(5);
      outer.UnsafeAppend(2);
      outer.UnsafeAppend(4);
      outer.UnsafeAppend(6);
      gs::BasicLocalVertexMapBuilder<int64_t, uint64_t> builder(client, inner,
                                                                outer);
      auto vmap =
          std::dynamic_pointer_cast<gs::LocalVertexMap<int64_t, uint64_t>>(
              builder.Seal(client));

      VINEYARD_CHECK_OK(client.Persist(vmap->id()));
      vmap_id = vmap->id();
      LOG(INFO) << "Worker [" << comm_spec.worker_id()
                << "Persist local vmap id: " << vmap->id();
    }
  }
  return vmap_id;
}

void TestGraphXVertexData(vineyard::Client& client) {
  vineyard::ObjectID id;
  {
    gs::VertexDataBuilder<uint64_t, std::string> builder;
    std::vector<char> buffer;
    std::vector<int32_t> offsets{sizeof(int32_t) * 3, sizeof(int32_t) * 2};
    std::vector<int32_t> values{1, 2, 3, 4, 5};
    buffer.resize(sizeof(int32_t) * 5);
    memcpy(buffer.data(), values.data(), sizeof(int32_t) * 5);
    for (auto i = 0; i < 20; ++i) {
      LOG(INFO) << "ind: " << i << (int) buffer[i];
    }
    builder.Init(2, buffer, offsets);

    auto vd = builder.MySeal(client);
    id = vd->id();
  }

  std::shared_ptr<gs::VertexData<uint64_t, std::string>> vd =
      std::dynamic_pointer_cast<gs::VertexData<uint64_t, std::string>>(
          client.GetObject(id));
  LOG(INFO) << "vnum: " << vd->VerticesNum();
  auto vdArray = vd->GetVdataArray();
  LOG(INFO) << "vd length: " << vdArray.GetLength();
  auto vec = vdArray.GetRawBytes();
  LOG(INFO) << "vd raw bytes: " << vec.size();
  CHECK(vec.size() == 20);
  int32_t* ptr = reinterpret_cast<int32_t*>(vec.data());
  for (auto i = 0; i < 5; ++i) {
    LOG(INFO) << "[" << i << "]" << (*(ptr + i));
  }
}

void TestGraphXEdgeData(vineyard::Client& client) {
  vineyard::ObjectID id;
  {
    gs::EdgeDataBuilder<uint64_t, std::string> builder;
    std::vector<char> buffer;
    std::vector<int32_t> offsets{sizeof(int32_t) * 3, sizeof(int32_t) * 2};
    std::vector<int32_t> values{1, 2, 3, 4, 5};
    buffer.resize(sizeof(int32_t) * 5);
    memcpy(buffer.data(), values.data(), sizeof(int32_t) * 5);
    for (auto i = 0; i < 20; ++i) {
      LOG(INFO) << "ind: " << i << (int) buffer[i];
    }
    builder.Init(2, buffer, offsets);

    auto ed = builder.MySeal(client);
    id = ed->id();
  }

  std::shared_ptr<gs::EdgeData<uint64_t, std::string>> ed =
      std::dynamic_pointer_cast<gs::EdgeData<uint64_t, std::string>>(
          client.GetObject(id));
  LOG(INFO) << "edge_num: " << ed->GetEdgeNum();
  auto edArray = ed->GetEdataArray();
  LOG(INFO) << "ed length: " << edArray.GetLength();
  auto vec = edArray.GetRawBytes();
  LOG(INFO) << "vd raw bytes: " << vec.size();
  CHECK(vec.size() == 20);
  int32_t* ptr = reinterpret_cast<int32_t*>(vec.data());
  for (auto i = 0; i < 5; ++i) {
    LOG(INFO) << "[" << i << "]" << (*(ptr + i));
  }
}

boost::leaf::result<void> generateData(arrow::Int64Builder& srcBuilder,
                                       arrow::Int64Builder& dstBuilder,
                                       arrow::Int64Builder& edataBuilder,
                                       grape::CommSpec& comm_spec) {
  // if (comm_spec.worker_id() == 0) {
  ARROW_OK_OR_RAISE(srcBuilder.Reserve(6));
  ARROW_OK_OR_RAISE(dstBuilder.Reserve(6));
  ARROW_OK_OR_RAISE(edataBuilder.Reserve(6));
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(1);
  srcBuilder.UnsafeAppend(2);
  srcBuilder.UnsafeAppend(3);
  srcBuilder.UnsafeAppend(4);
  srcBuilder.UnsafeAppend(5);

  dstBuilder.UnsafeAppend(2);
  dstBuilder.UnsafeAppend(3);
  dstBuilder.UnsafeAppend(3);
  dstBuilder.UnsafeAppend(4);
  dstBuilder.UnsafeAppend(6);
  dstBuilder.UnsafeAppend(4);

  edataBuilder.UnsafeAppend(1);
  edataBuilder.UnsafeAppend(2);
  edataBuilder.UnsafeAppend(3);
  edataBuilder.UnsafeAppend(4);
  edataBuilder.UnsafeAppend(5);
  edataBuilder.UnsafeAppend(6);
  // } else {
  //   srcBuilder.Reserve(3);
  //   dstBuilder.Reserve(3);
  //   edataBuilder.Reserve(3);
  //   srcBuilder.UnsafeAppend(1);
  //   srcBuilder.UnsafeAppend(3);
  //   srcBuilder.UnsafeAppend(5);
  //   dstBuilder.UnsafeAppend(4);
  //   dstBuilder.UnsafeAppend(6);
  //   dstBuilder.UnsafeAppend(2);
  //   edataBuilder.UnsafeAppend(4);
  //   edataBuilder.UnsafeAppend(5);
  //   edataBuilder.UnsafeAppend(6);
  // }
  return {};
}
void Init() {
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;
  google::InitGoogleLogging("graphx_test");
  google::InstallFailureSignalHandler();
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect("/tmp/vineyard.sock"));
  LOG(INFO) << "Connected to IPCServer: ";
  Init();

  // TestLocalVertexMap(client);
  // auto graphx_vm = TestGraphXVertexMap(client);
  // auto csr_id = TestGraphXCSR(client, graphx_vm);
  TestGraphXVertexData(client);
  TestGraphXEdgeData(client);
  //   TestGraphXFragment(client, graphx_vm.id(), csr_id, vdata_id);
  VLOG(1) << "Finish Querying.";

  grape::FinalizeMPIComm();
  google::ShutdownGoogleLogging();
  return 0;
}
