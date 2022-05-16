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
#include "core/java/graphx/graphx_fragment.h"
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/graphx/vertex_data.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

void generateData(arrow::Int64Builder& srcBuilder,
                  arrow::Int64Builder& dstBuilder,
                  arrow::Int64Builder& edataBuilder,
                  grape::CommSpec& comm_spec);

vineyard::ObjectID getLocalVM(vineyard::Client& client,
                              grape::CommSpec& comm_spec) {
  vineyard::ObjectID vmap_id;
  {
    arrow::Int64Builder inner, outer;
    if (comm_spec.worker_id() == 0) {
      inner.Reserve(2);
      outer.Reserve(1);
      inner.UnsafeAppend(1);
      inner.UnsafeAppend(2);
      outer.UnsafeAppend(3);
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
      inner.Reserve(1);
      outer.Reserve(2);
      inner.UnsafeAppend(3);
      outer.UnsafeAppend(2);
      outer.UnsafeAppend(1);
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
gs::GraphXVertexMap<int64_t, uint64_t> TestGraphXVertexMap(
    vineyard::Client& client) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_num() != 2) {
    LOG(FATAL) << "Expect worker num == 2";
  }
  vineyard::ObjectID vm_id;
  {
    vineyard::ObjectID partial_map = getLocalVM(client, comm_spec);
    LOG(INFO) << "Worker: " << comm_spec.worker_id()
              << " local vm: " << partial_map;
    gs::BasicGraphXVertexMapBuilder<int64_t, uint64_t> builder(
        client, comm_spec, partial_map);
    auto graphx_vm =
        std::dynamic_pointer_cast<gs::GraphXVertexMap<int64_t, uint64_t>>(
            builder.Seal(client));

    VINEYARD_CHECK_OK(client.Persist(graphx_vm->id()));
    vm_id = graphx_vm->id();
    LOG(INFO) << "Persist csr id: " << graphx_vm->id();
  }
  std::shared_ptr<gs::GraphXVertexMap<int64_t, uint64_t>> vm =
      std::dynamic_pointer_cast<gs::GraphXVertexMap<int64_t, uint64_t>>(
          client.GetObject(vm_id));
  LOG(INFO) << "worker " << comm_spec.worker_id() << " Got graphx vm "
            << vm->id();
  LOG(INFO) << "worker " << comm_spec.worker_id()
            << " total vnum: " << vm->GetTotalVertexSize();
  uint64_t gid;
  vm->GetGid(1, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 1: " << gid;
  vm->GetGid(2, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 2: " << gid;
  vm->GetGid(3, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 3: " << gid;
  return *vm;
}

vineyard::ObjectID TestGraphXCSR(
    vineyard::Client& client,
    gs::GraphXVertexMap<int64_t, uint64_t>& graphx_vm) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  vineyard::ObjectID csr_id;
  {
    arrow::Int64Builder srcBuilder, dstBuilder;
    arrow::Int64Builder edataBuilder;
    generateData(srcBuilder, dstBuilder, edataBuilder, comm_spec);

    gs::BasicGraphXCSRBuilder<int64_t, uint64_t, int64_t> builder(client);
    builder.LoadEdges(srcBuilder, dstBuilder, edataBuilder, graphx_vm);
    auto csr = std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t, int64_t>>(
        builder.Seal(client));

    // VINEYARD_CHECK_OK(client.Persist(csr->id()));
    csr_id = csr->id();
    LOG(INFO) << "Persist csr id: " << csr->id();
  }
  std::shared_ptr<gs::GraphXCSR<uint64_t, int64_t>> csr =
      std::dynamic_pointer_cast<gs::GraphXCSR<uint64_t, int64_t>>(
          client.GetObject(csr_id));
  LOG(INFO) << "Got csr " << csr->id();
  LOG(INFO) << "num edges: " << csr->GetTotalEdgesNum() << " vs "
            << csr->GetPartialEdgesNum(
                   0, graphx_vm.GetInnerVertexSize(comm_spec.fid()));
  LOG(INFO) << "lid 0 degreee: " << csr->GetDegree(0) << ", "
            << csr->GetPartialEdgesNum(0, 1);
  return csr->id();
}

vineyard::ObjectID TestGraphXVertexData(vineyard::Client& client) {
  vineyard::ObjectID id;
  {
    gs::VertexDataBuilder<uint64_t, int64_t> builder;
    builder.Init(3, 2);
    auto vd = builder.MySeal(client);
    id = vd->id();
  }

  std::shared_ptr<gs::VertexData<uint64_t, int64_t>> vd =
      std::dynamic_pointer_cast<gs::VertexData<uint64_t, int64_t>>(
          client.GetObject(id));
  LOG(INFO) << "vnum: " << vd->VerticesNum();
  LOG(INFO) << "vdata : " << vd->GetData(0);
  grape::Vertex<uint64_t> v;
  v.SetValue(0);
  LOG(INFO) <<"set vdata: ";
   vd->SetData(v,1);
  LOG(INFO) << "vdata : " << vd->GetData(0);
  return vd->id();
}

void TestGraphXFragment(vineyard::Client& client, vineyard::ObjectID vm_id,
                        vineyard::ObjectID csr_id,
                        vineyard::ObjectID vdata_id) {
  gs::GraphXFragmentBuilder<int64_t, uint64_t, int64_t, int64_t> builder(
      client, vm_id, csr_id, vdata_id);
  auto res = std::dynamic_pointer_cast<
      gs::GraphXFragment<int64_t, uint64_t, int64_t, int64_t>>(
      builder.Seal(client));
  LOG(INFO) << "Succesfully construct fragment: " << res->id();
}

void generateData(arrow::Int64Builder& srcBuilder,
                  arrow::Int64Builder& dstBuilder,
                  arrow::Int64Builder& edataBuilder,
                  grape::CommSpec& comm_spec) {
  if (comm_spec.worker_id() == 0) {
    srcBuilder.Reserve(2);
    dstBuilder.Reserve(2);
    edataBuilder.Reserve(2);
    srcBuilder.UnsafeAppend(2);
    srcBuilder.UnsafeAppend(1);

    dstBuilder.UnsafeAppend(1);
    dstBuilder.UnsafeAppend(3);

    edataBuilder.UnsafeAppend(1);
    edataBuilder.UnsafeAppend(2);
  } else {
    srcBuilder.Reserve(1);
    dstBuilder.Reserve(1);
    edataBuilder.Reserve(1);
    srcBuilder.UnsafeAppend(3);
    dstBuilder.UnsafeAppend(2);
    edataBuilder.UnsafeAppend(3);
  }
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
  auto graphx_vm = TestGraphXVertexMap(client);
  auto csr_id = TestGraphXCSR(client, graphx_vm);
  auto vdata_id = TestGraphXVertexData(client);
  TestGraphXFragment(client, graphx_vm.id(), csr_id, vdata_id);
  VLOG(1) << "Finish Querying.";

  grape::FinalizeMPIComm();
  google::ShutdownGoogleLogging();
  return 0;
}
