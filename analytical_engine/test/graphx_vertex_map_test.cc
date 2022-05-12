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

#include "grape/grape.h"

#include "arrow/array.h"
#include "arrow/array/builder_primitive.h"
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/graphx_vertex_map.h"
#include "core/java/graphx/local_vertex_map.h"
#include "glog/logging.h"
#include "vineyard/client/client.h"

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
void TestGraphXVertexMap(vineyard::Client& client) {
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
  if (comm_spec.worker_num() != 2) {
    LOG(ERROR) << "Expect worker num == 2";
    return;
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
            << " total vnum: "<< vm->GetTotalVertexSize();
  uint64_t gid;
  vm->GetGid(1, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 1: " << gid;
  vm->GetGid(2, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 2: " << gid;
  vm->GetGid(3, gid);
  LOG(INFO) << "worker " << comm_spec.worker_id() << "oid2 gid: 3: " << gid;
}
void Init(){
  grape::InitMPIComm();
  grape::CommSpec comm_spec;
  comm_spec.Init(MPI_COMM_WORLD);
}

int main(int argc, char* argv[]) {
  FLAGS_stderrthreshold = 0;
  google::InitGoogleLogging("graphx_vertex_map_test");
  google::InstallFailureSignalHandler();
  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect("/tmp/vineyard.sock"));
  LOG(INFO) << "Connected to IPCServer: ";
  Init();

  TestGraphXVertexMap(client);
  VLOG(1) << "Finish Querying.";

  google::ShutdownGoogleLogging();
  grape::FinalizeMPIComm();
  VLOG(1) << "Workers finalized.";
  return 0;
}
