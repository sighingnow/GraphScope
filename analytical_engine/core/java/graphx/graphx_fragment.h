
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H

#define WITH_PROFILING

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "flat_hash_map/flat_hash_map.hpp"

#include "grape/grape.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/basic/ds/hashmap.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"
#include "vineyard/graph/utils/error.h"
#include "vineyard/graph/utils/table_shuffler.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/graphx/graphx_vertex_map.h"
#include "core/java/graphx/graphx_csr.h"
#include "core/java/graphx/vertex_data.h"
#include "core/java/type_alias.h"

/**
 * @brief only stores local vertex mapping, construct global vertex map in mpi
 *
 */
namespace gs {

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXFragment
    : public vineyard::Registered<GraphXFragment<OID_T, VID_T, VD_T, ED_T>> {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;
  using csr_t = GraphXCSR<VID_T, ED_T>;
  using vm_t = GraphXVertexMap<OID_T, VID_T>;
  using graphx_vdata_t = VertexData<VID_T, VD_T>;

 public:
  GraphXFragment() {}
  ~GraphXFragment() {}

  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<GraphXFragment<OID_T, VID_T,VD_T,ED_T>>{
            new GraphXFragment<OID_T, VID_T, VD_T, ED_T>()});
  }

  void PrepareToRunApp(const grape::CommSpec& comm_spec,
                       grape::PrepareConf conf) {
    LOG(INFO) << "no preparation";
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();

    this->fnum_ = meta.GetKeyValue<fid_t>("fnum");
    this->fid_ = meta.GetKeyValue<fid_t>("fid");

    this->csr_.Construct(meta.GetMemberMeta("csr"));
    this->vm_.Construct(meta.GetMemberMeta("vm"));
    this->vdata_.Construct(meta.GetMemberMeta("vdata"));
    CHECK_EQ(vm_.GetVertexSize(), vdata_.VerticesNum());
    LOG(INFO) << "GraphXFragment finish construction : " << fid_;
  }
  fid_t fid() { return fid_; }
  fid_t fnum() { return fnum_; }

 private:
  grape::fid_t fnum_, fid_;
  csr_t csr_;
  vm_t vm_;
  graphx_vdata_t vdata_;

  template <typename _OID_T, typename _VID_T, typename _VD_T, typename _ED_T>
  friend class GraphXFragmentBuilder;
};

template <typename OID_T, typename VID_T, typename VD_T, typename ED_T>
class GraphXFragmentBuilder : public vineyard::ObjectBuilder {
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using edata_t = ED_T;
  using csr_t = GraphXCSR<VID_T, ED_T>;
  using vm_t = GraphXVertexMap<OID_T, VID_T>;
  using graphx_vdata_t = VertexData<VID_T, VD_T>;

 public:
  explicit GraphXFragmentBuilder(vineyard::Client& client,
                                 GraphXVertexMap<OID_T, VID_T>& vm,
                                 GraphXCSR<VID_T, ED_T>& csr,
                                 VertexData<VID_T, VD_T>& vdata)
      : client_(client) {
    fid_ = vm.fid();
    fnum_ = vm.fnum();
    vm_ = vm;
    csr_ = csr;
    vdata_ = vdata;
  };

  explicit GraphXFragmentBuilder(vineyard::Client& client,
                                 vineyard::ObjectID vm_id,
                                 vineyard::ObjectID csr_id,
                                 vineyard::ObjectID vdata_id)
      : client_(client) {
    vm_ = *std::dynamic_pointer_cast<vm_t>(client.GetObject(vm_id));
    fid_ = vm_.fid();
    fnum_ = vm_.fnum();
    csr_ = *std::dynamic_pointer_cast<csr_t>(client.GetObject(csr_id));
    vdata_ = *std::dynamic_pointer_cast<graphx_vdata_t>(client.GetObject(vdata_id));
  };

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));

    auto fragment =
        std::make_shared<GraphXFragment<oid_t, vid_t, vdata_t, edata_t>>();
    fragment->meta_.SetTypeName(
        type_name<GraphXFragment<oid_t, vid_t, vdata_t, edata_t>>());

    fragment->fid_ = fid_;
    fragment->fnum_ = fnum_;
    fragment->csr_ = csr_;
    fragment->vm_ = vm_;
    fragment->vdata_ = vdata_;

    fragment->meta_.AddKeyValue("fid", fid_);
    fragment->meta_.AddKeyValue("fnum", fnum_);
    fragment->meta_.AddMember("vdatas", vdata_.meta());
    fragment->meta_.AddMember("csr", csr_.meta());
    fragment->meta_.AddMember("vm", vm_.meta());

    size_t nBytes = 0;
    nBytes += vdata_.nbytes();
    nBytes += csr_.nbytes();
    nBytes += vm_.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;
    fragment->meta_.SetNBytes(nBytes);

    VINEYARD_CHECK_OK(client.CreateMetaData(fragment->meta_, fragment->id_));
    // mark the builder as sealed
    this->set_sealed(true);

    return std::static_pointer_cast<vineyard::Object>(fragment);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    LOG(INFO) << "no need for build";
    return vineyard::Status::OK();
  }

 private:
  grape::fid_t fnum_, fid_;
  csr_t csr_;
  vm_t vm_;
  graphx_vdata_t vdata_;
  vineyard::Client& client_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_GRAPHX_FRAGMENT_H
