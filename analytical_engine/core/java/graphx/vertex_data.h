
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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H
#define ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H

#include "grape/grape.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/basic/ds/array.h"
#include "vineyard/basic/ds/arrow.h"
#include "vineyard/basic/ds/arrow_utils.h"
#include "vineyard/client/client.h"
#include "vineyard/common/util/functions.h"
#include "vineyard/common/util/typename.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "core/error.h"
#include "core/io/property_parser.h"
#include "core/java/graphx/local_vertex_map.h"
#include "core/java/type_alias.h"

namespace gs {
template <typename VID_T, typename VD_T>
class VertexData : public vineyard::Registered<VertexData<VID_T, VD_T>> {
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using vid_array_t = typename vineyard::ConvertToArrowType<vid_t>::ArrayType;
  using vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;
  using vertex_t = grape::Vertex<VID_T>;

 public:
  VertexData() {}
  ~VertexData() {}
  static std::unique_ptr<vineyard::Object> Create() __attribute__((used)) {
    return std::static_pointer_cast<vineyard::Object>(
        std::unique_ptr<VertexData<VID_T, VD_T>>{
            new VertexData<VID_T, VD_T>()});
  }

  void Construct(const vineyard::ObjectMeta& meta) override {
    this->meta_ = meta;
    this->id_ = meta.GetId();
    this->frag_vnums_ = meta.GetKeyValue<fid_t>("frag_vnums");
    LOG(INFO) << "frag_vnums: " << frag_vnums_;
    vineyard::NumericArray<vdata_t> vineyard_array;
    vineyard_array.Construct(meta.GetMemberMeta("vdatas"));
    vdatas_ = vineyard_array.GetArray();

    CHECK_EQ(vdatas_->length(), frag_vnums_);

    vdatas_accessor_.Init(vdatas_);
    LOG(INFO) << "Finish construct vertex data, frag vnums: " << frag_vnums_;
  }

  vid_t VerticesNum() { return frag_vnums_; }

  VD_T& GetData(const vid_t& lid) {
    CHECK_LT(lid, frag_vnums_);
    return vdatas_accessor_[lid];
  }

  VD_T& GetData(const vertex_t& v) { return GetData(v.GetValue()); }

  void SetData(const vertex_t& v, vdata_t vd) { return vdatas_accessor_.Set(v.GetValue(), vd); }

  graphx::MutableTypedArray<vdata_t>& GetVdataArray() {
    return vdatas_accessor_;
  }

 private:
  vid_t frag_vnums_;
  std::shared_ptr<vdata_array_t> vdatas_;
  graphx::MutableTypedArray<vdata_t> vdatas_accessor_;

  template <typename _VID_T, typename _VD_T>
  friend class VertexDataBuilder;
};

template <typename VID_T, typename VD_T>
class VertexDataBuilder : public vineyard::ObjectBuilder {
  using vid_t = VID_T;
  using vdata_t = VD_T;
  using vdata_array_builder_t =
      typename vineyard::ConvertToArrowType<vdata_t>::BuilderType;
  using vdata_array_t =
      typename vineyard::ConvertToArrowType<vdata_t>::ArrayType;

 public:
  VertexDataBuilder() {}
  ~VertexDataBuilder() {}

  void Init(vid_t frag_vnums, vdata_t initValue) {
    vdata_array_builder_t vdata_builder;
    this->frag_vnums_ = frag_vnums;
    vdata_builder.Reserve(static_cast<int64_t>(frag_vnums_));
    for (size_t i = 0; i < static_cast<size_t>(frag_vnums_); ++i) {
      vdata_builder.UnsafeAppend(initValue);
    }
    vdata_builder.Finish(&(this->vdata_array_));
    LOG(INFO) << "Init vertex data with " << frag_vnums_
              << " vertices, init val : " << initValue;
  }

  void Init(vdata_array_builder_t& vdata_builder) {
    this->frag_vnums_ = vdata_builder.length();
    vdata_builder.Finish(&(this->vdata_array_));
    LOG(INFO) << "Init vertex data with " << frag_vnums_;
  }

  std::shared_ptr<VertexData<vid_t, vdata_t>> MySeal(vineyard::Client& client) {
    return std::dynamic_pointer_cast<VertexData<vid_t, vdata_t>>(
        this->Seal(client));
  }

  std::shared_ptr<vineyard::Object> _Seal(vineyard::Client& client) {
    // ensure the builder hasn't been sealed yet.
    ENSURE_NOT_SEALED(this);
    VINEYARD_CHECK_OK(this->Build(client));
    auto vertex_data = std::make_shared<VertexData<vid_t, vdata_t>>();
    vertex_data->meta_.SetTypeName(type_name<VertexData<vid_t, vdata_t>>());

    size_t nBytes = 0;
    vertex_data->vdatas_ = vineyard_array.GetArray();
    vertex_data->frag_vnums_ = frag_vnums_;
    vertex_data->vdatas_accessor_.Init(vertex_data->vdatas_);
    vertex_data->meta_.AddKeyValue("frag_vnums", frag_vnums_);
    vertex_data->meta_.AddMember("vdatas", vineyard_array.meta());
    nBytes += vineyard_array.nbytes();
    LOG(INFO) << "total bytes: " << nBytes;
    vertex_data->meta_.SetNBytes(nBytes);
    VINEYARD_CHECK_OK(
        client.CreateMetaData(vertex_data->meta_, vertex_data->id_));
    // mark the builder as sealed
    this->set_sealed(true);
    return std::static_pointer_cast<vineyard::Object>(vertex_data);
  }

  vineyard::Status Build(vineyard::Client& client) override {
    typename vineyard::InternalType<vdata_t>::vineyard_builder_type
        vdata_builder(client, this->vdata_array_);
    vineyard_array =
        *std::dynamic_pointer_cast<vineyard::NumericArray<vdata_t>>(
            vdata_builder.Seal(client));
    LOG(INFO) << "Finish building vertex data;";
    return vineyard::Status::OK();
  }

 private:
  vid_t frag_vnums_;
  std::shared_ptr<vdata_array_t> vdata_array_;
  vineyard::NumericArray<vdata_t> vineyard_array;
};
}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_GRAPHX_VERTEX_DATA_H
