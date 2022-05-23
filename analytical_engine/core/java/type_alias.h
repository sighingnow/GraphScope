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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_

#ifdef ENABLE_JAVA_SDK

#include "grape/graph/immutable_csr.h"
#include "grape/utils/vertex_array.h"
#include "vineyard/graph/fragment/arrow_fragment.h"
#include "vineyard/graph/fragment/property_graph_types.h"
#include "vineyard/graph/fragment/property_graph_utils.h"

#include "core/context/column.h"
#include "core/fragment/arrow_projected_fragment.h"
#include "core/loader/arrow_fragment_loader.h"

// Type alias for ease of use of some template types in Java.
namespace gs {

using ArrowFragmentLoaderDefault =
    ArrowFragmentLoader<vineyard::property_graph_types::OID_TYPE,
                        vineyard::property_graph_types::VID_TYPE>;

namespace arrow_projected_fragment_impl {

template <typename VID_T, typename EDATA_T>
using NbrDefault =
    Nbr<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;

template <typename VID_T, typename EDATA_T>
using AdjListDefault =
    AdjList<VID_T, vineyard::property_graph_types::EID_TYPE, EDATA_T>;
}  // namespace arrow_projected_fragment_impl

// vineyard property graph utils
template <typename VID_T>
using NbrUnitDefault = vineyard::property_graph_utils::NbrUnit<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename VID_T>
using NbrDefault = vineyard::property_graph_utils::Nbr<
    VID_T, vineyard::property_graph_types::VID_TYPE>;

template <typename VID_T>
using RawAdjListDefault = vineyard::property_graph_utils::RawAdjList<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename VID_T>
using AdjListDefault = vineyard::property_graph_utils::AdjList<
    VID_T, vineyard::property_graph_types::EID_TYPE>;

template <typename DATA_T>
using EdgeDataColumnDefault = vineyard::property_graph_utils::EdgeDataColumn<
    DATA_T, NbrUnitDefault<vineyard::property_graph_types::VID_TYPE>>;

template <typename DATA_T>
using VertexDataColumnDefault =
    vineyard::property_graph_utils::VertexDataColumn<
        DATA_T, vineyard::property_graph_types::VID_TYPE>;

template <typename OID_T>
using ArrowFragmentDefault = vineyard::ArrowFragment<OID_T, uint64_t>;

template <typename DATA_T>
using VertexArrayDefault =
    grape::VertexArray<grape::VertexRange<uint64_t>, DATA_T>;

template <typename VID_T, typename DATA_T>
using JavaVertexArray = grape::VertexArray<grape::VertexRange<VID_T>, DATA_T>;

template <typename FRAG_T>
using DoubleColumn = Column<FRAG_T, double>;

template <typename FRAG_T>
using LongColumn = Column<FRAG_T, uint64_t>;

template <typename FRAG_T>
using IntColumn = Column<FRAG_T, uint32_t>;

template <typename VID_T, typename ED_T>
using DefaultImmutableCSR = grape::ImmutableCSR<VID_T, grape::Nbr<VID_T, ED_T>>;

template <typename T>
using ArrowArrayBuilder = typename vineyard::ConvertToArrowType<T>::BuilderType;

template <typename T>
using ArrowArray = typename vineyard::ConvertToArrowType<T>::ArrayType;

template <typename T>
struct TypeName {
  static std::string Get() { return "NULL"; }
};

// a specialization for each type of those you want to support
// and don't like the string returned by typeid
template <>
struct TypeName<int32_t> {
  static std::string Get() { return "int32_t"; }
};
template <>
struct TypeName<int64_t> {
  static std::string Get() { return "int64_t"; }
};
template <>
struct TypeName<double> {
  static std::string Get() { return "double"; }
};
template <>
struct TypeName<uint32_t> {
  static std::string Get() { return "uint32_t"; }
};
template <>
struct TypeName<uint64_t> {
  static std::string Get() { return "uint64_t"; }
};

namespace graphx {
template <typename T>
class ImmutableTypedArray {
 public:
  using value_type = T;
  ImmutableTypedArray() : buffer_(NULL), length(0) {}
  explicit ImmutableTypedArray(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
      length = 0;
    } else {
      auto const_buffer_ =
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
              ->raw_values();
      buffer_ = const_cast<T*>(const_buffer_);
      length = array->length();
    }
  }

  void Init(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      buffer_ = NULL;
      length = 0;
    } else {
      auto const_buffer_ =
          std::dynamic_pointer_cast<
              typename vineyard::ConvertToArrowType<T>::ArrayType>(array)
              ->raw_values();
      buffer_ = const_cast<T*>(const_buffer_);
      length = array->length();
    }
  }

  value_type& operator[](size_t loc) const { return buffer_[loc]; }

  value_type Get(size_t loc) const { return buffer_[loc]; }

  // void Set(size_t loc, value_type newValue) { buffer_[loc] = newValue; }
  size_t GetLength() const { return length; }

 private:
  T* buffer_;
  size_t length;
};
template <>
struct ImmutableTypedArray<std::string> {
 public:
  using value_type = arrow::util::string_view;
  TypedArray() : array_(NULL) {}
  explicit TypedArray(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      array_ = NULL;
    } else {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array).get();
    }
  }

  void Init(std::shared_ptr<arrow::Array> array) {
    if (array == nullptr) {
      array_ = NULL;
    } else {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array).get();
    }
  }

  value_type operator[](size_t loc) const { return array_->GetView(loc); }

 private:
  arrow::LargeStringArray* array_;
};
}  // namespace graphx
}  // namespace gs

#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_TYPE_ALIAS_H_
