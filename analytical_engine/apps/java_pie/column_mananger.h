#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_COLUMN_MANAGER_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_COLUMN_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grape/app/context_base.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/context/column.h"
#include "core/context/i_context.h"
#include "core/context/tensor_dataframe_builder.h"
#include "core/utils/transform_utils.h"
// #define CONTEXT_TYPE_LABELED_VERTEX_PROPERTY "labeled_vertex_property"

namespace gs {

/**
 * @brief LabeledVertexPropertyContext can hold any number of columns. The
 * context is designed for labeled fragment - ArrowFragment. Compared with
 * LabeledVertexDataContext, the data type and column count can be determined at
 * runtime.
 *
 * @tparam FRAG_T The fragment class (labeled fragment only)
 */
template <typename FRAG_T>
class ColumnManager {
 public:
  using fragment_t = FRAG_T;
  using vertex_t = typename fragment_t::vertex_t;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;

  explicit ColumnManager(const fragment_t& fragment) : fragment_(fragment) {
    auto label_num = fragment.vertex_label_num();
    vertex_properties_.resize(label_num);
    properties_map_.resize(label_num);
  }

  const fragment_t& fragment() { return fragment_; }

  std::string get_name() { return std::string(name); }

  template <typename DATA_T>
  int64_t add_column(label_id_t label, const std::string& name) {
    if (static_cast<size_t>(label) >= properties_map_.size()) {
      return -1;
    }
    auto& map = properties_map_[label];
    if (map.find(name) != map.end()) {
      return -1;
    }
    // auto column =
    //     CreateColumn<fragment_t>(name, fragment_.InnerVertices(label), type);
    auto column =
        template MyCreateColumn<DATA_T>(name, fragment_.InnerVertices(label));
    map.emplace(name, column);
    auto& vec = vertex_properties_[label];
    auto ret = static_cast<int64_t>(vec.size());
    vec.emplace_back(column);
    return ret;
  }

  std::shared_ptr<IColumn> get_column(label_id_t label, int64_t index) {
    if (label >= vertex_properties_.size()) {
      return nullptr;
    }
    auto& vec = vertex_properties_[label];
    if (static_cast<size_t>(index) > vec.size()) {
      return nullptr;
    }
    return vec[index];
  }

  std::shared_ptr<IColumn> get_column(label_id_t label,
                                      const std::string& name) {
    if (label >= properties_map_.size()) {
      return nullptr;
    }
    auto& map = properties_map_[label];
    auto iter = map.find(name);
    if (iter == map.end()) {
      return nullptr;
    }
    return iter->second;
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(label_id_t label,
                                                               int64_t index) {
    if (static_cast<size_t>(label) >= vertex_properties_.size()) {
      return nullptr;
    }
    auto& vec = vertex_properties_[label];
    if (static_cast<size_t>(index) > vec.size()) {
      return nullptr;
    }
    auto ret = vec[index];
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  template <typename DATA_T>
  std::shared_ptr<Column<fragment_t, DATA_T>> get_typed_column(
      label_id_t label, const std::string& name) {
    if (label >= properties_map_.size()) {
      return nullptr;
    }
    auto& map = properties_map_[label];
    auto iter = map.find(name);
    if (iter == map.end()) {
      return nullptr;
    }
    auto ret = iter->second;
    if (ret->type() != ContextTypeToEnum<DATA_T>::value) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Column<fragment_t, DATA_T>>(ret);
  }

  std::vector<std::vector<std::shared_ptr<IColumn>>>& vertex_properties() {
    return vertex_properties_;
  }

  std::vector<std::map<std::string, std::shared_ptr<IColumn>>>&
  properties_map() {
    return properties_map_;
  }

 private:
  template <typename DATA_T>
  std::shared_ptr<IColumn> MyCreateColumn(
      const std::string& name, typename FRAG_T::vertex_range_t range, ) {
    return std::make_shared<Column<FRAG_T, DATA_T>>(name, range);
    // if (type == ContextDataType::kInt32) {
    //   return std::make_shared<Column<FRAG_T, int32_t>>(name, range);
    // } else if (type == ContextDataType::kInt64) {
    //   return std::make_shared<Column<FRAG_T, int64_t>>(name, range);
    // } else if (type == ContextDataType::kUInt32) {
    //   return std::make_shared<Column<FRAG_T, uint32_t>>(name, range);
    // } else if (type == ContextDataType::kUInt64) {
    //   return std::make_shared<Column<FRAG_T, uint64_t>>(name, range);
    // } else if (type == ContextDataType::kFloat) {
    //   return std::make_shared<Column<FRAG_T, float>>(name, range);
    // } else if (type == ContextDataType::kDouble) {
    //   return std::make_shared<Column<FRAG_T, double>>(name, range);
    // } else if (type == ContextDataType::kString) {
    //   return std::make_shared<Column<FRAG_T, std::string>>(name, range);
    // } else {
    //   return nullptr;
    // }
  }
  const fragment_t& fragment_;
  std::vector<std::vector<std::shared_ptr<IColumn>>> vertex_properties_;
  std::vector<std::map<std::string, std::shared_ptr<IColumn>>> properties_map_;
  static constexpr const char* name = "gs::ColumnManager";
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_COLUMN_MANAGER_H_