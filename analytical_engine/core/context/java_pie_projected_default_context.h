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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROJECTED_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROJECTED_DEFAULT_CONTEXT_H_

#ifdef ENABLE_JAVA_SDK

#include <jni.h>
#include <iomanip>
#include <limits>
#include <map>
#include <vector>

#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"

#include "grape/grape.h"
#include "grape/parallel/default_message_manager.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"

#include "core/config.h"
#include "core/context/java_context_base.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"

#define CONTEXT_TYPE_JAVA_PIE_PROJECTED_DEFAULT "java_pie_projected_default"

namespace gs {

static constexpr const char* _java_projected_message_manager_name =
    "grape::DefaultMessageManager";

/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEProjectedDefaultContext : public JavaContextBase<FRAG_T> {
 public:
  JavaPIEProjectedDefaultContext(const FRAG_T& fragment)
      : JavaContextBase<FRAG_T>(fragment) {}
  virtual ~JavaPIEProjectedDefaultContext() {}

  void Init(grape::DefaultMessageManager& messages, const std::string& params) {
    JavaContextBase<FRAG_T>::init(reinterpret_cast<jlong>(&messages),
                                  _java_projected_message_manager_name, params);
  }

 protected:
  const char* eval_descriptor() override {
    return "(Lcom/alibaba/grape/fragment/ArrowProjectedFragment;"
           "Lcom/alibaba/grape/parallel/DefaultMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
};

template <typename FRAG_T>
class JavaPIEProjectedDefaultContextWrapper
    : public IJavaPIEProjectedDefaultContextWrapper {
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = JavaPIEProjectedDefaultContext<fragment_t>;

 public:
  JavaPIEProjectedDefaultContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> context)
      : IJavaPIEProjectedDefaultContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(context)) {
    std::string java_ctx_type_name =
        get_java_ctx_type_name(ctx_->context_object());
    std::string ctx_name = "JavaPIEProjectedContext:" + java_ctx_type_name +
                           "@" + std::to_string(ctx_->inner_context_addr());
    LOG(INFO) << "ctx name " << ctx_name;
    // java ctx type name protocol are defined in java, not the same as cpp
    if (java_ctx_type_name == "VertexDataContext") {
      // Get the DATA_T;
      std::string data_type =
          get_vertex_data_context_data_type(ctx_->context_object());
      if (data_type == "double") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, double>;
        using inner_ctx_wrapper_type = VertexDataContextWrapper<FRAG_T, double>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "uint32_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, uint32_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, uint32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "int32_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, int32_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, int32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "uint64_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, uint64_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, uint64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "int64_t") {
        using inner_ctx_type = grape::VertexDataContext<FRAG_T, int64_t>;
        using inner_ctx_wrapper_type =
            VertexDataContextWrapper<FRAG_T, int64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else {
        LOG(FATAL) << "unregonizable data type";
      }
    } else if (java_ctx_type_name == "VertexPropertyContext") {
      using inner_ctx_type = VertexPropertyContext<FRAG_T>;
      using inner_ctx_wrapper_type = VertexPropertyContextWrapper<FRAG_T>;
      auto inner_ctx_impl =
          reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
      std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
      _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
          ctx_name, frag_wrapper, inner_ctx_impl_shared);

    } else {
      LOG(FATAL) << "unsupported context type";
    }
    LOG(INFO) << "Construct inner ctx wrapper: "
              << _inner_context_wrapper->context_type() << "," << ctx_name;
  }

  std::string context_type() override {
    std::string ret = CONTEXT_TYPE_JAVA_PIE_PROJECTED_DEFAULT;
    return ret + ":" + _inner_context_wrapper->context_type();
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }
  // Considering labeledSelector vs selector
  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == CONTEXT_TYPE_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == CONTEXT_TYPE_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const Selector& selector,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == CONTEXT_TYPE_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector,
                                                  range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector,
                                                  range);
    }
    return vineyard::InvalidObjectID();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, Selector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == CONTEXT_TYPE_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardDataframe(comm_spec, client,
                                                     selectors, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardDataframe(comm_spec, client,
                                                     selectors, range);
    }
    return vineyard::InvalidObjectID();
  }

  bl::result<std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
  ToArrowArrays(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, Selector>>& selectors) override {
    if (_inner_context_wrapper->context_type() == CONTEXT_TYPE_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    }
    std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>
        arrow_arrays;
    return arrow_arrays;
  }

 private:
  std::string get_java_ctx_type_name(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      // jclass context_utils_class = m.env()->FindClass(CONTEXT_UTILS_CLASS);
      jclass context_utils_class = load_class_with_class_loader(
          m.env(), ctx_->url_class_loader_object(), CONTEXT_UTILS_CLASS);
      CHECK_NOTNULL(context_utils_class);
      jmethodID ctx_base_class_name_get_method = m.env()->GetStaticMethodID(
          context_utils_class, "getProjectedCtxObjBaseClzName",
          "(Lio/graphscope/context/ProjectedDefaultContextBase;)"
          "Ljava/lang/String;");
      CHECK_NOTNULL(ctx_base_class_name_get_method);
      jstring ctx_base_clz_name = (jstring) m.env()->CallStaticObjectMethod(
          context_utils_class, ctx_base_class_name_get_method, ctx_object);
      CHECK_NOTNULL(ctx_base_clz_name);
      return jstring2string(m.env(), ctx_base_clz_name);
    }
    LOG(FATAL) << "java env not available";
    return NULL;
  }

  std::string get_vertex_data_context_data_type(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass app_context_getter_class = load_class_with_class_loader(
          m.env(), ctx_->url_class_loader_object(), APP_CONTEXT_GETTER_CLASS);
      CHECK_NOTNULL(app_context_getter_class);
      jmethodID getter_method = m.env()->GetStaticMethodID(
          app_context_getter_class, "getVertexDataContextDataType",
          "(Lio/graphscope/context/VertexDataContext;)"
          "Ljava/lang/String;");
      CHECK_NOTNULL(getter_method);
      // Pass app class's class object
      jstring context_class_jstring = (jstring) m.env()->CallStaticObjectMethod(
          app_context_getter_class, getter_method, ctx_object);
      CHECK_NOTNULL(context_class_jstring);
      return jstring2string(m.env(), context_class_jstring);
    }
    LOG(FATAL) << "java env not available";
    return NULL;
  }
  std::shared_ptr<IFragmentWrapper> frag_wrapper_;
  std::shared_ptr<context_t> ctx_;
  std::shared_ptr<IContextWrapper> _inner_context_wrapper;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROJECTED_DEFAULT_CONTEXT_H_
