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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROPERTY_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROPERTY_DEFAULT_CONTEXT_H_

#include <grape/grape.h>
#include <jni.h>

#include <iomanip>
#include <limits>
#include <map>
#include <vector>
//#include "core/context/i_context.h"
#include "boost/algorithm/string/classification.hpp"  // Include boost::for is_any_of
#include "boost/algorithm/string/split.hpp"  // Include for boost::split
#include "boost/property_tree/exceptions.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/config.h"
#include "core/context/java_context_base.h"
#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/parallel/property_message_manager.h"
#include "grape/app/context_base.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"
#define CONTEXT_TYPE_JAVA_PIE_PROPERTY_DEFAULT "java_pie_property_default"
namespace gs {


/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEPropertyDefaultContext : public JavaContextBase<FRAG_T> {
 public:
  JavaPIEPropertyDefaultContext(const FRAG_T& fragment)
      : JavaContextBase<FRAG_T>(fragment) {}
  virtual ~JavaPIEPropertyDefaultContext() {}
  void Init(PropertyMessageManager& messages, const std::string& params) {
    LOG(INFO) << "enter ctx init";
    JNIEnvMark m;
    JNIEnv* env = m.env();
    LOG(INFO) << "before check";
    CHECK_NOTNULL(env);
    LOG(INFO) << "before creat object";
    jobject messagesObject =
        createFFIPointerObject(env, "gs::PropertyMessageManager",
                               reinterpret_cast<jlong>(&messages));
    CHECK_NOTNULL(messagesObject);
    JavaContextBase<FRAG_T>::init(messagesObject, params);
  }

 protected:
  const char* eval_descriptor() override {
    return "(Lio/v6d/modules/graph/fragment/ArrowFragment;"
           "Lio/v6d/modules/graph/parallel/PropertyMessageManager;"
           "Lcom/alibaba/fastjson/JSONObject;)V";
  }
};

// This Wrapper works as a proxy, forward requests like toNdArray, to the c++
// context held by java object.
template <typename FRAG_T>
class JavaPIEPropertyDefaultContextWrapper
    : public IJavaPIEPropertyDefaultContextWrapper {
  using fragment_t = FRAG_T;
  using label_id_t = typename fragment_t::label_id_t;
  using prop_id_t = typename fragment_t::prop_id_t;
  using oid_t = typename fragment_t::oid_t;
  using context_t = JavaPIEPropertyDefaultContext<fragment_t>;
  static_assert(vineyard::is_property_fragment<FRAG_T>::value,
                "JavaPIEPropertyDefaultContextWrapper is only available for "
                "property graph");

 public:
  JavaPIEPropertyDefaultContextWrapper(
      const std::string& id, std::shared_ptr<IFragmentWrapper> frag_wrapper,
      std::shared_ptr<context_t> context)
      : IJavaPIEPropertyDefaultContextWrapper(id),
        frag_wrapper_(std::move(frag_wrapper)),
        ctx_(std::move(context)) {
    // Here we need to construct the ctx_wrapper from the ctx it self.
    // 0. first reinterpret as context
    // 0.1 get the type of java ctx through java function
    std::string java_ctx_type_name =
        get_java_ctx_type_name(ctx_->_context_object);
    LOG(INFO) << "java ctx type name" << java_ctx_type_name;
    std::string ctx_name = "JavaPIEPropertyContext:" + java_ctx_type_name +
                           "@" + std::to_string(ctx_->inner_context_addr());
    LOG(INFO) << "ctx name " << ctx_name;
    // java ctx type name protocol are defined in java, not the same as cpp
    if (java_ctx_type_name == "LabeledVertexDataContext") {
      // Get the DATA_T;
      std::string data_type =
          get_labeled_vertex_data_context_data_type(ctx_->_context_object);
      if (data_type == "double") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, double>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, double>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "uint32_t") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, uint32_t>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, uint32_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else if (data_type == "uint64_t") {
        using inner_ctx_type = LabeledVertexDataContext<FRAG_T, uint64_t>;
        using inner_ctx_wrapper_type =
            LabeledVertexDataContextWrapper<FRAG_T, uint64_t>;
        auto inner_ctx_impl =
            reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
        std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
      } else {
        LOG(FATAL) << "unregonizable data type";
      }
    } else if (java_ctx_type_name == "LabeledVertexPropertyContext") {
      using inner_ctx_type = LabeledVertexPropertyContext<FRAG_T>;
      using inner_ctx_wrapper_type =
          LabeledVertexPropertyContextWrapper<FRAG_T>;
      auto inner_ctx_impl =
          reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
      std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
      _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
          ctx_name, frag_wrapper, inner_ctx_impl_shared);

    }
    // else if (java_ctx_type_name == "VertexPropertyContext") {
    //   using inner_ctx_type = VertexPropertyContext<FRAG_T>;
    //   using inner_ctx_wrapper_type = VertexPropertyContextWrapper<FRAG_T>;
    //   auto inner_ctx_impl =
    //       reinterpret_cast<inner_ctx_type*>(ctx_->inner_context_addr());
    //   std::shared_ptr<inner_ctx_type> inner_ctx_impl_shared(inner_ctx_impl);
    //   _inner_context_wrapper = std::make_shared<inner_ctx_wrapper_type>(
    //       ctx_name, frag_wrapper, inner_ctx_impl_shared);
    // }
    else {
      LOG(FATAL) << "unsupported context type";
    }
    LOG(INFO) << "Construct inner ctx wrapper: "
              << _inner_context_wrapper->context_type() << "," << ctx_name;
  }

  std::string context_type() override {
    // auto _inner_context_wrapper = ctx_->inner_context_wrapper();
    std::string ret = CONTEXT_TYPE_JAVA_PIE_PROPERTY_DEFAULT;
    return ret + ":" + _inner_context_wrapper->context_type();
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }
  // Considering labeledSelector vs selector
  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const std::string& selector_string,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() ==
        CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selector, LabeledSelector::parse(selector_string));
      return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selector, LabeledSelector::parse(selector_string));
      return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    }
    // else if (_inner_context_wrapper->context_type() ==
    //            CONTEXT_TYPE_VERTEX_PROPERTY) {
    //   auto actual_ctx_wrapper =
    //       std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
    //           _inner_context_wrapper);
    //   BOOST_LEAF_AUTO(selector, Selector::parse(selector_string));
    //   return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    // }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec, const std::string& selector_string,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() ==
        CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    }
    // else if (_inner_context_wrapper->context_type() ==
    //            CONTEXT_TYPE_VERTEX_PROPERTY) {
    //   auto actual_ctx_wrapper =
    //       std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
    //           _inner_context_wrapper);
    //   BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(selector_string));
    //   return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    // }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::string& selector_string,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() ==
        CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selector, LabeledSelector::parse(selector_string));
      return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector,
                                                  range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selector, LabeledSelector::parse(selector_string));
      return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector,
                                                  range);
    }
    // else if (_inner_context_wrapper->context_type() ==
    //            CONTEXT_TYPE_VERTEX_PROPERTY) {
    //   auto actual_ctx_wrapper =
    //       std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
    //           _inner_context_wrapper);
    //   BOOST_LEAF_AUTO(selectors, Selector::parse(selector_string));
    //   return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client,
    //   selector,
    //                                               range);
    // }
    return vineyard::InvalidObjectID();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::string& selector_string,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() ==
        CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToVineyardDataframe(comm_spec, client,
                                                     selectors, range);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToVineyardDataframe(comm_spec, client,
                                                     selectors, range);
    }
    return vineyard::InvalidObjectID();
  }

  bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::string& selector_string) override {
    if (_inner_context_wrapper->context_type() ==
        CONTEXT_TYPE_LABELED_VERTEX_DATA) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    } else if (_inner_context_wrapper->context_type() ==
               CONTEXT_TYPE_LABELED_VERTEX_PROPERTY) {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexPropertyContextWrapper>(
              _inner_context_wrapper);
      BOOST_LEAF_AUTO(selectors,
                      LabeledSelector::ParseSelectors(selector_string));
      return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    }
    // else if (_inner_context_wrapper->context_type() ==
    //            CONTEXT_TYPE_VERTEX_PROPERTY) {
    //   auto actual_ctx_wrapper =
    //       std::dynamic_pointer_cast<IVertexPropertyContextWrapper>(
    //           _inner_context_wrapper);
    //   BOOST_LEAF_AUTO(selectors, Selector::ParseSelectors(selector_string));
    //   return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    // }
    std::map<label_id_t,
             std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>
        arrow_arrays;
    return arrow_arrays;
  }

 private:
  std::string get_java_ctx_type_name(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass context_utils_class =
          m.env()->FindClass("io/v6d/modules/graph/utils/ContextUtils");
      if (context_utils_class == NULL) {
        LOG(FATAL) << "context utils clss not found";
      }
      jmethodID ctx_base_class_name_get_method = m.env()->GetStaticMethodID(
          context_utils_class, "getCtxObjBaseClzName",
          "(Lio/v6d/modules/graph/context/PropertyDefaultContextBase;)"
          "Ljava/lang/String;");
      if (ctx_base_class_name_get_method == NULL) {
        LOG(FATAL) << "getCtxObjBaseClzName method null";
      }
      jstring ctx_base_clz_name = (jstring) m.env()->CallStaticObjectMethod(
          context_utils_class, ctx_base_class_name_get_method, ctx_object);
      if (ctx_base_clz_name == NULL) {
        LOG(FATAL) << "The retrived class string null";
      }
      return jstring2string(m.env(), ctx_base_clz_name);
    }
    LOG(FATAL) << "java env not available";
    return NULL;
  }

  std::string get_labeled_vertex_data_context_data_type(
      const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass app_context_getter_class =
          m.env()->FindClass(_app_context_getter_name);
      CHECK_NOTNULL(app_context_getter_class);
      jmethodID getter_method = m.env()->GetStaticMethodID(
          app_context_getter_class, "getLabeledVertexDataContextDataType",
          "(Lio/v6d/modules/graph/context/LabeledVertexDataContext;)"
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

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_PROPERTY_DEFAULT_CONTEXT_H_
