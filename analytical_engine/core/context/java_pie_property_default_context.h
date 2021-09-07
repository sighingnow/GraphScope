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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_

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
#include "core/context/vertex_data_context.h"
#include "core/object/i_fragment_wrapper.h"
#include "core/parallel/property_message_manager.h"
#include "grape/app/context_base.h"
#include "java_pie/column_mananger.h"
#include "java_pie/javasdk.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"
#define CONTEXT_TYPE_JAVA_PIE_PROPERTY_DEFAULT "java_pie_property_default"
namespace gs {

static constexpr const char* _message_manager_name =
    "gs::PropertyMessageManager";
static constexpr const char* _app_context_getter_name =
    "io/v6d/modules/graph/utils/AppContextGetter";
/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEPropertyDefaultContext : public JavaContextBase<FRAG_T> {
 public:
 public:
  using fragment_t = FRAG_T;
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;

  JavaPIEPropertyDefaultContext(const FRAG_T& fragment)
      : _app_class_name(NULL),
        // _context_class_name(NULL),
        _app_object(NULL),
        _context_object(NULL),
        _frag_object(NULL),
        _mm_object(NULL),
        //        inner_ctx_(NULL),
        fragment_(fragment),
        local_num_(1),
        inner_ctx_addr_(0) {}
  const fragment_t& fragment() { return fragment_; }

  // grape instance is killed by SIGINT, which cause vm_direct_exit.
  // when we try to release memery, segmentation fault incurred
  virtual ~JavaPIEPropertyDefaultContext() {
    if (_app_class_name) {
      delete[] _app_class_name;
    }
    //    if (inner_ctx_) {
    //      LOG(INFO) << "releasing inner ctx";
    //      delete inner_ctx_;
    //    }
    // delete[] _context_class_name;
    {
      JNIEnvMark m;
      if (m.env()) {
        LOG(INFO) << "before delete app obj";
        m.env()->DeleteGlobalRef(_app_object);
        LOG(INFO) << "before delete ctx obj";
        m.env()->DeleteGlobalRef(_context_object);
        LOG(INFO) << "before delete frag obj";
        m.env()->DeleteGlobalRef(_frag_object);
        LOG(INFO) << "before delete mm obj";
        m.env()->DeleteGlobalRef(_mm_object);
        LOG(INFO) << "after delete mm obj";
      }
    }

    jint res = GetJavaVM()->DestroyJavaVM();
    LOG(INFO) << "Kill javavm status: " << res;
  }

  void SetLocalNum(int local_num) { local_num_ = local_num; }

  void Init(gs::PropertyMessageManager& messages, const std::string& params) {
    if (params.empty()) {
      LOG(ERROR) << "no args received";
      return;
    }
    std::string user_library_name;
    std::string args_str =
        parse_params_and_setup_jvm_env(params, user_library_name);
    // set java environment variables

    // create jvm instance if not exists;
    JavaVM* jvm = GetJavaVM();
    (void) jvm;
    LOG(INFO) << "successfully get jvm";

    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      // 0. load required jni library
      load_jni_library(env, user_library_name);
      // 1. create app object, and get context class via jni

      jclass app_class = env->FindClass(_app_class_name);
      if (app_class == NULL) {
        LOG(ERROR) << "Cannot find class " << _app_class_name;
        return;
      }
      jobject app_object = createObject(env, app_class, _app_class_name);
      if (app_object != NULL) {
        _app_object = env->NewGlobalRef(app_object);
      } else {
        LOG(ERROR) << "create app object failed for " << _app_class_name;
        return;
      }

      std::string _context_class_name_str =
          get_ctx_class_name_from_app_object(env);
      LOG(INFO) << "context name " << _context_class_name_str;
      // _context_class_name = _context_class_name_str.c_str();
      // The retrived context class str is dash-sperated, convert to /-seperated
      char* _context_class_name_c_str;
      {
        _context_class_name_c_str =
            new char[_context_class_name_str.length() + 1];
        strcpy(_context_class_name_c_str, _context_class_name_str.c_str());
        char* p = _context_class_name_c_str;
        while (*p) {
          if (*p == '.')
            *p = '/';
          p++;
        }
      }
      if (!_context_class_name_c_str) {
        LOG(FATAL) << "get null string after convertion";
      }

      jclass context_class = env->FindClass(_context_class_name_c_str);
      if (context_class == NULL) {
        LOG(ERROR) << "context class not found: " << _context_class_name_str;
        return;
      }

      jobject ctx_object =
          createObject(env, context_class, _context_class_name_c_str);
      if (ctx_object != NULL) {
        _context_object = env->NewGlobalRef(ctx_object);
      } else {
        LOG(ERROR) << "Create context obj failed for context";
        return;
      }

      const char* descriptor =
          "(Lio/v6d/modules/graph/fragment/ArrowFragment;"
          "Lio/v6d/modules/graph/parallel/PropertyMessageManager;"
          "Lcom/alibaba/fastjson/JSONObject;)V";
      jmethodID InitMethodID =
          env->GetMethodID(context_class, "init", descriptor);
      if (InitMethodID == NULL) {
        LOG(ERROR) << "Cannot find method init" << descriptor;
        return;
      }

      jobject fragObject =
          createFFIPointerObject(env, _java_frag_type_name.c_str(),
                                 reinterpret_cast<jlong>(&fragment_));
      if (fragObject == NULL) {
        LOG(ERROR) << "Cannot create fragment Java object";
        return;
      } else {
        _frag_object = env->NewGlobalRef(fragObject);
      }

      // 2. Create Message manager Java object
      jobject messagesObject =
          createFFIPointerObject(env, GetPropertyMessageManagerFFITypeName(),
                                 reinterpret_cast<jlong>(&messages));
      if (messagesObject == NULL) {
        LOG(ERROR) << "Cannot create message manager Java object";
        return;
      } else {
        _mm_object = env->NewGlobalRef(messagesObject);
      }

      // 3. Create arguments array
      {
        jclass json_class = env->FindClass("com/alibaba/fastjson/JSON");
        if (json_class == NULL) {
          LOG(ERROR) << "fastjson class not found";
          return;
        }
        jmethodID parse_method = env->GetStaticMethodID(
            json_class, "parseObject",
            "(Ljava/lang/String;)Lcom/alibaba/fastjson/JSONObject;");
        if (parse_method == NULL) {
          LOG(ERROR) << "parserObjectMethod not found";
          return;
        }
        LOG(INFO) << "user defined kw args: " << args_str;
        jstring args_jstring = env->NewStringUTF(args_str.c_str());
        jobject json_object =
            env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
        if (json_object == NULL) {
          LOG(ERROR) << "json object creation failed";
          return;
        }

        // 4. Invoke java method
        env->CallVoidMethod(_context_object, InitMethodID, _frag_object,
                            _mm_object, json_object);
        if (env->ExceptionOccurred()) {
          LOG(ERROR) << std::string("Exception occurred in calling ctx init");
          env->ExceptionDescribe();
          env->ExceptionClear();
          // env->DeleteLocalRef(main_class);
          LOG(FATAL) << "exiting since exception occurred";
        }
        LOG(INFO) << "invokd ctx init method success";
        // 5. to output the result, we need the c++ context held by java object.
        jfieldID inner_ctx_address_field =
            env->GetFieldID(context_class, "ffiContextAddress", "J");
        if (inner_ctx_address_field == NULL) {
          LOG(FATAL) << "No such field ffiContextAddress";
        }
        LOG(INFO) << "get field success";

        inner_ctx_addr_ =
            env->GetLongField(_context_object, inner_ctx_address_field);

        LOG(INFO) << "innertex ctx address" << inner_ctx_addr_;
        //       inner_ctx_ =
        //       reinterpret_cast<grape::ContextBase*>(inner_ctx_addr_);
        LOG(INFO) << "successfully obtained inner ctx";
      }
    }
  }

  void Output(std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      LOG(INFO) << "enter javapp ctx output";
    }
  }

  const char* app_class_name() const { return _app_class_name; }

  // const char* context_class_name() const { return _context_class_name; }

  //  grape::ContextBase* inner_context() const { return inner_ctx_; }

  uint64_t inner_context_addr() { return inner_ctx_addr_; }

 public:
  char* _app_class_name;
  jobject _app_object;
  jobject _context_object;
  jobject _frag_object;
  jobject _mm_object;

 private:
  bool init_app_class_name(std::string& app_class) {
    if (app_class.empty()) {
      LOG(ERROR) << "Class names for java app is empty";
      return false;
    }
    {
      _app_class_name = new char[app_class.length() + 1];
      strcpy(_app_class_name, app_class.c_str());
      char* p = _app_class_name;
      while (*p) {
        if (*p == '.')
          *p = '/';
        p++;
      }
    }
    return true;
  }
  void load_jni_library(JNIEnv* env, std::string& user_library_name) {
    jclass grape_load_library =
        env->FindClass("com/alibaba/grape/utils/LoadLibrary");
    if (grape_load_library == NULL) {
      LOG(ERROR) << "Cannot find grape jni loader class";
      return;
    }

    jclass vineyard_load_library =
        env->FindClass("io/v6d/modules/graph/utils/LoadLibrary");
    if (vineyard_load_library == NULL) {
      LOG(ERROR) << "Cannot find vineyard jni loader class ";
      return;
    }

    const char* load_library_signature = "(Ljava/lang/String;)V";
    jstring user_library_jstring = env->NewStringUTF(user_library_name.c_str());
    jmethodID grape_load_library_method = env->GetStaticMethodID(
        grape_load_library, "invoke", load_library_signature);
    jmethodID vineyard_load_library_method = env->GetStaticMethodID(
        vineyard_load_library, "invoke", load_library_signature);

    // call static method
    env->CallStaticVoidMethod(grape_load_library, grape_load_library_method,
                              user_library_jstring);
    env->CallStaticVoidMethod(vineyard_load_library,
                              vineyard_load_library_method,
                              user_library_jstring);

    if (env->ExceptionOccurred()) {
      LOG(ERROR) << std::string("Exception occurred in loading user library");
      env->ExceptionDescribe();
      env->ExceptionClear();
      // env->DeleteLocalRef(main_class);
      LOG(FATAL) << "exiting since exception occurred";
    }

    LOG(INFO) << "loaded specified user jni library: " << user_library_name;
  }

  std::string parse_params_and_setup_jvm_env(const std::string& params,
                                             std::string& user_library_name) {
    boost::property_tree::ptree pt;
    std::stringstream ss;
    {
      ss << params;
      try {
        boost::property_tree::read_json(ss, pt);
      } catch (boost::property_tree::ptree_error& r) {
        LOG(FATAL) << "parse json failed: " << params;
      }
    }

    LOG(INFO) << "received json: " << params;
    std::string frag_name = pt.get<std::string>("frag_name");
    if (frag_name.empty()) {
      LOG(FATAL) << "empty frag name";
    }
    LOG(INFO) << "parse frag name: " << frag_name;
    _java_frag_type_name = frag_name;
    pt.erase("frag_name");

    std::string app_class_name = pt.get<std::string>("app_class");
    if (app_class_name.empty()) {
      LOG(FATAL) << "empty app class name";
    }
    LOG(INFO) << "parse app class name: " << app_class_name;
    if (!init_app_class_name(app_class_name)) {
      LOG(FATAL) << "Init app class name failed:" << app_class_name;
    }
    pt.erase("app_class");

    user_library_name = pt.get<std::string>("user_library_name");
    if (user_library_name.empty()) {
      LOG(FATAL) << "empty user library name";
    }
    LOG(INFO) << "user library name " << user_library_name;
    pt.erase("user_library_name");

    // JVM runtime opt should consists of java.libaray.path and
    // java.class.path maybe this should be set by the backend not user.
    std::string jvm_runtime_opt = pt.get<std::string>("jvm_runtime_opt");
    // put the cp and library.path in env
    if (setenv("JVM_OPTS", jvm_runtime_opt.c_str(), 1) == 0) {
      LOG(INFO) << " successfully set jvm opts to: " << jvm_runtime_opt;
    } else {
      LOG(ERROR) << " failed to set jvm opts";
    }
    SetupEnv(local_num_);
    pt.erase("jvm_runtime_opt");
    // extract the rest params, pack them as a json object.
    ss.str("");  // reset the stream buffer
    boost::property_tree::json_parser::write_json(ss, pt);

    return ss.str();
  }

  const char* GetPropertyMessageManagerFFITypeName() {
    return _message_manager_name;
  }

  std::string get_ctx_class_name_from_app_object(JNIEnv* env) {
    // get app_class's class object
    jclass app_class_class = env->GetObjectClass(_app_object);
    if (app_class_class == NULL) {
      LOG(FATAL) << "Cannot find object class ";
    }
    jmethodID app_class_getClass_method =
        env->GetMethodID(app_class_class, "getClass", "()Ljava/lang/Class;");
    if (app_class_getClass_method == NULL) {
      LOG(FATAL) << "no get class method ";
    }
    jobject app_class_obj =
        env->CallObjectMethod(_app_object, app_class_getClass_method);
    if (app_class_obj == NULL) {
      LOG(FATAL) << "app class obj ";
    }
    // the app's corresponding ctx name
    // jstring _app_context_getter_name_jstring =
    //     env->NewStringUTF(_app_context_getter_name);
    jclass app_context_getter_class = env->FindClass(_app_context_getter_name);
    if (app_context_getter_class == NULL) {
      LOG(FATAL) << "app get ContextClass not found";
      return NULL;
    }
    jmethodID app_context_getter_method = env->GetStaticMethodID(
        app_context_getter_class, "getPropertyDefaultContextName",
        "(Ljava/lang/Class;)Ljava/lang/String;");
    if (app_context_getter_method == NULL) {
      LOG(FATAL) << "appcontextclass getter method null";
      return NULL;
    }
    // Pass app class's class object
    jstring context_class_jstring = (jstring) env->CallStaticObjectMethod(
        app_context_getter_class, app_context_getter_method, app_class_obj);
    if (context_class_jstring == NULL) {
      LOG(FATAL) << "The retrived class string null";
      return NULL;
    }
    return jstring2string(env, context_class_jstring);
  }
  // char* _context_class_name;
  std::string _java_frag_type_name;
  // grape::ContextBase* inner_ctx_;
  const fragment_t& fragment_;
  int local_num_;
  uint64_t inner_ctx_addr_;
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
    std::string ctx_name = "JavaPIEContext:" + java_ctx_type_name + "@" +
                           std::to_string(ctx_->inner_context_addr());
    LOG(INFO) << "ctx name " << ctx_name;
    if (java_ctx_type_name == "LabeledVertexDataContext") {
      // Get the DATA_T;
      std::string data_type =
          get_vertex_data_context_data_type(ctx_->_context_object);
      if (data_type == "double") {
        auto inner_ctx_impl =
            reinterpret_cast<gs::LabeledVertexDataContext<FRAG_T, double>*>(
                ctx_->inner_context_addr());
        std::shared_ptr<gs::LabeledVertexDataContext<FRAG_T, double>>
            inner_ctx_impl_shared(inner_ctx_impl);
        _inner_context_wrapper = std::make_shared<
            gs::LabeledVertexDataContextWrapper<FRAG_T, double>>(
            ctx_name, frag_wrapper, inner_ctx_impl_shared);
        LOG(INFO) << "construct inner ctx wrapper: "
                  << _inner_context_wrapper->context_type() << "," << ctx_name;
      } else {
        LOG(FATAL) << "unregonizable data type";
      }
    } else {
      LOG(FATAL) << "unsupported context type";
    }
  }

  std::string context_type() override {
    // auto _inner_context_wrapper = ctx_->inner_context_wrapper();
    // std::string ret = CONTEXT_TYPE_JAVA_PIE_PROPERTY_DEFAULT;
    // return ret + ":" + _inner_context_wrapper->context_type();
    return CONTEXT_TYPE_JAVA_PIE_PROPERTY_DEFAULT + ":" +
           _inner_context_wrapper->context_type();
  }

  std::shared_ptr<IFragmentWrapper> fragment_wrapper() override {
    return frag_wrapper_;
  }
  // Considering labeledSelector vs selector
  bl::result<std::unique_ptr<grape::InArchive>> ToNdArray(
      const grape::CommSpec& comm_spec, const std::string& selector_string,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == "labeled_vertex_data") {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      auto selector = LabeledSelector::parse(selector_string);
      return actual_ctx_wrapper->ToNdArray(comm_spec, selector, range);
    }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<std::unique_ptr<grape::InArchive>> ToDataframe(
      const grape::CommSpec& comm_spec,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == "labeled_vertex_data") {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToDataframe(comm_spec, selectors, range);
    }
    return std::make_unique<grape::InArchive>();
  }

  bl::result<vineyard::ObjectID> ToVineyardTensor(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const LabeledSelector& selector,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == "labeled_vertex_data") {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardTensor(comm_spec, client, selector,
                                                  range);
    }
    return vineyard::InvalidObjectID();
  }

  bl::result<vineyard::ObjectID> ToVineyardDataframe(
      const grape::CommSpec& comm_spec, vineyard::Client& client,
      const std::vector<std::pair<std::string, LabeledSelector>>& selectors,
      const std::pair<std::string, std::string>& range) override {
    if (_inner_context_wrapper->context_type() == "labeled_vertex_data") {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToVineyardDataframe(comm_spec, client,
                                                     selectors, range);
    }
    return vineyard::InvalidObjectID();
  }

  bl::result<std::map<
      label_id_t,
      std::vector<std::pair<std::string, std::shared_ptr<arrow::Array>>>>>
  ToArrowArrays(const grape::CommSpec& comm_spec,
                const std::vector<std::pair<std::string, LabeledSelector>>&
                    selectors) override {
    if (_inner_context_wrapper->context_type() == "labeled_vertex_data") {
      auto actual_ctx_wrapper =
          std::dynamic_pointer_cast<ILabeledVertexDataContextWrapper>(
              _inner_context_wrapper);
      return actual_ctx_wrapper->ToArrowArrays(comm_spec, selectors);
    }
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

  std::string get_vertex_data_context_data_type(const jobject& ctx_object) {
    JNIEnvMark m;
    if (m.env()) {
      jclass app_context_getter_class =
          m.env()->FindClass(_app_context_getter_name);
      if (app_context_getter_class == NULL) {
        LOG(FATAL) << "app get ContextClass not found";
      }
      jmethodID getter_method = m.env()->GetStaticMethodID(
          app_context_getter_class, "getVertexDataContextDataType",
          "(Lio/v6d/modules/graph/context/LabeledVertexDataContext;)Ljava/lang/"
          "String;");
      if (getter_method == NULL) {
        LOG(FATAL) << "getVertexDataContextDataType method null";
      }
      // Pass app class's class object
      jstring context_class_jstring = (jstring) m.env()->CallStaticObjectMethod(
          app_context_getter_class, getter_method, ctx_object);
      if (context_class_jstring == NULL) {
        LOG(FATAL) << "The retrived class string null";
      }
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

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
