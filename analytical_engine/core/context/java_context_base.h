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

#ifndef ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_
#define ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_

#include <jni.h>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <vector>

#include <grape/grape.h>
#include "grape/app/context_base.h"
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
namespace gs {
static constexpr const char* _app_context_getter_name =
    "io/v6d/modules/graph/utils/AppContextGetter";
/**
 * @brief ContextBase is the base class for all user-defined contexts. A
 * context manages data through the whole computation. The data won't be cleared
 * during supersteps.
 *
 */
template <typename FRAG_T>
class JavaContextBase : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;

  JavaContextBase(const FRAG_T& fragment)
      : _app_class_name(NULL),
        _app_object(NULL),
        _context_object(NULL),
        _frag_object(NULL),
        _mm_object(NULL),
        fragment_(fragment),
        local_num_(1),
        inner_ctx_addr_(0) {}

  virtual ~JavaContextBase() {
    if (_app_class_name) {
      delete[] _app_class_name;
    }
    jint res = GetJavaVM()->DestroyJavaVM();
    LOG(INFO) << "Kill javavm status: " << res;
  }
  const fragment_t& fragment() { return fragment_; }

  void SetLocalNum(int local_num) { local_num_ = local_num; }

  void Output(std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      LOG(INFO) << "enter javapp ctx output";
    }
  }

  const char* app_class_name() const { return _app_class_name; }

  uint64_t inner_context_addr() { return inner_ctx_addr_; }

 public:
  char* _app_class_name;
  jobject _app_object;
  jobject _context_object;
  jobject _frag_object;
  jobject _mm_object;

 protected:
  virtual const char* eval_descriptor() = 0;
  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params) {
    LOG(INFO) << "enter init";
    if (params.empty()) {
      LOG(ERROR) << "no args received";
      return;
    }
    std::string user_library_name;
    LOG(INFO) << "before parse args";
    std::string args_str =
        parse_params_and_setup_jvm_env(params, user_library_name);
    LOG(INFO) << "afeter parse args";
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
      CHECK_NOTNULL(app_class);

      jobject app_object = createObject(env, app_class, _app_class_name);
      CHECK_NOTNULL(app_object);
      _app_object = env->NewGlobalRef(app_object);

      std::string _context_class_name_str =
          get_ctx_class_name_from_app_object(env);
      LOG(INFO) << "context name " << _context_class_name_str;
      // _context_class_name = _context_class_name_str.c_str();
      // The retrived context class str is dash-sperated, convert to /-seperated
      char* _context_class_name_c_str =
          java_class_name_dash_to_slash(_context_class_name_str);
      CHECK_NOTNULL(_context_class_name_c_str);

      jclass context_class = env->FindClass(_context_class_name_c_str);
      CHECK_NOTNULL(context_class);

      jobject ctx_object =
          createObject(env, context_class, _context_class_name_c_str);
      CHECK_NOTNULL(ctx_object);
      _context_object = env->NewGlobalRef(ctx_object);

      const char* descriptor = eval_descriptor();

      jmethodID InitMethodID =
          env->GetMethodID(context_class, "init", descriptor);
      CHECK_NOTNULL(InitMethodID);

      jobject fragObject =
          createFFIPointerObject(env, _java_frag_type_name.c_str(),
                                 reinterpret_cast<jlong>(&fragment_));
      CHECK_NOTNULL(fragObject);
      _frag_object = env->NewGlobalRef(fragObject);

      // 2. Create Message manager Java object
      jobject messagesObject =
          createFFIPointerObject(env, java_message_manager_name, messages_addr);
      CHECK_NOTNULL(messagesObject);
      _mm_object = env->NewGlobalRef(messagesObject);

      // 3. Create arguments array
      {
        jclass json_class = env->FindClass("com/alibaba/fastjson/JSON");
        CHECK_NOTNULL(json_class);
        jmethodID parse_method = env->GetStaticMethodID(
            json_class, "parseObject",
            "(Ljava/lang/String;)Lcom/alibaba/fastjson/JSONObject;");
        CHECK_NOTNULL(parse_method);
        LOG(INFO) << "user defined kw args: " << args_str;
        jstring args_jstring = env->NewStringUTF(args_str.c_str());
        jobject json_object =
            env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
        CHECK_NOTNULL(json_object);

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
        CHECK_NOTNULL(inner_ctx_address_field);

        inner_ctx_addr_ =
            env->GetLongField(_context_object, inner_ctx_address_field);
        CHECK_NE(inner_ctx_addr_, 0);
        LOG(INFO) << "successfully obtained inner ctx";
      }
    }
  }

 private:
  bool init_app_class_name(std::string& app_class) {
    if (app_class.empty()) {
      LOG(ERROR) << "Class names for java app is empty";
      return false;
    }
    _app_class_name = java_class_name_dash_to_slash(app_class);
    return true;
  }
  // Loading jni library with absolute path
  void load_jni_library(JNIEnv* env, std::string& user_library_name) {
    jclass grape_load_library =
        env->FindClass("com/alibaba/grape/utils/LoadLibrary");
    CHECK_NOTNULL(grape_load_library);

    jclass vineyard_load_library =
        env->FindClass("io/v6d/modules/graph/utils/LoadLibrary");
    CHECK_NOTNULL(vineyard_load_library);

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

  // user library name should be absolute
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
    CHECK(!frag_name.empty());
    LOG(INFO) << "parse frag name: " << frag_name;
    _java_frag_type_name = frag_name;
    pt.erase("frag_name");

    std::string app_class_name = pt.get<std::string>("app_class");
    CHECK(!app_class_name.empty());
    LOG(INFO) << "parse app class name: " << app_class_name;
    CHECK(init_app_class_name(app_class_name));
    pt.erase("app_class");

    user_library_name = pt.get<std::string>("user_library_name");
    CHECK(!user_library_name.empty());
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

  std::string get_ctx_class_name_from_app_object(JNIEnv* env) {
    // get app_class's class object
    jclass app_class_class = env->GetObjectClass(_app_object);
    CHECK_NOTNULL(app_class_class);
    jmethodID app_class_getClass_method =
        env->GetMethodID(app_class_class, "getClass", "()Ljava/lang/Class;");
    CHECK_NOTNULL(app_class_getClass_method);
    jobject app_class_obj =
        env->CallObjectMethod(_app_object, app_class_getClass_method);
    CHECK_NOTNULL(app_class_obj);

    jclass app_context_getter_class = env->FindClass(_app_context_getter_name);
    CHECK_NOTNULL(app_context_getter_class);

    jmethodID app_context_getter_method = env->GetStaticMethodID(
        app_context_getter_class, "getPropertyDefaultContextName",
        "(Ljava/lang/Class;)Ljava/lang/String;");
    CHECK_NOTNULL(app_context_getter_method);
    // Pass app class's class object
    jstring context_class_jstring = (jstring) env->CallStaticObjectMethod(
        app_context_getter_class, app_context_getter_method, app_class_obj);
    CHECK_NOTNULL(context_class_jstring);
    return jstring2string(env, context_class_jstring);
  }
  // char* _context_class_name;
  std::string _java_frag_type_name;
  // grape::ContextBase* inner_ctx_;
  const fragment_t& fragment_;
  int local_num_;
  uint64_t inner_ctx_addr_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_
