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

#ifdef ENABLE_JAVA_SDK
#include <jni.h>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <vector>

#include <grape/grape.h>
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/split.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "core/config.h"
#include "core/context/labeled_vertex_property_context.h"
#include "core/context/vertex_data_context.h"
#include "core/context/vertex_property_context.h"
#include "core/java/javasdk.h"
#include "core/object/i_fragment_wrapper.h"
#include "grape/app/context_base.h"
#include "vineyard/client/client.h"
#include "vineyard/graph/fragment/fragment_traits.h"
namespace gs {
static constexpr const char* APP_CONTEXT_GETTER_CLASS =
    "io/graphscope/utils/AppContextGetter";
static constexpr const char* LOAD_LIBRARY_CLASS =
    "io/graphscope/runtime/LoadLibrary";
static constexpr const char* CONTEXT_UTILS_CLASS =
    "io/graphscope/utils/ContextUtils";
static constexpr const char* JSON_CLASS_NAME = "com.alibaba.fastjson.JSON";

/**
 * @brief JavaContextBase is the base class for JavaPropertyContext and
 * JavaProjectedContext.
 *
 */
template <typename FRAG_T>
class JavaContextBase : public grape::ContextBase {
 public:
  using fragment_t = FRAG_T;

  JavaContextBase(const FRAG_T& fragment)
      : app_class_name_(NULL),
        inner_ctx_addr_(0),
        fragment_(fragment),
        app_object_(NULL),
        context_object_(NULL),
        fragment_object_(NULL),
        mm_object_(NULL),
        url_class_loader_object_(NULL) {}

  virtual ~JavaContextBase() {
    if (app_class_name_) {
      delete[] app_class_name_;
    }
    JNIEnvMark m;
    if (m.env()){
        m.env()->DeleteGlobalRef(url_class_loader_object_);
        invoke_gc(m.env());
        LOG(INFO) << "deleted usl class loader";
    }
    //jint res = GetJavaVM()->DestroyJavaVM();
    //LOG(INFO) << "Kill javavm status: " << res;

  }
  const fragment_t& fragment() const { return fragment_; }

  void Output(std::ostream& os) {
    LOG(INFO)
        << "Java app context will output with other methods: ToNdArray, etc. ";
  }

  const char* app_class_name() const { return app_class_name_; }

  uint64_t inner_context_addr() { return inner_ctx_addr_; }

  const std::string& graph_type_str() const { return graph_type_str_; }

  const jobject& app_object() const { return app_object_; }
  const jobject& context_object() const { return context_object_; }
  const jobject& fragment_object() const { return fragment_object_; }
  const jobject& message_manager_object() const { return mm_object_; }
  const jobject& url_class_loader_object() const {
    return url_class_loader_object_;
  }

 protected:
  virtual const char* eval_descriptor() = 0;
  void init(jlong messages_addr, const char* java_message_manager_name,
            const std::string& params) {
    if (params.empty()) {
      LOG(FATAL) << "no args received";
      return;
    }
    std::string user_library_name;
    std::string user_class_path;
    std::string args_str = parse_params_and_setup_jvm_env(
        params, user_library_name, user_class_path);

    JavaVM* jvm = GetJavaVM();
    (void) jvm;
    CHECK_NOTNULL(jvm);
    LOG(INFO) << "Successfully get jvm";

    // It is possible for multiple java app run is one grape instance, we need
    // to find the user jar. But it is not possible to restart a jvm with new
    // class path, so we utilize java class loader to load the new jar
    // add_class_path_at_runtime();

    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      load_jni_library(env, user_library_name);

      // Create a graphscope class loader to load app_class and ctx_class. This
      // means will create a new class loader for each for run_app.
      // The intent is to provide isolation, and avoid class conflicts。
      {
        jobject gs_class_loader_obj = create_class_loader(env, user_class_path);
        CHECK_NOTNULL(gs_class_loader_obj);
        url_class_loader_object_ = env->NewGlobalRef(gs_class_loader_obj);
      }

      {
        LOG(INFO) << "Now create app object: " << app_class_name_;
        jobject app_obj =
            load_and_create(env, url_class_loader_object_, app_class_name_);
        CHECK_NOTNULL(app_obj);
        app_object_ = env->NewGlobalRef(app_obj);
        LOG(INFO) << "Successfully create app object with class loader:"
                  << &url_class_loader_object_
                  << ", of type: " << std::string(app_class_name_);
      }
      {
        std::string _context_class_name_str =
            get_ctx_class_name_from_app_object(env);
        LOG(INFO) << "context class name: " << _context_class_name_str;
        // The retrived context class str is dash-sperated, convert to
        // -seperated
        // char* _context_class_name_c_str =
        //     java_class_name_dash_to_slash(_context_class_name_str);
        jobject ctx_obj = load_and_create(env, url_class_loader_object_,
                                          _context_class_name_str.c_str());
        CHECK_NOTNULL(ctx_obj);
        context_object_ = env->NewGlobalRef(ctx_obj);
        LOG(INFO) << "Successfully create ctx object with class loader:"
                  << &url_class_loader_object_
                  << ", of type: " << _context_class_name_str;
      }
      jclass context_class = env->GetObjectClass(context_object_);
      CHECK_NOTNULL(context_class);

      jmethodID InitMethodID =
          env->GetMethodID(context_class, "init", eval_descriptor());
      CHECK_NOTNULL(InitMethodID);

      // TODO: create ffi pointer object with gs_class_loader
      jobject fragObject = createFFIPointer(
          env, graph_type_str_.c_str(), url_class_loader_object_,
          reinterpret_cast<jlong>(&fragment_));
      CHECK_NOTNULL(fragObject);
      fragment_object_ = env->NewGlobalRef(fragObject);

      // 2. Create Message manager Java object
      jobject messagesObject =
          createFFIPointer(env, java_message_manager_name,
                           url_class_loader_object_, messages_addr);
      CHECK_NOTNULL(messagesObject);
      mm_object_ = env->NewGlobalRef(messagesObject);

      // 3. Create arguments array
      {
        // jclass clz = env->FindClass(GRAPHSCOPE_CLASS_LOADER);
        // CHECK_NOTNULL(clz);

        // jmethodID method = env->GetStaticMethodID(
        //     clz, "loadClass",
        //     "(Ljava/net/URLClassLoader;Ljava/lang/String;)Ljava/lang/Class;");
        // CHECK_NOTNULL(method);
        // jstring json_class_name_jstr = env->NewStringUTF(JSON_CLASS_NAME);
        // jclass json_class = (jclass) env->CallStaticObjectMethod(
        //     clz, method, url_class_loader_object_, json_class_name_jstr);
        jclass json_class = (jclass) load_class_with_class_loader(
            env, url_class_loader_object_, JSON_CLASS_NAME);
        if (env->ExceptionCheck()) {
          env->ExceptionDescribe();
          env->ExceptionClear();
          LOG(FATAL) << "Exception in loading json class ";
        }
        // jclass json_class = env->FindClass("com/alibaba/fastjson/JSON");
        CHECK_NOTNULL(json_class);
        jmethodID parse_method = env->GetStaticMethodID(
            json_class, "parseObject",
            "(Ljava/lang/String;)Lcom/alibaba/fastjson/JSONObject;");
        CHECK_NOTNULL(parse_method);
        LOG(INFO) << "User defined kw args: " << args_str;
        jstring args_jstring = env->NewStringUTF(args_str.c_str());
        jobject json_object =
            env->CallStaticObjectMethod(json_class, parse_method, args_jstring);
        CHECK_NOTNULL(json_object);

        // 4. Invoke java method
        env->CallVoidMethod(context_object_, InitMethodID, fragment_object_,
                            mm_object_, json_object);
        if (env->ExceptionCheck()) {
          env->ExceptionDescribe();
          env->ExceptionClear();
          LOG(FATAL) << "Exception in context Init";
        }
        LOG(INFO) << "invokd ctx init method success";
        // 5. to output the result, we need the c++ context held by java object.
        jfieldID inner_ctx_address_field =
            env->GetFieldID(context_class, "ffiContextAddress", "J");
        CHECK_NOTNULL(inner_ctx_address_field);

        inner_ctx_addr_ =
            env->GetLongField(context_object_, inner_ctx_address_field);
        CHECK_NE(inner_ctx_addr_, 0);
        LOG(INFO) << "Successfully obtained inner ctx address";
      }
    }
  }

 private:
  // Loading jni library with absolute path
  void load_jni_library(JNIEnv* env, std::string& user_library_name) {
    LOG(INFO) << "java.class.path: "
              << get_java_property(env, "java.class.path");
    jclass grape_load_library = env->FindClass(LOAD_LIBRARY_CLASS);
    CHECK_NOTNULL(grape_load_library);

    const char* load_library_signature = "(Ljava/lang/String;)V";
    jstring user_library_jstring = env->NewStringUTF(user_library_name.c_str());
    jmethodID grape_load_library_method = env->GetStaticMethodID(
        grape_load_library, "invoke", load_library_signature);

    // call static method
    env->CallStaticVoidMethod(grape_load_library, grape_load_library_method,
                              user_library_jstring);

    if (env->ExceptionOccurred()) {
      LOG(ERROR) << std::string("Exception occurred in loading user library");
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(FATAL) << "Exiting since exception occurred";
    }
    LOG(INFO) << "Loaded specified user jni library: " << user_library_name;
  }

  // user library name should be absolute
  std::string parse_params_and_setup_jvm_env(const std::string& params,
                                             std::string& user_library_name,
                                             std::string& user_class_path) {
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
    graph_type_str_ = frag_name;
    pt.erase("frag_name");

    std::string app_class_name = pt.get<std::string>("app_class");
    CHECK(!app_class_name.empty());
    LOG(INFO) << "parse app class name: " << app_class_name;
    const char* ch = app_class_name.c_str();
    app_class_name_ = new char[strlen(ch) + 1];
    memcpy(app_class_name_, ch, strlen(ch));
    app_class_name_[strlen(ch)] = '\0';
    // app_class_name_ = java_class_name_dash_to_slash(app_class_name);
    pt.erase("app_class");

    user_library_name = pt.get<std::string>("user_library_name");
    CHECK(!user_library_name.empty());
    LOG(INFO) << "user library name " << user_library_name;
    pt.erase("user_library_name");

    int num_hosts = std::stoi(pt.get<std::string>("num_hosts"));
    CHECK(num_hosts > 0);
    int num_worker = std::stoi(pt.get<std::string>("num_worker"));
    CHECK(num_worker > 0);
    pt.erase("num_hosts");
    pt.erase("num_worker");

    int local_num_ = (num_worker + num_hosts - 1) / num_hosts;
    LOG(INFO) << "num hosts: " << num_hosts << ", num worker: " << num_worker
              << ",local worker: " << num_worker / num_hosts;

    // JVM runtime opt should consists of java.libaray.path and
    // java.class.path maybe this should be set by the backend not user.
    std::string jvm_runtime_opt = pt.get<std::string>("jvm_runtime_opt");
    user_class_path = pt.get<std::string>("user_class_path");
    // put the cp and library.path in env
    if (setenv("JVM_OPTS", jvm_runtime_opt.c_str(), 1) == 0) {
      LOG(INFO) << " successfully set jvm opts to: " << jvm_runtime_opt;
    } else {
      LOG(ERROR) << "Failed to set jvm opts";
    }
    SetupEnv(local_num_);
    pt.erase("jvm_runtime_opt");
    pt.erase("user_class_path");
    ss.str("");  // reset the stream buffer
    boost::property_tree::json_parser::write_json(ss, pt);
    return ss.str();
  }

  // get the java context name with is bounded to app_object_.
  std::string get_ctx_class_name_from_app_object(JNIEnv* env) {
    // jclass app_context_getter_class =
    // env->FindClass(APP_CONTEXT_GETTER_CLASS);
    // jclass clz = env->FindClass(GRAPHSCOPE_CLASS_LOADER);
    // CHECK_NOTNULL(clz);

    // jmethodID method = env->GetStaticMethodID(
    //     clz, "loadClass",
    //     "(Ljava/net/URLClassLoader;Ljava/lang/String;)Ljava/lang/Class;");
    // CHECK_NOTNULL(method);

    // jstring context_getter_class_name =
    //     env->NewStringUTF(APP_CONTEXT_GETTER_CLASS);
    // jclass app_context_getter_class = (jclass) env->CallStaticObjectMethod(
    //     clz, method, url_class_loader_object_, context_getter_class_name);
    jclass app_context_getter_class = (jclass) load_class_with_class_loader(
        env, url_class_loader_object_, APP_CONTEXT_GETTER_CLASS);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception in loading class: "
                 << std::string(APP_CONTEXT_GETTER_CLASS);
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(FATAL) << "exiting since exception occurred";
    }

    CHECK_NOTNULL(app_context_getter_class);

    jmethodID app_context_getter_method =
        env->GetStaticMethodID(app_context_getter_class, "getContextName",
                               "(Ljava/lang/Object;)Ljava/lang/String;");
    CHECK_NOTNULL(app_context_getter_method);
    // Pass app class's class object
    jstring context_class_jstring = (jstring) env->CallStaticObjectMethod(
        app_context_getter_class, app_context_getter_method, app_object_);
    CHECK_NOTNULL(context_class_jstring);
    return jstring2string(env, context_class_jstring);
  }

  std::string graph_type_str_;
  char* app_class_name_;
  uint64_t inner_ctx_addr_;
  const fragment_t& fragment_;

  jobject app_object_;
  jobject context_object_;
  jobject fragment_object_;
  jobject mm_object_;
  jobject url_class_loader_object_;
};

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_CONTEXT_JAVA_CONTEXT_BASE_H_
