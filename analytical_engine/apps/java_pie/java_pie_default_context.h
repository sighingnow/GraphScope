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

#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
#ifdef ENABLE_JAVA_SDK
#include <grape/grape.h>
#include <jni.h>
#include "grape/grape.h"

#include <iomanip>
#include <limits>
#include <vector>
#include "core/java/javasdk.h"
#include "grape/app/context_base.h"
namespace gs {

/**
 * @brief Driver context for Java context, work along with @see
 * gs::JavaPIEDefaultApp.
 *
 * @tparam FRAG_T Should be grape::ImmutableEdgecutFragment<...>
 */
template <typename FRAG_T>
class JavaPIEDefaultContext : public grape::ContextBase<FRAG_T> {
 public:
  JavaPIEDefaultContext(const FRAG_T& fragment)
      : fragment_(fragment),
        app_class_name_(NULL),
        context_class_name_(NULL),
        app_object_(NULL),
        context_object_(NULL),
        fragment_object_(NULL),
        mm_object_(NULL) {}

  virtual ~JavaPIEDefaultContext() {
    delete[] app_class_name_;
    delete[] context_class_name_;
    JNIEnvMark m;
    if (m.env()) {
      m.env()->DeleteGlobalRef(app_object_);
      m.env()->DeleteGlobalRef(context_object_);
      m.env()->DeleteGlobalRef(fragment_object_);
      m.env()->DeleteGlobalRef(mm_object_);
    }
  }
  const fragment_t& fragment() { return fragment_; }
  const char* app_class_name() const { return app_class_name_; }
  const char* context_class_name() const { return context_class_name_; }
  const jobject& app_object() const { return app_object_; }
  const jobject& context_object() const { return context_object_; }
  const jobject& fragment_object() const { return fragment_object_; }
  const jobject& message_manager_object() const { return mm_object_; }

  void Init(grape::DefaultMessageManager& messages, std::string& frag_name,
            std::string& app_class_name, std::string& app_context_name,
            std::vector<std::string>& args) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      CHECK(!init_class_names(app_class_name, app_context_name));

      jclass context_class = env->FindClass(context_class_name_);
      CHECK_NOTNULL(context_class);
      {
        jobject object = createObject(env, context_class, context_class_name_);
        CHECK_NOTNULL(object);
        context_object_ = env->NewGlobalRef(object);
      }
      jclass app_class = env->FindClass(app_class_name_);
      CHECK_NOTNULL(app_class);
      {
        jobject object = createObject(env, app_class, app_class_name_);
        CHECK_NOTNULL(object);
        app_object_ = env->NewGlobalRef(object);
      }
      {
        java_frag_type_name_ = frag_name;
        jobject frag_object = createFFIPointerObject(
            env, java_frag_type_name_.c_str(), reinterpret_cast<jlong>(&frag));
        CHECK_NOTNULL(frag_object);
        fragment_object_ = env->NewGlobalRef(frag_object);
      }
      {
        jobject messages_object =
            createFFIPointerObject(env, default_java_message_mananger_name_,
                                   reinterpret_cast<jlong>(&messages));
        CHECK_NOTNULL(messages_object);
        mm_object_ = env->NewGlobalRef(messages_object);
      }

      jobject args_object = createFFIPointer(env, "std::vector<std::string>",
                                             url_class_loader_object(),
                                             reinterpret_cast<jlong>(&args));
      CHECK_NOTNULL(args_object);

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;"
          "Lcom/alibaba/grape/parallel/DefaultMessageManager;"
          "Lcom/alibaba/grape/stdcxx/StdVector;)V";
      jmethodID init_methodID =
          env->GetMethodID(context_class, "Init", descriptor);
      CHECK_NOTNULL(init_methodID);

      env->CallVoidMethod(context_object_, init_methodID, fragment_object_,
                          mm_object_, args_object);
    }
  }

  void Output(std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->FindClass(context_class_name_);
      if (context_class == NULL) {
        LOG(ERROR) << "Cannot find class " << context_class_name_;
        return;
      }

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;)V";
      jmethodID output_methodID =
          env->GetMethodID(context_class, "Output", descriptor);
      CHECK_NOTNULL(output_methodID);
      env->CallVoidMethod(context_object_, output_methodID, fragment_object_);
    }
  }

 private:
  bool init_class_names(std::string& app_class, std::string& context_class) {
    if (app_class.empty() || context_class.empty()) {
      LOG(ERROR) << "Class names for java app and java app context are empty";
      return false;
    }
    app_class_name_ = java_class_name_dash_to_slash(app_class);
    context_class_name_ = java_class_name_dash_to_slash(context_class);
    return true;
  }
  char* app_class_name_;
  char* context_class_name_;
  static const char* default_java_message_mananger_name_ =
      "grape::DefaultMessageManager";
  std::string java_frag_type_name_;
  jobject app_object_;
  jobject context_object_;
  jobject fragment_object_;
  jobject mm_object_;
  const fragment_t& fragment_;
};
}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
