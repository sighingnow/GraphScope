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

#include <grape/grape.h>
#include <jni.h>

#include <iomanip>
#include <limits>
#include <vector>

#include "flags.h"
#include "java_pie/javasdk.h"

namespace grape {

/**
 * @brief Context for the java pie app, used by java sdk.
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEDefaultContext : public JavaContextBase<FRAG_T> {
 public:
  bool init_class_names(std::string& app_class, std::string& context_class) {
    if (app_class.empty() || context_class.empty()) {
      LOG(ERROR) << "Class names for java app and java app context are empty";
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
    {
      _context_class_name = new char[context_class.length() + 1];
      strcpy(_context_class_name, context_class.c_str());

      char* p = _context_class_name;
      while (*p) {
        if (*p == '.')
          *p = '/';
        p++;
      }
    }
    return true;
  }

 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  using vdata_t = typename FRAG_T::vdata_t;
  using edata_t = typename FRAG_T::edata_t;

  JavaPIEDefaultContext()
      : _app_class_name(NULL),
        _app_object(NULL),
        _context_class_name(NULL),
        _context_object(NULL),
        _frag_object(NULL),
        _mm_object(NULL) {}

  virtual ~JavaPIEDefaultContext() {
    delete[] _app_class_name;
    delete[] _context_class_name;
    JNIEnvMark m;
    if (m.env()) {
      m.env()->DeleteGlobalRef(_app_object);
      m.env()->DeleteGlobalRef(_context_object);
      m.env()->DeleteGlobalRef(_frag_object);
      m.env()->DeleteGlobalRef(_mm_object);
    }
  }

  void GetJavaDefaultManagerFFITypeName(std::string& name) {
    name.append("grape::JavaDefaultMessageManager<")
        .append(_java_frag_type_name)
        .append(">");
  }

  void Init(const FRAG_T& frag, JavaDefaultMessageManager<FRAG_T>& messages,
            std::string& frag_name, std::string& app_class_name,
            std::string& app_context_name, std::vector<std::string>& args) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();
      if (!init_class_names(app_class_name, app_context_name)) {
        LOG(ERROR) << "Init app class and context class names failed:"
                   << app_class_name << "," << app_context_name;
        return;
      }

      jclass context_class = env->FindClass(_context_class_name);
      if (context_class == NULL) {
        LOG(ERROR) << "Cannot find context class " << _context_class_name;
        return;
      }
      {
        jobject object = createObject(env, context_class, _context_class_name);
        if (object != NULL) {
          _context_object = env->NewGlobalRef(object);
        } else {
          LOG(ERROR) << "Create context obj failed for " << _context_class_name;
          return;
        }
      }
      jclass app_class = env->FindClass(_app_class_name);
      if (app_class == NULL) {
        LOG(ERROR) << "Cannot find class " << _app_class_name;
        return;
      }
      {
        jobject object = createObject(env, app_class, _app_class_name);
        if (object != NULL) {
          _app_object = env->NewGlobalRef(object);
        } else {
          LOG(ERROR) << "create app object failed for " << _app_class_name;
          return;
        }
      }

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;"
          "Lcom/alibaba/grape/message/messageManager/JavaDefaultMessageManager;"
          "Lcom/alibaba/grape/stdcxx/StdVector;)V";
      jmethodID InitMethodID =
          env->GetMethodID(context_class, "Init", descriptor);
      if (InitMethodID == NULL) {
        LOG(ERROR) << "Cannot find method Init" << descriptor;
        return;
      }

      _java_frag_type_name = frag_name;
      jobject fragObject = createFFIPointerObject(
          env, _java_frag_type_name.c_str(), reinterpret_cast<jlong>(&frag));
      if (fragObject == NULL) {
        LOG(ERROR) << "Cannot create fragment Java object";
        return;
      } else {
        _frag_object = env->NewGlobalRef(fragObject);
      }

      // 2. Create Message manager Java object
      // TODO: create message pointer object
      std::string mm_name;
      GetJavaDefaultManagerFFITypeName(mm_name);
      jobject messagesObject = createFFIPointerObject(
          env, mm_name.c_str(), reinterpret_cast<jlong>(&messages));
      if (messagesObject == NULL) {
        LOG(ERROR) << "Cannot create message manager Java object";
        return;
      } else {
        _mm_object = env->NewGlobalRef(messagesObject);
      }

      // 3. Create arguments array
      jobject argsObject = createStdVectorObject(
          env, "std::vector<std::string>", reinterpret_cast<jlong>(&args));
      if (argsObject == NULL) {
        LOG(ERROR) << "Cannot create args Java object";
        return;
      }
      // 4. Invoke java method
      env->CallVoidMethod(_context_object, InitMethodID, _frag_object,
                          _mm_object, argsObject);
    }
  }

  void Output(const FRAG_T& frag, std::ostream& os) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jclass context_class = env->FindClass(_context_class_name);
      if (context_class == NULL) {
        LOG(ERROR) << "Cannot find class " << _context_class_name;
        return;
      }

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;)V";
      jmethodID OutputMethodID =
          env->GetMethodID(context_class, "Output", descriptor);
      if (OutputMethodID == NULL) {
        LOG(ERROR) << "Cannot find method Output" << descriptor;
        return;
      }

      env->CallVoidMethod(_context_object, OutputMethodID, _frag_object);
    }
  }

  const char* app_class_name() const { return _app_class_name; }

  const char* context_class_name() const { return _context_class_name; }

  char* _app_class_name;
  char* _context_class_name;
  std::string _java_frag_type_name;
  jobject _app_object;
  jobject _context_object;
  jobject _frag_object;
  jobject _mm_object;
};
}  // namespace grape

#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_CONTEXT_H_
