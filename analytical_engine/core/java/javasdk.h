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

#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_

#ifdef ENABLE_JAVA_SDK
#include <jni.h>
#include <stdlib.h>
#include <unistd.h>
#include <queue>
#include <utility>
#include <vector>
#include "grape/grape.h"

namespace gs {
static constexpr const char* GRAPHSCOPE_CLASS_LOADER =
    "io/graphscope/runtime/GraphScopeClassLoader";
static constexpr const char* FFI_TYPE_FACTORY_CLASS_NAME_DASH =
    "com.alibaba.ffi.FFITypeFactory";
static constexpr const char* FFI_TYPE_FACTORY_GET_TYPE_METHOD_NAME = "getType";

static constexpr const char* FFI_TYPE_FACTORY_GET_TYPE_METHOD_SIG =
    "(Ljava/lang/String;)Ljava/lang/Class;";
static JavaVM* _jvm = NULL;
static jclass FFITypeFactoryClass = NULL;
static jclass CommunicatorClass = NULL;
static jmethodID FFITypeFactory_getTypeMethodID = NULL;
static jmethodID FFITypeFactory_getTypeMethodID_plus = NULL;
static jclass FFIVectorClass = NULL;
static jclass StdVectorClass = NULL;
static jclass class_loader_clz = NULL;
static jmethodID class_loader_create_ffipointer_methodID = NULL;
static jmethodID class_loader_load_class_methodID = NULL;

std::string jstring2string(JNIEnv* env, jstring jStr);
bool InitWellKnownClasses(JNIEnv* env) {
  // FFITypeFactoryClass = env->FindClass("com/alibaba/ffi/FFITypeFactory");
  // FFIVectorClass = env->FindClass("com/alibaba/ffi/FFIVector");
  // StdVectorClass = env->FindClass("com/alibaba/grape/stdcxx/StdVector");
  class_loader_clz = env->FindClass(GRAPHSCOPE_CLASS_LOADER);
  CHECK_NOTNULL(class_loader_clz);
  class_loader_clz = (jclass) env->NewGlobalRef(class_loader_clz);

  class_loader_create_ffipointer_methodID = env->GetStaticMethodID(
      class_loader_clz, "createFFIPointer",
      "(Ljava/net/URLClassLoader;Ljava/lang/String;J)Ljava/lang/Object;");
  CHECK_NOTNULL(class_loader_create_ffipointer_methodID);

  class_loader_load_class_methodID = env->GetStaticMethodID(
      class_loader_clz, "loadClass",
      "(Ljava/net/URLClassLoader;Ljava/lang/String;)Ljava/lang/Class;");
  CHECK_NOTNULL(class_loader_load_class_methodID);

  return true;
}

inline uint64_t getTotalSystemMemory() {
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  uint64_t ret = pages * page_size;
  // LOG(INFO) << "---> getTotalSystemMemory() -> " << ret;
  ret = ret / 1024;
  ret = ret / 1024;
  ret = ret / 1024;
  return ret;
}

void SetupEnv(int local_num) {
  int systemMemory = getTotalSystemMemory() / 50;
  int systemMemoryPerWorker = std::max(systemMemory / local_num, 1);
  int mnPerWorker = std::max(systemMemoryPerWorker * 7 / 12, 1);

  char kvPair[32000];
  snprintf(kvPair, sizeof(kvPair), "-Xmx%dg -Xms%dg -Xmn%dg",
           systemMemoryPerWorker, systemMemoryPerWorker, mnPerWorker);

  char* jvm_opts = getenv("JVM_OPTS");
  // char* jvm_opts = jvm_option.c_str();
  char setStr[32010];
  if (jvm_opts == NULL || *jvm_opts == '\0') {
    snprintf(setStr, sizeof(setStr), "JVM_OPTS=%s", kvPair);
    putenv(setStr);
  } else {
    std::string jvmOptsStr = jvm_opts;
    size_t pos = 0;
    std::string token;
    std::string delimiter = " ";
    bool flag = true;
    while ((pos = jvmOptsStr.find(delimiter)) != std::string::npos) {
      token = jvmOptsStr.substr(0, pos);
      jvmOptsStr.erase(0, pos + delimiter.length());
      if (token.length() > 4) {
        std::string prefix = token.substr(0, 4);
        if (prefix == "-Xmx" || prefix == "-Xms" || prefix == "-Xmn") {
          LOG(INFO) << "token = " << token;
          flag = false;
          break;
        }
      }
    }
    if (flag) {
      snprintf(setStr, sizeof(setStr), "JVM_OPTS=%s %s", jvm_opts, kvPair);
      putenv(setStr);
    }
  }
}

JavaVM* CreateJavaVM() {
  char *p, *q;
  char* jvm_opts = getenv("JVM_OPTS");
  std::string jvm_opts_str = jvm_opts;
  LOG(INFO) << "jvm opts str " << jvm_opts_str;
  if (jvm_opts == NULL)
    return NULL;

  if (*jvm_opts == '\0')
    return NULL;

  int num_of_opts = 1;
  for (char* p = jvm_opts; *p; p++) {
    if (*p == ' ')
      num_of_opts++;
  }

  if (num_of_opts == 0)
    return NULL;

  JavaVM* jvm = NULL;
  JNIEnv* env = NULL;
  int i = 0;
  int status = 1;
  JavaVMInitArgs vm_args;

  JavaVMOption* options = new JavaVMOption[num_of_opts];
  memset(options, 0, sizeof(JavaVMOption) * num_of_opts);

  for (p = q = jvm_opts;; p++) {
    if (*p == ' ' || *p == '\0') {
      if (q >= p) {
        goto ret;
      }
      char* opt = new char[p - q + 1];
      memcpy(opt, q, p - q);
      opt[p - q] = '\0';
      options[i++].optionString = opt;
      q = p + 1;  // assume opts are separated by single space
      if (*p == '\0')
        break;
    }
  }

  memset(&vm_args, 0, sizeof(vm_args));
  vm_args.version = JNI_VERSION_1_8;
  vm_args.nOptions = num_of_opts;
  vm_args.options = options;

  status = JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &vm_args);
  if (status == JNI_OK) {
    InitWellKnownClasses(env);
  } else if (status == JNI_EEXIST) {
  } else {
    LOG(FATAL) << "error, create java virtual machine failed. return JNI_CODE ("
               << status << ")\n";
  }
  // Why does env JVM_OPTS unseted after this?
  if (setenv("JVM_OPTS", jvm_opts_str.c_str(), 1) == 0) {
    LOG(INFO) << "Successfully reset jvm opts to: " << jvm_opts_str;
  } else {
    LOG(ERROR) << "Failed to set jvm opts";
  }

ret:
  for (int i = 0; i < num_of_opts; i++) {
    delete[] options[i].optionString;
  }
  delete[] options;
  return jvm;
}

// One process can only create jvm for once.
JavaVM* GetJavaVM() {
  if (_jvm == NULL) {
    _jvm = CreateJavaVM();
    char* jvm_opts = getenv("JVM_OPTS");
    std::string jvm_opts_str = jvm_opts;
    LOG(INFO) << "after creating jvm, jvm opts str " << jvm_opts_str;
  }
  return _jvm;
}

struct JNIEnvMark {
  JNIEnv* _env;

  JNIEnvMark() : _env(NULL) {
    if (!GetJavaVM())
      return;
    int status = GetJavaVM()->AttachCurrentThread(
        reinterpret_cast<void**>(&_env), nullptr);
    if (status != JNI_OK) {
      LOG(ERROR) << "Error attach current thread: " << status;
    }
  }

  ~JNIEnvMark() {
    if (_env)
      GetJavaVM()->DetachCurrentThread();
  }

  JNIEnv* env() { return _env; }
};

// char* java_cp = getenv("JAVA_CP");
// if (java_cp == NULL) {
//   LOG(ERROR) << "No env var JAVA_CP found.";
//   return NULL;
// }
// std::string java_cp_str = java_cp;
// LOG(INFO) << "java cp str: " << java_cp_str;

// Create a URL class loader
jobject create_class_loader(JNIEnv* env, const std::string& class_path) {
  jclass clz = env->FindClass(GRAPHSCOPE_CLASS_LOADER);
  CHECK_NOTNULL(clz);

  jmethodID method =
      env->GetStaticMethodID(clz, "newGraphScopeClassLoader",
                             "(Ljava/lang/String;)Ljava/net/URLClassLoader;");
  CHECK_NOTNULL(method);

  jstring cp_jstring = env->NewStringUTF(class_path.c_str());
  jobject class_loader = env->CallStaticObjectMethod(clz, method, cp_jstring);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  CHECK_NOTNULL(class_loader);
  return env->NewGlobalRef(class_loader);
}

jobject createFFIPointer(JNIEnv* env, const char* type_name,
                         jobject& class_loader, jlong pointer) {
  jstring type_name_jstring = env->NewStringUTF(type_name);
  jobject ffi_pointer = env->CallStaticObjectMethod(
      class_loader_clz, class_loader_create_ffipointer_methodID, class_loader,
      type_name_jstring, pointer);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(FATAL) << "Fail to create FFIPointer " << type_name
               << " addr: " << pointer;
    return NULL;
  }
  return env->NewGlobalRef(ffi_pointer);
}

// TODO:Remove
jobject createObject(JNIEnv* env, jclass clazz, const char* class_name) {
  jmethodID ctor = env->GetMethodID(clazz, "<init>", "()V");
  if (ctor == NULL) {
    LOG(ERROR) << "Cannot find default constructor " << class_name;
    return NULL;
  }
  jobject object = env->NewObject(clazz, ctor);
  return object;
}
// TODO: remove
jobject createStdVectorObject(JNIEnv* env, const char* type_name,
                              jlong pointer) {
  // must be properly encoded
  StdVectorClass = env->FindClass("com/alibaba/grape/stdcxx/StdVector");
  jstring jstring_name = env->NewStringUTF(type_name);
  jclass clzClazz = env->GetObjectClass(StdVectorClass);
  jmethodID method_getName =
      env->GetMethodID(clzClazz, "getSimpleName", "()Ljava/lang/String;");
  jstring name =
      (jstring) env->CallObjectMethod(StdVectorClass, method_getName);
  LOG(INFO) << "stdvector class" << jstring2string(env, name);
  env->DeleteLocalRef(clzClazz);

  jclass the_class = (jclass) env->CallStaticObjectMethod(
      FFITypeFactoryClass, FFITypeFactory_getTypeMethodID_plus, StdVectorClass,
      jstring_name);
  if (env->ExceptionOccurred()) {
    LOG(ERROR) << std::string("Exception occurred in get stdVector class");
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  if (the_class == NULL) {
    LOG(FATAL) << "Cannot find Class for " << type_name;
    return NULL;
  }

  jmethodID the_ctor = env->GetMethodID(the_class, "<init>", "(J)V");
  if (the_ctor == NULL) {
    LOG(FATAL) << "Cannot find <init>(J)V constructor in " << type_name;
    return NULL;
  }

  jobject the_object = env->NewObject(the_class, the_ctor, pointer);
  if (the_object == NULL) {
    LOG(FATAL) << "Cannot call <init>(J)V constructor in " << type_name;
    return NULL;
  }

  return the_object;
}

// TODO: remove
jobject createFFIPointerObject(JNIEnv* env, const char* type_name,
                               jlong pointer) {
  // must be properly encoded
  std::string tmp = type_name;
  jstring jstring_name = env->NewStringUTF(type_name);
  jclass the_class = (jclass) env->CallStaticObjectMethod(
      FFITypeFactoryClass, FFITypeFactory_getTypeMethodID, jstring_name);
  if (env->ExceptionOccurred()) {
    LOG(ERROR) << std::string("Exception occurred in get ffi class: ")
               << type_name;
    env->ExceptionDescribe();
    env->ExceptionClear();
    return NULL;
  }
  if (the_class == NULL) {
    LOG(FATAL) << "Cannot find Class for " << type_name;
    return NULL;
  }

  jmethodID the_ctor = env->GetMethodID(the_class, "<init>", "(J)V");
  if (the_ctor == NULL) {
    LOG(FATAL) << "Cannot find <init>(J)V constructor in " << type_name;
    return NULL;
  }

  jobject the_object = env->NewObject(the_class, the_ctor, pointer);
  if (the_object == NULL) {
    LOG(FATAL) << "Cannot call <init>(J)V constructor in " << type_name;
    return NULL;
  }
  return the_object;
}

// TODO: remove
// jobject createFFIPointerObjectSafe(JNIEnv* env, const char* type_name,
//                                    jobject& gs_class_loader, jlong pointer) {
//   // must be properly encoded
//   jstring jstring_name = env->NewStringUTF(type_name);
//   // the global factory class may in previous jar, we need to load from
//   current
//   // jar.
//   jclass clz = env->FindClass(GRAPHSCOPE_CLASS_LOADER);
//   CHECK_NOTNULL(clz);

//   jmethodID method = env->GetStaticMethodID(
//       clz, "loadClass",
//       "(Ljava/net/URLClassLoader;Ljava/lang/String;)Ljava/lang/Class;");
//   CHECK_NOTNULL(method);

// jstring ffi_type_factory_jstring =
//     env->NewStringUTF(FFI_TYPE_FACTORY_CLASS_NAME_DASH);
// jclass ffi_type_factory_class = (jclass) env->CallStaticObjectMethod(
//     clz, method, gs_class_loader, ffi_type_factory_jstring);
// CHECK_NOTNULL(ffi_type_factory_class);
// jmethodID ffi_type_factory_get_type_method = env->GetStaticMethodID(
//     ffi_type_factory_class, FFI_TYPE_FACTORY_GET_TYPE_METHOD_NAME,
//     FFI_TYPE_FACTORY_GET_TYPE_METHOD_SIG);
// CHECK_NOTNULL(ffi_type_factory_get_type_method);
// jclass the_class = (jclass) env->CallStaticObjectMethod(
//     ffi_type_factory_class, ffi_type_factory_get_type_method, jstring_name);
// if (env->ExceptionCheck()) {
//   env->ExceptionDescribe();
//   env->ExceptionClear();  // clears the exception; e seems to remain valid
//   return NULL;
// }
// Reload the class with our class loader
//   jmethodID method_plus = env->GetStaticMethodID(
//       clz, "loadClassAndCreate",
//       "(Ljava/net/URLClassLoader;Ljava/lang/Class;J)Ljava/lang/Object;");
//   CHECK_NOTNULL(method_plus);
//   jobject the_object = env->CallStaticObjectMethod(
//       clz, method_plus, gs_class_loader, the_class, pointer);
//   if (env->ExceptionCheck()) {
//     env->ExceptionDescribe();
//     env->ExceptionClear();  // clears the exception; e seems to remain valid
//     return NULL;
//   }
//   CHECK_NOTNULL(the_object);
//   return the_object;
// }

std::string jstring2string(JNIEnv* env, jstring jStr) {
  if (!jStr)
    return "";

  const jclass stringClass = env->GetObjectClass(jStr);
  const jmethodID getBytes =
      env->GetMethodID(stringClass, "getBytes", "(Ljava/lang/String;)[B");
  const jbyteArray stringJbytes = (jbyteArray) env->CallObjectMethod(
      jStr, getBytes, env->NewStringUTF("UTF-8"));

  size_t length = (size_t) env->GetArrayLength(stringJbytes);
  jbyte* pBytes = env->GetByteArrayElements(stringJbytes, NULL);

  std::string ret = std::string((char*) pBytes, length);
  env->ReleaseByteArrayElements(stringJbytes, pBytes, JNI_ABORT);

  env->DeleteLocalRef(stringJbytes);
  env->DeleteLocalRef(stringClass);
  return ret;
}

std::string get_jobject_class_name(JNIEnv* env, jobject object) {
  CHECK_NOTNULL(object);
  jclass object_class = env->GetObjectClass(object);

  jmethodID obj_class_getClass_method =
      env->GetMethodID(object_class, "getClass", "()Ljava/lang/Class;");

  jobject obj_class_obj =
      env->CallObjectMethod(object, obj_class_getClass_method);

  jclass obj_class_obj_class = env->GetObjectClass(obj_class_obj);

  jmethodID get_name_method =
      env->GetMethodID(obj_class_obj_class, "getName", "()Ljava/lang/String;");

  jstring class_name_jstr =
      (jstring) env->CallObjectMethod(obj_class_obj, get_name_method);
  return jstring2string(env, class_name_jstr);
}

char* java_class_name_dash_to_slash(const std::string& str) {
  char* c_str = new char[str.length() + 1];
  strcpy(c_str, str.c_str());
  char* p = c_str;
  while (*p) {
    if (*p == '.')
      *p = '/';
    p++;
  }
  return c_str;
}
// judge whether java app class instance of Communicator, if yes, we call
// the init communicator method.
void init_java_communicator(JNIEnv* env, const jobject& url_class_loader,
                            const jobject& java_app, jlong app_address) {
  CHECK_NOTNULL(env);
  CHECK(app_address != 0);
  // load communicator class with class_loader
  jstring communicator_jstring = env->NewStringUTF("Communicator");
  jclass communicator_class = (jclass) env->CallStaticObjectMethod(
      class_loader_clz, class_loader_load_class_methodID, url_class_loader,
      communicator_jstring);
  CHECK_NOTNULL(communicator_class);
  if (env->IsInstanceOf(java_app, communicator_class)) {
    jmethodID init_communicator_method =
        env->GetMethodID(communicator_class, "initCommunicator", "(Z;)V");
    CHECK_NOTNULL(init_communicator_method);
    env->CallVoidMethod(java_app, init_communicator_method, app_address);
    if (env->ExceptionCheck()) {
      LOG(ERROR) << "Exception occurred in init communicator";
      env->ExceptionDescribe();
      env->ExceptionClear();
      LOG(FATAL) << "Exiting...";
    }
    LOG(INFO) << "Successfully init communicator.";
    return;
  }
  LOG(INFO) << "No initing since not a sub class from Communicator.";
}

std::string get_java_property(JNIEnv* env, const char* property_name) {
  jclass systemClass = env->FindClass("java/lang/System");
  jmethodID getPropertyMethod = env->GetStaticMethodID(
      systemClass, "getProperty", "(Ljava/lang/String;)Ljava/lang/String;");
  jstring propertyNameString = env->NewStringUTF(property_name);
  jstring propertyString = (jstring) env->CallStaticObjectMethod(
      systemClass, getPropertyMethod, propertyNameString);
  if (propertyString == NULL) {
    LOG(FATAL) << "empty property string for " << property_name;
  }
  return jstring2string(env, propertyString);
}
// May return null.
jclass load_class_with_class_loader(JNIEnv* env,const jobject& gs_class_loader,
                                    const char* class_name) {
  jstring class_name_jstring = env->NewStringUTF(class_name);

  jclass result_class = (jclass) env->CallStaticObjectMethod(
      class_loader_clz, class_loader_load_class_methodID, gs_class_loader,
      class_name_jstring);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(FATAL) << "Error in loading class " << class_name
               << " with class loader " << &gs_class_loader;
  }
  return result_class;
}
}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVASDK_H_
