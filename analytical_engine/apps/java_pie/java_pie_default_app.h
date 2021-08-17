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

#ifndef ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_APP_H_
#define ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_APP_H_

#include <utility>

#include "grape/grape.h"
#include "grape/types.h"
#include "java_pie/java_pie_default_context.h"

namespace grape {

/**
 * @brief Java default app driver
 *
 * @tparam FRAG_T
 */
template <typename FRAG_T>
class JavaPIEDefaultApp
    : public JavaDefaultAppBase<FRAG_T, JavaPIEDefaultContext<FRAG_T>> {
 public:
  // specialize the templated worker.
  INSTALL_JAVA_DEFAULT_WORKER(JavaPIEDefaultApp<FRAG_T>,
                              JavaPIEDefaultContext<FRAG_T>, FRAG_T)
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;
  using oid_t = typename fragment_t::oid_t;
  using vdata_t = typename fragment_t::vdata_t;
  using edata_t = typename fragment_t::edata_t;

 public:
  void PEval(const fragment_t& frag, context_t& ctx,
             message_manager_t& messages) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jobject app_object = ctx._app_object;

      if (app_object == NULL) {
        LOG(ERROR) << "AppObject is null";
        return;
      }

      jclass app_class = env->GetObjectClass(app_object);
      if (app_class == NULL) {
        LOG(ERROR) << "Cannot get app class " << ctx._app_class_name;
        return;
      }

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;"
          "Lcom/alibaba/grape/app/ContextBase;"
          "Lcom/alibaba/grape/message/messageManager/"
          "JavaDefaultMessageManager;)V";
      jmethodID PEvalMethodID =
          env->GetMethodID(app_class, "PEval", descriptor);
      if (PEvalMethodID == NULL) {
        LOG(ERROR) << "Cannot find method PEval" << descriptor;
        return;
      }

      jobject fragObject = ctx._frag_object;
      if (fragObject == NULL) {
        LOG(ERROR) << "context's frag object is null";
        return;
      }

      jobject contextObject = ctx._context_object;
      if (contextObject == NULL) {
        LOG(ERROR) << "Cannot get context object";
        return;
      }

      jobject mmObject = ctx._mm_object;
      if (mmObject == NULL) {
        LOG(ERROR) << "Cannot create message manager Java object";
        return;
      }

      env->CallVoidMethod(app_object, PEvalMethodID, fragObject, contextObject,
                          mmObject);
    }
  }

  /**
   * @brief Incremental evaluation for Java default app
   *
   * @param frag
   * @param ctx
   */
  void IncEval(const fragment_t& frag, context_t& ctx,
               message_manager_t& messages) {
    JNIEnvMark m;
    if (m.env()) {
      JNIEnv* env = m.env();

      jobject app_object = ctx._app_object;

      if (app_object == NULL) {
        LOG(ERROR) << "AppObject is null";
        return;
      }

      jclass app_class = env->GetObjectClass(app_object);
      if (app_class == NULL) {
        LOG(ERROR) << "Cannot get app class " << ctx._app_class_name;
        return;
      }

      const char* descriptor =
          "(Lcom/alibaba/grape/fragment/ImmutableEdgecutFragment;"
          "Lcom/alibaba/grape/app/ContextBase;"
          "Lcom/alibaba/grape/message/messageManager/"
          "JavaDefaultMessageManager;)V";
      jmethodID IncEvalMethodID =
          env->GetMethodID(app_class, "IncEval", descriptor);
      if (IncEvalMethodID == NULL) {
        LOG(ERROR) << "Cannot find method IncEval" << descriptor;
        return;
      }

      jobject fragObject = ctx._frag_object;
      if (fragObject == NULL) {
        LOG(ERROR) << "Cannot create fragment Java object";
        return;
      }

      jobject contextObject = ctx._context_object;
      if (contextObject == NULL) {
        LOG(ERROR) << "Cannot get context object";
        return;
      }

      jobject mmObject = ctx._mm_object;
      if (mmObject == NULL) {
        LOG(ERROR) << "Cannot create message manager Java object";
        return;
      }

      env->CallVoidMethod(app_object, IncEvalMethodID, fragObject,
                          contextObject, mmObject);
    }
  }
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_APPS_JAVA_PIE_JAVA_PIE_DEFAULT_APP_H_
