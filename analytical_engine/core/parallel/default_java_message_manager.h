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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_DEFAULT_JAVA_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_DEFAULT_JAVA_MESSAGE_MANAGER_H_

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include "jni.h"

#include "grape/communication/communicator.h"
#include "grape/parallel/default_message_manager.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/worker/comm_spec.h"

namespace gs {
class DefaultJavaMessageManager : public grape::DefaultMessageManager,
                                  public grape::Communicator {
 public:
  DefaultJavaMessageManager() {}
  ~DefaultJavaMessageManager() {}

  /**
   * @brief Called by worker when init.
   * for java ctx, we need to init the communicator,
   * for the functions like sum.
   */
  void Init(MPI_Comm comm) override {
    grape::DefaultMessageManager::Init(comm);
    grape::Communicator::InitCommunicator(comm);
  }
};
}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_DEFAULT_JAVA_MESSAGE_MANAGER_H_