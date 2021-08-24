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

#ifndef ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_WORKER_H_
#define ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_WORKER_H_

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "core/parallel/java_default_message_manager.h"
#include "grape/communication/communicator.h"
#include "grape/config.h"
// #include "grape/parallel/parallel_engine.h"
#include "grape/util.h"
#include "grape/worker/comm_spec.h"

namespace grape {

template <typename FRAG_T, typename CONTEXT_T>
class JavaDefaultAppBase;

template <typename APP_T>
class JavaDefaultWorker {
  static_assert(std::is_base_of<JavaDefaultAppBase<typename APP_T::fragment_t,
                                                   typename APP_T::context_t>,
                                APP_T>::value,
                "JavaDefaultWorker should work with JavaDefaultApp");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using context_t = typename APP_T::context_t;

  using message_manager_t = JavaDefaultMessageManager<fragment_t>;

  static_assert(check_app_fragment_consistency<APP_T, fragment_t>(),
                "The loaded graph is not valid for application");

  JavaDefaultWorker(std::shared_ptr<APP_T> app,
                    std::shared_ptr<fragment_t> graph)
      : app_(app), graph_(graph) {}

  virtual ~JavaDefaultWorker() {}

  void Init(const CommSpec& comm_spec,
            const ParallelEngineSpec& pe_spec = DefaultParallelEngineSpec()) {
    // prepare for the query
    graph_->PrepareToRunApp(APP_T::message_strategy, APP_T::need_split_edges);

    comm_spec_ = comm_spec;

    messages_.Init(comm_spec_.comm(), graph_);

    InitParallelEngine(app_, pe_spec);
    InitCommunicator(app_, comm_spec_.comm());
  }

  void Finalize() {}

  template <class... Args>
  void Query(Args&&... args) {
    MPI_Barrier(comm_spec_.comm());

    context_ = std::make_shared<context_t>();
    context_->Init(*graph_, messages_, std::forward<Args>(args)...);
    if (comm_spec_.worker_id() == kCoordinatorRank) {
      VLOG(1) << "[Coordinator]: Finished Init context";
    }

    int round = 0;

    messages_.Start();

    messages_.StartARound();
    double peval_time = -GetCurrentTime();
    app_->PEval(*graph_, *context_, messages_);
    peval_time += GetCurrentTime();

    messages_.FinishARound();

    if (comm_spec_.worker_id() == kCoordinatorRank) {
      VLOG(1) << "[Coordinator]: Finished PEval, time " << peval_time;
    }

    int step = 1;
    double total_inc_time = -GetCurrentTime();
    double start_around = 0.0;
    double finish_around = 0.0;
    double inc_eval_time = 0.0;
    while (!messages_.ToTerminate()) {
      round++;
      start_around -= GetCurrentTime();
      messages_.StartARound();
      start_around += GetCurrentTime();

      inc_eval_time -= GetCurrentTime();
      app_->IncEval(*graph_, *context_, messages_);
      inc_eval_time += GetCurrentTime();

      finish_around -= GetCurrentTime();
      messages_.FinishARound();
      finish_around += GetCurrentTime();
      if (comm_spec_.worker_id() == kCoordinatorRank) {
        VLOG(1) << "[Coordinator]: Finished IncEval - " << step;
      }
      ++step;
    }
    MPI_Barrier(comm_spec_.comm());
    messages_.Finalize();
    total_inc_time += GetCurrentTime();
    VLOG(1) << "[worker: " << comm_spec_.worker_id()
            << "], totoal inc time: " << total_inc_time << ", start a round"
            << start_around << ", finish around " << finish_around
            << ", incEval " << inc_eval_time;
  }

  void Output(std::ostream& os) { context_->Output(os); }

 private:
  std::shared_ptr<APP_T> app_;
  std::shared_ptr<fragment_t> graph_;
  std::shared_ptr<context_t> context_;
  message_manager_t messages_;

  CommSpec comm_spec_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_WORKER_H_
