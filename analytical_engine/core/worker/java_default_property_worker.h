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

#ifndef ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_PROPERTY_WORKER_H_
#define ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_PROPERTY_WORKER_H_

#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

#include "core/parallel/property_message_manager.h"
#include "grape/communication/communicator.h"
#include "grape/config.h"
// #include "grape/parallel/parallel_engine.h"
#include "grape/communication/sync_comm.h"
#include "grape/parallel/parallel_engine.h"
#include "grape/util.h"
#include "grape/worker/comm_spec.h"

namespace gs {

template <typename FRAG_T, typename CONTEXT_T>
class JavaDefaultPropertyAppBase;

template <typename APP_T>
class JavaDefaultPropertyWorker {
  static_assert(
      std::is_base_of<JavaDefaultPropertyAppBase<typename APP_T::fragment_t,
                                                 typename APP_T::context_t>,
                      APP_T>::value,
      "JavaDefaultPropertyWorker should work with JavaDefaultPropertyApp");

 public:
  using fragment_t = typename APP_T::fragment_t;
  using context_t = typename APP_T::context_t;

  using message_manager_t = gs::PropertyMessageManager;

  // static_assert(check_app_fragment_consistency<APP_T, fragment_t>(),
  //               "The loaded graph is not valid for application");

  JavaDefaultPropertyWorker(std::shared_ptr<APP_T> app,
                            std::shared_ptr<fragment_t> graph)
      : app_(app), context_(std::make_shared<context_t>(*graph)) {}

  virtual ~JavaDefaultPropertyWorker() {}

  void Init(const grape::CommSpec& comm_spec,
            const grape::ParallelEngineSpec& pe_spec =
                grape::DefaultParallelEngineSpec()) {
    auto& graph = const_cast<fragment_t&>(context_->fragment());
    // prepare for the query
    graph.PrepareToRunApp(APP_T::message_strategy, APP_T::need_split_edges);

    comm_spec_ = comm_spec;
    MPI_Barrier(comm_spec_.comm());
    // TODO: remove graph parameter
    messages_.Init(comm_spec_.comm());

    InitParallelEngine(app_, pe_spec);
    grape::InitCommunicator(app_, comm_spec_.comm());
  }

  void Finalize() {}
  template <class... Args>
  void Query(Args&&... args) {
    auto& graph = context_->fragment();
    MPI_Barrier(comm_spec_.comm());
    // Local num is used to reserve java memory
    context_->SetLocalNum(comm_spec_.local_num());

    context_->Init(messages_, std::forward<Args>(args)...);
    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      VLOG(1) << "[Coordinator]: Finished Init context";
    }

    int round = 0;

    messages_.Start();

    messages_.StartARound();
    double peval_time = -grape::GetCurrentTime();
    app_->PEval(graph, *context_, messages_);
    peval_time += grape::GetCurrentTime();

    messages_.FinishARound();

    if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
      VLOG(1) << "[Coordinator]: Finished PEval, time " << peval_time;
    }

    int step = 1;
    double total_inc_time = -grape::GetCurrentTime();
    double start_around = 0.0;
    double finish_around = 0.0;
    double inc_eval_time = 0.0;
    while (!messages_.ToTerminate()) {
      round++;
      start_around -= grape::GetCurrentTime();
      messages_.StartARound();
      start_around += grape::GetCurrentTime();

      inc_eval_time -= grape::GetCurrentTime();
      app_->IncEval(graph, *context_, messages_);
      inc_eval_time += grape::GetCurrentTime();

      finish_around -= grape::GetCurrentTime();
      messages_.FinishARound();
      finish_around += grape::GetCurrentTime();
      if (comm_spec_.worker_id() == grape::kCoordinatorRank) {
        VLOG(1) << "[Coordinator]: Finished IncEval - " << step;
      }
      ++step;
    }
    MPI_Barrier(comm_spec_.comm());
    messages_.Finalize();
    total_inc_time += grape::GetCurrentTime();
    VLOG(1) << "[worker: " << comm_spec_.worker_id()
            << "], totoal inc time: " << total_inc_time << ", start a round"
            << start_around << ", finish around " << finish_around
            << ", incEval " << inc_eval_time;
  }

  // must get after query
  std::shared_ptr<context_t> GetContext() { return context_; }

  void Output(std::ostream& os) { context_->Output(os); }

 private:
  std::shared_ptr<APP_T> app_;
  std::shared_ptr<context_t> context_;
  message_manager_t messages_;

  grape::CommSpec comm_spec_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_WORKER_JAVA_DEFAULT_PROPERTY_WORKER_H_
