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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_MANAGER_BASE_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_MANAGER_BASE_H_

#include <mpi.h>

#include "core/parallel/java_messages.h"
#include "grape/communication/sync_comm.h"
#include "grape/config.h"
#include "grape/parallel/message_manager_base.h"

namespace gs {

/**
 * @brief MessageManagerBase is the base class for message managers.
 *
 * @note: The pure virtual functions in the class work as interfaces,
 * instructing sub-classes to implement. The override functions in the
 * derived classes would be invoked directly, not via virtual functions.
 *
 */
template <typename GRAPH_T>
class JavaMessageManagerBase {
 public:
  JavaMessageManagerBase() {}
  virtual ~JavaMessageManagerBase() {}

  /**
   * @brief Initialize message manager.
   *
   * @param comm MPI_Comm object.
   */
  virtual void Init(MPI_Comm comm, std::shared_ptr<GRAPH_T> graph) = 0;

  /**
   * @brief This function will be called before Init step of applications.
   */
  virtual void Start() = 0;

  /**
   * @brief This function will be called before each evaluation step of
   * applications.
   */
  virtual void StartARound() = 0;

  /**
   * @brief This function will be called after each evaluation step of
   * applications.
   */
  virtual void FinishARound() = 0;

  /**
   * @brief This function will be called after the evaluation of applications.
   */
  virtual void Finalize() = 0;

  /**
   * @brief This function will be called by worker after a step to determine
   * whether evaluation is terminated.
   *
   * @return Whether evaluation is terminated.
   */
  virtual bool ToTerminate() = 0;

  /**
   * @brief Get size of messages sent by this message manager instance.
   * The return value is valid only after FinishARound is called.
   * StartARound will reset the value to zero.
   *
   * @return Size of messages sent by this message manager instance.
   */
  virtual size_t GetMsgSize() const = 0;

  /**
   * @brief Force continue to evaluate one more round even if all workers stop
   * sending message.
   *
   * This function can be called by applications.
   */
  virtual void ForceContinue() = 0;

  /**
   * @brief Force all workers terminate after this round of evaluation.
   *
   * This function can be called by applications.
   * @param info Termination info.
   */
  virtual void ForceTerminate(const std::string& info = "") = 0;

  /**
   * @brief This function is called to get gathered termination info after
   * evaluation finished.
   *
   * @return Termination info.
   */
  virtual const TerminateInfo& GetTerminateInfo() const = 0;

  inline void sumDoubleComm(double msg_in, grape::DoubleMsg& msg_out,
                            const MPI_Comm& comm_) {
    double sum = 0.0;
    AllReduce<double>(comm_, msg_in, sum,
                      [](double& lhs, const double& rhs) { lhs += rhs; });
    msg_out.data = sum;
  }
  inline void sumDoubleComm2(double msg_in, grape::DoubleMsg& msg_out,
                             const MPI_Comm& comm_) {
    double sum = 0.0;
    AllReduce2<double>(comm_, msg_in, sum,
                       [](double& lhs, const double& rhs) { lhs += rhs; });
    msg_out.data = sum;
  }

  template <typename T, typename FUNC_T>
  inline void AllReduce(const MPI_Comm& comm_, const T& msg_in, T& msg_out,
                        const FUNC_T& func) {
    int worker_id, worker_num;
    MPI_Comm_rank(comm_, &worker_id);
    MPI_Comm_size(comm_, &worker_num);
    if (worker_id == 0) {
      msg_out = msg_in;
      for (int src_worker = 1; src_worker < worker_num; ++src_worker) {
        T got_msg;
        RecvFrom<T>(src_worker, got_msg, comm_);
        func(msg_out, got_msg);
      }
      for (int dst_worker = 1; dst_worker < worker_num; ++dst_worker) {
        SendTo<T>(dst_worker, msg_out, comm_);
      }
    } else {
      SendTo<T>(0, msg_in, comm_);
      RecvFrom<T>(0, msg_out, comm_);
    }
  }

  // FIXME: check the usage of this function.
  template <typename T, typename FUNC_T>
  inline void AllReduce2(const MPI_Comm& comm_, const T& msg_in, T& msg_out,
                         const FUNC_T& func) {
    int worker_id, worker_num;
    MPI_Comm_rank(comm_, &worker_id);
    MPI_Comm_size(comm_, &worker_num);
    double local_sum = 0.0;
    MPI_Allreduce(&msg_in, &local_sum, 1, MPI_DOUBLE, MPI_SUM, comm_);
    VLOG(1) << "worker " << worker_id << " sum " << local_sum;
    msg_out = local_sum;
  }
  template <typename T>
  inline void SendTo(fid_t fid, const T& msg, const MPI_Comm& comm_) {
    int dst_worker = fid;
    InArchive arc;
    arc << msg;
    grape::SendArchive(arc, dst_worker, comm_);
  }

  template <typename T>
  static inline void RecvFrom(fid_t fid, T& msg, const MPI_Comm& comm_) {
    int src_worker = fid;
    OutArchive arc;
    grape::RecvArchive(arc, src_worker, comm_);
    arc >> msg;
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_MANAGER_BASE_H_
