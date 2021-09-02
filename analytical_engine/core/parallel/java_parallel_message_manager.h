#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_PARALLEL_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_PARALLEL_MESSAGE_MANAGER_H_

#include <mpi.h>

#include <array>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/parallel/java_message_in_buffer.h"
#include "core/parallel/java_message_manager_base.h"
#include "grape/communication/sync_comm.h"
#include "grape/parallel/thread_local_message_buffer.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/concurrent_queue.h"
#include "grape/worker/comm_spec.h"

// #ifdef GRAPE_SDK_CPP_GRAPE_GEN_DEF
// #include "grape_gen_def.h"
// #endif

namespace gs {

template <typename GRAPH_T>
class JavaParallelMessageManager : JavaMessageManagerBase<GRAPH_T> {
  static constexpr size_t default_msg_send_block_size = 2 * 1023 * 1024;
  static constexpr size_t default_msg_send_block_capacity = 2 * 1023 * 1024;

 public:
  JavaParallelMessageManager()
      : comm_(NULL_COMM), comm_dup(NULL_COMM), graph_(NULL) {}
  ~JavaParallelMessageManager() override {
    if (ValidComm(comm_)) {
      MPI_Comm_free(&comm_);
    }
    if (ValidComm(comm_dup)) {
      MPI_Comm_free(&comm_dup);
    }
  }

  /**
   * @brief Inherit
   */
  void Init(MPI_Comm comm, std::shared_ptr<GRAPH_T> graph) override {
    MPI_Comm_dup(comm, &comm_);
    comm_spec_.Init(comm_);
    MPI_Comm_dup(comm_, &comm_dup);
    fid_ = comm_spec_.fid();
    fnum_ = comm_spec_.fnum();

    recv_queues_[0].SetProducerNum(fnum_);
    recv_queues_[1].SetProducerNum(fnum_);

    terminate_info_.Init(fnum_);
    round_ = 0;

    sent_size_ = 0;

    graph_ = *(graph.get());
  }

  /**
   * @brief Inherit
   */
  void Start() override { startRecvThread(); }

  /**
   * @brief Inherit
   */
  void StartARound() override {
    if (round_ != 0) {
      waitSend();
      auto& rq = recv_queues_[round_ % 2];
      if (!to_self_.empty()) {
        for (auto& iarc : to_self_) {
          OutArchive oarc(std::move(iarc));
          rq.Put(std::move(oarc));
        }
        to_self_.clear();
      }
      rq.DecProducerNum();
    }
    sent_size_ = 0;
    startSendThread();
  }

  /**
   * @brief Inherit
   */
  void FinishARound() override {
    sent_size_ = finishMsgFilling();
    resetRecvQueue();
    round_++;
  }

  /**
   * @brief Inherit
   */
  bool ToTerminate() override {
    int flag = 1;
    if (sent_size_ == 0 && !force_continue_) {
      flag = 0;
    }
    int ret;
    MPI_Allreduce(&flag, &ret, 1, MPI_INT, MPI_SUM, comm_);
    return (ret == 0);
  }

  /**
   * @brief Inherit
   */
  void Finalize() override {
    waitSend();
    MPI_Barrier(comm_);
    stopRecvThread();

    MPI_Comm_free(&comm_);
    comm_ = NULL_COMM;
    MPI_Comm_free(&comm_dup);
    comm_dup = NULL_COMM;
  }

  /**
   * @brief Inherit
   */
  void ForceContinue() override { force_continue_ = true; }

  /**
   * @brief Inherit
   */
  size_t GetMsgSize() const override { return sent_size_; }

  void ForceTerminate(const std::string& terminate_info) override {}

  /**
   * @brief Inherit
   */
  const TerminateInfo& GetTerminateInfo() const override {
    return terminate_info_;
  }

  /**
   * @brief Init a set of channels, each channel is a thread local message
   * buffer.
   *
   * @param channel_num Number of channels.
   * @param block_size Size of each channel.
   * @param block_cap Capacity of each channel.
   */
  void InitChannels(int channel_num = 1,
                    size_t block_size = default_msg_send_block_size,
                    size_t block_cap = default_msg_send_block_capacity) {
    channels_.resize(channel_num);
    for (auto& channel : channels_) {
      channel.Init(fnum_, this, block_size, block_cap);
    }
  }

  std::vector<ThreadLocalMessageBuffer<JavaParallelMessageManager<GRAPH_T>>>&
  Channels() {
    return channels_;
  }

  /**
   * @brief Send a buffer to a fragment.
   *
   * @param fid Destination fragment id.
   * @param arc Message buffer.
   */
  inline void SendRawMsgByFid(fid_t fid, InArchive&& arc) {
    std::pair<fid_t, InArchive> item;
    item.first = fid;
    item.second = std::move(arc);
    sending_queue_.Put(std::move(item));
  }

  /**
   * @brief SyncStateOnOuterVertex on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename VERTEX_T, typename MSG_T>
  inline void SyncStateOnOuterVertex(const VERTEX_T& v, const MSG_T& msg,
                                     int channel_id) {
    channels_[channel_id].template SyncStateOnOuterVertex<GRAPH_T, MSG_T>(
        graph_, v, msg);
  }

  template <typename VERTEX_T>
  inline void SyncStateOnOuterVertex(const VERTEX_T& v, int channel_id) {
    channels_[channel_id].template SyncStateOnOuterVertex<GRAPH_T>(graph_, v);
  }

  inline void SyncStateOnOuterVertexLong(const typename GRAPH_T::vertex_t& v,
                                         const grape::LongMsg& msg,
                                         int channel_id) {
    channels_[channel_id]
        .template SyncStateOnOuterVertex<GRAPH_T, grape::LongMsg>(graph_, v,
                                                                  msg);
  }
  inline void SyncStateOnOuterVertexDouble(const typename GRAPH_T::vertex_t& v,
                                           const grape::DoubleMsg& msg,
                                           int channel_id) {
    channels_[channel_id]
        .template SyncStateOnOuterVertex<GRAPH_T, grape::DoubleMsg>(graph_, v,
                                                                    msg);
  }

  inline void SyncStateOnOuterVertex(const typename GRAPH_T::vertex_t& v,
                                     int channel_id = 0) {
    channels_[channel_id].template SyncStateOnOuterVertex<GRAPH_T>(graph_, v);
  }

  /**
   * @brief SendMsgThroughIEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename VERTEX_T, typename MSG_T>
  inline void SendMsgThroughIEdges(const VERTEX_T& v, const MSG_T& msg,
                                   int channel_id = 0) {
    channels_[channel_id].template SendMsgThroughIEdges<GRAPH_T, MSG_T>(graph_,
                                                                        v, msg);
  }

  /**
   * @brief SendMsgThroughOEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename VERTEX_T, typename MSG_T>
  inline void SendMsgThroughOEdges(const VERTEX_T& v, const MSG_T& msg,
                                   int channel_id = 0) {
    channels_[channel_id].template SendMsgThroughOEdges<GRAPH_T, MSG_T>(graph_,
                                                                        v, msg);
  }

  inline void sendMsgThroughOEdgesDouble(const typename GRAPH_T::vertex_t v,
                                         const double& msg, int channel_id) {
    channels_[channel_id].template SendMsgThroughOEdges<GRAPH_T, double>(
        graph_, v, msg);
  }

  /**
   * @brief SendMsgThroughEdges on a channel.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @param frag Source fragment.
   * @param v Source vertex.
   * @param msg
   * @param channel_id
   */
  template <typename VERTEX_T, typename MSG_T>
  inline void SendMsgThroughEdges(const VERTEX_T& v, const MSG_T& msg,
                                  int channel_id = 0) {
    channels_[channel_id].template SendMsgThroughEdges<GRAPH_T, MSG_T>(graph_,
                                                                       v, msg);
  }

  /**
   * @brief Parallel process all incoming messages with given function of last
   * round.
   *
   * @tparam GRAPH_T Graph type.
   * @tparam MESSAGE_T Message type.
   * @tparam FUNC_T Function type.
   * @param thread_num Number of threads.
   * @param frag
   * @param func
   */
  template <typename MESSAGE_T, typename FUNC_T>
  inline void ParallelProcess(int thread_num, const FUNC_T& func) {
    std::vector<std::thread> threads(thread_num);

    for (int i = 0; i < thread_num; ++i) {
      threads[i] = std::thread(
          [&](int tid) {
            typename GRAPH_T::vid_t id;
            typename GRAPH_T::vertex_t vertex;
            MESSAGE_T msg;
            auto& que = recv_queues_[round_ % 2];
            OutArchive arc;
            while (que.Get(arc)) {
              while (!arc.Empty()) {
                arc >> id >> msg;
                graph_.Gid2Vertex(id, vertex);
                func(tid, vertex, msg);
              }
            }
          },
          i);
    }

    for (auto& thrd : threads) {
      thrd.join();
    }
  }

  inline jboolean GetMessageInBuffer(JavaMessageInBuffer<GRAPH_T>& buf) {
    OutArchive arc;
    auto& que = recv_queues_[round_ % 2];
    if (que.Get(arc)) {
      buf.Init(std::move(arc), graph_);
      return true;
    } else {
      return false;
    }
  }
  inline void sumDouble(double msg_in, grape::DoubleMsg& msg_out) {
    JavaMessageManagerBase<GRAPH_T>::sumDoubleComm2(msg_in, msg_out, comm_dup);
  }

 private:
  void startSendThread() {
    force_continue_ = false;
    int round = round_;

    CHECK_EQ(sending_queue_.Size(), 0);
    sending_queue_.SetProducerNum(1);
    send_thread_ = std::thread(
        [this](int msg_round) {
          std::vector<MPI_Request> reqs;
          std::pair<fid_t, InArchive> item;
          while (sending_queue_.Get(item)) {
            if (item.second.GetSize() == 0) {
              continue;
            }
            if (item.first == fid_) {
              to_self_.emplace_back(std::move(item.second));
            } else {
              MPI_Request req;
              MPI_Isend(item.second.GetBuffer(), item.second.GetSize(),
                        MPI_CHAR, comm_spec_.FragToWorker(item.first),
                        msg_round, comm_, &req);
              reqs.push_back(req);
              to_others_.emplace_back(std::move(item.second));
            }
          }
          for (fid_t i = 0; i < fnum_; ++i) {
            if (i == fid_) {
              continue;
            }
            MPI_Request req;
            MPI_Isend(NULL, 0, MPI_CHAR, comm_spec_.FragToWorker(i), msg_round,
                      comm_, &req);
            reqs.push_back(req);
          }
          MPI_Waitall(reqs.size(), &reqs[0], MPI_STATUSES_IGNORE);
          to_others_.clear();
        },
        round + 1);
  }

  void probeAllIncomingMessages() {
    MPI_Status status;
    while (true) {
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm_, &status);
      if (status.MPI_SOURCE == comm_spec_.worker_id()) {
        MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, 0, comm_,
                 MPI_STATUS_IGNORE);
        return;
      }
      int tag = status.MPI_TAG;
      int count;
      MPI_Get_count(&status, MPI_CHAR, &count);
      if (count == 0) {
        MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, tag, comm_,
                 MPI_STATUS_IGNORE);
        recv_queues_[tag % 2].DecProducerNum();
        // end of msg receiving when receive empty msg
      } else {
        OutArchive arc(count);
        MPI_Recv(arc.GetBuffer(), count, MPI_CHAR, status.MPI_SOURCE, tag,
                 comm_, MPI_STATUS_IGNORE);
        recv_queues_[tag % 2].Put(std::move(arc));
      }
    }
  }

  int probeIncomingMessages() {
    int gotMessage = 0;
    int flag;
    MPI_Status status;
    while (true) {
      MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, comm_, &flag, &status);
      if (flag) {
        if (status.MPI_SOURCE == comm_spec_.worker_id()) {
          MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, 0, comm_,
                   MPI_STATUS_IGNORE);
          return -1;
        }
        gotMessage = 1;
        int tag = status.MPI_TAG;
        int count;
        MPI_Get_count(&status, MPI_CHAR, &count);
        if (count == 0) {
          MPI_Recv(NULL, 0, MPI_CHAR, status.MPI_SOURCE, tag, comm_,
                   MPI_STATUS_IGNORE);
          recv_queues_[tag % 2].DecProducerNum();
        } else {
          OutArchive arc(count);
          MPI_Recv(arc.GetBuffer(), count, MPI_CHAR, status.MPI_SOURCE, tag,
                   comm_, MPI_STATUS_IGNORE);
          recv_queues_[tag % 2].Put(std::move(arc));
        }
      } else {
        break;
      }
    }
    return gotMessage;
  }

  void startRecvThread() {
    recv_thread_ = std::thread([this]() {
#if 0
      int idle_time = 0;
      while (true) {
        int gotMessage = probeIncomingMessages();
        if (gotMessage == -1) {
          break;
        }
        idle_time += static_cast<int>(gotMessage == 0);
        if (idle_time > 10) {
          poll(NULL, 0, 1);
          idle_time = 0;
        } else if (gotMessage == 0) {
#if __APPLE__
          sched_yield();
#else
          pthread_yield();
#endif
        }
      }
#else
      probeAllIncomingMessages();
#endif
    });
  }

  void stopRecvThread() {
    MPI_Send(NULL, 0, MPI_CHAR, comm_spec_.worker_id(), 0, comm_);
    recv_thread_.join();
  }

  inline size_t finishMsgFilling() {
    size_t ret = 0;
    for (auto& channel : channels_) {
      channel.FlushMessages();
      ret += channel.SentMsgSize();
      channel.Reset();
    }
    sending_queue_.DecProducerNum();
    return ret;
  }

  void resetRecvQueue() {
    auto& curr_recv_queue = recv_queues_[round_ % 2];
    if (round_) {
      OutArchive arc;
      while (curr_recv_queue.Get(arc)) {}
    }
    curr_recv_queue.SetProducerNum(fnum_);
  }

  void waitSend() { send_thread_.join(); }

  fid_t fid_;
  fid_t fnum_;
  grape::CommSpec comm_spec_;

  MPI_Comm comm_;
  MPI_Comm comm_dup;

  std::vector<InArchive> to_self_;
  std::vector<InArchive> to_others_;

  std::vector<ThreadLocalMessageBuffer<JavaParallelMessageManager>> channels_;
  int round_;

  BlockingQueue<std::pair<fid_t, InArchive>> sending_queue_;
  std::thread send_thread_;

  std::array<BlockingQueue<OutArchive>, 2> recv_queues_;
  std::thread recv_thread_;

  TerminateInfo terminate_info_;
  bool force_continue_;
  size_t sent_size_;
  GRAPH_T graph_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_PARALLEL_MESSAGE_MANAGER_H_
