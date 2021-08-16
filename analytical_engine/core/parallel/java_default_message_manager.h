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

#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_DEFAULT_MESSAGE_MANAGER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_DEFAULT_MESSAGE_MANAGER_H_

#include <iostream>
#include <memory>
#include <utility>
#include <vector>
#include "jni.h"

#include "grape/communication/sync_comm.h"
// #include "grape/fragment/immutable_edgecut_fragment.h"
#include "core/parallel/java_message_manager_base.h"
#include "core/parallel/messages.h"
#include "grape/graph/adj_list.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#include "grape/utils/long_vector.h"
#include "grape/worker/comm_spec.h"

// only include codegen types in JNI library building, skip this in normal
// building
#ifdef GRAPE_SDK_CPP_GRAPE_GEN_DEF
#include "grape_gen_def.h"
#endif

namespace grape {

/**
 * @brief Default message manager.
 *
 * The send and recv methods are not thread-safe.
 */
template <typename GRAPH_T>
class JavaDefaultMessageManager : public JavaMessageManagerBase<GRAPH_T> {
 public:
  JavaDefaultMessageManager() : comm_(NULL_COMM), graph_(NULL) {}
  ~JavaDefaultMessageManager() override {
    if (ValidComm(comm_)) {
      MPI_Comm_free(&comm_);
    }
  }

  /**
   * @brief Inherit
   */
  void Init(MPI_Comm comm, std::shared_ptr<GRAPH_T> graph) override {
    MPI_Comm_dup(comm, &comm_);

    comm_spec_.Init(comm_);
    fid_ = comm_spec_.fid();
    fnum_ = comm_spec_.fnum();

    force_terminate_ = false;
    terminate_info_.Init(fnum_);

    lengths_out_.resize(fnum_);
    lengths_in_.resize(fnum_ * fnum_);

    to_send_.resize(fnum_);
    to_recv_.resize(fnum_);
    graph_ = graph;
  }

  /**
   * @brief Inherit
   */
  void Start() override {}

  /**
   * @brief Inherit
   */
  void StartARound() override {
    sent_size_ = 0;
    if (!reqs_.empty()) {
      MPI_Waitall(reqs_.size(), &reqs_[0], MPI_STATUSES_IGNORE);
      reqs_.clear();
    }
    for (auto& arc : to_send_) {
      arc.Clear();
    }
    force_continue_ = false;
    cur_ = 0;
  }

  /**
   * @brief Inherit
   */
  void FinishARound() override {
    to_terminate_ = syncLengths();
    if (to_terminate_) {
      return;
    }
    for (fid_t i = 1; i < fnum_; ++i) {
      fid_t src_fid = (fid_ + i) % fnum_;
      size_t length = lengths_in_[src_fid * fnum_ + fid_];
      if (length == 0) {
        continue;
      }
      auto& arc = to_recv_[src_fid];
      arc.Clear();
      arc.Allocate(length);
      MPI_Request req;
      MPI_Irecv(arc.GetBuffer(), length, MPI_CHAR,
                comm_spec_.FragToWorker(src_fid), 0, comm_, &req);
      // VLOG(2) << "frag " << fid_ << " receive message size " << length
      //         << " from " << src_fid;
      reqs_.push_back(req);
    }

    size_t cnt = 0;

    for (fid_t i = 1; i < fnum_; ++i) {
      fid_t dst_fid = (fid_ + fnum_ - i) % fnum_;
      auto& arc = to_send_[dst_fid];
      if (arc.Empty()) {
        continue;
      }
      cnt += arc.GetSize();

      MPI_Request req;
      MPI_Isend(arc.GetBuffer(), arc.GetSize(), MPI_CHAR,
                comm_spec_.FragToWorker(dst_fid), 0, comm_, &req);
      reqs_.push_back(req);
    }
    VLOG(2) << "frag " << fid_ << " totally send " << cnt << "msg";

    to_recv_[fid_].Clear();
    // VLOG(2) << "frag " << fid_ << " sending message to self "
    //         << to_send_[fid_].GetSize();
    if (!to_send_[fid_].Empty()) {
      to_recv_[fid_] = std::move(to_send_[fid_]);
    }
  }

  /**
   * @brief Inherit
   */
  bool ToTerminate() override { return to_terminate_; }

  /**
   * @brief Inherit
   */
  void Finalize() override {
    if (!reqs_.empty()) {
      MPI_Waitall(reqs_.size(), &reqs_[0], MPI_STATUSES_IGNORE);
      reqs_.clear();
    }

    MPI_Comm_free(&comm_);
    comm_ = NULL_COMM;
  }

  /**
   * @brief Inherit
   */
  size_t GetMsgSize() const override { return sent_size_; }

  /**
   * @brief Inherit
   */
  void ForceContinue() override { force_continue_ = true; }

  /**
   * @brief Inherit
   */
  void ForceTerminate(const std::string& terminate_info) override {
    force_terminate_ = true;
    terminate_info_.info[comm_spec_.fid()] = terminate_info;
  }

  /**
   * @brief Inherit
   */
  const TerminateInfo& GetTerminateInfo() const override {
    return terminate_info_;
  }
  /**
   * @brief Send message to a fragment.
   *
   * @tparam MESSAGE_T Message type.
   * @param dst_fid Destination fragment id.
   * @param msg
   */
  template <typename MESSAGE_T>
  inline void SendToFragment(fid_t dst_fid, const MESSAGE_T& msg) {
    to_send_[dst_fid] << msg;
  }

  /**
   * @brief Communication by synchronizing the status on outer vertices, for
   * edge-cut fragments.
   *
   * Assume a fragment F_1, a crossing edge a->b' in F_1 and a is an inner
   * vertex in F_1. This function invoked on F_1 send status on b' to b on F_2,
   * where b is an inner vertex.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename VERTEX_T, typename MESSAGE_T>
  inline void SyncStateOnOuterVertex(const VERTEX_T& v, const MESSAGE_T& msg) {
    fid_t fid = graph_->GetFragId(v);
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }
  template <typename VERTEX_T>
  inline void SyncStateOnOuterVertexDouble(const VERTEX_T& v,
                                           const grape::DoubleMsg& msg) {
    fid_t fid = graph_->GetFragId(v);
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }

  template <typename VERTEX_T>
  inline void SyncStateOnOuterVertex(const VERTEX_T& v) {
    fid_t fid = graph_->GetFragId(v);
    to_send_[fid] << graph_->GetOuterVertexGid(v);
  }

  inline void SyncStateOnOuterVertexLong(const typename GRAPH_T::vertex_t& v,
                                         const jlong& msg) {
    fid_t fid = graph_->GetFragId(v);
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }
  inline void SyncStateOnOuterVertexDouble(const typename GRAPH_T::vertex_t& v,
                                           const double& msg) {
    fid_t fid = graph_->GetFragId(v);
    // LOG(INFO) << "syncing " <<v.GetValue() << ", fid " << fid << ",this frag
    // id " << fid_ << msg.data;
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }
  inline void SyncStateOnOuterVertexLong(const typename GRAPH_T::vid_t& v,
                                         const jlong& msg) {
    fid_t fid = graph_->GetFragId(v);
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }
  inline void SyncStateOnOuterVertexDouble(const typename GRAPH_T::vid_t& v,
                                           const double& msg) {
    fid_t fid = graph_->GetFragId(v);
    // LOG(INFO) << "syncing " <<v.GetValue() << ", fid " << fid << ",this frag
    // id " << fid_ << msg.data;
    to_send_[fid] << graph_->GetOuterVertexGid(v) << msg;
  }

  /**
   * @brief Communication via a crossing edge a<-c. It sends message
   * from a to c.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename VERTEX_T, typename MESSAGE_T>
  inline void SendMsgThroughIEdges(const VERTEX_T& v, const MESSAGE_T& msg) {
    DestList dsts = graph_->IEDests(v);
    fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = graph_->GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  /**
   * @brief Communication via a crossing edge a->b. It sends message
   * from a to b.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename VERTEX_T, typename MESSAGE_T>
  inline void SendMsgThroughOEdges(const VERTEX_T& v, const MESSAGE_T& msg) {
    DestList dsts = graph_->OEDests(v);
    fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = graph_->GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  inline void sendMsgThroughOEdgesDouble(const typename GRAPH_T::vertex_t& v,
                                         const double& msg) {
    DestList dsts = graph_->OEDests(v);
    fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = graph_->GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  /**
   * @brief Communication via crossing edges a->b and a<-c. It sends message
   * from a to b and c.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v: a
   * @param msg
   */
  template <typename VERTEX_T, typename MESSAGE_T>
  inline void SendMsgThroughEdges(const VERTEX_T& v, const MESSAGE_T& msg) {
    DestList dsts = graph_->IOEDests(v);
    fid_t* ptr = dsts.begin;
    typename GRAPH_T::vid_t gid = graph_->GetInnerVertexGid(v);
    while (ptr != dsts.end) {
      fid_t fid = *(ptr++);
      to_send_[fid] << gid << msg;
    }
  }

  /**
   * @brief Get a message from message buffer.
   *
   * @tparam MESSAGE_T
   * @param msg
   *
   * @return Return true if got a message, and false if no message left.
   */
  template <typename MESSAGE_T>
  inline bool GetMessage(MESSAGE_T& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      return false;
    }
    to_recv_[cur_] >> msg;
    return true;
  }

  void ParseLongMessages(const GRAPH_T& frag, std::vector<LongVector>& msgsOut,
                         std::vector<LongVector>& msgsIn) {
    msgsOut.swap(msgsIn);
    for (auto& vec : msgsOut) {
      vec.clear();
    }

    typename GRAPH_T::vid_t gid, lid;
    int64_t msg;

    for (auto& arc : to_recv_) {
      while (!arc.Empty()) {
        arc >> gid >> msg;
        lid = frag.GetInnerVertexLid(gid);
        msgsIn[lid].push_back(msg);
      }
    }
  }

  /**
   * @brief Get a message and its target vertex from message buffer.
   *
   * @tparam GRAPH_T
   * @tparam MESSAGE_T
   * @param frag
   * @param v
   * @param msg
   *
   * @return Return true if got a message, and false if no message left.
   */
  template <typename MESSAGE_T>
  inline bool GetMessage(const GRAPH_T& frag, typename GRAPH_T::vertex_t& v,
                         MESSAGE_T& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    to_recv_[cur_] >> gid >> msg;
    frag.Gid2Vertex(gid, v);
    return true;
  }

  void SetMessageToFragment(fid_t fid, std::vector<char>& buf) {
    to_send_[fid].SwapVector(buf);
  }

  // template <typename GRAPH_T, typename MESSAGE_T>
  void sendBatchMessage(fid_t fid, std::vector<jint> gids,
                        std::vector<std::string> buf) {
    if (gids.size() != buf.size()) {
      LOG(INFO) << "gid size and buf size not match";
      return;
    }
    for (size_t i = 0; i < gids.size(); ++i) {
      // LOG(INFO) << "[In sending msg] gid " << gids[i] << "data" << buf[i];
      to_send_[fid] << gids[i] << buf[i];
    }
  }

  void sendBatchLongMessage(fid_t fid, std::vector<jint> gids,
                            std::vector<jlong> buf) {
    if (gids.size() != buf.size()) {
      LOG(INFO) << "gid size and buf size not match";
      return;
    }
    for (size_t i = 0; i < gids.size(); ++i) {
      // LOG(INFO) << "[In sending msg] gid " << gids[i] << "data" << buf[i];
      to_send_[fid] << gids[i] << buf[i];
    }
  }

  void sendBatchDoubleMessage(fid_t fid, std::vector<jlong> gids,
                              std::vector<jdouble> buf) {
    if (gids.size() != buf.size()) {
      LOG(INFO) << "gid size and buf size not match";
      return;
    }
    for (size_t i = 0; i < gids.size(); ++i) {
      // LOG(INFO) << "[In sending msg] gid " << gids[i] << "data" << buf[i];
      to_send_[fid] << gids[i] << buf[i];
    }
  }
  template <typename VERTEX_T, typename MSG_T>
  inline void sendMsgByLid(VERTEX_T& vertex, MSG_T& msg) {
    uint64_t gid = graph_->Vertex2Gid(vertex);
    fid_t fid = graph_->GetFragId(vertex);
    to_send_[fid] << gid << msg;
  }
  inline void sendMsgByLidLong(typename GRAPH_T::vertex_t& vertex,
                               grape::LongMsg& msg) {
    uint64_t gid = graph_->Vertex2Gid(vertex);
    fid_t fid = graph_->GetFragId(vertex);
    to_send_[fid] << gid << msg;
  }
  inline void sendMsgByLidDouble(typename GRAPH_T::vertex_t& vertex,
                                 grape::DoubleMsg& msg) {
    uint64_t gid = graph_->Vertex2Gid(vertex);
    fid_t fid = graph_->GetFragId(vertex);
    to_send_[fid] << gid << msg;
  }
  template <typename VERTEX_T>
  inline jboolean getMsg(VERTEX_T& vertex, grape::LongMsg& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      VLOG(1) << "no msg available";
      return false;
    }
    typename GRAPH_T::vid_t gid;
    to_recv_[cur_] >> gid >> msg;
    graph_->Gid2Vertex(gid, vertex);

    VLOG(1) << "get msg" << gid << " lid " << vertex.GetValue() << " msg  "
            << msg.getData();
    return true;
  }
  template <typename VERTEX_T, typename MSG_T>
  inline jboolean getMsg(VERTEX_T& vertex, MSG_T& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    to_recv_[cur_] >> gid >> msg;
    graph_->Gid2Vertex(gid, vertex);
    return true;
  }

  inline jboolean getMsgLong(typename GRAPH_T::vertex_t& vertex,
                             grape::LongMsg& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    to_recv_[cur_] >> gid >> msg;
    graph_->Gid2Vertex(gid, vertex);
    return true;
  }

  inline jboolean getMsgDouble(typename GRAPH_T::vertex_t& vertex,
                               grape::DoubleMsg& msg) {
    while (cur_ != fnum_ && to_recv_[cur_].Empty()) {
      ++cur_;
    }
    if (cur_ == fnum_) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    to_recv_[cur_] >> gid >> msg;
    graph_->Gid2Vertex(gid, vertex);
    return true;
  }

  inline void getBatchMessage(int fid, std::vector<jint>& gids,
                              std::vector<std::string>& buf) {
    jlong gid;
    std::string tmp;
    while (!to_recv_[fid].Empty()) {
      to_recv_[fid] >> gid >> tmp;
      gids.push_back(gid);
      buf.push_back(tmp);
    }
  }

  inline void getBatchLongMessage(int fid, std::vector<jint>& gids,
                                  std::vector<jlong>& buf) {
    jlong gid;
    jlong tmp;
    while (!to_recv_[fid].Empty()) {
      to_recv_[fid] >> gid >> tmp;
      gids.push_back(gid);
      buf.push_back(tmp);
    }
  }

  inline void getBatchDoubleMessage(int fid, std::vector<jlong>& gids,
                                    std::vector<jdouble>& buf) {
    jlong gid;
    jdouble tmp;
    while (!to_recv_[fid].Empty()) {
      to_recv_[fid] >> gid >> tmp;
      gids.push_back(gid);
      buf.push_back(tmp);
    }
  }
  inline void sumDouble(double msg_in, grape::DoubleMsg& msg_out) {
    JavaMessageManagerBase<GRAPH_T>::sumDoubleComm(msg_in, msg_out, comm_);
  }

 protected:
  fid_t fid() const { return fid_; }
  fid_t fnum() const { return fnum_; }

 private:
  bool syncLengths() {
    for (fid_t i = 0; i < fnum_; ++i) {
      sent_size_ += to_send_[i].GetSize();
      lengths_out_[i] = to_send_[i].GetSize();
    }
    if (force_continue_) {
      ++lengths_out_[fid_];
    }
    // LOG(INFO) << "lengths out " << lengths_out_[fid_];
    int terminate_flag = force_terminate_ ? 1 : 0;
    int terminate_flag_sum;
    MPI_Allreduce(&terminate_flag, &terminate_flag_sum, 1, MPI_INT, MPI_SUM,
                  comm_);
    // LOG(INFO) << "terminate flag sum" << terminate_flag_sum;
    if (terminate_flag_sum > 0) {
      terminate_info_.success = false;
      AllToAll(terminate_info_.info, comm_);
      return true;
    } else {
      MPI_Allgather(&lengths_out_[0], fnum_ * sizeof(size_t), MPI_CHAR,
                    &lengths_in_[0], fnum_ * sizeof(size_t), MPI_CHAR, comm_);
      for (auto s : lengths_in_) {
        if (s != 0) {
          return false;
        }
      }
      // LOG(INFO) << "all length in is zero";
      return true;
    }
  }

  bool checkTermination() {
    for (auto s : lengths_in_) {
      if (s != 0) {
        return false;
      }
    }
    return true;
  }

  std::vector<InArchive> to_send_;
  std::vector<OutArchive> to_recv_;
  fid_t cur_;

  std::vector<size_t> lengths_out_;
  std::vector<size_t> lengths_in_;

  std::vector<MPI_Request> reqs_;

  fid_t fid_;
  fid_t fnum_;
  CommSpec comm_spec_;

  MPI_Comm comm_;

  size_t sent_size_;
  bool to_terminate_;
  bool force_continue_;
  bool force_terminate_;

  TerminateInfo terminate_info_;

  std::shared_ptr<GRAPH_T> graph_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_DEFAULT_MESSAGE_MANAGER_H_
