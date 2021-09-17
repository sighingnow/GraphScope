#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGE_IN_BUFFER_H
#define ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGE_IN_BUFFER_H

#include "grape/serialization/out_archive.h"

namespace gs {

class MessageInBuffer {
 public:
  MessageInBuffer() {}

  explicit MessageInBuffer(grape::OutArchive&& arc) : arc_(std::move(arc)) {}

  void Init(grape::OutArchive&& arc) { arc_ = std::move(arc); }

  template <typename MESSAGE_T>
  inline bool GetMessage(MESSAGE_T& msg) {
    if (arc_.Empty()) {
      return false;
    }
    arc_ >> msg;
    return true;
  }

  template <typename GRAPH_T, typename MESSAGE_T>
  inline bool GetMessage(const GRAPH_T& frag, typename GRAPH_T::vertex_t& v,
                         MESSAGE_T& msg) {
    if (arc_.Empty()) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    arc_ >> gid >> msg;
    frag.Gid2Vertex(gid, v);
    return true;
  }

 private:
  grape::OutArchive arc_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGE_IN_BUFFER_H