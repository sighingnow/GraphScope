#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_IN_BUFFER_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_IN_BUFFER_H_

#include <jni.h>

#include "core/parallel/messages.h"
#include "grape/serialization/out_archive.h"

namespace grape {

template <typename GRAPH_T>
class JavaMessageInBuffer {
 public:
  JavaMessageInBuffer() {}

  explicit JavaMessageInBuffer(OutArchive&& arc) : arc_(std::move(arc)) {}

  void Init(OutArchive&& arc, std::shared_ptr<GRAPH_T> graph) {
    arc_ = std::move(arc);
    graph_ = graph;
  }

  template <typename MESSAGE_T>
  inline bool GetMessage(MESSAGE_T& msg) {
    if (arc_.Empty()) {
      return false;
    }
    arc_ >> msg;
    return true;
  }

  template <typename MESSAGE_T>
  inline bool GetMessage(typename GRAPH_T::vertex_t& v, MESSAGE_T& msg) {
    if (arc_.Empty()) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    arc_ >> gid >> msg;
    graph_->Gid2Vertex(gid, v);
    return true;
  }

  template <typename VERTEX_T, typename MESSAGE_T>
  inline jboolean GetMessage(VERTEX_T& v, MESSAGE_T& msg) {
    if (arc_.Empty()) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    arc_ >> gid >> msg;
    graph_->Gid2Vertex(gid, v);
    return true;
  }

  inline jboolean getMessageLong(typename GRAPH_T::vertex_t& v,
                                 grape::LongMsg& msg) {
    if (arc_.Empty()) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    arc_ >> gid >> msg;
    graph_->Gid2Vertex(gid, v);
    return true;
  }
  inline jboolean getMessageDouble(typename GRAPH_T::vertex_t& v,
                                   grape::DoubleMsg& msg) {
    if (arc_.Empty()) {
      return false;
    }
    typename GRAPH_T::vid_t gid;
    arc_ >> gid >> msg;
    graph_->Gid2Vertex(gid, v);
    return true;
  }

 private:
  OutArchive arc_;
  std::shared_ptr<GRAPH_T> graph_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_PARALLEL_JAVA_MESSAGE_IN_BUFFER_H_
