
#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include <string>
namespace gs {
class DoubleMsg {
 public:
  DoubleMsg() { data = -1.0; }
  DoubleMsg(double in_data) : data(in_data) {}
  ~DoubleMsg() {}
  inline void setData(double value) { data = value; }
  inline double getData() { return data; }

  double data;
};

class LongMsg {
 public:
  LongMsg() { data = -1; }
  LongMsg(uint64_t in_data) : data(in_data) {}
  ~LongMsg() {}
  inline void setData(uint64_t value) { data = value; }
  inline uint64_t getData() { return data; }
  uint64_t data;
};

class StringMsg {
 public:
  StringMsg() { data = nullptr; }
  StringMsg(std::string& in_data) { data = in_data; }
  ~StringMsg() {}
  inline void setData(std::string value) { data = value; }
  inline std::string& getData() { return data; }
  std::string data;
};
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     DoubleMsg& msg) {
  out_archive >> msg.data;
  return out_archive;
}
inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const DoubleMsg& msg) {
  in_archive << msg.data;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     LongMsg& msg) {
  out_archive >> msg.data;
  return out_archive;
}
inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const LongMsg& msg) {
  in_archive << msg.data;
  return in_archive;
}
inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                     StringMsg& msg) {
  out_archive >> msg.data;
  return out_archive;
}
inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                    const StringMsg& msg) {
  in_archive << msg.data;
  return in_archive;
}
// specify overloaded <, > operators
inline bool operator<(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return lhs.data < rhs.data;
}
inline bool operator>(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return rhs < lhs;
}
inline bool operator<=(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return !(lhs > rhs);
}
inline bool operator>=(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return !(lhs < rhs);
}
inline bool operator==(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return lhs.data == rhs.data;
}
inline bool operator!=(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  return !(lhs == rhs);
}
inline DoubleMsg& operator+=(const DoubleMsg& lhs, const DoubleMsg& rhs) {
  lhs.data += rhs.data;
  return lhs;
}

inline bool operator<(const LongMsg& lhs, const LongMsg& rhs) {
  return lhs.data < rhs.data;
}
inline bool operator>(const LongMsg& lhs, const LongMsg& rhs) {
  return rhs < lhs;
}
inline bool operator<=(const LongMsg& lhs, const LongMsg& rhs) {
  return !(lhs > rhs);
}
inline bool operator>=(const LongMsg& lhs, const LongMsg& rhs) {
  return !(lhs < rhs);
}
inline bool operator==(const LongMsg& lhs, const LongMsg& rhs) {
  return lhs.data == rhs.data;
}
inline bool operator!=(const LongMsg& lhs, const LongMsg& rhs) {
  return !(lhs == rhs);
}
inline LongMsg& operator+=(const LongMsg& lhs, const LongMsg& rhs) {
  lhs.data += rhs.data;
  return lhs;
}

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

#endif  // MESSAGES_H
