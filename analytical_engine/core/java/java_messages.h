
#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_
#ifdef ENABLE_JAVA_SDK
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include <string>
namespace gs {
/**
 * @brief Since Java can not pass Double, Long as reference, we need a wrapper
 * for primitives.
 */
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
inline DoubleMsg& operator+=(DoubleMsg& lhs, const DoubleMsg& rhs) {
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
inline LongMsg& operator+=(LongMsg& lhs, const LongMsg& rhs) {
  lhs.data += rhs.data;
  return lhs;
}

}  // namespace gs
#endif
#endif  // MESSAGES_H
