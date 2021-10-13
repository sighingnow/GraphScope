
#ifndef ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_
#define ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_

#ifdef ENABLE_JAVA_SDK

#include <string>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace gs {

/**
 * @brief Since Java can not pass Double, Long as reference, we need a wrapper
 * for primitives.
 */
template <typename T>
struct PrimitiveMessage {
  T data;

  PrimitiveMessage() { data = (T) -1; }
  PrimitiveMessage(const T in_data) : data(in_data) {}
  inline void setData(const T value) { data = value; }
  inline T getData() { return data; }

  friend inline grape::OutArchive& operator>>(grape::OutArchive& out_archive,
                                              PrimitiveMessage<T>& msg) {
    out_archive >> msg.data;
    return out_archive;
  }
  friend inline grape::InArchive& operator<<(grape::InArchive& in_archive,
                                             const PrimitiveMessage<T>& msg) {
    in_archive << msg.data;
    return in_archive;
  }
};

using DoubleMsg = PrimitiveMessage<double>;
using LongMsg = PrimitiveMessage<int64_t>;

// specify overloaded <, > operators
template <typename T>
inline bool operator<(const PrimitiveMessage<T>& lhs,
                      const PrimitiveMessage<T>& rhs) {
  return lhs.data < rhs.data;
}
template <typename T>
inline bool operator>(const PrimitiveMessage<T>& lhs,
                      const PrimitiveMessage<T>& rhs) {
  return lhs.data < rhs.data;
}
template <typename T>
inline bool operator<=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs > rhs);
}
template <typename T>
inline bool operator>=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs < rhs);
}
template <typename T>
inline bool operator==(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return lhs.data == rhs.data;
}
template <typename T>
inline bool operator!=(const PrimitiveMessage<T>& lhs,
                       const PrimitiveMessage<T>& rhs) {
  return !(lhs == rhs);
}
template <typename T>
inline PrimitiveMessage<T>& operator+=(PrimitiveMessage<T>& lhs,
                                       const PrimitiveMessage<T>& rhs) {
  lhs.data += rhs.data;
  return lhs;
}

template <typename T>
inline PrimitiveMessage<T>& operator-=(PrimitiveMessage<T>& lhs,
                                       const PrimitiveMessage<T>& rhs) {
  lhs.data -= rhs.data;
  return lhs;
}

}  // namespace gs
#endif
#endif  // ANALYTICAL_ENGINE_CORE_JAVA_JAVA_MESSAGES_H_
