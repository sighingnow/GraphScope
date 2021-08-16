
#ifndef ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_
#define ANALYTICAL_ENGINE_CORE_PARALLEL_MESSAGES_H_

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

#include <string>
namespace grape {
class DoubleMsg {
 public:
  DoubleMsg() { data = -1.0; }
  ~DoubleMsg() {}
  inline void setData(double value) { data = value; }
  inline double getData() { return data; }

  double data;
};

class LongMsg {
 public:
  LongMsg() { data = -1; }
  ~LongMsg() {}
  inline void setData(uint64_t value) { data = value; }
  inline uint64_t getData() { return data; }

  uint64_t data;
};

class StringMsg {
 public:
  StringMsg() { data = nullptr; }
  ~StringMsg() {}
  inline void setData(std::string value) { data = value; }
  inline std::string getData() { return data; }

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

}  // namespace grape

#endif  // MESSAGES_H
