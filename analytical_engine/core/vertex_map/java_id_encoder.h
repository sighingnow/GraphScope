#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_ID_ENCODER_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_ID_ENCODER_H_

#include <vector>

#include <mpi.h>
#include "core/vertex_map/hash_policy.h"
// #include "grape/id_encoder/id_encoder.h"
//#include "vineyard/graph/utils/string_collection.h"
#include "core/utils/immutable_vector.h"
#include "grape/communication/sync_comm.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
#ifdef GRAPE_SDK_CPP_GRAPE_GEN_DEF
#include "grape-gen.h"
#endif

namespace gs {

namespace id_encoder_impl {

static constexpr int8_t min_lookups = 4;
static constexpr double max_load_factor = 0.5f;

inline int8_t log2(size_t value) {
  static constexpr int8_t table[64] = {
      63, 0,  58, 1,  59, 47, 53, 2,  60, 39, 48, 27, 54, 33, 42, 3,
      61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
      62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
      56, 45, 25, 31, 35, 16, 9,  12, 44, 24, 15, 8,  23, 7,  6,  5};
  value |= value >> 1;
  value |= value >> 2;
  value |= value >> 4;
  value |= value >> 8;
  value |= value >> 16;
  value |= value >> 32;
  return table[((value - (value >> 1)) * 0x07EDD5E59A4E28C2) >> 58];
}

template <typename T>
struct BufferUtils {
  using mutable_type = std::vector<T>;
  using immutable_type = grape::PodVector<T>;

  static void dump_buffer(const mutable_type& buffer, const std::string& path) {
    grape::dump_vector<T>(buffer, path);
  }

  static void send_buffer(const mutable_type& buffer, int dst_worker_id,
                          MPI_Comm comm) {
    grape::SendVector(buffer, dst_worker_id, comm);
  }

  static void recv_buffer(mutable_type& buffer, int src_worker_id,
                          MPI_Comm comm) {
    grape::RecvVector(buffer, src_worker_id, comm);
  }

  static void load_buffer(immutable_type& buffer, const std::string& path) {
    buffer.load(path);
  }

  template <typename IOADAPTOR_T>
  static void serialize_buffer(mutable_type& buffer,
                               std::unique_ptr<IOADAPTOR_T>& adaptor) {
    size_t size = buffer.size();
    CHECK(adaptor->Write(&size, sizeof(size_t)));
    CHECK(adaptor->Write(buffer.data(), sizeof(T) * size));
  }

  template <typename IOADAPTOR_T>
  static void deserialize_buffer(mutable_type& buffer,
                                 std::unique_ptr<IOADAPTOR_T>& adaptor) {
    size_t size;
    CHECK(adaptor->Read(&size, sizeof(size_t)));
    buffer.clear();
    buffer.resize(size);
    CHECK(adaptor->Read(buffer.data(), sizeof(T) * size));
  }
};

}  // namespace id_encoder_impl

template <typename OID_T, typename VID_T, template <typename> class Hasher>
class JavaIdEncoderBuilder {
 public:
  using KeyBuffer = typename id_encoder_impl::BufferUtils<OID_T>::mutable_type;
  using ValueBuffer =
      typename id_encoder_impl::BufferUtils<VID_T>::mutable_type;
  using DistBuffer =
      typename id_encoder_impl::BufferUtils<int8_t>::mutable_type;

  JavaIdEncoderBuilder() : hasher_() { reset_to_empty_state(); }

  bool put(const OID_T& oid, VID_T& lid) {
    size_t index = hash_policy_.index_for_hash(hasher_(oid));

    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      VID_T cur_lid = values_[index];
      if (keys_[cur_lid] == oid) {
        lid = cur_lid;
        return false;
      }
    }

    lid = static_cast<VID_T>(keys_.size());
    keys_.push_back(oid);
    CHECK_EQ(keys_.size(), num_elements_ + 1);
    emplace_new_value(distance_from_desired, index, lid);
    CHECK_EQ(keys_.size(), num_elements_);
    return true;
  }

  VID_T at(const OID_T& oid) {
    size_t index = hash_policy_.index_for_hash(hasher_(oid));
    for (int8_t distance = 0; distances_[index] >= distance;
         ++distance, ++index) {
      VID_T lid = values_[index];
      if (keys_[lid] == oid) {
        return lid;
      }
    }
    throw std::out_of_range("Argument passed to at() was not in the map");
    return VID_T();
  }

  void emplace(VID_T lid) {
    OID_T key = keys_[lid];
    size_t index = hash_policy_.index_for_hash(hasher_(key));
    int8_t distance_from_desired = 0;
    for (; distances_[index] >= distance_from_desired;
         ++index, ++distance_from_desired) {
      if (values_[index] == lid) {
        return;
      }
    }

    emplace_new_value(distance_from_desired, index, lid);
  }

  void emplace_new_value(int8_t distance_from_desired, size_t index,
                         VID_T lid) {
    if (num_slots_minus_one_ == 0 || distance_from_desired == max_lookups_ ||
        num_elements_ + 1 >
            (num_slots_minus_one_ + 1) * id_encoder_impl::max_load_factor) {
      grow();
      return;
    } else if (distances_[index] < 0) {
      values_[index] = lid;
      distances_[index] = distance_from_desired;
      ++num_elements_;
      return;
    }
    VID_T to_insert = lid;
    std::swap(distance_from_desired, distances_[index]);
    std::swap(to_insert, values_[index]);
    for (++distance_from_desired, ++index;; ++index) {
      if (distances_[index] < 0) {
        values_[index] = to_insert;
        distances_[index] = distance_from_desired;
        ++num_elements_;
        return;
      } else if (distances_[index] < distance_from_desired) {
        std::swap(distance_from_desired, distances_[index]);
        std::swap(to_insert, values_[index]);
        ++distance_from_desired;
      } else {
        ++distance_from_desired;
        if (distance_from_desired == max_lookups_) {
          grow();
          return;
        }
      }
    }
  }

  void grow() { rehash(std::max(size_t(4), 2 * bucket_count())); }

  size_t bucket_count() const {
    return num_slots_minus_one_ ? num_slots_minus_one_ + 1 : 0;
  }

  void rehash(size_t num_buckets) {
    num_buckets = std::max(
        num_buckets, static_cast<size_t>(std::ceil(
                         num_elements_ / id_encoder_impl::max_load_factor)));

    if (num_buckets == 0) {
      reset_to_empty_state();
      return;
    }

    auto new_prime_index = hash_policy_.next_size_over(num_buckets);
    if (num_buckets == bucket_count()) {
      return;
    }

    int8_t new_max_lookups = compute_max_lookups(num_buckets);

    DistBuffer new_distances(num_buckets + new_max_lookups);
    size_t special_end_index = num_buckets + new_max_lookups - 1;
    for (size_t i = 0; i != special_end_index; ++i) {
      new_distances[i] = -1;
    }
    new_distances[special_end_index] = 0;

    ValueBuffer new_values(num_buckets + new_max_lookups);

    new_values.swap(values_);
    new_distances.swap(distances_);

    std::swap(num_slots_minus_one_, num_buckets);
    --num_slots_minus_one_;
    hash_policy_.commit(new_prime_index);

    max_lookups_ = new_max_lookups;

    num_elements_ = 0;
    VID_T elem_num = static_cast<VID_T>(keys_.size());
    for (VID_T lid = 0; lid < elem_num; ++lid) {
      emplace(lid);
    }
  }

  void reset_to_empty_state() {
    keys_.clear();

    values_.clear();
    distances_.clear();
    values_.resize(id_encoder_impl::min_lookups);
    distances_.resize(id_encoder_impl::min_lookups, -1);
    distances_[id_encoder_impl::min_lookups - 1] = 0;

    num_slots_minus_one_ = 0;
    hash_policy_.reset();
    max_lookups_ = id_encoder_impl::min_lookups - 1;
    num_elements_ = 0;
  }

  static int8_t compute_max_lookups(size_t num_buckets) {
    int8_t desired = id_encoder_impl::log2(num_buckets);
    return std::max(id_encoder_impl::min_lookups, desired);
  }

  bool empty() const { return (num_elements_ == 0); }

  const KeyBuffer& keys() const { return keys_; }

  size_t size() const { return num_elements_; }

  void dump(const std::string& path) {
    grape::InArchive arc;
    size_t mod_function_index = hash_policy_.get_mod_function_index();
    arc << static_cast<int>(max_lookups_) << num_elements_
        << num_slots_minus_one_ << mod_function_index;
    std::vector<char> arc_buf(arc.GetBuffer(), arc.GetBuffer() + arc.GetSize());
    grape::dump_vector<char>(arc_buf, path + ".desc");

    CHECK_EQ(keys_.size(), num_elements_);

    id_encoder_impl::BufferUtils<OID_T>::dump_buffer(keys_, path + ".keys");
    id_encoder_impl::BufferUtils<VID_T>::dump_buffer(values_, path + ".values");
    id_encoder_impl::BufferUtils<int8_t>::dump_buffer(distances_,
                                                      path + ".distances");
  }

  void SendTo(int dst_worker_id, MPI_Comm comm) {
    grape::InArchive arc;
    size_t mod_function_index = hash_policy_.get_mod_function_index();
    arc << static_cast<int>(max_lookups_) << num_elements_
        << num_slots_minus_one_ << mod_function_index;
    grape::SendArchive(arc, dst_worker_id, comm);

    id_encoder_impl::BufferUtils<OID_T>::send_buffer(keys_, dst_worker_id,
                                                     comm);

#ifdef CONSTRUCT_MAP
    VID_T vnum = static_cast<VID_T>(num_elements_);
    for (VID_T i = 0; i < vnum; ++i) {
      emplace(i);
    }
#else
    id_encoder_impl::BufferUtils<VID_T>::send_buffer(values_, dst_worker_id,
                                                     comm);
    id_encoder_impl::BufferUtils<int8_t>::send_buffer(distances_, dst_worker_id,
                                                      comm);
#endif
  }

  void RecvFrom(int src_worker_id, MPI_Comm comm) {
    grape::OutArchive arc;
    grape::RecvArchive(arc, src_worker_id, comm);
    int max_lookups_int;
    size_t mod_function_index;
    arc >> max_lookups_int >> num_elements_ >> num_slots_minus_one_ >>
        mod_function_index;
    max_lookups_ = static_cast<int8_t>(max_lookups_int);

    hash_policy_.set_mod_function_by_index(mod_function_index);

    id_encoder_impl::BufferUtils<OID_T>::recv_buffer(keys_, src_worker_id,
                                                     comm);
#ifndef CONSTRUCT_MAP
    id_encoder_impl::BufferUtils<VID_T>::recv_buffer(values_, src_worker_id,
                                                     comm);
    id_encoder_impl::BufferUtils<int8_t>::recv_buffer(distances_, src_worker_id,
                                                      comm);
#endif
  }

  template <typename IOADAPTOR_T>
  void Serialize(std::unique_ptr<IOADAPTOR_T>& io_adaptor) {
    size_t mod_function_index = hash_policy_.get_mod_function_index();
    grape::InArchive arc;
    arc << static_cast<int>(max_lookups_) << num_elements_
        << num_slots_minus_one_ << mod_function_index;
    CHECK(io_adaptor->WriteArchive(arc));

    id_encoder_impl::BufferUtils<OID_T>::template serialize_buffer<IOADAPTOR_T>(
        keys_, io_adaptor);
    id_encoder_impl::BufferUtils<VID_T>::template serialize_buffer<IOADAPTOR_T>(
        values_, io_adaptor);
    id_encoder_impl::BufferUtils<int8_t>::template serialize_buffer<
        IOADAPTOR_T>(distances_, io_adaptor);
  }

  template <typename IOADAPTOR_T>
  void Deserialize(std::unique_ptr<IOADAPTOR_T>& io_adaptor) {
    grape::OutArchive arc;
    size_t mod_function_index;
    CHECK(io_adaptor->ReadArchive(arc));

    int max_lookups_int;
    arc >> max_lookups_int >> num_elements_ >> num_slots_minus_one_ >>
        mod_function_index;

    max_lookups_ = static_cast<int8_t>(max_lookups_int);
    hash_policy_.set_mod_function_by_index(mod_function_index);

    id_encoder_impl::BufferUtils<OID_T>::template deserialize_buffer<
        IOADAPTOR_T>(keys_, io_adaptor);
    id_encoder_impl::BufferUtils<VID_T>::template deserialize_buffer<
        IOADAPTOR_T>(values_, io_adaptor);
    id_encoder_impl::BufferUtils<int8_t>::template deserialize_buffer<
        IOADAPTOR_T>(distances_, io_adaptor);
  }

 private:
  KeyBuffer keys_;        // size = num of elements
  ValueBuffer values_;    // size = num of buckets
  DistBuffer distances_;  // size = num of buckets

  id_encoder_impl::prime_number_hash_policy hash_policy_;
  int8_t max_lookups_ = id_encoder_impl::min_lookups - 1;
  size_t num_elements_ = 0;
  size_t num_slots_minus_one_ = 0;

  Hasher<OID_T> hasher_;
};

template <typename OID_T, typename VID_T, template <typename> class Hasher>
class JavaIdEncoder {
 public:
  using KeyBuffer =
      typename id_encoder_impl::BufferUtils<OID_T>::immutable_type;
  using ValueBuffer =
      typename id_encoder_impl::BufferUtils<VID_T>::immutable_type;
  using DistBuffer =
      typename id_encoder_impl::BufferUtils<int8_t>::immutable_type;

  JavaIdEncoder() : hasher_() {}

  VID_T at(const OID_T& oid) const {
    size_t index = hash_policy_.index_for_hash(hasher_(oid));
    for (int8_t distance = 0; distances_[index] >= distance;
         ++distance, ++index) {
      VID_T lid = values_[index];
      if (keys_[lid] == oid) {
        return lid;
      }
    }
    throw std::out_of_range("Argument passed to at() was not in the map");
    return VID_T();
  }

  bool get_key(const VID_T& lid, OID_T& oid) {
    if (lid >= num_elements_) {
      return false;
    }
    oid = keys_[lid];
    return true;
  }

  bool get_value(const OID_T& oid, VID_T& lid) {
    size_t index = hash_policy_.index_for_hash(hasher_(oid));
    for (int8_t distance = 0; distances_[index] >= distance;
         ++distance, ++index) {
      VID_T ret = values_[index];
      if (keys_[ret] == oid) {
        lid = ret;
        return true;
      }
    }
    return false;
  }

  size_t bucket_count() const {
    return num_slots_minus_one_ ? num_slots_minus_one_ + 1 : 0;
  }

  bool empty() const { return (num_elements_ == 0); }

  const KeyBuffer& keys() const { return keys_; }

  size_t size() const { return num_elements_; }

  void load(const std::string& path) {
    grape::PodVector<char> arc_buf;
    arc_buf.load(path + ".desc");
    int max_lookups_int;
    size_t mod_function_index;

    grape::OutArchive arc;
    arc.SetSlice(arc_buf.data(), arc_buf.size());

    arc >> max_lookups_int >> num_elements_ >> num_slots_minus_one_ >>
        mod_function_index;

    max_lookups_ = static_cast<int8_t>(max_lookups_int);
    hash_policy_.set_mod_function_by_index(mod_function_index);

    MPI_Barrier(MPI_COMM_WORLD);

    id_encoder_impl::BufferUtils<OID_T>::load_buffer(keys_, path + ".keys");
    id_encoder_impl::BufferUtils<VID_T>::load_buffer(values_, path + ".values");
    id_encoder_impl::BufferUtils<int8_t>::load_buffer(distances_,
                                                      path + ".distances");

    MPI_Barrier(MPI_COMM_WORLD);

    CHECK_EQ(keys_.size(), num_elements_);
    CHECK_EQ(values_.size(), distances_.size());
    MPI_Barrier(MPI_COMM_WORLD);
  }

 private:
  KeyBuffer keys_;        // size = num of elements
  ValueBuffer values_;    // size = num of buckets
  DistBuffer distances_;  // size = num of buckets

  id_encoder_impl::prime_number_hash_policy hash_policy_;
  int8_t max_lookups_ = id_encoder_impl::min_lookups - 1;
  size_t num_elements_ = 0;
  size_t num_slots_minus_one_ = 0;

  Hasher<OID_T> hasher_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_ID_ENCODER_H_
