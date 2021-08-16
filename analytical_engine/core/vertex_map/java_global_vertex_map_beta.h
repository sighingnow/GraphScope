#ifndef ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_GLOBAL_VERTEX_MAP_BETA_H_
#define ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_GLOBAL_VERTEX_MAP_BETA_H_

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/fragment/partitioner.h"
// include id_encoder for id_encoder_impl.
#include "core/vertex_map/java_id_encoder.h"
#include "flat_hash_map/flat_hash_map.hpp"
#include "grape/config.h"
#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"
//#include "grape/vertex_map/global_vertex_map_beta.h"
#include "grape/vertex_map/vertex_map_base.h"
#include "grape/worker/comm_spec.h"
namespace grape {

template <typename OID_T, typename VID_T, template <typename> class Hasher>
class JavaGlobalVertexMapBeta;

template <typename OID_T, typename VID_T, template <typename> class Hasher>
class JavaNativeVertexMapBuilder {
 public:
  JavaNativeVertexMapBuilder(){};
  JavaNativeVertexMapBuilder(fid_t fid, int fid_offset) : fid_(fid) {
    fid_frame_ = fid;
    fid_frame_ <<= fid_offset;
  }

  bool AddVertex(const OID_T& oid, VID_T& gid) {
    VID_T lid;
    bool ret = builder_.put(oid, lid);
    gid = (fid_frame_ | lid);
    return ret;
  }

  void Finish(JavaGlobalVertexMapBeta<OID_T, VID_T, Hasher>& gvm) {
    gvm.Construct(fid_, builder_);
  }

  template <typename IOADAPTOR_T>
  void Serialize(const std::string& prefix) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), "%s/%s_%d", prefix.c_str(),
             kSerializationVertexMapFilename, fid_);

    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));
    io_adaptor->Open("wb");

    builder_.template Serialize<IOADAPTOR_T>(io_adaptor);

    io_adaptor->Close();
  }
  template <typename IOADAPTOR_T>
  void Deserialize(const std::string& prefix) {
    char fbuf[1024];
    snprintf(fbuf, sizeof(fbuf), "%s/%s_%d", prefix.c_str(),
             kSerializationVertexMapFilename, fid_);

    auto io_adaptor =
        std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(std::string(fbuf)));
    io_adaptor->Open();

    // Don't serialize directly, redo the put
    // JavaIdEncoderBuilder<OID_T, VID_T, Hasher> tmp_builder_;
    // tmp_builder_.template Deserialize<IOADAPTOR_T>(io_adaptor);
    // JavaIdEncoderBuilder<OID_T, VID_T, Hasher>::KeyBuffer oids =
    // tmp_builder_.keys();
    // std::vector<OID_T> oids = tmp_builder_.keys();
    // VID_T lid;
    // for (auto oid : oids) {
    //   builder_.put(oid, lid);
    // }
    builder_.template Deserialize<IOADAPTOR_T>(io_adaptor);

    // ERROR:
    // builder.template Deserialize<IOADAPTOR_T>(io_adaptor);

    io_adaptor->Close();
  }

 private:
  fid_t fid_;
  VID_T fid_frame_;
  JavaIdEncoderBuilder<OID_T, VID_T, Hasher> builder_;

  template <typename _OID_T, typename _VID_T, template <typename> class Hasher_>
  friend class JavaGlobalVertexMapBeta;
};

// static inline std::string generate_vm_path(int pid, int fid) {
//  char str[1024];
//  snprintf(str, sizeof(str), "/proc-%d.vm.frag.%u", pid, fid);
//  std::string ret = str;
//  return ret;
//}

template <typename OID_T, typename VID_T, template <typename> class Hasher>
class JavaGlobalVertexMapBeta : public VertexMapBase<OID_T, VID_T> {
  using Base = VertexMapBase<OID_T, VID_T>;

 public:
  explicit JavaGlobalVertexMapBeta(const CommSpec& comm_spec)
      : Base(comm_spec) {}

  ~JavaGlobalVertexMapBeta() = default;

  void Init() {
    Base::Init();
    id_encoders_.resize(Base::GetCommSpec().fnum());
  }

  size_t GetTotalVertexSize() {
    size_t size = 0;
    for (auto& e : id_encoders_) {
      size += e.size();
    }
    return size;
  }

  size_t GetInnerVertexSize(fid_t fid) { return id_encoders_[fid].size(); }

  JavaNativeVertexMapBuilder<OID_T, VID_T, Hasher> CreateNativeBuilder() {
    return JavaNativeVertexMapBuilder<OID_T, VID_T, Hasher>(
        Base::GetCommSpec().fid(), Base::GetFidOffset());
  }

  void Construct(fid_t fid,
                 JavaIdEncoderBuilder<OID_T, VID_T, Hasher>& builder) {
    const CommSpec& comm_spec = Base::GetCommSpec();

    int pid;
    if (comm_spec.local_id() == 0) {
      pid = static_cast<int>(getpid());
    }
    MPI_Bcast(&pid, 1, MPI_INT, 0, comm_spec.local_comm());

    builder.dump(generate_vm_path(pid, comm_spec.fid()));
    LOG(INFO) << "[frag-" << fid << "] dumped vm";

    int host_num = comm_spec.host_num();
    int host_id = comm_spec.host_id();
    LOG(INFO) << "host_num = " << host_num << ", host_id = " << host_id;
    std::thread send_thread = std::thread([&]() {
      for (int i = 1; i < host_num; ++i) {
        int dst_host_id = (host_id + i) % host_num;
        const std::vector<int>& dst_host_worker_list =
            comm_spec.host_worker_list(dst_host_id);
        int dst_host_worker_num = dst_host_worker_list.size();
        int dst_worker_id =
            dst_host_worker_list[comm_spec.local_id() % dst_host_worker_num];
        builder.SendTo(dst_worker_id, comm_spec.comm());
      }
    });

    std::thread recv_thread = std::thread([&]() {
      for (int i = 1; i < host_num; ++i) {
        int src_host_id = (host_id + host_num - i) % host_num;
        const std::vector<int>& src_host_worker_list =
            comm_spec.host_worker_list(src_host_id);
        int src_host_worker_num = src_host_worker_list.size();
        for (int j = 0; j < src_host_worker_num; ++j) {
          if (j % comm_spec.local_num() == comm_spec.local_id()) {
            int src_worker_id = src_host_worker_list[j];
            JavaIdEncoderBuilder<OID_T, VID_T, Hasher> rc_builder;
            rc_builder.RecvFrom(src_worker_id, comm_spec.comm());
            rc_builder.dump(
                generate_vm_path(pid, comm_spec.WorkerToFrag(src_worker_id)));
          }
        }
      }
    });

    send_thread.join();
    recv_thread.join();

    MPI_Barrier(comm_spec.local_comm());
    // LOG(INFO) << "[frag-" << fid << "] after threads joined";

    for (fid_t i = 0; i < comm_spec.fnum(); ++i) {
      id_encoders_[i].load(generate_vm_path(pid, i));
      // LOG(INFO) << "id encoder[" << i << "] " << id_encoders_[i].size();
      // OID_T oid = (OID_T) ;
      // LOG(INFO) << id_encoders_[i].get_value(oid, lid);
      // LOG(INFO) << lid;
    }

    // LOG(INFO) << "[frag-" << fid << "] after load";
  }

  bool GetOid(const VID_T& gid, OID_T& oid) {
    fid_t fid = Base::GetFidFromGid(gid);
    VID_T lid = Base::GetLidFromGid(gid);
    return GetOid(fid, lid, oid);
  }

  bool GetOid(fid_t fid, const VID_T& lid, OID_T& oid) {
    return id_encoders_[fid].get_key(lid, oid);
  }

  bool GetGid(fid_t fid, const OID_T& oid, VID_T& gid) {
    VID_T lid;
    if (id_encoders_[fid].get_value(oid, lid)) {
      // LOG(INFO) << "lid " << lid;
      gid = Base::Lid2Gid(fid, lid);
      return true;
    } else {
      return false;
    }
  }

  bool GetGid(const OID_T& oid, VID_T& gid) {
    for (fid_t i = 0; i < Base::GetFragmentNum(); ++i) {
      if (GetGid(i, oid, gid)) {
        return true;
      }
    }
    return false;
  }

  using IdListType = typename JavaIdEncoder<OID_T, VID_T, Hasher>::KeyBuffer;
  const IdListType& GetVertexIdBuffer(fid_t fid) const {
    return id_encoders_[fid].keys();
  }

 private:
  std::vector<JavaIdEncoder<OID_T, VID_T, Hasher>> id_encoders_;
  //   HashPartitioner<OID_T> partitioner_;
};

}  // namespace grape

#endif  // ANALYTICAL_ENGINE_CORE_VERTEX_MAP_JAVA_GLOBAL_VERTEX_MAP_BETA_H_
