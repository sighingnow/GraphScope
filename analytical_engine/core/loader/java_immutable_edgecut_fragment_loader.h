#ifndef ANALYTICAL_ENGINE_CORE_LOADER_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_

#include <bitset>
#include <memory>
#include <vector>

// #include "grape/fragment/basic_fragment_loader.h"
// #include "grape/fragment/e_fragment_loader.h"
#include "core/loader/basic_java_immutable_edgecut_fragment_loader.h"
#include "grape/io/local_io_adaptor.h"
#include "grape/serialization/out_archive.h"
#include "grape/util.h"
#include "grape/worker/comm_spec.h"
namespace gs {
template <typename FRAG_T>
class JavaImmutableEdgecutFragmentLoader {
  // using vertex_map_t = typename fragment_t::vertex_map_t;
  // using partitioner_t = HashPartitioner<OID_T>;
  using OID_T = typename FRAG_T::oid_t;
  using VID_T = typename FRAG_T::vid_t;
  using VDATA_T = typename FRAG_T::vdata_t;
  using EDATA_T = typename FRAG_T::edata_t;
  // using partition_t = CityHashPartitioner<OID_T>;
  using io_adaptor_t = grape::LocalIOAdaptor;
  using vertex_map_t = typename FRAG_T::vertex_map_t;

  // using vertex_map_t = GlobalVertexMapBeta<OID_T, VID_T>;

 public:
  JavaImmutableEdgecutFragmentLoader() {}

  ~JavaImmutableEdgecutFragmentLoader() {}

  void Init() {
    comm_spec_.Init(MPI_COMM_WORLD);
    worker_num_ = comm_spec_.worker_num();
    worker_id_ = comm_spec_.worker_id();
    fid_ = comm_spec_.fid();
    fnum_ = comm_spec_.fnum();
    basic_fragment_loader_ =
        std::make_shared<BasicJavaImmutableEdgecutFragmentLoader<FRAG_T>>(
            comm_spec_);
    std::vector<OID_T> id_list;
    // partitioner_t partitioner(comm_spec_.fnum(), id_list);
    // partition_t partitioner;
    // basic_fragment_loader_->SetPartitioner(std::move(partitioner));
    // basic_fragment_loader_->Start();
  }

  void AddVertexBuffersV2(std::vector<std::vector<OID_T>>& vid_buffers,
                          std::vector<std::vector<VDATA_T>>& vdata_buffers) {
    std::vector<std::vector<OID_T>> allVidBuffers(fnum_);
    std::vector<std::vector<VDATA_T>> allVdataBuffers(fnum_);

    MPI_Barrier(comm_spec_.comm());
    // LOG(INFO) << "before add vertex buffers, worker [" << worker_id_ << "] ";
    // for (size_t i = 0; i < vid_buffers.size(); ++i) {
    //   LOG(INFO) << " worker [" << worker_id_ << "] vid buffer" << i << " size
    //   "
    //             << vid_buffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] vdata buffer" << i
    //             << " size " << vdata_buffers[i].size();
    // }

    std::thread send_thread = std::thread([&]() {
      for (int i = 1; i < worker_num_; ++i) {
        int dst_worker_id = (worker_id_ + i) % worker_num_;
        fid_t dst_frag_id = comm_spec_.WorkerToFrag(dst_worker_id);
        SendVector(vid_buffers[dst_frag_id], dst_worker_id, comm_spec_.comm());
        SendVector(vdata_buffers[dst_frag_id], dst_worker_id,
                   comm_spec_.comm());
      }
    });

    std::thread recv_thread = std::thread([&]() {
      // nativeVidBuffers[fid_].swap(vidBuffers[fid_]);
      // nativeVdataBuffers[fid_].swap(vdataBuffers[fid_]);
      allVidBuffers[fid_].insert(allVidBuffers[fid_].end(),
                                 vid_buffers[fid_].begin(),
                                 vid_buffers[fid_].end());
      allVdataBuffers[fid_].insert(allVdataBuffers[fid_].end(),
                                   vdata_buffers[fid_].begin(),
                                   vdata_buffers[fid_].end());

      for (int i = 1; i < worker_num_; ++i) {
        int src_worker_id = (worker_id_ + worker_num_ - i) % worker_num_;
        std::vector<OID_T> tmpVid;
        std::vector<VDATA_T> tmpVdata;
        // fid_t src_frag_id = comm_spec_.WorkerToFrag(src_worker_id);
        RecvVector(tmpVid, src_worker_id, comm_spec_.comm());
        RecvVector(tmpVdata, src_worker_id, comm_spec_.comm());
        allVidBuffers[worker_id_].insert(allVidBuffers[worker_id_].end(),
                                         tmpVid.begin(), tmpVid.end());
        allVdataBuffers[worker_id_].insert(allVdataBuffers[worker_id_].end(),
                                           tmpVdata.begin(), tmpVdata.end());
      }
    });

    send_thread.join();
    recv_thread.join();
    MPI_Barrier(comm_spec_.comm());
    // LOG(INFO) << "before add vertex buffers, worker [" << worker_id_ << "] ";
    // for (size_t i = 0; i < vid_buffers.size(); ++i) {
    //   LOG(INFO) << " worker [" << worker_id_ << "] vid buffer" << i << " size
    //   "
    //             << allVidBuffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] vdata buffer" << i
    //             << " size " << allVdataBuffers[i].size();
    // }
    vid_buffers.clear();
    vdata_buffers.clear();

    basic_fragment_loader_->AddVertices(allVidBuffers, allVdataBuffers);
  }

  void AddVertexBuffers(std::vector<std::vector<OID_T>>& vid_buffers,
                        std::vector<std::vector<VDATA_T>>& vdata_buffers) {
    std::vector<std::vector<OID_T>> allVidBuffers(fnum_);
    std::vector<std::vector<VDATA_T>> allVdataBuffers(fnum_);

    MPI_Barrier(comm_spec_.comm());
    std::thread send_thread = std::thread([&]() {
      for (int j = 0; j < comm_spec_.fnum(); ++j) {
        // we send all frag's corresponding data
        // LOG(INFO) << "worker [" << comm_spec_.worker_id() << "], frag [" << j
        //           << "] vdata size " << vid_buffers[j].size() << ","
        //           << vdata_buffers[j].size();
        for (int i = 1; i < worker_num_; ++i) {
          int dst_worker_id = (worker_id_ + i) % worker_num_;
          SendVector(vid_buffers[j], dst_worker_id, comm_spec_.comm());
          // LOG(INFO) << "worker [" << comm_spec_.worker_id()
          //           << "] sent vid buffer frag [" << j << "], to worker ["
          //           << dst_worker_id << "], data size ["
          //           << vid_buffers[j].size() << "]";
          SendVector(vdata_buffers[j], dst_worker_id, comm_spec_.comm());
          // LOG(INFO) << "worker [" << comm_spec_.worker_id()
          //           << "]sent vdata buffer frag [" << j << "], to worker ["
          //           << dst_worker_id << "], data size ["
          //           << vdata_buffers[j].size() << "]";
        }
        // fid_t dst_frag_id = comm_spec_.WorkerToFrag(dst_worker_id);
      }
    });

    std::thread recv_thread = std::thread([&]() {
      // allVidBuffers[fid_].swap(vid_buffers[fid_]);
      // allVdataBuffers[fid_].swap(vdata_buffers[fid_]);

      for (int j = 0; j < comm_spec_.fnum(); ++j) {
        allVidBuffers[j].insert(allVidBuffers[j].end(), vid_buffers[j].begin(),
                                vid_buffers[j].end());
        allVdataBuffers[j].insert(allVdataBuffers[j].end(),
                                  vdata_buffers[j].begin(),
                                  vdata_buffers[j].end());
        std::vector<OID_T> tmpVid;
        std::vector<VDATA_T> tmpVdata;
        for (int i = 1; i < worker_num_; ++i) {
          int src_worker_id = (worker_id_ + worker_num_ - i) % worker_num_;

          RecvVector(tmpVid, src_worker_id, comm_spec_.comm());
          RecvVector(tmpVdata, src_worker_id, comm_spec_.comm());
          allVidBuffers[j].insert(allVidBuffers[j].end(), tmpVid.begin(),
                                  tmpVid.end());
          allVdataBuffers[j].insert(allVdataBuffers[j].end(), tmpVdata.begin(),
                                    tmpVdata.end());
        }
      }
    });
    send_thread.join();
    recv_thread.join();
    vid_buffers.clear();
    vdata_buffers.clear();

    MPI_Barrier(comm_spec_.comm());
    basic_fragment_loader_->AddVertices(allVidBuffers, allVdataBuffers);
  }

  void AddEdgeBuffersV2(std::vector<std::vector<OID_T>>& esrc_buffers,
                        std::vector<std::vector<OID_T>>& edst_buffers,
                        std::vector<std::vector<EDATA_T>>& edata_buffers) {
    std::vector<std::vector<OID_T>> all_esrc_buffers(fnum_);
    std::vector<std::vector<OID_T>> all_edst_buffers(fnum_);
    std::vector<std::vector<EDATA_T>> all_edata_buffers(fnum_);

    // LOG(INFO) << "before add edge buffers, worker [" << worker_id_ << "] ";
    // for (size_t i = 0; i < esrc_buffers.size(); ++i) {
    //   LOG(INFO) << " worker [" << worker_id_ << "] esrc buffer" << i << "
    //   size "
    //             << esrc_buffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] edst buffer" << i << "
    //   size "
    //             << edst_buffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] edata buffer" << i
    //             << " size " << edata_buffers[i].size();
    // }

    MPI_Barrier(comm_spec_.comm());
    std::thread send_thread = std::thread([&]() {
      for (int i = 1; i < worker_num_; ++i) {
        int dst_worker_id = (worker_id_ + i) % worker_num_;
        SendVector(esrc_buffers[dst_worker_id], dst_worker_id,
                   comm_spec_.comm());
        SendVector(edst_buffers[dst_worker_id], dst_worker_id,
                   comm_spec_.comm());
        SendVector(edata_buffers[dst_worker_id], dst_worker_id,
                   comm_spec_.comm());
      }
    });

    std::thread recv_thread = std::thread([&]() {
      all_esrc_buffers[fid_].insert(all_esrc_buffers[fid_].end(),
                                    esrc_buffers[fid_].begin(),
                                    esrc_buffers[fid_].end());
      all_edst_buffers[fid_].insert(all_edst_buffers[fid_].end(),
                                    edst_buffers[fid_].begin(),
                                    edst_buffers[fid_].end());
      all_edata_buffers[fid_].insert(all_edata_buffers[fid_].end(),
                                     edata_buffers[fid_].begin(),
                                     edata_buffers[fid_].end());
      for (int i = 1; i < worker_num_; ++i) {
        int src_worker_id = (worker_id_ + worker_num_ - i) % worker_num_;
        // fid_t src_frag_id = comm_spec_.WorkerToFrag(src_worker_id);
        std::vector<OID_T> tmpSrc;
        std::vector<OID_T> tmpDst;
        std::vector<EDATA_T> tmpEData;
        RecvVector(tmpSrc, src_worker_id, comm_spec_.comm());
        RecvVector(tmpDst, src_worker_id, comm_spec_.comm());
        RecvVector(tmpEData, src_worker_id, comm_spec_.comm());
        all_esrc_buffers[worker_id_].insert(all_esrc_buffers[worker_id_].end(),
                                            tmpSrc.begin(), tmpSrc.end());
        all_edst_buffers[worker_id_].insert(all_edst_buffers[worker_id_].end(),
                                            tmpDst.begin(), tmpDst.end());
        all_edata_buffers[worker_id_].insert(
            all_edata_buffers[worker_id_].end(), tmpEData.begin(),
            tmpEData.end());
      }
    });

    send_thread.join();
    recv_thread.join();
    MPI_Barrier(comm_spec_.comm());
    // LOG(INFO) << "after add edge buffers, worker [" << worker_id_ << "] ";
    // for (size_t i = 0; i < esrc_buffers.size(); ++i) {
    //   LOG(INFO) << " worker [" << worker_id_ << "] esrc buffer" << i << "
    //   size "
    //             << all_esrc_buffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] edst buffer" << i << "
    //   size "
    //             << all_edst_buffers[i].size();
    //   LOG(INFO) << " worker [" << worker_id_ << "] edata buffer" << i
    //             << " size " << all_edata_buffers[i].size();
    // }

    esrc_buffers.clear();
    edst_buffers.clear();
    edata_buffers.clear();
    basic_fragment_loader_->AddEdges(all_esrc_buffers, all_edst_buffers,
                                     all_edata_buffers);
  }

  void AddEdgeBuffers(std::vector<std::vector<OID_T>>& esrc_buffers,
                      std::vector<std::vector<OID_T>>& edst_buffers,
                      std::vector<std::vector<EDATA_T>>& edata_buffers) {
    std::vector<std::vector<OID_T>> all_esrc_buffers(fnum_);
    std::vector<std::vector<OID_T>> all_edst_buffers(fnum_);
    std::vector<std::vector<EDATA_T>> all_edata_buffers(fnum_);

    MPI_Barrier(comm_spec_.comm());
    std::thread send_thread = std::thread([&]() {
      for (int j = 0; j < comm_spec_.fnum(); ++j) {
        for (int i = 1; i < worker_num_; ++i) {
          int dst_worker_id = (worker_id_ + i) % worker_num_;
          SendVector(esrc_buffers[j], dst_worker_id, comm_spec_.comm());
          SendVector(edst_buffers[j], dst_worker_id, comm_spec_.comm());
          SendVector(edata_buffers[j], dst_worker_id, comm_spec_.comm());
        }
      }
    });

    std::thread recv_thread = std::thread([&]() {
      for (int j = 0; j < comm_spec_.fnum(); ++j) {
        all_esrc_buffers[j].insert(all_esrc_buffers[j].end(),
                                   esrc_buffers[j].begin(),
                                   esrc_buffers[j].end());
        all_edst_buffers[j].insert(all_edst_buffers[j].end(),
                                   edst_buffers[j].begin(),
                                   edst_buffers[j].end());
        all_edata_buffers[j].insert(all_edata_buffers[j].end(),
                                    edata_buffers[j].begin(),
                                    edata_buffers[j].end());
        for (int i = 1; i < worker_num_; ++i) {
          int src_worker_id = (worker_id_ + worker_num_ - i) % worker_num_;
          // fid_t src_frag_id = comm_spec_.WorkerToFrag(src_worker_id);
          std::vector<OID_T> tmpEsrc;
          std::vector<OID_T> tmpEdst;
          std::vector<EDATA_T> tmpEdata;

          RecvVector(tmpEsrc, src_worker_id, comm_spec_.comm());
          RecvVector(tmpEdst, src_worker_id, comm_spec_.comm());
          RecvVector(tmpEdata, src_worker_id, comm_spec_.comm());

          all_esrc_buffers[j].insert(all_esrc_buffers[j].end(), tmpEsrc.begin(),
                                     tmpEsrc.end());
          all_edst_buffers[j].insert(all_edst_buffers[j].end(), tmpEdst.begin(),
                                     tmpEdst.end());
          all_edata_buffers[j].insert(all_edata_buffers[j].end(),
                                      tmpEdata.begin(), tmpEdata.end());
        }
      }
    });

    send_thread.join();
    recv_thread.join();

    esrc_buffers.clear();
    edst_buffers.clear();
    edata_buffers.clear();

    MPI_Barrier(comm_spec_.comm());
    basic_fragment_loader_->AddEdges(all_esrc_buffers, all_edst_buffers,
                                     all_edata_buffers);
  }

  std::shared_ptr<FRAG_T> LoadFragment(bool serialize, std::string& prefix) {
    std::shared_ptr<FRAG_T> fragment(nullptr);
    basic_fragment_loader_->ConstructFragment(fragment);
    if (serialize) {
      double t = -grape::GetCurrentTime();
      MPI_Barrier(comm_spec_.comm());
      basic_fragment_loader_->template SerializeFragment<io_adaptor_t>(fragment,
                                                                       prefix);
      MPI_Barrier(comm_spec_.comm());
      t += grape::GetCurrentTime();
      if (comm_spec_.worker_id() == 0) {
        VLOG(1) << "[Coordinator] Serialization cost" << t;
      }
    }
    return fragment;
  }

  // bool DeserializeFragment(std::shared_ptr<FRAG_T> fragment,
  //                          std::string& serialize_prefix) {
  //   auto io_adaptor =
  //       std::unique_ptr<io_adaptor_t>(new io_adaptor_t(serialize_prefix));
  //   if (io_adaptor->IsExist()) {
  //     vm_ptr_->template Deserialize<io_adaptor_t>(serialize_prefix);
  //     fragment = std::shared_ptr<FRAG_T>(new FRAG_T(vm_ptr_));
  //     fragment->template Deserialize<io_adaptor_t>(serialize_prefix,
  //                                                  comm_spec_.fid());
  //   }
  //   return true;
  // }
  bool DeserializeFragment(std::shared_ptr<FRAG_T>& fragment,
                           std::string& serialize_prefix) {
    return basic_fragment_loader_->template DeserializeFragment<io_adaptor_t>(
        fragment, serialize_prefix);
  }

 private:
  int worker_id_;
  int worker_num_;
  fid_t fid_;
  fid_t fnum_;
  grape::CommSpec comm_spec_;

  std::shared_ptr<BasicJavaImmutableEdgecutFragmentLoader<FRAG_T>>
      basic_fragment_loader_;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_
