#ifndef ANALYTICAL_ENGINE_CORE_LOADER_BASIC_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_
#define ANALYTICAL_ENGINE_CORE_LOADER_BASIC_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_

#include <stddef.h>

#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

// #include "grape/communication/pie_shuffle.h"
#include "grape/config.h"
// #include "grape/fragment/partition/pie_partitioner.h"
// #include "grape/fragment/partitioner.h"
#include "grape/graph/edge.h"
#include "grape/graph/vertex.h"
#include "grape/utils/concurrent_queue.h"
//#include "grape/utils/ref_string.h"
#include "grape/utils/vertex_array.h"
#include "grape/worker/comm_spec.h"
#include "vineyard/graph/utils/string_collection.h"

namespace gs {

template <typename FRAG_T>
class BasicJavaImmutableEdgecutFragmentLoader {
  using fragment_t = FRAG_T;
  using OID_T = typename fragment_t::oid_t;
  using VID_T = typename fragment_t::vid_t;
  using VDATA_T = typename fragment_t::vdata_t;
  using EDATA_T = typename fragment_t::edata_t;
  using vertex_map_t = typename fragment_t::vertex_map_t;
  using vertex_map_builder_t = typename fragment_t::vertex_map_builder_t;

  static constexpr grape::LoadStrategy load_strategy = LoadStrategy::kOnlyOut;

 public:
  explicit BasicJavaImmutableEdgecutFragmentLoader(
      const grape::CommSpec& comm_spec)
      : comm_spec_(comm_spec) {
    comm_spec_.Dup();
    vm_ptr_ = std::shared_ptr<vertex_map_t>(new vertex_map_t(comm_spec_));
    vm_ptr_->Init();
    vm_builder_ = vm_ptr_->CreateNativeBuilder();
  }

  ~BasicJavaImmutableEdgecutFragmentLoader() {}

  template <typename IOADAPTOR_T>
  bool SerializeFragment(std::shared_ptr<FRAG_T> fragment,
                         std::string& serialize_prefix) {
    char serial_file[1024];
    snprintf(serial_file, sizeof(serial_file), "%s/%s",
             serialize_prefix.c_str(), grape::kSerializationVertexMapFilename);
    VLOG(2) << " Persisting vm to " << serial_file;
    // if (comm_spec_.local_id() == 0 && !exists_file(serial_file)) {
    // if (!exists_file(serial_file)) {
    vm_builder_.template Serialize<IOADAPTOR_T>(serial_file);
    //}
    fragment->template Serialize<IOADAPTOR_T>(serialize_prefix);
    return true;
  }
  template <typename IOADAPTOR_T>
  bool DeserializeFragment(std::shared_ptr<FRAG_T>& fragment,
                           std::string& serialize_prefix) {
    auto exists_file = [](const std::string& name) {
      std::ifstream f(name.c_str());
      return f.good();
    };
    char serial_file[1024];
    snprintf(serial_file, sizeof(serial_file), "%s/%s",
             serialize_prefix.c_str(), kSerializationVertexMapFilename);
    // auto io_adaptor =
    //     std::unique_ptr<IOADAPTOR_T>(new IOADAPTOR_T(serialize_prefix));
    if (exists_file(serial_file)) {
      VLOG(2) << " Loading vm from " << serial_file;
      vm_builder_.template Deserialize<IOADAPTOR_T>(serial_file);
      vm_builder_.Finish(*vm_ptr_);

      // fragment = std::shared_ptr<fragment_t>();
      fragment.reset(new fragment_t(vm_ptr_));
      fragment->template Deserialize<IOADAPTOR_T>(serialize_prefix,
                                                  comm_spec_.fid());
      return true;
    }

    LOG(FATAL) << " File not found " << serialize_prefix;
    return false;
  }

  void AddVertices(std::vector<std::vector<OID_T>>& vidBuffers,
                   std::vector<std::vector<VDATA_T>>& vdataBuffers) {
    // fid_t fnum = comm_spec_.fnum();
    // for (fid_t i = 0; i < fnum; ++i) {
    //   for (size_t j = 0; j < vidBuffers[i].size(); ++j) {
    //     VDATA_T ref_data(vdataBuffers[i][j]);
    //     vertices_to_frag_[i].Emplace(vidBuffers[i][j], ref_data);
    //   }
    // }
    got_vertices_id_.emplace_back(std::move(vidBuffers[comm_spec_.fid()]));
    got_vertices_data_.emplace_back(std::move(vdataBuffers[comm_spec_.fid()]));
  }

  void sortThreadRoutine(
      fid_t fid, std::vector<std::vector<OID_T>>& vertex_id,
      std::vector<std::vector<VDATA_T>>& vertex_data,
      std::vector<grape::internal::Vertex<VID_T, VDATA_T>>& vertices) {
    vertices.clear();

    size_t buf_num = vertex_id.size();
    for (size_t buf_id = 0; buf_id < buf_num; ++buf_id) {
      auto& id_list = vertex_id[buf_id];
      auto& data_list = vertex_data[buf_id];
      size_t index = 0;
      for (auto& id : id_list) {
        // why add twice?
        // A: preventing from add multiple times
        // if (vm_ptr_->AddVertex(fid, id, gid)) {
        //   vertices.emplace_back(gid, data_list[index]);
        // }
        VID_T gid;
        VDATA_T ref_vdata(data_list[index]);
        if (vm_builder_.AddVertex(id, gid)) {
          vertices.emplace_back(gid, ref_vdata);
        }
        // vertices.emplace_back(id, data_list[index]);
        ++index;
      }
    }
    vm_builder_.Finish(*vm_ptr_);
  }
  void AddEdges(std::vector<std::vector<OID_T>>& esrcBuffers,
                std::vector<std::vector<OID_T>>& edstBuffers,
                std::vector<std::vector<EDATA_T>>& edataBuffers) {
    // fid_t fnum = comm_spec_.fnum();
    // for (fid_t i = 0; i < fnum; ++i) {
    //   for (size_t j = 0; j < esrcBuffers[i].size(); ++j) {
    //     EDATA_T ref_data(edataBuffers[i][j]);
    //     // let java pre compute whether src and dst are in the same frag.
    //     edges_to_frag_[i].Emplace(esrcBuffers[i][j], edstBuffers[i][j],
    //                               ref_data);
    //   }
    // }
    got_edges_src_.emplace_back(std::move(esrcBuffers[comm_spec_.fid()]));
    got_edges_dst_.emplace_back(std::move(edstBuffers[comm_spec_.fid()]));
    got_edges_data_.emplace_back(std::move(edataBuffers[comm_spec_.fid()]));
  }

  void processEdges(std::vector<std::vector<OID_T>>& edge_src,
                    std::vector<std::vector<OID_T>>& edge_dst,
                    std::vector<std::vector<EDATA_T>>& edge_data,
                    std::vector<grape::Edge<VID_T, EDATA_T>>& to) {
    to.clear();
    std::vector<work_unit> work_units;
    {
      size_t cur = 0;
      size_t index = 0;
      for (auto& buf : edge_src) {
        work_units.emplace_back(comm_spec_.fid(), index, cur);
        cur += buf.size();
        ++index;
      }
      to.resize(cur);
    }
    std::atomic<size_t> current_work_unit(0);
    int thread_num =
        (std::thread::hardware_concurrency() + comm_spec_.local_num() - 1) /
        comm_spec_.local_num();
    std::vector<std::thread> process_threads(thread_num);
    for (int tid = 0; tid < thread_num; ++tid) {
      process_threads[tid] = std::thread([&]() {
        size_t got;
        while (true) {
          got = current_work_unit.fetch_add(1, std::memory_order_release);
          if (got >= work_units.size()) {
            break;
          }
          auto& wu = work_units[got];
          size_t buf_ind = wu.index;
          size_t buf_begin = wu.begin;
          auto& src_buf = edge_src[buf_ind];
          auto& dst_buf = edge_dst[buf_ind];
          auto& data_buf = edge_data[buf_ind];
          grape::Edge<VID_T, EDATA_T>* ptr = &to[buf_begin];

          auto src_iter = src_buf.begin();
          auto src_end = src_buf.end();
          auto dst_iter = dst_buf.begin();
          auto data_iter = data_buf.begin();
          VID_T src_gid, dst_gid;
          while (src_iter != src_end) {
            CHECK(vm_ptr_->GetGid(OID_T(*src_iter), src_gid));
            CHECK(vm_ptr_->GetGid(OID_T(*dst_iter), dst_gid));
            ptr->set_edata(std::move(*data_iter));
            ptr->SetEndpoint(src_gid, dst_gid);
            ++src_iter;
            ++dst_iter;
            ++data_iter;
            ++ptr;
          }
        }
      });
    }
    for (auto& thrd : process_threads) {
      thrd.join();
    }
  }

  void sortDistinct() {
    sortThreadRoutine(comm_spec_.fid(), got_vertices_id_, got_vertices_data_,
                      processed_vertices_);
    // vm_ptr_->Construct();
  }
  void ConstructFragment(std::shared_ptr<fragment_t>& fragment) {
    // for (auto& va : vertices_to_frag_) {
    //   va.Flush();
    // }
    // for (auto& ea : edges_to_frag_) {
    //   ea.Flush();
    // }
    // vertex_recv_thread_.join();
    // edge_recv_thread_.join();
    // recv_thread_running_ = false;

    MPI_Barrier(comm_spec_.comm());

    // got_vertices_id_.emplace_back(
    //     std::move(vertices_to_frag_[comm_spec_.fid()].Buffer0()));
    // got_vertices_data_.emplace_back(std::move(std::vector<VDATA_T>(
    //     std::move(vertices_to_frag_[comm_spec_.fid()].Buffer1()))));
    // got_edges_src_.emplace_back(
    //     std::move(edges_to_frag_[comm_spec_.fid()].Buffer0()));
    // got_edges_dst_.emplace_back(
    //     std::move(edges_to_frag_[comm_spec_.fid()].Buffer1()));
    // got_edges_data_.emplace_back(std::move(std::vector<EDATA_T>(
    //     std::move(edges_to_frag_[comm_spec_.fid()].Buffer2()))));

    sortDistinct();
    got_vertices_id_.clear();
    got_vertices_data_.clear();
    VLOG(1) << "frag[ " << comm_spec_.fid() << "] inner vertex"
            << vm_ptr_->GetInnerVertexSize(comm_spec_.fid())
            << ",totoal vertex size:" << vm_ptr_->GetTotalVertexSize();

    // doesn't need construct
    // vm_ptr_->Construct();
    VLOG(1) << "[worker-" << comm_spec_.worker_id()
            << "]: finished construct vertex map and process vertices";

    processEdges(got_edges_src_, got_edges_dst_, got_edges_data_,
                 processed_edges_);

    got_edges_src_.clear();
    got_edges_dst_.clear();
    got_edges_data_.clear();
    VLOG(1) << "[worker-" << comm_spec_.worker_id()
            << "]: finished process edges";
    fragment = std::shared_ptr<fragment_t>(new fragment_t(vm_ptr_));
    // TODO: check processed vertices is used.
    fragment->Init(comm_spec_.fid(), processed_vertices_, processed_edges_);
    VLOG(1) << "[worker-" << comm_spec_.worker_id()
            << "]: finished construction";
    initMirrorInfo(fragment);
    initOuterVertexData(fragment);
  }

 private:
  void initOuterVertexData(std::shared_ptr<fragment_t> fragment) {
    int worker_id = comm_spec_.worker_id();
    int worker_num = comm_spec_.worker_num();

    std::thread send_thread([&]() {
      grape::InArchive arc;
      for (int i = 1; i < worker_num; ++i) {
        int dst_worker_id = (worker_id + i) % worker_num;
        fid_t dst_fid = comm_spec_.WorkerToFrag(dst_worker_id);
        arc.Clear();
        auto& vertices = fragment->MirrorVertices(dst_fid);
        for (auto v : vertices) {
          arc << fragment->GetData(v);
        }
        MPI_Send(arc.GetBuffer(), arc.GetSize(), MPI_CHAR, dst_worker_id, 0,
                 comm_spec_.comm());
      }
    });

    std::thread recv_thread([&]() {
      grape::OutArchive arc;
      for (int i = 1; i < worker_num; ++i) {
        int src_worker_id = (worker_id + worker_num - i) % worker_num;
        fid_t src_fid = comm_spec_.WorkerToFrag(src_worker_id);
        MPI_Status status;
        MPI_Probe(src_worker_id, 0, comm_spec_.comm(), &status);
        int count;
        MPI_Get_count(&status, MPI_CHAR, &count);
        arc.Clear();
        arc.Allocate(count);
        MPI_Recv(arc.GetBuffer(), arc.GetSize(), MPI_CHAR, src_worker_id, 0,
                 comm_spec_.comm(), MPI_STATUS_IGNORE);
        auto range = fragment->OuterVertices(src_fid);
        for (auto v : range) {
          VDATA_T val;
          arc >> val;
          fragment->SetData(v, val);
        }
      }
    });

    recv_thread.join();
    send_thread.join();
  }
  void initMirrorInfo(std::shared_ptr<fragment_t> fragment) {
    int worker_id = comm_spec_.worker_id();
    int worker_num = comm_spec_.worker_num();

    std::thread send_thread([&]() {
      std::vector<VID_T> gid_list;
      for (int i = 1; i < worker_num; ++i) {
        int dst_worker_id = (worker_id + i) % worker_num;
        fid_t dst_fid = comm_spec_.WorkerToFrag(dst_worker_id);
        auto range = fragment->OuterVertices(dst_fid);
        VID_T offsets[2];
        offsets[0] = range.begin().GetValue();
        offsets[1] = range.end().GetValue();
        MPI_Send(&offsets[0], sizeof(VID_T) * 2, MPI_CHAR, dst_worker_id, 0,
                 comm_spec_.comm());
        gid_list.clear();
        gid_list.reserve(range.size());
        for (auto v : range) {
          gid_list.push_back(fragment->Vertex2Gid(v));
        }
        MPI_Send(&gid_list[0], sizeof(VID_T) * gid_list.size(), MPI_CHAR,
                 dst_worker_id, 0, comm_spec_.comm());
      }
    });

    std::thread recv_thread([&]() {
      std::vector<VID_T> gid_list;
      for (int i = 1; i < worker_num; ++i) {
        int src_worker_id = (worker_id + worker_num - i) % worker_num;
        fid_t src_fid = comm_spec_.WorkerToFrag(src_worker_id);
        VID_T offsets[2];
        MPI_Recv(&offsets[0], sizeof(VID_T) * 2, MPI_CHAR, src_worker_id, 0,
                 comm_spec_.comm(), MPI_STATUS_IGNORE);
        VertexRange<VID_T> range(offsets[0], offsets[1]);
        gid_list.clear();
        gid_list.resize(range.size());
        MPI_Recv(&gid_list[0], gid_list.size() * sizeof(VID_T), MPI_CHAR,
                 src_worker_id, 0, comm_spec_.comm(), MPI_STATUS_IGNORE);
        fragment->SetupMirrorInfo(src_fid, range, gid_list);
      }
    });

    recv_thread.join();
    send_thread.join();
  }
  struct work_unit {
    work_unit(fid_t fid_, size_t index_, size_t begin_)
        : fid(fid_), index(index_), begin(begin_) {}
    fid_t fid;
    size_t index;
    size_t begin;
  };
  // void vertexRecvRoutine() {
  //   PIEShuffleInPair<OID_T, VDATA_T> data_in(comm_spec_.fnum() - 1);
  //   data_in.Init(comm_spec_.comm(), vertex_tag);
  //   fid_t dst_fid;
  //   int src_worker_id;
  //   while (!data_in.Finished()) {
  //     src_worker_id = data_in.Recv(dst_fid);
  //     if (src_worker_id == -1) {
  //       break;
  //     }
  //     auto& dst_buf0 = got_vertices_id_;
  //     auto& dst_buf1 = got_vertices_data_;
  //     dst_buf0.emplace_back(std::move(data_in.Buffer0()));
  //     dst_buf1.emplace_back(std::move(data_in.Buffer1()));
  //   }
  // }
  // void edgeRecvRoutine() {
  //   PIEShuffleInTriple<OID_T, OID_T, EDATA_T> data_in(comm_spec_.fnum() -
  //   1); data_in.Init(comm_spec_.comm(), edge_tag); fid_t dst_fid; int
  //   src_worker_id; while (!data_in.Finished()) {
  //     src_worker_id = data_in.Recv(dst_fid);
  //     if (src_worker_id == -1) {
  //       break;
  //     }
  //     CHECK_EQ(dst_fid, comm_spec_.fid());
  //     got_edges_src_.emplace_back(std::move(data_in.Buffer0()));
  //     got_edges_dst_.emplace_back(std::move(data_in.Buffer1()));
  //     got_edges_data_.emplace_back(std::move(data_in.Buffer2()));
  //   }
  // }

  grape::CommSpec comm_spec_;
  std::shared_ptr<vertex_map_t> vm_ptr_;
  vertex_map_builder_t vm_builder_;

  // std::vector<PIEShuffleOutPair<OID_T, VDATA_T>> vertices_to_frag_;
  // std::vector<PIEShuffleOutTriple<OID_T, OID_T, EDATA_T>> edges_to_frag_;
  // std::vector<RSVector> got_vertices_data_;
  // std::thread vertex_recv_thread_;
  // std::thread edge_recv_thread_;
  // bool recv_thread_running_;

  std::vector<std::vector<VDATA_T>> got_vertices_data_;
  std::vector<std::vector<OID_T>> got_vertices_id_;

  std::vector<std::vector<OID_T>> got_edges_src_;
  std::vector<std::vector<OID_T>> got_edges_dst_;
  std::vector<std::vector<EDATA_T>> got_edges_data_;
  // notice vid here
  std::vector<grape::Edge<VID_T, EDATA_T>> processed_edges_;
  std::vector<grape::internal::Vertex<VID_T, VDATA_T>> processed_vertices_;

  // partitioner_t partitioner_;
  static constexpr int vertex_tag = 5;
  static constexpr int edge_tag = 6;
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_CORE_LOADER_BASIC_JAVA_IMMUTABLE_EDGECUT_FRAGMENT_LOADER_H_
