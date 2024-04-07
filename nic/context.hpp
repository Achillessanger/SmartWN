// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#ifndef RDMA_CONTEXT_HPP
#define RDMA_CONTEXT_HPP
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

#include "endpoint.hpp"
#include "helper.hpp"
#include "memory.hpp"
#include "host.hpp"
#include "engine.hpp"


class rdma_context {
 private:
  void *master_ = nullptr;
  // For connection setup (Local infomation)
  std::string devname_;
  union ibv_gid local_gid_;
  std::string local_ip_;
  struct ibv_context *ctx_ = nullptr;
  uint16_t lid_;
  uint8_t sl_;
  int port_;  // tcp port for server
  uint64_t completion_timestamp_mask_;

  // Memory Management

  struct ibv_pd * pd_;
  std::vector<rdma_region *> local_mempool_ = std::vector<rdma_region *>(2);
  std::mutex rmem_lock_;
  // For each remote host, we have a single mempool for it

  // For hardware timestamp
  std::vector<uint64_t> nic_process_time_;

  std::vector<rdma_host *> hosts_;
  std::mutex hosts_lock_;

  std::vector<rdma_io_engine *> io_engines_;
  struct rdma_io_engine *qos_io_engine_;

  int num_of_hosts_ = 0;  // How many hosts to set up connections
  int num_per_host_ = 0;  // How many connections each host will set


  int num_of_recv_ = 0;
  std::mutex numlock_;

  bool _print_thp;

  rdma_buffer *CreateBufferFromInfo(struct connect_info *info);
  void SetInfoByBuffer(struct connect_info *info, rdma_buffer *buf);
  void SetEndpointInfo(rdma_endpoint *endpoint, struct connect_info *info);
  void GetEndpointInfo(rdma_endpoint *endpoint, struct connect_info *info);


  // Basic Initialization: connection setup info.
  int InitDevice();

  // Memory Management and Transportation Allocation
#ifdef GDR
  int InitCuda();
#endif
  int InitMemory();
  int InitTransport();
  int InitQoSEp();

  int ConnectionSetup(const char *server, int port);
  int AcceptHandler(int connfd);

  int PollCompletion();


  std::string GidToIP(
      const union ibv_gid &gid);  // Translate local gid to a IP string.

 public:
  rdma_context(const char *dev_name, int gid_idx, int num_of_hosts,
               int num_per_host, bool print_thp)
      : devname_(dev_name),
        num_of_hosts_(num_of_hosts),
        num_per_host_(num_per_host),
        _print_thp(print_thp) {}

  int Init();

  std::vector<rdma_io_engine *> GetEngines() { return io_engines_; }
  rdma_io_engine *GetQosEngine() { return qos_io_engine_; }

  // Connection Setup: Server side
  int Listen();

  // Connection Setup: Client side
  int Connect(const char *server, int port, int connid);

  // Assitant function: Find the EP of the given IP
  rdma_host *FindHost(std::string &ip) {
    // TODO
    for (auto host : hosts_) {
      if (host->address == ip) {
        return host;
      }
    }

    LOG(ERROR) << "Failed to find the host of " << ip;
    return nullptr;
  }

  int GetHostNum() {
    int ret;

    hosts_lock_.lock();
    ret = hosts_.size();
    hosts_lock_.unlock();

    return ret;
  }

  std::vector<rdma_host *> CopyHosts() {
    return hosts_;
  }

  rdma_region *MemoryRegion(int idx) {
    if (idx != 0 && idx != 1) return nullptr;
    return local_mempool_[idx];
  }

  struct ibv_pd *GetPd() {
    return pd_;
  }

  std::string GetIp() { return local_ip_; }

  struct rdma_io_engine *GetEngine(int id) {
    int engine_num = io_engines_.size();
    return io_engines_[id % engine_num];
  }

};

#endif
