// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#ifndef RDMA_ENDPOINT_HPP
#define RDMA_ENDPOINT_HPP
#include <queue>

#include "helper.hpp"
#include "memory.hpp"


struct rdma_transmit_status {
  struct rdma_endpoint *ep;
  struct rdma_request *req;
};


class rdma_request {
 public:
  enum ibv_wr_opcode opcode;  // Opcode of this request
  int sge_num;                // sge_num of this request
  std::vector<struct ibv_sge> sglist;
};

class rdma_endpoint {
 private:
  struct ibv_qp *qp_ = nullptr;
  uint32_t id_ = 0;
  enum ibv_qp_type qp_type_;
  union ibv_gid remote_gid_;


  // Remote info for UD
  uint16_t dlid_ = 0;
  uint8_t remote_sl_ = 0;
  // Remote memory pool id
  int rmem_id_ = -1;

  bool activated_ = false;
  void *master_ = nullptr;
  void *context_ = nullptr;
  void *host_ = nullptr;
  void *engine_ = nullptr;

 public:
  // Remote Information
  std::string remote_server_;
  uint32_t remote_qpn_ = 0;
  bool is_qos = false;

  rdma_endpoint(uint32_t id, ibv_qp *qp, ibv_qp_type qp_tp = IBV_QPT_RC)
      : qp_(qp),
        id_(id),
        qp_type_(qp_tp) {}
  ~rdma_endpoint() {
    if (qp_) ibv_destroy_qp(qp_);
  }

 public:
  int PostSend(rdma_request *req, ibv_ah *ah = nullptr);
  int PostRecv(rdma_request *req);
  int Activate(const union ibv_gid &remote_gid, int r_sl = 0);
  int RestoreFromERR();

  enum ibv_qp_type GetType() { return qp_type_; }
  int GetQpn() { return qp_->qp_num; }
  int GetMemId() { return rmem_id_; }
  bool GetActivated() { return activated_; }
  void SetQpn(int qpn) { remote_qpn_ = qpn; }
  void SetLid(int lid) { dlid_ = lid; }
  void SetSl(int sl) { remote_sl_ = sl; }
  void SetContext(void *context) { context_ = context; }
  void SetMaster(void *master) { master_ = master; }
  void SetActivated(bool state) { activated_ = state; }
  void SetMemId(int remote_mem_id) { rmem_id_ = remote_mem_id; }
  void SetServer(const std::string &name) { remote_server_ = name; }
  void SetHost(void *host) { host_ = host; }
  void SetEngine(void *engine) { engine_ = engine; }
  void *GetHost() { return host_; }
  void *GetEngine() { return engine_; }
};

#endif