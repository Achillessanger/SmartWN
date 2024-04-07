// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include "endpoint.hpp"

#include "context.hpp"


int rdma_endpoint::PostSend(rdma_request *req, ibv_ah *ah) {
  struct ibv_send_wr wr_list;
  struct ibv_sge sgs[kMaxSge];

  int wr_size = 0;
  for (int j = 0; j < req->sge_num; j++) {
    sgs[j].addr = ((struct rdma_buffer *)req->sglist[j].addr)->addr_;
    if (qp_type_ == IBV_QPT_UD) {
      sgs[j].addr += 40;
      sgs[j].length = 1000;     // MTU(To be improved.)
      LOG(INFO) << "Adjust for IBV_QPT_UD";
    }
    else {
      sgs[j].length = req->sglist[j].length;
    }
    sgs[j].lkey = req->sglist[j].lkey;
    wr_size += sgs[j].length;
  }
  memset(&wr_list, 0, sizeof(struct ibv_send_wr));
  wr_list.num_sge = req->sge_num;
  wr_list.opcode = IBV_WR_SEND;
  switch (wr_list.opcode) {
   case IBV_WR_SEND:
    if (qp_type_ == IBV_QPT_UD) {
      wr_list.wr.ud.remote_qkey = 0x1234;
      wr_list.wr.ud.remote_qpn = remote_qpn_;
      wr_list.wr.ud.ah = ah;
    }
    break;
   default:
    fprintf(stderr, "PostSend: Not IBV_WR_SEND opcode in ibv_post_send().\n");
    return -1;
  }
  wr_list.send_flags = IBV_SEND_SIGNALED;
  // Inline if we can
  if (wr_size <= kInlineThresh && wr_list.opcode != IBV_WR_RDMA_READ)
    wr_list.send_flags |= IBV_SEND_INLINE;

  struct rdma_transmit_status *status = new rdma_transmit_status();
  status->ep = this;
  status->req = req;

  wr_list.wr_id = (uint64_t)status;
  wr_list.sg_list = sgs;
  wr_list.next = nullptr;

  struct ibv_send_wr *bad_wr = nullptr;

  if (ibv_post_send(qp_, &wr_list, &bad_wr)) {
    // fprintf(stderr, "PostSend: Failed in ibv_post_send().\n");
    return -1;
  }

  return 0;
}

int rdma_endpoint::PostRecv(rdma_request *req) {
  rdma_context *ctx = (rdma_context *)master_;

  struct ibv_sge sg[kMaxSge];
  struct ibv_recv_wr wr;
  struct ibv_recv_wr *bad_wr;

  for (int j = 0; j < req->sge_num; j++) {
    sg[j].addr = ((struct rdma_buffer *)req->sglist[j].addr)->addr_;
    sg[j].lkey = req->sglist[j].lkey;
    if (qp_type_ == IBV_QPT_UD) {
      sg[j].length = 1040;    // MTU(To be improved.)
      LOG(INFO) << "Adjust for UD in PostRecv.";
    }
    else {
      sg[j].length = req->sglist[j].length;
    }

  }

  struct rdma_transmit_status *status = new rdma_transmit_status();
  status->ep = this;
  status->req = req;

  memset(&wr, 0, sizeof(struct ibv_recv_wr));
  wr.num_sge = req->sge_num;
  wr.sg_list = sg;
  wr.next = nullptr;
  wr.wr_id = (uint64_t)status;

  if (auto ret = ibv_post_recv(qp_, &wr, &bad_wr)) {
    fprintf(stderr, "PostRecv: Failed in ibv_post_recv().\n");
    return -1;
  }

  return 0;
}

int rdma_endpoint::RestoreFromERR() {
  struct ibv_qp_attr attr;
  int attr_mask;
  attr_mask = IBV_QP_STATE;
  memset(&attr, 0, sizeof(struct ibv_qp_attr));
  attr.qp_state = IBV_QPS_RESET;
  if (ibv_modify_qp(qp_, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to restore QP from ERR to RESET";
    return -1;
  }
  auto remote_gid = remote_gid_;
  if (Activate(remote_gid)) {
    PLOG(ERROR) << "Failed to restore QP to RTS";
    return -1;
  }
  return 0;
}

int rdma_endpoint::Activate(const union ibv_gid &remote_gid, int r_sl) {
  remote_gid_ = remote_gid;
  struct ibv_qp_attr attr;
  int attr_mask;
  attr = MakeQpAttr(IBV_QPS_INIT, qp_type_, 0, remote_gid, &attr_mask, r_sl);
  if (ibv_modify_qp(qp_, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to INIT";
    return -1;
  }
  attr = MakeQpAttr(IBV_QPS_RTR, qp_type_, remote_qpn_, remote_gid, &attr_mask, r_sl);
  if (ibv_modify_qp(qp_, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTR";
    return -1;
  }
  attr = MakeQpAttr(IBV_QPS_RTS, qp_type_, remote_qpn_, remote_gid, &attr_mask, r_sl);
  if (ibv_modify_qp(qp_, &attr, attr_mask)) {
    PLOG(ERROR) << "Failed to modify QP to RTS";
    return -1;
  }

  return 0;
}