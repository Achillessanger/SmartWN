#include "rdma-memorynode.hpp"


void server_session::data_channel(rdma_io_engine *engine) {
  int n = 0;
  struct ibv_wc wc[CQ_POLL_DEPTH];
  char callback_ret[FLAGS_sbuf_size] = {0};  /* The size of return value from the callback function */
  int callback_ret_len = 0;

  while (1) {
    // First, poll recv cq to launch a task.
    for (auto cq : engine->cqs) {
      n = ibv_poll_cq(cq, CQ_POLL_DEPTH, wc);
      if (n < 0) {
        LOG(ERROR) << "Get incorrect return values in ibv_poll_cq()";
        exit(-1);
      }
      for (int i=0; i<n; ++i) {
        if (wc[i].status != IBV_WC_SUCCESS) {
          LOG(ERROR) << "Get bad WC status " << wc[i].status;
          exit(-1);
        }
        switch (wc[i].opcode) {
         case IBV_WC_RECV: {
          struct rdma_transmit_status *status = (struct rdma_transmit_status *)wc[i].wr_id;
          struct rdma_endpoint *ep = status->ep;
          struct rdma_request *recv_req = status->req;
          char *recv_packet, *recv_payload;
          int recv_length;
          uint64_t recv_callback, recv_context;

          // 1. Parse the recv packet.
          callback_ret_len = 0;
          memset(callback_ret, 0, FLAGS_sbuf_size);

          recv_packet = (char *)((struct rdma_buffer *)recv_req->sglist[0].addr)->addr_;
          memcpy(&recv_callback, recv_packet, sizeof(uint64_t));
          memcpy(&recv_context, recv_packet+8, sizeof(uint64_t));
          memcpy(&recv_length, recv_packet+16, sizeof(int));
          recv_payload = recv_packet + 20;

          Callback(recv_payload, recv_length, callback_ret, &callback_ret_len);

          // 2. Another post recv.
          ep->PostRecv(recv_req);

          // 3. Send back the return value filled by the callback function.
          struct rdma_request *send_req = new rdma_request();

          send_req->sge_num = 1;
          send_req->opcode = IBV_WR_SEND;

          struct ibv_sge sge;
          auto buf = engine->PickNextBuffer(0);
          while (buf == nullptr) {
            usleep(50);
            buf = engine->PickNextBuffer(0);
          }
          sge.addr = (uint64_t)buf;
          sge.lkey = buf->local_K_;
          sge.length = buf->size_;
          send_req->sglist.push_back(sge);

          char *header = (char *)buf->addr_;
          char *payload = header + 20;

          if (FLAGS_sbuf_size < callback_ret_len + 20) {
            /* If the length is bigger than the buffer, cut down the packet. */
            callback_ret_len = FLAGS_sbuf_size - 20;
          }
          memcpy(header, &recv_callback, sizeof(uint64_t));
          memcpy(header+8, &recv_context, sizeof(uint64_t));
          memcpy(header+16, &callback_ret_len, sizeof(int));
          memcpy(payload, callback_ret, callback_ret_len);

          /* ibv_post_send. */
          while (ep->PostSend(send_req)) {
            usleep(50);
          }
          
          delete status;
          break;
         }
         case IBV_WC_SEND: {
          // Release the send buffers.
          struct rdma_transmit_status *sstatus = (struct rdma_transmit_status *)wc[i].wr_id;
          struct rdma_request *req = sstatus->req;

          for (int j=0; j<req->sge_num; ++j) {
            engine->ReleaseBuffer(0, (struct rdma_buffer *)req->sglist[j].addr);
          }
          
          // Relevant data structure can be freed.
          delete req;
          delete sstatus;
         }
          
         default:
          // Nothing to do here.
          break;
        }
      } 
    }
  }
}


/* Initialization */
bool server_session::Init(int argc, char **argv) {
  std::thread listen_thread;

  ibv_fork_init();

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (!ParametersCheck()) {
    goto destroy;
  }

  /* get global context */
  g_context = new rdma_context(FLAGS_dev.c_str(), FLAGS_gid, FLAGS_host_num, FLAGS_qp_num, FLAGS_print_thp);
  if (g_context->Init()) {
    fprintf(stderr, "rdma_context::Init: Failed to initiate global rdma context.\n");
    goto destroy;
  }

  listen_thread = std::thread(&rdma_context::Listen, g_context);
  listen_thread.detach();

  return false;

 destroy:
  return true;
}


/* Start program */
void server_session::Start() {
  for (auto engine : g_context->GetEngines()) {
    // Launch a thread of this engine.
    LOG(INFO) << "A worker thread is launched!";
    std::thread engine_thread = std::thread(&server_session::data_channel, this, engine);
    engine_thread.detach();
  }
}