#include "rdma-gpunode.hpp"


int client_engine::Send(void (*callback)(uint64_t, char *, int), uint64_t ctx, char *packet, int length, char *dest) {
  struct send_task *stask = new send_task(callback, ctx, length, packet, dest);
  if (!stask) {
    return -1;  // No more memory, upper programs should be responsible for this scenario.
  }

  engine->PutTask(stask);

  return 0;   // Return correctly.
}


/* Upper layer interface: SetHosts() */
int client_session::SetHosts(std::vector< std::string > &vec) {
  int ret = 0;
  for (auto host : vec) {
    remote_hosts.push_back(host);
    ++ret;
  }

  return ret;
}


// Get Engine.
client_engine client_session::GetEngine(int id) {
  struct rdma_io_engine *io_engine = g_context->GetEngine(id);
  client_engine ret(io_engine);

  return ret;
}


/* (Worker) IO Engine */
/* Responsible for only data channel. */
void client_session::data_channel(rdma_io_engine *engine) {
  struct send_task *task;
  int n;
  struct ibv_wc wc[CQ_POLL_DEPTH];

  while (1) {
    task = engine->GetTask();

    if (task) {
      // Generate a new rdma_request.
      struct rdma_request *rreq = new rdma_request();
      struct ibv_sge sge;

      // Allocate a buffer to store each sub-request.
      auto buf = engine->PickNextBuffer(0);
      if (buf == nullptr) {
        // No buffers left, put back the request.
        delete rreq;
        engine->PutTask(task);
      }
      else {
        sge.addr = (uint64_t)buf;
        sge.lkey = buf->local_K_;
        sge.length = buf->size_;
        rreq->sglist.push_back(sge);
        rreq->sge_num = 1;
        rreq->opcode = IBV_WR_SEND;

        // Header
        char *header = (char *)buf->addr_;
      
        // Payload
        char *payload = header + 20;
        int lreq_len = task->length;

        if (FLAGS_sbuf_size < lreq_len + 20) {
          // If the length is bigger than the buffer, cut down the packet.
          lreq_len = FLAGS_sbuf_size - 20;
        }
        memcpy(header, &(task->callback), sizeof(void *));
        memcpy(header+8, &(task->context), sizeof(uint64_t));
        memcpy(header+16, &lreq_len, sizeof(int));
        memcpy(payload, task->source, lreq_len);

        // Post Send!
        struct rdma_endpoint *send_ep = engine->PickEp(task->dest);
        if (send_ep->PostSend(rreq)) {
          // May be ENOMEM, which means that send queue is full.
          engine->ReleaseBuffer(0, (struct rdma_buffer *)rreq->sglist[0].addr);
          delete rreq;
          engine->PutTask(task);
        }
        else {
          delete task;
        } 
      }
    }

    // Poll CQ.
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
         case IBV_WC_SEND: {
          // Release the send buffers.
          struct rdma_transmit_status *status = (struct rdma_transmit_status *)wc[i].wr_id;
          struct rdma_request *req = status->req;

          for (int j=0; j<req->sge_num; ++j) {
            engine->ReleaseBuffer(0, (struct rdma_buffer *)req->sglist[j].addr);
          }
          
          // Relevant data structure can be freed.
          delete req;
          delete status;
          break;
         }
         case IBV_WC_RECV: {
          // Get relevant information.
          struct rdma_transmit_status *status = (struct rdma_transmit_status *)wc[i].wr_id;
          struct rdma_request *req = status->req;
          struct rdma_endpoint *recv_ep = status->ep;

          // Callback!
          char *recv_packet = (char *)((struct rdma_buffer *)req->sglist[0].addr)->addr_;
          void (*recv_callback)(uint64_t, char *, int);
          uint64_t recv_context;
          int recv_length;
          char *recv_payload = recv_packet + 20;
          memcpy(&recv_callback, recv_packet, sizeof(void *));
          memcpy(&recv_context, recv_packet+8, sizeof(uint64_t));
          memcpy(&recv_length, recv_packet+16, sizeof(int));

          recv_callback(recv_context, recv_payload, recv_length);
          delete status;

          // Another Post Recv.
          recv_ep->PostRecv(req);          
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
bool client_session::Init(int argc, char **argv) {
  ibv_fork_init();

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (!ParametersCheck()) {
    goto destroy;
  }

  /* get global context */
  g_context = new rdma_context(FLAGS_dev.c_str(), FLAGS_gid, remote_hosts.size(), FLAGS_qp_num, FLAGS_print_thp);
  if (g_context->Init()) {
    fprintf(stderr, "rdma_context::Init: Failed to initiate global rdma context.\n");
    goto destroy;
  }

  /* build connection */
  for (size_t i=0; i<remote_hosts.size(); ++i) {
    printf("Connect to %s:%d\n", remote_hosts[i].c_str(), FLAGS_port);
    fflush(stdout);
    if (g_context->Connect(remote_hosts[i].c_str(), FLAGS_port, i)) {
      fprintf(stderr, "rdma_context::Connect: Failed to connect to %s\n", remote_hosts[i].c_str());
      goto destroy;
    }
  }

  return false;

 destroy:
  return true;
}


/* Start program */
void client_session::Start() {
  for (auto engine : g_context->GetEngines()) {
    // Launch a thread of this engine.
    std::thread engine_thread = std::thread(&client_session::data_channel, this, engine);
    engine_thread.detach();
  }

  g_init_flag = true;
}