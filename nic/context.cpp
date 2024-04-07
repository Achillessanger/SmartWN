// MIT License

// Copyright (c) 2021 ByteDance Inc. All rights reserved.
// Copyright (c) 2021 Duke University.  All rights reserved.

// See LICENSE for license information

#include "context.hpp"

#include <malloc.h>

#include <algorithm>
#include <thread>



std::string rdma_context::GidToIP(const union ibv_gid &gid) {
  std::string ip =
      std::to_string(gid.raw[12]) + "." + std::to_string(gid.raw[13]) + "." +
      std::to_string(gid.raw[14]) + "." + std::to_string(gid.raw[15]);
  return ip;
}

int rdma_context::Init() {
#ifdef GDR
  if (InitCuda() < 0) {
    LOG(ERROR) << "InitCuda() failed";
    return -1;
  }
#endif
  if (InitDevice() < 0) {
    LOG(ERROR) << "InitDevice() failed";
    return -1;
  }
  if (InitMemory() < 0) {
    LOG(ERROR) << "InitMemory() failed";
    return -1;
  }
  return 0;
}


int rdma_context::InitDevice() {
  struct ibv_device *dev = nullptr;
  struct ibv_device **device_list = nullptr;
  int n;
  bool flag = false;
  device_list = ibv_get_device_list(&n);
  if (!device_list) {
    PLOG(ERROR) << "ibv_get_device_list() failed when initializing clients";
    return -1;
  }
  for (int i = 0; i < n; i++) {
    dev = device_list[i];
    if (!strncmp(ibv_get_device_name(dev), devname_.c_str(),
                 strlen(devname_.c_str()))) {
      flag = true;
      break;
    }
  }
  if (!flag) {
    LOG(ERROR) << "We didn't find device " << devname_ << ". So we exit.";
  }
  this->ctx_ = ibv_open_device(dev);
  if (!ctx_) {
    PLOG(ERROR) << "ibv_open_device() failed";
    return -1;
  }

  if (ibv_query_gid(ctx_, 1, FLAGS_gid, &local_gid_) < 0) {
    PLOG(ERROR) << "ibv_query_gid() failed";
    return -1;
  }
  local_ip_ = GidToIP(local_gid_);

  struct ibv_port_attr port_attr;
  memset(&port_attr, 0, sizeof(port_attr));
  if (ibv_query_port(ctx_, 1, &port_attr)) {
    PLOG(ERROR) << "ibv_query_port() failed";
    exit(1);
  }
  lid_ = port_attr.lid;
  sl_ = port_attr.sm_sl;
  port_ = FLAGS_port;
  return 0;
}

#ifdef GDR
int rdma_context::InitCuda() {
  if (FLAGS_use_cuda == false) return 0;
  int gpu_id = FLAGS_gpu_id;
  CUdevice cu_device;

  CUresult ret = cuInit(0);
  if (ret != CUDA_SUCCESS) {
    PLOG(ERROR) <<  "cuInit(0)";
    return -1;
  }

  int n = 0;
  ret = cuDeviceGetCount(&n);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuDeviceGetCount() return " << ret;
    return -1;
  }
  if (n == 0) {
    LOG(ERROR) << "No available Cuda device";
    return -1;
  }
  if (gpu_id >= n) {
    LOG(ERROR) << "No " << gpu_id << " device";
    return -1;
  }
  ret = cuDeviceGet(&cuDevice_, gpu_id);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuDeviceGet() failed with " << ret;
  }

  ret = cuCtxCreate(&cuContext_, CU_CTX_MAP_HOST, cuDevice_);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuCtxCreate() failed with " << ret; 
    return -1;
  }

  ret = cuCtxSetCurrent(cuContext_);
  if (ret != CUDA_SUCCESS) {
    LOG(ERROR) << "cuCtxSetCurrent() failed with " << ret; 
    return -1;
  }
  return 0;
}
#endif


int rdma_context::InitMemory() {
  // Allocate PD
  pd_ = ibv_alloc_pd(ctx_);
  if (!pd_) {
    PLOG(ERROR) << "ibv_alloc_pd() failed";
    return -1;
  }

  // Allocate IO engines
  for (int i = 0; i < FLAGS_ioengine_num; ++i) {
    struct rdma_io_engine *io_engine = new rdma_io_engine();
    io_engine->context = (void *)this;
    io_engines_.push_back(io_engine);

    // Allocate each IO engine's CQ and MP.
    for (int j=0; j<FLAGS_cq_num; ++j) {
      // Completion queues.
      struct ibv_cq *cq;
      cq = ibv_create_cq(ctx_, FLAGS_cq_depth, nullptr, nullptr, 0);
      if (!cq) {
        PLOG(ERROR) << "ibv_create_cq() failed";
        return -1;        
      }
      io_engine->cqs.push_back(cq);
    }

    // Memory pools.
    struct rdma_region *region;
    region = new rdma_region(pd_, FLAGS_sbuf_size, FLAGS_buf_num, FLAGS_memalign, 0);
    if (region->Mallocate()) {
      LOG(ERROR) << "Send Region Memory allocation failed";
      return -1;      
    }
    io_engine->send_region = region;
    region = new rdma_region(pd_, FLAGS_rbuf_size, FLAGS_buf_num, FLAGS_memalign, 0);
    if (region->Mallocate()) {
      LOG(ERROR) << "Recv Region Memory allocation failed";
      return -1;      
    }
    io_engine->recv_region = region;    
  }

  return 0;
}


int rdma_context::Listen() {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));
  hints.ai_flags = AI_PASSIVE;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char *service;
  int sockfd = -1, err, n;
  int *connfd;
  if (asprintf(&service, "%d", port_) < 0) return -1;
  if (getaddrinfo(nullptr, service, &hints, &res)) {
    LOG(ERROR) << gai_strerror(n) << " for port " << port_;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      n = 1;
      setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n));
      if (!bind(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't listen to port " << port_;
    return -1;
  }
  LOG(INFO) << "About to listen on port " << port_;
  err = listen(sockfd, 1024);
  if (err) {
    PLOG(ERROR) << "listen() failed";
    return -1;
  }
  LOG(INFO) << "Server listen thread starts";
  while (true) {
    connfd = (int *)malloc(sizeof(int));
    *connfd = accept(sockfd, nullptr, 0);
    if (*connfd < 0) {
      PLOG(ERROR) << "Accept Error";
      break;
    }
    // [TODO] connection handler
    std::thread handler =
        std::thread(&rdma_context::AcceptHandler, this, *connfd);
    handler.detach();
    free(connfd);
  }
  // The loop shall never end.
  free(connfd);
  close(sockfd);
  return -1;
}

void rdma_context::SetEndpointInfo(rdma_endpoint *endpoint,
                                   struct connect_info *info) {
  switch (endpoint->GetType()) {
    case IBV_QPT_UD:
      endpoint->SetLid((info->info.channel.dlid));
      endpoint->SetSl((info->info.channel.sl));
    case IBV_QPT_UC:
    case IBV_QPT_RC:
      endpoint->SetQpn((info->info.channel.qp_num));
      break;
    default:
      LOG(ERROR) << "Currently we don't support other type of QP";
  }
}

void rdma_context::GetEndpointInfo(rdma_endpoint *endpoint,
                                   struct connect_info *info) {
  memset(info, 0, sizeof(connect_info));
  info->type = (kChannelInfoKey);
  switch (endpoint->GetType()) {
    case IBV_QPT_UD:
      info->info.channel.dlid = (lid_);
      info->info.channel.sl = (sl_);
    case IBV_QPT_UC:
    case IBV_QPT_RC:
      info->info.channel.qp_num = (endpoint->GetQpn());
      break;
    default:
      LOG(ERROR) << "Currently we don't support other type of QP";
  }
}

int rdma_context::AcceptHandler(int connfd) {
  int n, number_of_qp;
  char *conn_buf = (char *)malloc(sizeof(connect_info));
  connect_info *info = (connect_info *)conn_buf;
  union ibv_gid gid;
  std::vector<rdma_buffer *> buffers;
  rdma_host *new_host;  
  int host_id, engine_id = -1, engine_cqid = -1;
  rdma_endpoint *ep;

  int rbuf_id = -1;
  if (!conn_buf) {
    LOG(ERROR) << "Malloc for exchange buffer failed";
    return -1;
  }
  n = read(connfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Server Read";
    LOG(ERROR) << n << "/" << (int)sizeof(connect_info)
               << ": Couldn't read remote address";
    goto out;
  }
  if ((info->type) != kHostInfoKey) {
    LOG(ERROR) << "The first exchange type should be " << kHostInfoKey;
    goto out;
  }
  number_of_qp = (info->info.host.number_of_qp);
  if (number_of_qp <= 0) {
    LOG(ERROR) << "The number of qp should be positive";
    goto out;
  }
  numlock_.lock();
  num_of_recv_ += number_of_qp;
  numlock_.unlock();

  // Copy the remote gid.
  memcpy(&gid, &info->info.host.gid, sizeof(union ibv_gid));

  // Put local info to connect_info and send
  memset(info, 0, sizeof(connect_info));
  info->type = (kHostInfoKey);
  memcpy(&info->info.host.gid, &local_gid_, sizeof(union ibv_gid));
  info->info.host.number_of_qp = (number_of_qp);
  if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send local address";
    goto out;
  }

  // Add a new rdma host.
  new_host = new rdma_host(GidToIP(gid));
  if (!new_host) {
    PLOG(ERROR) << "Failed to malloc rdma_host";
    goto out;
  }
  hosts_lock_.lock();
  host_id = hosts_.size();
  hosts_.push_back(new_host);
  hosts_lock_.unlock();


  // Get the connection channel info from remote
  for (int i=0; i<number_of_qp; ++i) {
    // Set the IO engine of the endpoint.
    ++engine_id;
    if (engine_id == FLAGS_ioengine_num) {
      engine_id = 0;
    }
    struct rdma_io_engine *engine = io_engines_[engine_id];

    ++engine_cqid;
    if (engine_cqid == engine->cqs.size()) {
      engine_cqid = 0;
    }

    // Get a new rdma_endpoint ep.
    struct ibv_qp_init_attr qp_init_attr = MakeQpInitAttr(
      engine->cqs[engine_cqid], engine->cqs[engine_cqid], FLAGS_send_wq_depth, FLAGS_recv_wq_depth, IBV_QPT_RC);
    auto qp = ibv_create_qp(pd_, &qp_init_attr);
    if (!qp) {
      PLOG(ERROR) << "ibv_create_qp() failed";
      delete qp;
      return -1;
    }
    ep = new rdma_endpoint(host_id, qp);
    ep->SetMaster(this);
    ep->SetHost(new_host);
    ep->SetEngine(engine);
    new_host->eps.push_back(ep);

    n = read(connfd, conn_buf, sizeof(connect_info));
    if (n != sizeof(connect_info)) {
      PLOG(ERROR) << "Server read";
      LOG(ERROR) << n << "/" << (int)sizeof(connect_info) << ": Read " << i
                 << " endpoint's info failed";
      goto out;
    }
    if ((info->type) != kChannelInfoKey) {
      LOG(ERROR) << "Exchange data failed. Type Error: " << (info->type);
      goto out;
    }
    SetEndpointInfo(ep, info);
    GetEndpointInfo(ep, info);
    if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
      LOG(ERROR) << "Couldn't send " << i << " endpoint's info";
      goto out;
    }

    if (ep->Activate(gid)) {
      LOG(ERROR) << "Activate Recv Endpoint " << i << " failed";
      goto out;
    }

    // Post The first batch
    for (int j=0; j<FLAGS_recv_batch; ++j) {
      struct rdma_request *req = new rdma_request();
      
      struct ibv_sge sg;
      auto buf = engine->PickNextBuffer(1);
      sg.addr = (uint64_t)buf;
      sg.lkey = buf->local_K_;
      sg.length = buf->size_;
      req->sglist.push_back(sg);
      req->sge_num = 1;

      if (ep->PostRecv(req)) {
        LOG(ERROR) << "The " << j << " -th ep failed to post first batch";
        goto out;
      }
    }

    ep->SetActivated(true);
    ep->SetServer(GidToIP(gid));
    engine->PutEndpoint(ep);
    LOG(INFO) << "Endpoint " << i << " has started (engine: " << (uint64_t)engine << ")";
  }


  // After connection setup. Tell remote that they can send.
  n = read(connfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Server read";
    LOG(ERROR) << n << "/" << (int)sizeof(connect_info)
               << ": Read Send request failed";
    goto out;
  }
  if ((info->type) != kGoGoKey) {
    LOG(ERROR) << "GOGO request failed";
    goto out;
  }
  memset(info, 0, sizeof(connect_info));
  info->type = (kGoGoKey);
  if (write(connfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send GOGO!!";
    goto out;
  }

  new_host->initialized = true;

  close(connfd);
  free(conn_buf);
  return 0;
out:
  close(connfd);
  free(conn_buf);
  return -1;
}


int rdma_context::ConnectionSetup(const char *server, int port) {
  struct addrinfo *res, *t;
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  char *service;
  int n;
  int sockfd = -1;
  int err;
  if (asprintf(&service, "%d", port) < 0) return -1;
  n = getaddrinfo(server, service, &hints, &res);
  if (n < 0) {
    LOG(ERROR) << gai_strerror(n) << " for " << server << ":" << port;
    free(service);
    return -1;
  }
  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }
  freeaddrinfo(res);
  free(service);
  if (sockfd < 0) {
    LOG(ERROR) << "Couldn't connect to " << server << ":" << port;
    return -1;
  }
  return sockfd;
}

int rdma_context::Connect(const char *server, int port, int connid) {
  struct rdma_host *new_host;
  int host_id;
  struct rdma_endpoint *ep;
  int engine_id = -1, engine_cqid = -1;

  int sockfd = -1;
  for (int i = 0; i < kMaxConnRetry; i++) {
    sockfd = ConnectionSetup(server, port);
    if (sockfd > 0) break;
    LOG(INFO) << "Try connect to " << server << ":" << port << " failed for "
              << i + 1 << " times...";
    sleep(1);
  }
  if (sockfd < 0) return -1;

  union ibv_gid remote_gid;
  char *conn_buf = (char *)malloc(sizeof(connect_info));
  if (!conn_buf) {
    LOG(ERROR) << "Malloc for metadata failed";
    return -1;
  }
  connect_info *info = (connect_info *)conn_buf;
  int number_of_qp, n = 0, rbuf_id = -1;

  std::vector<rdma_buffer *> buffers;
  memset(info, 0, sizeof(connect_info));
  info->info.host.number_of_qp = (num_per_host_ * io_engines_.size());
  memcpy(&info->info.host.gid, &local_gid_, sizeof(union ibv_gid));
  if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Couldn't send local address";
    n = -1;
    goto out;
  }
  n = read(sockfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "client read";
    LOG(ERROR) << "Read only " << n << "/" << sizeof(connect_info) << " bytes";
    goto out;
  }
  if (info->type != kHostInfoKey) {
    LOG(ERROR) << "The First exchange should be host info";
    goto out;
  }
  number_of_qp = (info->info.host.number_of_qp);
/*
  if (number_of_qp != num_per_host_) {
    LOG(ERROR) << "Receiver does not support " << num_per_host_ << " senders";
    goto out;
  }
*/
  memcpy(&remote_gid, &info->info.host.gid, sizeof(union ibv_gid));

  // Add a new rdma host.
  new_host = new rdma_host(GidToIP(remote_gid));
  if (!new_host) {
    PLOG(ERROR) << "Failed to malloc rdma_host";
    goto out;
  }
  hosts_lock_.lock();
  host_id = hosts_.size();
  hosts_.push_back(new_host);
  hosts_lock_.unlock();

  for (int i=0; i<num_per_host_; ++i) {
    // Set the IO engine of the endpoint.
    for (auto engine : io_engines_) {
      ++engine_cqid;
      if (engine_cqid == engine->cqs.size()) {
        engine_cqid = 0;
      }

      // Get a new rdma_endpoint ep.
      struct ibv_qp_init_attr qp_init_attr = MakeQpInitAttr(
        engine->cqs[engine_cqid], engine->cqs[engine_cqid], FLAGS_send_wq_depth, FLAGS_recv_wq_depth, IBV_QPT_RC);
  
      auto qp = ibv_create_qp(pd_, &qp_init_attr);
      if (!qp) {
        PLOG(ERROR) << "ibv_create_qp() failed";
        delete qp;
        return -1;
      }

      ep = new rdma_endpoint(host_id, qp);
      ep->SetMaster(this);
      ep->SetHost(new_host);
      ep->SetEngine(engine);
      ep->SetSl(7);

      GetEndpointInfo(ep, info);
      if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
        LOG(ERROR) << "Couldn't send " << i << " endpoint's info";
        goto out;
      }
      n = read(sockfd, conn_buf, sizeof(connect_info));
      if (n != sizeof(connect_info)) {
        PLOG(ERROR) << "Client Read";
        LOG(ERROR) << "Read only " << n << "/" << sizeof(connect_info)
                   << " bytes";
        goto out;
      }
      if ((info->type) != kChannelInfoKey) {
        LOG(ERROR) << "Exchange Data Failed. Type Received is " << (info->type)
                   << ", expected " << kChannelInfoKey;
        goto out;
      }
      SetEndpointInfo(ep, info);
      if (ep->Activate(remote_gid)) {
        LOG(ERROR) << "Activate " << i << " endpoint failed";
        goto out;
      }

      // Post The first batch
      for (int j=0; j<FLAGS_recv_batch; ++j) {
        struct rdma_request *req = new rdma_request();
        struct ibv_sge sg;
        auto buf = engine->PickNextBuffer(1);
        sg.addr = (uint64_t)buf;
        sg.lkey = buf->local_K_;
        sg.length = buf->size_;
        req->sglist.push_back(sg);
        req->sge_num = 1;

        if (ep->PostRecv(req)) {
          LOG(ERROR) << "The " << j << " -th ep failed to post first batch";
          goto out;
        }
      }

      ep->SetActivated(true);
      ep->SetServer(GidToIP(remote_gid));
      engine->PutEndpoint(ep);
    }
  }


  memset(info, 0, sizeof(connect_info));
  info->type = (kGoGoKey);
  if (write(sockfd, conn_buf, sizeof(connect_info)) != sizeof(connect_info)) {
    LOG(ERROR) << "Ask GOGO send failed";
    goto out;
  }
  n = read(sockfd, conn_buf, sizeof(connect_info));
  if (n != sizeof(connect_info)) {
    PLOG(ERROR) << "Client Read";
    LOG(ERROR) << "Read only " << n << " / " << sizeof(connect_info)
               << " bytes";
    goto out;
  }
  if ((info->type) != kGoGoKey) {
    LOG(ERROR) << "Ask to Send failed. Receiver reply with " << (info->type)
               << " But we expect " << kGoGoKey;
    goto out;
  }

  new_host->initialized = true;
  
  close(sockfd);
  free(conn_buf);
  return 0;
out:
  close(sockfd);
  free(conn_buf);
  return -1;
}