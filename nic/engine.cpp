

#include <mutex>
#include <queue>
#include <thread>

#include "engine.hpp"




// Put a task into the engine's task pool.
void rdma_io_engine::PutTask(send_task *task) {
  tasks_.push(task);
}


send_task *rdma_io_engine::GetTask() {
  struct send_task *ret;
  
  if (tasks_.try_pop(ret)) {
    return ret;
  }

  return nullptr;
}


void rdma_io_engine::PutEndpoint(rdma_endpoint *ep) {
  endpoints_.push_back(ep);
}


rdma_buffer *rdma_io_engine::PickNextBuffer(int id) {
  struct rdma_region *region = id ? recv_region : send_region;
  return region->GetBuffer();
}


void rdma_io_engine::ReleaseBuffer(int id, rdma_buffer *buf) {
  struct rdma_region *region = id ? recv_region : send_region;
  region->PutBuffer(buf);
}


int rdma_io_engine::RemainingBufferNum(int id) {
  struct rdma_region *region = id ? recv_region : send_region;
  return region->Size();  
}


int rdma_io_engine::GetEpNum() {
  int ret;
  ret =  endpoints_.size();
  return ret;
}


rdma_endpoint *rdma_io_engine::PickEp(std::string dest) {
  int eps_size = endpoints_.size();
  for (int i=endpoint_index_; i<eps_size; ++i) {
    struct rdma_endpoint *ep = endpoints_[i];
    if (ep->remote_server_ == dest) {
      ++endpoint_index_;
      if (endpoint_index_ == eps_size) {
        endpoint_index_ = 0;
      }
      return ep;
    }
  }
  for (int i=0; i<eps_size; ++i) {
    struct rdma_endpoint *ep = endpoints_[i];
    if (ep->remote_server_ == dest) {
      endpoint_index_ = i+1;
      if (endpoint_index_ == eps_size) {
        endpoint_index_ = 0;
      }
      return ep;
    }
  }

  LOG(ERROR) << "Failed to find the endpoint whose destination is " << dest;
  return nullptr;
}



rdma_io_engine::rdma_io_engine() {
  
}