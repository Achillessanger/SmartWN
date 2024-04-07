#ifndef ENGINE_HPP
#define ENGINE_HPP
#include "endpoint.hpp"
#include "memory.hpp"


struct send_task {
  int length;
  void (*callback)(uint64_t, char *, int);
  uint64_t context;
  char *source;
  char *dest;

  send_task(void (*cb_)(uint64_t, char *, int), uint64_t ctx_, int length_, char *source_, char *dest_) 
      : length(length_), source(source_), dest(dest_), callback(cb_), context(ctx_) {}
};

/*
union legoemb_cq {
  struct ibv_cq *cq;
  struct ibv_cq_ex *cq_ex;
};
*/

class rdma_io_engine {
 public:
  void PutTask(send_task *task);
  send_task *GetTask();
  void PutEndpoint(rdma_endpoint *ep);
  rdma_buffer *PickNextBuffer(int id);
  void ReleaseBuffer(int id, rdma_buffer *buf);
  int RemainingBufferNum(int id);
  int GetEpNum();
  rdma_endpoint *PickEp(std::string dest);

  // Transportation
  std::vector<ibv_cq *> cqs;

  // Memory
  int pd_num;
  struct rdma_region *send_region;
  struct rdma_region *recv_region;
  int send_buffer_size;
  int recv_buffer_size;

  // Credit
  int credit;

  // Global information
  void *context;

  rdma_io_engine();

 private:
  // Task pools relevant, eradicate the competitions.
  // std::mutex tasks_mutex_;
  // std::queue<send_task *> tasks_;
  tbb::concurrent_queue<send_task *> tasks_;
  // semaphore tasks_semaphore_;

  // Endpoint pools relevant.
  std::vector<rdma_endpoint *> endpoints_;
  int endpoint_index_ = 0;

};


#endif