#ifndef HOST_HPP
#define HOST_HPP

#include <vector>
#include <string>
#include <mutex>

#include "endpoint.hpp"
#include "helper.hpp"
#include "memory.hpp"



class rdma_host {

 public:
  std::vector<rdma_endpoint *> eps;
  struct rdma_endpoint *qos_ep;
  std::string address;
  bool initialized;

 private:
  int current_ep_index_;
  std::mutex ep_index_mutex_;

  int credit_ = 20;
  std::mutex credit_mutex_;


 public:
  rdma_host(std::string addr_) : address(addr_) {
    current_ep_index_ = 0;
    initialized = false;
  }


  rdma_endpoint *next_ep() {
    ep_index_mutex_.lock();
    auto ret = eps[current_ep_index_];
    if (current_ep_index_ == eps.size()-1) {
      current_ep_index_ = 0;
    }
    else {
      ++current_ep_index_;
    }
    ep_index_mutex_.unlock();

    return ret;
  }

  bool ApplyCredit() {
    credit_mutex_.lock();
    if (credit_ <= 0) {
      credit_mutex_.unlock();
      return false;
    }
    --credit_;
    credit_mutex_.unlock();

    return true;
  }

  void SetCredit(int c) {
    credit_mutex_.lock();
    credit_ = c;
    credit_mutex_.unlock();
  }
};


#endif