#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <thread>
#include <cstring>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "context.hpp"

#define CQ_POLL_DEPTH 10


class server_session {

 private:
  /* Definitions of following functions */
  void data_channel(rdma_io_engine *engine);

 public:
  bool Init(int argc, char **argv);
  void Start();
  void SetCallback(void (*callback)(char *, int, char *, int *)) {
    Callback = callback;
  }

 private:
  /* Definitions of global variables */
  rdma_context *g_context;

  void (*Callback)(char *, int, char *, int *);

};  // class rdma_server_engine