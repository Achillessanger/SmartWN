#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <thread>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "context.hpp"

#define CQ_POLL_DEPTH 10


/*
 * The format of each message sent to the memory node:
 *  +--------------+---------------+
 *  |    Length    |    Payload    |
 *  |     (8B)     |     ....      |
 *  +--------------+---------------+

 * The first 8 bytes are necessary.
 */

class client_engine {
  private:
    struct rdma_io_engine *engine;

  public:
    int Send(void (*callback)(uint64_t, char *, int), uint64_t ctx, char *packet, int length, char *dest);

    client_engine(rdma_io_engine *engine_) : engine(engine_)  {}
};


class client_session {

 private:
  /* Definitions of global variables */
  struct rdma_context *g_context;
  bool g_init_flag = false;

  std::vector<std::string> remote_hosts;


 public:
  // Send operation
  int SetHosts(std::vector< std::string > &vec);
  bool Init(int argc, char **argv);
  void Start();
  client_engine GetEngine(int id);

 private:
  /* Definitions of following functions */
  void data_channel(rdma_io_engine *engine);

};  // class client_session