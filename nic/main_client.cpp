#include "rdma-gpunode.hpp"


void Test(uint64_t ctx, char *arg, int length) {
  printf("Context: %ld, Received: %s\n", ctx, arg);
  fflush(stdout);
}


int main(int argc, char *argv[]) {

  int ret = 0;
  int current_engine_id = 0;
  client_session session;

  std::vector<std::string> hosts;
  hosts.push_back("192.168.25.25");
  session.SetHosts(hosts);

  if (session.Init(argc, argv)) {
    return 1;
  }

  session.Start();

  /* The main thread: Read data and put send tasks into g_tasks */
  char *payload = "SOMETHING";

  for (int i=0; i<40; ++i) {
    auto engine = session.GetEngine(current_engine_id++); // Balanced
    if (current_engine_id == FLAGS_ioengine_num) {
      current_engine_id = 0;
    }
    engine.Send(Test, i, payload, 10, "192.168.25.25");
  }

  pause();

  /* Now all requests have been consumed, we can exit now! */
  return 0;
}