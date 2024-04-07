#include "rdma-memorynode.hpp"

/* DEBUGGING USED */
void Test(char *input, int input_len, char *output, int *output_len) {
  char *str = "HELLOWORLD";
  memcpy(output, str, 11);
  *output_len = 11;

  printf("Received: %s\n", input);
  fflush(stdout);
}


int main(int argc, char *argv[]) {

  server_session session;

  if (session.Init(argc, argv)) {
    return 1;
  }

  session.SetCallback(Test);
  
  session.Start();

  pause();

  return 0;
}