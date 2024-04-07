#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <thread>
#include "data.pb.h"
#include <chrono>
#include "rdma-memorynode.hpp"



server_session session;
std::string original = "abcdefghijklmnopqrstuvwxyz";

std::string generateRandomString(int n) {
    std::string randomString;
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    srand(static_cast<unsigned int>(time(nullptr))); // 初始化随机数生成器

    for (int i = 0; i < n; ++i) {
        int randomIndex = rand() % (sizeof(charset) - 1);
        randomString += charset[randomIndex];
    }

    return randomString;
}

void ReturnBytes(char *input, int input_len, char *output, int *output_len) {
    Data message;
    message.ParseFromArray(input, input_len);
    std::string content = message.content();
  
    std::this_thread::sleep_for(std::chrono::nanoseconds(message.inverval()));
    std::string s = generateRandomString(message.rsize());
    const char* random_string = s.c_str();
    *output_len = message.rsize();
    memcpy(output, random_string, message.rsize());
    memcpy(output,&original,message.rsize());
}

int main(int argc, char* argv[]) {
  int ret = 0;
  int current_tid;
  original = generateRandomString(100000);
  
  if (session.Init(argc, argv)) {
    std::cout << "Init Failed." << std::endl;
    return 1;
  }

  session.SetCallback(ReturnBytes);
  
  session.Start();
  
  pause();

}