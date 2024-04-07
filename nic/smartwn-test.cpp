#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <argparse/argparse.hpp>
#include <thread>
#include "data.pb.h"
#include "rdma-gpunode.hpp"
#include <chrono>
#include <tbb/concurrent_vector.h>
#include <array>
#include <algorithm>
#include <limits>
#include <ctime>

struct Request;

struct Subreq
{
    const char* dest;
    int send;
    int receive;
    int lookup;
    struct timespec* start_timastamp;
    struct timespec* end_timestamp;
    Request* father;
};

struct Request
{
    int request_num;
    tbb::concurrent_vector<Subreq*>* subreqs;
    std::atomic<int> curCallbackTrigger;
};

using json = nlohmann::json;
// rdma_engine engine;
client_session session;
std::string original = "abcdefghijklmnopqrstuvwxyz";
tbb::concurrent_vector<Request*>* trace_data;


int expectedCallbackCount = 0;
std::atomic<int> callbackCount(0);

std::mutex mtx;
std::condition_variable cv;



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


void ReveiveFromMem(uint64_t ctx, char *arg, int length){

    struct timespec* tn = new timespec;
    clock_gettime(CLOCK_MONOTONIC, tn);


    // auto now = std::chrono::high_resolution_clock::now();
    // long nanoseconds = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();\
    

    Subreq* subreq = reinterpret_cast<Subreq*>(ctx);
    // subreq->end_timestamp = nanoseconds;
    subreq->end_timestamp = tn;//(long)tn.tv_nsec;

    Request* father_req = subreq->father;
    father_req->curCallbackTrigger.fetch_add(1,std::memory_order_relaxed);

    // std::cout << "[[ReveiveFromMem]]requestId:" << father_req->request_num;
    // std::cout << "start_time:" << subreq->start_timastamp->tv_nsec;
    // std::cout << ",send:" << subreq->send;
    // std::cout << ",lookup:" << subreq->lookup;
    // std::cout << ",end_timestamp:" << subreq->end_timestamp->tv_nsec;
    // std::cout << ",interval:" << (subreq->end_timestamp->tv_sec - subreq->start_timastamp->tv_sec) * 1000000000llu +  (subreq->end_timestamp->tv_nsec-subreq->start_timastamp->tv_nsec);
    // std::cout << ",receive:" << length;
    // std::cout << ",callbackCount:" << father_req->curCallbackTrigger.load(std::memory_order_relaxed);
    // std::cout << ",expectedCallbackCount:" << father_req->subreqs->size();
    // std::cout<< std::endl;

    
}


void thread_task(int thread_num, int part_size){
    auto engine = session.GetEngine(thread_num); 
    int left = part_size * thread_num;
    int right = part_size * (thread_num+1);
    if (right > trace_data->size()){
        right = trace_data->size();
    }
    // std::cout << "[thread_num]:" << thread_num ;
    // std::cout << "left:" << left ;
    // std::cout << ",right:" << right << std::endl;

    for(int reqInd = left; reqInd < right; reqInd++){
        Request* request = trace_data->at(reqInd);
        tbb::concurrent_vector<Subreq*>* subreqs = request->subreqs;
        // std::cout << "IOThread " << thread_num << "," << request->request_num << std::endl;
        for (int subInd = 0; subInd < subreqs->size(); subInd++){
            Subreq* subreq = subreqs->at(subInd);
            // std::cout << "IOThread " << thread_num << ",send" << subreq->send << std::endl;
            Data* d = new Data;
            d->set_rsize(subreq->receive);
            d->set_inverval(subreq->lookup);
            std::string random_string = original.substr(0, subreq->send);
            d->set_content(random_string);
            int max_length = d->ByteSizeLong(); 
            char* buffer = new char[max_length]; 
            d->SerializeToArray(buffer, max_length);

            // auto now = std::chrono::high_resolution_clock::now();
            // long nanoseconds = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();
            // subreq->start_timastamp = nanoseconds;

            uint64_t ptrValue = reinterpret_cast<uint64_t>(subreq);
            // uint64_t ptrValue = static_cast<uint64_t>(reqInd*10+subInd);

            char* dest_ptr = const_cast<char*>(subreq->dest);

            struct timespec* tn = new timespec;
            clock_gettime(CLOCK_MONOTONIC, tn);
            subreq->start_timastamp = tn;
            
            engine.Send(ReveiveFromMem, ptrValue, buffer, max_length, dest_ptr);
            // engine.Receive();
            // while (!engine.Receive()){
            //     ;
            // }
            // std::cout << "[[SEND]]IOThread:" << thread_num;
            // std::cout << ",ReqNum:" << reqInd;
            // std::cout << ",SubReqNum:" << subInd;
            // std::cout << ",send:" << subreq->send;
            // std::cout << ",max_length:" << max_length;
            // std::cout << ",reveive:" << subreq->receive;
            // std::cout << ",sleep:" << subreq->lookup;
            // std::cout << ",start_timastamp:" << subreq->start_timastamp->tv_nsec;
            // std::cout << std::endl;

        }
        
        // std::cout << ">>> this req fin:" << reqInd << std::endl;
        
        while(request->curCallbackTrigger.load(std::memory_order_relaxed)<subreqs->size()){
            // engine.Receive();
            // std::cout << "[[LOOP]]IOThread:" << thread_num;
            // std::cout << "curCallbackTrigger:" << request->curCallbackTrigger.load(std::memory_order_relaxed);
            // std::cout << "except:" << subreqs->size();
            // std::cout << ",ReqNum:" << reqInd;
            // std::cout << std::endl;

        }
    }

}

int main(int argc, char* argv[]) {
    // auto now = std::chrono::system_clock::now();
    // auto duration = now.time_since_epoch();
    // long milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    // std::cout << "当前毫秒级别时间戳: " << milliseconds << std::endl;
    auto now = std::chrono::high_resolution_clock::now();
    auto nanoseconds = std::chrono::time_point_cast<std::chrono::nanoseconds>(now).time_since_epoch().count();\
    std::cout << "当前纳秒级别时间戳: " << nanoseconds << std::endl;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string filename = FLAGS_trace_file;
    std::string nodes = FLAGS_mem_nodes;
    int thread_count = FLAGS_io_thread;
    std::cout << "filename: " << filename << std::endl;
    std::cout << "nodes: " << nodes << std::endl;
    std::cout << "count: " << thread_count << std::endl;

    std::vector<std::string> hosts;
    std::istringstream iss(nodes);
    std::string node;
    while (std::getline(iss, node, ',')) {
        hosts.push_back(node);
    }

    // //测试hosts
    // for (int i = 0; i < hosts.size(); i++) {
    //     std::cout << hosts[i] << " ";
    // }
    // std::cout << std::endl;

    
    session.SetHosts(hosts);
    if (session.Init(argc, argv)) {
        std::cout << "session init fail" << std::endl;
        return 1;
    }
    std::cout << "session init success" << std::endl;
    session.Start();

    trace_data = new tbb::concurrent_vector<Request*>;
    std::ifstream ifs(filename);
    std::string line;
    int line_count = 0;
    int original_length = original.size();
    while (std::getline(ifs, line)) {
        json j = json::parse(line);
        int request_num = j["request_num"];
        // std::cout << "request_num:" << request_num <<  std::endl;

        Request *request = new Request;
        request->request_num = request_num;

        tbb::concurrent_vector<Subreq*> * subreqs = new tbb::concurrent_vector<Subreq*>;
        for (const auto& sub : j["subreq"]) {
            std::string dest_str = sub["dest"].get<std::string>();
            const char* dest_from_json = dest_str.c_str();
            int send_size = sub["send"].get<int>();
            if (send_size > original_length){
                original_length = send_size;
            }   
            Subreq* sr = new Subreq;
            sr->dest = dest_from_json;
            sr->send = send_size;
            sr->receive = sub["receive"].get<int>();
            sr->lookup = sub["lookup"].get<int>();
            sr->start_timastamp = 0;
            sr->end_timestamp = 0;
            sr->father = request;
            subreqs->push_back(sr);
            expectedCallbackCount++;       
        }
        
        request->subreqs = subreqs;
        request->curCallbackTrigger.store(0);


        // std::cout << "request->request_num:" << request->request_num << std::endl;
        trace_data->push_back(request);
        line_count++;
    }

    std::cout << "size():" << trace_data->size();
    std::cout << ",randomStringLength:" << original_length;
    std::cout << std::endl;
    original = generateRandomString(original_length);

    
    int partSize = trace_data->size() / thread_count;

    if(partSize == 0){
        thread_count = 1;
        partSize = trace_data->size();
    }else if (trace_data->size() % thread_count != 0){
        partSize++;
    }


    const int num_threads = thread_count;
    std::thread threads[num_threads];

    struct timespec* startTime = new timespec;
    clock_gettime(CLOCK_MONOTONIC, startTime);
    
    for (int i = 0; i < thread_count; i++){
        // std::cout << "start std::thread(thread_task,i,partSize);  " << std::endl;
        threads[i] = std::thread(thread_task,i,partSize);  
    }

    // std::cout << "start join()" << std::endl;
    for (int i = 0; i < thread_count; i++){
        threads[i].join();
    }
    struct timespec* endTime = new timespec;
    clock_gettime(CLOCK_MONOTONIC, endTime);

    std::cout << "所有线程结束" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::ofstream out_file(FLAGS_output_file);

    // pause();

    // std::unique_lock<std::mutex> lock(mtx);
    // cv.wait(lock, [] { return callbackCount.load(std::memory_order_relaxed) == expectedCallbackCount; });
    // std::cout << "所有回调函数都已触发！" << std::endl;

    const int total_subreq = expectedCallbackCount;
    double interval_arr[total_subreq] = {0};

    const int total_req = trace_data->size();
    double req_interval_arr[total_req] = {0};
    double req_p90interval_arr[total_req] = {0};
    double req_p95interval_arr[total_req] = {0};
    double req_p85interval_arr[total_req] = {0};
    double req_p80interval_arr[total_req] = {0};
    double req_p70interval_arr[total_req] = {0};

    int interval_cnt = 0;
    for(int reqId = 0; reqId < trace_data->size(); reqId++){
        std::cout << "-----" << reqId << "-----" << std::endl;
        Request* R = trace_data->at(reqId);
        tbb::concurrent_vector<Subreq*>* SRs = R->subreqs;

        double min_start_time = std::numeric_limits<double>::max();
        double max_end_time = std::numeric_limits<double>::min();

        const int sub_total_num = SRs->size();
        double all_end_time[sub_total_num] = {0};

        for(int subId = 0; subId < SRs->size(); subId++){
            Subreq* SR = SRs->at(subId); 
            uint64_t _s = SR->start_timastamp->tv_sec * 1000000000llu +  SR->start_timastamp->tv_nsec;
            uint64_t _e = SR->end_timestamp->tv_sec * 1000000000llu +  SR->end_timestamp->tv_nsec;
            double inter = (double)(_e-_s);
            // long inter = long(SR->end_timestamp->tv_nsec - SR->start_timastamp->tv_nsec);
            // double inter = (SR->end_timestamp->tv_sec - SR->start_timastamp->tv_sec)+ (SR->end_timestamp->tv_nsec - SR->start_timastamp->tv_nsec)*pow(10,-9);
            if (double(_s) < min_start_time){
                // std::cout << ">" << SR->start_timastamp << std::endl;
                min_start_time = double(_s);
                // std::cout << ">>" << min_start_time << std::endl;
            }
            if (double(_e)> max_end_time){
                max_end_time = double(_e);
            }

            all_end_time[subId] = double(_e);
            
            interval_arr[interval_cnt] = inter;
            interval_cnt++;
            std::cout << "[";
            std::cout << _s << ",";
            std::cout << _e << ",";
            std::cout << std::fixed  << inter << "]";
            std::cout << std::endl;
            out_file << "reqId:" << reqId << ",";
            out_file << "subReqId:" << subId << ",";
            out_file << _s << ",";
            out_file << _e << "\n";

        }
        req_interval_arr[reqId] = max_end_time - min_start_time;

        std::sort(all_end_time, all_end_time + sub_total_num);
        int p95subInd = (sub_total_num * 95) / 100;
        if (p95subInd >= 0 && p95subInd < sub_total_num){
        }else{
            p95subInd = sub_total_num-1;
            std::cout << "Invalid p95 subreq return time (default as last return time)." << std::endl;
        }
        int p90subInd = (sub_total_num * 90) / 100;
        if (p90subInd >= 0 && p90subInd < sub_total_num){
        }else{
            p90subInd = sub_total_num-1;
            std::cout << "Invalid p90 subreq return time (default as last return time)." << std::endl;
        }
        int p85subInd = (sub_total_num * 85) / 100;
        if (p85subInd >= 0 && p85subInd < sub_total_num){
        }else{
            p85subInd = sub_total_num-1;
            std::cout << "Invalid p85 subreq return time (default as last return time)." << std::endl;
        }
        int p80subInd = (sub_total_num * 80) / 100;
        if (p80subInd >= 0 && p80subInd < sub_total_num){
        }else{
            p80subInd = sub_total_num-1;
            std::cout << "Invalid p80 subreq return time (default as last return time)." << std::endl;
        }
        int p70subInd = (sub_total_num * 70) / 100;
        if (p70subInd >= 0 && p70subInd < sub_total_num){
        }else{
            p70subInd = sub_total_num-1;
            std::cout << "Invalid p70 subreq return time (default as last return time)." << std::endl;
        }
        req_p95interval_arr[reqId] = all_end_time[p95subInd] - min_start_time;
        req_p90interval_arr[reqId] = all_end_time[p90subInd] - min_start_time;
        req_p85interval_arr[reqId] = all_end_time[p85subInd] - min_start_time;
        req_p80interval_arr[reqId] = all_end_time[p80subInd] - min_start_time;
        req_p70interval_arr[reqId] = all_end_time[p70subInd] - min_start_time;


        // std::cout << "??:" << all_end_time[p95subInd] << ":" << all_end_time[p90subInd] << std::endl;
        // std::cout << req_interval_arr[reqId] << "," << max_end_time << "," << min_start_time<<"/";
       
    }
    // std::cout << std::endl;

    //计算子请求测量值
    std::sort(interval_arr, interval_arr + total_subreq);


    

    int p90Index = (total_subreq * 90) / 100;
    if (p90Index >= 0 && p90Index < total_subreq) {
        std::cout << "[subreqs] p90 latency(ns): " << interval_arr[p90Index] << std::endl;
    } else {
        std::cout << "Invalid p90 latency." << std::endl;
    }

    int p95Index = (total_subreq * 95) / 100;
    if (p95Index >= 0 && p95Index < total_subreq) {
        std::cout << "[subreqs] p95 latency(ns): " << interval_arr[p95Index] << std::endl;
    } else {
        std::cout << "Invalid p95 latency." << std::endl;
    }

    int p99Index = (total_subreq * 99) / 100;
    if (p99Index >= 0 && p99Index < total_subreq) {
        std::cout << "[subreqs] p99 latency(ns): " << interval_arr[p99Index] << std::endl;
    } else {
        std::cout << "Invalid p99 latency." << std::endl;
    }

    out_file << "[subreqs] p90:" << interval_arr[p90Index] << "\n";
    out_file << "[subreqs] p95:" << interval_arr[p95Index] << "\n";
    out_file << "[subreqs] p99:" << interval_arr[p99Index] << "\n";

    


    double sum = 0;
    for (int i = 0; i < total_subreq; ++i) {
        sum += interval_arr[i]/total_subreq;
    }

    std::cout << std::fixed  << "[subreqs] average: " << sum << std::endl;
    out_file << "[subreqs] average:" << sum << "\n";


    double median;
    if (total_subreq % 2 == 0) {
        median = static_cast<double>(interval_arr[total_subreq / 2 - 1] + interval_arr[total_subreq / 2]) / 2;
    } else {
        median = interval_arr[total_subreq / 2];
    }

    std::cout << std::fixed  << "[subreqs] median: " << median << std::endl;
    out_file << "[subreqs] median:" << median << "\n";
    
    //计算请求的测量值
    std::sort(req_interval_arr, req_interval_arr + total_req);
    std::sort(req_p95interval_arr, req_p95interval_arr + total_req);
    std::sort(req_p90interval_arr, req_p90interval_arr + total_req);
    std::sort(req_p85interval_arr, req_p85interval_arr + total_req);
    std::sort(req_p80interval_arr, req_p80interval_arr + total_req);
    std::sort(req_p70interval_arr, req_p70interval_arr + total_req);


    p99Index = (total_req * 99) / 100;
    if (p99Index >= 0 && p99Index < total_req) {
        std::cout << "[reqs] p99 latency(ns): " << req_interval_arr[p99Index] << std::endl;
        std::cout << "[reqs] p99(p95 of subreqs):" << req_p95interval_arr[p99Index] << std::endl;
        std::cout << "[reqs] p99(p90 of subreqs):" << req_p90interval_arr[p99Index] << std::endl;
        std::cout << "[reqs] p99(p85 of subreqs): " << req_p85interval_arr[p99Index] << std::endl;
        std::cout << "[reqs] p99(p80 of subreqs):" << req_p80interval_arr[p99Index] << std::endl;
        std::cout << "[reqs] p99(p70 of subreqs): " << req_p70interval_arr[p99Index] << std::endl;
    } else {
        std::cout << "Invalid p99 latency." << std::endl;
    }
    // int p90Index = (total_req * 90) / 100;
    // if (p90Index >= 0 && p90Index < total_req) {
    //     std::cout << "[Reqs] p90 latency(ns): " << req_interval_arr[p90Index] << std::endl;
    // } else {
    //     std::cout << "Invalid p90 latency." << std::endl;
    // }

    // int p95Index = (total_req * 95) / 100;
    // if (p95Index >= 0 && p95Index < total_req) {
    //     std::cout << "[Reqs] p95 latency(ns): " << req_interval_arr[p95Index] << std::endl;
    // } else {
    //     std::cout << "Invalid p95 latency." << std::endl;
    // }

    out_file << "[reqs] p99(full subreqs):" << req_interval_arr[p99Index] << "\n";
    out_file << "[reqs] p99(p95 of subreqs):" << req_p95interval_arr[p99Index] << "\n";
    out_file << "[reqs] p99(p90 of subreqs):" << req_p90interval_arr[p99Index] << "\n";
    out_file << "[reqs] p99(p85 of subreqs):" << req_p85interval_arr[p99Index] << "\n";
    out_file << "[reqs] p99(p80 of subreqs):" << req_p80interval_arr[p99Index] << "\n";
    out_file << "[reqs] p99(p70 of subreqs):" << req_p70interval_arr[p99Index] << "\n";




    sum = 0;
    double sump95 = 0;
    double sump90 = 0;
    double sump85 = 0;
    double sump80 = 0;
    double sump70 = 0;
    for (int i = 0; i < total_req; ++i) {
        sum += req_interval_arr[i]/total_req;
        sump95 += req_p95interval_arr[i]/total_req;
        sump90 += req_p90interval_arr[i]/total_req;
        sump85 += req_p85interval_arr[i]/total_req;
        sump80 += req_p80interval_arr[i]/total_req;
        sump70 += req_p70interval_arr[i]/total_req;
    }

    std::cout << std::fixed  << "[Reqs] average: " << sum << std::endl;
    std::cout << std::fixed  << "[Reqs] average(p95 of subreqs): " << sump95 << std::endl;
    std::cout << std::fixed  << "[Reqs] average(p90 of subreqs): " << sump90 << std::endl;
    std::cout << std::fixed  << "[Reqs] average(p85 of subreqs): " << sump85 << std::endl;
    std::cout << std::fixed  << "[Reqs] average(p80 of subreqs): " << sump80 << std::endl;
    std::cout << std::fixed  << "[Reqs] average(p70 of subreqs): " << sump70 << std::endl;

    out_file << "[reqs] average(full subreqs):" << sum << "\n";
    out_file << "[reqs] average(p95 of subreqs):" << sump95 << "\n";
    out_file << "[reqs] average(p90 of subreqs):" << sump90 << "\n";
    out_file << "[reqs] average(p85 of subreqs):" << sump85 << "\n";
    out_file << "[reqs] average(p80 of subreqs):" << sump80 << "\n";
    out_file << "[reqs] average(p70 of subreqs):" << sump70 << "\n";

    double medianp95;
    double medianp90;
    double medianp85;
    double medianp80;
    double medianp70;
    if (total_req % 2 == 0) {
        median = static_cast<double>(req_interval_arr[total_req / 2 - 1] + req_interval_arr[total_req / 2]) / 2;
        medianp95 = static_cast<double>(req_p95interval_arr[total_req / 2 - 1] + req_p95interval_arr[total_req / 2]) / 2;
        medianp90 = static_cast<double>(req_p90interval_arr[total_req / 2 - 1] + req_p90interval_arr[total_req / 2]) / 2;
        medianp85 = static_cast<double>(req_p85interval_arr[total_req / 2 - 1] + req_p85interval_arr[total_req / 2]) / 2;
        medianp80 = static_cast<double>(req_p80interval_arr[total_req / 2 - 1] + req_p80interval_arr[total_req / 2]) / 2;
        medianp70 = static_cast<double>(req_p70interval_arr[total_req / 2 - 1] + req_p70interval_arr[total_req / 2]) / 2;
    } else {
        median = req_interval_arr[total_req / 2];
        medianp95 = req_p95interval_arr[total_req / 2];
        medianp90 = req_p90interval_arr[total_req / 2];
        medianp85 = req_p85interval_arr[total_req / 2];
        medianp80 = req_p80interval_arr[total_req / 2];
        medianp70 = req_p70interval_arr[total_req / 2];
    }

    std::cout << std::fixed  << "[Reqs] median: " << median << std::endl;
    std::cout << std::fixed  << "[Reqs] median(p95 of subreqs): " << medianp95 << std::endl;
    std::cout << std::fixed  << "[Reqs] median(p90 of subreqs): " << medianp90 << std::endl;
    std::cout << std::fixed  << "[Reqs] median(p85 of subreqs): " << medianp85 << std::endl;
    std::cout << std::fixed  << "[Reqs] median(p80 of subreqs): " << medianp80 << std::endl;
    std::cout << std::fixed  << "[Reqs] median(p70 of subreqs): " << medianp70 << std::endl;

    out_file << "[reqs] average(full subreqs):" << median << "\n";
    out_file << "[reqs] average(p95 of subreqs):" << medianp95 << "\n";
    out_file << "[reqs] average(p90 of subreqs):" << medianp90 << "\n";
    out_file << "[reqs] average(p85 of subreqs):" << medianp85 << "\n";
    out_file << "[reqs] average(p80 of subreqs):" << medianp80 << "\n";
    out_file << "[reqs] average(p70 of subreqs):" << medianp70 << "\n";
    
    
    uint64_t _start = startTime->tv_sec * 1000000000llu +  startTime->tv_nsec;
    uint64_t _end = endTime->tv_sec * 1000000000llu +  endTime->tv_nsec;
    double program_run_time = (double)(_end-_start);
    std::cout << std::fixed  << "program run time(ns): " << program_run_time;
    std::cout << std::endl; 
    out_file << "program run time(ns):" << program_run_time << "\n";
    return 0;
}