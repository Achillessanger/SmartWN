name = swn-n
objects = smartwn.o rdma-gpunode.o helper.o endpoint.o memory.o engine.o context.o data.pb.o
objects2 = smartwn.o rdma-gpunode.o helper.o endpoint.o memory.o engine.o context.o 
headers = helper.hpp endpoint.hpp memory.hpp engine.hpp context.hpp rdma-gpunode.hpp data.pb.h
CC = g++

CFLAGS = -std=c++17 -Wall -O3 
LDFLAGS = -lpthread -lprotobuf -libverbs -lmlx5 -lglog -lgflags -ltbb

PROTOC = protoc 
PROTOCFLAGS = --cpp_out=. 
PROTOC_FILES = $(wildcard *.protoc)

%.pb : %.proto 
	$(PROTOC) $(PROTOC_FILES) $*.proto 

%.o : %.pb.cc 
	$(CC) -c $(CFLAGS) -c -o $@ $<

$(name) : $(objects)
	g++ -o $(name) $(objects) $(LDFLAGS)
	rm -rf $(objects)

$(objects2) : %.o : %.cpp $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY : clean
clean:
	rm $(name)  $(objects)
