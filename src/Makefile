THRIFT_PREFIX=/usr/local/thrift

TARGETS = MyPolicyServer TetrischedService_sample_server YARNTetrischedService_sample_client
HPPFILES = tetrisched_constants.h tetrisched_types.h YARNTetrischedService.h TetrischedService.h
OBJS = tetrisched_constants.o tetrisched_types.o YARNTetrischedService.o TetrischedService.o

CXXFLAGS = -I$(THRIFT_PREFIX)/include -g -O2 -std=c++11 -Wall
LDFLAGS = -L$(THRIFT_PREFIX)/lib -lthrift
CXX = g++

default:	$(TARGETS)
all:		$(TARGETS)

MyPolicyServer: $(OBJS) MyPolicyServer.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

TetrischedService_sample_server: $(OBJS) TetrischedService_sample_server.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

YARNTetrischedService_sample_client: $(OBJS) YARNTetrischedService_sample_client.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp $(HPPFILES)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

MyPolicyServer.o: MyPolicyServer.cc MyPolicyServer.h $(HPPFILES)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean:
	-rm -f $(TARGETS) *.o

