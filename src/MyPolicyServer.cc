/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include <fstream>
#include <sstream>
#include <bitset>
#include <deque>

#include "MyPolicyServer.h"

#include "YARNTetrischedService.h"
#include "TetrischedService.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>

#include <rapidjson/document.h>
#include <stdlib.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace ::alsched;

namespace {
struct Job {
  JobID jobId;
  job_t::type jobType;
  size_t k;
};
}

template <int max_possible_machines = 256>
class MyTetrischedServiceHandler: public TetrischedServiceIf {
 private:
  int numMachines_;  // Total number of machines in the cluster
  std::bitset<max_possible_machines> machineMap_;
  std::deque<Job> jobQueue_;
  std::string yarnHost_;
  int yarnPort_;
 public:
  MyTetrischedServiceHandler(int numMachines) {
    numMachines_ = numMachines;
    fprintf(stderr, "== Total num machines: %d\n\n", numMachines_);
    yarnHost_ = "r0";
    yarnPort_ = 9090;
  }

  void Schedule(JobID jobId, const std::set<int32_t>& machines) {
    boost::shared_ptr<TTransport>
        socket(new TSocket(yarnHost_, yarnPort_));
    boost::shared_ptr<TTransport>
        transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol>
        protocol(new TBinaryProtocol(transport));
    YARNTetrischedServiceClient
        client(protocol);
    try {
      // Do it...
      transport->open();
      client.AllocResources(jobId, machines);
      transport->close();
      fprintf(stderr, "JOB %d -> %d machines...\n", int(jobId), int(machines.size()));
      for (auto it = machines.begin(); it != machines.end(); ++it) {
        fprintf(stderr, " > machine %d\n", int(*it));
      }
      // Done!
    } catch (TException& tx) {
      fprintf(stderr, "ERROR calling YARNTetrischedService: %s\n", tx.what());
      fprintf(stderr, "Exit...\n");
      exit(1);
    }
  }

  void TrySchedule() {
    while (jobQueue_.size() != 0) {
      Job job = jobQueue_.front();
      if (job.k <= numMachines_ - machineMap_.count()) {  // Schedule if enough resources
        std::set<int32_t> machines;
        // Always starts with the lowest ranked machines
        for (int i = 0; i < numMachines_; ++i) {
          if (machineMap_[i] == 0) {
            machines.insert(i);
            machineMap_[i] = 1;  // Mark machine as busy
            if (machines.size() == job.k) {
              break;
            }
          }
        }
        Schedule(job.jobId, machines);  // Send instructions to YARN
        jobQueue_.pop_front();
      } else {
        // Not enough resources....
        break;
      }
    }
  }

  virtual void FreeResources(const std::set<int32_t>& machines) override {
    fprintf(stderr, "Free machines -> %d machines...\n", int(machines.size()));
    for (auto it = machines.begin(); it != machines.end(); ++it) {
      fprintf(stderr, " > machine %d\n", int(*it));
      machineMap_[*it] = 0;  // Mark machine as free
    }

    TrySchedule();
  }

  virtual void AddJob(const JobID jobId, const job_t::type jobType,
                      const int32_t k, const int32_t priority,
                      const double duration,
                      const double slowDuration)
                      override {
    Job job;
    job.jobId = jobId;
    job.jobType = jobType;
    job.k = k;

    jobQueue_.push_back(job);

    TrySchedule();
  }
};

static rapidjson::Document* LoadExternalJsonConfig(const char* jsonf) {
  std::ifstream ifs(jsonf);
  std::stringstream buf;
  buf << ifs.rdbuf();
  std::string input = buf.str();
  rapidjson::Document* d = new rapidjson::Document;
  d->Parse(input.c_str());
  return d;
}

static int GetNumMachines(rapidjson::Document* d) {
  int result = 0;
  const rapidjson::Value& rack_cap = (*d)["rack_cap"];
  for (size_t i = 0; i < rack_cap.Size(); ++i) {
    result += rack_cap[i].GetInt();
  }
  return result;
}

int main(int argc, char* argv[]) {
  int numMachines = 22;  // 4 large nodes + 18 small ones by default
  int myPort = 9091;

  // Load json configuration from an external file
  const char* jsonf = getenv("MY_CONFIG");  // exported by run-policy-server.sh
  if (jsonf != NULL) {
    fprintf(stderr, "Loading json config from %s\n", jsonf);
    auto json = LoadExternalJsonConfig(jsonf);
    numMachines = GetNumMachines(json);
    delete json;
  }

  TetrischedServiceIf* const myhandler = new MyTetrischedServiceHandler<>(numMachines);

  boost::shared_ptr<TetrischedServiceIf>
      handler(myhandler);
  boost::shared_ptr<TProcessor>
      processor(new TetrischedServiceProcessor(handler));
  boost::shared_ptr<TServerTransport>
      serverTransport(new TServerSocket(myPort));
  boost::shared_ptr<TTransportFactory>
      transportFactory(new TBufferedTransportFactory());
  boost::shared_ptr<TProtocolFactory>
      protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor,
      serverTransport,
      transportFactory,
      protocolFactory
  );

  server.serve();

  return 0;
}
