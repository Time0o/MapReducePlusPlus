#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "mr.grpc.pb.h"

using namespace std::literals::chrono_literals;

namespace
{

void CheckRPC(grpc::Status const &status, std::string const &msg)
{
  if (!status.ok())
  {
    auto error_code = status.error_code();
    auto error_message = status.error_message();

    std::stringstream ss;
    ss << msg << ": status code " << error_code;

    if (!error_message.empty())
      ss << " (" << error_message << ")";

    throw std::runtime_error(ss.str());
  }
}

} // end namespace

namespace mr
{

class Worker
{
public:
  Worker(std::shared_ptr<grpc::ChannelInterface> channel)
  : _master(Master::NewStub(channel))
  {}

  bool run()
  {
    try {
      init();

      loop();

    } catch (std::runtime_error const &e) {
      std::cerr << "worker " << _id << ": shutting down: " << e.what() << std::endl;

      return false;
    }

    return true;
  }

private:
  void init()
  {
    auto config = get_config();

    _id = config.id();

    std::cout << "worker " << _id << ": initialized" << std::endl;
  }

  void loop() const
  {
    for (;;) {
      auto task = get_task();

      switch (task.kind()) {
        case WorkerTask::IDLE:
          break;
        case WorkerTask::MAP:
          std::cout << "worker " << _id << ": mapping task '" << task.file() << "'" << std::endl; // XXX
          break;
        case WorkerTask::REDUCE:
          std::cout << "worker " << _id << ": reducing" << std::endl; // XXX
          break;
        case WorkerTask::QUIT:
          std::cout << "worker " << _id << ": done" << std::endl;
          return;
        default:
          throw std::runtime_error("unexpected task kind");
      }

      sleep();
    }
  }

  static void sleep()
  {
    std::this_thread::sleep_for(
      std::chrono::milliseconds(CONFIG_WORKER_SLEEP_FOR));
  }

  WorkerConfig get_config() const
  {
    grpc::ClientContext context;

    Empty empty;
    WorkerConfig config;
    CheckRPC(_master->GetWorkerConfig(&context, empty, &config),
             "failed to get config");

    return config;
  }

  WorkerTask get_task() const
  {
    grpc::ClientContext context;

    Empty empty;
    WorkerTask task;
    CheckRPC(_master->GetWorkerTask(&context, empty, &task),
             "failed to get task");

    return task;
  }

  std::unique_ptr<Master::Stub> _master;

  uint32_t _id = 0u;
};

} // end namespace mr

int main(int argc, char **argv)
{
  using namespace mr;

  auto channel = grpc::CreateChannel(CONFIG_MASTER_SOCKET,
                                     grpc::InsecureChannelCredentials());

  Worker worker(channel);

  return worker.run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
