#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <spdlog/spdlog.h>

#include "mr.grpc.pb.h"

using namespace std::literals::chrono_literals;

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
      spdlog::error("worker {}: shutting down: {}", _id, e.what());

      return false;
    }

    return true;
  }

private:
  void init()
  {
    auto config = get_config();

    _id = config.id();

    spdlog::info("worker {}: initialized", _id);
  }

  void loop() const
  {
    for (;;) {
      auto task = get_task();

      switch (task.kind()) {
        case WorkerTask::IDLE:
          break;
        case WorkerTask::MAP:
          spdlog::info("worker {}: mapping task: {}", _id, task.file()); // XXX
          break;
        case WorkerTask::REDUCE:
          spdlog::info("worker {}: reducing", _id); // XXX
          break;
        case WorkerTask::QUIT:
          spdlog::info("worker {}: done", _id);
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
  { return rpc<WorkerConfig>("get config", &Master::Stub::GetWorkerConfig); }

  WorkerTask get_task() const
  { return rpc<WorkerTask>("get task", &Master::Stub::GetWorkerTask); }

  template<typename RET, typename FUNC, typename ...ARGS>
  RET rpc(std::string const &what, FUNC &&func, ARGS &&...args) const
  {
    grpc::ClientContext context;

    RET ret;
    auto status = ((*_master).*func)(&context, std::forward<ARGS>(args)..., &ret);

    check_rpc(what, status);

    return ret;
  }

  template<typename RET, typename FUNC>
  RET rpc(std::string const &what, FUNC &&func) const
  {
    Empty empty;
    return rpc<RET>(what, std::forward<FUNC>(func), empty);
  }

  static void check_rpc(std::string const &what, grpc::Status const &status)
  {
    if (!status.ok())
    {
      auto error_code = status.error_code();
      auto error_message = status.error_message();

      std::stringstream ss;
      ss << "failed to " << what << ": status code " << error_code;

      if (!error_message.empty())
        ss << " (" << error_message << ")";

      throw std::runtime_error(ss.str());
    }
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
