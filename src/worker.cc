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
  { _info.set_id(0); }

  int32_t id() const
  { return _info.id(); }

  bool run()
  {
    try {
      init();

      loop();

    } catch (std::runtime_error const &e) {
      spdlog::error("worker {}: shutting down: {}", id(), e.what());

      return false;
    }

    return true;
  }

private:
  void init()
  {
    auto config = get_config();

    _info.set_id(config.id());

    spdlog::info("worker {}: initialized", id());
  }

  void loop() const
  {
    for (;;) {
      auto command = get_command();

      switch (command.kind()) {
        case WorkerCommand::IDLE:
          break;
        case WorkerCommand::MAP:
          spdlog::info("worker {}: mapping file: {}", id(), command.task_file());
          finish_command(command);
          continue;
        case WorkerCommand::REDUCE:
          spdlog::info("worker {}: reducing", id()); // XXX
          break;
        case WorkerCommand::QUIT:
          spdlog::info("worker {}: done", id());
          return;
        default:
          throw std::runtime_error("unexpected command");
      }

      sleep();
    }
  }

  static void sleep()
  {
    std::this_thread::sleep_for(
      std::chrono::milliseconds(CONFIG_WORKER_SLEEP_FOR));
  }

  WorkerInfo get_config() const
  {
    GetConfigRequest request;

    GetConfigResponse response;
    rpc("get config", &Master::Stub::GetConfig, request, &response);

    return response.worker_info();
  }

  WorkerCommand get_command() const
  {
    GetCommandRequest request;
    *request.mutable_worker_info() = _info;

    GetCommandResponse response;

    rpc("get command", &Master::Stub::GetCommand, request, &response);

    return response.worker_command();
  }

  void finish_command(WorkerCommand const &command) const
  {
    FinishCommandRequest request;
    *request.mutable_worker_info() = _info;
    *request.mutable_worker_command() = command;

    FinishCommandResponse response;

    rpc("finish command", &Master::Stub::FinishCommand, request, &response);
  }

  template<typename FUNC, typename REQUEST, typename RESPONSE>
  void rpc(std::string const &what,
           FUNC &&func,
           REQUEST const &request,
           RESPONSE *response) const
  {
    grpc::ClientContext context;

    auto status = ((*_master).*func)(&context, request, response);

    check_rpc(what, status);
  }

  static void check_rpc(std::string const &what, grpc::Status const &status)
  {
    if (status.ok())
      return;

    auto error_code = status.error_code();
    auto error_message = status.error_message();

    std::stringstream ss;
    ss << "failed to " << what << ": status code " << error_code;

    if (!error_message.empty())
      ss << " (" << error_message << ")";

    throw std::runtime_error(ss.str());
  }

  std::unique_ptr<Master::Stub> _master;

  WorkerInfo _info;
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
