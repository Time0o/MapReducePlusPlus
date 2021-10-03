#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_builder.h>
#include <spdlog/spdlog.h>

#include "mr.grpc.pb.h"

namespace mr
{

class MasterImpl : public Master::Service
{
  struct Task
  {
    enum Status {
      TODO,
      DOING,
      DONE,
    };

    Task(int32_t id, std::string const &file)
    : id(id),
      file(file),
      worker(0),
      status(TODO)
    {}

    int32_t id;
    std::string file;

    int32_t worker;
    Status status;
  };

public:
  MasterImpl(std::vector<std::string> const &task_files)
  {
    for (auto const &task_file : task_files) {
      int32_t task_id = _tasks.size() + 1;

      _tasks.emplace_back(task_id, task_file);
    }
  }

  grpc::Status GetConfig(grpc::ServerContext *context,
                         GetConfigRequest const *request,
                         GetConfigResponse *response)
  {
    (void)request;

    auto worker_id = ++_workers;

    spdlog::info("master: configuring worker {}", worker_id);

    response->mutable_worker_info()->set_id(worker_id);

    return grpc::Status::OK;
  }

  grpc::Status GetCommand(grpc::ServerContext *context,
                          GetCommandRequest const *request,
                          GetCommandResponse *response)
  {
    auto const &worker_info = request->worker_info();

    auto worker_id = worker_info.id();

    auto worker_command = next_worker_command(worker_id);

    spdlog::info("master: sending command {} to worker {}",
                 describe_worker_command(worker_command), worker_id);

    *response->mutable_worker_command() = worker_command;

    return grpc::Status::OK;
  }

  grpc::Status FinishCommand(grpc::ServerContext *context,
                             FinishCommandRequest const *request,
                             FinishCommandResponse *response)
  {
    auto const &worker_info = request->worker_info();
    auto const &worker_command = request->worker_command();

    auto worker_id = worker_info.id();
    auto task_id = worker_command.task_id();

    spdlog::info("master: task {} finished by worker {}", task_id, worker_id);

    auto &task = _tasks[task_id - 1];

    task.worker = 0;
    task.status = Task::DONE;

    return grpc::Status::OK;
  }

private:
  WorkerCommand next_worker_command(int32_t worker_id)
  {
    WorkerCommand worker_command;

    bool done = true;

    for (auto &task : _tasks) {
      if (task.status == Task::TODO) {
        task.worker = worker_id;
        task.status = Task::DOING;

        worker_command.set_kind(WorkerCommand::MAP);
        worker_command.set_task_id(task.id);
        worker_command.set_task_file(task.file);

        done = false;

        break;
      }
    }

    if (done)
      worker_command.set_kind(WorkerCommand::QUIT);

    return worker_command;
  }

  std::string describe_worker_command(WorkerCommand const &worker_command)
  {
    std::stringstream ss;

    switch (worker_command.kind()) {
      case WorkerCommand::IDLE:
        ss << "IDLE";
        break;
      case WorkerCommand::MAP:
        ss << "MAP [task " << worker_command.task_id() << ", '"
                           << worker_command.task_file() << "']";
        break;
      case WorkerCommand::REDUCE:
        ss << "REDUCE"; // XXX
        break;
      case WorkerCommand::QUIT:
        ss << "QUIT";
        break;
      default:
        ss << "???";
        break;
    }

    return ss.str();
  }

  int32_t _workers = 0;
  std::vector<Task> _tasks;
};

} // end namespace hello

int main(int argc, char **argv)
{
  using namespace mr;

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " INPUT_FILES" << std::endl;

    return EXIT_FAILURE;
  }

  std::vector<std::string> files(argv + 1, argv + argc);

  MasterImpl service(files);

  grpc::ServerBuilder builder;

  builder.AddListeningPort(CONFIG_MASTER_SOCKET,
                           grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();

  return EXIT_SUCCESS;
}
