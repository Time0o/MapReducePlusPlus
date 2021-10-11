#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_builder.h>
#include <spdlog/spdlog.h>

#include "mr.grpc.pb.h"

using namespace std::literals::chrono_literals;

namespace mr
{

std::unique_ptr<grpc::Server> master_server;

class MasterImpl : public Master::Service
{
  struct Task
  {
    enum Status {
      TODO,
      DOING,
      DONE,
    };

    Task(int32_t id, std::optional<std::string> const &file = std::nullopt)
    : id(id),
      file(file),
      worker(0),
      status(TODO)
    {}

    int32_t id;
    std::optional<std::string> file;

    int32_t worker;
    Status status;
  };

public:
  MasterImpl(std::vector<std::string> const &input_files,
             int32_t map_num_tasks,
             int32_t reduce_num_tasks)
  {
    spdlog::info("master: starting");

    create_tasks(input_files, map_num_tasks, reduce_num_tasks);
  }

  grpc::Status GetConfig(grpc::ServerContext *context,
                         GetConfigRequest const *,
                         GetConfigResponse *response)
  {
    auto worker_id = ++_workers;

    auto worker_info = get_worker_info(worker_id);

    spdlog::info("master: configuring worker {}", worker_id);

    *response->mutable_worker_info() = worker_info;

    return grpc::Status::OK;
  }

  grpc::Status GetCommand(grpc::ServerContext *context,
                          GetCommandRequest const *request,
                          GetCommandResponse *response)
  {
    auto const &worker_info = request->worker_info();

    auto worker_id = worker_info.id();

    auto worker_command = get_worker_command(worker_id);

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

    bool done = finish_worker_command(worker_command);

    if (done) {
      spdlog::info("master: all tasks completed", task_id, worker_id);

      remove_worker();

    } else {
      spdlog::debug("master: {}/{} map/reduce tasks remaining",
                    _map_num_tasks_remaining, _reduce_num_tasks_remaining);
    }

    response->set_done(done);

    return grpc::Status::OK;
  }

private:
  void create_tasks(std::vector<std::string> const &input_files,
                    int32_t map_num_tasks,
                    int32_t reduce_num_tasks)
  {
    // create map tasks
    assert(input_files.size() == static_cast<std::size_t>(map_num_tasks)); // XXX

    _map_tasks.reserve(map_num_tasks);

    for (auto const &task_file : input_files)
      _map_tasks.emplace_back(_map_tasks.size() + 1, task_file);

    _map_num_tasks = map_num_tasks;
    _map_num_tasks_remaining = map_num_tasks;

    // create reduce tasks
    _reduce_tasks.reserve(reduce_num_tasks);

    for (int32_t reduce_task = 0; reduce_task < reduce_num_tasks; ++reduce_task)
      _reduce_tasks.emplace_back(_reduce_tasks.size() + 1);

    _reduce_num_tasks = reduce_num_tasks;
    _reduce_num_tasks_remaining = reduce_num_tasks;
  }

  WorkerInfo get_worker_info(int32_t worker_id) const
  {
    WorkerInfo worker_info;

    worker_info.set_id(worker_id);
    worker_info.set_map_num_tasks(_map_num_tasks);
    worker_info.set_reduce_num_tasks(_reduce_num_tasks);

    return worker_info;
  }

  WorkerCommand get_worker_command(int32_t worker_id)
  {
    WorkerCommand worker_command;

    if (_map_num_tasks_remaining > 0) {
      worker_command = get_worker_command(WorkerCommand::MAP,
                                          _map_tasks,
                                          worker_id);

    } else if (_reduce_num_tasks_remaining > 0) {
      worker_command = get_worker_command(WorkerCommand::REDUCE,
                                          _reduce_tasks,
                                          worker_id);

    } else {
      worker_command.set_kind(WorkerCommand::QUIT);

      remove_worker();
    }

    return worker_command;
  }

  WorkerCommand get_worker_command(WorkerCommand::Kind task_kind,
                                   std::vector<Task> &task_list,
                                   int32_t worker_id)
  {
    WorkerCommand worker_command;

    for (auto &task : task_list) {
      if (task.status != Task::TODO)
        continue;

      task.worker = worker_id;
      task.status = Task::DOING;

      worker_command.set_kind(task_kind);

      worker_command.set_task_id(task.id);

      if (task.file)
        worker_command.set_task_file(*task.file);

      return worker_command;
    }

    worker_command.set_kind(WorkerCommand::IDLE);

    return worker_command;
  }

  bool finish_worker_command(WorkerCommand const &worker_command)
  {
    auto task_id = worker_command.task_id();

    Task *task;

    switch (worker_command.kind()) {
      case WorkerCommand::MAP:
        task = &_map_tasks[task_id - 1];
        --_map_num_tasks_remaining;
        break;
      case WorkerCommand::REDUCE:
        task = &_reduce_tasks[task_id - _map_num_tasks - 1];
        --_reduce_num_tasks_remaining;
        break;
      default:
        assert(false);
    }

    task->worker = 0;
    task->status = Task::DONE;

    return _map_num_tasks_remaining == 0 && _reduce_num_tasks_remaining == 0;
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
        ss << "REDUCE [task " << worker_command.task_id() << "]";
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

  void remove_worker()
  {
    if (--_workers == 0) {
      std::thread t {
        [&]
        {
          std::this_thread::sleep_for(500ms);
          master_server->Shutdown();
        }
      };

      t.detach();
    }
  }

  int32_t _workers = 0;

  std::vector<Task> _map_tasks;
  int32_t _map_num_tasks;
  int32_t _map_num_tasks_remaining;

  std::vector<Task> _reduce_tasks;
  int32_t _reduce_num_tasks;
  int32_t _reduce_num_tasks_remaining;

};

} // end namespace mr

int main(int argc, char **argv)
{
  using namespace mr;

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " INPUT_FILES" << std::endl;

    return EXIT_FAILURE;
  }

  std::vector<std::string> input_files(argv + 1, argv + argc);

  int32_t map_num_tasks { static_cast<int32_t>(input_files.size()) };
  int32_t reduce_num_tasks { MR_REDUCE_NUM_TASKS };

  MasterImpl service(input_files, map_num_tasks, reduce_num_tasks);

  grpc::ServerBuilder builder;

  builder.AddListeningPort(CONFIG_MASTER_SOCKET,
                           grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  master_server = std::unique_ptr<grpc::Server>(builder.BuildAndStart());

  master_server->Wait();

  return EXIT_SUCCESS;
}
