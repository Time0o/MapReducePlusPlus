#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
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
  MasterImpl(std::vector<std::string> const &files)
  {
    for (auto const &file : files) {
      int32_t id = static_cast<int32_t>(_tasks.size()) + 1;

      _tasks.emplace_back(id, file);
    }
  }

  grpc::Status GetConfig(grpc::ServerContext *context,
                         Empty const *empty,
                         WorkerConfig *worker_config)
  {
    worker_config->set_id(++_workers);

    return grpc::Status::OK;
  }

  grpc::Status GetCommand(grpc::ServerContext *context,
                          WorkerInfo const *worker_info,
                          WorkerCommand *worker_command)
  {
    bool done = true;

    for (auto &task : _tasks) {
      if (task.status == Task::TODO) {
        spdlog::info("master: starting task {} on worker {}", task.id, worker_info->id());

        task.worker = worker_info->id();
        task.status = Task::DOING;

        worker_command->set_kind(WorkerCommand::MAP); // XXX
        worker_command->set_file(task.file); // XXX

        done = false;

        break;
      }
    }

    if (done) {
      spdlog::info("master: quitting worker {}", worker_info->id());

      worker_command->set_kind(WorkerCommand::QUIT); // XXX
    }

    return grpc::Status::OK;
  }

private:
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
