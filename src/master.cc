#include <chrono>
#include <memory>

#include <grpc/grpc.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_builder.h>

#include "mr.grpc.pb.h"

namespace mr
{

class MasterImpl : public Master::Service
{
public:
  grpc::Status GetWorkerConfig(grpc::ServerContext *context,
                               Empty const *empty,
                               WorkerConfig *config)
  {
    config->set_id(++_workers);

    return grpc::Status::OK;
  }

  grpc::Status GetWorkerTask(grpc::ServerContext *context,
                             Empty const *empty,
                             WorkerTask *task)
  {
    task->set_kind(WorkerTask::QUIT); // XXX

    return grpc::Status::OK;
  }

private:
  int32_t _workers = 0;
};

} // end namespace hello

int main()
{
  using namespace mr;

  MasterImpl service;

  grpc::ServerBuilder builder;

  builder.AddListeningPort(CONFIG_MASTER_SOCKET,
                           grpc::InsecureServerCredentials());

  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();
}
