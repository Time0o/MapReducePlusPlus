#include <memory>

#include <grpc/grpc.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_builder.h>

#include "hello.grpc.pb.h"

namespace hello
{

class HelloImpl : public Hello::Service
{
public:
  grpc::Status GetGreeting(grpc::ServerContext *context,
                           User const *user,
                           Greeting *greeting)
  {
    greeting->set_text("hello " + user->name() + "!");

    return grpc::Status::OK;
  }
};

} // end namespace hello

int main()
{
  using namespace hello;

  HelloImpl service;

  grpc::ServerBuilder builder;

  builder.AddListeningPort(CHANNEL, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();
}
