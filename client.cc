#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "hello.grpc.pb.h"

namespace hello
{

class HelloClient
{
public:
  HelloClient(std::shared_ptr<grpc::ChannelInterface> channel)
  : m_stub(Hello::NewStub(channel))
  {}

  Greeting GetGreeting(User const &user) {
    grpc::ClientContext context;

    Greeting greeting;
    auto status = m_stub->GetGreeting(&context, user, &greeting);

    if (!status.ok())
      throw std::runtime_error("failed to obtain greeting from server: " + status.error_message());

    return greeting;
  }

private:
  std::unique_ptr<Hello::Stub> m_stub;
};

} // end namespace hello

int main(int argc, char **argv)
{
  using namespace hello;

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " USER_NAME" << std::endl;
    return EXIT_FAILURE;
  }

  HelloClient client(
    grpc::CreateChannel(CHANNEL, grpc::InsecureChannelCredentials()));

  char const *user_name = argv[1];

  User user;
  user.set_name(user_name);

  try {
    auto greeting = client.GetGreeting(user);

    std::cout << greeting.text() << std::endl;

  } catch (std::runtime_error const &e) {
    std::cerr << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
