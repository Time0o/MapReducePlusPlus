#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpc/grpc.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <spdlog/spdlog.h>

#include "generator.h"
#include "key_value.h"

#include "mr.grpc.pb.h"

using namespace std::literals::chrono_literals;

namespace fs = std::filesystem;

namespace mr
{

class Worker
{
public:
  Worker(std::shared_ptr<grpc::ChannelInterface> channel)
  : _master(Master::NewStub(channel))
  { _info.set_id(0); }

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
    _info = get_config();

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
          {
            try {
              map_command(command);
            } catch (std::exception const &e) {
              spdlog::error("worker {}: map failed: {}", id(), e.what());
            }

            if (finish_command(command)) {
              spdlog::info("worker {}: done", id());
              return;
            }

            continue;
          }
        case WorkerCommand::REDUCE:
          {
            try {
              reduce_command(command);
            } catch (std::exception const &e) {
              spdlog::error("worker {}: reduce failed: {}", id(), e.what());
            }

            if (finish_command(command)) {
              spdlog::info("worker {}: done", id());
              return;
            }

            continue;
          }
        case WorkerCommand::QUIT:
          spdlog::info("worker {}: quitting", id());
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

  void map_command(WorkerCommand const &command) const
  {
    auto map_task { command.task_id() };
    auto map_file { command.task_file() };

    spdlog::info("worker {}: mapping file: {}", id(), map_file);

    // run map function
    std::ifstream map_stream(map_file);
    if (!map_stream)
      throw std::runtime_error("failed to open map file");

    std::stringstream map_ss;
    map_ss << map_stream.rdbuf();

    auto map_gen { map(map_file, map_ss) };

    // create temporary directory
    fs::path tmp_dir;

    for (;;) {
      tmp_dir = fs::temp_directory_path();

      try {
        fs::create_directories(tmp_dir);
        break;

      } catch (fs::filesystem_error& e) {
        throw std::runtime_error("failed to create temporary directory");
      }
    }

    std::vector<fs::path> reduce_paths;
    std::vector<fs::path> reduce_tmp_paths;
    std::vector<std::ofstream> reduce_tmp_streams;

    reduce_paths.reserve(reduce_num_tasks());
    reduce_tmp_paths.reserve(reduce_num_tasks());
    reduce_tmp_streams.reserve(reduce_num_tasks());

    // create temporary reduce files
    for (int32_t reduce_task = 1; reduce_task <= reduce_num_tasks(); ++reduce_task) {
      fs::path reduce_path { reduce_make_in_path(map_task, reduce_task) };
      fs::path reduce_tmp_path { tmp_dir / reduce_path };

      spdlog::info("worker {}: creating reduce input file {}", id(), reduce_path.string());

      reduce_paths.push_back(reduce_path);
      reduce_tmp_paths.push_back(reduce_tmp_path);
      reduce_tmp_streams.emplace_back(reduce_tmp_path);
    }

    // write to temporary reduce files
    while (map_gen.next()) {
      auto kv { map_gen.value() };

      std::hash<decltype(kv.key)> hasher;
      int32_t reduce_task = hasher(kv.key) % reduce_num_tasks() + 1;

      reduce_tmp_streams[reduce_task - 1] << kv.key << ' ' << kv.value << '\n';
      if (reduce_tmp_streams[reduce_task - 1].fail())
        throw std::runtime_error("failed to write to reduce input file");
    }

    // rename temporary reduce files
    for (int32_t reduce_task = 1; reduce_task <= reduce_num_tasks(); ++reduce_task) {
      auto const &old_path = reduce_tmp_paths[reduce_task - 1];
      auto const &new_path = reduce_paths[reduce_task - 1];

      reduce_tmp_streams[reduce_task - 1].flush();

      try {
        fs::rename(old_path, new_path);

      } catch (fs::filesystem_error &) {
        auto copy_options = fs::copy_options::overwrite_existing;
        fs::copy(old_path, new_path, copy_options);

        fs::remove(old_path);
      }
    }
  }

  void reduce_command(WorkerCommand const &command) const
  {
    auto reduce_task { command.task_id() };
    auto reduce_in_paths { reduce_glob_in_paths(reduce_task) };

    std::map<KV::key_type, std::vector<KV::value_type>> kv_map;

    for (auto const &reduce_in_path : reduce_in_paths) {
      spdlog::info("worker {}: reducing file {}", id(), reduce_in_path.string());

      std::ifstream reduce_in_stream { reduce_in_path };

      std::string line;

      KV::key_type key;
      KV::value_type value;

      while (std::getline(reduce_in_stream, line)) {
        std::istringstream ss { line };

        ss >> key >> value;
        if (ss.fail())
          throw std::runtime_error("failed to read from reduce input file");

        kv_map[key].push_back(value);
      }
    }

    auto reduce_out_path { reduce_make_out_path(reduce_task) };

    spdlog::info("worker {}: creating output file {}", id(), reduce_out_path.string());

    std::ofstream reduce_out_stream { reduce_out_path.string() };

    for (auto const &[key, values] : kv_map) {
      auto reduced_value { reduce(key, values.begin(), values.end()) };

      reduce_out_stream << key << ' ' << reduced_value << '\n';
      if (reduce_out_stream.fail())
        throw std::runtime_error("failed to write to reduce output file");
    }
  }

  int32_t id() const
  { return _info.id(); }

  int32_t map_num_tasks() const
  { return _info.map_num_tasks(); }

  int32_t reduce_num_tasks() const
  { return _info.reduce_num_tasks(); }

  static fs::path reduce_make_in_path(int32_t map_task, int32_t reduce_task)
  {
    return fmt::format(
      "{}_{}_{}.mr", MR_REDUCE_IN_FILE_PREFIX, map_task, reduce_task);
  }

  static std::vector<fs::path> reduce_glob_in_paths(int32_t reduce_task)
  {
    std::vector<fs::path> reduce_paths;

    std::regex r(fmt::format(
      "{}_\\d+_{}.mr", MR_REDUCE_IN_FILE_PREFIX, reduce_task));

    for (const auto & entry : fs::directory_iterator(".")) {
      if (std::regex_match(entry.path().filename().string(), r))
        reduce_paths.push_back(entry.path());
    }

    return reduce_paths;
  }

  static fs::path reduce_make_out_path(int32_t reduce_task)
  {
    return fmt::format(
      "{}_{}.mr", MR_REDUCE_OUT_FILE_PREFIX, reduce_task);
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

  bool finish_command(WorkerCommand const &command) const
  {
    FinishCommandRequest request;
    *request.mutable_worker_info() = _info;
    *request.mutable_worker_command() = command;

    FinishCommandResponse response;
    rpc("finish command", &Master::Stub::FinishCommand, request, &response);

    return response.done();
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
