cmake_minimum_required(VERSION 3.12)

project(Hello LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

# packages
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# directry structure
set(SRC_DIR "${CMAKE_SOURCE_DIR}")

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto/out")
set(GRPC_SRC "${PROTO_DIR}/hello.grpc.pb.cc")
set(GRPC_HDR "${PROTO_DIR}/hello.grpc.pb.h")
set(PROTO_SRC "${PROTO_DIR}/hello.pb.cc")
set(PROTO_HDR "${PROTO_DIR}/hello.pb.h")

# communication channel
if (NOT DEFINED CHANNEL)
  set(CHANNEL "localhost:50051")
endif()

add_compile_definitions(CHANNEL="${CHANNEL}")

# grpc/protobuf library
set(PROTO_LIB grpc_proto)

add_library("${PROTO_LIB}"
  "${GRPC_SRC}"
  "${PROTO_SRC}")

target_link_libraries("${PROTO_LIB}"
  "protobuf::libprotobuf"
  "gRPC::grpc++"
  "gRPC::grpc++_reflection")

target_include_directories("${PROTO_LIB}" PRIVATE "${PROTO_DIR}")

# server/client binaries
set(SERVER_BIN server)
set(CLIENT_BIN client)

foreach (bin "${SERVER_BIN}" "${CLIENT_BIN}")
  add_executable("${bin}" "${SRC_DIR}/${bin}.cc")

  target_include_directories("${bin}" PRIVATE "${PROTO_DIR}")

  target_link_libraries("${bin}"
    "${PROTO_LIB}"
    "protobuf::libprotobuf"
    "gRPC::grpc++"
    "gRPC::grpc++_reflection")
endforeach()
