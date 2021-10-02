cmake_minimum_required(VERSION 3.12)

project(Hello LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

# packages
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# directry structure
set(SRC_DIR "${CMAKE_SOURCE_DIR}")

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_OUT_DIR "${PROTO_DIR}/out")
set(PROTO_FILE "${PROTO_DIR}/hello.proto")
set(GRPC_SRC "${PROTO_OUT_DIR}/hello.grpc.pb.cc")
set(GRPC_HDR "${PROTO_OUT_DIR}/hello.grpc.pb.h")
set(PROTO_SRC "${PROTO_OUT_DIR}/hello.pb.cc")
set(PROTO_HDR "${PROTO_OUT_DIR}/hello.pb.h")

# communication channel
if (NOT DEFINED CHANNEL)
  set(CHANNEL "localhost:50051")
endif()

add_compile_definitions(CHANNEL="${CHANNEL}")

# grpc/protobuf library
set(PROTOC $<TARGET_FILE:protobuf::protoc>)
set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)

add_custom_command(
  OUTPUT "${PROTO_SRC}" "${PROTO_HDR}"
  COMMAND ${PROTOC} ${PROTO_FILE}
            --cpp_out=${PROTO_OUT_DIR}
            --proto_path=${PROTO_DIR})

add_custom_command(
  OUTPUT "${GRPC_SRC}" "${GRPC_HDR}"
  COMMAND ${PROTOC} ${PROTO_FILE}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            --grpc_out=${PROTO_OUT_DIR}
            --proto_path=${PROTO_DIR})

set(PROTO_LIB grpc_proto)

add_library("${PROTO_LIB}"
  "${GRPC_SRC}"
  "${PROTO_SRC}")

target_link_libraries("${PROTO_LIB}"
  "protobuf::libprotobuf"
  "gRPC::grpc++"
  "gRPC::grpc++_reflection")

target_include_directories("${PROTO_LIB}" PRIVATE "${PROTO_OUT_DIR}")

# server/client binaries
set(SERVER_BIN server)
set(CLIENT_BIN client)

foreach (bin "${SERVER_BIN}" "${CLIENT_BIN}")
  add_executable("${bin}" "${SRC_DIR}/${bin}.cc")

  target_include_directories("${bin}" PRIVATE "${PROTO_OUT_DIR}")

  target_link_libraries("${bin}"
    "${PROTO_LIB}"
    "protobuf::libprotobuf"
    "gRPC::grpc++"
    "gRPC::grpc++_reflection")
endforeach()
