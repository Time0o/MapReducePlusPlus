cmake_minimum_required(VERSION 3.12)

project(MapReduce LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

# packages
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# directry structure
set(SRC_DIR "${CMAKE_SOURCE_DIR}/src")
set(INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include")

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/proto")
set(PROTO_OUT_DIR "${PROTO_DIR}/out")
set(PROTO_FILE "${PROTO_DIR}/mr.proto")
set(GRPC_SRC "${PROTO_OUT_DIR}/mr.grpc.pb.cc")
set(GRPC_HDR "${PROTO_OUT_DIR}/mr.grpc.pb.h")
set(PROTO_SRC "${PROTO_OUT_DIR}/mr.pb.cc")
set(PROTO_HDR "${PROTO_OUT_DIR}/mr.pb.h")

# configuration
if (NOT DEFINED CONFIG_MASTER_SOCKET)
  set(CONFIG_MASTER_SOCKET "localhost:50051")
endif()

if (NOT DEFINED CONFIG_WORKER_SLEEP_FOR)
  set(CONFIG_WORKER_SLEEP_FOR "100ms")
endif()

add_compile_definitions(
  CONFIG_MASTER_SOCKET="${CONFIG_MASTER_SOCKET}"
  CONFIG_WORKER_SLEEP_FOR=${CONFIG_WORKER_SLEEP_FOR})

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

# master/worker binaries
foreach (bin master worker)
  add_executable("${bin}" "${SRC_DIR}/${bin}.cc")

  target_include_directories("${bin}" PRIVATE "${INCLUDE_DIR}" "${PROTO_OUT_DIR}")

  target_link_libraries("${bin}"
    "${PROTO_LIB}"
    "protobuf::libprotobuf"
    "gRPC::grpc++"
    "gRPC::grpc++_reflection")
endforeach()
