cmake_minimum_required(VERSION 3.16)

project(raft LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(PROTO_DIR ${CMAKE_SOURCE_DIR}/protos)
set(GEN_DIR ${CMAKE_BINARY_DIR}/generated)

find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

file(MAKE_DIRECTORY "${GEN_DIR}")

file(GLOB_RECURSE PROTO_FILES CONFIGURE_DEPENDS
    "${PROTO_DIR}/*.proto"
)

file(GLOB_RECURSE RAFT_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)

file(GLOB_RECURSE RAFT_HEADERS CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp"
)

set(GENERATED_PROTO_SOURCES)
set(GENERATED_PROTO_HEADERS)

foreach(PROTO_FILE ${PROTO_FILES})
    file(RELATIVE_PATH PROTO_REL_PATH "${PROTO_DIR}" "${PROTO_FILE}")
    get_filename_component(PROTO_REL_DIR "${PROTO_REL_PATH}" DIRECTORY)
    get_filename_component(PROTO_NAME_WE "${PROTO_FILE}" NAME_WE)

    if(PROTO_REL_DIR STREQUAL "")
        set(PROTO_GEN_BASE "${GEN_DIR}/${PROTO_NAME_WE}")
    else()
        file(MAKE_DIRECTORY "${GEN_DIR}/${PROTO_REL_DIR}")
        set(PROTO_GEN_BASE "${GEN_DIR}/${PROTO_REL_DIR}/${PROTO_NAME_WE}")
    endif()

    set(PROTO_SRC "${PROTO_GEN_BASE}.pb.cc")
    set(PROTO_HDR "${PROTO_GEN_BASE}.pb.h")
    set(GRPC_SRC "${PROTO_GEN_BASE}.grpc.pb.cc")
    set(GRPC_HDR "${PROTO_GEN_BASE}.grpc.pb.h")

    add_custom_command(
        OUTPUT
            "${PROTO_SRC}"
            "${PROTO_HDR}"
            "${GRPC_SRC}"
            "${GRPC_HDR}"
        COMMAND protobuf::protoc
        ARGS
            --proto_path=${PROTO_DIR}
            --cpp_out=${GEN_DIR}
            --grpc_out=${GEN_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${PROTO_FILE}
        DEPENDS "${PROTO_FILE}"
        VERBATIM
    )

    list(APPEND GENERATED_PROTO_SOURCES
        "${PROTO_SRC}"
        "${GRPC_SRC}"
    )

    list(APPEND GENERATED_PROTO_HEADERS
        "${PROTO_HDR}"
        "${GRPC_HDR}"
    )
endforeach()

add_library(raft_proto
    ${GENERATED_PROTO_SOURCES}
    ${GENERATED_PROTO_HEADERS}
)

target_link_libraries(raft_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

target_include_directories(raft_proto
    PUBLIC
        "${GEN_DIR}"
)

add_executable(raft
    ${RAFT_SOURCES}
    ${RAFT_HEADERS}
)

target_link_libraries(raft
    PRIVATE
        raft_proto
)

target_include_directories(raft
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

# --- temporary for testing only ---
add_executable(test_client
    test/test_client.cpp
)

target_link_libraries(test_client
    PRIVATE
        raft_proto
)
