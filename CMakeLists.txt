cmake_minimum_required(VERSION 2.8)

project(Hilbert)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake_modules)

include(rpcz_functions)

find_package(Boost COMPONENTS thread system filesystem)
find_package(ProtobufPlugin REQUIRED)
find_package(RPCZ REQUIRED)

include_directories(${Boost_INCLUDE_DIRS} ${RPCZ_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++0x")

PROTOBUF_GENERATE_RPCZ(INDEX_RPCZ_SRCS INDEX_RPCZ_HDRS index_server.proto)

set(hilbert_src
    main.cpp
    server.cpp
    stagedb.cpp
    hilbert.cpp
    ${INDEX_RPCZ_SRCS}
    ${INDEX_RPCZ_HDRS}
)

add_executable(hilbert ${hilbert_src})

find_library(ZMQPP_LIBRARY NAMES libzmqpp.so.2)
find_library(LEVELDB_LIBRARY NAMES leveldb)

target_link_libraries(hilbert ${Boost_LIBRARIES} ${ZMQPP_LIBRARY} ${RPCZ_LIBRARIES}
    ${LEVELDB_LIBRARY})
