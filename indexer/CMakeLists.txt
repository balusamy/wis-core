cmake_minimum_required(VERSION 2.8)

project(Indexer)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/../cmake_modules)

include(rpcz_functions)

find_package(Boost COMPONENTS thread system filesystem program_options)
find_package(ProtobufPlugin REQUIRED)
find_package(RPCZ REQUIRED)

include_directories(${Boost_INCLUDE_DIRS} ${RPCZ_INCLUDE_DIRS} 
    ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++11")

PROTOBUF_GENERATE_RPCZ(INDEX_RPCZ_SRCS INDEX_RPCZ_HDRS ../index_server.proto)

set(index_server_src
    main.cpp
    store_manager.cpp
    index_builder.cpp
    index_search.cpp
    value_db.cpp
    stagedb.cpp
    index.cpp
    fuzzy_processor.cpp
    trie.cpp
    ${INDEX_RPCZ_SRCS}
    ${INDEX_RPCZ_HDRS}
)

add_executable(index_server ${index_server_src})

find_library(ZMQPP_LIBRARY NAMES libzmqpp.so.2)
find_library(LEVELDB_LIBRARY NAMES leveldb)
find_library(MONGO_CLIENT_LIBRARY NAMES mongoclient)

target_link_libraries(index_server ${Boost_LIBRARIES} ${ZMQPP_LIBRARY} ${RPCZ_LIBRARIES}
    ${LEVELDB_LIBRARY} ${MONGO_CLIENT_LIBRARY})

add_executable(partstat partstat.cpp)
target_link_libraries(partstat ${Boost_LIBRARIES})
