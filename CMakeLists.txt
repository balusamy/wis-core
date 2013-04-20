cmake_minimum_required(VERSION 2.8)

project(Hilbert)

find_package(Boost COMPONENTS thread system)

include_directories(${Boost_INCLUDE_DIRS})

set(CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS} "-std=c++0x")

set(hilbert_src
    main.cpp
    hilbert.cpp
)

add_executable(hilbert ${hilbert_src})

find_library(ZMQPP_LIBRARY NAMES libzmqpp.so.2)

target_link_libraries(hilbert ${Boost_LIBRARIES} ${ZMQPP_LIBRARY})
