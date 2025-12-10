cmake_minimum_required(VERSION 3.0)
project(rdma)

set(CMAKE_CXX_STANDARD 23)

# Use pkg-config to find rdma libraries
find_package(PkgConfig REQUIRED)
pkg_check_modules(RDMA REQUIRED libibverbs)

add_executable(rdma src/main.cpp)

# Include dirs for <infiniband/verbs.h>
target_include_directories(rdma PRIVATE ${RDMA_INCLUDE_DIRS})

# Link RDMA libs
target_link_libraries(rdma PRIVATE ${RDMA_LIBRARIES})
