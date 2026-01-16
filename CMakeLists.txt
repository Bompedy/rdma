cmake_minimum_required(VERSION 3.0)
project(rdma)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)

# Find both libibverbs and librdmacm
pkg_check_modules(RDMA REQUIRED libibverbs librdmacm)

add_executable(rdma src/main.cpp)

# Include dirs for:
#   <infiniband/verbs.h>
#   <rdma/rdma_cma.h>
target_include_directories(rdma PRIVATE
        ${RDMA_INCLUDE_DIRS}
)

# Link RDMA libraries
target_link_libraries(rdma PRIVATE
        ${RDMA_LIBRARIES}
)
