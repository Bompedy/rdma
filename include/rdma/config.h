#pragma once

// Fixed RDMA constants. Things you'd never change between runs.
// Benchmark params come from env vars at runtime.

#include <cstdint>
#include <cstddef>

constexpr size_t   ENTRY_SIZE          = 8;
constexpr size_t   QP_DEPTH            = 2048;
constexpr size_t   MAX_INLINE_DATA     = 64;
constexpr size_t   MAX_REPLICAS        = 10;
constexpr size_t   MAX_THREADS         = 16;
constexpr uint8_t  RESPONDER_RESOURCES = 16;
constexpr uint8_t  INITIATOR_DEPTH     = 16;
constexpr size_t   BUF_SIZE            = 64 * 1024 * 1024;
constexpr size_t   PAGE_SIZE           = 4096;
