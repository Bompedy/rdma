#pragma once

#include <cstdint>
#include <cstddef>

constexpr uint32_t ENTRY_SIZE          = 8;
constexpr uint32_t QP_DEPTH            = 2048;
constexpr uint32_t MAX_INLINE_DATA     = 64;
constexpr uint32_t MAX_REPLICAS        = 10;
constexpr uint32_t MAX_THREADS         = 16;
constexpr uint8_t  RESPONDER_RESOURCES = 16;
constexpr uint8_t  INITIATOR_DEPTH     = 16;
constexpr uint64_t BUF_SIZE            = 64 * 1024 * 1024;
constexpr uint32_t PAGE_SIZE           = 4096;
