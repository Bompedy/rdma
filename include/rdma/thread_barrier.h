#pragma once

#include <condition_variable>
#include <cstdint>
#include <mutex>

class ThreadBarrier {
public:
    explicit ThreadBarrier(const uint32_t participants)
        : participants_(participants), remaining_(participants) {}

    void arrive_and_wait() {
        std::unique_lock lock(mutex_);
        const uint64_t generation = generation_;

        if (--remaining_ == 0) {
            remaining_ = participants_;
            ++generation_;
            lock.unlock();
            condition_.notify_all();
            return;
        }

        condition_.wait(lock, [&] { return generation_ != generation; });
    }

private:
    const uint32_t participants_;
    uint32_t remaining_;
    uint64_t generation_ = 0;
    std::mutex mutex_;
    std::condition_variable condition_;
};

