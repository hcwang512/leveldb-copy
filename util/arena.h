//
// Created by 王洪城 on 2024/5/4.
//

#ifndef LEVELDB_COPY_ARENA_H
#define LEVELDB_COPY_ARENA_H

#include <atomic>
#include <vector>
#include <cstdint>
#include <cstddef>

namespace leveldb {
class Arena {
public:

    Arena();
    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;
    ~Arena();

    char* Allocate(size_t bytes);

    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

    char* AllocateAligned(size_t bytes);

private:
    char* AllocateFallback(size_t bytes);
    char* AllocateNewBlock(size_t block_bytes);
    std::vector<char*> blocks_;
    char* alloc_ptr_;
    size_t alloc_bytes_remaining_;
    std::atomic<size_t> memory_usage_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_ARENA_H
