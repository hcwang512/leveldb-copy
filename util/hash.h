//
// Created by 王洪城 on 2024/4/28.
//

#ifndef LEVELDB_COPY_HASH_H
#define LEVELDB_COPY_HASH_H

#include <cstddef>
#include <cstdint>

namespace leveldb {
    uint32_t Hash(const char* data, size_t n, uint32_t seed);
}

#endif //LEVELDB_COPY_HASH_H
