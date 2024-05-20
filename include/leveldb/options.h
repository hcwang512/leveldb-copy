//
// Created by 王洪城 on 2024/4/29.
//

#ifndef LEVELDB_COPY_OPTIONS_H
#define LEVELDB_COPY_OPTIONS_H

#include <cstddef>
#include "leveldb/export.h"

namespace leveldb {
class Cache;
class Comparator;
class Env;
class FilterPolicy;
class Logger;
class Snapshot;

enum CompressionType {
    kNoCompression = 0x0,
    kSnappyCompression = 0x1,
    kZstdCompression = 0x2,
};

struct LEVELDB_EXPORT Options {
    Options();
    const Comparator* comparator;
    bool create_if_missing = false;
    bool error_if_exists = false;
    bool paranoid_checks = false;
    Env* env;
    Logger* info_log = nullptr;

    size_t write_buffer_size = 4 * 1024 * 1024;
    int max_open_files = 1000;
    Cache* block_cache= nullptr;
    size_t block_size = 4 * 1024;
    int block_restart_interval = 16;
    size_t max_file_size = 2 * 1024 * 1024;
    CompressionType compression = kSnappyCompression;

    int zstd_compression_level =1 ;
    bool reuse_logs = false;

    const FilterPolicy* filter_policy = nullptr;
};

struct LEVELDB_EXPORT ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;
    const Snapshot* snapshot = nullptr;
};
struct LEVELDB_EXPORT WriteOptions {
    WriteOptions() = default;
    bool sync = false;
};
}
#endif //LEVELDB_COPY_OPTIONS_H
