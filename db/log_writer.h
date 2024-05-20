//
// Created by 王洪城 on 2024/5/5.
//

#ifndef LEVELDB_COPY_LOG_WRITER_H
#define LEVELDB_COPY_LOG_WRITER_H

#include "db/log_format.h"
#include "leveldb/status.h"

namespace leveldb {
class WritableFile;
namespace log {
class Writer {
public:
    explicit Writer(WritableFile* dest);
    Writer(WritableFile* dest, uint64_t dest_length);

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    ~Writer();
    Status AddRecord(const Slice& slice);

private:
    Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);
    WritableFile* dest_;
    int block_offset_;
    uint32_t type_crc_[kMaxRecordType+1];
};
} // namespace log
} // namespace leveldb
#endif //LEVELDB_COPY_LOG_WRITER_H
