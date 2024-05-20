//
// Created by 王洪城 on 2024/5/5.
//

#ifndef LEVELDB_COPY_LOG_FORMAT_H
#define LEVELDB_COPY_LOG_FORMAT_H

namespace leveldb {
namespace log {
enum RecordType {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};
static const int kMaxRecordType = kLastType;
static const int kBlockSize = 32768;
static const int kHeaderSize = 4 + 2 + 1;
} // namespace log
} // namespace leveldb
#endif //LEVELDB_COPY_LOG_FORMAT_H
