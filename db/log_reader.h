//
// Created by 王洪城 on 2024/5/5.
//

#ifndef LEVELDB_COPY_LOG_READER_H
#define LEVELDB_COPY_LOG_READER_H

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {
class SequentialFile;

namespace log {
class Reader {
public:
    class Reporter {
        virtual ~Reporter();

    public:
        virtual void Corruption(size_t bytes, const Status& status) = 0;
    };

    Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset);
    Reader(const Reader* reader) = delete;
    Reader& operator=(const Reader&) = delete;

    ~Reader();

    bool ReadRecord(Slice* record, std::string* scratch);
    uint64_t LastRecordOffset();

private:
    enum {
        kEof = kMaxRecordType + 1,
        kBadRecord = kMaxRecordType + 2,
    };
    bool SkipToInitialBlock();
    unsigned int ReadPhysicalRecord(Slice* reuslt);
    void ReportCorruption(uint64_t bytes, const char* reason);
    void ReportDrop(uint64_t bytes, const Status &reason);

    SequentialFile* const file_;
    Reporter* const reporter_;
    bool const checksum_;
    char* const backing_store_;
    Slice buffer_;
    bool eof_;
    uint64_t last_record_offset_;
    uint64_t end_of_buffer_offset_;
    uint64_t const initial_offset_;
    bool resyncing_;
};
} // namespace log
} // namespace leveldb
#endif //LEVELDB_COPY_LOG_READER_H
