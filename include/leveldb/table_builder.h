//
// Created by 王洪城 on 2024/5/1.
//

#ifndef LEVELDB_COPY_TABLE_BUILDER_H
#define LEVELDB_COPY_TABLE_BUILDER_H

#include "leveldb/export.h"
#include "leveldb/status.h"
#include "leveldb/options.h"

namespace leveldb {
class BlockBuilder;
class BlockHandle;
class WritableFile;

class LEVELDB_EXPORT TableBuilder {
public:
    TableBuilder(const Options& options, WritableFile* file);
    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    ~TableBuilder();
    Status ChangeOptions(const Options& options);
    void Add(const Slice& key, const Slice& value);
    void Flush();
    Status status() const;
    Status Finish();
    void Abandon();
    uint64_t NumEntries() const;
    uint64_t FileSize() const;
private:
    bool ok() const { return status().ok();}
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle);

    struct Rep;
    Rep* rep_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_TABLE_BUILDER_H
