//
// Created by 王洪城 on 2024/5/1.
//

#ifndef LEVELDB_COPY_BLOCK_BUILDER_H
#define LEVELDB_COPY_BLOCK_BUILDER_H

#include <string>
#include "leveldb/slice.h"

namespace leveldb {
struct Options;
class BlockBuilder {
public:
    explicit BlockBuilder(const Options* options);
    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder& operator=(const BlockBuilder&) = delete;

    void Reset();
    void Add(const Slice& key, const Slice& value);
    Slice Finish();
    size_t CurrentSizeEstimate() const;
    bool empty() const { return buffer_.empty();}

private:
    const Options* options_;
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    int counter_;
    bool finished_;
    std::string last_key_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_BLOCK_BUILDER_H
