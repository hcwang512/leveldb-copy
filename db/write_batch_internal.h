//
// Created by 王洪城 on 2024/5/5.
//

#ifndef LEVELDB_COPY_WRITE_BATCH_INTERNAL_H
#define LEVELDB_COPY_WRITE_BATCH_INTERNAL_H

#include "leveldb/write_batch.h"
#include "db/dbformat.h"

namespace leveldb {
class MemTable;
class WriteBatchInternal {
public:
    static int Count(const WriteBatch* batch);
    static void SetCount(WriteBatch* batch, int n);
    static SequenceNumber Sequence(const WriteBatch* batch);
    static void SetSequence(WriteBatch* batch, SequenceNumber seq);
    static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }
    static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }
    static void SetContents(WriteBatch* batch, const Slice& contents);
    static Status InsertInto(const WriteBatch* batch, MemTable* table);
    static void Append(WriteBatch* dst, const WriteBatch* src);
};
} // namespace leveldb
#endif //LEVELDB_COPY_WRITE_BATCH_INTERNAL_H
