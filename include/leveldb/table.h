//
// Created by 王洪城 on 2024/5/1.
//

#ifndef LEVELDB_COPY_TABLE_H
#define LEVELDB_COPY_TABLE_H

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"

namespace leveldb {
class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
class TableCache;

class LEVELDB_EXPORT Table {
public:
    static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;
    ~Table();
    Iterator* NewIterator(const ReadOptions&) const;
    uint64_t ApproximateOffsetOf(const Slice& key) const;
private:
    friend class TableCache;
    struct Rep;
    static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);
    explicit Table(Rep* rep) : rep_(rep) {}
    Status InternalGet(const ReadOptions&, const Slice& key, void* arg, void(*handle_result)(void* arg, const Slice& k, const Slice& v));
    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);

    Rep* const rep_;
};

} // namespace leveldb
#endif //LEVELDB_COPY_TABLE_H
