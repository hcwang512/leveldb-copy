//
// Created by 王洪城 on 2024/5/4.
//

#ifndef LEVELDB_COPY_MEMTABLE_H
#define LEVELDB_COPY_MEMTABLE_H

#include "db/dbformat.h"
#include "util/arena.h"
#include "db/skiplist.h"
#include "leveldb/db.h"

namespace leveldb {
class InternalKeyComparator;
class MemTableIterator;

class MemTable {
public:
    explicit MemTable(const InternalKeyComparator& comparator);
    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Ref() { ++refs_; }
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ <= 0) {
            delete this;
        }
    }

    size_t ApproximateMemoryUsage();

    Iterator* NewIterator();
    void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);
    bool Get(const LookupKey& key, std::string* value, Status* s);

private:
    friend class MemTableIterator;
    friend class MemTableBackwardIterator;

    struct KeyComparator {
        const InternalKeyComparator comparator;
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
        int operator()(const char* a, const char* b) const;
    };

    typedef SkipList<const char*, KeyComparator> Table;

    ~MemTable();
    KeyComparator comparator_;
    int refs_;
    Arena arena_;
    Table table_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_MEMTABLE_H
