//
// Created by 王洪城 on 2024/5/1.
//

#include "db/table_cache.h"
#include "util/coding.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {
struct TableAndFile {
    RandomAccessFile* file;
    Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
    delete tf->table;
    delete tf->file;
    delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
    Cache* cache = reinterpret_cast<Cache*>(arg1);
    Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
    cache->Release(h);
}
TableCache::TableCache(const std::string &dbname, const leveldb::Options& options, int entries)
    : env_(options.env), dbname_(dbname), options_(options), cache_(NewLRUCache(entries)){}

TableCache::~TableCache() { delete cache_; }
Status TableCache::FindTable(int64_t file_number, uint64_t file_size, Cache::Handle **handle) {
    Status s;
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, file_number);
    *handle = cache_->Lookup(key);
    if (*handle == nullptr) {
        std::string fname = TableFileName(dbname_, file_number);
        RandomAccessFile* file = nullptr;
        Table* table = nullptr;
        s = env_->NewRandomAccessFile(fname, &file);
        if (!s.ok()) {
            std::string old_name = SSTableFileName(dbname_, file_number);
            if (env_->NewRandomAccessFile(old_name, &file).ok()) {
                s = Status::OK();
            }
        }
        if (s.ok()) {
            s = Table::Open(options_, file, file_size, &table);
        }
        if (!s.ok()) {
            assert(table == nullptr);
            delete file;

        } else {
            TableAndFile* tf = new TableAndFile;
            tf->file = file;
            tf->table = table;
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
        }
    }
    return s;
}
Iterator* TableCache::NewIterator(const leveldb::ReadOptions &options, uint64_t file_number, uint64_t file_size,
                                  leveldb::Table **tableptr) {
    if (tableptr != nullptr) {
        *tableptr = nullptr;
    }
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (!s.ok()) {
        return NewErrorIterator(s);
    }
    Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    Iterator* result = table->NewIterator(options);
    result->RegisterCleanup(&UnrefEntry, cache_, handle);
    if (tableptr != nullptr) {
        *tableptr = table;
    }
    return result;
}
Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)){
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
        Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
        s = t->InternalGet(options, k, arg, handle_result);
        cache_->Release(handle);
    }
    return s;
}
void TableCache::Evict(uint64_t file_number) {
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    cache_->Erase(Slice(buf, sizeof(buf)));
}
} // namespace leveldb