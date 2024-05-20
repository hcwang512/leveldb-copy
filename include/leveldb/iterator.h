//
// Created by 王洪城 on 2024/4/28.
//

#ifndef LEVELDB_COPY_ITERATOR_H
#define LEVELDB_COPY_ITERATOR_H

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class LEVELDB_EXPORT Iterator {
public:
    Iterator();
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    virtual ~Iterator();

    virtual bool Valid() const = 0;
    virtual void SeekToFirst() = 0;
    virtual void SeekToLast() = 0;
    virtual void Seek(const Slice& target) = 0;
    virtual void Next() = 0;
    virtual void Prev() = 0;
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;
    virtual Status status() const = 0;
    using CleanupFunction = void(*)(void* arg1, void* arg2);
    void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);
private:
    struct CleanupNode {
        bool IsEmpty() const { return function == nullptr; }
        void Run() {
            (*function)(arg1, arg2);
        }
        CleanupFunction function;
        void* arg1;
        void* arg2;
        CleanupNode* next;
    };
    CleanupNode cleanup_head_;
};

LEVELDB_EXPORT Iterator* NewEmptyIterator();

LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);
}
#endif //LEVELDB_COPY_ITERATOR_H
