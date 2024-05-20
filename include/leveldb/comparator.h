//
// Created by 王洪城 on 2024/4/28.
//

#ifndef LEVELDB_COPY_COMPARATOR_H
#define LEVELDB_COPY_COMPARATOR_H

#include <string>
#include "leveldb/export.h"

namespace leveldb {

class Slice;

class LEVELDB_EXPORT Comparator {
public:
    virtual ~Comparator();
    virtual int Compare(const Slice& a, const Slice& b) const = 0;
    virtual const char* Name() const = 0;
    virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;
    virtual void FindShortSuccessor(std::string* key) const = 0;
};

LEVELDB_EXPORT const Comparator* BytewiseComparator();
} // namespace leveldb
#endif //LEVELDB_COPY_COMPARATOR_H
