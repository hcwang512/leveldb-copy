//
// Created by 王洪城 on 2024/4/29.
//

#ifndef LEVELDB_COPY_FILTER_POLICY_H
#define LEVELDB_COPY_FILTER_POLICY_H

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT FilterPolicy {
public:
    virtual ~FilterPolicy();
    virtual const char* Name() const = 0;
    virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const = 0;
    virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const = 0;
};

LEVELDB_EXPORT const FilterPolicy* NewBloomFilterPolicy(int bits_per_key);
} // namespace leveldb

#endif //LEVELDB_COPY_FILTER_POLICY_H
