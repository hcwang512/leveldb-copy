//
// Created by 王洪城 on 2024/5/4.
//

#ifndef LEVELDB_COPY_TWO_LEVEL_ITERATOR_H
#define LEVELDB_COPY_TWO_LEVEL_ITERATOR_H

#include "leveldb/iterator.h"

namespace leveldb {
struct ReadOptions;

Iterator* NewTwoLevelIterator(Iterator* index_iter, Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
                              void* arg, const ReadOptions& options);
} // namespace leveldb
#endif //LEVELDB_COPY_TWO_LEVEL_ITERATOR_H
