//
// Created by 王洪城 on 2024/5/6.
//

#ifndef LEVELDB_COPY_MERGER_H
#define LEVELDB_COPY_MERGER_H

namespace leveldb {
    class Comparator;
    class Iterator;

    Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n);
}
#endif //LEVELDB_COPY_MERGER_H
