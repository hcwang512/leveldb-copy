//
// Created by 王洪城 on 2024/5/6.
//

#ifndef LEVELDB_COPY_SNAPSHOT_H
#define LEVELDB_COPY_SNAPSHOT_H

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {
    class SnapshotList;

    class SnapshotImpl : public Snapshot {
    public:
        SnapshotImpl(SequenceNumber sequence_number)
        : sequence_number_(sequence_number) {}
        SequenceNumber sequence_number() const { return sequence_number_; }
    private:
        friend class SnapshotList;
        SnapshotImpl* prev_;
        SnapshotImpl* next_;

        const SequenceNumber sequence_number_;

#if !defined(NDEBUG)
        SnapshotList* list_ = nullptr;
#endif
    };
    class SnapshotList {
    public:
        SnapshotList(): head_(0) {
            head_.prev_ = &head_;
            head_.next_ = &head_;
        }
        bool empty() const { return head_.next_ == &head_; }
        SnapshotImpl* oldest() const {
            assert(!empty());
            return head_.next_;
        }
        SnapshotImpl* newest() const {
            return head_.prev_;
        }
        SnapshotImpl* New(SequenceNumber sequence_number) {
            SnapshotImpl* snapshot = new SnapshotImpl(sequence_number);
            snapshot->list_ = this;
            snapshot->next_ = &head_;
            snapshot->prev_ = head_.prev_;
            snapshot->prev_->next_ = snapshot;
            snapshot->next_->prev_ = snapshot;
            return snapshot;
        }
        void Delete(const SnapshotImpl* snapshot) {
#if !defined(NDEBUG)
            assert(snapshot->list_ == this);
#endif  // !defined(NDEBUG)
            snapshot->prev_->next_ = snapshot->next_;
            snapshot->next_->prev_ = snapshot->prev_;
            delete snapshot;
        }

    private:
        SnapshotImpl head_;
    };
};

#endif //LEVELDB_COPY_SNAPSHOT_H
