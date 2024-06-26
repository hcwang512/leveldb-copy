//
// Created by 王洪城 on 2024/5/6.
//

#include "table/merger.h"
#include "leveldb/iterator.h"
#include "iterator_wrapper.h"
#include "leveldb/comparator.h"

namespace leveldb {
namespace {
class MergingIterator: public Iterator {
public:
    MergingIterator(const Comparator* comparator, Iterator** children, int n)
    : comparator_(comparator), children_(new IteratorWrapper[n]),
    n_(n), current_(nullptr), direction_(kForward) {
        for (int i = 0; i < n ; i ++) {
            children_[i].Set(children[i]);
        }
    }
    ~MergingIterator() { delete[] children_; }
    bool Valid() const override { return (current_ != nullptr); }
    void SeekToFirst() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();

        }
        FindSmallest();
        direction_ = kForward;
    }
    void SeekToLast() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }
    void Seek(const Slice& target) override {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }
    void Next() override {
        assert(Valid());

        if (direction_ != kForward) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid() && comparator_->Compare(key(), child->key()) == 0) {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }
        current_->Next();
        FindSmallest();
    }
    void Prev() override {
        if (direction_ != kReverse) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->Prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        child->SeekToLast();
                    }
                }
            }
            direction_ = kReverse;
        }

        current_->Prev();
        FindLargest();
    }
    Slice key() const override {
        assert(Valid());
        return current_->key();
    }

    Slice value() const override {
        assert(Valid());
        return current_->value();
    }

    Status status() const override {
        Status status;
        for (int i = 0; i < n_; i++) {
            status = children_[i].status();
            if (!status.ok()) {
                break;
            }
        }
        return status;
    }

private:
    enum Direction { kForward, kReverse };
    void FindSmallest();
    void FindLargest();
    const Comparator* comparator_;
    int n_;
    IteratorWrapper* current_;
    IteratorWrapper* children_;
    Direction direction_;
};
void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = nullptr;
    for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child->Valid()) {
            if (smallest == nullptr) {
                smallest = child;
            } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}
    void MergingIterator::FindLargest() {
        IteratorWrapper* largest = nullptr;
        for (int i = n_ - 1; i >= 0; i--) {
            IteratorWrapper* child = &children_[i];
            if (child->Valid()) {
                if (largest == nullptr) {
                    largest = child;
                } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                    largest = child;
                }
            }
        }
        current_ = largest;
    }
}   // namespace
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n) {
    assert(n >= 0);
    if (n == 0) {
        return NewEmptyIterator();
    } else if (n== 1) {
        return children[0];
    } else {
        return new MergingIterator(comparator, children, n);
    }
}
} // namespace leveldb