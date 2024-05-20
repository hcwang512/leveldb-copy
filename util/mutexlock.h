//
// Created by 王洪城 on 2024/4/27.
//

#ifndef LEVELDB_COPY_MUTEXLOCK_H
#define LEVELDB_COPY_MUTEXLOCK_H

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {
    class SCOPED_LOCKABLE MutexLock {
    public:
        explicit MutexLock(port::Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu): mu_(mu) {
            this->mu_->Lock();
        }
        ~MutexLock() UNLOCK_FUNCTION() { this->mu_->Unlock(); }
        MutexLock(const MutexLock&) = delete;
        MutexLock& operator=(const MutexLock&) = delete;

    private:
        port::Mutex* const mu_;
    };
}
#endif //LEVELDB_COPY_MUTEXLOCK_H
