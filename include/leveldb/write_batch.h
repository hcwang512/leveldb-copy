//
// Created by 王洪城 on 2024/5/5.
//

#ifndef LEVELDB_COPY_WRITE_BATCH_H
#define LEVELDB_COPY_WRITE_BATCH_H

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {
class Slice;
class LEVELDB_EXPORT WriteBatch {
public:
    class LEVELDB_EXPORT Handler {
    public:
        virtual ~Handler();
        virtual void Put(const Slice& key, const Slice& value) = 0;
        virtual void Delete(const Slice& key) = 0;
    };

    WriteBatch();
    // Intentionally copyable.
    WriteBatch(const WriteBatch&) = default;
    WriteBatch& operator=(const WriteBatch&) = default;

    ~WriteBatch();
    void Put(const Slice& key, const Slice& value);

    void Delete(const Slice& key);
    void Clear();
    size_t ApproximateSize() const;
    void Append(const WriteBatch& source);
    Status Iterate(Handler* handle) const;
private:
    friend class WriteBatchInternal;
    std::string rep_;
};
} // namespace leveldb
#endif //LEVELDB_COPY_WRITE_BATCH_H
