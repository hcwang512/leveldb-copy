//
// Created by 王洪城 on 2024/4/28.
//

#ifndef LEVELDB_COPY_DB_IMPL_H
#define LEVELDB_COPY_DB_IMPL_H

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/comparator.h"
#include "db/dbformat.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "port/port.h"
#include "db/log_writer.h"
#include "db/log_reader.h"
#include "snapshot.h"
#include <deque>
#include <set>

namespace leveldb {
class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
public:
    DBImpl(const Options &raw_options, const std::string &dbname);
    DBImpl(const DBImpl&) = delete;
    DBImpl& operator=(const DBImpl&) = delete;

    ~DBImpl() override;

    Status Put(const WriteOptions&, const Slice& key, const Slice& value) override;
    Status Delete(const WriteOptions&, const Slice& key) override;

    Status Write(const WriteOptions& options, WriteBatch* updates) override;
    Status Get(const ReadOptions& options, const Slice& key,
               std::string* value) override;
    Iterator *NewIterator(const ReadOptions &options);

    Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest, VersionEdit* edit, SequenceNumber* max_sequence)
        EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);

private:
    friend class DB;
    struct CompactionState;
    struct Writer;

    struct ManualCompaction {
        int level;
        bool done;
        const InternalKey* begin;
        const InternalKey* end;
        InternalKey tmp_storage;
    };
    struct CompactionStats {
        CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}
        void Add(const CompactionStats& c) {
            this->micros += c.micros;
            this->bytes_read += c.bytes_read;
            this->bytes_written += c.bytes_written;
        }
        int64_t micros;
        int64_t bytes_read;
        int64_t bytes_written;
    };
    Iterator* NewIntenralIterator(const ReadOptions&, SequenceNumber* latest_snapshot, uint32_t* seed);
    Status NewDB();
    Status Recover(VersionEdit* edit, bool* save_manifest) EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void MaybeIgnoreError(Status* s) const;
    void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    const Comparator* user_comparator() const {
        return internal_comparator_.user_comparator();
    }
    void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    static void BGWork(void* db);
    void BackgroundCall();
    void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CleanupCompaction(CompactionState* compact)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    Status DoCompactionWork(CompactionState* compact)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
    void RecordBackgroundError(const Status &s);
    Status FinishCompactionOutputFile(CompactionState *compact, Iterator *input);
    Status OpenCompactionOutputFile(CompactionState* compact);
    Status InstallCompactionResults(CompactionState *compact);
    Status MakeRoomForWrite(bool force);
    WriteBatch *BuildBatchGroup(Writer **last_writer);
    Iterator* NewInternalIterator(const ReadOptions&,
                                  SequenceNumber* latest_snapshot,
                                  uint32_t* seed);

    Env* const env_;
    const InternalKeyComparator internal_comparator_;
    const InternalFilterPolicy internal_filter_policy_;
    const Options options_;
    const bool owns_info_log_;
    const bool owns_cache_;
    const std::string dbname_;

    TableCache* const table_cache_;

    FileLock* db_lock_;

    port::Mutex mutex_;
    std::atomic<bool> shutting_down_;
    port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);
    MemTable* mem_;
    MemTable* imm_ GUARDED_BY(mutex_);
    std::atomic<bool> has_imm_;
    WritableFile* logfile_;
    uint64_t logfile_number_ GUARDED_BY(mutex_);
    log::Writer* log_;
    uint32_t seed_ GUARDED_BY(mutex_);

    std::deque<Writer*> writers_ GUARDED_BY(mutex_);
    WriteBatch* tmp_batch_ GUARDED_BY(mutex_);
    SnapshotList snapshots_ GUARDED_BY(mutex_);
    std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);
    bool background_compaction_scheduled_ GUARDED_BY(mutex_);
    ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);
    VersionSet* const versions_ GUARDED_BY(mutex_);
    Status bg_error_ GUARDED_BY(mutex_);
    CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);

private:
    friend class DB;
    struct CompactionState;
    struct Writer;
};

Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);
}// namespace leveldb
#endif //LEVELDB_COPY_DB_IMPL_H
