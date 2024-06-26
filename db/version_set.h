//
// Created by 王洪城 on 2024/4/29.
//

#ifndef LEVELDB_COPY_VERSION_SET_H
#define LEVELDB_COPY_VERSION_SET_H

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "version_edit.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "leveldb/options.h"

namespace leveldb {
namespace log {
    class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;


int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key);
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files, const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key, const Slice* largest_user_key);
class Version {
public:
    struct GetStats {
        FileMetaData* seek_file;
        int seek_file_level;
    };
    void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);
    Status Get(const ReadOptions&, const LookupKey& key, std::string* val, GetStats* stats);
    bool UpdateStats(const GetStats& stats);
    bool RecordReadSample(Slice key);
    void Ref();
    void Unref();
    void GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs);
    bool OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key);

    int PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key);

    int NumFiles(int levels) const { return files_[levels].size(); }
    std::string DebugString() const;
private:
    friend class Compaction;
    friend class VersionSet;

    class LevelFileNumIterator;

    explicit Version(VersionSet* vset)
    : vset_(vset),
    next_(this),
    prev_(this),
    refs_(0),
    file_to_compact_(nullptr),
    file_to_compact_level_(-1),
    compaction_score_(-1),
    compaction_level_(-1) {}

    Version(const Version&) = delete;
    Version& operator=(const Version&) = delete;

    ~Version();

    Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

    void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg, bool (*func)(void*, int, FileMetaData*));

    VersionSet* vset_;
    Version* next_;
    Version* prev_;
    int refs_;

    std::vector<FileMetaData*> files_[config::kNumLevels];
    FileMetaData* file_to_compact_;
    int file_to_compact_level_;

    double compaction_score_;
    int compaction_level_;
};

class VersionSet {
public:
    VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache, const InternalKeyComparator*);
    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;

    ~VersionSet();

    Status LogAndApply(VersionEdit* edit, port::Mutex* mu) EXCLUSIVE_LOCKS_REQUIRED(mu);
    Status Recover(bool* save_manifest);

    uint64_t LastSequence() const { return last_sequence_; }
    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s) {
        assert(s >= last_sequence_);
        last_sequence_ = s;
    }
    void ReuseFileNumber(uint64_t file_number) {
        if (next_file_number_ == file_number + 1) {
            next_file_number_ = file_number;
        }
    }
    void MarkFileNumberUsed(uint64_t number);
    uint64_t LogNumber() const { return log_number_; }
    uint64_t PrevLogNumber() const { return prev_log_number_; }
    uint64_t NewFileNumber() { return next_file_number_++; }
    void AddLiveFiles(std::set<uint64_t>* live);
    // Return the current manifest file number
    uint64_t ManifestFileNumber() const { return manifest_file_number_; }
    // Returns true iff some level needs a compaction.
    bool NeedsCompaction() const {
        Version* v = current_;
        return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
    }
    Version* current() const { return current_; }

    Compaction *CompactRange(int level, const InternalKey *begin, const InternalKey *end);
    Compaction* PickCompaction();
    struct LevelSummaryStorage {
        char buffer[100];
    };
    const char* LevelSummary(LevelSummaryStorage* scratch) const;
    Iterator* MakeInputIterator(Compaction* c);
    int NumLevelFiles(int level) const;

private:
    class Builder;
    friend class Compaction;
    friend class Version;
    bool ReuseManifest(const std::string& dscname, const std::string& dscbase);
    void Finalize(Version* v);
    void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest, InternalKey* largest);
    void GetRange2(const std::vector<FileMetaData*>& inputs1, const std::vector<FileMetaData*>& inputs2,
                   InternalKey* smallest, InternalKey* largest);
    void SetupOtherInputs(Compaction* c);
    Status WriteSnapshot(log::Writer* log);
    void AppendVersion(Version* v);

    Env* env_;
    const std::string dbname_;
    const Options* const options_;
    TableCache* const table_cache_;
    const InternalKeyComparator icmp_;
    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;

    WritableFile* descriptor_file_;
    log::Writer* descriptor_log_;
    Version dummy_versions_;
    Version* current_;

    std::string compact_pointer_[config::kNumLevels];
};

class Compaction {
public:
    ~Compaction();

    int level() const { return level_; }
    VersionEdit* edit() { return &edit_; }
    int num_input_files(int which) const { return inputs_[which].size(); }
    FileMetaData* input(int which, int i) const { return inputs_[which][i]; }
    uint64_t MaxOutputFileSize() const { return max_output_file_size_; }
    bool IsTrivialMove() const;
    void AddInputDeletions(VersionEdit* edit);
    bool IsBaseLevelForKey(const Slice& internal_key);
    bool ShouldStopBefore(const Slice& internal_key);
    void ReleaseInputs();
private:
    friend class Version;
    friend class VersionSet;

    Compaction(const Options* options, int level);

    int level_;
    uint64_t max_output_file_size_;
    Version* input_version_;
    VersionEdit edit_;

    std::vector<FileMetaData*> inputs_[2];
    std::vector<FileMetaData*> grandparents_;
    size_t grandparent_index_;
    bool seek_key_;
    int64_t overlapped_bytes_;

    size_t level_ptrs_[config::kNumLevels];
};
} // namespace leveldb
#endif //LEVELDB_COPY_VERSION_SET_H
