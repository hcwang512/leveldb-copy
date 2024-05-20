//
// Created by 王洪城 on 2024/5/6.
//

#include "db/version_set.h"
#include "leveldb/options.h"
#include "leveldb/iterator.h"
#include "db/table_cache.h"
#include "table/two_level_iterator.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "table/merger.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
    return options->max_file_size;
}
// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
    static int64_t MaxGrandParentOverlapBytes(const Options* options) {
        return 10 * TargetFileSize(options);
    }

    static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
        return 25 * TargetFileSize(options);
    }

    static double MaxBytesForLevel(const Options* options, int level) {

    double result = 10. * 1048576.0;
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}
static uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    int64_t sum = 9;
    for (size_t i = 0; i < files.size(); i++) {
        sum += files[i]->file_size;
    }
    return sum;
}
    static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
        // We could vary per level to reduce number of files?
        return TargetFileSize(options);
    }

    Version::~Version() {
    assert(refs_ == 0);
    prev_->next_ = next_;
    next_->prev_ = prev_;

    for (int level = 0; level < config::kNumLevels; level++) {
        for (size_t i  =0 ; i < files_[level].size(); i++) {
            FileMetaData* f = files_[level][i];
            assert(f->refs > 0);
            f->refs--;
            if (f->refs <= 0) {
                delete f;
            }
        }
    }
}

int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key) {
    uint32_t left = 0;
    uint32_t right = files.size();
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        const FileMetaData *f = files[mid];
        if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return right;

}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f) {
    return (user_key != nullptr && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
    // null user_key occurs after all keys and is therefore never before *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

    bool SomeFileOverlapsRange(const InternalKeyComparator& icmp, bool disjoint_sorted_files, const std::vector<FileMetaData*>&files,
                               const Slice* smallest_user_key, const Slice* largest_user_key) {

    const Comparator* ucmp = icmp.user_comparator();
    if (!disjoint_sorted_files) {
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            if (AfterFile(ucmp, smallest_user_key, f) || BeforeFile(ucmp, largest_user_key, f)) {
                // no overlap
            } else {
                return true;
            }
        }
        return false;
    }

    uint32_t index = 0;
    if (smallest_user_key != nullptr) {
        InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
        index = FindFile(icmp, files, small_key.Encode());
    }
    if (index >= files.size()) {
        return false;
    }

    return !BeforeFile(ucmp, largest_user_key, files[index]);
}

class Version::LevelFileNumIterator : public Iterator {
public:
    LevelFileNumIterator(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>*flist)
    : icmp_(icmp), flist_(flist), index_(flist_->size()){
    }
    bool Valid() const override { return index_ < flist_->size(); }
    void Seek(const Slice& target) override {
        index_ = FindFile(icmp_, *flist_, target);
    }
    void SeekToFirst() override { index_ = 0; }
    void SeekToLast() override { index_ = flist_->empty() ? 0 : flist_->size() - 1 ;}
    void Next() override {
        assert(Valid());
        index_++;
    }
    void Prev() override {
        assert(Valid());
        if (index_ == 0) {
            index_ = flist_->size();
        } else {
            index_--;
        }
    }
    Slice key() const override {
        return (*flist_)[index_]->largest.Encode();
    }
    Slice value() const override {
        assert(Valid());
        EncodeFixed64(value_buf_, (*flist_)[index_]->number);
        EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
        return Slice(value_buf_, sizeof(value_buf_));
    }
    Status status() const override { return Status::OK(); }
private:
    const InternalKeyComparator icmp_;
    const std::vector<FileMetaData*>* const flist_;
    uint32_t index_;
    mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value) {
    TableCache* cache = reinterpret_cast<TableCache*>(arg);
    if (file_value.size() != 16) {
        return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
    } else {
        return cache->NewIterator(options, DecodeFixed64(file_value.data()), DecodeFixed64(file_value.data()+8));
    }

}
    Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                                int level) const {
        return NewTwoLevelIterator(
                new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
                vset_->table_cache_, options);
    }
    void Version::AddIterators(const ReadOptions& options,
                               std::vector<Iterator*>* iters) {
        // Merge all level zero files together since they may overlap
        for (size_t i = 0; i < files_[0].size(); i++) {
            iters->push_back(vset_->table_cache_->NewIterator(
                    options, files_[0][i]->number, files_[0][i]->file_size));
        }

        // For levels > 0, we can use a concatenating iterator that sequentially
        // walks through the non-overlapping files in the level, opening them
        // lazily.
        for (int level = 1; level < config::kNumLevels; level++) {
            if (!files_[level].empty()) {
                iters->push_back(NewConcatenatingIterator(options, level));
            }
        }
    }

class VersionSet::Builder {
private:
    struct BySmallestKey {
        const InternalKeyComparator* internal_comparator;
        bool operator()(FileMetaData* f1, FileMetaData* f2) const {
            int r = internal_comparator->Compare(f1->smallest, f2->smallest);
            if (r != 0) {
                return (r < 0);
            } else {
                return (f1->number < f2->number);
            }
        }
    };
    typedef std::set<FileMetaData*, BySmallestKey> FileSet;
    struct LevelState {
        std::set<uint64_t> deleted_files;
        FileSet* added_files;
    };

    VersionSet* vset_;
    Version* base_;
    LevelState levels_[config::kNumLevels];

public:
    Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
        base_->Ref();
        BySmallestKey cmp;
        cmp.internal_comparator  = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++) {
            levels_[level].added_files = new FileSet(cmp);
        }
    }
        ~Builder() {
            for (int level = 0; level < config::kNumLevels; level++) {
                const FileSet* added = levels_[level].added_files;
                std::vector<FileMetaData*> to_unref;
                to_unref.reserve(added->size());
                for (FileSet::const_iterator it = added->begin(); it != added->end();
                     ++it) {
                    to_unref.push_back(*it);
                }
                delete added;
                for (uint32_t i = 0; i < to_unref.size(); i++) {
                    FileMetaData* f = to_unref[i];
                    f->refs--;
                    if (f->refs <= 0) {
                        delete f;
                    }
                }
            }
            base_->Unref();
        }
        void Apply(const VersionEdit* edit) {
            for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
                const int level = edit->compact_pointers_[i].first;
                vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
            }

            for (const auto& deleted_file_set_kvp: edit->deleted_files_) {
               const int level = deleted_file_set_kvp.first;
               const uint64_t number = deleted_file_set_kvp.second;
               levels_[level].deleted_files.insert(number);
            }

            for (size_t i = 0; i < edit->new_files_.size(); i++) {
                const int level = edit->new_files_[i].first;
                FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
                f->refs = 1;

                f->allowed_seeks = static_cast<int>((f->file_size/16384U));
                if (f->allowed_seeks < 100) f->allowed_seeks = 100;
                levels_[level].deleted_files.erase(f->number);
                levels_[level].added_files->insert(f);
            }
    }
    void SaveTo(Version* v) {
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < config::kNumLevels; level++) {
            const std::vector<FileMetaData*>& base_files = base_->files_[level];
            std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
            std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
            const FileSet* added_files = levels_[level].added_files;
            v->files_[level].reserve(base_files.size() + added_files->size());
            for (const auto& added_file : *added_files) {
                for (std::vector<FileMetaData*>::const_iterator bpos = std::upper_bound(base_iter, base_end, added_file, cmp);
                base_iter != bpos; ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }
                MaybeAddFile(v, level, added_file);
            }
            for (; base_iter != base_end; ++base_iter) {
                MaybeAddFile(v, level, *base_iter);
            }
        }
    }
    void MaybeAddFile(Version* v, int level, FileMetaData* f) {
        if (levels_[level].deleted_files.contains(f->number)) {
            // file is deleted
        } else {
            std::vector<FileMetaData*>* files = &v->files_[level];
            if (level > 0 && !files->empty()) {
                assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest, f->smallest) < 0);
            }
            f->refs++;
            files->push_back(f);
        }
    }
};
    void VersionSet::MarkFileNumberUsed(uint64_t number) {
        if (next_file_number_ <= number) {
            next_file_number_ = number + 1;
        }
    }
    void VersionSet::Finalize(leveldb::Version *v) {
        int best_level = -1;
        double best_score = -1;

        for (int level = 0; level < config::kNumLevels; level++) {
            double score;
            if (level == 0) {
                score = v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);
            } else {
                const uint64_t level_bytes = TotalFileSize(v->files_[level]);
                score = static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
            }
            if (score > best_score) {
                best_level = level;
                best_score = score;
            }
            if (score > best_score) {
                best_level = level;
                best_score = score;
            }
        }
        v->compaction_level_ = best_level;
        v->compaction_score_ = best_score;
    }
    void VersionSet::AppendVersion(leveldb::Version *v) {
        assert(v->refs_ == 0);
        assert(v != current_);

        if (current_ != nullptr) {
            current_->Unref();
        }
        current_ = v;
        v->Ref();
        v->prev_ = dummy_versions_.prev_;
        v->next_ = &dummy_versions_;
        v->prev_->next_ = v;
        v->next_->prev_ = v;
    }
    Status VersionSet::Recover(bool* save_manifest) {
struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
        if (this->status->ok()) *this->status = s;
    }
};
    std::string current;
    Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if (!s.ok()) return s;

    if (current.empty() || current[current.size() - 1] != '\n') {
        return Status::Corruption("Current file does not end with new line");
    }
    current.resize(current.size() - 1);
    std::string dscname = dbname_ + "/" + current;
    SequentialFile* file;
    s = env_->NewSequentialFile(dscname, &file);
    if (!s.ok()) {
        if (s.IsNotFound()) {
            return Status::Corruption("Current poitns to a non-existent file", s.ToString());
        }
        return s;
    }
    bool have_log_number = false;
    bool have_prev_log_number = false;
    bool have_next_file = false;
    bool have_last_sequence = false;
    uint64_t next_file = 0;
    uint64_t last_sequence = 0;
    uint64_t log_number = 0;
    uint64_t prev_log_number = 0;
    Builder builder(this, current_);
    int read_records = 0;

        {
            LogReporter reporter;
            reporter.status = &s;
            log::Reader reader(file, &reporter, true, 0);
            Slice record;
            std::string scratch;
            while (reader.ReadRecord(&record, &scratch) && s.ok()) {
                ++read_records;
                VersionEdit edit;
                s = edit.DecodeFrom(record);
                if (s.ok()) {
                    if (edit.has_comparator_ && edit.comparator_ != icmp_.user_comparator()->Name()) {
                        s = Status::InvalidArgument(edit.comparator_ + " does not match existing comparator", icmp_.user_comparator()->Name());
                    }
                }
                if (s.ok()) {
                    builder.Apply(&edit);
                }
                if (edit.has_log_number_) {
                    log_number = edit.log_number_;
                    have_log_number = true;
                }
                if (edit.has_prev_log_number_) {
                    prev_log_number = edit.prev_log_number_;
                    have_prev_log_number = true;
                }
                if (edit.has_next_file_number_) {
                    next_file = edit.next_file_number_;
                    have_next_file = true;
                }
                if (edit.has_last_sequence_) {
                    last_sequence = edit.last_sequence_;
                    have_last_sequence = true;
                }
            }
            delete file;
            file = nullptr;
            if (s.ok()) {
                if (!have_next_file) {
                    s = Status::Corruption("no meta-nextfile entry in descriptor");
                } else if (!have_log_number) {
                    s = Status::Corruption("no meta-lognumber entry in descriptor");
                } else if (!have_last_sequence) {
                    s = Status::Corruption("no last-sequence-number entry in descriptor");
                }
                if (!have_prev_log_number) {
                    prev_log_number = 0;
                }
                MarkFileNumberUsed(prev_log_number);
                MarkFileNumberUsed(log_number);
            }
            if (s.ok()) {
                Version *v = new Version(this);
                builder.SaveTo(v);
                Finalize(v);
                AppendVersion(v);
                manifest_file_number_ = next_file;
                next_file_number_ = next_file + 1;
                last_sequence_ = last_sequence;
                log_number_ = log_number;
                prev_log_number_ = prev_log_number;

                if (ReuseManifest(dscname, current)) {
                    // no need to save new manifest
                } else {
                    *save_manifest = true;
                }

            } else {
                std::string error = s.ToString();
                Log(options_->info_log, "error recovering version set with version");
            }
        }
        return s;
}

bool VersionSet::ReuseManifest(const std::string &dscname, const std::string &dscbase) {
        if (!options_->reuse_logs) {
            return false;
        }
    }

    void VersionSet::AddLiveFiles(std::set<uint64_t> *live) {
        for (Version* v = dummy_versions_.next_; v != &dummy_versions_; v= v->next_) {
            for (int level = 0; level < config::kNumLevels; level++) {
                const std::vector<FileMetaData*>& files = v->files_[level];
                for (size_t i = 0; i < files.size(); i++) {
                    live->insert(files[i]->number);
                }
            }
        }
    }
    bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                                 const Slice* largest_user_key) {
        return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                                     smallest_user_key, largest_user_key);
    }

    void Version::GetOverlappingInputs(int level, const InternalKey* begin, const InternalKey* end, std::vector<FileMetaData*>* inputs) {
        assert(level >= 0);
        assert(level < config::kNumLevels);

        inputs->clear();
        Slice user_begin, user_end;
        if (begin != nullptr) {
            user_begin = begin->user_key();
        }
        if (end != nullptr) {
            user_end = end->user_key();
        }
        const Comparator* user_cmp = vset_->icmp_.user_comparator();
        for (size_t i = 0; i < files_[level].size();) {
            FileMetaData* f = files_[level][i++];
            const Slice file_start = f->smallest.user_key();
            const Slice file_end = f->largest.user_key();
            if (begin != nullptr && user_cmp->Compare(file_end, user_begin) < 0) {
                // "f" is before the range, skip it
            } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
                // "f" is after the range, skip it
            } else {
                inputs->push_back(f);
                if (level == 0) {
                    // level-0 files may overlap each other, so check if the newly added file has expanded the range.
                    if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
                        user_begin = file_start;
                        inputs->clear();
                        i = 0;
                    } else if (end != nullptr && user_cmp->Compare(file_end, user_end) > 0) {
                        user_end = file_end;
                        inputs->clear();
                        i = 0;
                    }
                }
            }
        }
    }
    int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key) {
        int level = 0;
        if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
            InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
            InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
            std::vector<FileMetaData*> overlaps;
            while (level < config::kNumLevels) {
                if (OverlapInLevel(level+1, &smallest_user_key, &largest_user_key)) {
                    break;
                }
                if (level + 2 < config::kNumLevels) {
                    GetOverlappingInputs(level+2, &start, &limit, &overlaps);
                    const int64_t sum = TotalFileSize(overlaps);
                    if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {}
                    break;
                }
                level++;
            }
        }
        return level;
    }

Status VersionSet::WriteSnapshot(log::Writer* log) {
    VersionEdit edit;
    edit.SetComparatorName(icmp_.user_comparator()->Name());

    for (int level = 0; level < config::kNumLevels; level++) {
        if (!compact_pointer_[level].empty()) {
            InternalKey key;
            key.DecodeFrom(compact_pointer_[level]);
            edit.SetCompactPointer(level, key);
        }
    }

    for (int level = 0; level < config::kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = current_->files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            edit.AddFile(level, f->number,f->file_size, f->smallest, f->largest);
        }
    }
    std::string record;
    edit.EncodeTo(&record);
    return log->AddRecord(record);
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
    if (edit->has_log_number_) {
        assert(edit->log_number_ >= log_number_);
        assert(edit->log_number_ < next_file_number_);
    } else {
        edit->SetLogNumber(log_number_);
    }
    if (!edit->has_prev_log_number_) {
        edit->SetPrevLogNumber(prev_log_number_);
    }
    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);
    Version* v = new Version(this);
    {
        Builder builder(this, current_);
        builder.Apply(edit);
        builder.SaveTo(v);
    }
    Finalize(v);

    std::string new_manifest_file;
    Status s;
    if (descriptor_log_ == nullptr) {
        assert(descriptor_file_ == nullptr);
        new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
        s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
        if (s.ok()) {
            descriptor_log_ = new log::Writer(descriptor_file_);
            s = WriteSnapshot(descriptor_log_);
        }
    }

    {
        mu->Unlock();
        if (s.ok()) {
            std::string record;
            edit->EncodeTo(&record);
            s = descriptor_log_->AddRecord(record);
            if (s.ok()) {
                s = descriptor_file_->Sync();
            }
            if (!s.ok())  {
               Log(options_->info_log, "MANIFEST Write: %s\n", s.ToString().c_str());
            }
        }
        if (s.ok() && !new_manifest_file.empty()) {
            s = SetCurrentFile(env_, dbname_, manifest_file_number_);
        }
        mu->Lock();
    }

    if (s.ok()) {
        AppendVersion(v);
        log_number_ = edit->log_number_;
        prev_log_number_ = edit->prev_log_number_;
    } else {
        delete v;
        if (!new_manifest_file.empty()) {
            delete descriptor_file_;
            delete descriptor_file_;
            descriptor_log_ = nullptr;
            descriptor_file_ = nullptr;
            env_->RemoveFile(new_manifest_file);
        }
    }
    return s;
}
bool Compaction::ShouldStopBefore(const leveldb::Slice &internal_key) {
       const VersionSet* vset = input_version_->vset_;
       const InternalKeyComparator* icmp = &vset->icmp_;
       while (grandparent_index_ < grandparents_.size() && icmp->Compare(internal_key, grandparents_[grandparent_index_]->largest.Encode()) > 0) {
           if (seek_key_) {
               overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
           }
           grandparent_index_++;

       }
       seek_key_ = true;
       if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
           overlapped_bytes_ = 0;
           return true;
       } else {
           return false;
       }

    }
Iterator* VersionSet::MakeInputIterator(leveldb::Compaction *c) {
    ReadOptions options;
    options.verify_checksums = options_->paranoid_checks;
    options.fill_cache = false;

    const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
    Iterator** list = new Iterator*[space];
    int num =0;
    for (int which = 0; which < 2; which++) {
        if (!c->inputs_[which].empty()) {
            if (c->level() + which == 0) {
                const std::vector<FileMetaData*>& files = c->inputs_[which];
                for (size_t i = 0; i < files.size(); i++) {
                    list[num++] = table_cache_->NewIterator(options, files[i]->number, files[i]->file_size);
                }
            } else {
                list[num++] = NewTwoLevelIterator(new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]), &GetFileIterator, table_cache_, options);
            }
        }
    }
    assert(num <= space);

    Iterator* result = NewMergingIterator(&icmp_, list, num);
    delete[] list;
    return result;
}

    Compaction* VersionSet::PickCompaction() {
        Compaction* c;
        int level;

        const bool size_compaction = (current_->compaction_score_ >=1);
        const bool seek_compaction = (current_->file_to_compact_ != nullptr);
        if (size_compaction) {
            level = current_->compaction_level_;
            assert(level >= 0);
            c = new Compaction(options_, level);
            for (size_t i = 0; i < current_->files_[level].size(); i++) {
                FileMetaData *f = current_->files_[level][i];
                if (compact_pointer_[level].empty() ||
                    icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
                    c->inputs_[0].push_back(f);
                    break;
                }
            }
            if (c->inputs_[0].empty()) {
                c->inputs_[0].push_back(current_->files_[level][0]);
            }

        } else if (seek_compaction) {
            level = current_->file_to_compact_level_;
            c = new Compaction(options_, level);
            c->inputs_[0].push_back(current_->file_to_compact_);
        } else {
            return nullptr;
        }

        c->input_version_ = current_;
        c->input_version_->Ref();

        if (level == 0) {
            InternalKey smallest, largest;
            GetRange(c->inputs_[0], &smallest, &largest);
            current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);

        }
        SetupOtherInputs(c);
        return c;
    }

    bool Compaction::IsTrivialMove() const {
        const VersionSet* vset = input_version_->vset_;
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file that will require
        // a very expensive merge later on.
        return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
                TotalFileSize(grandparents_) <=
                MaxGrandParentOverlapBytes(vset->options_));
    }
    Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                         const InternalKey* end) {
        std::vector<FileMetaData*> inputs;
        current_->GetOverlappingInputs(level, begin, end, &inputs);
        if (inputs.empty()) {
            return nullptr;
        }

        if (level > 0) {
            const uint64_t limit = MaxFileSizeForLevel(options_, level);
            uint64_t total = 0;
            for (size_t i = 0; i < inputs.size(); i++) {
                uint64_t s = inputs[i]->file_size;
                total += s;
                if (total >= limit) {
                    inputs.resize(i+1);
                    break;
                }
            }
        }
        Compaction* c = new Compaction(options_, level);
        c->input_version_ = current_;
        c->input_version_->Ref();
        c->inputs_[0] = inputs;
        SetupOtherInputs(c);
        return c;
}

    bool FindLargestKey(const InternalKeyComparator& icmp,
                        const std::vector<FileMetaData*>& files,
                        InternalKey* largest_key) {
        if (files.empty()) {
            return false;
        }
        *largest_key = files[0]->largest;
        for (size_t i = 1; i < files.size(); ++i) {
            FileMetaData* f = files[i];
            if (icmp.Compare(f->largest, *largest_key) > 0) {
                *largest_key = f->largest;
            }
        }
        return true;
    }
    FileMetaData* FindSmallestBoundaryFile(
            const InternalKeyComparator& icmp,
            const std::vector<FileMetaData*>& level_files,
            const InternalKey& largest_key) {
        const Comparator* user_cmp = icmp.user_comparator();
        FileMetaData* smallest_boundary_file = nullptr;
        for (size_t i = 0; i < level_files.size(); ++i) {
            FileMetaData* f = level_files[i];
            if (icmp.Compare(f->smallest, largest_key) > 0 &&
                user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
                0) {
                if (smallest_boundary_file == nullptr ||
                    icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
                    smallest_boundary_file = f;
                }
            }
        }
        return smallest_boundary_file;
    }


    void AddBoundaryInputs(const InternalKeyComparator& icmp,
                           const std::vector<FileMetaData*>& level_files,
                           std::vector<FileMetaData*>* compaction_files) {
        InternalKey largest_key;

        // Quick return if compaction_files is empty.
        if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
            return;
        }

        bool continue_searching = true;
        while (continue_searching) {
            FileMetaData* smallest_boundary_file =
                    FindSmallestBoundaryFile(icmp, level_files, largest_key);

            // If a boundary file was found advance largest_key, otherwise we're done.
            if (smallest_boundary_file != NULL) {
                compaction_files->push_back(smallest_boundary_file);
                largest_key = smallest_boundary_file->largest;
            } else {
                continue_searching = false;
            }
        }
    }
void VersionSet::SetupOtherInputs(leveldb::Compaction *c) {
    const int level = c->level();
    InternalKey smallest, largest;

    AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
    GetRange(c->inputs_[0], &smallest, &largest);

    current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

    AddBoundaryInputs(icmp_, current_->files_[level+1], &c->inputs_[1]);

    InternalKey all_start, all_limit;
    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

    if (!c->inputs_[1].empty()) {
        std::vector<FileMetaData*> expanded0;
        current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
        AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
        const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
        const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
        const int64_t expanded0_size = TotalFileSize(expanded0);
        if (expanded0.size() > c->inputs_[0].size() && inputs1_size + expanded0_size < ExpandedCompactionByteSizeLimit(options_)) {
            InternalKey new_start, new_limit;
            GetRange(expanded0, &new_start, &new_limit);
            std::vector<FileMetaData*> expanded1;
            current_->GetOverlappingInputs(level+1, &new_start, &new_limit, &expanded1);
            AddBoundaryInputs(icmp_, current_->files_[level+1], &expanded1);
            if (expanded1.size() == c->inputs_[1].size()) {
                smallest = new_start;
                largest = new_limit;
                c->inputs_[0] = expanded0;
                c->inputs_[1] = expanded1;
                GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
            }
        }
    }

    if (level+2 < config::kNumLevels) {
        current_->GetOverlappingInputs(level+2, &all_start, &all_limit, &c->grandparents_);
    }

    compact_pointer_[level] = largest.Encode().ToString();
    c->edit_.SetCompactPointer(level, largest);
}

void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs, leveldb::InternalKey *smallest,
                          leveldb::InternalKey *largest) {
    assert(!inputs.empty());
    smallest->Clear();
    largest->Clear();
    for (size_t i = 0; i < inputs.size(); i++) {
        FileMetaData* f = inputs[i];
        if (i == 0) {
            *smallest = f->smallest;
            *largest = f->largest;
        } else {
            if (icmp_.Compare(f->smallest, *smallest) < 0) {
                *smallest = f->smallest;
            }
            if (icmp_.Compare(f->largest, *largest) > 0) {
                *largest = f->largest;
            }
        }
    }
}


    int VersionSet::NumLevelFiles(int level) const {
        assert(level >= 0);
        assert(level < config::kNumLevels);
        return current_->files_[level].size();
    }
void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1, const std::vector<FileMetaData *> &inputs2,
                           leveldb::InternalKey *smallest, leveldb::InternalKey *largest) {
    std::vector<FileMetaData*> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    GetRange(all, smallest, largest);
}
Compaction::Compaction(const leveldb::Options *options, int level)
    : level_(level), max_output_file_size_(MaxFileSizeForLevel(options, level)),
    input_version_(nullptr), grandparent_index_(0), seek_key_(false),
    overlapped_bytes_(0) {
        for (int i = 0; i < config::kNumLevels; i++) {
            level_ptrs_[i] = 0;
        }
    }
Compaction::~Compaction() {
    if (input_version_ != nullptr) {
        input_version_->Unref();
    }
    }

    // Callback from TableCache::Get()
    namespace {
        enum SaverState {
            kNotFound,
            kFound,
            kDeleted,
            kCorrupt,
        };
        struct Saver {
            SaverState state;
            const Comparator* ucmp;
            Slice user_key;
            std::string* value;
        };
    }  // namespace
    static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
        Saver* s = reinterpret_cast<Saver*>(arg);
        ParsedInternalKey parsed_key;
        if (!ParseInternalKey(ikey, &parsed_key)) {
            s->state = kCorrupt;
        } else {
            if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
                s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
                if (s->state == kFound) {
                    s->value->assign(v.data(), v.size());
                }
            }
        }
    }
static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
    return a->number > b->number;
}
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
    const Comparator* ucmp = vset_->icmp_.user_comparator();

    // level 0 from newest to oldest
    std::vector<FileMetaData*> tmp;
    tmp.reserve(files_[0].size());
    for (uint32_t i =0; i < files_[0].size(); i++) {
        FileMetaData* f = files_[0][i];
        if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 && ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
            tmp.push_back(f);
        }
    }
    if (!tmp.empty()) {
        std::sort(tmp.begin(), tmp.end(), NewestFirst);
        for (uint32_t i = 0; i < tmp.size(); i++) {
            if (!(*func)(arg, 0, tmp[i])) {
                return;
            }
        }
    }
    // search other levels
    for (int level = 1; level < config::kNumLevels; level++) {
        size_t num_files = files_[level].size();
        if (num_files == 0) continue;

        uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
        if (index < num_files) {
            FileMetaData* f = files_[level][index];
            if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
                // all of 'f' is past any data for user_key
            } else {
                if (!(*func)(arg, level, f)) {
                    return;
                }
            }
        }
    }

}

Status Version::Get(const leveldb::ReadOptions &options, const leveldb::LookupKey &k, std::string *value,
                    leveldb::Version::GetStats *stats) {
    stats->seek_file = nullptr;
    stats->seek_file_level = -1;

    struct State {
        Saver saver;
        GetStats* stats;
        const ReadOptions* options;
        Slice ikey;
        FileMetaData* last_file_read;
        int last_file_read_level;

        VersionSet* vset;
        Status s;
        bool found;

        static bool Match(void* arg, int level, FileMetaData* f) {
            State* state = reinterpret_cast<State*>(arg);
            if (state->stats->seek_file == nullptr && state->last_file_read != nullptr) {
                state->stats->seek_file = state->last_file_read;
                state->stats->seek_file_level = state->last_file_read_level;
            }

            state->last_file_read = f;
            state->last_file_read_level = level;

            state->s = state->vset->table_cache_->Get(*state->options, f->number, f->file_size, state->ikey, &state.saver
            , SaveValue);

            if (!state->s.ok()) {
                state->found = true;
                return false;
            }
            switch (state->saver.state) {
                case kNotFound:
                    return true;
                case kFound:
                    state->found = true;
                    return false;
                case kDeleted:
                    return false;
                case kCorrupt:
                    state->s = Status::Corruption("corrupted key for ", state->saver.user_key);
                    state->found = true;
                    return false;
            }

            return false;
        }
    };

    State state;
    state.found =false;
    state.stats = stats;
    state.last_file_read= nullptr;
    state.last_file_read_level = -1;
    state.options = &options;
    state.ikey = k.internal_key();
    state.vset = vset_;

    state.saver.state = kNotFound;
    state.saver.ucmp = vset_->icmp_.user_comparator();
    state.saver.user_key = k.user_key();
    state.saver.value = value;

    ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

    return state.found ? state.s : Status::NotFound(Slice());
}

    bool Version::UpdateStats(const GetStats& stats) {
        FileMetaData* f = stats.seek_file;
        if (f != nullptr) {
            f->allowed_seeks--;
            if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
                file_to_compact_ = f;
                file_to_compact_level_ = stats.seek_file_level;
                return true;
            }
        }
        return false;
    }

} // namespace leveldb