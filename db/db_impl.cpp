//
// Created by 王洪城 on 2024/4/29.
//

#include "db/db_impl.h"
#include "db/version_set.h"
#include "db/dbformat.h"
#include "util/logging.h"
#include "db/filename.h"
#include "leveldb/cache.h"
#include "db/table_cache.h"
#include "leveldb/write_batch.h"
#include "table/memtable.h"
#include "leveldb/status.h"
#include "write_batch_internal.h"
#include "db/builder.h"
#include "util/mutexlock.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"

namespace leveldb {
const int kNumNonTableCacheFiles = 10;

struct DBImpl::Writer {
    explicit Writer(port::Mutex* mu) : batch(nullptr), sync(false), done(false), cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;
};

struct DBImpl::CompactionState {
    struct Output {
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };
    Output* current_output() { return &outputs[outputs.size() -1]; }

    explicit CompactionState(Compaction* c)
        : compaction(c), smallest_snapshot(0),
        outfile(nullptr), builder(nullptr),, total_bytes(0){}

    Compaction* const compaction;
    SequenceNumber smallest_snapshot;
    std::vector<Output> outputs;

    WritableFile* outfile;
    TableBuilder* builder;
    uint64_t total_bytes;
};

template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
    Options result = src;
    result.comparator = icmp;
    result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
    ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1<< 30);
    ClipToRange(&result.block_size, 1<< 10, 4 << 20);
    if (result.info_log == nullptr) {
        src.env->CreateDir(dbname);
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            result.info_log = nullptr;
        }
    }
    if (result.block_cache == nullptr) {
        result.block_cache = NewLRUCache(8 << 20);
    }
    return result;
}
static int TableCacheSize(const Options& sanitized_options) {
    // Reserve ten files or so for other uses and give the rest to TableCache.
    return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}


    DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_)) {}

DBImpl::~DBImpl() {
    mutex_.Lock();
    shutting_down_.store(true, std::memory_order_release);
    while (background_compaction_scheduled_) {
        background_work_finished_signal_.Wait();
    }
    mutex_.Unlock();
    if (db_lock_ != nullptr) {
        env_->UnlockFile(db_lock_);
    }
    delete versions_;
    if (mem_!= nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    if (owns_cache_) {
        delete options_.block_cache;
    }
}

Status DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) {
        return s;
    }
    {
        log::Writer log(file);
        std::string record;
        new_db.EncodeTo(&record);
        s = log.AddRecord(record);
        if (s.ok()) {
            s = file->Sync();
        }
        if (s.ok()) {
            s = file->Close();
        }
    }
    delete file;
    if (s.ok()) {
        s = SetCurrentFile(env_, dbname_, 1);
    } else {
        env_->RemoveFile(manifest);
    }
    return s;
}
void DBImpl::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_.paranoid_checks) {

    } else {
        Log(options_.info_log, "ignoring error %s", s->ToString().c_str());
        *s = Status::OK();
    }
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log, bool *save_manifest, VersionEdit *edit,
               SequenceNumber* max_sequence) {
struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
        Log(info_log, "%s%s: dropping %d bytes", (this->status == nullptr ? "(ignoreing error) " : ""), fname,
        static_cast<int>(bytes), s.ToString().c_str());
        if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
};

    mutex_.AssertHeld();

    std::string fname = LogFileName(dbname_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : nullptr);

    log::Reader reader(file, &reporter, true, 0);
    Log(options_.info_log, "recovering log #%llu", (unsigned long long)log_number);

    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;
    MemTable* mem = nullptr;

    while (reader.ReadRecord(&record, &scratch) && status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem == nullptr) {
            mem = new MemTable(internal_comparator_);
            mem->Ref();
        }
        status = WriteBatchInternal::InsertInto(&batch, mem);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) + WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }

        if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
            compactions++;
            *save_manifest = true;
            status = WriteLevel0Table(mem, edit, nullptr);
        }
        mem->Unref();
    }
    return status;
}

    Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                    Version* base) {
        mutex_.AssertHeld();
        const uint64_t start_micros = env_->NowMicros();
        FileMetaData meta;
        meta.number = versions_->NewFileNumber();
        pending_outputs_.insert(meta.number);
        Iterator* iter = mem->NewIterator();
        Log(options_.info_log, "Level-0 table #%llu: started", (unsigned long long)meta.number);

        Status s;
        {
            mutex_.Unlock();
            s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
            mutex_.Lock();
        }

        Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s", (unsigned long long)meta.number,
            (unsigned long long)meta.file_size, s.ToString().c_str());

        delete iter;
        pending_outputs_.erase(meta.number);

        int level = 0;
        if (s.ok() && meta.file_size > 0) {
            const Slice min_user_key = meta.smallest.user_key();
            const Slice max_user_key = meta.largest.user_key();
            if (base != nullptr) {
                level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
            }
            edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
        }

        CompactionStats stats;
        stats.micros = env_->NowMicros() - start_micros;
        stats.bytes_written = meta.file_size;
        stats_[level].Add(stats);
        return s;
    }
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {
    mutex_.AssertHeld();
    env_->CreateDir(dbname_);
    assert(db_lock_ == nullptr);
    Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
        return s;
    }
    if (!env_->FileExists(CurrentFileName(dbname_))) {
        if (options_.create_if_missing) {
            Log(options_.info_log, "Creating DB %s since it was missing.", dbname_.c_str());
            s = NewDB();
            if (!s.ok()) {
                return s;
            }
        } else {
            return Status::InvalidArgument(dbname_, "does not exist");
        }
    } else {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(dbname_, "exists (error if exists)");
        }
    }

    s = versions_->Recover(save_manifest);
    if (!s.ok()) {
        return s;
    }
    SequenceNumber  max_sequence(0);

    const uint64_t min_log = versions_->LogNumber();
    const uint64_t prev_Log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if (!s.ok()) {
        return s;
    }
    std::set<uint64_t > expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t > logs;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            expected.erase(number);
            if (type == kLogFile && ((number >= min_log) || (number == prev_Log))) {
                logs.push_back(number);
            }
        }
    }
    if (!expected.empty()) {
        char buf[50];
        std::snprintf(buf, sizeof(buf), "%d missing files;", static_cast<int>(expected.size()));
        return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
    }

    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) {
        s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit, &max_sequence);
        if (!s.ok()) {
            return s;
        }
        versions_->MarkFileNumberUsed(logs[i]);
    }
    if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
    }
    return Status::OK();
}


void DBImpl::BGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}


void DBImpl::RecordBackgroundError(const Status& s) {
    mutex_.AssertHeld();
    if (bg_error_.ok()) {
        bg_error_ = s;
        background_work_finished_signal_.SignalAll();
    }
}
void DBImpl::CompactMemTable() {
    mutex_.AssertHeld();
    assert(imm_ != nullptr);

    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    Status s = WriteLevel0Table(imm_, &edit, base);
    base->Unref();
    if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
        s = Status::IOError("Deleting DB during memtable compaction");
    }
    if (s.ok()) {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(logfile_number_);
        s = versions_->LogAndApply(&edit, &mutex_);
    }

    if (s.ok()) {
        imm_->Unref();
        imm_ = nullptr;
        has_imm_.store(false, std::memory_order_release);
        RemoveObsoleteFiles();
    } else {
        RecordBackgroundError(s);
    }
}
void DBImpl::BackgroundCompaction() {
    mutex_.AssertHeld();
    if (imm_ != nullptr) {
        CompactMemTable();
        return;
    }

    Compaction* c;
    bool is_manual = (manual_compaction_ != nullptr);
    InternalKey manual_end;
    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        c = versions_->CompactRange(m->level, m->begin, m->end);
        m->done = (c == nullptr);
        if (c != nullptr) {
            manual_end = c->input(0, c->num_input_files(0)-1)->largest;
        }
    } else {
        c = versions_->PickCompaction();
    }

    Status status;
    if (c == nullptr) {
        // noting to do
    } else if (!is_manual && c->IsTrivialMove()) {
        FileMetaData* f = c->input(0, 0);
        c->edit()->RemoveFile(c->level(), f->number);
        c->edit()->AddFile(c->level()+1, f->number, f->file_size, f->smallest, f->largest);
        status = versions_->LogAndApply(c->edit(), &mutex_);
        if (!status.ok()) {}
        RecordBackgroundError(status);
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
            static_cast<unsigned long long>(f->number), c->level() + 1,
            static_cast<unsigned long long>(f->file_size),
            status.ToString().c_str(), versions_->LevelSummary(&tmp));
    } else {
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        CleanupCompaction(compact);
        c->ReleaseInputs();
        RemoveObsoleteFiles();
    }
    delete c;

    if (status.ok()) {
        // Done
    } else if (shutting_down_.load(std::memory_order_acquire)) {
        // Ignore compaction errors found during shutting down
    } else {
        Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
    }

    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        if (!status.ok()) {
            m->done = true;
        }
        if (!m->done) {
            // We only compacted part of the requested range.  Update *m
            // to the range that is left to be compacted.
            m->tmp_storage = manual_end;
            m->begin = &m->tmp_storage;
        }
        manual_compaction_ = nullptr;
    }
}

    Status DBImpl::DoCompactionWork(CompactionState *compact) {
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;
    if (snapshots_.empty()) {
        compact->smallest_snapshot = versions_->LastSequence();
    } else {
        compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
    }

    Iterator* input = versions_->MakeInputIterator(compact->compaction);

    mutex_.Unlock();
    input->SeekToFirst();
    Status status;

    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
    while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
        if (has_imm_.load(std::memory_order_relaxed)) {
            const uint64_t imm_start = env_->NowMicros();
            mutex_.Lock();
            if (imm_ != nullptr) {
                CompactMemTable();
                background_work_finished_signal_.SignalAll();
            }
            mutex_.Unlock();
            imm_micros += (env_->NowMicros() - imm_start);
        }

        Slice key = input->key();
        if (compact->compaction->ShouldStopBefore(key) && compact->builder != nullptr) {
            status = FinishCompactionOutputFile(compact, input);
            if (!status.ok()) {
                break;
            }
        }

        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key || user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }
            if (last_sequence_for_key <= compact->smallest_snapshot) {
                drop = true;
            } else if (ikey.type == kTypeDeletion && ikey.sequece <= compact->smallest_snapshot &&
                compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                drop = true;
            }
            last_sequence_for_key = ikey.sequece;
        }
        if (!drop) {
            // Open output file if necessary
            if (compact->builder == nullptr) {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) {
                    break;
                }
            }
            if (compact->builder->NumEntries() == 0) {
                compact->current_output()->smallest.DecodeFrom(key);
            }
            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, input->value());

            // Close output file if it is big enough
            if (compact->builder->FileSize() >=
                compact->compaction->MaxOutputFileSize()) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }
        }

        input->Next();
    }
    if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
        status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && compact->builder != nullptr) {
        status = FinishCompactionOutputFile(compact, input);
    }
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    input = nullptr;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        stats.bytes_written += compact->outputs[i].file_size;
    }

    mutex_.Lock();
    stats_[compact->compaction->level() + 1].Add(stats);

    if (status.ok()) {
        status = InstallCompactionResults(compact);
    }
    if (!status.ok()) {
        RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
    return status;
}

    Status DBImpl::InstallCompactionResults(CompactionState* compact) {
        mutex_.AssertHeld();
        Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
            compact->compaction->num_input_files(0), compact->compaction->level(),
            compact->compaction->num_input_files(1), compact->compaction->level() + 1,
            static_cast<long long>(compact->total_bytes));

        // Add compaction outputs
        compact->compaction->AddInputDeletions(compact->compaction->edit());
        const int level = compact->compaction->level();
        for (size_t i = 0; i < compact->outputs.size(); i++) {
            const CompactionState::Output& out = compact->outputs[i];
            compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                                 out.smallest, out.largest);
        }
        return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
    }

    Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t file_number;
        {
            mutex_.Lock();
            file_number = versions_->NewFileNumber();
            pending_outputs_.insert(file_number);
            CompactionState::Output out;
            out.number = file_number;
            out.smallest.Clear();
            out.largest.Clear();
            compact->outputs.push_back(out);
            mutex_.Unlock();
        }

        // Make the output file
        std::string fname = TableFileName(dbname_, file_number);
        Status s = env_->NewWritableFile(fname, &compact->outfile);
        if (s.ok()) {
            compact->builder = new TableBuilder(options_, compact->outfile);
        }
        return s;
    }

    Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                              Iterator* input) {
        assert(compact != nullptr);
        assert(compact->outfile != nullptr);
        assert(compact->builder != nullptr);

        const uint64_t output_number = compact->current_output()->number;
        assert(output_number != 0);

        // Check for iterator errors
        Status s = input->status();
        const uint64_t current_entries = compact->builder->NumEntries();
        if (s.ok()) {
            s = compact->builder->Finish();
        } else {
            compact->builder->Abandon();
        }
        const uint64_t current_bytes = compact->builder->FileSize();
        compact->current_output()->file_size = current_bytes;
        compact->total_bytes += current_bytes;
        delete compact->builder;
        compact->builder = nullptr;

        // Finish and check for file errors
        if (s.ok()) {
            s = compact->outfile->Sync();
        }
        if (s.ok()) {
            s = compact->outfile->Close();
        }
        delete compact->outfile;
        compact->outfile = nullptr;

        if (s.ok() && current_entries > 0) {
            // Verify that the table is usable
            Iterator* iter =
                    table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
            s = iter->status();
            delete iter;
            if (s.ok()) {
                Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
                    (unsigned long long)output_number, compact->compaction->level(),
                    (unsigned long long)current_entries,
                    (unsigned long long)current_bytes);
            }
        }
        return s;
    }
void DBImpl::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(background_compaction_scheduled_);
    if (shutting_down_.load(std::memory_order_acquire)) {
        // no more background work when shutting down
    } else if (!bg_error_.ok()) {
        // no more background work after a background error
    } else {
        BackgroundCompaction();
    }
    background_compaction_scheduled_ = false;
    MaybeScheduleCompaction();
    background_work_finished_signal_.SignalAll();
}
void DBImpl::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    if (background_compaction_scheduled_) {
        // already scheduled
    } else if (shutting_down_.load(std::memory_order_acquire)) {
        // db is being deleted
    } else if (!bg_error_.ok()) {
        // already got an error, no more changes
    } else if (imm_ == nullptr && manual_compaction_ == nullptr && !versions_->NeedsCompaction()) {
        // no work to be done
    } else {
        background_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }

}
void DBImpl::RemoveObsoleteFiles() {
    mutex_.AssertHeld();

    if (!bg_error_.ok()) {
        return;
    }
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames);
    uint64_t number;
    FileType type;
    std::vector<std::string> files_to_delete;
    for (std::string& filename: filenames) {
        if (ParseFileName(filename, &number, &type)) {
            bool keep = true;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions_->LogNumber()) || (number == versions_->PrevLogNumber()));
                    break;
                case kDescriptorFile:
                    keep = (number >= versions_->ManifestFileNumber());
                    break;
                case kTableFile:
                    keep = (live.find(number) != live.end());
                    break;
                case kTempFile:
                    keep = (live.find(number) != live.end());
                    break;
                case kCurrentFile:
                case kDBLockFile:
                case kInfoLogFile:
                    keep = true;
                    break;
            }
            if (!keep) {
                files_to_delete.push_back(std::move(filename));
                if (type == kTableFile) {
                    table_cache_->Evict(number);
                }
            }
        }
    }
    mutex_.Unlock();
    for (const std::string& filename : files_to_delete) {
        env_->RemoveFile(dbname_ + "/" + filename);
    }
    mutex_.Lock();
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
    *dbptr = nullptr;
    DBImpl* impl = new DBImpl(options, dbname);
    impl->mutex_.Lock();
    VersionEdit edit;
    bool save_manifest = false;
    Status s = impl->Recover(&edit, &save_manifest);
    if (s.ok() && impl->mem_ == nullptr) {
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        edit.SetLogNumber(new_log_number);
        WritableFile *lfile;
        s = options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile);
        if (s.ok()) {
            edit.SetLogNumber(new_log_number);
            impl->logfile_ = lfile;
            impl->logfile_number_ = new_log_number;
            impl->log_ = new log::Writer(lfile);
            impl->mem_ = new MemTable(impl->internal_comparator_);
            impl->mem_->Ref();
        }
    }
    if (s.ok() && save_manifest) {
        edit.SetPrevLogNumber(0);
        edit.SetLogNumber(impl->logfile_number_);
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }
    if (s.ok()) {
        impl->RemoveObsoleteFiles();
        impl->MaybeScheduleCompaction();
    }
    impl->mutex_.Unlock();
    if (s.ok()) {
        assert(impl->mem_ != nullptr);
        *dbptr = impl;
    } else {
        delete impl;
    }
    return s;
}


Status DBImpl::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();

    assert(!writers_.empty());
    bool allow_delay = !force;
    Status s;
    while (true) {
        if (!bg_error_.ok()) {
            s = bg_error_;
            break;
        } else if (allow_delay && versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
            mutex_.Unlock();
            env_->SleepForMicroseconds(1000);
            allow_delay = false;
            mutex_.Lock();
        } else if (!force && (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
            // threr is room in current memtable
            break;
        } else if (imm_ != nullptr) {
            // wait for previ
            background_work_finished_signal_.Wait();
        } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
            // too many level 0 files
            background_work_finished_signal_.Wait();
        } else {
            uint64_t new_log_number = versions_->NewFileNumber();
            WritableFile* lfile = nullptr;
            s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
            if (!s.ok()) {
                versions_->ReuseFileNumber(new_log_number);
                break;
            }
            delete log_;
            s = logfile_->Close();
            if (!s.ok()) {
                RecordBackgroundError(s);
            }
            delete logfile_;
            logfile_ = lfile;
            logfile_number_ = new_log_number;
            log_ = new log::Writer(lfile);
            imm_ = mem_;
            has_imm_.store(true, std::memory_order_release);
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
            force = false;
            MaybeScheduleCompaction();
        }
    }
    return s;
}


WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    size_t max_size = 1 << 20;
    if (size <= (128 << 10)) {
        max_size = size + (128 << 10);
    }

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;
    for (; iter != writers_.end(); ++iter) {
        Writer *w = *iter;
        if (w->sync && !first->sync) {
            break;
        }

        if (w->batch != nullptr) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                break;
            }

            if (result == first->batch) {
                result = tmp_batch_;
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
    Writer w(&mutex_);
    w.batch = updates;
    w.sync = options.sync;
    w.done = false;

    MutexLock l(&mutex_);
    writers_.push_back(&w);

    while (!w.done && &w != writers_.front()) {
        w.cv.Wait();
    }
    if (w.done) {
        return w.status;
    }

    Status status = MakeRoomForWrite(updates == nullptr);
    uint64_t last_sequence  = versions_->LastSequence();
    Writer* last_writer = &w;
    if (status.ok() && updates != nullptr) {
        WriteBatch* write_batch = BuildBatchGroup(&last_writer);
        WriteBatchInternal::SetSequence(write_batch, last_sequence+1);
        last_sequence += WriteBatchInternal::Count(write_batch);

        {
            mutex_.Unlock();
            status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
            bool sync_error = false;
            if (status.ok() && options.sync) {
                status = logfile_->Sync();
                if (!status.ok()) {
                    sync_error = true;
                }
            }
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(write_batch, mem_);
            }
            mutex_.Lock();
            if (sync_error) {
                RecordBackgroundError(status);
            }
        }
        if (write_batch == tmp_batch_) tmp_batch_->Clear();
        versions_->SetLastSequence(last_sequence);
    }

    while (true) {
        Writer* ready = writers_.front();
        writers_.pop_front();
        if (ready != &w) {
            ready->status = status;
            ready->done = true;
            ready->cv.Signal();
        }
        if (ready == last_writer) break;
    }
    if (!writers_.empty()) {
        writers_.front()->cv.Signal();
    }
    return status;
}

DB::~DB() = default;

Status DB::Put(const leveldb::WriteOptions &opt, const leveldb::Slice &key, const leveldb::Slice &value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

Status DestroyDB(const std::string& dbname, const Options& options) {
    Env* env = options.env;
    std::vector<std::string> filenames;
    Status result = env->GetChildren(dbname, &filenames);
    if (!result.ok()) {
        return Status::OK();
    }
    FileLock* lock;
    const std::string lockname = LockFileName(dbname);
    result = env->LockFile(lockname, &lock);
    if (result.ok()) {
        uint64_t number;
        FileType type;
        for (size_t i = 0; i < filenames.size(); i++) {
            if (ParseFileName(filenames[i], &number, &type) && type != kDBLockFile) {
                Status del = env->RemoveFile(dbname + "/" + filenames[i]);
                if (result.ok() && !del.ok()) {
                    result = del;
                }
            }
        }
        env->UnlockFile(lock);
        env->RemoveFile(lockname);
        env->RemoveDir(dbname);
    }
    return result;
}

Status DBImpl::Put(const leveldb::WriteOptions &o, const leveldb::Slice &key, const leveldb::Slice &value) {
    return DB::Put(o, key, value);
}
Status DBImpl::Delete(const leveldb::WriteOptions &o, const leveldb::Slice &key) {
    return DB::Delete(o, key);
}
Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    Status s;
    MutexLock l(&mutex_);
    SequenceNumber  snapshot;
    if (options.snapshot != nullptr) {
        snapshot = static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
    } else {
        snapshot = versions_->LastSequence();
    }

    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    mem->Ref();
    if (imm != nullptr) imm->Ref();
    current->Ref();
    bool have_stat_update = false;
    Version::GetStats stats;
    {
        mutex_.Unlock();
        LookupKey lkey(key, snapshot);
        if (mem->Get(lkey, value, &s)) {
            // done
        } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
            // done
        } else {
            s = current->Get(options, lkey, value, &stats);
            have_stat_update = true;
        }
        mutex_.Lock();
    }

    if (have_stat_update && current->UpdateStats(stats)) {
        MaybeScheduleCompaction();
    }
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    current->Unref();
    return s;

}
    namespace {

        struct IterState {
            port::Mutex* const mu;
            Version* const version GUARDED_BY(mu);
            MemTable* const mem GUARDED_BY(mu);
            MemTable* const imm GUARDED_BY(mu);

            IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
                    : mu(mutex), version(version), mem(mem), imm(imm) {}
        };

        static void CleanupIteratorState(void* arg1, void* arg2) {
            IterState* state = reinterpret_cast<IterState*>(arg1);
            state->mu->Lock();
            state->mem->Unref();
            if (state->imm != nullptr) state->imm->Unref();
            state->version->Unref();
            state->mu->Unlock();
            delete state;
        }

    }  // anonymous namespace


Iterator* DBImpl::NewIntenralIterator(const leveldb::ReadOptions &options, leveldb::SequenceNumber *latest_snapshot,
                                      uint32_t *seed) {
    mutex_.Lock();
    *latest_snapshot = versions_->LastSequence();

    std::vector<Iterator*> list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();
    if (imm_ != nullptr) {
        list.push_back(imm_->NewIterator());
        imm_->Ref();
    }
    versions_->current()->AddIterators(options, &list);
    Iterator* internal_iter = NewMergingIterator(&internal_comparator_, &list[0], list.size());
    versions_->current()->Ref();

    IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
    *seed = ++seed_;
    mutex_.Unlock();
    return internal_iter;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    SequenceNumber  latest_snapshot;
    uint32_t seed;
    Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);

}
} // namespace leveldb