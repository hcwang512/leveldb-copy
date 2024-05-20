//
// Created by 王洪城 on 2024/4/30.
//

#include <string>
#include <unistd.h>
#include <dirent.h>
#include <utility>
#include <cstdlib>
#include <atomic>
#include <cstdio>
#include <set>
#include <queue>
#include <fcntl.h>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include "leveldb/status.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "util/posix_logger.h"

namespace leveldb {
int g_open_read_only_file_limit = -1;

constexpr const int kDefaultMmapLimit = (sizeof(void*) >= 8) ? 1000 : 0;

#if defined(HAVE_O_CLOEXEC)
constexpr const int kOpenBaseFlags = O_CLOEXEC;
#else
constexpr const int kOpenBaseFlags = 0;
#endif

constexpr const size_t kWritableFileBufferSize = 65536;

Status PosixError(const std::string& context, int error_number) {
    if (error_number == ENOENT) {
        return Status::NotFound(context, std::strerror(error_number));
    } else {
        return Status::IOError(context, std::strerror(error_number));
    }
}

class Limiter {
public:
    Limiter(int max_acquires) :
#if !defined(NDEBUG)
    max_acquires_(max_acquires),
#endif
    acquires_allowed_(max_acquires) {
        assert(max_acquires >= 0);
    }
    Limiter(const Limiter&) = delete;
    Limiter operator=(const Limiter&) = delete;
    bool Acquire() {
        int old_acquires_allowed = acquires_allowed_.fetch_sub(1, std::memory_order_relaxed);
        if (old_acquires_allowed > 0) return true;
        int pre_increment_acquires_allowed = acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
        (void)pre_increment_acquires_allowed;
        assert(pre_increment_acquires_allowed < max_acquires_);
        return false;
    }

    void Release() {
        int old_acquires_allowed = acquires_allowed_.fetch_add(1, std::memory_order_relaxed);
        (void) old_acquires_allowed;
        assert(old_acquires_allowed < max_acquires_);
    }
private:
#if !defined(NDEBUG)
    const int max_acquires_;
#endif
    std::atomic<int> acquires_allowed_;
};

class PosixSequentialFile final : public SequentialFile {
public:
    PosixSequentialFile(std::string filename, int fd) : fd_(fd), filename_(std::move(filename)) {}
    ~PosixSequentialFile() override { close(fd_); }
    Status Read(size_t n, Slice* result, char* scratch) override {
        Status status;
        while (true) {
            ::ssize_t read_size = ::read(fd_, scratch, n);
            if (read_size < 0) {
                if (errno == EINTR) {
                    continue;
                }
                status = PosixError(filename_, errno);
                break;
            }
            *result = Slice(scratch, read_size);
            break;
        }
        return status;
    }
    Status Skip(uint64_t n) override {
        if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
            return PosixError(filename_, errno);
        }
        return Status::OK();
    }
private:
    const int fd_;
    const std::string filename_;

};

class PosixRandomAccessFile final : public RandomAccessFile {
public:
    PosixRandomAccessFile(std::string filename, int fd, Limiter* fd_limiter) :
            has_permanent_fd_(fd_limiter->Acquire()),
            fd_(has_permanent_fd_ ? fd : -1), fd_limiter_(fd_limiter), filename_(std::move(filename)) {
        if (!has_permanent_fd_) {
            assert(fd_ == -1);
            ::close(fd_);
        }
    }
    ~PosixRandomAccessFile() override {
        if (has_permanent_fd_) {
            assert(fd_ != -1);
            ::close(fd_);
            fd_limiter_->Release();
        }
    }
    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) override {
        int fd = fd_;
        if (!has_permanent_fd_) {
            fd = ::open(filename_.c_str(), O_RDONLY | kOpenBaseFlags);
            if (fd < 0) {
                return PosixError(filename_, errno);
            }
        }
        assert(fd != -1);
        Status status;
        ssize_t read_size = ::pread(fd, scratch, n, static_cast<off_t>(offset));
        *result = Slice(scratch, (read_size < 0) ? 0 : read_size);
        if (read_size < 0) {
            status = PosixError(filename_, errno);
        }
        if (!has_permanent_fd_) {
            assert(fd != fd_);
            ::close(fd);
        }
        return status;
    }
private:
    const bool has_permanent_fd_;
    const int fd_;
    Limiter* const fd_limiter_;
    const std::string filename_;
};

class PosixMmapReadableFile final : public RandomAccessFile {
public:
    PosixMmapReadableFile(std::string filename, char* mmap_base, size_t length, Limiter* mmap_limiter) :
        mmap_base_(mmap_base), length_(length), mmap_limiter_(mmap_limiter), filename_(std::move(filename)) {}

    ~PosixMmapReadableFile() override {
        ::munmap(static_cast<void*>(mmap_base_), length_);
    }
    Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) override {
        if (offset + n > length_) {
            *result = Slice();
            return PosixError(filename_, EINVAL);
        }
        *result = Slice(mmap_base_ + offset, n);
        return Status::OK();
    }
private:
    char* const mmap_base_;
    const size_t length_;
    Limiter* const mmap_limiter_;
    const std::string filename_;
};
class PosixWritableFile final : public WritableFile {
public:
    PosixWritableFile(std::string filename, int fd) :
        pos_(0), fd_(fd), is_manifest_(IsManifest(filename)),
        filename_(std::move(filename)), dirname_(Dirname(filename_)){}
    ~PosixWritableFile() override {
        if (fd_ >= 0) {
            Close();
        }
    }
    Status Append(const Slice& data) override {
        size_t write_size = data.size();
        const char* write_data = data.data();
        size_t copy_size = std::min(write_size, kWritableFileBufferSize - pos_);
        std::memcpy(buf_ + pos_, write_data, copy_size);
        write_data += copy_size;
        write_size -= copy_size;
        pos_ += copy_size;
        if (write_size == 0) {
            return Status::OK();
        }
        Status status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }
        if (write_size < kWritableFileBufferSize) {
            std::memcpy(buf_, write_data, write_size);
            pos_ = write_size;
            return Status::OK();
        }
        return WriteUnbuffered(write_data, write_size);
    }
    Status Close() override {
        Status status = FlushBuffer();
        const int close_result = ::close(fd_);
        if (close_result < 0 && status.ok()) {
            status = PosixError(filename_, errno);
        }
        fd_ = -1;
        return status;
    }
    Status Flush() override { return FlushBuffer(); }
    Status Sync() override {
        Status status = SyncDirIfManifest();
        if (!status.ok()) {
            return status;
        }
        status = FlushBuffer();
        if (!status.ok()) {
            return status;
        }
        return SyncFd(fd_, filename_);
    }
private:
    Status SyncDirIfManifest() {
        Status status;
        if (!is_manifest_) {
            return status;
        }
        int fd = ::open(dirname_.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            status = PosixError(dirname_, errno);
        } else {
            status = SyncFd(fd, dirname_);
        }

    }
    static Status SyncFd(int fd, const std::string& fd_path) {
#if HAVE_FULLFSYNC
        if (::fcntl(fd, F_FULLFSYNC) == 0) {
            return Status::OK();
        }
#endif
#if HAVE_FDATASYNC
    bool sync_success = ::fdatasync(fd) == 0;
#else
    bool sync_success = ::fsync(fd) == 0;
#endif
    if (sync_success) {
        return Status::OK();
    }
    return PosixError(fd_path, errno);

    }
    Status FlushBuffer() {
        Status status = WriteUnbuffered(buf_, pos_);
        pos_ = 0;
        return status;
    }
    Status WriteUnbuffered(const char* data, size_t size) {
        while (size > 0) {
            size_t write_result = ::write(fd_, data, size);
            if (write_result < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return PosixError(filename_, errno);
            }
            data += write_result;
            size -= write_result;
        }
        return Status::OK();
    }
    static std::string Dirname(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return std::string(".");
        }
        assert(filename.find('/', separator_pos + 1) == std::string::npos);
        return filename.substr(0, separator_pos);
    }
    static Slice Basename(const std::string& filename) {
        std::string::size_type separator_pos = filename.rfind('/');
        if (separator_pos == std::string::npos) {
            return Slice(filename);
        }
        assert(filename.find('/', separator_pos + 1) == std::string::npos);
        return Slice(filename.data() + separator_pos + 1, filename.length() - separator_pos - 1);
    }
    static bool IsManifest(const std::string& filename) {
        return Basename(filename).starts_with("MANIFEST");
    }
    char buf_[kWritableFileBufferSize];
    size_t pos_;
    int fd_;
    const bool is_manifest_;
    const std::string filename_;
    const std::string dirname_;
};

int LockOrUnlock(int fd, bool lock) {
    errno = 0;
    struct ::flock file_lock_info;
    std::memset(&file_lock_info, 0, sizeof(file_lock_info));
    file_lock_info.l_type = (lock ? F_WRLCK : F_UNLCK);
    file_lock_info.l_whence = SEEK_SET;
    file_lock_info.l_start = 0;
    file_lock_info.l_len = 0;
    return ::fcntl(fd, F_SETLK, &file_lock_info);
}

class PosixFileLock : public FileLock {
public:
    PosixFileLock(int fd, std::string filename) : fd_(fd), filename_(std::move(filename)) {}
    int fd() const { return fd_;}
    const std::string& filename() const { return filename_; }

private:
    const int fd_;
    const std::string filename_;
};

class PosixLockTable {
public:
    bool Insert(const std::string& fname) LOCKS_EXCLUDED(mu_) {
        mu_.Lock();
        bool succeeded = locked_files_.insert(fname).second;
        mu_.Unlock();
        return succeeded;
    }
    void Remove(const std::string& fname) LOCKS_EXCLUDED(mu_) {
        mu_.Lock();
        locked_files_.erase(fname);
        mu_.Unlock();
    }
private:
    port::Mutex mu_;
    std::set<std::string> locked_files_ GUARDED_BY(mu_);
};

class PosixEnv : public Env {
public:
    PosixEnv();
    ~PosixEnv() override {
        static const char msg[] = "PosixEnv signleton destroyed\n";
        std::fwrite(msg, 1, sizeof(msg), stderr);
        std::abort();
    }
    Status NewSequentialFile(const std::string& filename, SequentialFile** result) override {
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }
        *result = new PosixSequentialFile(filename, fd);
        return Status::OK();
    }
    Status NewRandomAccessFile(const std::string& filename, RandomAccessFile** result) override {
        *result = nullptr;
        int fd = ::open(filename.c_str(), O_RDONLY | kOpenBaseFlags);
        if (fd < 0) {
            return PosixError(filename, errno);
        }
        if (!mmap_limiter_.Acquire()) {
            *result = new PosixRandomAccessFile(filename, fd, &fd_limiter_);
            return Status::OK();
        }

        uint64_t file_size;
        Status status = GetFileSize(filename, &file_size);
        if (status.ok()) {
            void* mmap_base = ::mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
            if (mmap_base != MAP_FAILED) {
                *result = new PosixMmapReadableFile(filename, reinterpret_cast<char*>(mmap_base), file_size, &mmap_limiter_);
            } else {
                status = PosixError(filename, errno);
            }
        }
        ::close(fd);
        if (!status.ok()) {
            mmap_limiter_.Release();
        }
        return status;
    }

    Status NewWritableFile(const std::string& filename, WritableFile** result) override {
        int fd = ::open(filename.c_str(), O_TRUNC | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }
        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }
    Status NewAppendableFile(const std::string& filename, WritableFile** result) override {
        int fd = ::open(filename.c_str(), O_APPEND | O_WRONLY | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }
        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }
    bool FileExists(const std::string& filename) override {
        return ::access(filename.c_str(), F_OK) == 0;
    }
    Status GetChildren(const std::string& directory_path, std::vector<std::string>* result) override {
        result->clear();
        ::DIR* dir = ::opendir(directory_path.c_str());
        if (dir == nullptr) {
            return PosixError(directory_path, errno);
        }
        struct ::dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            result->emplace_back(entry->d_name);
        }
        ::closedir(dir);
        return Status::OK();
    }
    Status RemoveFile(const std::string& filename) override {
        if (::unlink(filename.c_str())!= 0) {
            return PosixError(filename, errno);
        }
        return Status::OK();
    }
    Status CreateDir(const std::string& dirname) override {
        if (::mkdir(dirname.c_str(), 0755) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }
    Status RemoveDir(const std::string& dirname) override {
        if (::rmdir(dirname.c_str()) != 0) {
            return PosixError(dirname, errno);
        }
        return Status::OK();
    }
    Status GetFileSize(const std::string& filename, uint64_t* size) override {
        struct ::stat file_stat;
        if (::stat(filename.c_str(), &file_stat) != 0) {
            *size = 0;
            return PosixError(filename, errno);
        }
        *size = file_stat.st_size;;
        return Status::OK();
    }
    Status RenameFile(const std::string& from, const std::string& to) override {
        if (std::rename(from.c_str(), to.c_str()) != 0) {
            return PosixError(from, errno);
        }
        return Status::OK();
    }
    Status LockFile(const std::string& filename, FileLock** lock) override {
        *lock = nullptr;
        int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | kOpenBaseFlags, 0644);
        if (fd < 0) {
            return PosixError(filename, errno);
        }
        if (!locks_.Insert(filename)) {
            ::close(fd);
            return Status::IOError("lock" + filename, "already held by process");
        }
        if (LockOrUnlock(fd, true) == -1) {
            int lock_errno = errno;
            ::close(fd);
            locks_.Remove(filename);
            return PosixError(filename, lock_errno);
        }
        *lock = new PosixFileLock(fd, filename);
        return Status::OK();
    }
    Status UnlockFile(FileLock* lock) override {
        PosixFileLock* posix_file_lock = static_cast<PosixFileLock*>(lock);
        if (LockOrUnlock(posix_file_lock->fd(), false) == -1) {
            return PosixError("unlock" + posix_file_lock->filename(), errno);
        }
        locks_.Remove(posix_file_lock->filename());
        ::close(posix_file_lock->fd());
        delete posix_file_lock;
        return Status::OK();
    }
    void Schedule(void (*background_work_function)(void* background_work_arg), void* background_work_arg) override;
    void StartThread(void (*thread_main)(void* thread_main_arg), void* thread_main_arg) override {
        std::thread new_thread(thread_main, thread_main_arg);
        new_thread.detach();
    }
    Status GetTestDirectory(std::string* result) override {
        const char* env = std::getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
            *result = env;
        } else {
            char buf[100];
            std::snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", static_cast<int>(::getuid()));
            *result = buf;
        }
        return Status::OK();
    }
    Status NewLogger(const std::string& filename, Logger** result) override {
        int fd = ::open(filename.c_str(), O_APPEND | O_CREAT | O_WRONLY | kOpenBaseFlags, 0644);
        if (fd < 0) {
            *result = nullptr;
            return PosixError(filename, errno);
        }
        std::FILE* fp = ::fdopen(fd, "w");
        if (fp == nullptr) {
            ::close(fd);
            *result = nullptr;
            return PosixError(filename, errno);
        } else {
            *result = new PosixLogger(fp);
            return Status::OK();
        }
    }
    uint64_t NowMicros() override {
        static constexpr uint64_t kUserSecondsPerSecond = 1000000;
        struct ::timeval tv;
        ::gettimeofday(&tv, nullptr);
        return static_cast<uint64_t>(tv.tv_sec) * kUserSecondsPerSecond + tv.tv_usec;
    }
    void SleepForMicroseconds(int micros) override {
        std::this_thread::sleep_for(std::chrono::microseconds(micros));
    }

private:
    void BackgroundThreadMain();
    static void BackgroundTHreadENtryPoint(PosixEnv* env) {
        env->BackgroundThreadMain();
    }
    struct BackgroundWorkItem {
        explicit BackgroundWorkItem(void (*function)(void* arg), void* arg) : function(function), arg(arg) {}
        void (*const function)(void*);
        void* const arg;
    };
    port::Mutex background_work_mutex_;
    port::CondVar background_work_vc_ GUARDED_BY(background_work_mutex_);
    bool started_background_thread_ GUARDED_BY(background_work_mutex_);

    std::queue<BackgroundWorkItem> background_work_queue_ GUARDED_BY(background_work_queue_);
    PosixLockTable locks_;
    Limiter mmap_limiter_;
    Limiter fd_limiter_;
};
} //namespace leveldb