//
// Created by 王洪城 on 2024/4/30.
//

#ifndef LEVELDB_COPY_POSIX_LOGGER_H
#define LEVELDB_COPY_POSIX_LOGGER_H

#include <cstdarg>
#include <sys/time.h>
#include <sstream>
#include "leveldb/env.h"

namespace leveldb {
    class PosixLogger final : public Logger {
    public:
        explicit PosixLogger(std::FILE* fp) : fp_(fp) { assert(fp != nullptr); }
        ~PosixLogger() override { std::fclose(fp_); }

        void logv(const char* format, std::va_list arguments) override {
            struct ::timeval now_timeval;
            ::gettimeofday(&now_timeval, nullptr);
            const std::time_t now_seconds = now_timeval.tv_sec;
            struct std::tm now_components;
            ::localtime_r(&now_seconds, &now_components);

            constexpr const int kMaxThreadIdSize = 32;
            std::ostringstream thread_stream;
            thread_stream << std::this_thread::get_id();
            std::string thread_id = thread_stream.str();
            if (thread_id.size() > kMaxThreadIdSize) {
                thread_id.resize(kMaxThreadIdSize);
            }
            constexpr const int kStackBufferSize = 512;
            char stack_buffer[kStackBufferSize];
            static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize), "sizeof(char) is expected to be 1 in c++");
            int dynamic_buffer__size = 0;
            for (int iteration = 0; iteration < 2; ++iteration) {
                const int buffer_size = (iteration == 0) ? kStackBufferSize : dynamic_buffer__size;
                char* const buffer = (iteration == 0) ? stack_buffer : new char[dynamic_buffer__size];
                int buffer_offset = std::snprintf(
                        buffer, buffer_size, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s",
                        now_components.tm_year + 1900, now_components.tm_mon + 1,
                        now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
                        now_components.tm_sec, static_cast<int>(now_timeval.tv_usec),
                        thread_id.c_str()
                        );
                assert(buffer_offset <= 28 + kMaxThreadIdSize);
                assert(buffer_offset < buffer_size);
                std::va_list arguments_copy;
                va_copy(arguments_copy, arguments);
                buffer_offset += std::vsnprintf(buffer + buffer_offset, buffer_size - buffer_offset, format, arguments_copy);
                va_end(arguments_copy);
                if (buffer_offset >= buffer_size - 1) {
                    if (iteration ==  0) {
                        dynamic_buffer__size = buffer_offset + 2;
                        continue;
                    }
                    assert(false);
                    buffer_offset = buffer_size - 1;
                }
                if (buffer[buffer_offset -1] != '\n') {}
                buffer[buffer_offset] = '\n';
                ++buffer_offset;
                assert(buffer_offset < buffer_size);
                std::fwrite(buffer, 1, buffer_offset, fp_);
                std::fflush(fp_);
                if (iteration != 0) {
                    delete[] buffer;
                }
                break;
            }
        }
    private:
        std::FILE* const fp_;
    };
}
#endif //LEVELDB_COPY_POSIX_LOGGER_H
