//
// Created by 王洪城 on 2024/5/1.
//

#ifndef LEVELDB_COPY_LOGGING_H
#define LEVELDB_COPY_LOGGING_H

#include <string>
#include <cstdint>

namespace leveldb {
class Slice;
class WritableFile;

void AppendNumberTo(std::string* str, uint64_t num);
void AppendEscapedStringTo(std::string* str, const Slice& value);

std::string NumberToString(uint64_t num);
std::string EscapeString(const Slice& value);
bool ConsumeDecimalNumber(Slice* in, uint64_t* val);
} // namespace leveldb
#endif //LEVELDB_COPY_LOGGING_H
