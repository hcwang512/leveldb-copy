//
// Created by 王洪城 on 2024/5/7.
//

#ifndef LEVELDB_COPY_BUILDER_H
#define LEVELDB_COPY_BUILDER_H

#include "leveldb/status.h"

namespace leveldb {
struct Options;
struct FileMetaData;
class Env;
class Iterator;
class TableCache;
class VersionEdit;

Status BuildTable(const std::string& dbname, Env* env, const Options& options, TableCache* table_cache, Iterator* iter, FileMetaData* meta);
} // namespace leveldb
#endif //LEVELDB_COPY_BUILDER_H
