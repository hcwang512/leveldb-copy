//
// Created by 王洪城 on 2024/4/26.
//

#ifndef LEVELDB_COPY_EXPORT_H
#define LEVELDB_COPY_EXPORT_H

#if !defined(LEVELDB_EXPORT)

#if defined(LEVELDB_SHARED_LIBRARY)
#if defined(_WIN32)
#if defined(LEVELDB_COMPILE_LIBRARY)
#define LEVELDB_EXPORT __declspec(dllexport)
#else
#define LEVELDB_EXPORT __declspec(dllimport)
#endif // defined(LEVELDB_COMPILE_LIBRARY)

#else // defined(_WIN32)
#define LEVELDB_EXPORT
#endif // defined(_WIN32)
#else // defined(LEVELDB_SHARED_LIBRARY)
#define LEVELDB_EXPORT
#endif

#endif // !defined(LEVELDB_EXPORT)
#endif //LEVELDB_COPY_EXPORT_H
