cmake_minimum_required(VERSION 3.9)
project(leveldb_copy VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD  11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)


include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(zstd zstd_compress "" HAVE_ZSTD)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

if (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_WINDOWS)
    add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)
endif (WIN32)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)

include(CheckCXXSymbolExists)
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)
check_cxx_compiler_flag(-Wno-missing-field-initializers LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
#if defined(__has_include) && __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
  "port/port_config.h.in"
  "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "."
)

include(GNUInstallDirs)

add_library(leveldbcopy "")
target_sources(leveldbcopy
  PRIVATE
  "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
  "db/c.cpp"
        "db/db_impl.h"
        "db/db_impl.cpp"
        "db/dbformat.h"
        "db/dbformat.cpp"
        "db/version_set.h"
        "db/filename.h"
        "db/filename.cpp"
        "db/table_cache.h"
        "db/table_cache.cpp"
        "db/skiplist.h"
        "db/write_batch.cpp"
        "db/write_batch_internal.h"
        "db/log_format.h"
        "db/log_reader.h"
        "db/log_reader.cpp"
        "db/log_writer.h"
        "db/log_writer.cpp"
        "db/snapshot.h"
        "db/version_edit.h"
        "db/version_edit.cpp"
        "db/version_set.h"
        "db/version_set.cpp"
        "db/builder.h"
        "db/builder.cpp"
        "table/table.cpp"
        "table/filter_block.h"
        "table/filter_block.cpp"
        "table/format.cpp"
        "table/format.h"
        "table/table_builder.cpp"
        "table/block_builder.h"
        "table/block_builder.cpp"
        "table/block.h"
        "table/block.cpp"
        "table/two_level_iterator.h"
        "table/two_level_iterator.cpp"
        "table/iterator_wrapper.h"
        "table/memtable.h"
        "table/memtable.cpp"
        "table/merger.cpp"
        "table/merger.h"
        "table/iterator.cpp"
  "util/cache.cpp"
  "util/mutexlock.h"
        "util/coding.h"
        "util/coding.cpp"
        "util/status.cpp"
        "util/crc32c.h"
        "util/crc32c.cpp"
        "util/bloom.cpp"
        "util/arena.h"
        "util/arena.cpp"
  "util/hash.cpp"
  "util/hash.h"
        "util/comparator.cpp"
        "util/no_destructor.h"
        "util/posix_logger.h"
        "util/env_posix.cpp"
        "util/env.cpp"
        "util/logging.h"
        "util/logging.cpp"
        "util/random.h"
        "port/port.h"
  "port/port_stdcxx.h"
  "port/thread_annotations.h"

  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
  "${LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
  "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
  "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
  "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
  "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
)

target_include_directories(leveldbcopy
  PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDE_DIR}>
)
target_compile_definitions(leveldbcopy
        PRIVATE
        # Used by include/export.h when building shared libraries.
        LEVELDB_COMPILE_LIBRARY
        # Used by port/port.h.
        ${LEVELDB_PLATFORM_NAME}=1
)

enable_testing()
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(install_gtest OFF)
set(install_gmock OFF)
set(build_gmock ON)
add_subdirectory("third_party/googletest")

add_executable(leveldb_tests "")
target_sources(leveldb_tests
  PRIVATE
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)
if(NOT BUILD_SHARED_LIBS)
    target_sources(leveldb_tests
    PRIVATE
            "util/cache_test.cpp"
    )
endif (NOT BUILD_SHARED_LIBS)
target_Link_libraries(leveldb_tests leveldbcopy gmock gtest gtest_main)
target_compile_definitions(leveldb_tests
        PRIVATE
        ${LEVELDB_PLATFORM_NAME}=1
)