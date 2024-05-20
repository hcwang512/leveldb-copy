//
// Created by 王洪城 on 2024/4/26.
//

#ifndef LEVELDB_COPY_C_H
#define LEVELDB_COPY_C_H

#include <stddef.h>
#include <stdint.h>

#include "leveldb/export.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Exported types */

typedef struct leveldb_t leveldb_t;
typedef struct leveldb_cache_t leveldb_cache_t;
typedef struct leveldb_comparator_t leveldb_comparator_t;
typedef struct leveldb_env_t leveldb_env_t;
typedef struct leveldb_filelock_t leveldb_filelock_t;
typedef struct leveldb_filterpolicy_t leveldb_filterpolicy_t;
typedef struct leveldb_iterator_t leveldb_iterator_t;
typedef struct leveldb_logger_t leveldb_logger_t;
typedef struct leveldb_options_t leveldb_options_t;
typedef struct leveldb_randomfile_t leveldb_randomfile_t;
typedef struct leveldb_readoptions_t leveldb_readoptions_t;
typedef struct leveldb_seqfile_t leveldb_seqfile_t;
typedef struct leveldb_snapshot_t leveldb_snapshot_t;
typedef struct leveldb_writablefile_t leveldb_writablefile_t;
typedef struct leveldb_writebatch_t leveldb_writebatch_t;
typedef struct leveldb_writeoptions_t leveldb_writeoptions_t;

/* DB operations */
LEVELDB_EXPORT leveldb_t* leveldb_open(const leveldb_options_t* options, const char* name, char** errptr);
LEVELDB_EXPORT void leveldb_close(leveldb_t* db);
LEVELDB_EXPORT void leveldb_put(
        leveldb_t* db,
        const leveldb_writeoptions_t* options,
        const char* key, size_t keylen, const char*val,
        size_t vallen, char** errptr
        );
LEVELDB_EXPORT void leveldb_delete(leveldb_t *db,
                                   const leveldb_writeoptions_t* options,
                                   const char* key, size_t keylen, char** errptr);
LEVELDB_EXPORT void leveldb_write(leveldb_t* db,
                                  const leveldb_writeoptions_t* options,
                                  leveldb_writebatch_t* batch, char** errptr);
LEVELDB_EXPORT char* leveldb_get(leveldb_t* db, const leveldb_readoptions_t* options,
                                 const char* key, size_t keylen, size_t* vallen, char** errptr);
LEVELDB_EXPORT leveldb_iterator_t* leveldb_create_iterator(leveldb_t* db, const leveldb_readoptions_t* options);
LEVELDB_EXPORT const leveldb_snapshot_t* leveldb_create_snapshot(leveldb_t* db);
LEVELDB_EXPORT void leveldb_release_snapshot(leveldb_t* db, const leveldb_snapshot_t* snapshot);
LEVELDB_EXPORT char* leveldb_property_value(leveldb_t* db, const char* propname);
LEVELDB_EXPORT void leveldb_approximate_sizes(
        leveldb_t* db, int num_ranges, const char* const* range_start_key,
        const size_t* range_start_key_len, const char* const* range_limit_key,
        const size_t* range_limit_key_len, uint64_t* sizes
        );
LEVELDB_EXPORT void leveldb_compact_range(leveldb_t* db, const char* start_key,
                                          size_t staart_key_len, const char* limit_Key, size_t limit_key_len);
LEVELDB_EXPORT void leveldb_destroy_db(const leveldb_options_t* options,
                                       const char* name, char** errptr);
LEVELDB_EXPORT void leveldb_repair_db(const leveldb_options_t* options,
                                      const char* name, char** errptr);
LEVELDB_EXPORT void leveldb_iter_destroy(leveldb_iterator_t*);
LEVELDB_EXPORT uint8_t leveldb_iter_valid(const leveldb_iterator_t*);
LEVELDB_EXPORT void leveldb_iter_seek_to_first(leveldb_iterator_t*);
LEVELDB_EXPORT void leveldb_iter_seek_to_last(leveldb_iterator_t*);
LEVELDB_EXPORT void leveldb_iter_seek(leveldb_iterator_t*, const char* k, size_t klen);
LEVELDB_EXPORT void leveldb_iter_next(const leveldb_iterator_t*);
LEVELDB_EXPORT void leveldb_iter_prev(const leveldb_iterator_t*);
LEVELDB_EXPORT const char* leveldb_iter_key(const leveldb_iterator_t*, size_t* klen);
LEVELDB_EXPORT const char* leveldb_iter_value(const leveldb_iterator_t*, size_t* vlen);
LEVELDB_EXPORT void leveldb_iter_get_error(const leveldb_iterator_t*, char** errptr);

/* Write batch */
LEVELDB_EXPORT leveldb_writebatch_t* leveldb_writebatch_create(void);
LEVELDB_EXPORT void leveldb_writebatch_destroy(leveldb_writebatch_t*);
LEVELDB_EXPORT void leveldb_writebatch_clear(leveldb_writebatch_t*);
LEVELDB_EXPORT void leveldb_writebatch_put(leveldb_writebatch_t*,
                                           const char* key, size_t klen,
                                           const char* val, size_t vlen);
LEVELDB_EXPORT void leveldb_writebatch_delete(leveldb_writebatch_t*, const char* key, size_t klen);
LEVELDB_EXPORT void leveldb_writebatch_iterate(const leveldb_writebatch_t* b, void* state,
                                               void (*put)(void *, const char* k, size_t klen, const char* v, size_t vlen),
                                               void (*deleted)(void*, const char* k, size_t klen));
LEVELDB_EXPORT void leveldb_writebatch_append(leveldb_writebatch_t* destination, leveldb_writebatch_t* source);

/* Options */
LEVELDB_EXPORT leveldb_options_t* leveldb_options_create(void);
LEVELDB_EXPORT void leveldb_options_destroy(leveldb_options_t*);
LEVELDB_EXPORT void leveldb_options_set_comparator(leveldb_options_t*, leveldb_comparator_t*);
LEVELDB_EXPORT void leveldb_options_set_filter_policy(leveldb_options_t*, leveldb_filterpolicy_t*);
LEVELDB_EXPORT void leveldb_options_set_create_if_missing(leveldb_options_t*, uint8_t);
LEVELDB_EXPORT void leveldb_options_set_paranoid_checks(leveldb_options_t*, uint8_t);
LEVELDB_EXPORT void leveldb_options_setenv(leveldb_options_t*, leveldb_env_t*);
LEVELDB_EXPORT void leveldb_options_set_info_log(leveldb_options_t*, leveldb_logger_t*);
LEVELDB_EXPORT void leveldb_options_set_write_buffer_size(leveldb_options_t*, size_t);

enum { leveldb_no_compression = 0, leveldb_snappy_compression = 1 };
LEVELDB_EXPORT void leveldb_options_set_compression(leveldb_options_t*, int);

/* Comparator */
LEVELDB_EXPORT leveldb_comparator_t* leveldb_comparator_create(
        void* state, void (*destructor)(void*),
        int (*compare)(void*, const char* a, size_t alen, const char* b, size_t blen),
        const char* (*name)(void*)
        );
LEVELDB_EXPORT void leveldb_comparator_destroy(leveldb_comparator_t*);

/* Filter policy */
LEVELDB_EXPORT leveldb_filelock_t* leveldb_filterpolicy_create(
        void* state, void (*destructor)(void*),
        char *(*create_filter)(void*, const char* const key_array, const size_t* key_length_array, int num_keys, size_t* filter_length),
        uint8_t (*key_may_match)(void*, const char* key, size_t length, const char* filter, size_t filter_length),
        const char* (*name)(void*)
        );
LEVELDB_EXPORT void leveldb_filterpolicy_destroy(leveldb_filterpolicy_t*);
LEVELDB_EXPORT leveldb_filterpolicy_t* leveldb_filterpolicy_create_bloom(int bits_per_key);

/* Read options */
LEVELDB_EXPORT leveldb_readoptions_t* leveldb_readoptions_create(void);
LEVELDB_EXPORT void leveldb_readoptions_destroy(leveldb_readoptions_t*);
LEVELDB_EXPORT void leveldb_readoptions_set_verify_checksums(leveldb_readoptions_t*, uint8_t);
LEVELDB_EXPORT void leveldb_readoptions_set_fiil_cache(leveldb_readoptions_t*, uint8_t);
LEVELDB_EXPORT void leveldb_readoptions_set_snapshot(leveldb_readoptions_t*, const leveldb_snapshot_t*);

/* Write options */
LEVELDB_EXPORT leveldb_writeoptions_t* leveldb_writeoptions_create(void);
LEVELDB_EXPORT void leveldb_writeoptions_destroy(leveldb_writeoptions_t*);
LEVELDB_EXPORT void leveldb_writeoptions_set_sync(leveldb_writeoptions_t*, uint8_t);

/* Cache */
LEVELDB_EXPORT leveldb_cache_t* leveldb_cache_create_lru(size_t capacity);
LEVELDB_EXPORT void leveldb_cache_destroy(leveldb_cache_t*);

/* ENV */
LEVELDB_EXPORT leveldb_env_t* leveldb_create_default_env(void);
LEVELDB_EXPORT void leveldb_env_destroy(leveldb_env_t*);
LEVELDB_EXPORT char* levendb_env_get_test_directory(leveldb_env_t*);

LEVELDB_EXPORT void leveldb_free(void* ptr);
LEVELDB_EXPORT int leveldb_major_version(void);
LEVELDB_EXPORT int leveldb_minor_version(void);

#ifdef __cplusplus
}; /* end extern "C" */
#endif
#endif //LEVELDB_COPY_C_H
