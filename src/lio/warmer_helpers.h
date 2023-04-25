/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
//***********************************************************************
// Warmer helper definitions
//***********************************************************************


#ifndef _WARMER_H_
#define _WARMER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <lio/visibility.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#define WFE_SUCCESS   1
#define WFE_FAIL      2
#define WFE_WRITE_ERR 4

#define DB_OPEN_EXISTS            0  //** Open's an existing DB - No creating a DB if mising
#define DB_OPEN_WIPE_CLEAN        1  //** New DB always.  Wipes it clean if it already exists
#define DB_OPEN_CREATE_IF_NEEDED  2  //** Fresh blank DB ONLY if the DB doesn't exist - Creates an empty DB if missing otherwise use the existing DB (no wiping)
#define DB_OPEN_CREATE_ONLY       3  //** Always creates the DB. Fails if it already exists

//**Hack until packaged version of RocksDB catches up with git
#ifdef _ROCKSDB_CANCEL_MISSING
void rocksdb_cancel_all_background_work(rocksdb_t* db, unsigned char wait);
#endif

typedef struct {  //** Databases
    rocksdb_t *db;
    rocksdb_comparator_t *cmp;
    rocksdb_writeoptions_t *wopt;
    rocksdb_readoptions_t  *ropt;
} warm_db_t;

typedef struct {    //** PREP DB partition
    warm_db_t *inode;
    warm_db_t *rid;
    warm_db_t *write_errors;
    warm_db_t *missing_exnode_errors;
    rocksdb_comparator_t *inode_cmp;
    rocksdb_comparator_t *rid_cmp;
} warm_prep_db_part_t;

typedef struct {  //** PREP databases
    warm_prep_db_part_t **p;
    int n_partitions;
} warm_prep_db_t;

typedef struct {    //** Warmer DB partition
    warm_db_t *inode;
    warm_db_t *rid;
} warm_results_db_part_t;

typedef struct {
    warm_results_db_part_t **p;
    int n_partitions;
} warm_results_db_t;

typedef struct {
    ex_id_t id;
    int rid_len;
    int mcap_len;
    char strings[];
} rid_prep_key_t;

typedef struct {
    int n_allocs;
    int n_rids;
    int fname_len;
    int rid_list_len;
    char strings[];
} inode_value_t;

//** These are helpers for lio_wamer and warmer_query *******
LIO_API warm_results_db_t *open_results_db(char *db_base, int mode);
LIO_API void close_results_db(warm_results_db_t *r);
LIO_API int warm_put_inode(warm_db_t *db, ex_id_t inode, int state, int nfailed, char *name);
LIO_API int warm_parse_inode(char *buf, int bufsize, int *state, int *nfailed, char **name);
LIO_API int warm_put_rid(warm_db_t *db, char *rid, ex_id_t inode, ex_off_t nbytes, int state);
LIO_API int warm_parse_rid(char *buf, int bufsize, ex_id_t *inode, ex_off_t *nbytes,int *state);
LIO_API warm_results_db_t *create_results_db(char *db_base, int n_partitions);
LIO_API warm_prep_db_t *open_prep_db(char *db_dir, int mode, int n_partitions);
LIO_API void close_prep_db(warm_prep_db_t *wdb);
LIO_API warm_prep_db_t *create_prep_db(char *db_dir, int n_partitions);
LIO_API void prep_warm_rid_db_put(warm_prep_db_t *wdb, ex_id_t inode, char *rid_key, char *mcap, ex_off_t nbytes);
LIO_API int prep_warm_rid_db_parse(warm_prep_db_t *wdb, char *buffer, int blen, ex_id_t *inode, char **rid_key, char **mcap);
LIO_API void prep_warm_inode_db_put(warm_prep_db_t *wdb, ex_id_t inode, char *fname, int n_allocs, apr_hash_t *rids, int free_rids);
LIO_API void update_warm_prep_db(tbx_log_fd_t *ifd, warm_prep_db_t *wdb, char *fname, char **vals, int *v_size);
LIO_API void warm_prep_put_timestamp(char *prefix, int year, int month, int day, int line);
LIO_API int warm_prep_get_timestamp(char *prefix, int *year, int *month, int *day, int *line);

#ifdef __cplusplus
}
#endif

#endif

