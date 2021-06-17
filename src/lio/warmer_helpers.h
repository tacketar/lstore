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

#define WFE_SUCCESS   1
#define WFE_FAIL      2
#define WFE_WRITE_ERR 4

#include <lio/visibility.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

//**Hack until packaged version of RocksDB catches up with git
#ifdef _ROCKSDB_CANCEL_MISSING
void rocksdb_cancel_all_background_work(rocksdb_t* db, unsigned char wait);
#endif

typedef struct {  //** PREP databases
    rocksdb_t *db;
    rocksdb_comparator_t *cmp;
    rocksdb_writeoptions_t *wopt;
    rocksdb_readoptions_t  *ropt;
} warm_db_t;

//** These are helpers for lio_wamer and warmer_query *******
LIO_API int open_warm_db(char *db_base, warm_db_t **inode_db, warm_db_t **rid_db);
LIO_API void close_warm_db(warm_db_t *inode, warm_db_t *rid);
LIO_API int warm_put_inode(warm_db_t *db, ex_id_t inode, int state, int nfailed, char *name);
LIO_API int warm_parse_inode(char *buf, int bufsize, int *state, int *nfailed, char **name);
LIO_API int warm_put_rid(warm_db_t *db, char *rid, ex_id_t inode, ex_off_t nbytes, int state);
LIO_API int warm_parse_rid(char *buf, int bufsize, ex_id_t *inode, ex_off_t *nbytes,int *state);
LIO_API void create_warm_db(char *db_base, warm_db_t **inode_db, warm_db_t **rid_db);

typedef struct {  //** PREP databases
    rocksdb_t *inode;
    rocksdb_t *rid;
    rocksdb_t *write_errors;
} warm_prep_db_t;

#ifdef __cplusplus
}
#endif

#endif

