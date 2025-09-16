/*
   Copyright 2025 Vanderbilt University

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

//******************************************************************************
// inode DB header file
//******************************************************************************


#ifndef _INODE_H_
#define _INODE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <lio/visibility.h>
#include <rocksdb/c.h>

#define OS_INODE_OPEN_EXISTS            1  //** Open's an existing DB - No creating a DB if mising
#define OS_INODE_OPEN_CREATE_IF_NEEDED  2  //** Fresh blank DB ONLY if the DB doesn't exist - Creates an empty DB if missing otherwise use the existing DB (no wiping)
#define OS_INODE_OPEN_CREATE_ONLY       4  //** Always creates the DB. Fails if it already exists
#define OS_INODE_OPEN_READ_ONLY         8  //** Open in READ-only mode. Not compatible with the create modes
#define OS_INODE_OPEN_READ_WRITE       16  //** OPen R/W mode.  This is the default for the create modes

#define OS_INODE_IS_DIR(ftype) ((ftype) & OS_OBJECT_DIR_FLAG)

#define OS_MSG_DELETE_INODE    "DELETE-INODE"    //** Delete the inode
#define OS_MSG_NO_INODE        "NO-INODE"        //** No system.inode attr
#define OS_MSG_MISSING_INODE   "MISSING-INODE"   //** The dest is missing the inode from the src
#define OS_MSG_MISMATCH_PARENT "MISMATCH-PARENT" //** The parent inode is different between src and dest
#define OS_MSG_MISMATCH_DENTRY "MISMATCH-DENTRY" //** The dentry is different between src and dest
#define OS_MSG_MISMATCH_FTYPE  "MISMATCH-FTYPE"  //** The ftype is different between src and dest
#define OS_MSG_ORPHAN_INODE    "ORPHAN-INODE"    //** The inode only exists in the src

#define OS_INODE_MISSING  123456789        //** Reserved inode value to signify there is no system.inode
#define OS_INODE_MISSING_TEXT "123456789"  //** String version of above
#define OS_INODE_MISSING_TEXT_LEN 9        //** String length

typedef struct {
    rocksdb_t *db;
    rocksdb_comparator_t *cmp;
    rocksdb_writeoptions_t *wopt;
    rocksdb_readoptions_t  *ropt;
} os_inode_shard_t;

typedef struct {  //** LStore Inode DB. Has sharding and inode and directory DB info
    int n_shards;           //** Number of shards
    char *prefix;           //** Directory prefix containing the the Inode DB
    os_inode_shard_t **shard;  //** List of shards
} os_inode_db_t;

typedef struct {  //** LStore Inode DB. Has sharding and inode and directory DB info
    char *prefix;        //** Location containing the DBs as subdirectories
    rocksdb_comparator_t *inode_cmp;  //** inode comparison op. Note the modulo sharding is done in the os_inode_db_t structure
    os_inode_db_t *inode;   //** DB containing all Inodes
    os_inode_db_t *dir;     //** DB containing only directory Inodes
} os_inode_ctx_t;

typedef struct {  //** Inode dentry structure
    ex_id_t parent_inode;
    int len;
    char dentry[];
} os_inode_dentry_t;

typedef struct {  //** inode DB record
    ex_id_t inode;
    int ftype;
    os_inode_dentry_t dentry;
} os_inode_rec_t;

typedef struct os_inode_lut_s os_inode_lut_t;

LIO_API os_inode_ctx_t *os_inode_open_ctx(const char *prefix, int mode, int n_shards);
LIO_API void os_inode_close_ctx(os_inode_ctx_t *ctx);
LIO_API int os_inode_compare_shard_count(os_inode_ctx_t *ctx1, os_inode_ctx_t *ctx2);
LIO_API int os_inode_put(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry);
LIO_API int os_inode_put_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry);
LIO_API int os_inode_get(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_dir_get(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_db_get(os_inode_db_t *db, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_db_get_with_lut(os_inode_db_t *db, os_inode_lut_t *ilut, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_get_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_shard_get(os_inode_shard_t *db, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API int os_inode_db_del(os_inode_db_t *db, ex_id_t inode);
LIO_API int os_inode_del(os_inode_ctx_t *ctx, ex_id_t inode, int ftype);
LIO_API int os_inode_del_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode, int ftype);
LIO_API int os_inode_lookup_path_db(os_inode_db_t *db, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs);
LIO_API int os_inode_lookup_path_db_wth_lut(os_inode_db_t *db, os_inode_lut_t *ilut, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs);
LIO_API int os_inode_lookup_path(os_inode_ctx_t *ctx, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs);
LIO_API int os_inode_lookup_path_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs);
LIO_API void os_inode_lookup2path(char *path, int n, ex_id_t *inode, char **dentry);

//** These are routines for DB maintenance
typedef void *(*walk_create_fn_t)(const char *prefix, int max_recurse_depth);
typedef void (*walk_destroy_fn_t)(void *arg);
typedef int (*walk_next_fn_t)(void *arg, char **fname, ex_id_t *inode, int *ftype, ex_id_t *parent, int *prefix_len, int *len);
typedef int (*walk_object_info_t)(void *arg, char *fname, ex_id_t *inode, int *ftype);

LIO_API extern walk_create_fn_t walk_create;
LIO_API extern walk_destroy_fn_t walk_destroy;
LIO_API extern walk_next_fn_t walk_next;
LIO_API extern walk_object_info_t walk_info;
LIO_API extern void *walk_arg;

LIO_API void os_inode_walk_setup(int is_lio);
LIO_API void os_inode_process_entry(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, char *entry, FILE *fdout);

#ifdef __cplusplus
}
#endif

#endif
