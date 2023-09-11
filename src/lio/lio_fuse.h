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
// lio_fuse.h - LIO Linux FUSE header file
//***********************************************************************

#ifndef _LIO_FUSE_H_
#define _LIO_FUSE_H_

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <lio/fs.h>
#include <lio/lio_fuse.h>
#include <lio/visibility.h>
#include <lio/path_acl.h>
#include <tbx/atomic_counter.h>
#include <tbx/list.h>

#include "ex3.h"
#include "lio.h"

#ifdef __cplusplus
extern "C" {
#endif


struct fuse_conn_info;
struct lio_fuse_t;

struct lio_fuse_t {
    int enable_osaz_acl_mappings;
    int fs_checks_acls;
    int no_cache_stat_if_file;
    int enable_flock;
    int shutdown;
    int mount_point_len;
    lio_config_t *lc;
    apr_pool_t *mpool;
    struct fuse_operations fops;
    char *id;
    char *mount_point;
    tbx_atomic_int_t n_pending_delete;
    int pending_delete_prefix_len;
    char *pending_delete_prefix;
    apr_hash_t *pending_delete_table;
    apr_thread_mutex_t *lock;
    char *lfs_section;
    struct fuse_conn_info *conn;
    lio_fs_t *fs;
};

#ifdef HAS_FUSE3
    void *lfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg);
#else
    void *lfs_init(struct fuse_conn_info *conn);
#endif
void lfs_destroy(void *lfs); // expects a lio_fuse_t* as the argument

#ifdef __cplusplus
}
#endif

#endif
