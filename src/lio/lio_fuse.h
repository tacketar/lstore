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
#include "inode.h"
#include "inode_lut.h"

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
    int enable_pending_delete_relocate;
    int shutdown;
    int mount_point_len;
    int use_lowlevel_api;
    lio_config_t *lc;
    apr_pool_t *mpool;
    struct fuse_operations fops;
    struct fuse_lowlevel_ops ll_ops;
    char *id;
    char *mount_point;
    tbx_atomic_int_t n_pending_delete;
    int pending_delete_prefix_len;
    char *pending_delete_prefix;
    apr_hash_t *pending_delete_table;
    apr_thread_mutex_t *lock;
    char *lfs_section;
    char *ilut_section;
    os_inode_lut_t *ilut;
    struct fuse_conn_info *conn;
    struct fuse_config *fuse_cfg;
    lio_fs_t *fs;
    double ll_entry_timeout;
    double ll_stat_timeout;
};

#define IS_CAP(cap, var) (((cap) & (var)) ? 1 : 0)
//**FIXME QWERT
//#if FUSE_VERSION > FUSE_MAKE_VERSION(3, 12)
//    #define CAP_SET_WANT(conn, cap) conn->want_ext |= cap
//    #define CAP_GET_WANT(conn, cap) IS_CAP(cap, conn->want_ext)
//    #define CAP_GET_CAPABLE(conn, cap) IS_CAP(cap, conn->capable_ext)
//#else
    #define CAP_SET_WANT(conn, cap) conn->want |= cap
    #define CAP_GET_WANT(conn, cap) IS_CAP(cap, conn->want)
    #define CAP_GET_CAPABLE(conn, cap) IS_CAP(cap, conn->capable)
//#endif
#define CAP_MANGLE(conn, cap) CAP_GET_CAPABLE(conn, cap), CAP_GET_WANT(conn, cap)
#define CAP_PRINTF(fd, conn, cap) fprintf(fd, "#" #cap " : %d - %d\n", CAP_MANGLE(conn, cap)) \

void lfs_pending_delete_mapping_remove(lio_fuse_t *lfs, const char *path);
int lfs_pending_delete_relocate_object(lio_fuse_t *lfs, const char *oldname, const char *newname);
int lfs_pending_delete_remove_object(lio_fuse_t *lfs, const char *original, char *mapping);
char *lfs_pending_delete_mapping_get(lio_fuse_t *lfs, const char *path);
void _lfs_hint_release(lio_fuse_t *lfs, lio_os_authz_local_t *ug);
void lfs_ll_print_fuse_info(void *arg, FILE *fd);
void *lfs_init_real(struct fuse_config *fuse_cfg, struct fuse_conn_info *conn, lio_fuse_init_args_t *lfs_init_args);

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
