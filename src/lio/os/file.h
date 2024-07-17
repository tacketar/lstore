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
// OS file header file
//***********************************************************************

#ifndef _OS_FILE_H_
#define _OS_FILE_H_

#include <openssl/md5.h>
#include <tbx/chksum.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>

#include "authn.h"
#include "os.h"
#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OS_TYPE_FILE "file"

struct local_object_iter_t {
    lio_object_service_fn_t *os;
    os_object_iter_t  *oit;
};

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len);
local_object_iter_t *create_local_object_iter(lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth);
void destroy_local_object_iter(local_object_iter_t *it);

lio_object_service_fn_t *object_service_file_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
int osf_store_val(void *src, int src_size, void **dest, int *v_size);

#define SAFE_MIN_LEN 2

#define FILE_ATTR_PREFIX "_^FA^_"
#define FILE_ATTR_PREFIX_LEN 6

#define DIR_PERMS  S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH
#define FILE_PERMS S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH

#define OSF_LOCK_CHKSUM CHKSUM_MD5
#define OSF_LOCK_CHKSUM_SIZE MD5_DIGEST_LENGTH

typedef struct {  //** Internal structure to handle file locking
    apr_pool_t *mpool;
    apr_thread_mutex_t *fobj_lock;
    tbx_pc_t *fobj_pc;
    tbx_list_t *fobj_table;
    tbx_pc_t *task_pc;
} fobject_lock_t;

struct lio_osfile_priv_t {
    int base_path_len;
    int file_path_len;
    int hardlink_path_len;
    int internal_lock_size;
    int hardlink_dir_size;
    int shard_enable;
    int piter_enable;
    int n_piter_threads;
    int n_piter_que_fname;
    int n_piter_que_attr;
    int n_piter_fname_size;
    int n_piter_attr_size;
    int shard_splits;
    int n_shard_prefix;
    int relocate_namespace_attr_to_shard;
    int relocate_min_size;
    int rebalance_running;
    int rebalance_count;
    int rebalance_path_len;
    double delta_fraction;
    tbx_atomic_int_t hardlink_count;
    apr_thread_t *rebalance_thread;
    tbx_notify_t *rebalance_log;
    char *rebalance_path;
    char *rebalance_notify_section;
    char *rebalance_section;
    char *rebalance_config_fname;
    char **shard_prefix;
    char *base_path;
    char *file_path;
    char *hardlink_path;
    char *host_id;
    char *section;
    char *authz_section;
    char *os_activity;
    tbx_notify_t *olog;
    tbx_notify_t *rlog;
    gop_thread_pool_context_t *tpc;
    apr_thread_mutex_t **internal_lock;
    lio_os_authz_t *osaz;
    lio_authn_t *authn;
    apr_pool_t *mpool;
    apr_hash_t *vattr_hash;
    tbx_list_t *vattr_prefix;
    tbx_list_t *open_fd_obj;
    tbx_list_t *open_fd_rp;
    apr_thread_mutex_t *open_fd_lock;
    apr_thread_mutex_t *rebalance_lock;
    fobject_lock_t *os_lock;
    fobject_lock_t *os_lock_user;
    lio_os_virtual_attr_t lock_va;
    lio_os_virtual_attr_t lock_user_va;
    lio_os_virtual_attr_t realpath_va;
    lio_os_virtual_attr_t link_va;
    lio_os_virtual_attr_t link_count_va;
    lio_os_virtual_attr_t type_va;
    lio_os_virtual_attr_t create_va;
    lio_os_virtual_attr_t attr_link_pva;
    lio_os_virtual_attr_t attr_type_pva;
    lio_os_virtual_attr_t timestamp_pva;
    lio_os_virtual_attr_t append_pva;
    int max_copy;
};


#ifdef __cplusplus
}
#endif

#endif
