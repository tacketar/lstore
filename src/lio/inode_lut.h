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

#ifndef _INODE_LUT__H_
#define _INODE_LUT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <lio/visibility.h>
#include <tbx/list.h>
#include "inode.h"

typedef struct {  //** inode LUT record
    int epoch;
    os_inode_dentry_t *hardlink_list;  //** This struct has a pointer appended so it can act as a list
    os_inode_rec_t r;
} os_inode_lut_rec_t;

struct os_inode_lut_s {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_hash_t *table;
    int n_lut_max;
    int n_lut_shrink;
    int n_lut;
    int n_epoch;
    int count;
    int n_new_epoch;
    int epoch;
    int min_epoch;
    int *epoch_tally;
    tbx_list_t *dentry_table;
};


LIO_API void os_inode_lut_dentry_rename(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, int ftype, ex_id_t oldparent, const char *oldname, ex_id_t newparent, const char *newname);
LIO_API void os_inode_lut_dentry_del(os_inode_lut_t *ilut, int do_lock, ex_id_t parent, const char *name);
LIO_API int os_inode_lut_dentry_get(os_inode_lut_t *ilut, int do_lock, ex_id_t parent_target, const char *dentry_target, ex_id_t *inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API os_inode_lut_t *os_inode_lut_create(int n_lut_max, double shrink_fraction, int n_epoch, int n_new_epoch, int enable_dentry_lut);
LIO_API void os_inode_lut_destroy(os_inode_lut_t *ilut);
LIO_API os_inode_lut_t *os_inode_lut_load(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section);
LIO_API void os_inode_lut_print_running_config(os_inode_lut_t *ilut, FILE *fd);
LIO_API void _lut_epoch_get(os_inode_lut_t *ilut, os_inode_lut_rec_t *r);
LIO_API void _lut_epoch_dec(os_inode_lut_t *ilut, os_inode_lut_rec_t *r);
LIO_API int os_inode_lut_get(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry);
LIO_API void os_inode_lut_put(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry);
LIO_API void os_inode_lut_del(os_inode_lut_t *ilut, int do_lock, ex_id_t inode);

#ifdef __cplusplus
}
#endif

#endif