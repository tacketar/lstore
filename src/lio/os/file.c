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
// Simple file backed object storage service implementation
//***********************************************************************

#define _log_module_index 155

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_network_io.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <dirent.h>
#include <gop/gop.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <regex.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/append_printf.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/chksum.h>
#include <tbx/fmttypes.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/io.h>
#include <tbx/pigeon_coop.h>
#include <tbx/random.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <tbx/que.h>
#include <unistd.h>

#include "authn.h"
#include "authn/fake.h"
#include "ex3/system.h"
#include "ex3/types.h"
#include "os.h"
#include "os/file.h"
#include "osaz/fake.h"

static lio_osfile_priv_t osf_default_options = {
    .section = "os_file",
    .base_path = "/lio/osfile",
    .os_activity = "notify_os_activity",
    .piter_enable = 0,
    .n_piter_threads = 2,
    .n_piter_que_fname = 1024,
    .n_piter_que_attr = 1024,
    .n_piter_fname_size = 1024,
    .n_piter_attr_size = 1*1024*1024,
    .shard_enable = 0,
    .shard_splits = 10000,
    .n_shard_prefix = 0,
    .shard_prefix = NULL,
    .internal_lock_size = 200,
    .max_copy = 1024*1024,
    .hardlink_dir_size = 256,
    .authz_section = NULL,
};

typedef struct {
    DIR *d;
    struct dirent *entry;
    int slot;
    int type;
    char *frag;
} osf_dir_t;

typedef struct {
    tbx_stack_t *active_stack;
    tbx_stack_t *pending_stack;
    int read_count;
    int write_count;
    tbx_pch_t pch;
} fobj_lock_t;

#define FOL_OS   0
#define FOL_USER 1

typedef struct {
    char realpath[OS_PATH_MAX];
    lio_object_service_fn_t *os;
    char *object_name;
    char *attr_dir;
    char *id;
    int ftype;
    int mode;
    int user_mode;
    fobj_lock_t *fol[2];
    int ilock_rp;     //** Realpath slot
    int ilock_obj;    //** User specified path slot
    uint64_t uuid;
} osfile_fd_t;

typedef struct {
    apr_thread_cond_t *cond;
    osfile_fd_t *fd;
    int rw_mode;
    int abort;
} fobj_lock_task_t;

typedef struct {
    lio_object_service_fn_t *os;
    char *path;
    int mode;
    char *id;
    lio_creds_t *creds;
    osfile_fd_t **fd;
    osfile_fd_t *cfd;
    char *realpath;
    lio_os_authz_local_t  *ug;
    uint64_t uuid;
    int max_wait;
} osfile_open_op_t;

typedef struct {
    lio_object_service_fn_t *os;
    int mode;
    osfile_fd_t *fd;
    int max_wait;
} osfile_lock_user_op_t;

typedef struct {
    lio_object_service_fn_t *os;
    osfile_fd_t *fd;
    lio_creds_t *creds;
    char *realpath;
    lio_os_authz_local_t  *ug;
    char **key;
    void **val;
    char *key_tmp;
    void *val_tmp;
    int *v_size;
    int v_tmp;
    int n;
} osfile_attr_op_t;


typedef struct {
    lio_object_service_fn_t *os;
    osfile_fd_t *fd;
    lio_creds_t *creds;
    char *realpath;
    lio_os_authz_local_t  *ug;
    DIR *d;
    apr_pool_t       *mpool;  //** Needa separate pool for making the va_index. Only way to do this since no apr_hash_iter_destroy fn exists
    apr_hash_index_t *va_index;
    lio_os_regex_table_t *regex;
    char *key;
    void *value;
    int v_max;
} osfile_attr_iter_t;

typedef struct {
    osf_dir_t *d;
    char *entry;
    char path[OS_PATH_MAX];
    char realpath[OS_PATH_MAX];
    regex_t *preg;
    long prev_pos;
    long curr_pos;
    int firstpass;
    char *fragment;
    int fixed_prefix;
} osf_obj_level_t;

typedef struct {
    int prefix_len;
    int ftype;
    char *fname;
    char *realpath;
} piq_fname_t;

typedef struct {
    int len;
    void *value;
} piq_attr_t;


typedef struct {
    apr_pool_t *mpool;
    apr_thread_t **attr_workers;
    apr_thread_t *fname_worker;
    tbx_atomic_int_t n_active;
    tbx_atomic_int_t abort;
    tbx_que_t *que_fname;
    tbx_que_t *que_attr;
    int attr_curr_slot;
    piq_attr_t *attr_curr;
    int optimized_enable;
} piter_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_os_regex_table_t *table;
    lio_os_regex_table_t *attr;
    lio_os_regex_table_t *object_regex;
    regex_t *object_preg;
    osf_obj_level_t *level_info;
    lio_creds_t *creds;
    lio_os_authz_local_t ug;
    os_attr_iter_t **it_attr;
    piter_t *piter;
    os_fd_t *fd;
    tbx_stack_t *recurse_stack;
    apr_pool_t *mpool;
    apr_hash_t *symlink_loop;
    int (*next_object)(os_object_iter_t *oit, char **fname, int *prefix_len);
    char rp[OS_PATH_MAX];
    char prev_match[OS_PATH_MAX];
    char *realpath;
    char **key;
    void **val;
    int *v_size;
    int *v_size_user;
    int n_list;
    int v_fixed;
    int recurse_depth;
    int max_level;
    int v_max;
    int curr_level;
    int prev_match_prefix;
    int mode;
    int object_types;
    int finished;
} osf_object_iter_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    lio_os_regex_table_t *rpath;
    lio_os_regex_table_t *object_regex;
    tbx_atomic_int_t abort;
    int obj_types;
    int recurse_depth;
} osfile_remove_regex_op_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    lio_os_regex_table_t *rpath;
    lio_os_regex_table_t *object_regex;
    int recurse_depth;
    int object_types;
    char **key;
    void **val;
    char *id;
    int *v_size;
    int n_keys;
    tbx_atomic_int_t abort;
} osfile_regex_object_attr_op_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    char *src_path;
    char *dest_path;
    char *id;
    int type;
} osfile_mk_mv_rm_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    osfile_fd_t *fd;
    char **key_old;
    char **key_new;
    char *single_old;
    char *single_new;
    int n;
} osfile_move_attr_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    osfile_fd_t *fd_src;
    osfile_fd_t *fd_dest;
    char **key_src;
    char **key_dest;
    char *single_path;
    char *single_src;
    char *single_dest;
    char **src_path;
    int n;
} osfile_copy_attr_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    char *path;
    lio_os_regex_table_t *regex;
    DIR *ad;
    char *ad_path;
    os_object_iter_t *it;
    int mode;
} osfile_fsck_iter_t;

#define osf_obj_lock(lock)  apr_thread_mutex_lock(lock)
#define osf_obj_unlock(lock)  apr_thread_mutex_unlock(lock)

gop_op_status_t osf_get_multiple_attr_fn(void *arg, int id);
char *resolve_hardlink(lio_object_service_fn_t *os, char *src_path, int add_prefix);
apr_thread_mutex_t *osf_retrieve_lock(lio_object_service_fn_t *os, const char *path, int *table_slot);
int osf_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *ofd, char *attr, void *val, int v_size, int *atype, int append_val);
int osf_get_attr(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *ofd, char *attr, void **val, int *v_size, int *atype, lio_os_authz_local_t *ug, char *realpath);
gop_op_generic_t *osfile_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size);
os_attr_iter_t *osfile_create_attr_iter(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, lio_os_regex_table_t *attr, int v_max);
void osfile_destroy_attr_iter(os_attr_iter_t *oit);
gop_op_status_t osfile_open_object_fn(void *arg, int id);
gop_op_generic_t *osfile_open_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait);
gop_op_status_t osfile_close_object_fn(void *arg, int id);
gop_op_generic_t *osfile_close_object(lio_object_service_fn_t *os, os_fd_t *fd);
os_object_iter_t *osfile_create_object_iter(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        lio_os_regex_table_t *attr,  int recurse_depth, os_attr_iter_t **it_attr, int v_max);
int osfile_next_object(os_object_iter_t *oit, char **fname, int *prefix_len);
void osfile_destroy_object_iter(os_object_iter_t *it);
gop_op_status_t osf_set_multiple_attr_fn(void *arg, int id);
int lowlevel_set_attr(lio_object_service_fn_t *os, char *attr_dir, char *attr, void *val, int v_size);
char *object_attr_dir(lio_object_service_fn_t *os, char *prefix, char *path, int ftype);

//*************************************************************
// osaz_attr_filter_apply - Applies any OSAZ filter required on the attr
//*************************************************************

void osaz_attr_filter_apply(lio_os_authz_t *osa, char *key, int mode, void **value, int *len, osaz_attr_filter_t filter)
{
    void *v_out;
    int len_out;

    if (filter == NULL) return;

    filter(osa, key, mode, *value, *len, &v_out, &len_out);
    free(*value);
    *value = v_out;
    *len = len_out;
    return;
}


//*************************************************************
//  _osf_realpath - Takes the user path and converts it to the
//      "realpath" in LStore
//      include_basename = 1 means to realpath the provided path
//      include_basename = 0 means drop the basename and realpath the parent
//                           and add the basename back.
//      if NO path can be resolved NULL is returned
//*************************************************************

char *_osf_realpath(lio_object_service_fn_t *os, const char *path, char *rpath, int include_basename)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char fname[OS_PATH_MAX], real_path[OS_PATH_MAX];
    char *rp, *dir, *file;
    int n;

retry:
    dir = NULL; file = NULL;

    //** Check the Path ACL
    //** Split out the parent directory
    if (include_basename == 1) {
        dir = (char *)path;
    } else {
        lio_os_path_split(path, &dir, &file);
    }

    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, dir);
    if (dir != path) free(dir);
    rp = realpath(fname, real_path);
    if (!rp) {  //** This could be a bad symlink. If so retry but drop the basename
        if (file) free(file);
        if (include_basename == -1) return(NULL);
        include_basename = -1;
        goto retry;
    }
    strcpy(rpath, rp+osf->file_path_len);
    if (file) {  //** Need to now add the file name afte resolving the parent
        n = strlen(rpath);
        snprintf(rpath + n, OS_PATH_MAX-n, "/%s", file);
        free(file);
    }
    log_printf(15, "fname=%s  realpath=%s rp=%s strlen(realpath)=" ST "\n", path, rpath, rp, strlen(rpath));

    if (rpath[0] == '\0') {
        if ((strlen(path) == 1) && (path[0] == '/')) {
            rpath[0] = '/'; rpath[1] = 0;
        }
    }

    return(rpath);
}

//*************************************************************
//  osf_store_val - Stores the return attribute value
//*************************************************************

int osf_store_val(void *src, int src_size, void **dest, int *v_size)
{
    char *buf;

    if (*v_size > 0) {
        if (*v_size < src_size) {
            *v_size = -src_size;
            return(1);
        } else if (*v_size > src_size) {
            buf = *dest;
            buf[src_size] = 0;  //** IF have the space NULL terminate
        }
    } else if (src_size <= 0) {
        *dest = NULL;
        *v_size = src_size;
        return(0);
    } else {
        *dest = malloc(src_size+1);
        buf = *dest;
        buf[src_size] = 0;  //** IF have the space NULL terminate
    }

    *v_size = src_size;
    memcpy(*dest, src, src_size);
    return(0);
}

//*************************************************************
//  osf_make_attr_symlink - Makes an attribute symlink
//*************************************************************

void osf_make_attr_symlink(lio_object_service_fn_t *os, char *link_path, char *dest_path, char *dest_key)
{
    snprintf(link_path, OS_PATH_MAX, "%s/%s", dest_path, dest_key);
}

//*************************************************************
//  osf_resolve_attr_symlink - Resolves an attribute symlink
//*************************************************************

int osf_resolve_attr_path(lio_object_service_fn_t *os, char *real_path, char *path, char *key, int ftype, int *atype, int max_recurse)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char *attr, *dkey, *dfile;
    char *pdir, *pfile;
    int n, dtype, err;
    char fullname[OS_PATH_MAX];

    //** Get the key path
    attr = object_attr_dir(os, osf->file_path, path, ftype);
    snprintf(real_path, OS_PATH_MAX, "%s/%s", attr, key);
    *atype = lio_os_local_filetype(real_path);
    log_printf(15, "fullname=%s atype=%d\n", real_path, *atype);
    if ((*atype & OS_OBJECT_SYMLINK_FLAG) == 0) {  //** If a normal file then just return
        free(attr);
        return(0);
    }

    free(attr);

    //** It's a symlink so read it first
    n = readlink(real_path, fullname, OS_PATH_MAX-1);
    if (n < 0) {
        log_printf(0, "Bad link:  path=%s key=%s ftype=%d fullname=%s\n", path, key, ftype, fullname);
        return(1);
    }

    fullname[n] = 0;

    log_printf(15, "fullname=%s real_path=%s\n", fullname, real_path);

    //** Now split it out into object and key
    lio_os_path_split(fullname, &dfile, &dkey);

    log_printf(15, "fullname=%s dfile=%s dkey=%s\n", fullname, dfile, dkey);

    //** Find out what ftype the target is
    if (dfile[0] == '/') {
        snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, dfile);
    } else {
        if ((ftype & OS_OBJECT_DIR_FLAG) && ((ftype & OS_OBJECT_SYMLINK_FLAG) == 0)) {
            log_printf(15, "Directory so no peeling needed\n");
            snprintf(fullname, OS_PATH_MAX, "%s%s/%s", osf->file_path, path, dfile);
        } else {
            lio_os_path_split(path, &pdir, &pfile);
            snprintf(fullname, OS_PATH_MAX, "%s%s/%s", osf->file_path, pdir, dfile);
            free(pdir);
            free(pfile);
        }
    }
    log_printf(15, "fullattrpath=%s ftype=%d\n", fullname, ftype);

    dtype = lio_os_local_filetype(fullname);
    if (dtype == 0) {
        log_printf(0, "Missing object:  path=%s key=%s ftype=%d fullname=%s\n", path, key, ftype, fullname);
        free(dfile);
        free(dkey);
        return(1);
    }

    attr = object_attr_dir(os, "", fullname, dtype);
    snprintf(real_path, OS_PATH_MAX, "%s/%s", attr, dkey);

    log_printf(15, "path=%s key=%s ftype=%d real_path=%s\n", path, key, ftype, real_path);

    err = 0;
    dtype = lio_os_local_filetype(real_path);
    if (dtype & OS_OBJECT_SYMLINK_FLAG) {  //** Need to recurseively resolve the link
        if (max_recurse > 0) {
            err = osf_resolve_attr_path(os, real_path, &(fullname[osf->file_path_len]), dkey, dtype, &n, max_recurse-1);
        } else {
            log_printf(0, "Oops! Hit max recurse depth! last path=%s\n", real_path);
        }
    }

    free(attr);
    free(dfile);
    free(dkey);

    return(err);
}

//*************************************************************
// fobj_add_active - Adds the object to the active list
//*************************************************************

void fobj_add_active(fobj_lock_t *fol, osfile_fd_t *fd)
{
    tbx_stack_move_to_bottom(fol->active_stack);
    tbx_stack_insert_below(fol->active_stack, fd);
}

//*************************************************************
// fobj_remove_active - Removes the object to the active list
//*************************************************************

int fobj_remove_active(fobj_lock_t *fol, osfile_fd_t *myfd)
{
    osfile_fd_t *fd;
    int err = 1;

    tbx_stack_move_to_top(fol->active_stack);
    while ((fd = (osfile_fd_t *)tbx_stack_get_current_data(fol->active_stack)) != NULL) {
        if (fd == myfd) {  //** Found a match
            tbx_stack_delete_current(fol->active_stack, 0, 0);
            err = 0;
            break;
        }

        tbx_stack_move_down(fol->active_stack);
    }

    return(err);
}

//*************************************************************
// fobj_lock_task_new - Creates a new shelf of for object locking
//*************************************************************

void *fobj_lock_task_new(void *arg, int size)
{
    apr_pool_t *mpool = (apr_pool_t *)arg;
    fobj_lock_task_t *shelf;
    int i;

    tbx_type_malloc_clear(shelf, fobj_lock_task_t, size);

    for (i=0; i<size; i++) {
        apr_thread_cond_create(&(shelf[i].cond), mpool);
    }

    return((void *)shelf);
}

//*************************************************************
// fobj_lock_task_free - Destroys a shelf of object locking variables
//*************************************************************

void fobj_lock_task_free(void *arg, int size, void *data)
{
    fobj_lock_task_t *shelf = (fobj_lock_task_t *)data;
    int i;

    for (i=0; i<size; i++) {
        apr_thread_cond_destroy(shelf[i].cond);
    }

    free(shelf);
    return;
}

//*************************************************************
// fobj_lock_new - Creates a new shelf of for object locking
//*************************************************************

void *fobj_lock_new(void *arg, int size)
{
    fobj_lock_t *shelf;
    int i;

    tbx_type_malloc_clear(shelf, fobj_lock_t, size);

    for (i=0; i<size; i++) {
        shelf[i].active_stack = tbx_stack_new();
        shelf[i].pending_stack = tbx_stack_new();
        shelf[i].read_count = 0;
        shelf[i].write_count = 0;
    }

    return((void *)shelf);
}

//*************************************************************
// fobj_lock_free - Destroys a shelf of object locking variables
//*************************************************************

void fobj_lock_free(void *arg, int size, void *data)
{
    fobj_lock_t *shelf = (fobj_lock_t *)data;
    int i;

    for (i=0; i<size; i++) {
        tbx_stack_free(shelf[i].active_stack, 0);
        tbx_stack_free(shelf[i].pending_stack, 0);
    }

    free(shelf);
    return;
}

//*************************************************************
// fobj_lock_create - Creates a file object locking structure
//*************************************************************

fobject_lock_t *fobj_lock_create()
{
    fobject_lock_t *fol;

    tbx_type_malloc_clear(fol, fobject_lock_t, 1);
    apr_pool_create(&(fol->mpool), NULL);
    apr_thread_mutex_create(&(fol->fobj_lock), APR_THREAD_MUTEX_DEFAULT, fol->mpool);
    fol->fobj_table = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
    fol->fobj_pc = tbx_pc_new("fobj_pc", 50, sizeof(fobj_lock_t), fol->mpool, fobj_lock_new, fobj_lock_free);
    fol->task_pc = tbx_pc_new("fobj_task_pc", 50, sizeof(fobj_lock_task_t), fol->mpool, fobj_lock_task_new, fobj_lock_task_free);

    return(fol);
}

//*************************************************************
// fobj_lock_create - Creates a file object locking structure
//*************************************************************

void fobj_lock_destroy(fobject_lock_t *fol)
{
    apr_thread_mutex_destroy(fol->fobj_lock);
    tbx_list_destroy(fol->fobj_table);
    tbx_pc_destroy(fol->fobj_pc);
    tbx_pc_destroy(fol->task_pc);
    apr_pool_destroy(fol->mpool);
    free(fol);
}

//***********************************************************************
// fobj_wait - Waits for my turn to access the object
//    NOTE: On entry I should be holding osf->fobj_lock
//          The lock is cycled in the routine
//***********************************************************************

int fobj_wait(fobject_lock_t *flock, fobj_lock_t *fol, osfile_fd_t *fd, int rw_mode, int max_wait)
{
    tbx_pch_t task_pch;
    fobj_lock_task_t *handle;
    int aborted, dummy;
    apr_time_t timeout = apr_time_make(max_wait, 0);
    apr_time_t dt, start_time;
    tbx_stack_ele_t *ele;

    //** Get my slot
    task_pch = tbx_pch_reserve(flock->task_pc);
    handle = (fobj_lock_task_t *)tbx_pch_data(&task_pch);
    handle->fd = fd;
    handle->abort = 1;
    handle->rw_mode = rw_mode;

    log_printf(15, "SLEEPING id=%s fname=%s mymode=%d read_count=%d write_count=%d handle->fd->uuid=" LU " max_wait=%d\n", fd->id, fd->object_name, rw_mode, fol->read_count, fol->write_count, handle->fd->uuid, max_wait);

    tbx_stack_move_to_bottom(fol->pending_stack);
    tbx_stack_insert_below(fol->pending_stack, handle);
    ele = tbx_stack_get_current_ptr(fol->pending_stack);

    //** Sleep until it's my turn.  Remember fobj_lock is already set upon entry
    start_time = apr_time_now();
    apr_thread_cond_timedwait(handle->cond, flock->fobj_lock, timeout);
    aborted = handle->abort;

    dt = apr_time_now() - start_time;

    dummy = apr_time_sec(dt);
    log_printf(15, "CHECKING id=%s fname=%s mymode=%d read_count=%d write_count=%d handle=%p abort=%d uuid=" LU " dt=%d\n", fd->id, fd->object_name, rw_mode, fol->read_count, fol->write_count, handle, aborted, fd->uuid, dummy);

    tbx_stack_move_to_top(fol->pending_stack);
    if (tbx_stack_get_current_ptr(fol->pending_stack) != ele) { //** I should be on top of the stack
        aborted = 1;
    }

    dummy = apr_time_sec(dt);
    log_printf(15, "AWAKE id=%s fname=%s mymode=%d read_count=%d write_count=%d handle=%p abort=%d uuid=" LU " dt=%d\n", fd->id, fd->object_name, rw_mode, fol->read_count, fol->write_count, handle, aborted, fd->uuid, dummy);

    //** Remove myself
    tbx_stack_move_to_ptr(fol->pending_stack, ele);
    tbx_stack_delete_current(fol->pending_stack, 0, 0);

    //** I'm off the stack so just free my handle and update the counter
    tbx_pch_release(flock->task_pc, &task_pch);

    if (aborted == 1) { //** Open was aborted. I've already removed mysewlf from the que so just return
        return(1);
    }

    //** Check if the next person should be woke up as well
    if (tbx_stack_count(fol->pending_stack) != 0) {
        tbx_stack_move_to_top(fol->pending_stack);
        handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack);

        if ((rw_mode == OS_MODE_READ_BLOCKING) && (handle->rw_mode == OS_MODE_READ_BLOCKING)) {
            log_printf(15, "WAKEUP ALARM id=%s uuid=" LU "fname=%s mymode=%d read_count=%d write_count=%d handle->fd->uuid=" LU " handle->mode=%d\n", fd->id, fd->uuid, fd->object_name, rw_mode, fol->read_count, fol->write_count, handle->fd->uuid, handle->rw_mode);

            handle->abort = 0;
            apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released in the calling routine
        }
    }

    return(0);
}

//***********************************************************************
// full_object_lock -  Locks the object across all systems
//***********************************************************************

int full_object_lock(int fol_slot, fobject_lock_t *flock, osfile_fd_t *fd, int rw_lock, int max_wait)
{
    tbx_pch_t obj_pch;
    fobj_lock_t *fol;
    fobj_lock_task_t *handle;
    int err, rw_mode, do_wait;

    rw_mode = rw_lock; do_wait = 1;
    if (rw_lock & OS_MODE_NONBLOCKING) {
        rw_mode = rw_lock - OS_MODE_NONBLOCKING;
        do_wait = 0;
    }

    if (rw_mode & OS_MODE_READ_IMMEDIATE) return(0);

    apr_thread_mutex_lock(flock->fobj_lock);

    //** See if we already have a reference stored
    fol = (fd->fol[fol_slot]) ? fd->fol[fol_slot] : tbx_list_search(flock->fobj_table, fd->realpath);

    if (fol == NULL) {  //** No one else is accessing the file
        obj_pch =  tbx_pch_reserve(flock->fobj_pc);
        fol = (fobj_lock_t *)tbx_pch_data(&obj_pch);
        fol->pch = obj_pch;  //** Reverse link my PCH for release later
        tbx_list_insert(flock->fobj_table, fd->realpath, fol);
        log_printf(15, "fname=%s new lock!\n", fd->object_name);
    }

    if (fd->fol[fol_slot] == NULL) fd->fol[fol_slot] = fol;  //** Go ahead and store if for future reference

    log_printf(15, "START id=%s uuid=" LU " fname=%s mymode=%d do_wait=%d read_count=%d write_count=%d\n", fd->id, fd->uuid, fd->object_name, rw_mode, do_wait, fol->read_count, fol->write_count);

    err = 0;
    if (rw_mode & OS_MODE_READ_BLOCKING) { //** I'm reading
        if (fol->write_count == 0) { //** No one currently writing
            //** Check and make sure the person waiting isn't a writer
            if (tbx_stack_count(fol->pending_stack) != 0) {
                tbx_stack_move_to_top(fol->pending_stack);
                handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack);
                if (handle->rw_mode & OS_MODE_WRITE_BLOCKING) {  //** They want to write so sleep until my turn
                    err = (do_wait) ? fobj_wait(flock, fol, fd, rw_mode, max_wait) : 1;  //** The fobj_lock is released/acquired inside
                }
            }
        } else {
            err = (do_wait) ? fobj_wait(flock, fol, fd, rw_mode, max_wait) : 1;  //** The fobj_lock is released/acquired inside
        }

        if (err == 0) fol->read_count++;
    } else {   //** I'm writing
        if ((fol->write_count != 0) || (fol->read_count != 0) || (tbx_stack_count(fol->pending_stack) != 0)) {  //** Make sure no one else is doing anything
            err = (do_wait) ? fobj_wait(flock, fol, fd, rw_mode, max_wait) : 1;  //** The fobj_lock is released/acquired inside
        }
        if (err == 0) fol->write_count++;
    }

    if (err == 0) fobj_add_active(fol, fd);

    log_printf(15, "END id=%s uuid=" LU " fname=%s mymode=%d read_count=%d write_count=%d\n", fd->id, fd->uuid, fd->object_name, fd->mode, fol->read_count, fol->write_count);

    apr_thread_mutex_unlock(flock->fobj_lock);

    return(err);
}

//***********************************************************************
// full_object_unlock - Unlocks an object
//***********************************************************************

void full_object_unlock(int fol_slot, fobject_lock_t *flock, osfile_fd_t *fd, int rw_mode)
{
    fobj_lock_t *fol;
    fobj_lock_task_t *handle;
    int err;

    fol = fd->fol[fol_slot];
    if (fol == NULL) { return; }  //** Nothing to do. No outstanding lock

    apr_thread_mutex_lock(flock->fobj_lock);

    err = fobj_remove_active(fol, fd);

    if (err != 0) {  //**Exit if it wasn't found
        apr_thread_mutex_unlock(flock->fobj_lock);
        return;
    }

    //** Update the counts
    if (rw_mode & OS_MODE_READ_BLOCKING) {
        fol->read_count--;
    } else {
        fol->write_count--;
    }

    log_printf(15, "fname=%s mymode=%d read_count=%d write_count=%d fd->id=%s fd->uuid=" LU "\n", fd->object_name, fd->mode, fol->read_count, fol->write_count, fd->id, fd->uuid);

    if ((tbx_stack_count(fol->pending_stack) == 0) && (fol->read_count == 0) && (fol->write_count == 0)) {  //** No one else is waiting so remove the entry
        tbx_list_remove(flock->fobj_table, fd->realpath, NULL);
        tbx_pch_release(flock->fobj_pc, &(fol->pch));
    } else if (tbx_stack_count(fol->pending_stack) > 0) { //** Wake up the next person
        tbx_stack_move_to_top(fol->pending_stack);
        handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack);

        if (((handle->rw_mode & OS_MODE_READ_BLOCKING) && (fol->write_count == 0)) ||
                ((handle->rw_mode & OS_MODE_WRITE_BLOCKING) && (fol->write_count == 0) && (fol->read_count == 0))) {
            log_printf(15, "WAKEUP ALARM fname=%s mymode=%d read_count=%d write_count=%d handle->mode=%d handle->fd->id=%s handle->fd->uuid=" LU "\n", fd->object_name, rw_mode, fol->read_count, fol->write_count, handle->rw_mode, handle->fd->id, handle->fd->uuid);
            handle->abort = 0;
            apr_thread_cond_broadcast(handle->cond);   //** They will wake up when fobj_lock is released in the calling routine
        }
    }

    apr_thread_mutex_unlock(flock->fobj_lock);
}

//***********************************************************************
// full_object_downgrade_lock - Downgrades the lock from a WRITE to READ lock
//***********************************************************************

void full_object_downgrade_lock(int fol_slot, fobject_lock_t *flock, osfile_fd_t *fd)
{
    fobj_lock_t *fol;
    fobj_lock_task_t *handle;

    fol = fd->fol[fol_slot];
    if (fol == NULL) { return; };  //** No lock currently stored so kick out

    apr_thread_mutex_lock(flock->fobj_lock);

    //** Adjust the counts
    fol->write_count--;
    fol->read_count++;

    log_printf(15, "fname=%s mymode=%d read_count=%d write_count=%d\n", fd->object_name, fd->mode, fol->read_count, fol->write_count);

    if (tbx_stack_count(fol->pending_stack) > 0) { //** Wake up the next person if also a reader
        tbx_stack_move_to_top(fol->pending_stack);
        handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack);

        if (handle->rw_mode & OS_MODE_READ_BLOCKING) {
            log_printf(15, "WAKEUP ALARM fname=%s mymode=%d read_count=%d write_count=%d handle->fd->uuid=" LU "\n", fd->object_name, fd->mode, fol->read_count, fol->write_count, handle->fd->uuid);
            handle->abort = 0;
            apr_thread_cond_broadcast(handle->cond);   //** They will wake up when fobj_lock is released in the calling routine
        }
    }

    apr_thread_mutex_unlock(flock->fobj_lock);
}

//***********************************************************************
// osf_multi_unlock - Releases a collection of locks
//***********************************************************************

void osf_multi_unlock(apr_thread_mutex_t **lock, int n)
{
    int i;

    for (i=0; i<n; i++) {
        apr_thread_mutex_unlock(lock[i]);
    }

    return;
}

//***********************************************************************

int compare_int(const void *av, const void *bv)
{
    const int a = *(const int *)av;
    const int b = *(const int *)bv;

    if (a>b) {
        return(1);
    } else if (a==b) {
        return(0);
    }

    return(-1);
}

//***********************************************************************
// osf_compact_ilocks - Compact the list of lock indexes down to an irreducible
//     in ascending order.  The ilock table is MODIFIED so the that unique
//     locks are stored at the beginning.
// Return
//     The number of unique locks are returned.
//
//***********************************************************************

int osf_compact_ilocks(int n_locks, int *ilock)
{
    int i, n;

    //** Sort them
    qsort(ilock, n_locks, sizeof(int), compare_int);

    //** Make them unique
    n = 0;
    for (i=1; i<n_locks; i++) {
        if (ilock[n] != ilock[i]) {
            n++;
            ilock[n] = ilock[i];
        }
    }

    n++;
    return(n);
}

//***********************************************************************
// osf_multi_attr_lock - Used to resolve/lock a collection of attrs that are
//     links
//***********************************************************************

void osf_multi_attr_lock(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *fd, char **key, int n_keys, int first_link, apr_thread_mutex_t **lock_table, int *n_locks)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i, j, n, len, atype, small_slot, small_index, max_index;
    int slot_rp, slot_obj;
    int lock_slot[n_keys+2];
    char linkname[OS_PATH_MAX];
    char rpath[OS_PATH_MAX];

    //** Always get the primary
try_again:
    n = 0;
    slot_rp = tbx_atomic_get(fd->ilock_rp);
    lock_slot[n] = slot_rp;
    lock_table[n] = osf->internal_lock[slot_rp];
    n++;
    slot_obj = tbx_atomic_get(fd->ilock_obj);
    if (slot_obj != slot_rp) {
        lock_slot[n] = slot_obj;
        lock_table[n] = osf->internal_lock[slot_obj];
        n++;
    }

    log_printf(15, "lock_slot[0]=%d n=%d fname=%s rp=%s\n", lock_slot[0], n, fd->object_name, fd->realpath);

    //** Now cycle through the attributes starting with the 1 that triggered the call
    linkname[sizeof(linkname)-1] = 0;
    for (i=first_link; i<n_keys; i++) {
        len = osf_resolve_attr_path(os, linkname, fd->object_name, key[i], fd->ftype, &atype, 20);
        log_printf(15, "i=%d len=%d fname=%s key=%s linkname=%s\n", i, len, fd->object_name, key[i], linkname);
        if ((atype & OS_OBJECT_SYMLINK_FLAG) && (len > 0)) {
            j=len-1;  //** Peel off the key name.  We only need the parent object path
            while (linkname[j] != '/' && (j>0)) {
                j--;
            }
            linkname[j] = 0;
            lock_table[n] = osf_retrieve_lock(os, _osf_realpath(os, linkname + osf->file_path_len, rpath, 1), &lock_slot[n]);
            log_printf(15, "checking n=%d key=%s lname=%s lrp=%s lock_slot=%d j=%d\n", n, key[i], linkname, rpath, lock_slot[n], j);

            //** Make sure I don't already have it in the list
            for (j=0; j<n; j++) {
                if (lock_slot[n] == lock_slot[j]) {
                    n--;
                    break;
                }
            }
            n++;
        }
    }

    log_printf(15, "n_locks=%d\n", n);

    *n_locks = n;  //** Return the lock count

    //** This is done naively cause normally there will be just a few locks
    max_index = osf->internal_lock_size;
    for (i=0; i<n; i++) {
        small_slot = -1;
        small_index = max_index;
        for (j=0; j<n; j++) {
            if (small_index > lock_slot[j]) {
                small_index = lock_slot[j];
                small_slot = j;
            }
        }

        apr_thread_mutex_lock(lock_table[small_slot]);
        lock_slot[small_slot] = max_index;
    }

    //** Make sure the fd slot didn't change
    i = tbx_atomic_get(fd->ilock_rp);
    if ((tbx_atomic_get(fd->ilock_rp) != slot_rp) || (tbx_atomic_get(fd->ilock_obj) != slot_obj)) { //** If so unlock everything and try again
        osf_multi_unlock(lock_table, n);
        goto try_again;
    }

    return;
}

//***********************************************************************
// osf_internal_fd_lock - Locking for multiple FDs
//***********************************************************************

void osf_internal_2fd_lock(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *fd1, osfile_fd_t *fd2, apr_thread_mutex_t **lock_table, int *n_locks)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i;
    int lock_slot[4];

    //** Always get the primary
try_again:
    lock_slot[0] = tbx_atomic_get(fd1->ilock_obj);
    lock_slot[1] = tbx_atomic_get(fd1->ilock_rp);
    lock_slot[2] = tbx_atomic_get(fd2->ilock_obj);
    lock_slot[3] = tbx_atomic_get(fd2->ilock_rp);

    log_printf(15, "lock_slot[0]=%d fname1=%s\n", lock_slot[0], fd1->object_name);

    //** Sort and compact them
    *n_locks = osf_compact_ilocks(4, lock_slot);

    //** Now acquire them in order from smallest->largest
    for (i=0; i<(*n_locks); i++) {
        lock_table[i] = osf->internal_lock[lock_slot[i]];
        apr_thread_mutex_lock(lock_table[i]);
    }

    //** Make sure the fd slot didn't change
    if ((tbx_atomic_get(fd1->ilock_obj) != lock_slot[0]) || (tbx_atomic_get(fd1->ilock_rp) != lock_slot[1]) ||
        (tbx_atomic_get(fd2->ilock_obj) != lock_slot[2]) || (tbx_atomic_get(fd2->ilock_rp) != lock_slot[3])) { //** If so unlock everything and try again
        osf_multi_unlock(lock_table, *n_locks);
        goto try_again;
    }

    return;
}

//***********************************************************************

int mycompare(int n_prefix, const char *prefix, const char *fname)
{
    int cmp;

    if (!fname) return(1);  //** No file

    cmp = strncmp(prefix, fname, n_prefix);
    if (cmp == 0) {
        if ((fname[n_prefix] == 0) || (fname[n_prefix] == '/')) {
            cmp = 0;
        } else {
            cmp = 1;
        }
     }

     return(cmp);
}

//***********************************************************************
// _get_matching_fobj_locks - Gets the matching locks to the prefix for open files
//     Returns the number of locks added to the list.
//
// NOTE: The flock structure should be locked by the calling program and a if the table is to small
//***********************************************************************

int _get_matching_fobj_locks(lio_object_service_fn_t *os, fobject_lock_t *flock, const char *prefix, int *lock_index, int max_locks)
{
    int n, n_prefix;
    fobj_lock_t *fol;
    tbx_list_iter_t it;
    char *fname;

    n_prefix = strlen(prefix);
    n = 0;
    it = tbx_list_iter_search(flock->fobj_table, (char *)prefix, 0);
    tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fol);
    while (mycompare(n_prefix, prefix, fname) == 0) {
        if (n < max_locks) osf_retrieve_lock(os, fname, &(lock_index[n]));
        n++;
        tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fol);
    }

    return(n);
}

//***********************************************************************
// _get_matching_fobj_locks_and_alloc - Same as _get_matching_fobj_locks but iterates allocating space as needed
//***********************************************************************

int _get_matching_fobj_locks_and_alloc(lio_object_service_fn_t *os, fobject_lock_t *flock, const char *prefix, int n_used, int *max_locks, int **lock_index, int do_lock)
{
    int k, nleft;
    int *ilock = *lock_index;

    //** Now add the os_lock
again:
    nleft = (*max_locks) - n_used;
    if (do_lock) apr_thread_mutex_lock(flock->fobj_lock);
    k = _get_matching_fobj_locks(os, flock, prefix, ilock + n_used, nleft);
    if (do_lock) apr_thread_mutex_unlock(flock->fobj_lock);

    if (nleft < k) {
        *max_locks = 2 * (*max_locks);
        if (k> (*max_locks)) *max_locks = 2*(k + n_used);
        tbx_type_realloc(ilock, int, *max_locks);
        goto again;
    }

    //** Update the count
    n_used = n_used + k;

    return(n_used);
}


//***********************************************************************
// _get_matching_open_fd_locks - Gets the matching open FD locks to the prefix for open files
//     Returns the number of locks added to the list.
//
// NOTE: The open_fd_lock structure should be locked by the calling program and a if the table is to small
//***********************************************************************

int _get_matching_open_fd_locks(lio_object_service_fn_t *os, const char *prefix, int *lock_index, int max_locks)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int n, n_prefix;
    osfile_fd_t *fd;
    tbx_list_iter_t it;
    char *fname;

    n_prefix = strlen(prefix);
    n = 0;
    it = tbx_list_iter_search(osf->open_fd, (char *)prefix, 0);
    tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fd);
    while (mycompare(n_prefix, prefix, fname) == 0) {
        if (n < max_locks) osf_retrieve_lock(os, fname, &(lock_index[n]));
        n++;
        tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fd);
    }

    return(n);
}

//***********************************************************************
// _get_matching_open_fd_locks_and_alloc - Same as _get_matching_open_fd_locks but iterates allocating space as needed
//***********************************************************************

int _get_matching_open_fd_locks_and_alloc(lio_object_service_fn_t *os, const char *prefix, int n_used, int *max_locks, int **lock_index, int do_lock)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int k, nleft;
    int *ilock = *lock_index;

    //** Now add the open FD locks
again:
    nleft = (*max_locks) - n_used;
    if (do_lock) apr_thread_mutex_lock(osf->open_fd_lock);
    k = _get_matching_open_fd_locks(os, prefix, ilock + n_used, nleft);
    if (do_lock) apr_thread_mutex_unlock(osf->open_fd_lock);

    if (nleft < k) {
        *max_locks = 2 * (*max_locks);
        if (k> (*max_locks)) *max_locks = 2*(k + n_used);
        tbx_type_realloc(ilock, int, *max_locks);
        goto again;
    }

    //** Update the count
    n_used = n_used + k;

    return(n_used);
}

//***********************************************************************
// osf_match_fobj_lock_try - Locks all the matching prefix/file objects and and any open objects
//  NOTE: The flock structs are locked when returned
//***********************************************************************

int osf_match_fobj_lock_try(lio_object_service_fn_t *os, const char *rp_src, const char *rp_dest, const char *obj_src, const char *obj_dest, int *max_locks, int **lock_index, int do_lock)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int n, i;
    int *ilock = *lock_index;

    if (*max_locks <= 4) {
        *max_locks = 1024;
        tbx_type_malloc_clear(ilock, int, *max_locks);
        *lock_index = ilock;
    }

    //** Add the src and dest to the locks initially
    osf_retrieve_lock(os, rp_src, &(ilock[0]));
    osf_retrieve_lock(os, rp_dest, &(ilock[1]));
    osf_retrieve_lock(os, obj_src, &(ilock[2]));
    osf_retrieve_lock(os, obj_dest, &(ilock[3]));
    n = 4;

    //** Now add the os_locks and os_user_locks
    n = _get_matching_fobj_locks_and_alloc(os, osf->os_lock, rp_src, n, max_locks, lock_index, do_lock);
    n = _get_matching_fobj_locks_and_alloc(os, osf->os_lock, rp_dest, n, max_locks, lock_index, do_lock);
    n = _get_matching_fobj_locks_and_alloc(os, osf->os_lock_user, rp_src, n, max_locks, lock_index, do_lock);
    n = _get_matching_fobj_locks_and_alloc(os, osf->os_lock_user, rp_dest, n, max_locks, lock_index, do_lock);

    //** And the open FD locks
    n = _get_matching_open_fd_locks_and_alloc(os, obj_src, n, max_locks, lock_index, do_lock);
    n = _get_matching_open_fd_locks_and_alloc(os, obj_dest, n, max_locks, lock_index, do_lock);

    //** Compact them
    n = osf_compact_ilocks(n, ilock);

    if (do_lock == 0) return(n);

    for (i=0; i<n; i++) {
        apr_thread_mutex_lock(osf->internal_lock[ilock[i]]);
    }

    //** And the lock table's
    apr_thread_mutex_lock(osf->os_lock->fobj_lock);
    apr_thread_mutex_lock(osf->os_lock_user->fobj_lock);

    return(n);
}


//***********************************************************************
// osf_match_fobj_unlock - Unlocks all the matching prefix/file objects and and any open objects
//  NOTE: The flock structs are unlocked when returned
//***********************************************************************

void osf_match_fobj_unlock(lio_object_service_fn_t *os, int n_locks, int *lock_index)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i;

    apr_thread_mutex_unlock(osf->os_lock->fobj_lock);
    apr_thread_mutex_unlock(osf->os_lock_user->fobj_lock);
    apr_thread_mutex_unlock(osf->open_fd_lock);

    for (i=0; i<n_locks; i++) {
        apr_thread_mutex_unlock(osf->internal_lock[lock_index[i]]);
    }
}


//***********************************************************************
// _compare_fobj_locks - Compares 2 lists of objects and makes sure that
//    all indices in lock2 are in lock1.  On success 0 is returned 1 otherwise
//***********************************************************************

int _compare_fobj_locks(int n1, int *ilock1, int n2, int *ilock2)
{
    int i1, i2;

    if (n2>n1) return(1);  //** The 2nd is to big

    i1 = 0;
    for (i2=0; i2<n2; i2++) {
        while ((i1>=n1) || (ilock2[i2] > ilock1[i1])) {
            i1++;
        }
        if (ilock2[i2] != ilock1[i1]) return(1);  //** Got a miss
        i1++;  //** Got a match so skip to the next index for
    }

    return(0);
}

//***********************************************************************
// osf_match_fobj_lock - Locks all the matching prefix/file objects and and any open objects
//  NOTE: The flock structs are locked when returned
//***********************************************************************

int osf_match_fobj_lock(lio_object_service_fn_t *os, const char *rp_src, const char *rp_dest, const char *obj_src, const char *obj_dest, int max_locks, int **lock_index, int do_lock)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int n1, n2, max1, max2, i;
    int *ilock1;
    int *ilock2;

    max1 = max_locks;
    ilock1 = *lock_index;
    max2 = 0;
    ilock2 = NULL;

    n1 = osf_match_fobj_lock_try(os, rp_src, rp_dest, obj_src, obj_dest, &max1, &ilock1, 1);  //** Get the inital set
again:
    n2 = osf_match_fobj_lock_try(os, rp_src, rp_dest, obj_src, obj_dest, &max2, &ilock2, 0);  //** again but under the locks

    //**Compare lock tables to see if we're Ok.
    if (_compare_fobj_locks(n1, ilock1, n2, ilock2) != 0) {  //** They aren't a subset so got to do it again
        osf_match_fobj_unlock(os, n1, ilock1);  //** Unlock the base set
        if (ilock1 != *lock_index) free(ilock1);  //** Free the space
        ilock1 = ilock2; ilock2 = NULL;   //** And swap the sets
        n1 = n2; n2 = 0;
        max1 = max2; max2 = 0;

        //** Lock the new set
        for (i=0; i<n1; i++) {
            apr_thread_mutex_lock(osf->internal_lock[ilock1[i]]);
        }
        apr_thread_mutex_lock(osf->os_lock->fobj_lock);
        apr_thread_mutex_lock(osf->os_lock_user->fobj_lock);
        apr_thread_mutex_lock(osf->open_fd_lock);

        goto again;  //** And try again
    } else {
        if (ilock2) free(ilock2);
    }

    *lock_index = ilock1;
    return(n1);
}


//***********************************************************************
// _update_fobj_path_active_stack - Does the FD path update for active locks
//***********************************************************************

void _update_fobj_path_active_stack(int n_old, int n_new, const char *rp_new, tbx_stack_t *stack)
{
    osfile_fd_t *fd;
    char fname[OS_PATH_MAX];

    //** The active stack has FD's on it
    tbx_stack_move_to_top(stack);
    while ((fd = (osfile_fd_t *)tbx_stack_get_current_data(stack)) != NULL) {
        snprintf(fname, OS_PATH_MAX, "%s%s", rp_new, fd->realpath + n_old);
        strcpy(fd->realpath, fname);
        tbx_stack_move_down(stack);
    }

    return;
}

//***********************************************************************
// _update_fobj_path_pending_stack - Does the FD path update for pending locks
//***********************************************************************

void _update_fobj_path_pending_stack(int n_old, int n_new, const char *rp_new, tbx_stack_t *stack)
{
    fobj_lock_task_t *ftask;
    osfile_fd_t *fd;
    char fname[OS_PATH_MAX];

    //** The pending stack has task handles
    tbx_stack_move_to_top(stack);
    while ((ftask = (fobj_lock_task_t *)tbx_stack_get_current_data(stack)) != NULL) {
        fd = ftask->fd;
        snprintf(fname, OS_PATH_MAX, "%s%s", rp_new, fd->realpath + n_old);
        strcpy(fd->realpath, fname);
        tbx_stack_move_down(stack);
    }

    return;
}

//***********************************************************************
// _update_fobj_path_entry - Updates the fol individual fd paths
//     NOTE: Assummes the internal locks are all held.
//***********************************************************************

void _update_fobj_path_entry(fobject_lock_t *flock, int n_old, int n_new, const char *rp_new, char *fname, fobj_lock_t *fol)
{
    //** Update the individual FDs
    _update_fobj_path_pending_stack(n_old, n_new, rp_new, fol->pending_stack);
    _update_fobj_path_active_stack(n_old, n_new, rp_new, fol->active_stack);

    return;
}

//***********************************************************************
// _osf_update_fobj_path - Updates all the open FD's fnames with the updated prefix from the rename
//   NOTE: The flock and all internal FDs to be modified should be locked!
//***********************************************************************

void _osf_update_fobj_path(lio_object_service_fn_t *os, fobject_lock_t *flock, const char *rp_old, const char *rp_new)
{
    int n_old, n_new, cmp;
    fobj_lock_t *fol;
    tbx_list_iter_t it;
    char *fname;
    char fnew[OS_PATH_MAX];

    n_old = strlen(rp_old);
    n_new = strlen(rp_new);
    do {
        it = tbx_list_iter_search(flock->fobj_table, (char *)rp_old, 0);
        tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fol);
        cmp = mycompare(n_old, rp_old, fname);
        if (cmp == 0) {  //** Got a match
            _update_fobj_path_entry(flock, n_old, n_new, rp_new, fname, fol); //** Upate the entry
            snprintf(fnew, OS_PATH_MAX, "%s%s", rp_new, fname + n_old);  //** Make the new path

            //** Remove/add the new entry list entry
            tbx_list_iter_remove(&it);
            tbx_list_insert(flock->fobj_table, fnew, fol);

            //** Need to restart the iter since we did an update
            it = tbx_list_iter_search(flock->fobj_table, (char *)rp_old, 0);
            tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fol);
        }
    } while (cmp == 0);

    return;
}


//***********************************************************************
// _osf_update_open_fd_path - Updates all the open FD's fnames with the updated prefix from the rename
//   NOTE: The open_fd_lock and all internal FDs to be modified should be locked!
//***********************************************************************

void _osf_update_open_fd_path(lio_object_service_fn_t *os, const char *prefix_old, const char *prefix_new)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int n_old, cmp;
    osfile_fd_t *fd;
    tbx_list_iter_t it;
    char *fname;
    char fnew[OS_PATH_MAX];

    n_old = strlen(prefix_old);
    do {
        it = tbx_list_iter_search(osf->open_fd, (char *)prefix_old, 0);
        tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fd);
        cmp = mycompare(n_old, prefix_old, fname);
        if (cmp == 0) {  //** Got a match
            snprintf(fnew, OS_PATH_MAX, "%s%s", prefix_new, fname + n_old);  //** Make the new path
            free(fd->object_name);
            fd->object_name = strdup(fnew);

            //** Remove/add the new entry list entry
            tbx_list_iter_remove(&it);
            tbx_list_insert(osf->open_fd, fnew, fd);

            //** Need to restart the iter since we did an update
            it = tbx_list_iter_search(osf->open_fd, (char *)prefix_old, 0);
            tbx_list_next(&it, (tbx_list_key_t **)&fname, (tbx_list_data_t **)&fd);
        }
    } while (cmp == 0);

    return;
}


//***********************************************************************
// va_create_get_attr - Returns the object creation time in secs since epoch
//***********************************************************************

int va_create_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    struct stat s;
    int  bufsize, err;
    uint64_t dt;
    char buffer[32];
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);
    err = stat(fullname, &s);
    if (err != 0) {
        *v_size = -1;
        return(1);
    }

    dt = s.st_ctime;  //** Linux doesn't really have a creation time but we don;t touch the proxy after creation
    snprintf(buffer, sizeof(buffer), LU , dt);
    bufsize = strlen(buffer);

    log_printf(15, "fname=%s sec=%s\n", fd->object_name, buffer);

    return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// va_realpath_attr - Returns the object realpath information
//***********************************************************************

int va_realpath_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;

    *atype = OS_OBJECT_VIRTUAL_FLAG;
    return(osf_store_val(fd->realpath, strlen(fd->realpath), val, v_size));
}

//***********************************************************************
// va_link_get_attr - Returns the object link information
//***********************************************************************

int va_link_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    struct stat s;
    char buffer[32*1024];
    int err, n, offset;
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);

    err = lstat(fullname, &s);
    if (err == 0) {
        if (S_ISLNK(s.st_mode) == 0) {
            *v_size = 0;
            *val = NULL;
            return(0);
        }

        n = readlink(fullname, buffer, sizeof(buffer)-1);
        if (n > 0) {
            buffer[n] = 0;
            log_printf(15, "file_path=%s fullname=%s link=%s\n", osf->file_path, fullname, buffer);

            if (buffer[0] == '/') {
                offset = osf->file_path_len;
                n = n - offset;
                return(osf_store_val(&(buffer[offset]), n, val, v_size));
            } else {
                return(osf_store_val(buffer, n, val, v_size));
            }
        }
    }

    *v_size = 0;

    return(0);
}

//***********************************************************************
// va_link_count_get_attr - Returns the object link count information
//***********************************************************************

int va_link_count_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    struct stat s;
    char buffer[32];
    int err, n;
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);

    err = lstat(fullname, &s);
    if (err == 0) {
        n = s.st_nlink;
        if (S_ISDIR(s.st_mode)) {
            n = n - 1;  //** IF a dir don't count the attribute dir
        } else if ( n > 1) { //** Normal files should only have 1.  If more then it's a hardlink so tweak it
            n = n - 1;
        }
    } else {
        n = 1;   //** Dangling link probably
    }

    err = snprintf(buffer, 32, "%d", n);
    return(osf_store_val(buffer, err, val, v_size));
}

//***********************************************************************
// va_type_get_attr - Returns the object type information
//***********************************************************************

int va_type_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    int ftype, bufsize;
    char buffer[32];
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fd->object_name);
    ftype = lio_os_local_filetype(fullname);

    snprintf(buffer, sizeof(buffer), "%d", ftype);
    bufsize = strlen(buffer);

    log_printf(15, "fname=%s type=%s v_size=%d\n", fd->object_name, buffer, *v_size);

    return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// _lock_get_attr - Returns the lock attribute value
//***********************************************************************

void _lock_get_attr(fobj_lock_t *fol, char *buf, int *used, int bufsize, int is_lock_user)
{
    fobj_lock_task_t *handle;
    osfile_fd_t *pfd;
    int mode;

    //** Print the active info
    if (fol->read_count > 0) {
        tbx_append_printf(buf, used, bufsize, "active_mode=READ\n");
        tbx_append_printf(buf, used, bufsize, "active_count=%d\n", fol->read_count);
    } else {
        tbx_append_printf(buf, used, bufsize, "active_mode=WRITE\n");
        tbx_append_printf(buf, used, bufsize, "active_count=%d\n", fol->write_count);
    }

    //** The active stack just has FDs
    tbx_stack_move_to_top(fol->active_stack);
    while ((pfd = (osfile_fd_t *)tbx_stack_get_current_data(fol->active_stack)) != NULL) {
        mode = (is_lock_user) ? pfd->user_mode : pfd->mode;
        if (mode & OS_MODE_READ_BLOCKING) {
            tbx_append_printf(buf, used, bufsize, "active_id=%s:" LU ":READ\n", pfd->id, pfd->uuid);
        } else if (mode & OS_MODE_WRITE_BLOCKING) {
            tbx_append_printf(buf, used, bufsize, "active_id=%s:" LU ":WRITE\n", pfd->id, pfd->uuid);
        } else {
            tbx_append_printf(buf, used, bufsize, "active_id=%s:" LU ":UNKNOWN(%d)\n", pfd->id, pfd->uuid, mode);
        }
        tbx_stack_move_down(fol->active_stack);
    }

    //** The pending task has a lock handle
    tbx_append_printf(buf, used, bufsize, "\n");
    tbx_append_printf(buf, used, bufsize, "pending_count=%d\n", tbx_stack_count(fol->pending_stack));
    tbx_stack_move_to_top(fol->pending_stack);
    while ((handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack)) != NULL) {
        mode = handle->rw_mode;
        pfd = handle->fd;
        if (mode & OS_MODE_READ_BLOCKING) {
            tbx_append_printf(buf, used, bufsize, "pending_id=%s:" LU ":READ\n", pfd->id, pfd->uuid);
        } else if (mode & OS_MODE_WRITE_BLOCKING) {
            tbx_append_printf(buf, used, bufsize, "pending_id=%s:" LU ":WRITE\n", pfd->id, pfd->uuid);
        } else {
            tbx_append_printf(buf, used, bufsize, "pending_id=%s:" LU ":UNKNOWN(%d)\n", pfd->id, pfd->uuid, mode);
        }
        tbx_stack_move_down(fol->pending_stack);
    }

}

//***********************************************************************
// va_lock_get_attr - Returns the file lock information
//***********************************************************************

int va_lock_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    fobj_lock_t *fol;
    int used;
    int bufsize = 10*1024;
    char result[bufsize];
    char *buf;

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    log_printf(15, "fname=%s rp=%s\n", fd->object_name, fd->realpath);

    apr_thread_mutex_lock(osf->os_lock->fobj_lock);

    fol = (fd->fol[FOL_OS]) ? fd->fol[FOL_OS] : tbx_list_search(osf->os_lock->fobj_table, fd->realpath);

    if (fol == NULL) {
        *val = NULL;
        *v_size = 0;
        apr_thread_mutex_unlock(osf->os_lock->fobj_lock);
        return(0);
    }

    //** Figure out the buffer
    buf = result;
    if (*v_size > 0) {
        buf = (char *)(*val);
        bufsize = *v_size;
    }

    used = 0;
    tbx_append_printf(buf, &used, bufsize, "[os.lock]\n");

    _lock_get_attr(fol, buf, &used, bufsize, 0);

    apr_thread_mutex_unlock(osf->os_lock->fobj_lock);

    if (*v_size < 0) *val = strdup(buf);
    *v_size = strlen(buf);

    return(0);
}

//***********************************************************************
// va_lock_user_get_attr - Returns the file os.lock.user information
//***********************************************************************

int va_lock_user_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    fobj_lock_t *fol;
    int used;
    int bufsize = 10*1024;
    char result[bufsize];
    char *buf;

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    log_printf(15, "fname=%s rp=%s\n", fd->object_name, fd->realpath);

    apr_thread_mutex_lock(osf->os_lock_user->fobj_lock);

    fol = (fd->fol[FOL_USER]) ? fd->fol[FOL_USER] : tbx_list_search(osf->os_lock_user->fobj_table, fd->realpath);

    if (fol == NULL) {
        *val = NULL;
        *v_size = 0;
        apr_thread_mutex_unlock(osf->os_lock_user->fobj_lock);
        return(0);
    }

    //** Figure out the buffer
    buf = result;
    if (*v_size > 0) {
        buf = (char *)(*val);
        bufsize = *v_size;
    }

    used = 0;
    tbx_append_printf(buf, &used, bufsize, "[os.lock.user]\n");

    _lock_get_attr(fol, buf, &used, bufsize, 1);

    apr_thread_mutex_unlock(osf->os_lock_user->fobj_lock);

    if (*v_size < 0) *val = strdup(buf);
    *v_size = strlen(buf);

    return(0);
}

//***********************************************************************
// va_attr_type_get_attr - Returns the attribute type information
//***********************************************************************

int va_attr_type_get_attr(lio_os_virtual_attr_t *myva, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    lio_os_virtual_attr_t *va;
    int ftype, bufsize, n;
    char *key;
    char buffer[32];
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    n = (int)(long)myva->priv;  //** HACKERY ** to get the attribute prefix length
    key = &(fullkey[n+1]);

    //** See if we have a VA first
    va = apr_hash_get(osf->vattr_hash, key, APR_HASH_KEY_STRING);
    if (va != NULL) {
        ftype = OS_OBJECT_VIRTUAL_FLAG;
    } else {
        snprintf(fullname, OS_PATH_MAX, "%s/%s", fd->attr_dir, key);
        ftype = lio_os_local_filetype(fullname);
        if (ftype & OS_OBJECT_BROKEN_LINK_FLAG) ftype = ftype ^ OS_OBJECT_BROKEN_LINK_FLAG;
    }

    snprintf(buffer, sizeof(buffer), "%d", ftype);
    bufsize = strlen(buffer);

    log_printf(15, "fname=%s type=%s\n", fd->object_name, buffer);

    return(osf_store_val(buffer, bufsize, val, v_size));
}

//***********************************************************************
// va_attr_link_get_attr - Returns the attribute link information
//***********************************************************************

int va_attr_link_get_attr(lio_os_virtual_attr_t *myva, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    lio_os_virtual_attr_t *va;
    tbx_list_iter_t it;
    struct stat s;
    char buffer[OS_PATH_MAX];
    char *key;
    char *ca;
    int err, n;
    char fullname[OS_PATH_MAX];

    *atype = OS_OBJECT_VIRTUAL_FLAG;

    n = (int)(long)myva->priv;  //** HACKERY ** to get the attribute prefix length
    key = &(fullkey[n+1]);

    //** Do a Virtual Attr check
    //** Check the prefix VA's first
    it = tbx_list_iter_search(osf->vattr_prefix, key, -1);
    tbx_list_next(&it, (tbx_list_key_t **)&ca, (tbx_list_data_t **)&va);

    if (va != NULL) {
        n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
        log_printf(15, "va=%s attr=%s n=%d\n", va->attribute, key, n);
        int d=strncmp(key, va->attribute, n);
        log_printf(15, "strncmp=%d\n", d);
        if (strncmp(key, va->attribute, n) == 0) {  //** Prefix matches
            return(va->get_link(va, os, creds, ofd, key, val, v_size, atype));
        }
    }

    //** Now check the normal VA's
    va = apr_hash_get(osf->vattr_hash, key, APR_HASH_KEY_STRING);
    if (va != NULL) {
        return(va->get_link(va, os, creds, ofd, key, val, v_size, atype));
    }


    //** Now check the normal attributes
    snprintf(fullname, OS_PATH_MAX, "%s/%s", fd->attr_dir, key);

    err = lstat(fullname, &s);
    if (err == 0) {
        if (S_ISLNK(s.st_mode) == 0) {
            *v_size = 0;
            return(0);
        }

        buffer[0] = 0;
        n = readlink(fullname, buffer, OS_PATH_MAX-1);
        if (n > 0) {
            buffer[n] = 0;
            log_printf(15, "readlink(%s)=%s  n=%d\n", fullname, buffer, n);
            log_printf(15, "munged path=%s\n", buffer);
            return(osf_store_val(buffer, n, val, v_size));
        }
    }

    *v_size = 0;

    return(0);
}

//***********************************************************************
// va_timestamp_set_attr - Sets the requested timestamp
//***********************************************************************

int va_timestamp_set_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void *val, int v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    char buffer[512];
    char *key;
    int64_t curr_time;
    int n;

    n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length
    key = &(fullkey[n+1]);

    if ((int)strlen(fullkey) < n) {  //** Nothing to do so return;
        *atype = OS_OBJECT_VIRTUAL_FLAG;
        return(1);
    }

    curr_time = apr_time_sec(apr_time_now());
    if (v_size > 0) {
        n = snprintf(buffer, sizeof(buffer), I64T "|%s", curr_time, (char *)val);
    } else {
        n = snprintf(buffer, sizeof(buffer), I64T, curr_time);
    }

    n = osf_set_attr(os, creds, fd, key, (void *)buffer, n, atype, 0);
    *atype |= OS_OBJECT_VIRTUAL_FLAG;

    return(n);
}


//***********************************************************************
// va_timestamp_get_attr - Returns the requested timestamp or current time
//    if no timestamp is specified
//***********************************************************************

int va_timestamp_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    char buffer[32];
    char *key;
    int64_t curr_time;
    int n;

    n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length

    log_printf(15, "fullkey=%s va=%s\n", fullkey, va->attribute);

    if ((int)strlen(fullkey) > n) {  //** Normal attribute timestamp
        key = &(fullkey[n+1]);
        n = osf_get_attr(os, creds, fd, key, val, v_size, atype, NULL, NULL);
        *atype |= OS_OBJECT_VIRTUAL_FLAG;
    } else {  //** No attribute specified so just return my time
        curr_time = apr_time_sec(apr_time_now());
        n = snprintf(buffer, sizeof(buffer), I64T, curr_time);
        log_printf(15, "now=%s\n", buffer);
        *atype = OS_OBJECT_VIRTUAL_FLAG;
        n = osf_store_val(buffer, n, val, v_size);
    }

    return(n);
}

//***********************************************************************
// va_timestamp_get_link - Returns the requested timestamp's link if available
//***********************************************************************

int va_timestamp_get_link_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    char buffer[OS_PATH_MAX];
    char *key;
    int n;

    n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length
    key = &(fullkey[n+1]);

    if ((int)strlen(fullkey) > n) {  //** Normal attribute timestamp
        n = (long)osf->attr_link_pva.priv;
        strcpy(buffer, osf->attr_link_pva.attribute);
        buffer[n] = '.';
        n++;

        strcpy(&(buffer[n]), key);
        n = osf->attr_link_pva.get(&osf->attr_link_pva, os, creds, fd, buffer, val, v_size, atype);
    } else {  //** No attribute specified os return 0
        *atype = OS_OBJECT_VIRTUAL_FLAG;
        *v_size = 0;
        n = 0;
    }

    return(n);
}

//***********************************************************************
// va_append_set_attr - Appends the data to the attribute
//***********************************************************************

int va_append_set_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void *val, int v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    char buffer[512];
    char *key;
    int n;

    n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length
    key = &(fullkey[n+1]);

    if ((int)strlen(fullkey) < n) {  //** Nothing to do so return;
        *atype = OS_OBJECT_VIRTUAL_FLAG;
        return(1);
    }

    n = osf_set_attr(os, creds, fd, key, (void *)buffer, n, atype, 1);
    *atype |= OS_OBJECT_VIRTUAL_FLAG;

    return(n);
}

//***********************************************************************
// va_append_get_attr - Just returns the attr after peeling off the PVA
//***********************************************************************

int va_append_get_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *fullkey, void **val, int *v_size, int *atype)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    char *key;
    int n;

    n = (int)(long)va->priv;  //** HACKERY ** to get the attribute prefix length

    log_printf(15, "fullkey=%s va=%s\n", fullkey, va->attribute);

    if ((int)strlen(fullkey) > n) {  //** Normal attribute
        key = &(fullkey[n+1]);
        n = osf_get_attr(os, creds, fd, key, val, v_size, atype, NULL, NULL);
        *atype |= OS_OBJECT_VIRTUAL_FLAG;
    } else {  //** No attribute specified so nothing to do
        *atype = OS_OBJECT_VIRTUAL_FLAG;
        *v_size = 0;
        n = 0;
    }

    return(n);
}


//***********************************************************************
// va_null_set_attr - Dummy routine since it can't be set
//***********************************************************************

int va_null_set_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size, int *atype)
{
    *atype = OS_OBJECT_VIRTUAL_FLAG;
    return(-1);
}

//***********************************************************************
// va_null_get_link_attr - Routine for key's without links
//***********************************************************************

int va_null_get_link_attr(lio_os_virtual_attr_t *va, lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size, int *atype)
{
    *atype = OS_OBJECT_VIRTUAL_FLAG;
    *v_size = 0;
    return(0);
}

//***********************************************************************
//  osf_retrieve_lock - Returns the internal lock for the object
//***********************************************************************

apr_thread_mutex_t *osf_retrieve_lock(lio_object_service_fn_t *os, const char *path, int *table_slot)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    tbx_chksum_t cs;
    char  digest[OSF_LOCK_CHKSUM_SIZE];
    unsigned int *n;
    int nbytes, slot;
    tbx_tbuf_t tbuf;

    nbytes = strlen(path);
    tbx_tbuf_single(&tbuf, nbytes, (char *)path);
    tbx_chksum_set(&cs, OSF_LOCK_CHKSUM);
    tbx_chksum_add(&cs, nbytes, &tbuf, 0);
    tbx_chksum_get(&cs, CHKSUM_DIGEST_BIN, digest);

    n = (unsigned int *)(&digest[OSF_LOCK_CHKSUM_SIZE-sizeof(unsigned int)]);
    slot = (*n) % osf->internal_lock_size;
    log_printf(15, "n=%u internal_lock_size=%d slot=%d path=!%s!\n", *n, osf->internal_lock_size, slot, path);
    tbx_log_flush();
    if (table_slot != NULL) *table_slot = slot;

    return(osf->internal_lock[slot]);
}

//***********************************************************************
// osf_internal_fd_lock - Locks the internal quick lock associated with the FD
//***********************************************************************

void osf_internal_fd_lock(lio_object_service_fn_t *os, osfile_fd_t *fd)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int s1, s2, s3;

    //** Do the initial lock
try_again:
    s1 = tbx_atomic_get(fd->ilock_obj);
    s2 = tbx_atomic_get(fd->ilock_rp);
    if (s1 == s2) {
        apr_thread_mutex_lock(osf->internal_lock[s1]);
    } else if (s1 < s2) {
        apr_thread_mutex_lock(osf->internal_lock[s1]);
        apr_thread_mutex_lock(osf->internal_lock[s2]);
    } else {
        apr_thread_mutex_lock(osf->internal_lock[s2]);
        apr_thread_mutex_lock(osf->internal_lock[s1]);
    }

    //** Verify the slot didn't change
    s3= tbx_atomic_get(fd->ilock_obj);
    if (s1 == s3) {
        s3= tbx_atomic_get(fd->ilock_rp);
        if (s2 == s3) {
            return;    //** No change so kick out
        }
    }

    //** IF we made it here we have to try again
    apr_thread_mutex_unlock(osf->internal_lock[s1]);
    apr_thread_mutex_unlock(osf->internal_lock[s2]);
    goto try_again;

    return;
}

//***********************************************************************
// osf_internal_fd_unlock - Unlocks the internal quick lock associated with the FD
//***********************************************************************

void osf_internal_fd_unlock(lio_object_service_fn_t *os, osfile_fd_t *fd)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int s1, s2;

    s1 = tbx_atomic_get(fd->ilock_obj);
    s2 = tbx_atomic_get(fd->ilock_rp);
    apr_thread_mutex_unlock(osf->internal_lock[s1]);
    if (s1 != s2) apr_thread_mutex_unlock(osf->internal_lock[s2]);
}

//***********************************************************************
// safe_remove - Does a simple check that the object to be removed
//     is not "/".
//***********************************************************************

int safe_remove(lio_object_service_fn_t *os, char *path)
{
    if (strlen(path) > SAFE_MIN_LEN) {
        return(remove(path));
    }

    log_printf(0, " ERROR with remove! path=%s safe_len=%d \n", path, SAFE_MIN_LEN);
    return(-1234);
}

//***********************************************************************
// object_attr_dir - Returns the object attribute directory
//***********************************************************************

char *object_attr_dir(lio_object_service_fn_t *os, char *prefix, char *path, int ftype)
{
    char fname[OS_PATH_MAX];
    char *dir, *base;
    char *attr_dir = NULL;
    int n;

    if ((ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG|OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG)) != 0) {
        strncpy(fname, path, OS_PATH_MAX);
        fname[OS_PATH_MAX-1] = '\0';
        lio_os_path_split(fname, &dir, &base);
        n = strlen(dir);
        if (dir[n-1] == '/') dir[n-1] = 0; //** Peel off a trialing /
        snprintf(fname, OS_PATH_MAX, "%s%s/%s/%s%s", prefix, dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
        attr_dir = strdup(fname);
        free(dir);
        free(base);
    } else if (ftype == OS_OBJECT_DIR_FLAG) {
        snprintf(fname, OS_PATH_MAX, "%s%s/%s", prefix, path, FILE_ATTR_PREFIX);
        attr_dir = strdup(fname);
    }

    return(attr_dir);
}

//***********************************************************************
// osf_is_dir_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_dir_empty(char *path)
{
    DIR *d;
    struct dirent *entry;

    int empty = 1;

    d = tbx_io_opendir(path);
    if (d == NULL) return(1);

    while ((empty == 1) && ((entry = tbx_io_readdir(d)) != NULL)) {
        if ( ! ((strcmp(entry->d_name, FILE_ATTR_PREFIX) == 0) ||
                (strcmp(entry->d_name, ".") == 0) || (strcmp(entry->d_name, "..") == 0)) ) empty = 0;
    }

    if (empty == 0) log_printf(15, "path=%s found entry=%s\n", path, entry->d_name);
    tbx_io_closedir(d);

    return(empty);
}
//***********************************************************************
// osf_is_empty - Returns if the directory is empty
//***********************************************************************

int osf_is_empty(char *path)
{
    int ftype;

    ftype = lio_os_local_filetype(path);
    if (ftype == OS_OBJECT_FILE_FLAG) {  //** Simple file
        return(1);
    } else if (ftype == OS_OBJECT_DIR_FLAG) { //** Directory
        return(osf_is_dir_empty(path));
    }

    return(0);
}

//***********************************************************************

char *my_readdir(osf_dir_t *d)
{
    char *fname;

    if (d->type == 0) {
        if (d->d == NULL) return(NULL);
        d->entry = tbx_io_readdir(d->d);
        if (d->entry == NULL) return(NULL);
        fname = &(d->entry->d_name[0]);
        return(fname);
    }

    if (d->slot < 1) {
        d->slot++;
        return(d->frag);
    }
    return(NULL);
}

//***********************************************************************

osf_dir_t *my_opendir(char *fullname, char *frag)
{
    osf_dir_t *d;

    tbx_type_malloc(d, osf_dir_t, 1);

    if (frag == NULL) {
        d->type = 0;
        d->d = tbx_io_opendir(fullname);
    } else {
        d->type = 1;
        d->frag = frag;
        d->slot = 0;
    }

    return(d);
}

//***********************************************************************

void my_closedir(osf_dir_t *d)
{
    if (d->type == 0) {
        if (d->d) tbx_io_closedir(d->d);
    }

    free(d);
}

//***********************************************************************

long my_telldir(osf_dir_t *d)
{
    if (d->type == 0) {
        return((d->d) ? tbx_io_telldir(d->d) : 0);
    }

    return(d->slot);
}

//***********************************************************************

void my_seekdir(osf_dir_t *d, long offset)
{
    if (d->type == 0) {
        if (d->d) tbx_io_seekdir(d->d, offset);
    } else {
        d->slot = offset;
    }
}

//***********************************************************************
// osf_next_object - Returns the iterators next object
//***********************************************************************

int osf_next_object(osf_object_iter_t *it, char **myfname, int *prefix_len, int *dir_change)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    int i, rmatch, tweak, do_recurse, can_access;
    struct stat link_stat, object_stat;
    ino_t *ino_sys;
    osf_obj_level_t *itl;
    osf_obj_level_t *it_top = NULL;
    char fname[OS_PATH_MAX];
    char fullname[OS_PATH_MAX];
    char rp[OS_PATH_MAX];
    char *obj_fixed = NULL;

    *dir_change = 0;
    *prefix_len = 0;
    if (it->finished == 1) {
        *myfname = NULL;
        it->realpath = NULL;
        return(0);
    }

    //** Check if we have a fixed object regex.  If so it's handled directly via strcmp()
    if (it->object_regex != NULL) {
        if (it->object_regex->regex_entry->fixed == 1) obj_fixed = it->object_regex->regex_entry->expression;
    }

    //** Check if we have a prefix path of '/'.  If so make a fake itl level
    tweak = 0;
    if (it->table->n == 0) {
        *prefix_len = 1;
        if (tbx_stack_count(it->recurse_stack) == 0) {  //**Make a fake level to get things going
            tbx_type_malloc_clear(itl, osf_obj_level_t, 1);
            strncpy(itl->path, "/", OS_PATH_MAX);
            strncpy(itl->realpath, "/", OS_PATH_MAX);
            itl->d = my_opendir(osf->file_path, NULL);
            itl->curr_pos = my_telldir(itl->d);
            itl->firstpass = 1;
            tbx_stack_push(it->recurse_stack, itl);
        }
    }
    it_top = (it->table->n > 0) ? &(it->level_info[it->table->n-1]) : NULL;
    if ((it->table->n == 1) && (it_top->fragment != NULL)) {
        tweak = it_top->fixed_prefix;
        if (tweak > 0) tweak += 2;
    }

    if (it_top != NULL) {
        log_printf(15, "top_level=%d it_top->fragment=%s it_top->path=%s tweak=%d\n", it->table->n-1, it_top->fragment, it_top->path, tweak);
    }

    do {
        if (it->curr_level >= it->table->n) {
            itl = (osf_obj_level_t *)tbx_stack_pop(it->recurse_stack);
        } else {
            itl = &(it->level_info[it->curr_level]);
        }

        log_printf(15, "curr_level=%d table->n=%d path=%s\n", it->curr_level, it->table->n, itl->path);

        while ((itl->entry = my_readdir(itl->d)) != NULL) {
            itl->prev_pos = itl->curr_pos;
            itl->curr_pos = my_telldir(itl->d);

            i = ((it->curr_level >= it->table->n) || (itl->fragment != NULL)) ? 0 : regexec(itl->preg, itl->entry, 0, NULL, 0);
            if (i == 0) {
                if ((strncmp(itl->entry, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) ||
                        (strcmp(itl->entry, ".") == 0) || (strcmp(itl->entry, "..") == 0)) i = 1;
            }
            if (i == 0) { //** Regex match
                snprintf(fname, OS_PATH_MAX, "%s/%s", itl->path, itl->entry);
                snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fname);

                i = os_local_filetype_stat(fullname, &link_stat, &object_stat);
                if ((i & OS_OBJECT_SYMLINK_FLAG) || (strchr(itl->entry, '/'))) {
                     _osf_realpath(it->os, fname, rp, 1);
                } else {
                    snprintf(rp, OS_PATH_MAX, "%s/%s", itl->realpath, itl->entry);
                }
                log_printf(15, "POSSIBLE MATCH level=%d table->n=%d fname=%s max_level=%d\n", it->curr_level, it->table->n, fname, it->max_level);

                can_access = osaz_object_access(osf->osaz, it->creds, &(it->ug), rp, OS_MODE_READ_IMMEDIATE);
                if (can_access > 0) { //** See if I can access it
                    if (it->curr_level < it->max_level) {     //** Cap the recurse depth
                        if (it->curr_level < it->table->n-1) { //** Still on the static table
                            if ((i & OS_OBJECT_DIR_FLAG) && (can_access == 2)) {  //*  Skip normal files since we still have static levels left
                                it->curr_level++;  //** Move to the next level
                                itl = &(it->level_info[it->curr_level]);

                                //** Initialize the level for use
                                strncpy(itl->path, fname, OS_PATH_MAX);
                                strncpy(itl->realpath, rp, OS_PATH_MAX);
                                itl->d = my_opendir(fullname, itl->fragment);
                            }
                        } else { //** Off the static table or on the last level.  From here on all hits are matches. Just have to check ftype
                            log_printf(15, " ftype=%d object_types=%d firstpass=%d\n", i, it->object_types, itl->firstpass);
                            do_recurse = 1;
                            if (i & OS_OBJECT_SYMLINK_FLAG) {  //** Check if we follow symlinks
                                if ((it->object_types & OS_OBJECT_FOLLOW_SYMLINK_FLAG) == 0) {
                                    if ((it->table->n-1) <= it->curr_level) do_recurse = 0;  //** Off the static level and hit a symlink so ignore it.
                                } else {  //** Check if we have a symlink loop
                                    if (apr_hash_get(it->symlink_loop, &link_stat.st_ino, sizeof(ino_t)) != NULL) {
                                        log_printf(15, "Already been here via symlink so pruning\n");
                                        if (itl->firstpass == 1) i = 0;   //** Already been here so don't print and prune the branch
                                    } else {  //** First time so add it for tracking
                                        tbx_type_malloc(ino_sys, ino_t, 1);
                                        *ino_sys = link_stat.st_ino;
                                        apr_hash_set(it->symlink_loop, ino_sys, sizeof(ino_t), "dummy");
                                    }
                                }
                            }

                            //** See if we have a match: either it's a file or a symlink to a file or dir we don't recurse into
                            if ((i & (OS_OBJECT_FILE_FLAG|OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG)) || ((i & OS_OBJECT_DIR_FLAG) && (do_recurse == 0)) || (can_access == 1)) {
                                if ((i & it->object_types) > 0) {
                                    rmatch = (it->object_regex == NULL) ? 0 : ((obj_fixed != NULL) ? strcmp(itl->entry, obj_fixed) : regexec(it->object_preg, itl->entry, 0, NULL, 0));
                                    if (rmatch == 0) { //** IF a match return
                                        *myfname=strdup(fname);
                                        strncpy(it->rp, rp, OS_PATH_MAX); it->realpath = it->rp;
                                        if (*prefix_len == 0) {
                                            *prefix_len = (it_top != NULL) ? strlen(it_top->path) : 0;
                                            if (*prefix_len == 0) *prefix_len = tweak;
                                        }
                                        log_printf(15, "MATCH=%s prefix=%d\n", fname, *prefix_len);
                                        if ((strcmp(itl->path, it->prev_match) != 0)) *dir_change = 1;
                                        strcpy(it->prev_match, itl->path);
                                        if (it->curr_level >= it->table->n) tbx_stack_push(it->recurse_stack, itl);  //** Off the static table
                                        return(i);
                                    }
                                }
                            } else if ((i & OS_OBJECT_DIR_FLAG) && (do_recurse == 1)) {  //** It's a dir or a symlink that should be checked
                                if (itl->firstpass == 1) { //** 1st pass so store the pos and recurse
                                    itl->firstpass = 0;              //** Flag it as already processed
                                    my_seekdir(itl->d, itl->prev_pos);  //** Move the dirp back one slot

                                    if (it->curr_level >= it->table->n) tbx_stack_push(it->recurse_stack, itl);  //** Off the static table

                                    it->curr_level++;  //** Move to the next level which is *always* off the static table

                                    //** Make a new level and initialize it for use
                                    if (it->curr_level < it->max_level) {
                                        tbx_type_malloc_clear(itl, osf_obj_level_t, 1);
                                        strncpy(itl->realpath, rp, OS_PATH_MAX);
                                        strncpy(itl->path, fname, OS_PATH_MAX);
                                        itl->d = my_opendir(fullname, itl->fragment);
                                        itl->curr_pos = my_telldir(itl->d);
                                        itl->firstpass = 1;
                                    } else {                //** Hit max recursion
                                        it->curr_level--;
                                        if (it->curr_level >= it->table->n) tbx_stack_pop(it->recurse_stack);
                                    }
                                } else {  //** Already been here so just return the name
                                    itl->firstpass = 1;        //** Set up for the next dir
                                    if ((i & it->object_types) > 0) {
                                        rmatch = (it->object_regex == NULL) ? 0 : ((obj_fixed != NULL) ? strcmp(itl->entry, obj_fixed) : regexec(it->object_preg, itl->entry, 0, NULL, 0));
                                        if (rmatch == 0) { //** IF a match return
                                            if (*prefix_len == 0) {
                                                *prefix_len = strlen(it_top->path);
                                                *prefix_len = (it_top != NULL) ? strlen(it_top->path) : 0;
                                                if (*prefix_len == 0) *prefix_len = tweak;
                                            }
                                            *myfname=strdup(fname);
                                            strncpy(it->rp, rp, OS_PATH_MAX); it->realpath = it->rp;
                                            log_printf(15, "MATCH=%s prefix=%d\n", fname, *prefix_len);
                                            if ((strcmp(itl->path, it->prev_match) != 0)) *dir_change = 1;
                                            strcpy(it->prev_match, itl->path);
                                            if (it->curr_level >= it->table->n) tbx_stack_push(it->recurse_stack, itl);  //** Off the static table
                                            return(i);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }  //** end osaz
            }
        }


        log_printf(15, "DROPPING from level=%d table->n=%d fname=%s max_level=%d\n", it->curr_level, it->table->n, itl->path, it->max_level);

        my_closedir(itl->d);
        itl->d = NULL;
        if (it->curr_level >= it->table->n) free(itl);
        it->curr_level--;
    } while (it->curr_level >= 0);

    it->finished = 1;

    *myfname = NULL;
    it->realpath = NULL;
    return(0);
}

//***********************************************************************
// piter_fname_thread - Thread that handles the parallel fname fetching
//***********************************************************************

void *piter_fname_thread(apr_thread_t *th, void *arg)
{
    osf_object_iter_t *it = (osf_object_iter_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    piter_t *pi = it->piter;
    piq_fname_t *pf;
    char *fname;
    int slot, ftype, prefix_len, dir_change, n;

    tbx_type_malloc_clear(pf, piq_fname_t, osf->n_piter_fname_size+1);
    n = sizeof(piq_fname_t)*(osf->n_piter_fname_size+1);

    slot = 0;
    while ((ftype = osf_next_object(it, &fname, &prefix_len, &dir_change)) > 0) {
        if (((dir_change) && (slot>0)) || (slot >= osf->n_piter_fname_size)) {
            pf[slot].fname = NULL;
            tbx_que_put(pi->que_fname, pf, TBX_QUE_BLOCK);
            slot = 0;
            bzero(pf, n);  //** Make sure and blank the data

            if (tbx_atomic_get(pi->abort) > 0) {  //** See if we got an abort
                free(fname);  //** Cleanup this since we going to kick out
                goto kickout;
            }
        }

        //** Store the next entry
        pf[slot].ftype = ftype;
        pf[slot].prefix_len = prefix_len;
        pf[slot].fname = fname;
        pf[slot].realpath = strdup(it->realpath);
        slot++;
    }

kickout:
    if (slot > 0) {  //** Flush the last entry
        pf[slot].fname = NULL;
        tbx_que_put(pi->que_fname, pf, TBX_QUE_BLOCK);
    }

    //** Now dump the terminators for each of the worker threads
    pf[0].fname = NULL;
    for (slot=0; slot < osf->n_piter_threads; slot++) {
        tbx_que_put(pi->que_fname, pf, TBX_QUE_BLOCK);
    }

    //** Now we can clean up and exit
    free(pf);

    apr_thread_exit(th, 0);
    return(NULL);
}


//***********************************************************************
// attr_list_is_special - Check if any attribute in the list is special
//     and if so return 1 otherwise 0.
//***********************************************************************

int attr_list_is_special(lio_object_service_fn_t *os, char **key, int n_keys)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i, n;
    tbx_list_iter_t it;
    lio_os_virtual_attr_t *va;
    char *ca;

    for (i=0; i<n_keys; i++) {
        //** Do a Virtual Attr check
        //** Check the prefix VA's first
        va = NULL;
        it = tbx_list_iter_search(osf->vattr_prefix, key[i], -1);
        tbx_list_next(&it, (tbx_list_key_t **)&ca, (tbx_list_data_t **)&va);
        if (va != NULL) {
            n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
            if (strncmp(key[i], va->attribute, n) == 0) {  //** Prefix matches
                return(1);
            }
        }

        //** Now check the normal VA's
        va = apr_hash_get(osf->vattr_hash, key[i], APR_HASH_KEY_STRING);
        if (va != NULL)  return(1);
    }

    return(0);
}

//***********************************************************************
// fast_get_attr - Optimized get_attr. It assumes no special files or symlinked objects or attrs
//***********************************************************************

int fast_get_attr(lio_object_service_fn_t *os, osfile_fd_t *ofd, char *attr, void **val, int *v_size)
{
    FILE *fd;
    char *ca;
    char fname[OS_PATH_MAX];
    int n, bsize, err;

    err = 0;

    snprintf(fname, OS_PATH_MAX, "%s/%s", ofd->attr_dir, attr);

    fd = tbx_io_fopen(fname, "r");
    if (fd == NULL) {
        if (*v_size < 0) *val = NULL;
        *v_size = -1;
        return(1);
    }

    if (*v_size < 0) { //** Need to determine the size
        tbx_io_fseek(fd, 0L, SEEK_END);
        n = tbx_io_ftell(fd);
        tbx_io_fseek(fd, 0L, SEEK_SET);
        if (n < 1) {    //** Either have an error (-1) or an empty file (0)
           *v_size = 0;
            *val = NULL;
            tbx_io_fclose(fd);
            return((n<0) ? 1 : 0);
        } else {
            *v_size = (n > (-*v_size)) ? -*v_size : n;
            bsize = *v_size + 1;
            log_printf(15, " adjusting v_size=%d n=%d\n", *v_size, n);
            *val = malloc(bsize);
         }
    } else {
        bsize = *v_size;
    }

    *v_size = tbx_io_fread(*val, 1, *v_size, fd);
    if (bsize > *v_size) {
        ca = (char *)(*val);    //** Add a NULL terminator in case it may be a string
        ca[*v_size] = 0;
    }

    tbx_io_fclose(fd);

    return(err);
}

//***********************************************************************

gop_op_status_t my_osf_get_multiple_attr_fn(void *arg, int id)
{
    osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
    int err, i;
    gop_op_status_t status;

    status = gop_success_status;

    err = 0;
    for (i=0; i<op->n; i++) {
        err += fast_get_attr(op->os, op->fd, op->key[i], (void **)&(op->val[i]), &(op->v_size[i]));
    }

    if (err) status = gop_failure_status;
    return(status);
}

//***********************************************************************
// pattr_append - Adds an entry to the que_attr
//***********************************************************************

int pattr_append_optimized(osf_object_iter_t *it, piq_attr_t *pa, int *aslot, piq_fname_t *pf)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    int v_size[it->n_list];
    void *value[it->n_list];
    osfile_attr_op_t aop;
    osfile_fd_t fd_manual;
    osfile_fd_t *fd;
    int slot = *aslot;
    int i, nbytes;

    nbytes = 0;
    memset(&fd_manual, 0, sizeof(fd_manual));
    fd_manual.object_name = pf->fname;
    fd_manual.ftype = pf->ftype;
    fd_manual.attr_dir = object_attr_dir(it->os, osf->file_path, fd_manual.object_name, fd_manual.ftype);
    fd = &fd_manual;
    aop.os = it->os;
    aop.creds = it->creds;
    aop.fd = fd;
    aop.key = it->key;
    aop.ug = &(it->ug);
    aop.realpath = pf->realpath;

    aop.val = value;
    aop.v_size = v_size;
    memcpy(v_size, it->v_size_user, sizeof(int)*it->n_list);
    aop.n = it->n_list;
    my_osf_get_multiple_attr_fn(&aop, 0);

    //** Dump the info into the slot
    pa[slot].len = pf->ftype; slot++;
    pa[slot].len = pf->prefix_len; pa[slot].value = (void *)pf->fname; slot++;

    for (i=0; i<it->n_list; i++) {
        pa[slot].len = v_size[i];
        pa[slot].value = value[i];
        value[i] = NULL;
        nbytes += v_size[i];
        slot++;
    }

    *aslot = slot;

    if (fd_manual.attr_dir) free(fd_manual.attr_dir);
    if (pf->realpath) free(pf->realpath);  //** Cleanup the realpath

    return(nbytes);
}

//***********************************************************************
// pattr_append - Adds an entry to the que_attr
//***********************************************************************

int pattr_append_general(osf_object_iter_t *it, piq_attr_t *pa, int *aslot, piq_fname_t *pf)
{
    int v_size[it->n_list];
    void *value[it->n_list];
    osfile_open_op_t op;
    osfile_attr_op_t aop;
    osfile_fd_t *fd;
    gop_op_status_t status;
    int slot = *aslot;
    int i, nbytes;

    nbytes = 0;

    //** Open the file and make the attr iterator
    op.os = it->os;
    op.creds = it->creds;
    op.path = strdup(pf->fname);
    op.fd = &fd;
    op.mode = OS_MODE_READ_IMMEDIATE;
    op.id = NULL;
    op.max_wait = 0;
    op.uuid = 0;
    op.ug = &(it->ug);
    op.realpath = pf->realpath;
    tbx_random_get_bytes(&(op.uuid), sizeof(op.uuid));
    status = osfile_open_object_fn(&op, 0);
    if (status.op_status != OP_STATE_SUCCESS) return(0);

    aop.os = it->os;
    aop.creds = it->creds;
    aop.fd = fd;
    aop.key = it->key;
    aop.ug = &(it->ug);
    aop.realpath = pf->realpath;

    aop.val = value;
    aop.v_size = v_size;
    memcpy(v_size, it->v_size_user, sizeof(int)*it->n_list);
    aop.n = it->n_list;
    osf_get_multiple_attr_fn(&aop, 0);

    //** Dump the info into the slot
    pa[slot].len = pf->ftype; slot++;
    pa[slot].len = pf->prefix_len; pa[slot].value = (void *)pf->fname; slot++;

    for (i=0; i<it->n_list; i++) {
        pa[slot].len = v_size[i];
        pa[slot].value = value[i];
        value[i] = NULL;
        nbytes += v_size[i];
        slot++;
    }

    *aslot = slot;

    //**Close the file and iter
    op.os = it->os;
    op.cfd = fd;
    osfile_close_object_fn((void *)&op, 0);

    free(pf->realpath);  //** Cleanup the realpath

    return(nbytes);
}

//***********************************************************************
// piter_attr_thread - Thread that handles the parallel attribute iterator
//***********************************************************************

void *piter_attr_thread(apr_thread_t *th, void *arg)
{
    osf_object_iter_t *it = (osf_object_iter_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    piter_t *pi = it->piter;
    piq_fname_t *pf;
    piq_attr_t *pa;
    int n, i, aslot, nbytes, finished, active_count, used;
    int (*pattr_append)(osf_object_iter_t *it, piq_attr_t *pa, int *aslot, piq_fname_t *pf);

    pattr_append = (pi->optimized_enable) ? pattr_append_optimized : pattr_append_general;

    i = 0;
    tbx_type_malloc_clear(pf, piq_fname_t, osf->n_piter_fname_size+1);
    n = (it->n_list+2) * (osf->n_piter_que_attr + 1);
    tbx_type_malloc_clear(pa, piq_attr_t, n);

    aslot = 0; finished = 0; nbytes = 0; used = 0;
    while ((tbx_que_get(pi->que_fname, pf, TBX_QUE_BLOCK) == 0) && (finished == 0)) {
        if (pf[0].fname == NULL) break;  //** Got the sentinel

        for (i = 0; pf[i].fname; i++) {
            //** Add the entry
            nbytes += pattr_append(it, pa, &aslot, pf + i);
            used++;
            if ((nbytes > osf->n_piter_attr_size) || (used >= osf->n_piter_que_attr)) {  //** Dump it on the que if full
                pa[aslot].len = 0;
                tbx_que_put(pi->que_attr, pa, TBX_QUE_BLOCK);
                nbytes = 0;
                aslot = 0;
                used = 0;
                bzero(pa, n);
                finished = tbx_atomic_get(pi->abort); //** See if we kick out after processing the block
            }
        }
    }

    //** Dump any remaining objects on the que
    if (aslot > 0) {
        pa[aslot].len = 0;
        tbx_que_put(pi->que_attr, pa, TBX_QUE_BLOCK);
        nbytes = 0;
    }

    if (finished) { //** We got an early abort so go ahead and dump everything on the fname que
        while (tbx_que_get(pi->que_fname, pf, TBX_QUE_BLOCK) == 0) {
            if (pf[0].fname == NULL) break;  //** Got the sentinel

            for (i = 0; pf[i].fname; i++) {
                free(pf[i].fname);
                free(pf[i].realpath);
            }
        }
    }

    //** See if we throw the sentinel
    active_count = tbx_atomic_dec(pi->n_active);
    if (active_count == 0) {
        tbx_que_set_finished(pi->que_attr);
    }

    //** Cleanup and exit
    free(pa);
    free(pf);

    apr_thread_exit(th, 0);
    return(NULL);
}


//***********************************************************************
// osf_purge_dir - Removes all files from the path and will recursively
//     purge subdirs based o nteh recursion depth
//***********************************************************************

int osf_purge_dir(lio_object_service_fn_t *os, char *path, int depth)
{
    int ftype;
    char fname[OS_PATH_MAX];
    DIR *d;
    struct dirent *entry;

    d = opendir(path);
    if (d == NULL) return(1);

    while ((entry = readdir(d)) != NULL) {
        if ((strcmp(".", entry->d_name) == 0) || (strcmp("..", entry->d_name) == 0)) continue;
        snprintf(fname, OS_PATH_MAX, "%s/%s", path, entry->d_name);
        ftype = lio_os_local_filetype(fname);
        if (ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG)) {
            safe_remove(os, fname);
        } else if (ftype & OS_OBJECT_DIR_FLAG) {
            if (depth > 0) {
                osf_purge_dir(os, fname, depth-1);
                safe_remove(os, fname);
            }
        }
    }

    closedir(d);

    return(0);
}

//***********************************************************************
// osfile_free_mk_mv_rm
//***********************************************************************

void osfile_free_mk_mv_rm(void *arg)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;

    if (op->src_path != NULL) free(op->src_path);
    if (op->dest_path != NULL) free(op->dest_path);
    if (op->id != NULL) free(op->id);

    free(op);
}

//***********************************************************************
// osfile_free_realpath
//***********************************************************************

void osfile_free_realpath(void *arg)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;

    if (op->src_path != NULL) free(op->src_path);
    if (op->id != NULL) free(op->id);

    free(op);
}

//***********************************************************************
// osf_object_exec_modify - Sets/clears the exec bit for the file
//***********************************************************************

int osf_object_exec_modify(lio_object_service_fn_t *os, char *path, int mode)
{
    int ftype, err;
    struct stat stat_obj, stat_link;

    ftype = os_local_filetype_stat(path, &stat_link, &stat_obj);

    log_printf(15, "ftype=%d path=%s\n", ftype, path);

    if (ftype & OS_OBJECT_DIR_FLAG) return(0);  //** It's a directory so nothing to do
    if (ftype == 0) return(1);  //** Doesn't exist

    //** If we made it here it's a normal file or a symlink to a file
    err = 0;
    if (ftype & OS_OBJECT_EXEC_FLAG) {
        if (mode == 0) {
            stat_obj.st_mode ^= S_IXUSR;
            err = chmod(path, stat_obj.st_mode);
        }
    } else if (mode == 1) {
        stat_obj.st_mode |= S_IXUSR;
        err = chmod(path, stat_obj.st_mode);
    }

    return(err);
}

//***********************************************************************
// osfile_object_exec_modify_fn - Sets/clears the exec bit for the file called via the GOP
//***********************************************************************

gop_op_status_t osfile_object_exec_modify_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    char fname[OS_PATH_MAX];
    char rp[OS_PATH_MAX];
    gop_op_status_t status;
    apr_thread_mutex_t *lock;

    if (osaz_object_access(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->src_path, rp, 1), OS_MODE_READ_IMMEDIATE) == 0)  return(gop_failure_status);
    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);

    lock = osf_retrieve_lock(op->os, rp, NULL);
    osf_obj_lock(lock);

    if (osf_object_exec_modify(op->os, fname, op->type) == 0) {
        status = gop_success_status;
    } else {
        status = gop_failure_status;
    }

    osf_obj_unlock(lock);

    return(status);
}

//***********************************************************************
// osfile_object_exec_modify - Sets/clears the exec bit
//***********************************************************************

gop_op_generic_t *osfile_object_exec_modify(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int exec_mode)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = exec_mode;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_object_exec_modify_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osf_object_remove - Removes the current dir or object (non-recursive)
//***********************************************************************

int osf_object_remove(lio_object_service_fn_t *os, char *path)
{
    int ftype, atype, err, n;
    char *dir, *base, *hard_inode;
    struct stat s;
    char fattr[OS_PATH_MAX];
    char alink[OS_PATH_MAX];

    ftype = lio_os_local_filetype(path);
    hard_inode = NULL;

    log_printf(15, "ftype=%d path=%s\n", ftype, path);

    //** It's a file or the proxy is missing so assume it's a file and remoe the FA directoory
    if ((ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG|OS_OBJECT_HARDLINK_FLAG|OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG)) || (ftype == 0)) {
        log_printf(15, "file or link removal\n");
        if (ftype & OS_OBJECT_HARDLINK_FLAG) {  //** If this is the last hardlink we need to remove the hardlink inode as well
            memset(&s, 0, sizeof(s));
            stat(path, &s);
            if (s.st_nlink <= 2) {  //** Yep we have to remove it
                hard_inode = resolve_hardlink(os, path, 0);
            }
        }

        remove(path);  //** Remove the file
        lio_os_path_split(path, &dir, &base);
        snprintf(fattr, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
        if ((ftype & OS_OBJECT_HARDLINK_FLAG) == 0) {
            osf_purge_dir(os, fattr, 0);
        }
        remove(fattr);
        free(dir);
        free(base);

        if (hard_inode != NULL) {  //** Remove the hard inode as well
            err = osf_object_remove(os, hard_inode);
            free(hard_inode);
            return(err);
        }
    } else if (ftype & OS_OBJECT_DIR_FLAG) {  //** A directory
        log_printf(15, "dir removal\n");
        snprintf(fattr, OS_PATH_MAX, "%s/%s", path,  FILE_ATTR_PREFIX);

        //** See if we have a shard.  If so we need to remove it
        atype = lio_os_local_filetype(fattr);
        if (atype & OS_OBJECT_SYMLINK_FLAG) {
            n = readlink(fattr, alink, OS_PATH_MAX);
            if (n == -1) {
                log_printf(0, "ERROR: failed to remove shard attr dir=%s\n", fattr);
            } else {
                osf_purge_dir(os, alink, 1);
            }
            osf_purge_dir(os, alink, 0);  //** Removes all the files AFTER the shard since the attr dir is a symlink it will get lopped off
            safe_remove(os, alink);
        } else {
            osf_purge_dir(os, path, 0);  //** Removes all the files
            osf_purge_dir(os, fattr, 1); //** And the attr directory
        }
        safe_remove(os, fattr);
        safe_remove(os, path);
    }

    return(0);

}

//***********************************************************************
// osf_get_inode - Get's the inode for the file
//***********************************************************************

int osf_get_inode(lio_object_service_fn_t  *os, lio_creds_t *creds, char *rpath, int ftype, char *inode, int *inode_len)
{
    osfile_fd_t fd;
    int n, atype;

    strncpy(fd.realpath, rpath, sizeof(fd.realpath)-1);
    fd.object_name = rpath;
    fd.ftype = ftype;

    n = osf_get_attr(os, creds, &fd, "system.inode", (void **)&inode, inode_len, &atype, NULL, rpath);
    return(n);
}

//***********************************************************************
// osfile_remove_object - Removes an object
//***********************************************************************

gop_op_status_t osfile_remove_object_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    int ftype, inode_len;
    char fname[OS_PATH_MAX];
    char rp[OS_PATH_MAX];
    char inode[64];
    gop_op_status_t status;
    apr_thread_mutex_t *lock;

    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
    ftype = lio_os_local_filetype(fname);
    if (osaz_object_remove(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->src_path, rp, ((ftype&OS_OBJECT_SYMLINK)?0:1) )) == 0)  return(gop_failure_status);

    lock = osf_retrieve_lock(op->os, rp, NULL);
    osf_obj_lock(lock);

    inode[0] = '0'; inode[1] = '\0'; inode_len = sizeof(inode);
    if (ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG|OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG)) {  //** Regular file so rm the attributes dir and the object
        log_printf(15, "Simple file removal: fname=%s\n", op->src_path);
        osf_get_inode(op->os, op->creds, op->src_path, ftype, inode, &inode_len);
        status = (osf_object_remove(op->os, fname) == 0) ? gop_success_status : gop_failure_status;
    } else {  //** Directory so make sure it's empty
        if (osf_is_empty(fname) != 1) {
            osf_obj_unlock(lock);
            log_printf(15, "Oops! trying to remove a non-empty dir: fname=%s ftype=%d\n", op->src_path, ftype);
            return(gop_failure_status);
        }

        log_printf(15, "Remove an empty dir: fname=%s\n", op->src_path);
        osf_get_inode(op->os, op->creds, op->src_path, ftype, inode, &inode_len);

        //** The directory is empty so can safely remove it
        status = (osf_object_remove(op->os, fname) == 0) ? gop_success_status : gop_failure_status;
    }

    if (status.op_status == OP_STATE_SUCCESS) {
        char *etext = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', ((ftype & OS_OBJECT_SYMLINK_FLAG) ? op->src_path : rp));
        notify_printf(osf->olog, 1, op->creds, "REMOVE(%d, %s, %s)\n", ftype, etext, inode);
        if (etext) free(etext);
    }

    osf_obj_unlock(lock);

    return(status);
}

//***********************************************************************
// osfile_remove_object - Makes a remove object operation
//***********************************************************************

gop_op_generic_t *osfile_remove_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(path);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_remove_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_remove_regex_fn - Does the actual bulk object removal
//***********************************************************************

gop_op_status_t osfile_remove_regex_fn(void *arg, int id)
{
    osfile_remove_regex_op_t *op = (osfile_remove_regex_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    osfile_mk_mv_rm_t rm_op;
    os_object_iter_t *it;
    int prefix_len, count;
    char *fname;
    char rp[OS_PATH_MAX];
    gop_op_status_t status, op_status;

    rm_op.os = op->os;
    rm_op.creds = op->creds;

    status = gop_success_status;

    it = osfile_create_object_iter(op->os, op->creds, op->rpath, op->object_regex, op->obj_types, NULL, op->recurse_depth, NULL, 0);

    count = 0;
    while (osfile_next_object(it, &fname, &prefix_len) > 0) {
        log_printf(15, "removing fname=%s\n", fname);
        if (osaz_object_remove(osf->osaz, op->creds, NULL, _osf_realpath(op->os, fname, rp, 1)) == 0) {
            status.op_status = OP_STATE_FAILURE;
            status.error_code++;
        } else {
            rm_op.src_path = fname;
            op_status = osfile_remove_object_fn(&rm_op, 0);
            if (op_status.op_status != OP_STATE_SUCCESS) {
                status.op_status = OP_STATE_FAILURE;
                status.error_code++;
            }
        }

        free(fname);

        count++;  //** Check for an abort
        if (count == 20) {
            count = 0;
            if (tbx_atomic_get(op->abort) != 0) {
                status.op_status = OP_STATE_FAILURE;
                break;
            }
        }
    }

    osfile_destroy_object_iter(it);

    return(status);
}

//***********************************************************************
// osfile_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************


gop_op_generic_t *osfile_remove_regex_object(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_remove_regex_op_t *op;

    tbx_type_malloc(op, osfile_remove_regex_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->rpath = path;
    op->object_regex = object_regex;
    op->recurse_depth = recurse_depth;
    op->obj_types = obj_types;
    return(gop_tp_op_new(osf->tpc, NULL, osfile_remove_regex_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_abort_remove_regex_object_fn - Performs the actual open abort operation
//***********************************************************************

gop_op_status_t osfile_abort_remove_regex_object_fn(void *arg, int id)
{
    osfile_remove_regex_op_t *op = (osfile_remove_regex_op_t *)arg;

    tbx_atomic_set(op->abort, 1);

    return(gop_success_status);
}

//***********************************************************************
//  osfile_abort_remove_regex_object - Aborts an ongoing remove operation
//***********************************************************************

gop_op_generic_t *osfile_abort_remove_regex_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    gop_thread_pool_op_t *tpop = gop_get_tp(gop);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_abort_remove_regex_object_fn, tpop->arg, NULL, 1));
}

//***********************************************************************
// osfile_regex_object_set_multiple_attrs - Recursivley sets the fixed attibutes
//***********************************************************************

gop_op_status_t osfile_regex_object_set_multiple_attrs_fn(void *arg, int id)
{
    osfile_regex_object_attr_op_t *op = (osfile_regex_object_attr_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    os_object_iter_t *it;
    char *fname;
    char rp[OS_PATH_MAX];
    gop_op_status_t status, op_status;
    osfile_attr_op_t op_attr;
    osfile_fd_t *fd;
    osfile_open_op_t op_open;
    int prefix_len, count;

    memset(&op_attr, 0, sizeof(op_attr));
    op_attr.os = op->os;
    op_attr.creds = op->creds;
    op_attr.fd = NULL; //** filled in for each object
    op_attr.key = op->key;
    op_attr.val = op->val;
    op_attr.v_size = op->v_size;
    op_attr.n = op->n_keys;

    memset(&op_open, 0, sizeof(op_open));
    op_open.os = op->os;
    op_open.creds = op->creds;
    op_open.path = NULL;  //** Filled in for each open
    op_open.id = op->id;
    op_open.fd = &fd;
    op_open.mode = OS_MODE_READ_IMMEDIATE;
    op_open.id = NULL;
    op_open.uuid = 0;
    tbx_random_get_bytes(&(op_open.uuid), sizeof(op_open.uuid));
    op_open.max_wait = 0;

    status = gop_success_status;

    it = osfile_create_object_iter(op->os, op->creds, op->rpath, op->object_regex, op->object_types, NULL, op->recurse_depth, NULL, 0);
    count = 0;
    while (osfile_next_object(it, &fname, &prefix_len) > 0) {
        if (osaz_object_access(osf->osaz, op->creds, NULL, _osf_realpath(op->os, fname, rp, 1), OS_MODE_WRITE_IMMEDIATE) < 2) {
            status.op_status = OP_STATE_FAILURE;
            status.error_code += op->n_keys;
        } else {
            op_open.path = strdup(fname);
            op_open.realpath = rp;
            op_status = osfile_open_object_fn(&op_open, 0);
            if (op_status.op_status != OP_STATE_SUCCESS) {
                status.op_status = OP_STATE_FAILURE;
                status.error_code += op->n_keys;
            } else {
                op_attr.fd = fd;
                op_status = osf_set_multiple_attr_fn(&op_attr, 0);
                if (op_status.op_status != OP_STATE_SUCCESS) {
                    status.op_status = OP_STATE_FAILURE;
                    status.error_code++;
                }

                op_open.cfd = fd;
                osfile_close_object_fn((void *)&op_open, 0);  //** Got to close it as well
            }
        }

        free(fname);

        count++;  //** Check for an abort
        if (count == 20) {
            count = 0;
            if (tbx_atomic_get(op->abort) != 0) {
                status.op_status = OP_STATE_FAILURE;
                break;
            }
        }

    }

    osfile_destroy_object_iter(it);

    return(status);
}

//***********************************************************************
// osfile_regex_object_set_multiple_attrs - Does a bulk regex change attr.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************


gop_op_generic_t *osfile_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char *id, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_regex_object_attr_op_t *op;

    tbx_type_malloc(op, osfile_regex_object_attr_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->id = id;
    op->rpath = path;
    op->object_regex = object_regex;
    op->recurse_depth = recurse_depth;
    op->key = key;
    op->val = val;
    op->v_size = v_size;
    op->n_keys = n_attrs;
    op->object_types = object_types;
    op->abort = 0;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_regex_object_set_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_abort_regex_object_set_multiple_attrs_fn - Performs the actual open abort operation
//***********************************************************************

gop_op_status_t osfile_abort_regex_object_set_multiple_attrs_fn(void *arg, int id)
{
    osfile_regex_object_attr_op_t *op = (osfile_regex_object_attr_op_t *)arg;

    tbx_atomic_set(op->abort, 1);

    return(gop_success_status);
}

//***********************************************************************
//  osfile_abort_regex_object_set_multiple_attrs - Aborts an ongoing remove operation
//***********************************************************************

gop_op_generic_t *osfile_abort_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    gop_thread_pool_op_t *tpop = gop_get_tp(gop);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_abort_regex_object_set_multiple_attrs_fn, tpop->arg, NULL, 1));
}

//***********************************************************************
// osfile_exists_fn - Check for file type and if it exists
//***********************************************************************

gop_op_status_t osfile_exists_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    char fname[OS_PATH_MAX];
    char rp[OS_PATH_MAX];
    gop_op_status_t status = gop_success_status;

    if (osaz_object_access(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->src_path, rp, 1), OS_MODE_READ_IMMEDIATE) == 0)  return(gop_failure_status);

    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
    status.error_code = lio_os_local_filetype(fname);
    log_printf(15, "fname=%s  ftype=%d\n", fname, status.error_code);
    if (status.error_code == 0) status.op_status = OP_STATE_FAILURE;

    return(status);
}

//***********************************************************************
//  osfile_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

gop_op_generic_t *osfile_exists(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    if (path == NULL) return(gop_dummy(gop_failure_status));

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(path);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_exists_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_realpath_fn - Returns the realpath
//***********************************************************************

gop_op_status_t osfile_realpath_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    char rpath[OS_PATH_MAX];
    char *rp;
    gop_op_status_t status = gop_success_status;

    rp = _osf_realpath(op->os, op->src_path, rpath, 1);
    if (!rp) return(gop_failure_status);

    if (osaz_object_access(osf->osaz, op->creds, NULL, rp, OS_MODE_READ_IMMEDIATE) == 0)  return(gop_failure_status);

    strcpy(op->dest_path, rp);
    log_printf(15, "fname=%s  realpath=%s\n", op->src_path, op->dest_path);

    return(status);
}

//***********************************************************************
//  osfile_realpath - Returns the realpath
//***********************************************************************

gop_op_generic_t *osfile_realpath(lio_object_service_fn_t *os, lio_creds_t *creds, const char *path, char *realpath)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    if (path == NULL) return(gop_dummy(gop_failure_status));

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(path);
    op->dest_path = realpath;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_realpath_fn, (void *)op, osfile_free_realpath, 1));
}



//***********************************************************************
// osfile_create_object_fn - Does the actual object creation
//***********************************************************************

gop_op_status_t osfile_create_object_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    struct stat sbuf;
    FILE *fd;
    int err, mod;
    dev_t dev = 0;
    char *dir, *base;
    char fname[OS_PATH_MAX];
    char fattr[OS_PATH_MAX];
    char sattr[OS_PATH_MAX];
    char rpath[OS_PATH_MAX];
    apr_thread_mutex_t *lock;

    if (osaz_object_create(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->src_path, rpath, 0)) == 0)  return(gop_failure_status);

    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);

    log_printf(15, "base=%s src=%s fname=%s mode=%x\n", osf->file_path, op->src_path, fname, op->type);

    lock = osf_retrieve_lock(op->os, rpath, NULL);
    osf_obj_lock(lock);

    if (op->type & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SOCKET_FLAG|OS_OBJECT_FIFO_FLAG)) {
        if (op->type & (OS_OBJECT_SOCKET_FLAG|OS_OBJECT_FIFO_FLAG)) {
            if (op->type & OS_OBJECT_SOCKET_FLAG) {
                err = tbx_io_mknod(fname, S_IFSOCK|S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH, dev);  //** For FIFOs we just set the flag that it's a FIFO. It's up to the higher level routines to make it work
            } else {
                err = tbx_io_mknod(fname, S_IFIFO|S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH, dev);  //** For sockets we just set the flag that it's a socket. It's up to the higher level routines to make it work
            }
            if (err == -1) {
                osf_obj_unlock(lock);
                return(gop_failure_status);
            }
            tbx_io_close(err);
        } else {
            fd = tbx_io_fopen(fname, "w");
            if (fd == NULL) {
                osf_obj_unlock(lock);
                return(gop_failure_status);
            }
            tbx_io_fclose(fd);
        }

        if (op->type & OS_OBJECT_EXEC_FLAG) { //** See if we need to set the executable bit
            osf_object_exec_modify(op->os, fname, op->type);
        }

        //** Also need to make the attributes directory
        lio_os_path_split(fname, &dir, &base);
        snprintf(fattr, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
        err = mkdir(fattr, DIR_PERMS);
        if (err != 0) {
            log_printf(0, "Error creating object attr directory! path=%s full=%s\n", op->src_path, fattr);
            safe_remove(op->os, fname);
            free(dir);
            free(base);
            osf_obj_unlock(lock);
            return(gop_failure_status);
        } else {
            free(dir);
            free(base);
        }
        char *etext1 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', rpath);
        char *etext2 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', op->src_path);
        notify_printf(osf->olog, 1, op->creds, "CREATE(%d, %s, %s)\n", op->type, etext1, etext2);
        if (etext1) free(etext1);
        if (etext2) free(etext2);
    } else {  //** Directory object
        err = mkdir(fname, DIR_PERMS);
        if (err != 0) {
            osf_obj_unlock(lock);
            return(gop_failure_status);
        }

        //** Also need to make the attributes directory
        snprintf(fattr, OS_PATH_MAX, "%s/%s", fname, FILE_ATTR_PREFIX);
        if (osf->shard_enable) {
            //** Use the ino of the directory as a random number
            stat(fname, &sbuf);

            //** Make the shard directory
            mod = sbuf.st_ino % osf->n_shard_prefix;
            snprintf(sattr, OS_PATH_MAX, "%s/%d/" LU, osf->shard_prefix[mod], mod, sbuf.st_ino);
            err = mkdir(sattr, DIR_PERMS);
            if (err != 0) {
                log_printf(0, "Error creating object shard attr directory! path=%s full=%s\n", op->src_path, sattr);
                safe_remove(op->os, fname);
                osf_obj_unlock(lock);
                return(gop_failure_status);
            }

            //** And symlink it in
            err = symlink(sattr, fattr);
            if (err != 0) {
                log_printf(0, "Error creating object attr directory! path=%s full=%s\n", op->src_path, fattr);
                safe_remove(op->os, fname);
                safe_remove(op->os, sattr);
                osf_obj_unlock(lock);
                return(gop_failure_status);
            }
        } else {
            err = mkdir(fattr, DIR_PERMS);
            if (err != 0) {
                log_printf(0, "Error creating object attr directory! path=%s full=%s\n", op->src_path, fattr);
                safe_remove(op->os, fname);
                osf_obj_unlock(lock);
                return(gop_failure_status);
            }
        }
        char *etext1 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', rpath);
        char *etext2 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', op->src_path);
        notify_printf(osf->olog, 1, op->creds, "CREATE(%d, %s, %s)\n", op->type, etext1, etext2);
        if (etext1) free(etext1);
        if (etext2) free(etext2);

    }

    osf_obj_unlock(lock);

    return(gop_success_status);
}

//***********************************************************************
// osfile_create_object - Creates an object
//***********************************************************************

gop_op_generic_t *osfile_create_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type, char *id)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = type;
    op->id = (id != NULL) ? strdup(id) : NULL;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_create_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osfile_symlink_object_fn - Symlink two objects
//***********************************************************************

gop_op_status_t osfile_symlink_object_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    osfile_mk_mv_rm_t dop;
    gop_op_status_t status;
    char sfname[OS_PATH_MAX];
    char dfname[OS_PATH_MAX];
    char rpath[OS_PATH_MAX];
    int err;

    if (osaz_object_create(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->dest_path, rpath, 0)) == 0) return(gop_failure_status);

    //** Create the object like normal
    dop.os = op->os;
    dop.creds = op->creds;
    dop.src_path = op->dest_path;
    dop.type = OS_OBJECT_FILE_FLAG | OS_OBJECT_SYMLINK_FLAG;
    dop.id = op->id;
    status = osfile_create_object_fn(&dop, id);
    if (status.op_status != OP_STATE_SUCCESS) {
        log_printf(15, "Failed creating the dest object: %s\n", op->dest_path);
        return(gop_failure_status);
    }

    //** Now remove the placeholder and replace it with the link
//  snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
    if (op->src_path[0] == '/') {
        snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
    } else {
        snprintf(sfname, OS_PATH_MAX, "%s", op->src_path);
    }
    snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, op->dest_path);

    log_printf(15, "sfname=%s dfname=%s\n", sfname, dfname);
    err = safe_remove(op->os, dfname);
    if (err != 0) log_printf(15, "Failed removing dest place holder %s  err=%d\n", dfname, err);

    err = symlink(sfname, dfname);
    if (err != 0) log_printf(15, "Failed making symlink %s -> %s  err=%d\n", sfname, dfname, err);

    char *etext1 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', op->src_path);
    char *etext2 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', op->dest_path);
    notify_printf(osf->olog, 1, op->creds, "SYMLINK(%s, %s)\n", etext1, etext2);
    if (etext1) free(etext1);
    if (etext2) free(etext2);
    return((err == 0) ? gop_success_status : gop_failure_status);
}


//***********************************************************************
// osfile_symlink_object - Generates a symbolic link object operation
//***********************************************************************

gop_op_generic_t *osfile_symlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    //** Make sure the files are different
    if (strcmp(src_path, dest_path) == 0) {
        return(gop_dummy(gop_failure_status));
    }

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);
    op->id = (id == NULL) ? NULL : strdup(id);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_symlink_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osf_file2hardlink - Converts a normal file to a hardlink version
//***********************************************************************

int osf_file2hardlink(lio_object_service_fn_t *os, char *src_path)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int slot, i;
    ex_id_t id;
    char *sattr, *hattr;
    char fullname[OS_PATH_MAX], sfname[OS_PATH_MAX];

    //** IF the src is a symlink we need to get that

    //** Pick a hardlink location
    id = 0;
    tbx_random_get_bytes(&id, sizeof(id));
    slot = tbx_atomic_counter(&(osf->hardlink_count)) % osf->hardlink_dir_size;
    snprintf(fullname, OS_PATH_MAX, "%s/%d/" XIDT, osf->hardlink_path, slot, id);
    snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, src_path);
    hattr = object_attr_dir(os, "", fullname, OS_OBJECT_FILE_FLAG);
    sattr = object_attr_dir(os, osf->file_path, src_path, OS_OBJECT_FILE_FLAG);

    //** Move the src attr dir to the hardlink location
    i = rename(sattr, hattr);
    log_printf(0, "rename(%s,%s)=%d\n", sattr, hattr, i);

    if (i != 0) {
        log_printf(0, "rename(%s,%s) FAILED!\n", sattr, hattr);
        free(hattr);
        free(sattr);
        return(1);
    }

    //** Link the src attr dir with the hardlink
    i = symlink(hattr, sattr);
    log_printf(0, "symlink(%s,%s)=%d!\n", hattr, sattr, i);
    if (i != 0) {
        log_printf(5, "symlink(%s,%s) FAILED!\n", hattr, sattr);
        free(hattr);
        free(sattr);
        return(1);
    }
    free(hattr);
    free(sattr);


    //** Move the source to the hardlink proxy
    i = rename(sfname, fullname);
    log_printf(5, "rename(%s,%s)=%d\n", sfname, fullname, i);
    if (i != 0) {
        log_printf(0, "rename(%s,%s) FAILED!\n", sfname, fullname);
        return(1);
    }

    //** Link the src file to the hardlink proxy
    i = link(fullname, sfname);
    log_printf(5, "link(%s,%s)=%d\n", fullname, sfname, i);
    if (i != 0) {
        log_printf(0, "link(%s,%s) FAILED!\n", fullname, sfname);
        return(1);
    }

    return(0);
}


//***********************************************************************
// resolve_hardlink - DEtermines which object in the hard link dir the object
//  points to
//***********************************************************************

char *resolve_hardlink(lio_object_service_fn_t *os, char *src_path, int add_prefix)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char *hpath, *tmp;
    char buffer[OS_PATH_MAX];
    int n, i;

    if (add_prefix == 1) {
        hpath = object_attr_dir(os, osf->file_path, src_path, OS_OBJECT_FILE_FLAG);
    } else {
        hpath = object_attr_dir(os, "", src_path, OS_OBJECT_FILE_FLAG);
    }

    n = readlink(hpath, buffer, OS_PATH_MAX-1);
    if (n <= 0) {
        log_printf(0, "Readlink error!  src_path=%s hpath=%s\n", src_path, hpath);
        return(NULL);
    }
    free(hpath);

    buffer[n] = 0;
    log_printf(15, "file_path=%s fullname=%s link=%s\n", osf->file_path, src_path, buffer);

    hpath = buffer;
    tmp = strstr(hpath, FILE_ATTR_PREFIX "/" FILE_ATTR_PREFIX);
    n = FILE_ATTR_PREFIX_LEN + 1 + FILE_ATTR_PREFIX_LEN;
    for (i=0; tmp[i+n] != 0; i++) tmp[i] = tmp[i+n];
    tmp[i] = 0;

    log_printf(15, "fullname=%s link=%s\n", src_path, tmp);

    return(strdup(hpath));
}

//***********************************************************************
// osfile_hardlink_object_fn - hard links two objects
//***********************************************************************

gop_op_status_t osfile_hardlink_object_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    gop_op_status_t status;
    apr_thread_mutex_t *hlock, *dlock;
    int hslot, dslot;
    char sfname[OS_PATH_MAX];
    char dfname[OS_PATH_MAX];
    char rp_src[OS_PATH_MAX];
    char rp_dest[OS_PATH_MAX];
    char *sapath, *dapath, *link_path;
    int err, ftype;

    if ((osaz_object_access(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->src_path, rp_src, 1), OS_MODE_READ_IMMEDIATE) < 2) ||
            (osaz_object_create(osf->osaz, op->creds, NULL, _osf_realpath(op->os, op->dest_path, rp_dest, 0)) == 0)) return(gop_failure_status);

    //** Verify the source exists
    snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, op->src_path);
    ftype = lio_os_local_filetype(sfname);
    if (ftype == 0) {
        log_printf(15, "ERROR source file missing sfname=%s dfname=%s\n", op->src_path, op->dest_path);
        return(gop_failure_status);
    }

    //** Check if the source is already a hardlink
    if ((ftype & OS_OBJECT_HARDLINK_FLAG) == 0) { //** If not convert it to a hard link
        err = osf_file2hardlink(op->os, op->src_path);
        if (err != 0) {
            log_printf(15, "ERROR converting source file to a hard link sfname=%s\n", op->src_path);
            return(gop_failure_status);
        }

    }


    //** Resolve the hardlink by looking at the src objects attr path
    link_path = resolve_hardlink(op->os, op->src_path, 1);
    if (link_path == NULL) {
        log_printf(15, "ERROR resolving src hard link sfname=%s dfname=%s\n", op->src_path, op->dest_path);
        free(link_path);
        return(gop_failure_status);
    }

    //** Make the dest path
    snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, op->dest_path);

    //** Acquire the locks
    hlock = osf_retrieve_lock(op->os, link_path, &hslot);   //FIXME what should this rp be????
    dlock = osf_retrieve_lock(op->os, rp_dest, &dslot);
    if (hslot < dslot) {
        apr_thread_mutex_lock(hlock);
        apr_thread_mutex_lock(dlock);
    } else if (hslot > dslot) {
        apr_thread_mutex_lock(dlock);
        apr_thread_mutex_lock(hlock);
    } else {
        apr_thread_mutex_lock(hlock);
    }

    //** Hardlink the proxy
    if (link(link_path, dfname) != 0) {
        log_printf(15, "ERROR making proxy hardlink link_path=%s sfname=%s dfname=%s\n", link_path, op->src_path, dfname);
        status = gop_failure_status;
        goto finished;
    }

    //** Symlink the attr dirs together
    sapath = object_attr_dir(op->os, "", link_path, OS_OBJECT_FILE_FLAG);
    dapath = object_attr_dir(op->os, osf->file_path, op->dest_path, OS_OBJECT_FILE_FLAG);
    if (symlink(sapath, dapath) != 0) {
        unlink(dfname);
        free(sapath);
        free(dapath);
        log_printf(15, "ERROR making proxy hardlink link_path=%s sfname=%s dfname=%s\n", link_path, op->src_path, op->dest_path);
        status = gop_failure_status;
        goto finished;
    }
    free(sapath);
    free(dapath);

    status = gop_success_status;

finished:
    apr_thread_mutex_unlock(hlock);
    if (hslot != dslot)  apr_thread_mutex_unlock(dlock);
    free(link_path);

    return(status);
}


//***********************************************************************
// osfile_hardlink_object - Generates a hard link object operation
//***********************************************************************

gop_op_generic_t *osfile_hardlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    //** Make sure the files are different
    if (strcmp(src_path, dest_path) == 0) {
        return(gop_dummy(gop_failure_status));
    }

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);
    op->id = (id == NULL) ? NULL : strdup(id);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_hardlink_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}

//***********************************************************************
// osf_move_object - Actually Moves an object
//***********************************************************************

gop_op_status_t osf_move_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, int id, int dolock)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t rm;
    int ftype, dtype;
    int n_locks = 0;
    int max_locks = 1024;
    int ilock_table[max_locks];
    int *ilock;
    unsigned int ui;
    char sfname[OS_PATH_MAX];
    char dfname[OS_PATH_MAX];
    char dfname2[OS_PATH_MAX];
    char srpath[OS_PATH_MAX];
    char drpath[OS_PATH_MAX];
    char *dir, *base;
    int err;
    gop_op_status_t status;

    if ((osaz_object_remove(osf->osaz, creds, NULL,  _osf_realpath(os, src_path, srpath, 1)) == 0) ||
            (osaz_object_create(osf->osaz, creds, NULL, _osf_realpath(os, dest_path, drpath, 0)) == 0)) return(gop_failure_status);

    ftype = 0;  //** Init it to make the compiler happy on the warn

    //** Lock the individual objects based on their slot positions to avoid a deadlock
    if (dolock == 1) {
        ilock = ilock_table;
        n_locks = osf_match_fobj_lock(os, srpath, drpath, src_path, dest_path, max_locks, &ilock, 1);
    }

    snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, src_path);
    snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, dest_path);

    // ** check if the dest already exists. If so we need to preserve it in case of an error
    dtype = lio_os_local_filetype(dfname);
    if (dtype != 0) {  //** Recursively call our selves and move the dest out of the way
       tbx_random_get_bytes(&ui, sizeof(ui));  //** Make the random name
       snprintf(dfname2, OS_PATH_MAX, "%s_dmv_%u", dest_path, ui);
       status = osf_move_object(os, creds, dest_path, dfname2, id, 0);
       err = status.op_status;
       if (status.op_status != OP_STATE_SUCCESS) goto fail;
    }

    //** If we made it here we know the DEST does NOT exist
    //** Figure out what we are trying to move.
    ftype = lio_os_local_filetype(sfname);

    //** Attempt to move the main file entry
    err = rename(sfname, dfname);  //** Move the file/dir
    log_printf(15, "sfname=%s dfname=%s err=%d\n", sfname, dfname, err);

    if ((ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG)) && (err==0)) { //** File move
        //** Also need to move the attributes entry
        lio_os_path_split(sfname, &dir, &base);
        snprintf(sfname, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
        free(dir);
        free(base);
        lio_os_path_split(dfname, &dir, &base);
        snprintf(dfname, OS_PATH_MAX, "%s/%s/%s%s", dir, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX, base);
        free(dir);
        free(base);

        log_printf(15, "ATTR sfname=%s dfname=%s\n", sfname, dfname);
        err = rename(sfname, dfname);  //** Move the attribute directory
        if (err != 0) { //** Got to undo the main file/dir entry if the attr rename fails
            snprintf(sfname, OS_PATH_MAX, "%s%s", osf->file_path, src_path);
            snprintf(dfname, OS_PATH_MAX, "%s%s", osf->file_path, dest_path);
            rename(dfname, sfname);
        }
    }

   if (dtype != 0) {  //** There was already something in the dest so need to clean up
        if (err == 0) {  //** No errors so just remove the old entry
            rm.os = os;
            rm.creds = creds;
            rm.src_path = dfname2;
            osfile_remove_object_fn(&rm, id);
        } else {  //** Move failed so undo things
            osf_move_object(os, creds, dfname2, dest_path, id, 0);
        }
    }

fail:
    if (dolock == 1) {
        if (err == 0) {
            _osf_update_fobj_path(os, osf->os_lock, srpath, drpath);
            _osf_update_fobj_path(os, osf->os_lock_user, srpath, drpath);
            _osf_update_open_fd_path(os, src_path, dest_path);
        }
        osf_match_fobj_unlock(os, n_locks, ilock);
        if (ilock != ilock_table) free(ilock);
    }

    if (err == 0) {
        char *etext1 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', srpath);
        char *etext2 = tbx_stk_escape_text(OS_FNAME_ESCAPE, '\\', drpath);
        notify_printf(osf->olog, 1, creds, "MOVE(%d, %s, %s)\n", ftype, etext1, etext2);
        if (etext1) free(etext1);
        if (etext2) free(etext2);
    }

    return((err == 0) ? gop_success_status : gop_failure_status);
}


//***********************************************************************
// osfile_move_object_fn - Actually Moves an object
//***********************************************************************

gop_op_status_t osfile_move_object_fn(void *arg, int id)
{
    osfile_mk_mv_rm_t *op = (osfile_mk_mv_rm_t *)arg;

    return(osf_move_object(op->os, op->creds, op->src_path, op->dest_path, id, 1));
}

//***********************************************************************
// osfile_move_object - Generates a move object operation
//***********************************************************************

gop_op_generic_t *osfile_move_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, osfile_mk_mv_rm_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_move_object_fn, (void *)op, osfile_free_mk_mv_rm, 1));
}


//***********************************************************************
// osfile_copy_multiple_attrs_fn - Actually copies the object attrs
//***********************************************************************

gop_op_status_t osfile_copy_multiple_attrs_fn(void *arg, int id)
{
    osfile_copy_attr_t *op = (osfile_copy_attr_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    gop_op_status_t status;
    lio_os_authz_local_t  ug;
    osaz_attr_filter_t filter;
    void *val;
    int v_size;
    int slot_src, slot_dest;
    int i, j, err, atype;

    //** Lock the individual objects based on their slot positions to avoid a deadlock
try_again:
    slot_src = tbx_atomic_get(op->fd_src->ilock_rp);
    slot_dest = tbx_atomic_get(op->fd_dest->ilock_rp);
    if (slot_src < slot_dest) {
        osf_obj_lock(osf->internal_lock[slot_src]);
        osf_obj_lock(osf->internal_lock[slot_dest]);
    } else if (slot_src > slot_dest) {
        osf_obj_lock(osf->internal_lock[slot_dest]);
        osf_obj_lock(osf->internal_lock[slot_src]);
    } else {  //** Same slot so only need to lock one
        osf_obj_lock(osf->internal_lock[slot_src]);
    }

    //** Make sure nothing changed while we were doin the handshake
    i = tbx_atomic_get(op->fd_src->ilock_rp);
    j = tbx_atomic_get(op->fd_dest->ilock_rp);
    if ((i != slot_src) || (j != slot_dest)) {
        osf_obj_lock(osf->internal_lock[slot_dest]);
        osf_obj_lock(osf->internal_lock[slot_src]);
        goto try_again;
    }
    osaz_ug_hint_init(osf->osaz, op->creds, &ug);
    ug.creds = op->creds;
    osaz_ug_hint_set(osf->osaz, op->creds, &ug);

    status = gop_success_status;
    for (i=0; i<op->n; i++) {
        log_printf(15, " fsrc=%s (lock=%d) fdest=%s (lock=%d)   n=%d i=%d key_src=%s key_dest=%s\n", op->fd_src->object_name, slot_src, op->fd_dest->object_name, slot_dest, op->n, i, op->key_src[i], op->key_dest[i]);
        if ((osaz_attr_access(osf->osaz, op->creds, NULL, op->fd_src->realpath, op->key_src[i], OS_MODE_READ_IMMEDIATE, &filter) != 0) &&
                (osaz_attr_create(osf->osaz, op->creds, NULL, op->fd_src->realpath, op->key_dest[i]) == 1)) {

            v_size = -osf->max_copy;
            val = NULL;
            err = osf_get_attr(op->os, op->creds, op->fd_src, op->key_src[i], &val, &v_size, &atype, &ug, op->fd_src->realpath);
            osaz_attr_filter_apply(osf->osaz, op->key_src[i], OS_MODE_READ_IMMEDIATE, &val, &v_size, filter);
            if (err == 0) {
                err = osf_set_attr(op->os, op->creds, op->fd_dest, op->key_dest[i], val, v_size, &atype, 0);
                free(val);
                if (err != 0) {
                    status.op_status = OP_STATE_FAILURE;
                    status.error_code++;
                }
            } else {
                status.op_status = OP_STATE_FAILURE;
                status.error_code++;
            }
        } else {
            status.op_status = OP_STATE_FAILURE;
            status.error_code++;
        }
    }

    osf_obj_lock(osf->internal_lock[slot_src]);
    if (slot_src != slot_dest) osf_obj_lock(osf->internal_lock[slot_dest]);

    osaz_ug_hint_free(osf->osaz, op->creds, &ug);

    log_printf(15, "fsrc=%s fdest=%s err=%d\n", op->fd_src->object_name, op->fd_dest->object_name, status.error_code);

    return(status);
}

//***********************************************************************
// osfile_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

gop_op_generic_t *osfile_copy_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_copy_attr_t *op;

    tbx_type_malloc_clear(op, osfile_copy_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd_src = (osfile_fd_t *)fd_src;
    op->fd_dest = (osfile_fd_t *)fd_dest;
    op->key_src = key_src;
    op->key_dest = key_dest;
    op->n = n;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_copy_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_copy_attr - Generates a copy object attribute operation
//***********************************************************************

gop_op_generic_t *osfile_copy_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_copy_attr_t *op;

    tbx_type_malloc_clear(op, osfile_copy_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd_src = (osfile_fd_t *)fd_src;
    op->fd_dest = (osfile_fd_t *)fd_dest;
    op->key_src = &(op->single_src);
    op->single_src = key_src;
    op->key_dest = &(op->single_dest);
    op->single_dest = key_dest;
    op->n = 1;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_copy_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_symlink_multiple_attrs_fn - Actually links the multiple attrs
//***********************************************************************

gop_op_status_t osfile_symlink_multiple_attrs_fn(void *arg, int id)
{
    osfile_copy_attr_t *op = (osfile_copy_attr_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    gop_op_status_t status;
    char sfname[OS_PATH_MAX];
    char dfname[OS_PATH_MAX];
    int i, err;

    //** Lock the source
    osf_internal_fd_lock(op->os, op->fd_dest);

    log_printf(15, " fsrc[0]=%s fdest=%s n=%d key_src[0]=%s key_dest[0]=%s\n", op->src_path[0], op->fd_dest->object_name, op->n, op->key_src[0], op->key_dest[0]);

    status = gop_success_status;
    for (i=0; i<op->n; i++) {
        if (osaz_attr_create(osf->osaz, op->creds, NULL, op->fd_dest->realpath, op->key_dest[i]) == 1) {

            osf_make_attr_symlink(op->os, sfname, op->src_path[i], op->key_src[i]);
            snprintf(dfname, OS_PATH_MAX, "%s/%s", op->fd_dest->attr_dir, op->key_dest[i]);

            log_printf(15, "sfname=%s dfname=%s\n", sfname, dfname);

            err = symlink(sfname, dfname);
            if (err != 0) {
                log_printf(15, "Failed making symlink %s -> %s  err=%d\n", sfname, dfname, err);
                status.op_status = OP_STATE_FAILURE;
                status.error_code++;
            }

        } else {
            status.op_status = OP_STATE_FAILURE;
            status.error_code++;
        }
    }

    osf_internal_fd_unlock(op->os, op->fd_dest);

    log_printf(15, "fsrc[0]=%s fdest=%s err=%d\n", op->src_path[0], op->fd_dest->object_name, status.error_code);

    return(status);
}

//***********************************************************************
// osfile_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

gop_op_generic_t *osfile_symlink_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_copy_attr_t *op;

    tbx_type_malloc_clear(op, osfile_copy_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = src_path;
    op->fd_dest = (osfile_fd_t *)fd_dest;
    op->key_src = key_src;
    op->key_dest = key_dest;
    op->n = n;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_symlink_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_symlink_attr - Generates a link attribute operation
//***********************************************************************

gop_op_generic_t *osfile_symlink_attr(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_copy_attr_t *op;

    tbx_type_malloc_clear(op, osfile_copy_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->src_path = &(op->single_path);
    op->single_path = src_path;
    op->fd_dest = (osfile_fd_t *)fd_dest;
    op->key_src = &(op->single_src);
    op->single_src = key_src;
    op->key_dest = &(op->single_dest);
    op->single_dest = key_dest;
    op->n = 1;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_symlink_multiple_attrs_fn, (void *)op, free, 1));
}


//***********************************************************************
// osfile_move_multiple_attrs_fn - Actually Moves the object attrs
//***********************************************************************

gop_op_status_t osfile_move_multiple_attrs_fn(void *arg, int id)
{
    osfile_move_attr_t *op = (osfile_move_attr_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    lio_os_virtual_attr_t *va1, *va2;
    gop_op_status_t status;
    int i, err;
    char sfname[OS_PATH_MAX];
    char dfname[OS_PATH_MAX];

    osf_internal_fd_lock(op->os, op->fd);

    status = gop_success_status;
    for (i=0; i<op->n; i++) {
        if ((osaz_attr_create(osf->osaz, op->creds, NULL, op->fd->realpath, op->key_new[i]) == 1) &&
                (osaz_attr_remove(osf->osaz, op->creds, NULL, op->fd->realpath, op->key_old[i]) == 1)) {

            //** Do a Virtual Attr check
            va1 = apr_hash_get(osf->vattr_hash, op->key_old[i], APR_HASH_KEY_STRING);
            va2 = apr_hash_get(osf->vattr_hash, op->key_new[i], APR_HASH_KEY_STRING);
            if ((va1 != NULL) || (va2 != NULL)) {
                err = 1;
            } else {
                snprintf(sfname, OS_PATH_MAX, "%s/%s", op->fd->attr_dir, op->key_old[i]);
                snprintf(dfname, OS_PATH_MAX, "%s/%s", op->fd->attr_dir, op->key_new[i]);
                err = rename(sfname, dfname);
            }

            if (err != 0) {
                status.op_status = OP_STATE_FAILURE;
                status.error_code++;
            }
        } else {
            status.op_status = OP_STATE_FAILURE;
            status.error_code++;
        }
    }

    osf_internal_fd_unlock(op->os, op->fd);

    return(status);
}

//***********************************************************************
// osfile_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

gop_op_generic_t *osfile_move_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_move_attr_t *op;

    tbx_type_malloc_clear(op, osfile_move_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)fd;
    op->key_old = key_old;
    op->key_new = key_new;
    op->n = n;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_move_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_move_attr - Generates a move object attribute operation
//***********************************************************************

gop_op_generic_t *osfile_move_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_move_attr_t *op;

    tbx_type_malloc_clear(op, osfile_move_attr_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)fd;
    op->key_old = &(op->single_old);
    op->single_old = key_old;
    op->key_new = &(op->single_new);
    op->single_new = key_new;
    op->n = 1;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_move_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// osf_get_attr - Gets the attribute given the name and base directory
//***********************************************************************

int osf_get_attr(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *ofd, char *attr, void **val, int *v_size, int *atype, lio_os_authz_local_t *ug, char *realpath)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    lio_os_virtual_attr_t *va;
    osaz_attr_filter_t filter;
    tbx_list_iter_t it;
    char *ca;
    FILE *fd;
    char fname[OS_PATH_MAX];
    int n, bsize, err;

    err = 0;
    if (osaz_attr_access(osf->osaz, creds, ug, ofd->realpath, attr, OS_MODE_READ_BLOCKING, &filter) == 0) {
        *atype = 0;
        return(1);
    }

    //** We log exnode or data access
    if ((strcmp(attr, "system.exnode") == 0) || (strcmp(attr, "system.exnode.data") == 0)) {
        notify_printf(osf->olog, 1, creds, "ATTR_READ(%s, %d, %s)\n", attr, ofd->ftype, ofd->realpath);
    }

    //** Do a Virtual Attr check
    //** Check the prefix VA's first
    it = tbx_list_iter_search(osf->vattr_prefix, attr, -1);
    tbx_list_next(&it, (tbx_list_key_t **)&ca, (tbx_list_data_t **)&va);

    if (va != NULL) {
        n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
        if (strncmp(attr, va->attribute, n) == 0) {  //** Prefix matches
            err = va->get(va, os, creds, ofd, attr, val, v_size, atype);
            goto done;
        }
    }

    //** Now check the normal VA's
    va = apr_hash_get(osf->vattr_hash, attr, APR_HASH_KEY_STRING);
    if (va != NULL) {
        err = va->get(va, os, creds, ofd, attr, val, v_size, atype);
        goto done;
    }


    //** Lastly look at the actual attributes
    n = osf_resolve_attr_path(os, fname, ofd->object_name, attr, ofd->ftype, atype, 20);
    log_printf(15, "fname=%s *v_size=%d resolve=%d\n", fname, *v_size, n);
    if (n != 0) {
        if (*v_size < 0) *val = NULL;
        *v_size = -1;
        return(1);
    }

    fd = tbx_io_fopen(fname, "r");
    if (fd == NULL) {
        if (*v_size < 0) *val = NULL;
        *v_size = -1;
        return(1);
    }

    if (*v_size < 0) { //** Need to determine the size
        tbx_io_fseek(fd, 0L, SEEK_END);
        n = tbx_io_ftell(fd);
        tbx_io_fseek(fd, 0L, SEEK_SET);
        if (n < 1) {    //** Either have an error (-1) or an empty file (0)
           *v_size = 0;
            *val = NULL;
            tbx_io_fclose(fd);
            return((n<0) ? 1 : 0);
        } else {
            *v_size = (n > (-*v_size)) ? -*v_size : n;
            bsize = *v_size + 1;
            log_printf(15, " adjusting v_size=%d n=%d\n", *v_size, n);
            *val = malloc(bsize);
         }
    } else {
        bsize = *v_size;
    }

    *v_size = tbx_io_fread(*val, 1, *v_size, fd);
    if (bsize > *v_size) {
        ca = (char *)(*val);    //** Add a NULL terminator in case it may be a string
        ca[*v_size] = 0;
    }

    tbx_io_fclose(fd);

done:
    osaz_attr_filter_apply(osf->osaz, attr, OS_MODE_READ_BLOCKING, val, v_size, filter);

    log_printf(15, "PTR val=%p *val=%s\n", val, (char *)(*val));

    return(err);
}

//***********************************************************************
// osf_get_ma_links - Does the actual attribute retreival when links are
//       encountered
//***********************************************************************

gop_op_status_t osf_get_ma_links(void *arg, int id, int first_link)
{
    osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
    int err, i, atype, n_locks;
    apr_thread_mutex_t *lock_table[op->n+1];
    gop_op_status_t status;

    status = gop_success_status;

    osf_multi_attr_lock(op->os, op->creds, op->fd, op->key, op->n, first_link, lock_table, &n_locks);

    err = 0;
    for (i=0; i<op->n; i++) {
        err += osf_get_attr(op->os, op->creds, op->fd, op->key[i], (void **)&(op->val[i]), &(op->v_size[i]), &atype, NULL, op->fd->realpath);
        if (op->v_size[i] > 0) {
            log_printf(15, "PTR i=%d key=%s val=%s v_size=%d\n", i, op->key[i], (char *)op->val[i], op->v_size[i]);
        } else {
            log_printf(15, "PTR i=%d key=%s val=NULL v_size=%d\n", i, op->key[i], op->v_size[i]);
        }
    }

    osf_multi_unlock(lock_table, n_locks);

    if (err != 0) status = gop_failure_status;

    return(status);
}


//***********************************************************************
// osf_get_multiple_attr_fn - Does the actual attribute retreival
//***********************************************************************

gop_op_status_t osf_get_multiple_attr_fn(void *arg, int id)
{
    osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
    int err, i, j, atype, v_start[op->n], oops;
    gop_op_status_t status;

    status = gop_success_status;

    osf_internal_fd_lock(op->os, op->fd);

    err = 0;
    oops = 0;
    for (i=0; i<op->n; i++) {
        v_start[i] = op->v_size[i];
        atype = 0;
        err += osf_get_attr(op->os, op->creds, op->fd, op->key[i], (void **)&(op->val[i]), &(op->v_size[i]), &atype, op->ug, op->fd->realpath);
        if (op->v_size[i] != 0) {
            log_printf(15, "PTR i=%d key=%s val=%s v_size=%d atype=%d err=%d\n", i, op->key[i], (char *)op->val[i], op->v_size[i], atype, err);
        } else {
            log_printf(15, "PTR i=%d key=%s val=NULL v_size=%d atype=%d err=%d\n", i, op->key[i], op->v_size[i], atype, err);
        }
        if ((atype & OS_OBJECT_SYMLINK_FLAG) > 0) {
            oops=1;
            break;
        }
    }

    osf_internal_fd_unlock(op->os, op->fd);

    if (oops == 1) { //** Multi object locking required
        for (j=0; j<=i; j++) {  //** Clean up any data allocated
            if (v_start[i] < 0) {
                if (op->val[i] != NULL) {
                    free(op->val[i]);
                    op->val[i] = NULL;
                }
            }
            op->v_size[i] = v_start[i];
        }

        return(osf_get_ma_links(arg, id, i));
    }

    return(status);
}

//***********************************************************************
// osfile_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osfile_get_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void **val, int *v_size)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_attr_op_t *op;

    tbx_type_malloc_clear(op, osfile_attr_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)ofd;
    op->key = &(op->key_tmp);
    op->key_tmp = key;
    op->val = val;
    op->v_size = v_size;
    op->n = 1;

    log_printf(15, "PTR val=%p op->val=%p\n", val, op->val);

    return(gop_tp_op_new(osf->tpc, NULL, osf_get_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osfile_get_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_attr_op_t *op;

    tbx_type_malloc_clear(op, osfile_attr_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)ofd;
    op->key = key;
    op->val = val;
    op->v_size= v_size;
    op->n = n;

    return(gop_tp_op_new(osf->tpc, NULL, osf_get_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lowlevel_set_attr - Lowlevel routione to set an attribute without cred checks
//     Designed for use with timestamps or other auto touched fields
//***********************************************************************

int lowlevel_set_attr(lio_object_service_fn_t *os, char *attr_dir, char *attr, void *val, int v_size)
{
    FILE *fd;
    char fname[OS_PATH_MAX];

    snprintf(fname, OS_PATH_MAX, "%s/%s", attr_dir, attr);
    if (v_size < 0) { //** Want to remove the attribute
        safe_remove(os, fname);
    } else {
        fd = tbx_io_fopen(fname, "w");
        if (fd == NULL) return(-1);
        if (v_size > 0) tbx_io_fwrite(val, v_size, 1, fd);
        tbx_io_fclose(fd);
    }

    return(0);
}

//***********************************************************************
// osf_set_attr - Sets the attribute given the name and base directory
//***********************************************************************

int osf_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, osfile_fd_t *ofd, char *attr, void *val, int v_size, int *atype, int append_val)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    tbx_list_iter_t it;
    FILE *fd;
    lio_os_virtual_attr_t *va;
    osaz_attr_filter_t filter;
    int n;
    char *ca;
    char fname[OS_PATH_MAX];

    if (osaz_attr_access(osf->osaz, creds, NULL, ofd->realpath, attr, OS_MODE_WRITE_BLOCKING, &filter) == 0) {
        *atype = 0;
        return(1);
    }

    //** Do a Virtual Attr check
    //** Check the prefix VA's first
    it = tbx_list_iter_search(osf->vattr_prefix, attr, -1);
    tbx_list_next(&it, (tbx_list_key_t **)&ca, (tbx_list_data_t **)&va);
    if (va != NULL) {
        n = (int)(long)va->priv;  //*** HACKERY **** to get the attribute length
        if (strncmp(attr, va->attribute, n) == 0) {  //** Prefix matches
            return(va->set(va, os, creds, ofd, attr, val, v_size, atype));
        }
    }

    //** Now check the normal VA's
    va = apr_hash_get(osf->vattr_hash, attr, APR_HASH_KEY_STRING);
    if (va != NULL) {
        return(va->set(va, os, creds, ofd, attr, val, v_size, atype));
    }

    if (v_size == -2) { //** Want to remove the attribute from the object ignoring if it's a symlink
        if (osaz_attr_remove(osf->osaz, creds, NULL, ofd->realpath, attr) == 0) return(1);
        snprintf(fname, OS_PATH_MAX, "%s/%s", ofd->attr_dir, attr);
        safe_remove(os, fname);
        return(0);
    }

    n = osf_resolve_attr_path(os, fname, ofd->object_name, attr, ofd->ftype, atype, 20);
    if (n != 0) {
        log_printf(15, "ERROR resolving path: fname=%s object_name=%s attr=%s\n", fname, ofd->object_name, attr);
        return(1);
    }

    if ( v_size == -1) {  //** Want to delete the attribute target following the symlinks
        if (osaz_attr_remove(osf->osaz, creds, NULL, ofd->realpath, attr) == 0) return(1);
        safe_remove(os, fname);
        return(0);
    }

    //** Store the value
    if (lio_os_local_filetype(fname) != OS_OBJECT_FILE_FLAG) {
        if (osaz_attr_create(osf->osaz, creds, NULL, ofd->realpath, attr) == 0) return(1);
    }

    fd = tbx_io_fopen(fname, (append_val == 0) ? "w" : "a");
    if (fd == NULL) log_printf(0, "ERROR opening attr file attr=%s val=%p v_size=%d fname=%s append=%d\n", attr, val, v_size, fname, append_val);
    if (fd == NULL) return(-1);
    if (v_size > 0) tbx_io_fwrite(val, v_size, 1, fd);
    tbx_io_fclose(fd);

    return(0);
}

//***********************************************************************
// osf_set_multiple_attr_fn - Does the actual attribute setting
//***********************************************************************

gop_op_status_t osf_set_multiple_attr_fn(void *arg, int id)
{
    osfile_attr_op_t *op = (osfile_attr_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    int err, i, atype, n_locks;
    apr_thread_mutex_t *lock_table[op->n+1];
    gop_op_status_t status;

    status = gop_success_status;

    osf_multi_attr_lock(op->os, op->creds, op->fd, op->key, op->n, 0, lock_table, &n_locks);

    err = 0;
    for (i=0; i<op->n; i++) {
        err += osf_set_attr(op->os, op->creds, op->fd, op->key[i], op->val[i], op->v_size[i], &atype, 0);
    }

    os_log_warm_if_needed(osf->olog, op->creds, op->fd->realpath, op->fd->ftype, op->n, op->key, op->v_size);

    osf_multi_unlock(lock_table, n_locks);

    if (err != 0) status = gop_failure_status;

    return(status);
}

//***********************************************************************
// osfile_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

gop_op_generic_t *osfile_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char *key, void *val, int v_size)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_attr_op_t *op;

    tbx_type_malloc(op, osfile_attr_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)ofd;
    op->key = &(op->key_tmp);
    op->key_tmp = key;
    op->val = &(op->val_tmp);
    op->val_tmp = val;
    op->v_size = &(op->v_tmp);
    op->v_tmp = v_size;
    op->n = 1;

    return(gop_tp_op_new(osf->tpc, NULL, osf_set_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

gop_op_generic_t *osfile_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, char **key, void **val, int *v_size, int n)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_attr_op_t *op;

    tbx_type_malloc(op, osfile_attr_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->fd = (osfile_fd_t *)ofd;
    op->key = key;
    op->val = val;
    op->v_size = v_size;
    op->n = n;

    return(gop_tp_op_new(osf->tpc, NULL, osf_set_multiple_attr_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_next_attr - Returns the next matching attribute
//***********************************************************************

int osfile_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
    osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
    int i, n, atype;
    apr_ssize_t klen;
    lio_os_virtual_attr_t *va;
    struct dirent *entry;
    char *rp;
    lio_os_regex_table_t *rex = it->regex;

    //** Check the VA's 1st
    rp = (it->realpath) ? it->realpath : it->fd->realpath;
    while (it->va_index != NULL) {
        apr_hash_this(it->va_index, (const void **)key, &klen, (void **)&va);
        it->va_index = apr_hash_next(it->va_index);
        for (i=0; i<rex->n; i++) {
            n = (rex->regex_entry[i].fixed == 1) ? strcmp(rex->regex_entry[i].expression, va->attribute) : regexec(&(rex->regex_entry[i].compiled), va->attribute, 0, NULL, 0);
            if (n == 0) { //** got a match
                n = it->v_max;
                if (osf_get_attr(it->fd->os, it->creds, it->fd, va->attribute, val, &n, &atype, NULL, rp) == 0) {
                    *v_size = n;
                    *key = strdup(va->attribute);
                    return(0);
                }
            }
        }
    }

    if (it->d == NULL) {
        log_printf(0, "ERROR: it->d=NULL\n");
        return(-1);
    }

    while ((entry = readdir(it->d)) != NULL) {
        for (i=0; i<rex->n; i++) {
            n = (rex->regex_entry[i].fixed == 1) ? strcmp(rex->regex_entry[i].expression, entry->d_name) : regexec(&(rex->regex_entry[i].compiled), entry->d_name, 0, NULL, 0);
            log_printf(15, "key=%s match=%d\n", entry->d_name, n);
            if (n == 0) {
                if ((strncmp(entry->d_name, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) ||
                        (strcmp(entry->d_name, ".") == 0) || (strcmp(entry->d_name, "..") == 0)) n = 1;
            }

            if (n == 0) { //** got a match
                n = it->v_max;
                if (osf_get_attr(it->fd->os, it->creds, it->fd, entry->d_name, val, &n, &atype, NULL, rp) == 0) {
                    *v_size = n;
                    *key = strdup(entry->d_name);
                    log_printf(15, "key=%s val=%s\n", *key, (char *)(*val));
                    return(0);
                }
            }
        }
    }

    return(-1);
}

//***********************************************************************
// osfile_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//***********************************************************************

os_attr_iter_t *osfile_create_attr_iter(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, lio_os_regex_table_t *attr, int v_max)
{
    osfile_fd_t *fd = (osfile_fd_t *)ofd;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)fd->os->priv;
    osfile_attr_iter_t *it;

    tbx_type_malloc_clear(it, osfile_attr_iter_t, 1);

    it->os = os;

    //** This is a real kludge but the only option since APR doesn't support a hash iterator destroy function.  The only way to do it
    //** is create a memory pool just for the hash iterator and destroy it when the attr iter is destroyed.
    //** If this isn't done and you use the osf->mpool you end up with a slow memory accumulator and also need to add locks to protect the
    //** the shared mpoll since they aren't thread safe
    assert_result(apr_pool_create(&(it->mpool), NULL), APR_SUCCESS);
    it->va_index = apr_hash_first(it->mpool, osf->vattr_hash);

    it->d = opendir(fd->attr_dir);
    it->regex = attr;
    it->fd = fd;
    it->creds = creds;
    it->v_max = v_max;

    return((os_attr_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_attr_iter - Destroys an attribute iterator
//***********************************************************************

void osfile_destroy_attr_iter(os_attr_iter_t *oit)
{
    osfile_attr_iter_t *it = (osfile_attr_iter_t *)oit;
    if (it->d != NULL) closedir(it->d);

    apr_pool_destroy(it->mpool);
    free(it);
}

//***********************************************************************
// osfile_next_object_serial - Returns the iterators next matching object
//***********************************************************************

int osfile_next_object_serial(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    osf_object_iter_t *it = (osf_object_iter_t *)oit;
    osfile_attr_iter_t *ait;
    osfile_open_op_t op;
    osfile_attr_op_t aop;
    gop_op_status_t status;
    int ftype, dir_change;

    ftype = osf_next_object(it, fname, prefix_len, &dir_change);

    if (*fname != NULL) {
        if (it->n_list < 0) {  //** Attr regex mode
            if (it->it_attr != NULL) {
                if (*(it->it_attr) != NULL) osfile_destroy_attr_iter(*(it->it_attr));
                if (it->fd != NULL) {
                    op.os = it->os;
                    op.cfd = it->fd;
                    osfile_close_object_fn((void *)&op, 0);
                    it->fd = NULL;
                }

                log_printf(15, "making new iterator\n");
                op.os = it->os;
                op.creds = it->creds;
                op.path = strdup(*fname);
                op.fd = (osfile_fd_t **)&(it->fd);
                op.mode = OS_MODE_READ_IMMEDIATE;
                op.id = NULL;
                op.max_wait = 0;
                op.uuid = 0;
                op.ug = &(it->ug);
                op.realpath = it->realpath;
                tbx_random_get_bytes(&(op.uuid), sizeof(op.uuid));
                status = osfile_open_object_fn(&op, 0);
                if (status.op_status != OP_STATE_SUCCESS) return(-1);

                log_printf(15, "after object open it->rp=%s\n", it->realpath);
                ait = osfile_create_attr_iter(it->os, it->creds, it->fd, it->attr, it->v_max);
                *(it->it_attr) = ait;
                ait->ug = &(it->ug);
                ait->realpath = it->realpath;
            }
        } else if (it->n_list > 0) {  //** Fixed list mode
            op.os = it->os;
            op.creds = it->creds;
            op.path = strdup(*fname);
            op.fd = (osfile_fd_t **)&(it->fd);
            op.mode = OS_MODE_READ_IMMEDIATE;
            op.id = NULL;
            op.max_wait = 0;
            op.uuid = 0;
            op.ug = &(it->ug);
            op.realpath = it->realpath;
            tbx_random_get_bytes(&(op.uuid), sizeof(op.uuid));
            status = osfile_open_object_fn(&op, 0);
            if (status.op_status != OP_STATE_SUCCESS) return(-1);

            aop.os = it->os;
            aop.creds = it->creds;
            aop.fd = (os_fd_t *)it->fd;
            aop.key = it->key;
            aop.ug = &(it->ug);
            aop.realpath = it->realpath;

            aop.val = it->val;
            aop.v_size = it->v_size;
            memcpy(it->v_size, it->v_size_user, sizeof(int)*it->n_list);
            aop.n = it->n_list;
            osf_get_multiple_attr_fn(&aop, 0);

            op.os = it->os;
            op.cfd = it->fd;
            osfile_close_object_fn((void *)&op, 0);
            it->fd = NULL;

        }

        return(ftype);
    }

    return(0);
}

//***********************************************************************
// osfile_next_object_parallel - Returns the iterators next matching object
//***********************************************************************

int osfile_next_object_parallel(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    osf_object_iter_t *it = (osf_object_iter_t *)oit;
    piter_t *piter = it->piter;
    int slot = piter->attr_curr_slot;
    piq_attr_t *pa = piter->attr_curr;
    int i, ftype;

    if (slot == -1) return(0);  //** Nothing left to do

    //** See if we load the next one
    if (pa[slot].len == 0) {
        if (tbx_que_get(piter->que_attr, pa, TBX_QUE_BLOCK) != 0) {
            piter->attr_curr_slot = -1;
            return(0);  //** Nothing left so kick out
        }
        slot = 0;
    }

    ftype = pa[slot].len; slot++;  //**Base slot is the ftype
    *prefix_len = pa[slot].len; *fname = (char *)pa[slot].value; slot++;  //** The next is the fname and prefix_len

    //**Now peel off the attrs
    for (i=0; i<it->n_list; i++) {
        it->v_size[i] = pa[slot].len;
        it->val[i] = pa[slot].value;
        slot++;
    }

    piter->attr_curr_slot = slot;  //** Update the slot
    return(ftype);
}

//***********************************************************************
// osfile_next_object - Returns the iterators next matching object
//***********************************************************************

int osfile_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    osf_object_iter_t *it = (osf_object_iter_t *)oit;

    return(it->next_object(oit, fname, prefix_len));
}

//***********************************************************************
// osfile_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//***********************************************************************

os_object_iter_t *osfile_create_object_iter(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        lio_os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osf_object_iter_t *it;
    osf_obj_level_t *itl;
    char fname[OS_PATH_MAX];
    int i;

    tbx_type_malloc_clear(it, osf_object_iter_t, 1);

    it->os = os;
    it->table = path;
    it->object_regex = object_regex;
    it->recurse_depth = recurse_depth;
    it->max_level = path->n + recurse_depth;
    it->creds = creds;
    it->prev_match_prefix = -1;
    it->next_object = osfile_next_object_serial;

    osaz_ug_hint_init(osf->osaz, it->creds, &(it->ug));
    it->ug.creds = creds;
    osaz_ug_hint_set(osf->osaz, it->creds, &(it->ug));

    it->v_max = v_max;
    it->attr = attr;
    it->it_attr = it_attr;
    if (it_attr != NULL) *it_attr = NULL;
    it->n_list = (it_attr == NULL) ? 0 : -1;  //**  Using the attr iter if -1
    it->recurse_stack = tbx_stack_new();
    it->object_types = object_types;
    if (object_types & OS_OBJECT_FOLLOW_SYMLINK_FLAG) { //** Following symlinks so setup the hash
        apr_pool_create(&it->mpool, NULL);
        it->symlink_loop = apr_hash_make(it->mpool);
    }
    tbx_type_malloc_clear(it->level_info, osf_obj_level_t, it->table->n);
    for (i=0; i<it->table->n; i++) {
        itl = &(it->level_info[i]);
        itl->firstpass = 1;
        itl->preg = &(path->regex_entry[i].compiled);
        if (path->regex_entry[i].fixed == 1) {
            itl->fragment = path->regex_entry[i].expression;
            itl->fixed_prefix = path->regex_entry[i].fixed_prefix;
        }
    }

    if (it->table->n == 1) { //** Single level so check if a fixed path and if so tweak things
        if ((itl->fragment != NULL) && (itl->fixed_prefix > 0)) itl->fixed_prefix--;
    }

    if (object_regex != NULL) it->object_preg = &(object_regex->regex_entry[0].compiled);

//log_printf(0, "OBJITER: n=%d\n", it->table->n);
    if (it->table->n > 0) {
        itl = &(it->level_info[0]);
        itl->path[0] = '\0';
//log_printf(0, "OBJITER: fragment=%s\n", itl->fragment);
//log_printf(0, "OBJITER: file_path=%s\n", itl->fragment);
        snprintf(fname, OS_PATH_MAX, "/%s", itl->fragment);
        itl->realpath[0] = '\0';
        itl->d = my_opendir(osf->file_path, itl->fragment);
        itl->curr_pos = my_telldir(itl->d);
        itl->firstpass = 1;
    }

    return((os_object_iter_t *)it);
}

//***********************************************************************
// osfile_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *osfile_create_object_iter_alist(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    osf_object_iter_t *it;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i, n;

    //** Use the regex attr version to make the base struct
    it = osfile_create_object_iter(os, creds, path, object_regex, object_types, NULL, recurse_depth, NULL, 0);
    if (it == NULL) return(NULL);

    if (n_keys < 1) return(it);

    //** Tweak things for the fixed key list
    it->n_list = n_keys;
    it->key = key;
    it->val = val;
    it->v_size = v_size;
    tbx_type_malloc(it->v_size_user, int, it->n_list);
    memcpy(it->v_size_user, v_size, sizeof(int)*it->n_list);

    it->v_fixed = 1;
    for (i=0; i < n_keys; i++) {
        if (v_size[i] < 0) {
            it->v_fixed = 0;
            break;
        }
    }

    //** See if we need to setup for a parallel iter
    if (osf->piter_enable) {
        tbx_type_malloc_clear(it->piter, piter_t, 1);
        if ((object_types & OS_OBJECT_SYMLINK_FLAG) == 0) { //** We might be able short circuit some steps
            if (attr_list_is_special(os, key, n_keys) == 0) it->piter->optimized_enable = 1;
        }
        it->piter->que_fname = tbx_que_create(osf->n_piter_que_fname, sizeof(piq_fname_t)*(osf->n_piter_fname_size+1));
        n = (it->n_list+2) * (osf->n_piter_que_attr + 1);  //** The (n_list+2) is for the ftype and fname which are the first 2 and the last +1 is used as an fname terminator
        it->piter->que_attr = tbx_que_create(osf->n_piter_que_attr, sizeof(piq_attr_t) * n);
        tbx_type_malloc_clear(it->piter->attr_curr, piq_attr_t, n);
        assert_result(apr_pool_create(&(it->piter->mpool), NULL), APR_SUCCESS);
        tbx_type_malloc_clear(it->piter->attr_workers, apr_thread_t *, osf->n_piter_threads);
        tbx_atomic_set(it->piter->n_active, osf->n_piter_threads);
        for (i=0; i<osf->n_piter_threads; i++) {
            tbx_thread_create_assert(&(it->piter->attr_workers[i]), NULL, piter_attr_thread, (void *)it, it->piter->mpool);
        }
        tbx_thread_create_assert(&(it->piter->fname_worker), NULL, piter_fname_thread, (void *)it, it->piter->mpool);
        it->next_object = osfile_next_object_parallel;
    }

    return(it);
}

//***********************************************************************
// osfile_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void osfile_destroy_object_iter(os_object_iter_t *oit)
{
    osf_object_iter_t *it = (osf_object_iter_t *)oit;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    osf_obj_level_t *itl;
    osfile_open_op_t open_op;
    apr_hash_index_t *hi;
    void *key, *val;
    apr_ssize_t klen;
    apr_status_t value;
    char *fname;
    int i, ftype;

    //** Shutdown the piter if enabled
    if (it->piter) {
        //** Flag us as finished
        tbx_atomic_set(it->piter->abort, 1);
        tbx_que_set_finished(it->piter->que_fname);

        //** Wait for the fname thread to complete
        apr_thread_join(&value, it->piter->fname_worker);

        //** Drop everything on the attr que
        while ((ftype = osfile_next_object(oit, &fname, &i)) > 0) {
            for (i=0; i<it->n_list; i++) {
                if (it->v_size[i] > 0) free(it->val[i]);
            }
            free(fname);
        }

        //** Wait for the threads to complete
        for (i=0; i<osf->n_piter_threads; i++) {
            apr_thread_join(&value, it->piter->attr_workers[i]);
        }
        free(it->piter->attr_workers);
        free(it->piter->attr_curr);

        //** Tear down the piter
        tbx_que_destroy(it->piter->que_fname);
        tbx_que_destroy(it->piter->que_attr);
        apr_pool_destroy(it->piter->mpool);
        free(it->piter);
    }

    //** Close any open directories
    for (i=0; i<it->table->n; i++) {
        if (it->level_info[i].d != NULL) my_closedir(it->level_info[i].d);
    }

    while ((itl = (osf_obj_level_t *)tbx_stack_pop(it->recurse_stack)) != NULL) {
        my_closedir(itl->d);
        free(itl);
    }

    if (it->it_attr != NULL) {
        if (*it->it_attr != NULL) osfile_destroy_attr_iter(*(it->it_attr));
    }

    if (it->fd != NULL) {
        open_op.cfd = it->fd;
        open_op.os = it->os;
        osfile_close_object_fn(&open_op, 0);
    }

    if (it->v_size_user != NULL) free(it->v_size_user);

    if (it->object_types & OS_OBJECT_FOLLOW_SYMLINK_FLAG) { //** Following symlinks so cleanup
        for (hi = apr_hash_first(NULL, it->symlink_loop); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&key, &klen, &val);
            free(key);
        }
        apr_pool_destroy(it->mpool);  //** This should also destroy the hash
    }

    osaz_ug_hint_free(osf->osaz, it->creds, &(it->ug));

    tbx_stack_free(it->recurse_stack, 1);
    free(it->level_info);
    free(it);
}


//***********************************************************************
// osfile_free_open - Frees an open object
//***********************************************************************

void osfile_free_open(void *arg)
{
    osfile_open_op_t *op = (osfile_open_op_t *)arg;

    if (op->path != NULL) free(op->path);
    if (op->id != NULL) free(op->id);

    free(op);
}

//***********************************************************************
// osfile_open_object - Opens an object
//***********************************************************************

gop_op_status_t osfile_open_object_fn(void *arg, int id)
{
    osfile_open_op_t *op = (osfile_open_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    osfile_fd_t *fd;
    int ftype, err;
    char fname[OS_PATH_MAX];
    char rpath[OS_PATH_MAX];
    char *rp;
    gop_op_status_t status;

    log_printf(15, "Attempting to open object=%s\n", op->path);

    *op->fd = NULL;
    snprintf(fname, OS_PATH_MAX, "%s%s", osf->file_path, op->path);
    ftype = lio_os_local_filetype(fname);
    if (ftype <= 0) {
        return(gop_failure_status);
    }

    rp = (op->realpath) ? op->realpath : _osf_realpath(op->os, op->path, rpath, 1);
    if (osaz_object_access(osf->osaz, op->creds, op->ug, rp, op->mode) == 0)  {
        return(gop_failure_status);
    }

    tbx_type_malloc(fd, osfile_fd_t, 1);

    osf_retrieve_lock(op->os, rpath, &(fd->ilock_rp));
    osf_retrieve_lock(op->os, op->path, &(fd->ilock_obj));

    fd->os = op->os;
    fd->ftype = ftype;
    fd->mode = op->mode & OS_MODE_BASE_MODES;  //** We ignore any lock modifier
    fd->object_name = op->path;
    fd->id = op->id;
    fd->uuid = op->uuid;
    strncpy(fd->realpath, rp, OS_PATH_MAX-1);

    fd->attr_dir = object_attr_dir(op->os, osf->file_path, fd->object_name, ftype);

    err = full_object_lock(FOL_OS, osf->os_lock, fd, fd->mode, op->max_wait);  //** Do a full lock if needed
    log_printf(15, "full_object_lock=%d fname=%s uuid=" LU " max_wait=%d fd=%p fd->fol=%p\n", err, fd->object_name, fd->uuid, op->max_wait, fd, fd->fol);
    if (err != 0) {  //** Either a timeout or abort occured
        *(op->fd) = NULL;
        free(fd->attr_dir);
        free(fd);
        status = gop_failure_status;
    } else {
        *(op->fd) = (os_fd_t *)fd;
        op->path = NULL;  //** This is now used by the fd
        op->id = NULL;
        status = gop_success_status;
    }

    //** Also add us to the open file list
    apr_thread_mutex_lock(osf->open_fd_lock);
    tbx_list_insert(osf->open_fd, fd->object_name, fd);
    apr_thread_mutex_unlock(osf->open_fd_lock);

    return(status);
}

//***********************************************************************
//  osfile_open_object - Makes the open file op
//***********************************************************************

gop_op_generic_t *osfile_open_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_open_op_t *op;

    tbx_type_malloc_clear(op, osfile_open_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->path = strdup(path);
    op->fd = (osfile_fd_t **)pfd;
    op->max_wait = max_wait;
    op->mode = mode;
    op->id = (id == NULL) ? strdup(osf->host_id) : strdup(id);
    op->uuid = 0;
    tbx_random_get_bytes(&(op->uuid), sizeof(op->uuid));

    return(gop_tp_op_new(osf->tpc, NULL, osfile_open_object_fn, (void *)op, osfile_free_open, 1));
}

//***********************************************************************
// osfile_abort_open_object_fn - Performs the actual open abort operation
//***********************************************************************

gop_op_status_t osfile_abort_open_object_fn(void *arg, int id)
{
    osfile_open_op_t *op = (osfile_open_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    gop_op_status_t status;
    fobj_lock_t *fol;
    fobj_lock_task_t *handle;

    if (op->mode == OS_MODE_READ_IMMEDIATE) return(gop_success_status);

    apr_thread_mutex_lock(osf->os_lock->fobj_lock);

    fol = tbx_list_search(osf->os_lock->fobj_table, op->path);

    //** Find the task in the pending list and remove it
    status = gop_failure_status;
    tbx_stack_move_to_top(fol->pending_stack);
    while ((handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack)) != NULL) {
        if (handle->fd->uuid == op->uuid) {
            tbx_stack_delete_current(fol->pending_stack, 1, 0);
            status = gop_success_status;
            handle->abort = 1;
            apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released
            break;
        }
        tbx_stack_move_down(fol->pending_stack);
    }

    apr_thread_mutex_unlock(osf->os_lock->fobj_lock);

    return(status);
}


//***********************************************************************
//  osfile_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

gop_op_generic_t *osfile_abort_open_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    gop_thread_pool_op_t *tpop = gop_get_tp(gop);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_abort_open_object_fn, tpop->arg, NULL, 1));
}


//***********************************************************************
// osfile_close_object - Closes an object
//***********************************************************************

gop_op_status_t osfile_close_object_fn(void *arg, int id)
{
    osfile_open_op_t *op = (osfile_open_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->cfd->os->priv;

    if (op->cfd == NULL) return(gop_success_status);

    full_object_unlock(FOL_OS, osf->os_lock, op->cfd, op->cfd->mode);
    if (op->cfd->user_mode != 0) full_object_unlock(FOL_USER, osf->os_lock_user, op->cfd, op->cfd->user_mode);
    apr_thread_mutex_lock(osf->open_fd_lock);
    tbx_list_remove(osf->open_fd, op->cfd->object_name, op->cfd);
    apr_thread_mutex_unlock(osf->open_fd_lock);
    free(op->cfd->object_name);
    free(op->cfd->attr_dir);
    free(op->cfd->id);
    free(op->cfd);

    return(gop_success_status);
}

//***********************************************************************
//  osfile_close_object - Makes the open file op
//***********************************************************************

gop_op_generic_t *osfile_close_object(lio_object_service_fn_t *os, os_fd_t *ofd)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_open_op_t *op;

    tbx_type_malloc(op, osfile_open_op_t, 1);

    op->os = os;
    op->cfd = (osfile_fd_t *)ofd;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_close_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_lock_user_object_fn - Applies a user lock on the object
//***********************************************************************

gop_op_status_t osfile_lock_user_object_fn(void *arg, int id)
{
    osfile_lock_user_op_t *op = (osfile_lock_user_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    int err;
    gop_op_status_t status;

    if (op->fd->user_mode != 0) { //** Already have a lock so see how we change it
        if (op->mode & OS_MODE_UNLOCK) { //** Got an unlock operation
           full_object_unlock(FOL_USER, osf->os_lock_user, op->fd, op->fd->user_mode);
           op->fd->user_mode = op->mode;
           return(gop_success_status);
        } else if (op->fd->user_mode & OS_MODE_READ_BLOCKING) {
           if (op->mode & OS_MODE_WRITE_BLOCKING) {
               full_object_unlock(FOL_USER, osf->os_lock_user, op->fd, op->fd->user_mode);
               op->fd->user_mode = 0;
               err = full_object_lock(FOL_USER, osf->os_lock_user, op->fd, op->mode, op->max_wait);
               if (err == 0) op->fd->user_mode = op->mode;
               goto finished;
           }
           return(gop_success_status);
        } else if (op->fd->user_mode & OS_MODE_WRITE_BLOCKING) {
            if (op->mode & OS_MODE_READ_BLOCKING) {
                full_object_downgrade_lock(FOL_USER, osf->os_lock_user, op->fd);
                op->fd->user_mode = op->mode;
            }
            return(gop_success_status);
        }
    } else if (op->mode & OS_MODE_UNLOCK) {  //** No previous lock held so just ignore it
        return(gop_success_status);
    }

    err = full_object_lock(FOL_USER, osf->os_lock_user, op->fd, op->mode, op->max_wait);
    if (err == 0) op->fd->user_mode = op->mode;

finished:
    if (err != 0) {  //** Either a timeout or abort occured
        status = gop_failure_status;
    } else {
        status = gop_success_status;
    }

    return(status);
}

//***********************************************************************
//  osfile_lock_user_object - Makes the user lock op
//***********************************************************************

gop_op_generic_t *osfile_lock_user_object(lio_object_service_fn_t *os, os_fd_t *ofd, int rw_mode, int max_wait)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_lock_user_op_t *op;

    tbx_type_malloc_clear(op, osfile_lock_user_op_t, 1);

    op->os = os;
    op->fd = (osfile_fd_t *)ofd;
    op->max_wait = max_wait;
    op->mode = rw_mode;

    return(gop_tp_op_new(osf->tpc, NULL, osfile_lock_user_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_abort_lock_user_object_fn - Performs the actual uer lock abort operation
//***********************************************************************

gop_op_status_t osfile_abort_lock_user_object_fn(void *arg, int id)
{
    osfile_lock_user_op_t *op = (osfile_lock_user_op_t *)arg;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)op->os->priv;
    gop_op_status_t status;
    fobj_lock_t *fol;
    fobj_lock_task_t *handle;

    if (op->mode & OS_MODE_READ_IMMEDIATE) return(gop_success_status);

    apr_thread_mutex_lock(osf->os_lock_user->fobj_lock);

    fol = tbx_list_search(osf->os_lock_user->fobj_table, op->fd->realpath);
    if (fol == NULL) return(gop_failure_status);

    //** Find the task in the pending list and remove it
    status = gop_failure_status;
    tbx_stack_move_to_top(fol->pending_stack);
    while ((handle = (fobj_lock_task_t *)tbx_stack_get_current_data(fol->pending_stack)) != NULL) {
        if (handle->fd->uuid == op->fd->uuid) {
            tbx_stack_delete_current(fol->pending_stack, 1, 0);
            status = gop_success_status;
            handle->abort = 1;
            apr_thread_cond_signal(handle->cond);   //** They will wake up when fobj_lock is released
            break;
        }
        tbx_stack_move_down(fol->pending_stack);
    }

    apr_thread_mutex_unlock(osf->os_lock_user->fobj_lock);

    return(status);
}


//***********************************************************************
//  osfile_abort_lock_user_object - Aborts an ongoing user lock operation
//***********************************************************************

gop_op_generic_t *osfile_abort_lock_user_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    gop_thread_pool_op_t *tpop = gop_get_tp(gop);

    return(gop_tp_op_new(osf->tpc, NULL, osfile_abort_lock_user_object_fn, tpop->arg, NULL, 1));
}

//***********************************************************************
// osf_fsck_check_file - Checks the file integrity
//***********************************************************************

int osf_fsck_check_file(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int dofix)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char fullname[OS_PATH_MAX];
    char rpath[OS_PATH_MAX];
    char *faname;
    int ftype;
    FILE *fd;

    //** Check if we can access it.  If not flag success and return
    if (osaz_object_access(osf->osaz, creds, NULL, _osf_realpath(os, fname, rpath, 1), OS_MODE_READ_IMMEDIATE) != 2) return(OS_FSCK_GOOD);

    //** Make sure the proxy entry exists
    snprintf(fullname, OS_PATH_MAX, "%s%s", osf->file_path, fname);
    ftype = lio_os_local_filetype(fullname);
    if (ftype == 0) {
        if (dofix == OS_FSCK_MANUAL) return(OS_FSCK_MISSING_OBJECT);
        if (dofix == OS_FSCK_REMOVE) {
            //** Remove the FA dir
            osf_object_remove(os, fullname);
            return(OS_FSCK_GOOD);
        }

        log_printf(15, "repair  fullname=%s\n", fullname);
        fd = tbx_io_fopen(fullname, "w");
        if (fd == NULL) return(OS_FSCK_MISSING_OBJECT);
        tbx_io_fclose(fd);

        ftype = OS_OBJECT_FILE_FLAG;
    }

    log_printf(15, "fullname=%s\n", fullname);

    //** Make sure the FA directory exists
    faname = object_attr_dir(os, osf->file_path, fname, ftype);
    ftype = lio_os_local_filetype(faname);
    log_printf(15, "faname=%s ftype=%d\n", faname, ftype);

    if (((ftype & OS_OBJECT_DIR_FLAG) == 0) || ((ftype & OS_OBJECT_BROKEN_LINK_FLAG) > 0)) {
        if (dofix == OS_FSCK_MANUAL) {
            free(faname);
            return(OS_FSCK_MISSING_ATTR);
        }
        if (dofix == OS_FSCK_REMOVE) {
            //** Remove the FA dir
            osf_object_remove(os, fullname);
            free(faname);
            return(OS_FSCK_GOOD);
        }

        ftype = mkdir(faname, DIR_PERMS);
        if (ftype != 0) {
            free(faname);
            return(OS_FSCK_MISSING_ATTR);
        }
    }

    free(faname);
    return(OS_FSCK_GOOD);
}

//***********************************************************************
// osf_fsck_check_dir - Checks the dir integrity
//***********************************************************************

int osf_fsck_check_dir(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int dofix)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char *faname;
    char rpath[OS_PATH_MAX];
    int ftype;

    //** Check if we can access it.  If not flag success and return
    if (osaz_object_access(osf->osaz, creds,NULL,  _osf_realpath(os, fname, rpath, 1), OS_MODE_READ_IMMEDIATE) != 2) return(OS_FSCK_GOOD);

    //** Make sure the FA directory exists
    faname = object_attr_dir(os, osf->file_path, fname, OS_OBJECT_DIR_FLAG);
    ftype = lio_os_local_filetype(faname);
    log_printf(15, "fname=%s faname=%s ftype=%d\n", fname, faname, ftype);
    if ((ftype & OS_OBJECT_DIR_FLAG) == 0) {
        if (dofix == OS_FSCK_MANUAL) {
            free(faname);
            return(OS_FSCK_MISSING_ATTR);
        }
        if (dofix == OS_FSCK_REMOVE) {
            //** Remove the FA dir
            osf_object_remove(os, fname);
            free(faname);
            return(OS_FSCK_GOOD);
        }

        ftype = mkdir(faname, DIR_PERMS);
        if (ftype != 0) {
            free(faname);
            return(OS_FSCK_MISSING_ATTR);
        }
    }

    free(faname);
    return(OS_FSCK_GOOD);
}


//***********************************************************************
// osf_next_fsck - Returns the next object to check
//***********************************************************************

int osf_next_fsck(os_fsck_iter_t *oit, char **fname)
{
    osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;
    int prefix_len;
    char fullname[OS_PATH_MAX];
    char *faname;
    struct dirent *entry;

    int atype;

    if (it->ad != NULL) {  //** Checking attribute dir
        while ((entry = readdir(it->ad)) != NULL) {
            if (strncmp(entry->d_name, FILE_ATTR_PREFIX, FILE_ATTR_PREFIX_LEN) == 0) {  //** Got a match
                snprintf(fullname, OS_PATH_MAX, "%s/%s", it->ad_path, &(entry->d_name[FILE_ATTR_PREFIX_LEN]));
                log_printf(15, "ad_path=%s fname=%s d_name=%s\n", it->ad_path, fullname, entry->d_name);
                *fname = strdup(fullname);
                return(OS_OBJECT_FILE_FLAG);
            }
        }

        log_printf(15, "free(ad_path=%s)\n", it->ad_path);
        free(it->ad_path);
        it->ad_path = NULL;
        closedir(it->ad);
        it->ad = NULL;
    }

    //** Use the object iterator
    atype = os_next_object(it->os, it->it, fname, &prefix_len);

    if (atype & OS_OBJECT_DIR_FLAG) {  //** Got a directory so prep scanning it for next round
        faname = object_attr_dir(it->os, osf->file_path, *fname, OS_OBJECT_DIR_FLAG);
        it->ad = opendir(faname);
        log_printf(15, "ad_path faname=%s ad=%p\n", faname, it->ad);
        free(faname);
        if (it->ad != NULL) it->ad_path = strdup(*fname);
    }

    return(atype);
}

//***********************************************************************
// osfile_fsck_object_check - Resolves the error with the problem object
//***********************************************************************

int osfile_fsck_object_check(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int ftype, int resolution)
{
    int err;

    log_printf(15, "mode=%d ftype=%d fname=%s\n", resolution, ftype, fname);
    if (ftype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG)) {
        err = osf_fsck_check_file(os, creds, fname, resolution);
    } else {
        err = osf_fsck_check_dir(os, creds, fname, resolution);
    }

    return(err);
}

//***********************************************************************
//  osfile_fsck_object_fn - Does the actual object checking
//***********************************************************************

gop_op_status_t osfile_fsck_object_fn(void *arg, int id)
{
    osfile_open_op_t *op = (osfile_open_op_t *)arg;
    gop_op_status_t status;

    status = gop_success_status;

    status.error_code = osfile_fsck_object_check(op->os, op->creds, op->path, op->uuid, op->mode);

    return(status);
}

//***********************************************************************
//  osfile_fsck_object - Allocates space for the object check
//***********************************************************************

gop_op_generic_t *osfile_fsck_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int ftype, int resolution)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    osfile_open_op_t *op;

    tbx_type_malloc_clear(op, osfile_open_op_t, 1);

    op->os = os;
    op->creds = creds;
    op->path = fname;
    op->mode = resolution;
    op->uuid = ftype;   //** We store the ftype here

    return(gop_tp_op_new(osf->tpc, NULL, osfile_fsck_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// osfile_next_fsck - Returns the next problem object
//***********************************************************************

int osfile_next_fsck(lio_object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;
    char *fname;
    int atype;
    int err;

    while ((atype = osf_next_fsck(oit, &fname)) != 0) {
        if (atype & (OS_OBJECT_FILE_FLAG|OS_OBJECT_SYMLINK_FLAG)) {   //** File object
            err = osf_fsck_check_file(it->os, it->creds, fname, OS_FSCK_MANUAL);
        } else {   //** Directory object
            err = osf_fsck_check_dir(it->os, it->creds, fname, OS_FSCK_MANUAL);
        }

        if (err != OS_FSCK_GOOD) {
            *bad_atype = atype;
            *bad_fname = fname;
            return(err);
        }

        free(fname);
    }

    *bad_atype = 0;
    *bad_fname = NULL;
    return(OS_FSCK_FINISHED);
}

//***********************************************************************
// osfile_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *osfile_create_fsck_iter(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode)
{
    osfile_fsck_iter_t *it;

    tbx_type_malloc_clear(it, osfile_fsck_iter_t, 1);

    it->os = os;
    it->creds = creds;
    it->path = strdup(path);
    it->mode = mode;

    it->regex = lio_os_path_glob2regex(it->path);
    it->it = os_create_object_iter(os, creds, it->regex, NULL, OS_OBJECT_ANY_FLAG, NULL, 10000, NULL, 0);
    if (it->it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
        return(NULL);
    }

    return((os_fsck_iter_t *)it);
}

//***********************************************************************
// osfile_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void osfile_destroy_fsck_iter(lio_object_service_fn_t *os, os_fsck_iter_t *oit)
{
    osfile_fsck_iter_t *it = (osfile_fsck_iter_t *)oit;

    os_destroy_object_iter(os, it->it);

    if (it->ad != NULL) closedir(it->ad);
    if (it->ad_path != NULL) {
        log_printf(15, "free(ad_path=%s)\n", it->ad_path);
        free(it->ad_path);
    }

    lio_os_regex_table_destroy(it->regex);
    free(it->path);
    free(it);

    return;
}

//***********************************************************************
// osfile_print_open_fd - Prints the open file list
//***********************************************************************

void osfile_print_open_fd(lio_object_service_fn_t *os, FILE *rfd, int print_section_heading)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    char *fname;
    osfile_fd_t *fd;
    tbx_list_iter_t it;

    apr_thread_mutex_lock(osf->open_fd_lock);
    fprintf(rfd, "OSFile Open Files (n=%d) -----------------------------\n", tbx_list_key_count(osf->open_fd));

    it = tbx_list_iter_search(osf->open_fd, NULL, 0);
    tbx_list_next(&it, (tbx_list_key_t *)&fname, (tbx_list_data_t **)&fd);
    while (fname) {
        fprintf(rfd, "   fname=%s ftype=%d mode=%d  id=%s\n", fname, fd->ftype, fd->mode, fd->id);
        tbx_list_next(&it, (tbx_list_key_t *)&fname, (tbx_list_data_t **)&fd);
    }
    fprintf(rfd, "\n");
    apr_thread_mutex_unlock(osf->open_fd_lock);


}
//***********************************************************************
// osfile_print_running_config - Prints the running config
//***********************************************************************

void osfile_print_running_config(lio_object_service_fn_t *os, FILE *fd, int print_section_heading)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i;

    if (print_section_heading) fprintf(fd, "[%s]\n", osf->section);
    fprintf(fd, "type = %s\n", OS_TYPE_FILE);
    fprintf(fd, "base_path = %s\n", osf->base_path);
    fprintf(fd, "lock_table_size = %d\n", osf->internal_lock_size);
    fprintf(fd, "max_copy = %d\n", osf->max_copy);
    fprintf(fd, "hardlink_dir_size = %d\n", osf->hardlink_dir_size);
    fprintf(fd, "authz = %s\n", osf->authz_section);
    fprintf(fd, "log_activity = %s\n", osf->os_activity);

    fprintf(fd, "shard_enable = %d\n", osf->shard_enable);
    if (osf->shard_enable) {
        fprintf(fd, "shard_splits = %d\n", osf->shard_splits);
        fprintf(fd, "#n_shard_prefix = %d\n", osf->n_shard_prefix);
        for (i=0; i<osf->n_shard_prefix; i++) {
            fprintf(fd, "shard_prefix = %s\n", osf->shard_prefix[i]);
        }

    }

    fprintf(fd, "piter_enable = %d\n", osf->piter_enable);
    if (osf->piter_enable) {
        fprintf(fd, "n_piter_threads = %d #** Number of threads for each alist iterator\n", osf->n_piter_threads);
        fprintf(fd, "n_piter_que_fname = %d #** Objects in the fname que\n", osf->n_piter_que_fname);
        fprintf(fd, "n_piter_fname_size = %d #** Max bundle of fnames in a fname que objects\n", osf->n_piter_fname_size);
        fprintf(fd, "n_piter_que_qttr = %d #** Objects in the attr que\n", osf->n_piter_que_attr);
        fprintf(fd, "n_piter_attr_size = %d #** Max size in bytes for each que attr object\n", osf->n_piter_attr_size);
    }

    fprintf(fd, "\n");

    //** Print the notification log section
    notify_print_running_config(osf->olog, fd, 1);

    //** Print the AuthZ configuration
    osaz_print_running_config(osf->osaz, fd, 1);

    //** Also print the open files
    osfile_print_open_fd(os, fd, 0);
}

//***********************************************************************
// osfile_destroy
//***********************************************************************

void osfile_destroy(lio_object_service_fn_t *os)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    int i;

    for (i=0; i<osf->internal_lock_size; i++) {
        apr_thread_mutex_destroy(osf->internal_lock[i]);
    }
    free(osf->internal_lock);

    fobj_lock_destroy(osf->os_lock);
    fobj_lock_destroy(osf->os_lock_user);
    tbx_list_destroy(osf->vattr_prefix);
    tbx_list_destroy(osf->open_fd);

    if (osf->shard_prefix) {
        for (i=0; i<osf->n_shard_prefix; i++) {
            if (osf->shard_prefix[i]) free(osf->shard_prefix[i]);
        }
        free(osf->shard_prefix);
    }

    osaz_destroy(osf->osaz);

    apr_pool_destroy(osf->mpool);

    if (osf->olog) notify_destroy(osf->olog);
    if (osf->os_activity) free(osf->os_activity);
    if (osf->authz_section) free(osf->authz_section);
    if (osf->section) free(osf->section);
    free(osf->host_id);
    free(osf->base_path);
    free(osf->file_path);
    free(osf->hardlink_path);
    free(osf);
    free(os);
}

//***********************************************************************
// osf_load_shard_prefix - Loads the shard prefixes
//***********************************************************************

void osf_load_shard_prefix(lio_object_service_fn_t *os, tbx_inip_file_t *fd, const char *section)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)os->priv;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *key, *value;
    int n;
    tbx_stack_t *stack;

    g = tbx_inip_group_find(fd, section);
    if (g == NULL) {
        log_printf(0, "WARNING: Can't find OSFile section: %s\n", section);
        return;
    }

    //** Get all the paths
    stack = tbx_stack_new();
    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "shard_prefix") == 0) {
            value = tbx_inip_ele_get_value(ele);
            tbx_stack_move_to_bottom(stack);
            tbx_stack_insert_below(stack, strdup(value));
        }
        ele = tbx_inip_ele_next(ele);
    }

    //** Now convert them to a list
    tbx_type_malloc_clear(osf->shard_prefix, char *, tbx_stack_count(stack));
    n = 0;
    while ((osf->shard_prefix[n] = tbx_stack_pop(stack)) != NULL) {
        n++;
    }

    osf->n_shard_prefix = n;
    tbx_stack_free(stack, 0);
}


//***********************************************************************
//  object_service_file_create - Creates a file backed OS
//***********************************************************************

lio_object_service_fn_t *object_service_file_create(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section)
{
    lio_object_service_fn_t *os;
    lio_osfile_priv_t *osf;
    osaz_create_t *osaz_create;
    char pname[OS_PATH_MAX], pattr[OS_PATH_MAX], rpath[OS_PATH_MAX];
    char *atype, *asection, *rp;
    int i, j, err;

    if (section == NULL) section = osf_default_options.section;

    tbx_type_malloc_clear(os, lio_object_service_fn_t, 1);
    tbx_type_malloc_clear(osf, lio_osfile_priv_t, 1);
    os->priv = (void *)osf;

    osf->section = strdup(section);

    osf->tpc = lio_lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);
    osf->base_path = NULL;
    osf->authn = lio_lookup_service(ess, ESS_RUNNING, ESS_AUTHN);
    if (fd == NULL) {
        osf->base_path = strdup("./osfile");

        osaz_create = lio_lookup_service(ess, OSAZ_AVAILABLE, OSAZ_TYPE_FAKE);
        osf->osaz = (*osaz_create)(ess, NULL, NULL, os);
        osf->internal_lock_size = 200;
        osf->max_copy = 1024*1024;
        osf->hardlink_dir_size = 256;
        osf->os_activity = strdup(osf_default_options.os_activity);
    } else {
        osf->base_path = tbx_inip_get_string(fd, section, "base_path", osf_default_options.base_path);
        osf->os_activity = tbx_inip_get_string(fd, section, "log_activity", osf_default_options.os_activity);
        osf->internal_lock_size = tbx_inip_get_integer(fd, section, "lock_table_size", osf_default_options.internal_lock_size);
        osf->max_copy = tbx_inip_get_integer(fd, section, "max_copy", osf_default_options.max_copy);
        osf->hardlink_dir_size = tbx_inip_get_integer(fd, section, "hardlink_dir_size", osf_default_options.hardlink_dir_size);
        asection = tbx_inip_get_string(fd, section, "authz", osf_default_options.authz_section);
        osf->authz_section = asection;
        atype = (asection == NULL) ? strdup(OSAZ_TYPE_FAKE) : tbx_inip_get_string(fd, asection, "type", OSAZ_TYPE_FAKE);
        osaz_create = lio_lookup_service(ess, OSAZ_AVAILABLE, atype);
        osf->osaz = (*osaz_create)(ess, fd, asection, os);
        free(atype);
        if (osf->osaz == NULL) {
            free(osf->base_path);
            free(osf);
            free(os);
            return(NULL);
        }

        //** Now get the sharding info
        osf->shard_enable = tbx_inip_get_integer(fd, section, "shard_enable", osf_default_options.shard_enable);
        if (osf->shard_enable) {
            osf->shard_splits = tbx_inip_get_integer(fd, section, "shard_splits", osf_default_options.shard_splits);
            if (osf->shard_splits <= 0) {
                log_printf(0, "WARNING: shard_splits=%d. Disabling sharding\n", osf->shard_splits);
                osf->shard_enable = 0;
                goto next;
            }

            osf_load_shard_prefix(os, fd, section);
            if (osf->n_shard_prefix == 0) {
                log_printf(0, "WARNING: n_shard_prefix=%d. Disabling sharding\n", osf->n_shard_prefix);
                osf->shard_enable = 0;
                goto next;
            }
        }

        //** Get all the parallel iter values if enabled
        osf->piter_enable = tbx_inip_get_integer(fd, section, "piter_enable", osf_default_options.piter_enable);  //** Enable parallel iterators
        if (osf->piter_enable) {
            //** Number of piter threads. For max performance this should be a a multiple of the number of shards.
            osf->n_piter_threads = tbx_inip_get_integer(fd, section, "n_piter_threads", osf_default_options.n_piter_threads);

            //** These handle how many fnames can be buffered. que_fname=# of objects in the fname que and fname_size=max # of fname in each object
            //** A new object is always created on a directory change.
            osf->n_piter_que_fname = tbx_inip_get_integer(fd, section, "n_piter_que_fname", osf_default_options.n_piter_que_fname);
            osf->n_piter_fname_size = tbx_inip_get_integer(fd, section, "n_piter_fname_size", osf_default_options.n_piter_fname_size);

            //** These handle how much memory is used to buffer parallel iter responses.  que_attr=# of objects in the attr que and
            //** attr_size=Max size in bytes for each que_attr object.  This allows packing of multiple fname/attr fetches into a single object
            //** Each entry consists of all the requested objects for a single fname. attrs for an fname are not split across objects.
            osf->n_piter_que_attr = tbx_inip_get_integer(fd, section, "n_piter_que_attr", osf_default_options.n_piter_que_attr);
            osf->n_piter_attr_size = tbx_inip_get_integer(fd, section, "n_piter_attr_size", osf_default_options.n_piter_attr_size);
        }
    }

next:
    //** Get the base path and also make sure it isn't symlinked in.  This is a requirement for all the realpath() calls to work
    snprintf(pname, OS_PATH_MAX, "%s/%s", osf->base_path, "file");
    rpath[0] = '\0';
    rp = realpath(pname, rpath);
    if ((rp == NULL) || (strcmp(pname, rpath) != 0)) {
        log_printf(0, "ERROR: File base path is a symlink which is not allowed!!!!!!!\n");
        log_printf(0, "ERROR: base_path=%s with base path for files=%s\n", osf->base_path, pname);
        log_printf(0, "ERROR: realpath(%s) = %s and they should be the same!\n", pname, rpath);
        log_printf(0, "aborting\n");
        fprintf(stderr, "ERROR: File base path is a symlink which is not allowed!!!!!!!\n");
        fprintf(stderr, "ERROR: base_path=%s with base path for files=%s\n", osf->base_path, pname);
        fprintf(stderr, "ERROR: realpath(%s) = %s and they should be the same!\n", pname, rpath);
        fprintf(stderr, "aborting\n");
        tbx_log_flush();
        fflush(stderr);
        abort();
    }

    osf->file_path = strdup(pname);
    osf->file_path_len = strlen(osf->file_path);
    snprintf(pname, OS_PATH_MAX, "%s/%s", osf->base_path, "hardlink");
    osf->hardlink_path = strdup(pname);
    osf->hardlink_path_len = strlen(osf->hardlink_path);

    apr_pool_create(&osf->mpool, NULL);
    tbx_type_malloc_clear(osf->internal_lock, apr_thread_mutex_t *, osf->internal_lock_size);
    for (i=0; i<osf->internal_lock_size; i++) {
        apr_thread_mutex_create(&(osf->internal_lock[i]), APR_THREAD_MUTEX_DEFAULT, osf->mpool);
    }

    osf->os_lock = fobj_lock_create();
    osf->os_lock_user = fobj_lock_create();
    osf->open_fd = tbx_list_create(1, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
    apr_thread_mutex_create(&(osf->open_fd_lock), APR_THREAD_MUTEX_DEFAULT, osf->mpool);

    osf->base_path_len = strlen(osf->base_path);

    //** Get the default host ID for opens
    char hostname[1024];
    apr_gethostname(hostname, sizeof(hostname), osf->mpool);
    osf->host_id = strdup(hostname);

    //** Make and install the virtual attributes
    osf->vattr_hash = apr_hash_make(osf->mpool);
    osf->vattr_prefix = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);

    osf->lock_va.attribute = "os.lock";
    osf->lock_va.priv = os;
    osf->lock_va.get = va_lock_get_attr;
    osf->lock_va.set = va_null_set_attr;
    osf->lock_va.get_link = va_null_get_link_attr;

    osf->lock_user_va.attribute = "os.lock.user";
    osf->lock_user_va.priv = os;
    osf->lock_user_va.get = va_lock_user_get_attr;
    osf->lock_user_va.set = va_null_set_attr;
    osf->lock_user_va.get_link = va_null_get_link_attr;

    osf->realpath_va.attribute = "os.realpath";
    osf->realpath_va.priv = os;
    osf->realpath_va.get = va_realpath_attr;
    osf->realpath_va.set = va_null_set_attr;
    osf->realpath_va.get_link = va_null_get_link_attr;

    osf->link_va.attribute = "os.link";
    osf->link_va.priv = os;
    osf->link_va.get = va_link_get_attr;
    osf->link_va.set = va_null_set_attr;
    osf->link_va.get_link = va_null_get_link_attr;

    osf->link_count_va.attribute = "os.link_count";
    osf->link_count_va.priv = os;
    osf->link_count_va.get = va_link_count_get_attr;
    osf->link_count_va.set = va_null_set_attr;
    osf->link_count_va.get_link = va_null_get_link_attr;

    osf->type_va.attribute = "os.type";
    osf->type_va.priv = os;
    osf->type_va.get = va_type_get_attr;
    osf->type_va.set = va_null_set_attr;
    osf->type_va.get_link = va_null_get_link_attr;

    osf->create_va.attribute = "os.create";
    osf->create_va.priv = os;
    osf->create_va.get = va_create_get_attr;
    osf->create_va.set = va_null_set_attr;
    osf->create_va.get_link = va_null_get_link_attr;

    apr_hash_set(osf->vattr_hash, osf->lock_va.attribute, APR_HASH_KEY_STRING, &(osf->lock_va));
    apr_hash_set(osf->vattr_hash, osf->lock_user_va.attribute, APR_HASH_KEY_STRING, &(osf->lock_user_va));
    apr_hash_set(osf->vattr_hash, osf->realpath_va.attribute, APR_HASH_KEY_STRING, &(osf->realpath_va));
    apr_hash_set(osf->vattr_hash, osf->link_va.attribute, APR_HASH_KEY_STRING, &(osf->link_va));
    apr_hash_set(osf->vattr_hash, osf->link_count_va.attribute, APR_HASH_KEY_STRING, &(osf->link_count_va));
    apr_hash_set(osf->vattr_hash, osf->type_va.attribute, APR_HASH_KEY_STRING, &(osf->type_va));
    apr_hash_set(osf->vattr_hash, osf->create_va.attribute, APR_HASH_KEY_STRING, &(osf->create_va));

    osf->attr_link_pva.attribute = "os.attr_link";
    osf->attr_link_pva.priv = (void *)(long)strlen(osf->attr_link_pva.attribute);
    osf->attr_link_pva.get = va_attr_link_get_attr;
    osf->attr_link_pva.set = va_null_set_attr;
    osf->attr_link_pva.get_link = va_attr_link_get_attr;

    osf->attr_type_pva.attribute = "os.attr_type";
    osf->attr_type_pva.priv = (void *)(long)(strlen(osf->attr_type_pva.attribute));
    osf->attr_type_pva.get = va_attr_type_get_attr;
    osf->attr_type_pva.set = va_null_set_attr;
    osf->attr_type_pva.get_link = va_null_get_link_attr;

    osf->timestamp_pva.attribute = "os.timestamp";
    osf->timestamp_pva.priv = (void *)(long)(strlen(osf->timestamp_pva.attribute));
    osf->timestamp_pva.get = va_timestamp_get_attr;
    osf->timestamp_pva.set = va_timestamp_set_attr;
    osf->timestamp_pva.get_link = va_timestamp_get_link_attr;

    osf->append_pva.attribute = "os.append";
    osf->append_pva.priv = (void *)(long)(strlen(osf->append_pva.attribute));
    osf->append_pva.get = va_append_get_attr;
    osf->append_pva.set = va_append_set_attr;
    osf->append_pva.get_link = va_timestamp_get_link_attr;  //** The timestamp routine just peels off the PVA so can reuse it

    tbx_list_insert(osf->vattr_prefix, osf->attr_link_pva.attribute, &(osf->attr_link_pva));
    tbx_list_insert(osf->vattr_prefix, osf->attr_type_pva.attribute, &(osf->attr_type_pva));
    tbx_list_insert(osf->vattr_prefix, osf->timestamp_pva.attribute, &(osf->timestamp_pva));
    tbx_list_insert(osf->vattr_prefix, osf->append_pva.attribute, &(osf->append_pva));

    os->type = OS_TYPE_FILE;

    os->print_running_config = osfile_print_running_config;
    os->destroy_service = osfile_destroy;
    os->exists = osfile_exists;
    os->realpath = osfile_realpath;
    os->exec_modify = osfile_object_exec_modify;
    os->create_object = osfile_create_object;
    os->remove_object = osfile_remove_object;
    os->remove_regex_object = osfile_remove_regex_object;
    os->abort_remove_regex_object = osfile_abort_remove_regex_object;
    os->move_object = osfile_move_object;
    os->symlink_object = osfile_symlink_object;
    os->hardlink_object = osfile_hardlink_object;
    os->create_object_iter = osfile_create_object_iter;
    os->create_object_iter_alist = osfile_create_object_iter_alist;
    os->next_object = osfile_next_object;
    os->destroy_object_iter = osfile_destroy_object_iter;
    os->open_object = osfile_open_object;
    os->close_object = osfile_close_object;
    os->abort_open_object = osfile_abort_open_object;
    os->lock_user_object = osfile_lock_user_object;
    os->abort_lock_user_object = osfile_abort_lock_user_object;
    os->get_attr = osfile_get_attr;
    os->set_attr = osfile_set_attr;
    os->symlink_attr = osfile_symlink_attr;
    os->copy_attr = osfile_copy_attr;
    os->get_multiple_attrs = osfile_get_multiple_attrs;
    os->set_multiple_attrs = osfile_set_multiple_attrs;
    os->copy_multiple_attrs = osfile_copy_multiple_attrs;
    os->symlink_multiple_attrs = osfile_symlink_multiple_attrs;
    os->move_attr = osfile_move_attr;
    os->move_multiple_attrs = osfile_move_multiple_attrs;
    os->regex_object_set_multiple_attrs = osfile_regex_object_set_multiple_attrs;
    os->abort_regex_object_set_multiple_attrs = osfile_abort_regex_object_set_multiple_attrs;
    os->create_attr_iter = osfile_create_attr_iter;
    os->next_attr = osfile_next_attr;
    os->destroy_attr_iter = osfile_destroy_attr_iter;

    os->create_fsck_iter = osfile_create_fsck_iter;
    os->destroy_fsck_iter = osfile_destroy_fsck_iter;
    os->next_fsck = osfile_next_fsck;
    os->fsck_object = osfile_fsck_object;

    //** Check if everything is copacetic with the root dir
    if (lio_os_local_filetype(osf->base_path) <= 0) {
        log_printf(0, "Base Path doesn't exist!  base_path=%s\n", osf->base_path);
        os_destroy(os);
        os = NULL;
        return(NULL);
    }

    if (lio_os_local_filetype(osf->file_path) <= 0) {
        log_printf(0, "File Path doesn't exist!  file_path=%s\n", osf->file_path);
        os_destroy(os);
        os = NULL;
        return(NULL);
    }

    if (lio_os_local_filetype(osf->hardlink_path) <= 0) {
        log_printf(0, "Hard link Path doesn't exist!  hardlink_path=%s\n", osf->hardlink_path);
        os_destroy(os);
        os = NULL;
        return(NULL);
    }

    snprintf(pname, OS_PATH_MAX, "%s/%s", osf->file_path, FILE_ATTR_PREFIX);
    if (lio_os_local_filetype(pname) <= 0) {  //** Missing attr directory for base so create it
        i = mkdir(pname, DIR_PERMS);
        if (i != 0) {
            log_printf(0, "Base path attributes directory cannot be created! base_path_attr=%s\n", pname);
            os_destroy(os);
            os = NULL;
            return(NULL);
        }
    }

    //** Make sure all the hardlink dirs exist
    for (i=0; i<osf->hardlink_dir_size; i++) {
        snprintf(pname, OS_PATH_MAX, "%s/%d", osf->hardlink_path, i);
        if (lio_os_local_filetype(pname) == 0) {
            err = mkdir(pname, DIR_PERMS);
            if (err != 0) {
                log_printf(0, "Error creating hardlink directory! full=%s\n", pname);
                os_destroy(os);
                os = NULL;
                return(NULL);
            }

            //** Also need to make the attributes directory
            snprintf(pattr, OS_PATH_MAX, "%s/%s", pname, FILE_ATTR_PREFIX);
            err = mkdir(pattr, DIR_PERMS);
            if (err != 0) {
                log_printf(0, "Error creating object attr directory! full=%s\n", pattr);
                os_destroy(os);
                os = NULL;
                return(NULL);
            }
        }
    }

    //** If we have sharding enabled make sure all those directories are there
    if (osf->shard_enable == 1) {
        for (i=0; i<osf->n_shard_prefix; i++) {
            for (j=0; j<osf->shard_splits; j++) {
                snprintf(pname, OS_PATH_MAX, "%s/%d", osf->shard_prefix[i], j);
                if (lio_os_local_filetype(pname) == 0) {
                    err = mkdir(pname, DIR_PERMS);
                    if (err != 0) {
                        log_printf(0, "ERROR creating shard_prefix split directory! full=%s\n", pname);
                        os_destroy(os);
                        os = NULL;
                        return(NULL);
                    }
                }
            }
        }
    }
    //** Make the activity log
    osf->olog = notify_create(fd, NULL, osf->os_activity);

    return(os);
}


//***********************************************************************
//  local_next_object - returns the next local object
//***********************************************************************

int local_next_object(local_object_iter_t *it, char **myfname, int *prefix_len)
{
    int dir_change;

    return(osf_next_object(it->oit, myfname, prefix_len, &dir_change));
}


//***********************************************************************
//  Dummy OSAZ routine for the local iter
//***********************************************************************

int local_osaz_object_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, int mode)
{
    return(2);
}

int local_osaz_object_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path)
{
    return(1);
}

int local_osaz_attr_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key)
{
    return(1);
}

int local_osaz_attr_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key, int mode, osaz_attr_filter_t *filter)
{
    return(2);
}

void local_osaz_ug_hint_init(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

void local_osaz_ug_hint_free(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

void local_osaz_ug_hint_set(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

int local_osaz_ug_hint_get(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return(-1);
}

//***********************************************************************
// create_local_object_iter - Creates a local object iterator
//***********************************************************************

local_object_iter_t *create_local_object_iter(lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth)
{
    local_object_iter_t *it;
    lio_osfile_priv_t *osf;

    tbx_type_malloc_clear(it, local_object_iter_t, 1);

    //** Make a bare bones os_file object
    tbx_type_malloc_clear(it->os, lio_object_service_fn_t, 1);
    tbx_type_malloc_clear(osf, lio_osfile_priv_t, 1);
    tbx_type_malloc_clear(osf->osaz, lio_os_authz_t, 1);
    it->os->priv = (void *)osf;
    osf->file_path = "";
    osf->osaz->object_create = local_osaz_object_create_remove;
    osf->osaz->object_remove = local_osaz_object_create_remove;
    osf->osaz->object_access = local_osaz_object_access;
    osf->osaz->attr_create = local_osaz_attr_create_remove;
    osf->osaz->attr_remove = local_osaz_attr_create_remove;
    osf->osaz->attr_access = local_osaz_attr_access;
    osf->osaz->ug_hint_set = local_osaz_ug_hint_set;
    osf->osaz->ug_hint_get = local_osaz_ug_hint_get;
    osf->osaz->ug_hint_init = local_osaz_ug_hint_init;
    osf->osaz->ug_hint_free = local_osaz_ug_hint_free;

    it->oit = osfile_create_object_iter(it->os, NULL, path, object_regex, object_types, NULL, recurse_depth, NULL, 0);

    return(it);
}

//***********************************************************************
// destroy_local_object_iter -Destroys the loca file iter
//***********************************************************************

void destroy_local_object_iter(local_object_iter_t *it)
{
    lio_osfile_priv_t *osf = (lio_osfile_priv_t *)it->os->priv;

    osfile_destroy_object_iter(it->oit);

    free(osf->osaz);
    free(osf);
    free(it->os);
    free(it);
}
