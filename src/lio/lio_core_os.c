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

#define _log_module_index 189

#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/normalize_path.h>
#include <tbx/skiplist.h>
#include <tbx/type_malloc.h>
#include <tbx/random.h>
#include <unistd.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/types.h"
#include "lio.h"
#include "os.h"

#define COS_DEBUG_NOTIFY(fmt, ...) if (os_notify_handle) _tbx_notify_printf(os_notify_handle, 1, NULL, __func__, __LINE__, fmt, ## __VA_ARGS__)

#define _n_fsck_keys 5
static char *_fsck_keys[] = { "system.owner", "system.inode", "system.exnode", "system.exnode.size", "system.exnode.data" };

#define _n_lio_file_keys 7
#define _n_lio_dir_keys 6
#define _n_lio_create_keys 7

static char *_lio_create_keys[] = { "system.owner", "os.timestamp.system.create", "os.timestamp.system.modify_data",
                                    "os.timestamp.system.modify_attr", "system.inode", "system.exnode", "system.exnode.size"
                                  };

//** NOTE: the _lio_stat_keys it is assumed the system.inode is in the 1st slot
#define _lio_stat_keys_size 7
char *_lio_stat_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count", "os.link" };

typedef struct {
    char *dentry;
    char *fname;
    char *readlink;
    struct stat stat;
} _dentry_t;

struct lio_stat_iter_t {
    lio_config_t *lc;
    lio_creds_t *creds;
    os_object_iter_t *it;
    lio_os_regex_table_t *path_regex;
    char *val[_lio_stat_keys_size];
    int v_size[_lio_stat_keys_size];
    _dentry_t dot;
    _dentry_t dotdot;
    int stat_symlink;
    int prefix_len;
    int state;
};

typedef struct {
    lio_config_t *lc;
    lio_creds_t *creds;
    char *src_path;
    char *dest_path;
    char *id;
    char *ex;
    ex_id_t *inode;
    const char **attr_extra;
    const char **val_extra;
    int *v_size_extra;
    int n_extra;
    int type;
} lio_mk_mv_rm_t;

typedef struct {
    lio_config_t *lc;
    lio_creds_t *creds;
    lio_os_regex_table_t *rpath;
    lio_os_regex_table_t *robj;
    int recurse_depth;
    int obj_types;
    int np;
} lio_remove_regex_t;

typedef struct {
    char *fname;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    int ftype;
} lio_fsck_task_t;

struct lio_fsck_iter_t {
    lio_config_t *lc;
    lio_creds_t *creds;
    char *path;
    lio_os_regex_table_t *regex;
    os_object_iter_t *it;
    int owner_mode;
    int exnode_mode;
    char *owner;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    lio_fsck_task_t *task;
    gop_opque_t *q;
    int n;
    int firsttime;
    ex_off_t visited_count;
};

typedef struct {
    lio_config_t *lc;
    lio_creds_t *creds;
    char *path;
    char **val;
    int *v_size;
    int ftype;
    int full;
    int owner_mode;
    int exnode_mode;
    char *owner;
} lio_fsck_check_t;

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t ino);
int ex_id_compare_fn(void *arg, tbx_sl_key_t *a, tbx_sl_key_t *b);
tbx_sl_compare_t ex_id_compare = {.fn=ex_id_compare_fn, .arg=NULL };

//***********************************************************************
// Core LFS functionality
//***********************************************************************

//***********************************************************************
// lio_get_open_file_size - Returns the size of the file if it is currrently open and -1 otherwise
//***********************************************************************

ex_off_t lio_get_open_file_size(lio_config_t *lc, ex_id_t sid_ino, int do_lock)
{
    lio_file_handle_t *fh;
    ex_off_t n = -1;

    //** Get the handle and flag that we need it for a short period
    if (do_lock) { lio_lock(lc); }
    fh = _lio_get_file_handle(lc, sid_ino);
    if (fh == NULL) {   //** Kick out if nothing to do
        if (do_lock) { lio_unlock(lc); }
        return(-1);
    }
    fh->quick_lock++;
    if (do_lock) { lio_unlock(lc); }

    //** Get the size outside the lock
    n = lio_size_fh(fh);

    //** Decr our quick lock
    if (do_lock) { lio_lock(lc); }
    fh->quick_lock--;
    if (do_lock) { lio_unlock(lc); }

    return(n);
}

//***********************************************************************
// lio_store_stat_size - Stores the stat file size info in the struct
//***********************************************************************

void lio_store_stat_size(struct stat *stat, ex_off_t nbytes)
{
    stat->st_size = nbytes;
    stat->st_blocks = nbytes/512;
    if (stat->st_blocks == 0) stat->st_blocks = 1;
}

//***********************************************************************
// lio_update_stat_open_file_size - Updates the stat size fields for open files
//***********************************************************************

void lio_update_stat_open_file_size(lio_config_t *lc, ex_id_t sid_ino, struct stat *stat, int do_lock)
{
    ex_off_t nbytes;

    nbytes = lio_get_open_file_size(lc, sid_ino, do_lock);
    if (nbytes < 0) return;

    lio_store_stat_size(stat, nbytes);
}

//************************************************************************
//  ex_id_compare_fn  - ID comparison function
//************************************************************************

int ex_id_compare_fn(void *arg, tbx_sl_key_t *a, tbx_sl_key_t *b)
{
    ex_id_t *al = (ex_id_t *)a;
    ex_id_t *bl = (ex_id_t *)b;

    if (*al<*bl) {
        return(-1);
    } else if (*al == *bl) {
        return(0);
    }

    return(1);
}

//***********************************************************************
// lio_exists_gop - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

gop_op_generic_t *lio_exists_gop(lio_config_t *lc, lio_creds_t *creds, char *path)
{
    return(os_exists(lc->os, creds, path));
}

//***********************************************************************
// lio_exists - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

int lio_exists(lio_config_t *lc, lio_creds_t *creds, char *path)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(os_exists(lc->os, creds, path));
    return(status.error_code);
}

//***********************************************************************
// lio_realpath_gop - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

gop_op_generic_t *lio_realpath_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *realpath)
{
    return(os_realpath(lc->os, creds, path, realpath));
}

//***********************************************************************
// lio_realpath - Returns the filetype of the object or 0 if it
//   doesn't exist
//***********************************************************************

int lio_realpath(lio_config_t *lc, lio_creds_t *creds, const char *path, char *realpath)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(os_realpath(lc->os, creds, path, realpath));
    return((status.op_status == OP_STATE_SUCCESS) ? 0 : -1);
}

//***********************************************************************
// lio_free_mk_mv_rm
//***********************************************************************

void lio_free_mk_mv_rm(void *arg)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;

    if (op->src_path != NULL) free(op->src_path);
    if (op->dest_path != NULL) free(op->dest_path);
    if (op->id != NULL) free(op->id);
    if (op->ex != NULL) free(op->ex);

    free(op);
}

//***********************************************************************
// lio_update_parent - Updates the parent directories modify timestamp
//***********************************************************************

void lio_update_parent(lio_config_t *lc, lio_creds_t *creds, char *id, const char *fname)
{
    char *parent, *file;

    //** Kick out if no fname or we are at the root directory
    if ((fname == NULL) || (fname[1] == '\0')) return;

    //** Get the parent.
    lio_os_path_split(fname, &parent, &file);

    //** Do the update
    lio_setattr(lc, creds, parent, id, "os.timestamp.system.modify_data", NULL, 0);

    if (parent) free(parent);
    if (file) free(file);
}

//***********************************************************************
// lio_create_object_fn - Does the actual object creation
//***********************************************************************

gop_op_status_t lio_create_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    os_fd_t *fd;
    char *dir, *fname;
    lio_exnode_exchange_t *exp;
    lio_exnode_t *ex, *cex;
    ex_id_t ino;
    char inode[32];
    char *val[_n_lio_create_keys + op->n_extra];
    char *attrs[_n_lio_create_keys + op->n_extra];
    char **create_keys;
    gop_op_status_t status, status2;
    int v_size[_n_lio_create_keys + op->n_extra];
    int ll, i, n;
    int ex_key = 5;

    status = gop_success_status;

    ll = 0;
    val[ex_key] = NULL;

    log_printf(15, "START op->ex=%p !!!!!!!!!\n fname=%s\n",  op->ex, op->src_path);

    //** Sanity check the type is supported
    if (op->type & OS_OBJECT_UNSUPPORTED_FLAG) {
        log_printf(ll, "ERROR: Unsupported object type! ftype=%d fname=%s\n", op->type, op->src_path);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - Unsupported object type! ftype=%d fname=%s\n", op->type, op->src_path);
        _op_set_status(status, OP_STATE_FAILURE, EPERM);
        goto fail;
    }

    //** Get the parent exnode to dup
    if (op->ex == NULL) {
        lio_os_path_split(op->src_path, &dir, &fname);
        log_printf(15, "dir=%s\n fname=%s\n", dir, fname);
        free(fname);

        status = gop_sync_exec_status(os_open_object(op->lc->os, op->creds, dir, OS_MODE_READ_IMMEDIATE, op->id, &fd, op->lc->timeout));
        if (status.op_status != OP_STATE_SUCCESS) {
            log_printf(ll, "ERROR: opening parent=%s\n", dir);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - opening parent=%s error_code=%d\n", dir, status.error_code);
            free(dir);
            goto fail;
        }

        v_size[0] = -op->lc->max_attr;
        status = gop_sync_exec_status(os_get_attr(op->lc->os, op->creds, fd, "system.exnode", (void **)&(val[ex_key]), &(v_size[0])));
        if (status.op_status != OP_STATE_SUCCESS) {
            log_printf(ll, "ERROR: getting parent exnode parent=%s\n", dir);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn  - getting parent exnode parent=%s error_code=%d\n", dir, status.error_code);
        }

        //** Close the parent
        status2 = gop_sync_exec_status(os_close_object(op->lc->os, fd));
        if (status2.op_status != OP_STATE_SUCCESS) {
            log_printf(ll, "ERROR: closing parent fname=%s\n", dir);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - closing parent fname=%s error_code=%d\n", dir, status2.error_code);
        }

        free(dir);

        //** See if we need to kick out
        if (status.op_status != OP_STATE_SUCCESS) {
            goto fail;
        } else if (status2.op_status != OP_STATE_SUCCESS) {
            status = status2;   //** Return our error code
            goto fail;
        }
    } else {
        val[ex_key] = op->ex;
    }

    if (val[ex_key] == NULL) { //** Oops no valid exnode!
        log_printf(0, "ERROR: No valid exnode could be located.  fname=%s\n", op->src_path);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - No valid exnode could be located.  fname=%s\n", op->src_path);
        _op_set_status(status, OP_STATE_FAILURE, EBADE);
        goto fail;
    }

    //** For a directory we can just copy the exnode.  For a file we have to
    //** Clone it to get unique IDs
    if ((op->type & OS_OBJECT_DIR_FLAG) == 0) {
        //** If this has a caching segment we need to disable it from being added
        //** to the global cache table cause there could be multiple copies of the
        //** same segment being serialized/deserialized.

        //** Deserialize it
        exp = lio_exnode_exchange_text_parse(val[ex_key]);
        ex = lio_exnode_create();
        if (lio_exnode_deserialize(ex, exp, op->lc->ess_nocache) != 0) {
            log_printf(ll, "ERROR: parsing parent exnode src_path=%s\n", op->src_path);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - parsing parent exnode src_path=%s\n", op->src_path);
            _op_set_status(status, OP_STATE_FAILURE, EBADE);
            val[ex_key] = NULL;
            lio_exnode_exchange_destroy(exp);
            lio_exnode_destroy(ex);
            goto fail;
        }

        //** Execute the clone operation
        status = gop_sync_exec_status(lio_exnode_clone_gop(op->lc->tpc_unlimited, ex, op->lc->da, &cex, NULL, CLONE_STRUCTURE, op->lc->timeout));
        if (status.op_status != OP_STATE_SUCCESS) {
            log_printf(ll, "ERROR: cloning parent src_path=%s\n", op->src_path);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - cloning parent src_path=%s\n", op->src_path);
            if (status.error_code == 0) status.error_code = EFAULT;
            status = gop_failure_status;
            val[ex_key] = NULL;
            lio_exnode_exchange_destroy(exp);
            lio_exnode_destroy(ex);
            lio_exnode_destroy(cex);
            goto fail;
        }

        //** Serialize it for storage
        exnode_exchange_free(exp);
        lio_exnode_serialize(cex, exp);
        val[ex_key] = exp->text.text;
        exp->text.text = NULL;
        lio_exnode_exchange_destroy(exp);
        lio_exnode_destroy(ex);
        lio_exnode_destroy(cex);
    }


    //** Now add the required attributes
    val[0] = an_cred_get_id(op->creds, &v_size[0]);
    if (op->id) {
        val[1] = op->id;
        v_size[1] = strlen(op->id);
    } else {
        val[1] = op->lc->host_id;
        v_size[1] = op->lc->host_id_len;
    }
    val[2] = val[1];
    v_size[2] = v_size[1];
    val[3] = val[1];
    v_size[3] = v_size[1];
    ino = 0;
    generate_ex_id(&ino);
    snprintf(inode, 32, XIDT, ino);
    if (op->inode) *op->inode = ino;
    val[4] = inode;
    v_size[4] = strlen(inode);
    v_size[ex_key] = strlen(val[ex_key]);
    val[6] = "0";
    v_size[6] = 1;

    log_printf(15, "NEW ino=%s exnode=%s\n", val[4], val[ex_key]);
    tbx_log_flush();

    //** Make the object with attrs
    n = (op->type & OS_OBJECT_FILE_FLAG) ? _n_lio_file_keys : _n_lio_dir_keys;
    create_keys = _lio_create_keys;
    if (op->n_extra) {
        create_keys = attrs;
        for (i=0; i<n; i++) {
            attrs[i] = _lio_create_keys[i];
        }
        for (i=0; i<op->n_extra; i++) {
            attrs[i + n] = (char *)op->attr_extra[i];
            val[i+n] = (char *)op->val_extra[i];
            v_size[i+n] = op->v_size_extra[i];
        }
        n = n + op->n_extra;
    }

    status = gop_sync_exec_status(os_create_object_with_attrs(op->lc->os, op->creds, op->src_path, op->type, op->id, create_keys, (void **)val, v_size, n));
    if (status.op_status != OP_STATE_SUCCESS) {
        log_printf(ll, "ERROR: creating object fname=%s errno=%d\n", op->src_path, status.error_code);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_create_object_fn - creating object fname=%s errno=%d\n", op->src_path, status.error_code);
    } else if (op->lc->update_parent) {
        lio_update_parent(op->lc, op->creds, op->id, op->src_path);
    }

fail:
    notify_printf(op->lc->notify, 1, op->creds, "LIO_CREATE_OBJECT_FN: fname=%s ftype=%d  status=%d error_code=%d\n", op->src_path, op->type, status.op_status, status.error_code);

    if (val[ex_key] != NULL) free(val[ex_key]);

    return(status);
}

//*************************************************************************
//  lio_create_inode_gop - Generate a create object task and also returns the inode number
//*************************************************************************

gop_op_generic_t *lio_create_inode_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int type, char *ex, char *id, ex_id_t *inode, const char **attr_extra, const char **val_extra, int *v_size_extra, int n_extra)
{
    lio_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = type;
    op->inode = inode;
    op->id = (id != NULL) ? strdup(id) : NULL;
    op->ex = (ex != NULL) ? strdup(ex) : NULL;
    op->attr_extra = attr_extra;
    op->val_extra = val_extra;
    op->v_size_extra = v_size_extra;
    op->n_extra = n_extra;
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_create_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}

//*************************************************************************
//  lio_create_gop - Generate a create object task
//*************************************************************************

gop_op_generic_t *lio_create_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int type, char *ex, char *id, const char **attr_extra, const char **val_extra, int *v_size_extra, int n_extra)
{
    lio_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = type;
    op->id = (id != NULL) ? strdup(id) : NULL;
    op->ex = (ex != NULL) ? strdup(ex) : NULL;
    op->attr_extra = attr_extra;
    op->val_extra = val_extra;
    op->v_size_extra = v_size_extra;
    op->n_extra = n_extra;
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_create_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}


//***********************************************************************
// lio_mkpath_fn - Does the actual path creation
//***********************************************************************

gop_op_status_t lio_mkpath_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    gop_op_status_t status;
    regex_t *np_regex;
    char fullpath[OS_PATH_MAX];
    char *curr, *next;
    int exists;

    //** See if we get lucky and all the intervening directories already exist
    status = gop_sync_exec_status(lio_create_gop(op->lc, op->creds, op->src_path, op->type, op->id, op->ex, op->attr_extra, op->val_extra, op->v_size_extra, op->n_extra));
    if (status.op_status == OP_STATE_SUCCESS) return(status);  //** Got lucky so kick out

    //** Ok we have to normalize the path and recurse down making the intermediate directories
    np_regex = tbx_normalize_check_make();  //** Make the regex for seeing if we need to simplify the path
    if (tbx_path_is_normalized(np_regex, op->src_path) == 0) { //** Got to normalize it first
        if (tbx_normalize_path(op->src_path, fullpath) == NULL) {
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_mkpath_fn - tbx_normalize_path error! path=%s\n", op->src_path);
            status = gop_failure_status;
            goto finished;
        }
    } else {
        strncpy(fullpath, op->src_path, sizeof(fullpath)-1); fullpath[sizeof(fullpath)-1] = 0;
    }

    //** Cycle through checking if the intermediate directories exists and creating if needed
    curr = fullpath;
    exists = 1;
    while ((next = index(curr + 1, '/')) != NULL) {
        if (next[0] == '\0') { //** Got the terminal so use the provided object type
            status = gop_sync_exec_status(lio_create_gop(op->lc, op->creds, fullpath, op->type, op->id, op->ex, op->attr_extra, op->val_extra, op->v_size_extra, op->n_extra));
            goto finished;
        } else {  //** Intermediate directory
            next[0] = '\0';  //** NULL terminate the string
            if (exists) {  //** See if the current part exists. Skip it if already in the "new" bits
                exists = lio_exists(op->lc, op->creds, fullpath);
            }

            if (exists) {
                status = gop_sync_exec_status(lio_create_gop(op->lc, op->creds, fullpath, OS_OBJECT_DIR_FLAG, op->id, op->ex, op->attr_extra, op->val_extra, op->v_size_extra, op->n_extra));
                if (status.op_status != OP_STATE_SUCCESS) {
                    log_printf(1, "ERROR: src_path=%s failed creating partial path=%s\n", op->src_path, fullpath);
                    notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_mkpath_fn - src_path=%s failed creating partial path=%s\n", op->src_path, fullpath);
                    goto finished;
                }
            }
            next[0] = '/';  //** Put back the '/'
        }

        curr = next;
    }

finished:
    tbx_normalize_check_destroy(np_regex);
    return(status);
}

//*************************************************************************
//  lio_mkpath_gop - Generate a make path task.
//*************************************************************************

gop_op_generic_t *lio_mkpath_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int type, char *ex, char *id, const char **attr_extra, const char **val_extra, int *v_size_extra, int n_extra)
{
    lio_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->type = type;
    op->id = (id != NULL) ? strdup(id) : NULL;
    op->ex = (ex != NULL) ? strdup(ex) : NULL;
    op->attr_extra = attr_extra;
    op->val_extra = val_extra;
    op->v_size_extra = v_size_extra;
    op->n_extra = n_extra;
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_mkpath_fn, (void *)op, lio_free_mk_mv_rm, 1));
}

//***********************************************************************
// lio_remove_object - Removes an object
//***********************************************************************

gop_op_status_t lio_remove_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    char *ex_data, *val[2], *inode;
    char *hkeys[] = { "os.link_count", "system.exnode" };
    char sfname[OS_PATH_MAX];
    lio_exnode_exchange_t *exp;
    lio_exnode_t *ex;
    int err, v_size, ex_remove, vs[2], n;
    gop_op_status_t status = gop_success_status;

    COS_DEBUG_NOTIFY("LIO_REMOVE_OBJECT_FN: fname=%s ftype=%d START\n", op->src_path, op->type);

    //** First remove and data associated with the object
    v_size = -op->lc->max_attr;

    //** If no object type need to retrieve it
    if (op->type == 0) op->type = lio_exists(op->lc, op->creds, op->src_path);

    ex_remove = 0;
    if ((op->type & OS_OBJECT_HARDLINK_FLAG) > 0) { //** Got a hard link so check if we do a data removal
        val[0] = val[1] = NULL;
        vs[0] = vs[1] = -op->lc->max_attr;
        lio_get_multiple_attrs(op->lc, op->creds, op->src_path, op->id, hkeys, (void **)val, vs, 2, 1);

        if (val[0] == NULL) {
            log_printf(15, "ERROR: Missing link count for fname=%s\n", op->src_path);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: Missing link count for fname=%s\n", op->src_path);
            if (val[1] != NULL) free(val[1]);
            return(gop_failure_status);
        }

        n = 100;
        sscanf(val[0], "%d", &n);
        free(val[0]);
        if (n <= 1) {
            ex_remove = 1;
            if (op->ex == NULL) {
                op->ex = val[1];
            } else {
                if (val[1] != NULL) free(val[1]);
            }
        } else {
            if (val[1] != NULL) free(val[1]);
        }
    } else if ((op->type & (OS_OBJECT_SYMLINK_FLAG|OS_OBJECT_DIR_FLAG)) == 0) {
        ex_remove = 1;
    }

    ex_data = op->ex;
    if ((op->ex == NULL) && (ex_remove == 1)) {
        lio_getattr(op->lc, op->creds, op->src_path, op->id, "system.exnode", (void **)&ex_data, &v_size);
    }

    //** See if it's a special file and if so remove the local entry if it exists
    if ((ex_remove == 1) && (op->type & (OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG))) {
        inode = NULL;
        v_size = -100;
        lio_getattr(op->lc, op->creds, op->src_path, op->id, "system.inode", (void **)&inode, &v_size);
        if (inode) {
            snprintf(sfname, OS_PATH_MAX, "%s%s", op->lc->special_file_prefix, inode);
            remove(sfname);
            free(inode);
        }
    }

    //** Remove the OS entry first.  This way if it fails we'll just kick out and the data is still good.
    err = gop_sync_exec(os_remove_object(op->lc->os, op->creds, op->src_path));
    if (err != OP_STATE_SUCCESS) {
        if ((op->ex == NULL) && (ex_data != NULL)) free(ex_data);  //** Fee the exnode we just fetched if needed
        log_printf(0, "ERROR: removing file: %s err=%d\n", op->src_path, err);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: removing file: %s err=%d\n", op->src_path, err);
        status = gop_failure_status;
        return(status);
    }


    //** Load the exnode and remove it if needed.
    //** Only done for normal files.  No links or dirs
    //** We also ignore any errors since nothing can be done becasue the OS entry is gone.
    if ((ex_remove == 1) && (ex_data != NULL)) {
        //** Deserialize it
        exp = lio_exnode_exchange_text_parse(ex_data);
        ex = lio_exnode_create();
        if (lio_exnode_deserialize(ex, exp, op->lc->ess) != 0) {
            log_printf(15, "ERROR: Failed deserializing the exnode for for removal! object fname=%s\n", op->src_path);
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: Failed deserializing the exnode for for removal! object fname=%s\n", op->src_path);
        } else {  //** Execute the remove operation since we have a good exnode
            err = gop_sync_exec(exnode_remove_gop(op->lc->tpc_unlimited, ex, op->lc->da, op->lc->timeout));
            if (err != OP_STATE_SUCCESS) {
                log_printf(15, "ERROR: removing data for object fname=%s\n", op->src_path);
                notify_printf(op->lc->notify, 1, op->creds, "SOFT_ERROR: removing data for object fname=%s. Probably a down RID/depot\n", op->src_path);
            }
        }

        //** Clean up
        if (op->ex != NULL) exp->text.text = NULL;  //** The inital exnode is free()-ed by the TP op
        lio_exnode_exchange_destroy(exp);
        lio_exnode_destroy(ex);
    }

    if (op->lc->update_parent)  lio_update_parent(op->lc, op->creds, op->id, op->src_path);

    notify_printf(op->lc->notify, 1, op->creds, "LIO_REMOVE_OBJECT_FN: fname=%s ftype=%d status=%d error_code=%d\n", op->src_path, op->type, status.op_status, status.error_code);

    return(status);
}

//*************************************************************************
// lio_remove_gop
//*************************************************************************

gop_op_generic_t *lio_remove_gop(lio_config_t *lc, lio_creds_t *creds, char *path, char *ex_optional, int ftype_optional)
{
    lio_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(path);
    op->ex = ex_optional;
    op->type = ftype_optional;
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_remove_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}

//***********************************************************************
// lio_remove_regex_object - Removes objects using regex's
//***********************************************************************

gop_op_status_t lio_remove_regex_object_fn(void *arg, int id)
{
    lio_remove_regex_t *op = (lio_remove_regex_t *)arg;
    os_object_iter_t *it;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    int n, nfailed, atype, prefix_len;
    char *ex, *fname;
    char *key[1];
    int v_size[1];
    gop_op_status_t status2;
    gop_op_status_t status = gop_success_status;

    key[0] = "system.exnode";
    ex = NULL;
    v_size[0] = -op->lc->max_attr;
    it = os_create_object_iter_alist(op->lc->os, op->creds, op->rpath, op->robj, op->obj_types, op->recurse_depth, key, (void **)&ex, v_size, 1);
    if (it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation\n");
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_remove_regex_object_fn - Failed with object_iter creation. rpath=%s\n", op->rpath);
        return(gop_failure_status);
    }

    //** Cycle through removing the objects
    q = gop_opque_new();
    n = 0;
    nfailed = 0;
    while ((atype = os_next_object(op->lc->os, it, &fname, &prefix_len)) > 0) {

        //** If it's a directory so we need to flush all existing rm's first
        //** Otherwire the rmdir will see pending files
        if ((atype & OS_OBJECT_DIR_FLAG) > 0) {
            opque_waitall(q);
        }

        gop = lio_remove_gop(op->lc, op->creds, fname, ex, atype);
        ex = NULL;  //** Freed in lio_remove_object
        free(fname);
        gop_opque_add(q, gop);

        if (gop_opque_tasks_left(q) > op->np) {
            gop = opque_waitany(q);
            status2 = gop_get_status(gop);
            if (status2.op_status != OP_STATE_SUCCESS) {
                log_printf(1, "ERROR: Failed with gid=%d\n", gop_id(gop));
                notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_remove_regex_fn - Failed with gid=%d\n", gop_id(gop));
                nfailed++;
            }
            gop_free(gop, OP_DESTROY);
        }

        n++;
    }

    os_destroy_object_iter(op->lc->os, it);

    opque_waitall(q);
    gop_opque_free(q, OP_DESTROY);

    status.op_status = (nfailed > 0) ? OP_STATE_FAILURE : OP_STATE_SUCCESS;
    status.error_code = n;
    return(status);
}

//*************************************************************************
// lio_remove_regex_gop
//*************************************************************************

gop_op_generic_t *lio_remove_regex_gop(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *rpath, lio_os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np)
{
    lio_remove_regex_t *op;

    tbx_type_malloc_clear(op, lio_remove_regex_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->rpath = rpath;
    op->robj = object_regex;
    op->obj_types = obj_types;
    op->recurse_depth = recurse_depth;
    op->np = np;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_remove_regex_object_fn, (void *)op, free, 1));
}

//*************************************************************************
// lio_regex_object_set_multiple_attrs_gop - Sets multiple object attributes
//*************************************************************************

gop_op_generic_t *lio_regex_object_set_multiple_attrs_gop(lio_config_t *lc, lio_creds_t *creds, char *id, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n)
{
    return(os_regex_object_set_multiple_attrs(lc->os, creds, id, path, object_regex, object_types, recurse_depth, key, val, v_size, n));
}

//*************************************************************************
// lio_abort_regex_object_set_multiple_attrs_gop - Aborts an ongoing set attr call
//*************************************************************************

gop_op_generic_t *lio_abort_regex_object_set_multiple_attrs_gop(lio_config_t *lc, gop_op_generic_t *gop)
{
    return(os_abort_regex_object_set_multiple_attrs(lc->os, gop));
}

//*************************************************************************
//  lio_dir_empty - Returns 0 if the directory is empty and a non-0 otherwise
//*************************************************************************

int lio_dir_empty(lio_config_t *lc, lio_creds_t *creds, char *path)
{
    lio_os_regex_table_t *rp;
    os_object_iter_t *it;
    char *p2;
    int err, prefix_len;
    char *fname;
    int n = strlen(path);
    int obj_types = OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG|OS_OBJECT_SYMLINK_FLAG;

    tbx_type_malloc(p2, char, n+3);
    snprintf(p2, n+3, "%s/*", path);
    rp = lio_os_path_glob2regex(p2);

    it = lio_create_object_iter(lc, creds, rp, NULL, obj_types, NULL, 0, NULL, 0);
    if (it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation\n");
        err = -1;
        goto fail;
    }

    err = lio_next_object(lc, it, &fname, &prefix_len);
    if (err != 0) free(fname);
    lio_destroy_object_iter(lc, it);

fail:
    free(p2);

    log_printf(5, "err=%d\n", err);
    return(err);
}

//*************************************************************************
// lio_move_object_fn - Renames an object.  Does the actual move operation
//*************************************************************************

gop_op_status_t lio_move_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    lio_mk_mv_rm_t rm;
    char *dtmp = NULL;
    gop_op_status_t status, s2;
    int stype, dtype, n;
    unsigned int ui;

    COS_DEBUG_NOTIFY("LIO_MOVE_OBJECT_FN: src=%s dest=%s START\n", op->src_path, op->dest_path);

    stype = lio_exists(op->lc, op->creds, op->src_path);

    //** Check if the dest exists. If it does we need to move it out of the
    //** way for inode and data removal later
    dtype = lio_exists(op->lc, op->creds, op->dest_path);

    log_printf(15, "src=%s dest=%s stype=%d dtype=%d\n", op->src_path, op->dest_path, stype, dtype);
    if (dtype != 0) {  //** The destination exists to lets make sure its compatible with a move
        if (dtype & OS_OBJECT_DIR_FLAG) { //** It's a directory
            if ((stype & OS_OBJECT_DIR_FLAG) == 0) {  //** Source is a file so mv will fail
               status.op_status = OP_STATE_FAILURE; status.error_code = EISDIR;
               return(status);
            } else { // ** Make sure the dest directory is empty
                if (lio_dir_empty(op->lc, op->creds, op->dest_path) != 0) {
                   status.op_status = OP_STATE_FAILURE; status.error_code = ENOTEMPTY;
                   return(status);
                }
            }
        }

        //** Now move it out of the way in case we fail
        n = strlen(op->dest_path);
        tbx_type_malloc(dtmp, char, n + 100);
        dtmp[n+99] = '\0';
        tbx_random_get_bytes(&ui, sizeof(ui));  //** Make the random name
        snprintf(dtmp, n+100, "%s.mv-%u", op->dest_path, ui);
        status = gop_sync_exec_status(os_move_object(op->lc->os, op->creds, op->dest_path, dtmp));
        notify_printf(op->lc->notify, 1, op->creds, "LIO_MOVE_OBJECT_FN: temp move -- interim=%s interim_new=%s  status=%d error_code=%d\n", op->dest_path, dtmp, status.op_status, status.error_code);

        if (status.op_status != OP_STATE_SUCCESS) {  //** Temp move failed so kick out
            notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_move_object_fn - Esiting object in dest and temp move failed. old=%s temp=%s\n", op->dest_path, dtmp);
            free(dtmp);
            return(status);
       }
    }

    //** If we made it here the dest file or directory is safely stashed so we can do the rename
    status = gop_sync_exec_status(os_move_object(op->lc->os, op->creds, op->src_path, op->dest_path));

    notify_printf(op->lc->notify, 1, op->creds, "LIO_MOVE_OBJECT_FN: old=%s new=%s status=%d error_code=%d\n", op->src_path, op->dest_path, status.op_status, status.error_code);

    if ((status.op_status == OP_STATE_SUCCESS) && (op->lc->update_parent)){
         lio_update_parent(op->lc, op->creds, op->id, op->src_path);
         lio_update_parent(op->lc, op->creds, op->id, op->dest_path);
    }

    //** Now clean up
    if (dtmp) {  //** If this exists we had to move an object out of the way
        if (status.op_status == OP_STATE_SUCCESS) {  //** All is good so just remove the original
            rm = *op; rm.src_path = dtmp;
            status = lio_remove_object_fn(&rm, id);
        } else {  //** A problem occurred so move the object back
            s2 = gop_sync_exec_status(os_move_object(op->lc->os, op->creds, dtmp, op->dest_path));
            if (s2.op_status != OP_STATE_SUCCESS) {  //** Temp move failed so kick out
                log_printf(0, "ERROR: Failed to move file back!!! dtmp=%s dest_path=%s error_code=%d\n", dtmp, op->dest_path, s2.error_code);
                notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_move_object_fn - Failed to move file back!!! dtmp=%s dest_path=%s error_code=%d\n", dtmp, op->dest_path, s2.error_code);
            }
        }
    }

    if (dtmp) free(dtmp);

    return(status);
}


//*************************************************************************
// lio_move_object_gop - Renames an object
//*************************************************************************

gop_op_generic_t *lio_move_object_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path)
{
    lio_mk_mv_rm_t *op = op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_move_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}


//***********************************************************************
// lio_link_object_fn - Does the actual object creation
//***********************************************************************

gop_op_status_t lio_link_object_fn(void *arg, int id)
{
    lio_mk_mv_rm_t *op = (lio_mk_mv_rm_t *)arg;
    os_fd_t *dfd;
    gop_opque_t *q;
    int err, i;
    ex_id_t ino;
    char inode[32];
    gop_op_status_t status;
    char *lkeys[] = {"system.exnode", "system.exnode.size", "system.exnode.data", "system.write_errors", "system.soft_errors", "system.hard_errors"};
    char *spath[6];
    char *vkeys[] = {"system.owner", "system.inode", "os.timestamp.system.create", "os.timestamp.system.modify_data", "os.timestamp.system.modify_attr"};
    char *val[5];
    int vsize[5];

    //** Link the base object
    if (op->type == 1) { //** Symlink
        err = gop_sync_exec(os_symlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
    } else {
        err = gop_sync_exec(os_hardlink_object(op->lc->os, op->creds, op->src_path, op->dest_path, op->id));
    }
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR: linking base object sfname=%s dfname=%s\n", op->src_path, op->dest_path);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_link_object_fn - linking base object sfname=%s dfname=%s\n", op->src_path, op->dest_path);
        status = gop_failure_status;
        goto finished;
    }

    if (op->lc->update_parent)  lio_update_parent(op->lc, op->creds, op->id, op->dest_path);

    if (op->type == 0) {  //** Hard link so exit
        status = gop_success_status;
        goto finished;
    }

    q = gop_opque_new();

    //** Open the Destination object
    gop_opque_add(q, os_open_object(op->lc->os, op->creds, op->dest_path, OS_MODE_READ_IMMEDIATE, op->id, &dfd, op->lc->timeout));
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR: opening src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_link_object_fn - opening src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        status = gop_failure_status;
        goto open_fail;
    }

    //** Now link the exnode and size
    for (i=0; i<6; i++) spath[i] = op->src_path;
    gop_opque_add(q, os_symlink_multiple_attrs(op->lc->os, op->creds, spath, lkeys, dfd, lkeys, 5));

    //** Store the owner, inode, and dates
    val[0] = an_cred_get_id(op->creds, &vsize[0]);
    ino = 0;
    generate_ex_id(&ino);
    snprintf(inode, 32, XIDT, ino);
    val[1] = inode;
    vsize[1] = strlen(inode);
    val[2] = op->id;
    vsize[2] = (op->id == NULL) ? 0 : strlen(op->id);
    val[3] = op->id;
    vsize[3] = vsize[2];
    val[4] = op->id;
    vsize[4] = vsize[2];
    gop_opque_add(q, os_set_multiple_attrs(op->lc->os, op->creds, dfd, vkeys, (void **)val, vsize, 5));


    //** Wait for everything to complete
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR: with attr link or owner set src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: with attr link or owner set src(%s) or dest(%s) file\n", op->src_path, op->dest_path);
        status = gop_failure_status;
        goto open_fail;
    }

    status = gop_success_status;

open_fail:
    if (dfd != NULL) gop_opque_add(q, os_close_object(op->lc->os, dfd));
    opque_waitall(q);

    gop_opque_free(q, OP_DESTROY);

finished:
    notify_printf(op->lc->notify, 1, op->creds, "LIO_LINK_OBJECT_FN: src=%s dest=%s  symlink=%d  status=%d error_code=%d\n", op->src_path, op->dest_path, op->type, status.op_status, status.error_code);

    return(status);

}

//***********************************************************************
// lio_link_gop - Generates a link object task
//***********************************************************************

gop_op_generic_t *lio_link_gop(lio_config_t *lc, lio_creds_t *creds, int symlink, char *src_path, char *dest_path, char *id)
{
    lio_mk_mv_rm_t *op;

    tbx_type_malloc_clear(op, lio_mk_mv_rm_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->type = symlink;
    op->src_path = strdup(src_path);
    op->dest_path = strdup(dest_path);
    op->id = (id != NULL) ? strdup(id) : NULL;
    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_link_object_fn, (void *)op, lio_free_mk_mv_rm, 1));
}


//*************************************************************************
//  lio_symlink_object_gop - Create a symbolic link to another object
//*************************************************************************

gop_op_generic_t *lio_symlink_object_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    return(lio_link_gop(lc, creds, 1, src_path, dest_path, id));
}


//*************************************************************************
//  lio_hardlink_gop - Create a hard link to another object
//*************************************************************************

gop_op_generic_t *lio_hardlink_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    return(lio_link_gop(lc, creds, 1, src_path, dest_path, id));
}



//*************************************************************************
// lio_create_object_iter - Creates an object iterator using a regex for the attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *obj_regex, int object_types, lio_os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max)
{
    return(os_create_object_iter(lc->os, creds, path, obj_regex, object_types, attr, recurse_dpeth, it, v_max));
}

//*************************************************************************
// lio_create_object_iter_alist - Creates an object iterator using a fixed attribute list
//*************************************************************************

os_object_iter_t *lio_create_object_iter_alist(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    return(os_create_object_iter_alist(lc->os, creds, path, obj_regex, object_types, recurse_depth, key, val, v_size, n_keys));
}


//*************************************************************************
// lio_next_object - Returns the next iterator object
//*************************************************************************

int lio_next_object(lio_config_t *lc, os_object_iter_t *it, char **fname, int *prefix_len)
{
    return(os_next_object(lc->os, it, fname, prefix_len));
}


//*************************************************************************
// lio_destroy_object_iter - Destroy's an object iterator
//*************************************************************************

void lio_destroy_object_iter(lio_config_t *lc, os_object_iter_t *it)
{
    os_destroy_object_iter(lc->os, it);
}


//***********************************************************************
// lio_*_attrs - Get/Set LIO attribute routines
//***********************************************************************

typedef struct {
    lio_config_t *lc;
    lio_creds_t *creds;
    const char *path;
    os_fd_t *fd;
    char *id;
    char **mkeys;
    void **mvals;
    int *mv_size;
    char *skey;
    void *sval;
    int *sv_size;
    int n_keys;
    int no_cache_attrs_if_file;
} lio_attrs_op_t;

//***********************************************************************
// lio_get_multiple_attrs_fd
//***********************************************************************

int lio_get_multiple_attrs_fd(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n_keys)
{
    int serr;

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    serr = gop_sync_exec(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR getting attributes\n");
        notify_printf(lc->notify, 1, creds, "ERROR: lio_get_multiple_Attrs_fd - getting attributes\n");
    }

    return(serr);
}

//***********************************************************************

gop_op_generic_t *lio_get_multiple_attrs_fd_gop(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n_keys)
{
    return(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));
}

//***********************************************************************
// lio_get_multiple_attrs
//***********************************************************************

int lio_get_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys, int no_cache_attrs_if_file)
{
    int err;

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    err = gop_sync_exec(os_get_multiple_attrs_immediate(lc->os, creds, (char *)path, key, val, v_size, n_keys));

    return(err);
}

//***********************************************************************
// lio_get_multiple_attrs_lock
//***********************************************************************

int lio_get_multiple_attrs_lock(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys, int lock_flags)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE|lock_flags, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: opening object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: lio_get_multiple_attrs_lock - opening object=%s\n", path);
        return(err);
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    serr = gop_sync_exec(os_get_multiple_attrs(lc->os, creds, fd, key, val, v_size, n_keys));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: closing object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: getting attributes object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: getting attributes object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_get_multiple_attrs_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;
    int err;

    err = lio_get_multiple_attrs(op->lc, op->creds, (char *)op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys, op->no_cache_attrs_if_file);
    status.error_code = err;
    status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_get_multiple_attrs_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys, int no_cache_attrs_if_file)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->mkeys = key;
    op->mvals = val;
    op->mv_size = v_size;
    op->n_keys = n_keys;
    op->no_cache_attrs_if_file = no_cache_attrs_if_file;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_get_multiple_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_getattr_fd - Returns an attribute from the object FD
//***********************************************************************

int lio_getattr_fd(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
    int err;

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    err = gop_sync_exec(os_get_attr(lc->os, creds, fd, key, val, v_size));

    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: getting attribute!\n");
        notify_printf(lc->notify, 1, creds, "ERROR: lio_getattr_fd - getting attribute!\n");
    }

    return(err);
}


//***********************************************************************
// lio_getattr_fd_gop - GOP version for fetching an attribute from the object FD
//***********************************************************************

gop_op_generic_t *lio_getattr_fd_gop(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
    return(os_get_attr(lc->os, creds, fd, key, val, v_size));
}


//***********************************************************************
// lio_getattr - Returns an attribute
//***********************************************************************

int lio_getattr(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: opening object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: opening object=%s\n", path);
        return(err);
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    serr = gop_sync_exec(os_get_attr(lc->os, creds, fd, key, val, v_size));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: closing object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: getting attribute object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: getting attribute object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_getattr_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;
    int err;

    err = lio_getattr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->sv_size);
    status.error_code = 0;
    status.op_status = err;
    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_getattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->skey = key;
    op->sval = val;
    op->sv_size = v_size;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_getattr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_multiple_setattr_op_real - Returns an attribute
//***********************************************************************

int lio_multiple_setattr_op_real(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
    int err;

    err = gop_sync_exec(os_set_multiple_attrs_immediate(lc->os, creds, (char *)path, key, val, v_size, n));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: setting attributes object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: lio_multiple_setattr_op_real - setting attributes object=%s\n", path);
    }

    return(err);
}

//***********************************************************************
// lio_multiple_setattr_op - Returns an attribute
//***********************************************************************

int lio_multiple_setattr_op(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n)
{
    int err;

    err = lio_multiple_setattr_op_real(lc, creds, path, id, key, val, v_size, n);
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = lio_multiple_setattr_op_real(lc, creds, path, id, key, val, v_size, n);
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_multiple_setattr_op_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;
    int err;

    err = lio_multiple_setattr_op(op->lc, op->creds, op->path, op->id, op->mkeys, op->mvals, op->mv_size, op->n_keys);
    status.error_code = err;
    status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
    if (err != 0) {
        notify_printf(op->lc->notify, 1, op->creds, "ERROR: lio_multiple_setattr_op_fn - fname=%s n_keys=%s\n", op->path, op->n_keys);
    }

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_multiple_setattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->mkeys = key;
    op->mvals = val;
    op->mv_size = v_size;
    op->n_keys = n_keys;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_multiple_setattr_op_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_setattr_real - Sets an attribute
//***********************************************************************

int lio_setattr_real(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    int err, serr;
    os_fd_t *fd;

    err = gop_sync_exec(os_open_object(lc->os, creds, (char *)path, OS_MODE_READ_IMMEDIATE, id, &fd, lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: opening object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: lio_setattr_real - opening object=%s\n", path);
        return(err);
    }

    serr = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));

    //** Close the parent
    err = gop_sync_exec(os_close_object(lc->os, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: closing object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: closing object=%s\n", path);
    }

    if (serr != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: setting attribute object=%s\n", path);
        notify_printf(lc->notify, 1, creds, "ERROR: setting attribute object=%s\n", path);
        err = OP_STATE_FAILURE;
    }

    return(err);
}

//***********************************************************************
// lio_setattr - Sets a single attribute
//***********************************************************************

int lio_setattr(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    int err;

    err = lio_setattr_real(lc, creds, path, id, key, val, v_size);
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = lio_setattr_real(lc, creds, path, id, key, val, v_size);
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_setattr_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;

    status.op_status = lio_setattr(op->lc, op->creds, op->path, op->id, op->skey, op->sval, op->n_keys); //** NOTE: n_keys = v_size
    status.error_code = 0;
    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_setattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void *val, int v_size)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->path = path;
    op->id = id;
    op->skey = key;
    op->sval = val;
    op->n_keys = v_size;  //** Double use for the vaiable

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_setattr_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_multiple_setattr_fd_op - Returns an attribute
//***********************************************************************

int lio_multiple_setattr_fd_op(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    int err;

    err = gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = gop_sync_exec(os_set_multiple_attrs(lc->os, creds, fd, key, val, v_size, n));
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_multiple_setattr_fd_op_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;
    int err;

    err = lio_multiple_setattr_fd_op(op->lc, op->creds, op->fd, op->mkeys, op->mvals, op->mv_size, op->n_keys);
    status.error_code = err;
    status.op_status = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_multiple_setattr_fd_gop(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n_keys)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->fd = fd;
    op->mkeys = key;
    op->mvals = val;
    op->mv_size = v_size;
    op->n_keys = n_keys;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_multiple_setattr_fd_op_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_setattr_fd - Sets a single attribute for an open file
//***********************************************************************

int lio_setattr_fd(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
    int err;

    err = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));
    if (err != OP_STATE_SUCCESS) {  //** Got an error
        sleep(1);  //** Wait a bit before retrying
        err = gop_sync_exec(os_set_attr(lc->os, creds, fd, key, val, v_size));
    }

    return(err);
}

//***********************************************************************

gop_op_status_t lio_setattr_fd_fn(void *arg, int id)
{
    lio_attrs_op_t *op = (lio_attrs_op_t *)arg;
    gop_op_status_t status;

    status.op_status = lio_setattr_fd(op->lc, op->creds, op->fd, op->skey, op->sval, op->n_keys); //** NOTE: n_keys = v_size
    status.error_code = 0;
    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_setattr_fd_gop(lio_config_t *lc, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
    lio_attrs_op_t *op;
    tbx_type_malloc_clear(op, lio_attrs_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->fd = fd;
    op->skey = key;
    op->sval = val;
    op->n_keys = v_size;  //** Double use for the vaiable

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_setattr_fd_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_create_attr_iter - Creates an attribute iterator
//***********************************************************************

os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, lio_creds_t *creds, const char *path, lio_os_regex_table_t *attr, int v_max)
{
    return(os_create_attr_iter(lc->os, creds, (char *)path, attr, v_max));
}

//***********************************************************************
// lio_next_attr - Returns the next attribute from the iterator
//***********************************************************************

int lio_next_attr(lio_config_t *lc, os_attr_iter_t *it, char **key, void **val, int *v_size)
{
    return(os_next_attr(lc->os, it, key, val, v_size));
}

//***********************************************************************
// lio_destroy_attr_iter - Destroy the attribute iterator
//***********************************************************************

void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it)
{
    os_destroy_attr_iter(lc->os, it);
}

//*************************************************************************
// ftype_lio2posix - Converts a LIO filetype to a posix mode
//*************************************************************************

mode_t ftype_lio2posix(int ftype)
{
    mode_t mode;

    mode = lio_os2mode_flags(ftype);

    if (ftype & OS_OBJECT_SYMLINK_FLAG) {
        mode |= 0777;
    } else if (ftype & OS_OBJECT_DIR_FLAG) {
        mode |= 0755;
    } else {
        mode |= 0666;  //** Make it so that everything has RW access. It'll get overwritten by the ACLs if enabled
    }

    return(mode);
}

//*************************************************************************

int lio_get_symlink_inode(lio_config_t *lc, lio_creds_t *creds, const char *fname, char *rpath, int fetch_realpath, ex_id_t *ino)
{
    const char *rp;
    char val[128];
    char *vptr = val;
    int v_size;

    //** Get the realpath
    rp = fname;
    rpath[0] = '\0';
    if (fetch_realpath) {
        if (lio_realpath(lc, creds, fname, rpath) != 0) return(-1);
        rp = rpath;
    }

    //** Now fetch the inode
    v_size = sizeof(val)-1;

    if (lio_getattr(lc, creds, rp, NULL, "system.inode", (void **)&vptr, &v_size) != OP_STATE_SUCCESS) return(-1);

    if (v_size > 0) {
        sscanf(val, XIDT, ino);
    } else {
        return(-1);
    }

    return(0);
}

//*************************************************************************
// lio_parse_stat_vals - Parses the stat values received
//   NOTE: All the val[*] strings are free'ed!
//*************************************************************************

void lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char **flink, int *ftype)
{
    int i, n;
    ex_id_t ino;
    ex_off_t len;
    int ts;

    memset(stat, 0, sizeof(struct stat));

    ino = 0;
    if (val[0] != NULL) {
        sscanf(val[0], XIDT, &ino);
    } else {
        generate_ex_id(&ino);
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT "\n", ino);
    }
    stat->st_ino = ino;

    //** Modify TS's
    ts = 0;
    if (val[1] != NULL) lio_get_timestamp(val[1], &ts, NULL);
    stat->st_mtime = ts;
//QWERT FIXME    ts = 0;
//QWERT FIXME    if (val[1] != NULL) lio_get_timestamp(val[2], &ts, NULL);
    stat->st_ctime = ts;
    stat->st_atime = stat->st_ctime;

    //** Get the symlink if it exists and optionally store it
    if (flink) {
        *flink = val[6];
        val[6] = NULL;
    }

    //** File types
    n = 0;
    if (val[4] != NULL) sscanf(val[4], "%d", &n);
    stat->st_mode = ftype_lio2posix(n);
    if (ftype) *ftype = n;

    len = 0;
    if (val[3] != NULL) sscanf(val[3], XOT, &len);

    stat->st_size = (stat->st_mode & S_IFDIR) ? 1 : len;
    lio_store_stat_size(stat, stat->st_size);
    stat->st_blksize = 4096;

    //** N-links
    n = 0;
    if (val[5] != NULL) sscanf(val[5], "%d", &n);
    stat->st_nlink = n;

    //** Clean up
    for (i=0; i<_lio_stat_key_size; i++) {
        if (val[i] != NULL) {
            free(val[i]);
            val[i] = NULL;
        }
    }
}

//***********************************************************************
// lio_stat - Do a simple file stat
//***********************************************************************

int lio_stat(lio_config_t *lc, lio_creds_t *creds, char *fname, struct stat *stat, char **readlink, int stat_symlink, int no_cache_stat_if_file)
{
    char *val[_lio_stat_key_size];
    int v_size[_lio_stat_key_size], i, err;
    char *slink;
    char rpath[OS_PATH_MAX];
    ex_id_t ino;
    int hit, ocl_slot;

    log_printf(1, "fname=%s\n", fname);
    tbx_log_flush();

    //** Get the inode if it exists
    //** Assumes the system.inode is in slot 0 in the _inode_keys
    v_size[0] = -lc->max_attr;
    val[0] = NULL;
    err = lio_get_multiple_attrs(lc, creds, fname, NULL, _lio_stat_keys, (void **)val, v_size, 1, 0);
    hit = 0;
    if (err == OP_STATE_SUCCESS) {
        hit = 1;
        if (val[0] != NULL) {
            sscanf(val[0], XIDT, &ino);
            free(val[0]);
        }
        ocl_slot = ino % lc->open_close_lock_size;
        apr_thread_mutex_lock(lc->open_close_lock[ocl_slot]);
    }

    for (i=0; i<_lio_stat_key_size; i++) v_size[i] = -lc->max_attr;
    err = lio_get_multiple_attrs(lc, creds, fname, NULL, _lio_stat_keys, (void **)val, v_size, _lio_stat_key_size, no_cache_stat_if_file);

    if (err != OP_STATE_SUCCESS) {
        if (hit == 1) apr_thread_mutex_unlock(lc->open_close_lock[ocl_slot]);  //** Unlock if needed
        return(-ENOENT);
    }
    slink = NULL;
    lio_parse_stat_vals(fname, stat, val, v_size, &slink, NULL);

    //** Now update the fields based on the requested symlink behavior
    if (slink) {
        if (stat_symlink == 1) {
            stat->st_size = strlen(slink);
        } else { //** Get the symlink target values
            ino = 0;
            if (lio_get_symlink_inode(lc, creds, fname, rpath, 1, &ino) == 0) {
                stat->st_ino = ino;
                lio_update_stat_open_file_size(lc, ino, stat, 1);
            }
       }
    } else {
       lio_update_stat_open_file_size(lc, stat->st_ino, stat, 1);
    }

    if (hit == 1) apr_thread_mutex_unlock(lc->open_close_lock[ocl_slot]);  //** Unlock if needed

    if (readlink) {
        *readlink = slink;
    } else if (slink) {
        free(slink);
    }

    log_printf(1, "END fname=%s err=%d\n", fname, err);
    tbx_log_flush();

    return(0);
}

//***********************************************************************
// lio_stat_iter_destroy - Destroy's the stat iter
//***********************************************************************

void lio_stat_iter_destroy(lio_stat_iter_t *dit)
{
    if (dit->dot.readlink) free(dit->dot.readlink);
    if (dit->dotdot.readlink) free(dit->dotdot.readlink);

    if (dit->it) lio_destroy_object_iter(dit->lc, dit->it);
    lio_os_regex_table_destroy(dit->path_regex);
    free(dit);
}

//***********************************************************************
// lio_stat_iter_create - Creates a stat iterator
//***********************************************************************

lio_stat_iter_t *lio_stat_iter_create(lio_config_t *lc, lio_creds_t *creds, const char *path, int stat_symlink)
{
    lio_stat_iter_t *dit;
    char prefix[OS_PATH_MAX];
    char *dir, *file;
    int i;

    tbx_type_malloc_clear(dit, lio_stat_iter_t, 1);
    dit->lc = lc;
    dit->creds = creds;
    dit->stat_symlink = stat_symlink;

    for (i=0; i<_lio_stat_key_size; i++) {
        dit->v_size[i] = -lc->max_attr;
        dit->val[i] = NULL;
    }

    i = strlen(path);
    if (path[i-1] == '/') {
        dit->prefix_len = i;
    } else {
        dit->prefix_len = i+1;
    }

    snprintf(prefix, OS_PATH_MAX, "%s/*", path);
    dit->path_regex = lio_os_path_glob2regex(prefix);

    dit->it = lio_create_object_iter_alist(lc, lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY_FLAG, 0, _lio_stat_keys, (void **)dit->val, dit->v_size, _lio_stat_key_size);
    if (dit->it == NULL) {
        lio_stat_iter_destroy(dit);
        return(NULL);
    }

    //** Add "."
    dit->dot.dentry = ".";
    if (lio_stat(lc, creds, (char *)path, &(dit->dot.stat), &(dit->dot.readlink), stat_symlink, 0) != 0) {
        lio_stat_iter_destroy(dit);
        return(NULL);
    }

    //** And ".."
    if (strcmp(path, "/") != 0) {
        lio_os_path_split((char *)path, &dir, &file);
        free(file);
    } else {
        dir = strdup(path);
    }

    dit->dot.dentry = "..";
    if (lio_stat(lc, creds, (char *)dir, &(dit->dot.stat), &(dit->dot.readlink), stat_symlink, 0) != 0) {
        if (dir) free(dir);
        lio_stat_iter_destroy(dit);
        return(NULL);
    }

    return(dit);
}

//***********************************************************************
// lio_stat_iter_next - Returns the next entries
//***********************************************************************

int lio_stat_iter_next(lio_stat_iter_t *dit, struct stat *stat, char **dentry, char **readlink)
{
    int ftype, prefix_len;
    char *fname, *flink;
    _dentry_t *de;
    char rpath[OS_PATH_MAX];
    ex_id_t ino;

    if (dit->state < 2) {
        de = (dit->state == 0) ? &(dit->dot) : &(dit->dotdot);
        *stat = de->stat;
        if (dentry) {
            if (de->dentry) *dentry = strdup(de->dentry);
        }
        if (readlink) {
            if (de->readlink) *readlink = strdup(de->readlink);
        }
        dit->state++;
        return(0);
    }

    if (dit->state == 3) goto finished;

    //** Off . and .. and into regular objects
    //** Get the next object
    ftype = lio_next_object(dit->lc, dit->it, &fname, &prefix_len);
    if (ftype <= 0) {
        dit->state = 3;
        goto finished;
    }

    //** And parse it
    lio_parse_stat_vals(fname, stat, dit->val, dit->v_size, &flink, NULL);
    if (dentry) *dentry = strdup(fname + prefix_len);

    //** Now update the fields based on the requested symlink behavior
    if (flink) {
        if (dit->stat_symlink == 1) {
            stat->st_size = strlen(flink);
        } else { //** Get the symlink target values
            ino = 0;
            if (lio_get_symlink_inode(dit->lc, dit->creds, fname, rpath, 1, &ino) == 0) {
                stat->st_ino = ino;
                lio_update_stat_open_file_size(dit->lc, ino, stat, 1);
            }
       }
    } else {
        lio_update_stat_open_file_size(dit->lc, ino, stat, 1);
    }

    if (readlink) {
        *readlink = flink;
    } else if (flink) {
        free(flink);
    }

    return(0);

finished:  //** If we make it here we're done
    if (dentry) *dentry = NULL;
    if (readlink) *readlink = NULL;
    return(1);
}

//***********************************************************************
//***********************************************************************
//  FSCK related routines
//***********************************************************************
//***********************************************************************

//***********************************************************************
// lio_fsck_check_file - Checks a file for errors and optionally repairs them
//***********************************************************************

int lio_fsck_check_object(lio_config_t *lc, lio_creds_t *creds, char *path, int ftype, lio_fsck_repair_t owner_mode, char *owner, lio_fsck_repair_t exnode_mode, char **val, int *v_size)
{
    int state, err, srepair, index, vs, ex_index;
    char *dir, *file, ssize[128], *v;
    ex_id_t ino;
    ex_off_t attr_size, md_size, seg_size;
    lio_exnode_exchange_t *exp;
    lio_exnode_t *ex, *cex;
    lio_segment_t *seg;
    int do_clone, exnode_retry;
    lio_fsck_repair_t ex_mode;
    ex_index = 2;
    state = 0;
    exnode_retry = 1;

    srepair = exnode_mode & LIO_FSCK_SIZE_REPAIR;
    ex_mode = (srepair > 0) ? exnode_mode - LIO_FSCK_SIZE_REPAIR : exnode_mode;

    log_printf(15, "fname=%s vs[0]=%d vs[1]=%d vs[2]=%d\n", path, v_size[0], v_size[1], v_size[2]);

    //** Check the owner
    index = 0;
    vs = v_size[index];
    if (vs <= 0) { //** Missing owner
        switch (owner_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_OWNER;
            log_printf(15, "fname=%s missing owner\n", path);
            break;
        case LIO_FSCK_PARENT:
            lio_os_path_split(path, &dir, &file);
            log_printf(15, "fname=%s parent=%s file=%s\n", path, dir, file);
            free(file);
            file = NULL;
            vs = -lc->max_attr;
            lio_getattr(lc, creds, dir, NULL, "system.owner", (void **)&file, &vs);
            log_printf(15, "fname=%s parent=%s owner=%s\n", path, dir, file);
            if (vs > 0) {
                if (file) {
                    lio_setattr(lc, creds, path, NULL, "system.owner", (void *)file, strlen(file));
                    free(file);
                }
            } else {
                state |= LIO_FSCK_MISSING_OWNER;
            }
            free(dir);
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(lio_remove_gop(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        case LIO_FSCK_USER:
            lio_setattr(lc, creds, path, NULL, "system.owner", (void *)owner, strlen(owner));
            break;
        case LIO_FSCK_SIZE_REPAIR:
            log_printf(0, "ERROR: Got size_repair on the owner repair\n");
            break;
        }
    }

    //** Check the inode
    index = 1;
    vs = v_size[index];
    if (vs <= 0) { //** Missing inode
        switch (owner_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_INODE;
            log_printf(15, "fname=%s missing owner\n", path);
            break;
        case LIO_FSCK_PARENT:
        case LIO_FSCK_USER:
            ino = 0;
            generate_ex_id(&ino);
            snprintf(ssize, sizeof(ssize),  XIDT, ino);
            lio_setattr(lc, creds, path, NULL, "system.inode", (void *)ssize, strlen(ssize));
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(lio_remove_gop(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        case LIO_FSCK_SIZE_REPAIR:
            log_printf(0, "ERROR: Got size_repair on the owner + inode repair\n");
            break;
        }
    }

    //** Check if we have an exnode
exnode_again:
    do_clone = 0;
    index = 2;
    vs = v_size[index];
    if (vs <= 0) {
        switch (ex_mode) {
        case LIO_FSCK_MANUAL:
            state |= LIO_FSCK_MISSING_EXNODE;
            return(state);
            break;
        case LIO_FSCK_PARENT:
            lio_os_path_split(path, &dir, &file);
            free(file);
            file = NULL;
            vs = -lc->max_attr;
            lio_getattr(lc, creds, dir, NULL, "system.exnode", (void **)&file, &vs);
            if (vs > 0) {
                val[index] = file;
                do_clone = 1;  //** flag we need to clone and store it
            } else {
                state |= LIO_FSCK_MISSING_EXNODE;
                free(dir);
                return(state);
            }
            free(dir);
            break;
        case LIO_FSCK_DELETE:
            gop_sync_exec(lio_remove_gop(lc, creds, path, val[ex_index], ftype));
            return(state);
            break;
        case LIO_FSCK_SIZE_REPAIR:
            log_printf(0, "ERROR: Got size_repair on the exnode repair\n");
            break;
        case LIO_FSCK_USER:
            log_printf(0, "ERROR: Got user_repair on the exnode repair\n");
            break;

        }
    }

    //** Make sure it's valid by loading it
    //** If this has a caching segment we need to disable it from being adding
    //** to the global cache table cause there could be multiple copies of the
    //** same segment being serialized/deserialized.
    //** Deserialize it
    exp = lio_exnode_exchange_text_parse(val[ex_index]);
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, lc->ess_nocache) != 0) {
        log_printf(15, "ERROR parsing parent exnode path=%s\n", path);
        state |= LIO_FSCK_MISSING_EXNODE;
        lio_exnode_destroy(ex);
        lio_exnode_exchange_destroy(exp);

        //** See if we give it another pass
        if (exnode_retry == 1) {
            exnode_retry = 0;
            val[ex_index] = NULL;
            v_size[ex_index] = 0;
            goto exnode_again;
        }

        log_printf(15, "fname=%s state=%d\n", path, state);
        return(state);
    }
    exp->text.text = NULL;

    //** Execute the clone operation if needed
    if (do_clone == 1) {
        err = gop_sync_exec(lio_exnode_clone_gop(lc->tpc_unlimited, ex, lc->da, &cex, NULL, CLONE_STRUCTURE, lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR cloning parent path=%s\n", path);
            state |= LIO_FSCK_MISSING_EXNODE;
            goto finished;
        }

        //** Serialize it for storage
        lio_exnode_serialize(cex, exp);
        lio_setattr(lc, creds, path, NULL, "system.exnode", (void *)exp->text.text, strlen(exp->text.text));
        lio_exnode_destroy(ex);
        ex = cex;   //** WE use the clone for size checking
    }

    if ((ftype & OS_OBJECT_DIR_FLAG) > 0) goto finished;  //** Nothing else to do if a directory

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        state |= LIO_FSCK_MISSING_EXNODE;
        goto finished;
    }

    //** Verify the size
    //** Fetch the size stored in system.exnode.size
    index = 3;
    v = val[index];
    vs = v_size[index];
    if (vs <= 0) {
        attr_size = -1;
    } else {
        sscanf(v, XOT, &attr_size);
    }

    //** Get the size from system.exnode.data
    index = 4;
    md_size = v_size[index];
    if (md_size < 0) md_size = 0;

    //** From the segment itself
    seg_size = segment_size(seg);

    if (srepair != LIO_FSCK_SIZE_REPAIR) {
        if (attr_size < 0) {
            state |= LIO_FSCK_MISSING_EXNODE_SIZE;
        }
        if (seg_size == 0) {  //** Stored as metadata
            if (attr_size != md_size) { //** Mismatch so store it
                state |= LIO_FSCK_SIZE_MISMATCH;
            }
        } else if (md_size == 0) {  //** Stored in the exnode
            if (attr_size != seg_size) { //** Mismatch so store it
                state |= LIO_FSCK_SIZE_MISMATCH;
            }
        }
        goto finished;
    }

    //** If we made it here we're going to try and do a repair
    if (seg_size == 0) {  //** Stored as metadata
        if (attr_size != md_size) { //** Mismatch so store it
            sprintf(ssize, I64T, md_size);
            lio_setattr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
        }
    } else if (md_size == 0) {  //** Stored in the exnode
        if (attr_size != seg_size) { //** Mismatch so store it
            sprintf(ssize, I64T, seg_size);
            lio_setattr(lc, creds, path, NULL, "system.exnode.size", (void *)ssize, strlen(ssize));
        }
    } else {  //** Got a size mismatvch and we don't handle those
        state |= LIO_FSCK_SIZE_MISMATCH;
    }


    //** Clean up
finished:
    lio_exnode_destroy(ex);
    lio_exnode_exchange_destroy(exp);

    log_printf(15, "fname=%s state=%d\n", path, state);

    return(state);
}


//***********************************************************************
// lio_fsck_gop - Inspects and optionally repairs the file
//***********************************************************************

gop_op_status_t lio_fsck_gop_fn(void *arg, int id)
{
    lio_fsck_check_t *op = (lio_fsck_check_t *)arg;
    int err, i;
    gop_op_status_t status;
    char *val[_n_fsck_keys];
    int v_size[_n_fsck_keys];
    log_printf(15, "fname=%s START\n", op->path);
    tbx_log_flush();

    if (op->ftype <= 0) { //** Bad Ftype so see if we can figure it out
        op->ftype = lio_exists(op->lc, op->creds, op->path);
    }

    if (op->ftype == 0) { //** No file
        status = gop_failure_status;
        status.error_code = LIO_FSCK_MISSING_FILE;
        return(status);
    }

    if (op->ftype & OS_OBJECT_BROKEN_LINK_FLAG) {  //** IF we have a broken symlink kick out
        info_printf(lio_ifd, 1, "Skipping broken symlink: %s\n", op->path);
        return(gop_success_status);
    }

    if (op->full == 0) {
        log_printf(15, "fname=%s getting attrs\n", op->path);
        tbx_log_flush();
        for (i=0; i<_n_fsck_keys; i++) {
            val[i] = NULL;
            v_size[i] = -op->lc->max_attr;
        }
        lio_get_multiple_attrs(op->lc, op->creds, op->path, NULL, _fsck_keys, (void **)&val, v_size, _n_fsck_keys, 0);
        err = lio_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, val, v_size);
        for (i=0; i<_n_fsck_keys; i++) if (val[i] != NULL) free(val[i]);
    } else {
        err = lio_fsck_check_object(op->lc, op->creds, op->path, op->ftype, op->owner_mode, op->owner, op->exnode_mode, op->val, op->v_size);
    }

    log_printf(15, "fname=%s status=%d\n", op->path, err);
    status = gop_success_status;
    status.error_code = err;
    return(status);
}

//***********************************************************************
// lio_fsck_gop - Inspects and optionally repairs the file
//***********************************************************************

gop_op_generic_t *lio_fsck_gop(lio_config_t *lc, lio_creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode)
{
    lio_fsck_check_t *op;

    log_printf(15, "fname=%s START\n", fname);
    tbx_log_flush();

    tbx_type_malloc_clear(op, lio_fsck_check_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->ftype = ftype;
    op->path = fname;
    op->owner_mode = owner_mode;
    op->owner = owner;
    op->exnode_mode = exnode_mode;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_fsck_gop_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_fsck_gop - Inspects and optionally repairs the file
//***********************************************************************

gop_op_generic_t *lio_fsck_gop_full(lio_config_t *lc, lio_creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode, char **val, int *v_size)
{
    lio_fsck_check_t *op;

    tbx_type_malloc(op, lio_fsck_check_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->ftype = ftype;
    op->path = fname;
    op->owner_mode = owner_mode;
    op->owner = owner;
    op->exnode_mode = exnode_mode;
    op->val = val;
    op->v_size = v_size;
    op->full = 1;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_fsck_gop_fn, (void *)op, free, 1));
}

//***********************************************************************
// lio_next_fsck - Returns the next broken object
//***********************************************************************

int lio_next_fsck(lio_config_t *lc, lio_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;
    int i, prefix_len, slot;
    lio_fsck_task_t *task;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    if (it->firsttime == 1) {  //** First time through so fill up the tasks
        it->firsttime = 2;
        for (slot=0; slot< it->n; slot++) {
            task = &(it->task[slot]);
            task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
            if (task->ftype <= 0) break;  //** No more tasks
            log_printf(15, "fname=%s slot=%d\n", task->fname, slot);

            memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
            memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

            gop = lio_fsck_gop_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
            gop_set_myid(gop, slot);
            gop_opque_add(it->q, gop);
        }
    }

    log_printf(15, "main loop start nque=%d\n", gop_opque_tasks_left(it->q));

    //** Start processing the results
    while ((gop = opque_waitany(it->q)) != NULL) {
        it->visited_count++;
        slot = gop_get_myid(gop);
        task = &(it->task[slot]);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
        *bad_atype = task->ftype;  //** Preserve the info before launching a new one
        *bad_fname = task->fname;
        log_printf(15, "fname=%s slot=%d state=%d\n", task->fname, slot, status.error_code);
        for (i=0; i<_n_fsck_keys; i++) {
            if (task->val[i] != NULL) free(task->val[i]);
        };

        if (it->firsttime == 2) {  //** Only go here if we hanve't finished iterating
            task->ftype = os_next_object(it->lc->os, it->it, &(task->fname), &prefix_len);
            if (task->ftype <= 0) {
                it->firsttime = 3;
            } else {
                memcpy(task->val, it->val, _n_fsck_keys*sizeof(char *));
                memcpy(task->v_size, it->v_size, _n_fsck_keys*sizeof(int));

                gop = lio_fsck_gop_full(it->lc, it->creds, task->fname, task->ftype, it->owner_mode, it->owner, it->exnode_mode, task->val, task->v_size);
                gop_set_myid(gop, slot);
                gop_opque_add(it->q, gop);
            }
        }

        log_printf(15, "fname=%s state=%d LIO_FSCK_GOOD=%d\n", *bad_fname, status.error_code, LIO_FSCK_GOOD);
        if (status.error_code != LIO_FSCK_GOOD) { //** Found one
            log_printf(15, "ERROR fname=%s state=%d\n", *bad_fname, status.error_code);
            return(status.error_code);
        }

        free(*bad_fname);  //** IF we made it here we can throw away the old fname
    }

    log_printf(15, "nothing left\n");
    *bad_atype = 0;
    *bad_fname = NULL;
    return(LIO_FSCK_FINISHED);

}

//***********************************************************************
// lio_create_fsck_iter - Creates an FSCK iterator
//***********************************************************************

lio_fsck_iter_t *lio_create_fsck_iter(lio_config_t *lc, lio_creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode)
{
    lio_fsck_iter_t *it;
    int i;

    tbx_type_malloc_clear(it, lio_fsck_iter_t, 1);

    it->lc = lc;
    it->creds = creds;
    it->path = strdup(path);
    it->owner_mode = owner_mode;
    it->owner = owner;
    it->exnode_mode = exnode_mode;

    it->regex = lio_os_path_glob2regex(it->path);

    for (i=0; i<_n_fsck_keys; i++) {
        it->v_size[i] = -lc->max_attr;
        it->val[i] = NULL;
    }

    it->it = os_create_object_iter_alist(it->lc->os, creds, it->regex, NULL, OS_OBJECT_ANY_FLAG, 10000, _fsck_keys, (void **)it->val, it->v_size, _n_fsck_keys);
    if (it->it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
        return(NULL);
    }

    it->n = lio_parallel_task_count;
    it->firsttime = 1;
    tbx_type_malloc_clear(it->task, lio_fsck_task_t, it->n);
    it->q = gop_opque_new();
    opque_start_execution(it->q);

    return((lio_fsck_iter_t *)it);
}

//***********************************************************************
// lio_destroy_fsck_iter - Creates an FSCK iterator
//***********************************************************************

void lio_destroy_fsck_iter(lio_config_t *lc, lio_fsck_iter_t *oit)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;
    gop_op_generic_t *gop;
    int slot;

    while ((gop = opque_waitany(it->q)) != NULL) {
        slot = gop_get_myid(gop);
        if (it->task[slot].fname != NULL) free(it->task[slot].fname);
    }
    gop_opque_free(it->q, OP_DESTROY);

    os_destroy_object_iter(it->lc->os, it->it);

    lio_os_regex_table_destroy(it->regex);
    free(it->path);
    free(it->task);
    free(it);

    return;
}

//***********************************************************************
// lio_fsck_visited_count - Returns the number of files checked
//***********************************************************************

ex_off_t lio_fsck_visited_count(lio_config_t *lc, lio_fsck_iter_t *oit)
{
    lio_fsck_iter_t *it = (lio_fsck_iter_t *)oit;

    return(it->visited_count);
}

