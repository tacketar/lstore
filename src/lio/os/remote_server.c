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
// Remote OS implementation for the Server side
//***********************************************************************

#define _log_module_index 214

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/mq_helpers.h>
#include <gop/mq_ongoing.h>
#include <gop/mq.h>
#include <gop/mq_stream.h>
#include <gop/types.h>
#include <inttypes.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include <time.h>

#include "authn.h"
#include "authn/fake.h"
#include "ex3/system.h"
#include "ex3/types.h"
#include "os.h"
#include "os/file.h"
#include "os/remote.h"
#include "service_manager.h"

#ifdef ENABLE_OSRS_DEBUG
    #define OSRS_DEBUG(...) __VA_ARGS__
    #define OSRS_DEBUG_NOTIFY(fmt, ...) if (os_notify_handle) _tbx_notify_printf(os_notify_handle, 1, NULL, __func__, __LINE__, fmt, ## __VA_ARGS__)
#else
    #define OSRS_DEBUG(...)
    #define OSRS_DEBUG_NOTIFY(fmt, ...)
#endif

#define OSRS_LONG_RUNNING(p, mode)  if (mode & (OS_MODE_READ_BLOCKING|OS_MODE_WRITE_BLOCKING)) gop_mq_long_running_set(p, 1)

#define FIXME_SIZE 1024*1024

static lio_osrs_priv_t osrs_default_options = {
    .section = "os_remote_server",
    .hostname = NULL,
    .fname_activity = NULL,
    .ongoing_interval = 30,
    .max_stream = 10*1024*1024,
    .os_local_section = "rs_simple",
    .fname_active = "/lio/log/os_active.log",
    .max_active = 1024
};

typedef struct {
    char *host_id;
    ex_off_t  count;
    int host_id_len;
    apr_time_t start;
    apr_time_t last;
} osrs_active_t;

typedef struct {
    lio_object_service_fn_t *os;
    os_fd_t *fd;
} osrs_close_fail_t;

typedef struct {
    char *handle;
    apr_ssize_t handle_len;
    gop_op_generic_t *gop;
} osrs_abort_handle_t;

typedef struct {
    char *key;
    int key_len;
    int abort;
    apr_time_t last_hb;
    gop_op_generic_t *gop;
} spin_hb_t;

typedef struct {
    intptr_t key;
    int count;
} pending_lock_entry_t;

lio_object_service_fn_t *_os_global = NULL;  //** This is used for the signal

static gop_op_status_t bad_creds_status = {.op_status = OP_STATE_FAILURE, .error_code = -ENOKEY };


//***********************************************************************
// osrs_print_active_table - Print the active table
//***********************************************************************

void osrs_print_active_table(lio_object_service_fn_t *os, FILE *fd)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    char sdate[128], ldate[128];
    osrs_active_t *a;

    apr_thread_mutex_lock(osrs->lock);
    apr_ctime(sdate, apr_time_now());
    fprintf(fd, "OSRS Dumping active table (n=%d max_active=%d) --------------------------------\n", tbx_stack_count(osrs->active_lru), osrs->max_active);
    fprintf(fd, "#timestamp: %s\n", sdate);
    fprintf(fd, "#host|start_time|last_time|count\n");
    tbx_stack_move_to_top(osrs->active_lru);
    a = tbx_stack_get_current_data(osrs->active_lru);
    while (a != NULL) {
        apr_ctime(sdate, a->start);
        apr_ctime(ldate, a->last);
        fprintf(fd, "%s|%s|%s|"XOT "\n", a->host_id, sdate, ldate, a->count);
        tbx_stack_move_down(osrs->active_lru);
        a = tbx_stack_get_current_data(osrs->active_lru);
    }
    apr_thread_mutex_unlock(osrs->lock);
    fprintf(fd, "\n");
}


//***********************************************************************
//  signal_print_active_table - Dumps the active table
//***********************************************************************

void osrs_siginfo_fn(void *arg, FILE *fd)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;

    if (_os_global == NULL) return;

    osrs_print_active_table(os, fd);

    return;
}

//***********************************************************************
// osrs_update_active_table - Updates the active table
//    NOTE:  Currently this only tracks callbacks that use streams
//***********************************************************************

void osrs_update_active_table(lio_object_service_fn_t *os, gop_mq_frame_t *hid)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    char *host_id;
    int id_len;
    tbx_stack_ele_t *ele;
    osrs_active_t *a;

    gop_mq_get_frame(hid, (void **)&host_id, &id_len);

    apr_thread_mutex_lock(osrs->lock);

    ele = apr_hash_get(osrs->active_table, host_id, id_len);
    if (ele == NULL) { //** 1st time so need to add it
        //** Check if we need to clean things up
        if (tbx_stack_count(osrs->active_lru) >= osrs->max_active) {
            tbx_stack_move_to_bottom(osrs->active_lru);
            a = (osrs_active_t *)tbx_stack_get_current_data(osrs->active_lru);
            apr_hash_set(osrs->active_table, a->host_id, a->host_id_len, NULL);
            if (a->host_id) free(a->host_id);
            free(a);
            tbx_stack_delete_current(osrs->active_lru, 1, 0);
        }

        //** Now make the new entry
        tbx_type_malloc_clear(a, osrs_active_t, 1);
        tbx_type_malloc(a->host_id, char, id_len+1);
        memcpy(a->host_id, host_id, id_len);
        a->host_id[id_len] = 0;
        a->host_id_len = id_len;
        a->start = a->last = apr_time_now();
        a->count = 0;

        //** add it
        tbx_stack_push(osrs->active_lru, a);
        ele = tbx_stack_get_current_ptr(osrs->active_lru);
        apr_hash_set(osrs->active_table, a->host_id, a->host_id_len, ele);
    }

    //** Get the handle
    a = (osrs_active_t *)tbx_stack_ele_get_data(ele);
    a->last = apr_time_now();  //** Update it
    a->count++;

    //** and move it to the front
    tbx_stack_move_to_ptr(osrs->active_lru, ele);
    tbx_stack_unlink_current(osrs->active_lru, 1);
    tbx_stack_link_push(osrs->active_lru, ele);

    apr_thread_mutex_unlock(osrs->lock);
}

//***********************************************************************
// osrs_release_creds - Release the creds
//***********************************************************************

void osrs_release_creds(lio_object_service_fn_t *os, lio_creds_t *creds)
{
    if (creds) an_cred_destroy(creds);
}

//***********************************************************************
// osrs_get_creds - Retreives the creds from the message
//
// =====NOTE: this routine is a hack to pass around the userid and host ===
//***********************************************************************

lio_creds_t *osrs_get_creds(lio_object_service_fn_t *os, gop_mq_frame_t *f)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    lio_creds_t *creds;
    void *cred_args[2];
    int len;
    void *ptr;

    gop_mq_get_frame(f, &ptr, &len);
    cred_args[0] = ptr;
    cred_args[1] = &len;

    creds = authn_cred_init(osrs->authn, AUTHN_INIT_LOOKUP, cred_args);
    return(creds);
}

//***********************************************************************
// osrs_add_abort_handle - Installs the provided handle into the table
//    to allow an abort.
//***********************************************************************

void osrs_add_abort_handle(lio_object_service_fn_t *os, osrs_abort_handle_t *ah)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;

    apr_thread_mutex_lock(osrs->abort_lock);
    apr_hash_set(osrs->abort, ah->handle, ah->handle_len, ah);
    apr_thread_mutex_unlock(osrs->abort_lock);
}


//***********************************************************************
// osrs_remove_abort_handle - Removes the provided handle from the abort table
//***********************************************************************

void osrs_remove_abort_handle(lio_object_service_fn_t *os, osrs_abort_handle_t *ah)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;

    apr_thread_mutex_lock(osrs->abort_lock);
    apr_hash_set(osrs->abort, ah->handle, ah->handle_len, NULL);
    apr_thread_mutex_unlock(osrs->abort_lock);
}

//***********************************************************************
// osrs_perform_abort_handle - Performs the actual abort
//***********************************************************************

gop_op_status_t osrs_perform_abort_handle(lio_object_service_fn_t *os, char *handle, int handle_len)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    osrs_abort_handle_t *ah;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    status = gop_failure_status;

    apr_thread_mutex_lock(osrs->abort_lock);
    ah = apr_hash_get(osrs->abort, handle, handle_len);

    if (ah != NULL) {
        gop = os_abort_open_object(osrs->os_child, ah->gop);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    }
    apr_thread_mutex_unlock(osrs->abort_lock);

    return(status);
}

//***********************************************************************
// close_can_fail_check
//***********************************************************************

int close_can_fail_check(void *arg, void *handle, int count)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    pending_lock_entry_t *entry;
    intptr_t key;

    if (handle == NULL) return(1);

    key = *(intptr_t *)&handle;
    apr_thread_mutex_lock(osrs->lock);
    entry = apr_hash_get(osrs->pending_lock_table, &key, sizeof(key));
    apr_thread_mutex_unlock(osrs->lock);

    if (entry) {
        if (count > entry->count) {
            return(0);
        } else {
            return(1);
        }
    }

    //** We default to failing and defer to when the oo->count is 0
    return(0);
}

//***********************************************************************
//  pending_lock_inc - Increments the pending lock in case of a failure
//***********************************************************************

pending_lock_entry_t *pending_lock_inc(lio_object_service_fn_t *os, os_fd_t *fd)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    intptr_t key;
    pending_lock_entry_t *entry;

    key = *(intptr_t *)&fd;
    apr_thread_mutex_lock(osrs->lock);
    entry = apr_hash_get(osrs->pending_lock_table, &key, sizeof(key));
    if (entry == NULL) {
        tbx_type_malloc_clear(entry, pending_lock_entry_t, 1);
        entry->key = key;
        entry->count = 0;
        apr_hash_set(osrs->pending_lock_table, &(entry->key), sizeof(intptr_t), entry);
    }
    entry->count++;

    apr_thread_mutex_unlock(osrs->lock);

    return(entry);
}

//***********************************************************************
//  pending_lock_dec - Decrements the pending lock in case of a failure
//***********************************************************************

void pending_lock_dec(lio_object_service_fn_t *os, pending_lock_entry_t *entry)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;

    apr_thread_mutex_lock(osrs->lock);
    entry->count--;
    if (entry->count == 0) {
       apr_hash_set(osrs->pending_lock_table, &(entry->key), sizeof(entry->key), NULL);
       free(entry);
    }
    apr_thread_mutex_unlock(osrs->lock);
}


//***********************************************************************
// osrs_exists_cb - Processes the object exists command
//***********************************************************************

void osrs_exists_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred;
    char *name;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    if (creds != NULL) {
        gop = os_exists(osrs->os_child, creds, name);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    log_printf(5, "END\n");

}

//***********************************************************************
// osrs_realpath_cb - Processes the object realpath command
//***********************************************************************

void osrs_realpath_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred;
    char *name;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    char realpath[OS_PATH_MAX];
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    if (creds != NULL) {
        status = gop_sync_exec_status(os_realpath(osrs->os_child, creds, name, realpath));
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));

    if (status.op_status == OP_STATE_SUCCESS) {
        gop_mq_msg_append_mem(response, strdup(realpath), strlen(realpath)+1, MQF_MSG_AUTO_FREE);
    }

    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    log_printf(5, "END\n");

}

//***********************************************************************
// osrs_spin_hb_cb - Processes the Spin HB command
//***********************************************************************

void osrs_spin_hb_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fcred, *fspin;
    char *spin_hb;
    spin_hb_t *spin;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    gop_mq_frame_destroy(mq_msg_pop(msg));  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Host/user ID

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fspin = mq_msg_pop(msg);  //** This has the Spin ID
    gop_mq_get_frame(fspin, (void **)&spin_hb, &fsize);

    //** Now check if the handle is valid
    apr_thread_mutex_lock(osrs->lock);
    if ((spin = apr_hash_get(osrs->spin, spin_hb, fsize)) != NULL) {
        spin->last_hb = apr_time_now();
    } else {
        log_printf(5, "Invalid handle!\n");
    }
    apr_thread_mutex_unlock(osrs->lock);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fspin);
    gop_mq_frame_destroy(fcred);

    log_printf(5, "END\n");
}

//***********************************************************************
// osrs_object_exec_modify_cb - Processes the exec bit setting
//***********************************************************************

void osrs_object_exec_modify_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred, *f;
    char *name;
    char *data;
    lio_creds_t *creds;
    int fsize, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    int64_t exec_mode;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    f = mq_msg_pop(msg);  //** This has the Object type
    gop_mq_get_frame(f, (void **)&data, &nbytes);
    tbx_zigzag_decode((unsigned char *)data, nbytes, &exec_mode);
    gop_mq_frame_destroy(f);

    if (creds != NULL) {
        status = gop_sync_exec_status(os_object_exec_modify(osrs->os_child, creds, name, exec_mode));
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    log_printf(5, "END\n");
}

//***********************************************************************
// osrs_create_object_cb - Processes the create object command
//***********************************************************************

void osrs_create_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred, *f;
    char *name;
    char *data;
    lio_creds_t *creds;
    int fsize, nbytes;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    int64_t ftype;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    f = mq_msg_pop(msg);  //** This has the Object type
    gop_mq_get_frame(f, (void **)&data, &nbytes);
    tbx_zigzag_decode((unsigned char *)data, nbytes, &ftype);
    gop_mq_frame_destroy(f);

    f = mq_msg_pop(msg);  //** This has the ID used for the create attribute
    gop_mq_get_frame(f, (void **)&data, &nbytes);

    if (creds != NULL) {
        data = gop_mq_frame_strdup(f);
        gop = os_create_object(osrs->os_child, creds, name, ftype, data);
        gop_waitall(gop);
        if (data != NULL) free(data);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);
    gop_mq_frame_destroy(f);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    log_printf(5, "END\n");
}


//***********************************************************************
// osrs_create_object_wth_attrs_cb - Processes the create object with attrs command
//***********************************************************************

void osrs_create_object_with_attrs_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred, *f, *fdata;
    char *name;
    char *id;
    unsigned char *data;
    lio_creds_t *creds;
    int fsize, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    int64_t ftype;
    char **key;
    char **val;
    int *v_size;
    int bpos, i;
    int64_t n_keys, v;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    f = mq_msg_pop(msg);  //** This has the Object type
    gop_mq_get_frame(f, (void **)&data, &nbytes);
    tbx_zigzag_decode(data, nbytes, &ftype);
    gop_mq_frame_destroy(f);

    f = mq_msg_pop(msg);  //** This has the ID used for the create attribute
    gop_mq_get_frame(f, (void **)&id, &nbytes);

    //** Parse the attr list
    fdata = mq_msg_pop(msg);  //** attr list to set
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    key = NULL;
    v_size = NULL;
    val = NULL;
    n_keys = 0;

    bpos = 0;
    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n_keys);
    if (i<0) goto fail;
    bpos += i;
    fsize -= i;

    log_printf(5, "n_keys=" I64T "\n", n_keys);
    tbx_type_malloc_clear(key, char *, n_keys);
    tbx_type_malloc_clear(val, char *, n_keys);
    tbx_type_malloc(v_size, int, n_keys);

    for (i=0; i<n_keys; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key[i], &(data[bpos]), v);
        key[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key=%s bpos=%d\n", i, key[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        v_size[i] = v;
        if (v > 0) {
            tbx_type_malloc(val[i], char, v+1);
            if (v > fsize) goto fail;
            memcpy(val[i], &(data[bpos]), v);
            val[i][v] = 0;
            bpos += v;
            fsize -= nbytes;
        } else {
            val[i] = NULL;
        }
        log_printf(5, "i=%d val=%s bpos=%d\n", i, val[i], bpos);

        log_printf(5, "i=%d v_size=%d bpos=%d\n", i, v_size[i], bpos);
    }

    if (creds != NULL) {
        status = gop_sync_exec_status(os_create_object_with_attrs(osrs->os_child, creds, name, ftype, id, key, (void **)val, v_size, n_keys));
//        if (id != NULL) free(id);
    } else {
        status = bad_creds_status;
    }

fail:
    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);
    gop_mq_frame_destroy(f);
    gop_mq_frame_destroy(fdata);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    if (key) {
        for (i=0; i<n_keys; i++) if (key[i]) free(key[i]);
        free(key);
    }

    if (val) {
        for (i=0; i<n_keys; i++) if (val[i]) free(val[i]);
        free(val);
    }

    if (v_size) free(v_size);

    log_printf(5, "END\n");
}

//***********************************************************************
// osrs_remove_object_cb - Processes the object remove command
//***********************************************************************

void osrs_remove_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fname, *fcred;
    char *name;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");
    OSRS_DEBUG_NOTIFY("REMOVE: mq_count=" LU " START\n", task->uuid);

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fname = mq_msg_pop(msg);  //** This has the filename
    gop_mq_get_frame(fname, (void **)&name, &fsize);

    if (creds != NULL) {
        gop = os_remove_object(osrs->os_child, creds, name);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fname);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    OSRS_DEBUG_NOTIFY("REMOVE: mq_count=" LU " END\n", task->uuid);
}

//***********************************************************************
// osrs_remove_regex_object_cb - Processes the regex based object remove command
//***********************************************************************

void osrs_remove_regex_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *hid;
    unsigned char *buffer;
    unsigned char tbuf[32];
    lio_os_regex_table_t *path, *object_regex;
    lio_creds_t *creds;
    int fsize, bpos, n;
    int64_t recurse_depth, obj_types, timeout, len, hb_timeout, loop;
    mq_msg_t *msg;
    apr_time_t expire;
    gop_op_generic_t *g, *gop;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;
    spin_hb_t spin;

    log_printf(5, "Processing incoming request\n");

    status = gop_failure_status;
    memset(&spin, 0, sizeof(spin));
    mqs = NULL;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fdata = mq_msg_pop(msg);  //** This has the data
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);

    //** Parse the buffer
    path = NULL;
    object_regex = NULL;
    bpos = 0;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
    if (n < 0) {
        timeout = 60;
        goto fail;
    }
    bpos += n;

    //** Get the spin heartbeat handle ID
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
    if (n < 0)  goto fail;
    bpos += n;

    if ((bpos+len) > fsize) goto fail;
    tbx_type_malloc(spin.key, char, len+1);
    memcpy(spin.key, &(buffer[bpos]), len);
    spin.key[len] = 0;
    spin.key_len = len;
    spin.last_hb = apr_time_now();
    bpos += len;
    apr_thread_mutex_lock(osrs->lock);
    apr_hash_set(osrs->spin, spin.key, spin.key_len, &spin);
    apr_thread_mutex_unlock(osrs->lock);

    //** Spin Heartbeat timeout
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &hb_timeout);
    if (n < 0) goto fail;
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
    if (n < 0) goto fail;
    bpos += n;

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, (recurse_depth>0) ? 1 : 0);

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
    if (n < 0) goto fail;
    bpos += n;

    path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;

    //** run the task
    if (creds != NULL) {
        gop = os_remove_regex_object(osrs->os_child, creds, path, object_regex, obj_types, recurse_depth);

        loop = 0;
        while ((g = gop_waitany_timed(gop, 1)) == NULL) {
            if ((loop%10) == 0) osrs_update_active_table(os, hid);
            loop++;

            expire = apr_time_now() - apr_time_from_sec(hb_timeout);
            apr_thread_mutex_lock(osrs->lock);
            n = ((expire > spin.last_hb) || (spin.abort > 0)) ? 1 : 0;
            log_printf(5, "n=%d spin.abort=%d expire=" TT " spin.last_hb=" TT "\n", n, spin.abort, expire, spin.last_hb);
            apr_thread_mutex_unlock(osrs->lock);

            if (n == 1) { //** Kill the gop
                log_printf(1, "Aborting gop=%d\n", gop_id(spin.gop));
                g = os_abort_remove_regex_object(osrs->os_child, gop);
                gop_waitall(g);
                gop_free(g, OP_DESTROY);
                break;
            }
        }

        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

fail:
    if (spin.key != NULL) {
        apr_thread_mutex_lock(osrs->lock);
        apr_hash_set(osrs->spin, spin.key, spin.key_len, NULL);
        free(spin.key);
        if (spin.gop != NULL) gop_free(spin.gop, OP_DESTROY);
        apr_thread_mutex_unlock(osrs->lock);
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);

    if (path != NULL) lio_os_regex_table_destroy(path);
    if (object_regex != NULL) lio_os_regex_table_destroy(object_regex);

    //** Send the response
    if (mqs == NULL) mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);
    gop_mq_stream_destroy(mqs);
}

//***********************************************************************
// osrs_abort_remove_regex_object_cb - Aborts a bulk remove command
//***********************************************************************

void osrs_abort_remove_regex_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fcred, *fspin, *fid;
    char *spin_hb;
    spin_hb_t *spin;
    lio_creds_t *creds;
    int fsize;
    gop_op_status_t status;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;

    log_printf(5, "Processing incoming request\n");

    status = gop_failure_status;  //** Store a default response

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    gop_mq_frame_destroy(mq_msg_pop(msg));  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    fid = mq_msg_pop(msg);  //** Host/user ID

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fspin = mq_msg_pop(msg);  //** This has the Spin ID
    gop_mq_get_frame(fspin, (void **)&spin_hb, &fsize);

    //** Now check if the handle is valid
    apr_thread_mutex_lock(osrs->lock);
    if ((spin = apr_hash_get(osrs->spin, spin_hb, fsize)) != NULL) {
        gop = os_abort_remove_regex_object(osrs->os_child, spin->gop);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        log_printf(5, "Invalid handle!\n");
    }
    apr_thread_mutex_unlock(osrs->lock);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fspin);
    gop_mq_frame_destroy(fcred);


    log_printf(5, "END\n");
}


//***********************************************************************
// osrs_symlink_object_cb - Processes the symlink object command
//***********************************************************************

void osrs_symlink_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fsname, *fdname, *fcred, *fuserid;
    char *src_name, *dest_name, *userid;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fsname = mq_msg_pop(msg);  //** Source file
    gop_mq_get_frame(fsname, (void **)&src_name, &fsize);

    fdname = mq_msg_pop(msg);  //** Destination file
    gop_mq_get_frame(fdname, (void **)&dest_name, &fsize);

    fuserid = mq_msg_pop(msg);  //** User ID

    if (creds != NULL) {
        userid = gop_mq_frame_strdup(fuserid);
        gop = os_symlink_object(osrs->os_child, creds, src_name, dest_name, userid);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
        if (userid != NULL) free(userid);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fsname);
    gop_mq_frame_destroy(fdname);
    gop_mq_frame_destroy(fuserid);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_hardlink_object_cb - Processes the hard link object command
//***********************************************************************

void osrs_hardlink_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fsname, *fdname, *fcred, *fuserid;
    char *src_name, *dest_name, *userid;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fsname = mq_msg_pop(msg);  //** Source file
    gop_mq_get_frame(fsname, (void **)&src_name, &fsize);

    fdname = mq_msg_pop(msg);  //** Destination file
    gop_mq_get_frame(fdname, (void **)&dest_name, &fsize);

    fuserid = mq_msg_pop(msg);  //** User ID

    if (creds != NULL) {
        userid = gop_mq_frame_strdup(fuserid);
        gop = os_hardlink_object(osrs->os_child, creds, src_name, dest_name, userid);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
        if (userid != NULL) free(userid);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fsname);
    gop_mq_frame_destroy(fdname);
    gop_mq_frame_destroy(fuserid);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_move_object_cb - Processes the move object command
//***********************************************************************

void osrs_move_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fsname, *fdname, *fcred;
    char *src_name, *dest_name;
    lio_creds_t *creds;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fsname = mq_msg_pop(msg);  //** Source file
    gop_mq_get_frame(fsname, (void **)&src_name, &fsize);

    fdname = mq_msg_pop(msg);  //** Destination file
    gop_mq_get_frame(fdname, (void **)&dest_name, &fsize);

    if (creds != NULL) {
        gop = os_move_object(osrs->os_child, creds, src_name, dest_name);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fsname);
    gop_mq_frame_destroy(fdname);
    gop_mq_frame_destroy(fcred);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}


//***********************************************************************
// osrs_open_object_cb - Processes the object open command
//***********************************************************************

void osrs_open_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fsname, *fmode, *fcred, *fhandle, *fhb, *fuid;
    char *src_name, *id, *handle;
    osrs_abort_handle_t ah;
    gop_mq_ongoing_object_t *oo;
    unsigned char *data;
    lio_creds_t *creds;
    intptr_t rkey;
    int fsize, handle_len, n;
    int64_t mode, max_wait;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    os_fd_t *fd;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** User ID for storing in lock attribute

    fsname = mq_msg_pop(msg);  //** Source file
    gop_mq_get_frame(fsname, (void **)&src_name, &fsize);

    fmode = mq_msg_pop(msg);  //** Mode and max wait
    gop_mq_get_frame(fmode, (void **)&data, &fsize);
    n = tbx_zigzag_decode(data, fsize, &mode);
    tbx_zigzag_decode(&(data[n]), fsize, &max_wait);
    log_printf(5, "fname=%s mode=%" PRId64 " max_wait=%" PRId64 "\n", src_name, mode, max_wait);

    OSRS_DEBUG_NOTIFY("OBJECT_OPEN: mq_count=" LU " fname=%s START\n", task->uuid, src_name);

    fhb = mq_msg_pop(msg);  //** Heartbeat frame on success
    fhandle = mq_msg_pop(msg);  //** Handle for aborts
    if (creds != NULL) {
        gop_mq_get_frame(fhandle, (void **)&(ah.handle), &n);
        ah.handle_len = n;
        id = gop_mq_frame_strdup(fuid);
        OSRS_LONG_RUNNING(osrs->server_portal, mode);
        ah.gop = os_open_object(osrs->os_child, creds, src_name, mode, id, &fd, max_wait);
        osrs_add_abort_handle(os, &ah);  //** Add us to the abort list

        status = gop_sync_exec_status(ah.gop);
        osrs_remove_abort_handle(os, &ah);  //** Can remove us now since finished

        if (id != NULL) free(id);
    } else {
        status = bad_creds_status;
    }

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));

    //** On success add us to the ongoing monitor thread and return the handle
    if (status.op_status == OP_STATE_SUCCESS) {
        handle = NULL;
        gop_mq_get_frame(fhb, (void **)&handle, &handle_len);
        log_printf(5, "handle=%s\n", handle);
        log_printf(5, "handle_len=%d\n", handle_len);
        tbx_random_get_bytes(&rkey, sizeof(rkey));
        oo = gop_mq_ongoing_add(osrs->ongoing, 1, handle, handle_len, rkey, (void *)fd, (gop_mq_ongoing_fail_fn_t)osrs->os_child->close_object, osrs->os_child, close_can_fail_check, os);

        n=sizeof(intptr_t);
        log_printf(5, "PTR key=%" PRIdPTR " len=%d\n", oo->key, n);
        OSRS_DEBUG_NOTIFY("OBJECT_OPEN: mq_count=" LU " key=%" PRIdPTR " fname=%s\n", task->uuid, oo->key, src_name);
        gop_mq_msg_append_mem(response, &(oo->key), sizeof(oo->key), MQF_MSG_KEEP_DATA);
    }

    //** Do some house cleaning
    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fhb);
    gop_mq_frame_destroy(fsname);
    gop_mq_frame_destroy(fcred);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fmode);
    gop_mq_frame_destroy(fhandle);

    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    OSRS_DEBUG_NOTIFY("OBJECT_OPEN: mq_count=" LU " END\n", task->uuid);
}

//***********************************************************************
// osrs_close_object_cb - Processes an object close
//***********************************************************************

void osrs_close_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_op_generic_t *gop;
    gop_mq_frame_t *fid, *fuid, *fhid;
    char *id, *fhandle;
    void *handle;
    int fsize, hsize;
    intptr_t key;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    fhid = mq_msg_pop(msg);  //** Host handle
    gop_mq_get_frame(fhid, (void **)&fhandle, &hsize);
   FATAL_UNLESS(hsize == sizeof(intptr_t));

    key = *(intptr_t *)fhandle;
    log_printf(5, "PTR key=%" PRIdPTR "\n", key);

    OSRS_DEBUG_NOTIFY("OBJECT_CLOSE: mq_count=" LU " START key=%" PRIdPTR "\n", task->uuid, key);

    //** Do the host lookup
    if ((handle = gop_mq_ongoing_remove(osrs->ongoing, id, fsize, key, 1)) != NULL) {
        log_printf(6, "Found handle: handle=%p\n", handle);

        gop = os_close_object(osrs->os_child, handle);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        log_printf(6, "ERROR missing host=%s\n", id);
        if (os_notify_handle) tbx_notify_printf(os_notify_handle, 1, NULL, "osrs_close_object_cb: ERROR no matching ongoing object! host=%s\n", id);
        status = gop_failure_status;
    }

    gop_mq_frame_destroy(fhid);
    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    OSRS_DEBUG_NOTIFY("OBJECT_CLOSE: mq_count=" LU " END key=%" PRIdPTR "\n", task->uuid, key);
}

//***********************************************************************
// osrs_abort_open_object_cb - Aborts a pending open object call
//***********************************************************************

void osrs_abort_open_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid;
    char *id;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    //** Perform the abort
    status = osrs_perform_abort_handle(os, id, fsize);

    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_lock_user_object_cb - Processes a user lock object request
//***********************************************************************

void osrs_lock_user_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fhid, *fdata, *fabort;
    gop_mq_ongoing_handle_t ohandle;
    osrs_abort_handle_t ah;
    gop_op_generic_t *gop;
    char *id, *fhandle;
    unsigned char *data;
    os_fd_t *fd;
    int fsize, hsize, idsize;
    int i, bpos;
    int64_t  rw_mode, timeout;
    intptr_t key;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    pending_lock_entry_t *entry;

    log_printf(5, "Processing incoming request\n");
    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &idsize);

    fabort = mq_msg_pop(msg);  //** Abort handle
    gop_mq_get_frame(fabort, (void **)&(ah.handle), &hsize);
    ah.handle_len = hsize;

    fhid = mq_msg_pop(msg);  //** Host handle
    gop_mq_get_frame(fhid, (void **)&fhandle, &hsize);
    FATAL_UNLESS(hsize == sizeof(intptr_t));

    key = *(intptr_t *)fhandle;
    log_printf(5, "PTR key=%" PRIdPTR "\n", key);
    OSRS_DEBUG_NOTIFY("FLOCK: mq_count=" LU " START key=%" PRIdPTR "\n", task->uuid, key);

    //** Do the host lookup
    if ((fd = gop_mq_ongoing_get(osrs->ongoing, id, idsize, key, &ohandle)) == NULL) {
        log_printf(6, "ERROR missing host=%s\n", id);
        if (os_notify_handle) tbx_notify_printf(os_notify_handle, 1, NULL, "ERROR missing host=%s\n", id);
        status = gop_failure_status;
    } else {
        log_printf(6, "Found handle\n");

        fdata = mq_msg_pop(msg);  //** Get the mode and wait time
        gop_mq_get_frame(fdata, (void **)&data, &fsize);

        //** Parse the mode and wait time
        bpos = 0;
        rw_mode = 0;
        i = tbx_zigzag_decode(&(data[bpos]), fsize, &rw_mode);

        if ((i<0) || (rw_mode<=0)) goto fail_fd;
        bpos += i;
        fsize -= i;

        timeout = 0;
        i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
        if (i<0) goto fail_fd;
        if (timeout < 0) timeout = 10;
        bpos += i;
        fsize -= i;

        OSRS_LONG_RUNNING(osrs->server_portal, rw_mode);
        gop = os_lock_user_object(osrs->os_child, fd, rw_mode, timeout);
        ah.gop = gop;
        osrs_add_abort_handle(os, &ah);
        entry = pending_lock_inc(os, fd);  //** We track pending locks in case of a dead client to properly order the FD closing
        status = gop_sync_exec_status(gop);
        pending_lock_dec(os, entry);
        osrs_remove_abort_handle(os, &ah);
        gop_mq_ongoing_release(osrs->ongoing, &ohandle);

fail_fd:
        gop_mq_frame_destroy(fdata);
    }

    gop_mq_frame_destroy(fhid);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fabort);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    OSRS_DEBUG_NOTIFY("FLOCK: mq_count=" LU " END key=%" PRIdPTR "\n", task->uuid, key);
}


//***********************************************************************
// osrs_perform_abort_lock_user_handle - Performs the actual abort
//***********************************************************************

gop_op_status_t osrs_perform_abort_lock_user_handle(lio_object_service_fn_t *os, char *handle, int handle_len)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    osrs_abort_handle_t *ah;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    status = gop_failure_status;

    apr_thread_mutex_lock(osrs->abort_lock);
    ah = apr_hash_get(osrs->abort, handle, handle_len);

    if (ah != NULL) {
        gop = os_abort_lock_user_object(osrs->os_child, ah->gop);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    }
    apr_thread_mutex_unlock(osrs->abort_lock);

    return(status);
}

//***********************************************************************
// osrs_abort_lock_user_object_cb - Aborts a pending user lock object call
//***********************************************************************

void osrs_abort_lock_user_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid;
    char *id;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    //** Perform the abort
    status = osrs_perform_abort_lock_user_handle(os, id, fsize);

    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// osrs_get_mult_attr_fn - Retrieves object attributes
//***********************************************************************

void osrs_get_mult_attr_fn(void *arg, gop_mq_task_t *task, int is_immediate)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd, *hid;
    lio_creds_t *creds;
    char *id, *path;
    unsigned char *data;
    gop_mq_ongoing_handle_t ohandle;
    int fsize, bpos, len, id_size, i;
    int64_t max_stream, timeout, n, v, nbytes;
    mq_msg_t *msg;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;
    unsigned char buffer[32];
    char **key;
    void **val;
    int *v_size;
    os_fd_t *fd;
    intptr_t fd_key;

    log_printf(5, "Processing incoming request\n");

    mqs = NULL;
    key = NULL;
    val = NULL;
    v_size = NULL;
    path = NULL;
    fd = NULL;
    fd_key = 0;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    //** Get the fd handle
    ffd = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd, (void **)&data, &len);
    if (is_immediate) {
        path = (char *)data;
        if (len > 0) {
            if (path[len-1] != 0) path[len-1] = 0;  //** Make sure it's NULL terminated
        } else {
            if (os_notify_handle) tbx_notify_printf(os_notify_handle, 1, NULL, "osrs_get_mult_attr_fn: ERROR path_len=%d! host=%s\n", len, id);
        }
    } else {
        fd_key = *(intptr_t *)data;
    }

    fdata = mq_msg_pop(msg);  //** attr list
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    //** Now check if the handle is valid
    if (!is_immediate) {
        if ((fd = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key, &ohandle)) == NULL) {
            log_printf(5, "Invalid handle!\n");
            goto fail_fd;
        }
    }

    //** Parse the attr list
    i = tbx_zigzag_decode(data, fsize, &max_stream);
    if (i<0) goto fail_fd;
    if ((max_stream <= 0) || (max_stream > osrs->max_stream)) max_stream = osrs->max_stream;
    bpos = i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
    if (i<0) goto fail_fd;
    if (timeout < 0) timeout = 10;
    bpos += i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n);
    if ((i<0) || (n<=0)) goto fail_fd;
    bpos += i;
    fsize -= i;

    log_printf(5, "max_stream=%" PRId64 " timeout=%" PRId64 " n=%" PRId64 "\n", max_stream, timeout, n);
    tbx_type_malloc_clear(key, char *, n);
    tbx_type_malloc_clear(val, void *, n);
    tbx_type_malloc(v_size, int, n);

    for (i=0; i<n; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key[i], &(data[bpos]), v);
        key[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key=%s bpos=%d\n", i, key[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;
        v_size[i] = -llabs(v);
        log_printf(5, "i=%d v_size=" XOT " bpos=%d\n", i, v, bpos);
    }

    //** Execute the get attribute call
    if (creds != NULL) {
        if (is_immediate) {
            status = gop_sync_exec_status(os_get_multiple_attrs_immediate(osrs->os_child, creds, path, key, (void **)val, v_size, n));
        } else {
            status = gop_sync_exec_status(os_get_multiple_attrs(osrs->os_child, creds, fd, key, (void **)val, v_size, n));
        }
    } else {
        status = bad_creds_status;
    }

    //** Create the stream
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, max_stream, timeout, msg, fid, hid, 0);
    osrs_update_active_table(os, hid);  //** Update the active log

    //** Return the results
    i = tbx_zigzag_encode(status.op_status, buffer);
    i = i + tbx_zigzag_encode(status.error_code, &(buffer[i]));
    gop_mq_stream_write(mqs, buffer, i);

    log_printf(5, "status.op_status=%d status.error_code=%d len=%d\n", status.op_status, status.error_code, i);
    if (status.op_status == OP_STATE_SUCCESS) {
        for (i=0; i<n; i++) {
            gop_mq_stream_write_varint(mqs, v_size[i]);
            if (v_size[i] > 0) {
                gop_mq_stream_write(mqs, val[i], v_size[i]);
            }
            if (v_size[i] > 0) {
                log_printf(15, "val[%d]=%s\n", i, (char *)val[i]);
            } else {
                log_printf(15, "val[%d]=NULL\n", i);
            }
        }
    }

fail_fd:
fail:
    if (fd != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle);
    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(ffd);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);

    if (mqs != NULL) {
        gop_mq_stream_destroy(mqs);  //** This also flushes the data to the client
    } else {  //** there was an error processing the record
        log_printf(5, "ERROR status being returned!\n");
        mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_RAW, 1024, 30, msg, fid, hid, 0);
        status = gop_failure_status;
        i = tbx_zigzag_encode(status.op_status, buffer);
        i = i + tbx_zigzag_encode(status.error_code, &(buffer[i]));
        gop_mq_stream_write(mqs, buffer, i);
        gop_mq_stream_destroy(mqs);
    }

    if (key) {
        for (i=0; i<n; i++) if (key[i]) free(key[i]);
        free(key);
    }

    if (val) {
        for (i=0; i<n; i++) if (val[i]) free(val[i]);
        free(val);
    }

    if (v_size) free(v_size);
}

//***********************************************************************
// osrs_get_mult_attr_cb - Retrieves object attributes
//***********************************************************************

void osrs_get_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;

    OSRS_DEBUG_NOTIFY("OS_GET_MULT_ATTR: mq_count=" LU " START\n", task->uuid);

    osrs_get_mult_attr_fn(os, task, 0);

    OSRS_DEBUG_NOTIFY("OS_GET_MULT_ATTR: mq_count=" LU " END\n", task->uuid);

}

//***********************************************************************
// osrs_get_mult_attr_immediate_cb - Retrieves object attributes in immediate mode
//***********************************************************************

void osrs_get_mult_attr_immediate_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;

    osrs_get_mult_attr_fn(os, task, 1);
}

//***********************************************************************
// osrs_set_mult_attr_fn - Sets the given object attributes
//***********************************************************************

void osrs_set_mult_attr_fn(lio_object_service_fn_t *os, gop_mq_task_t *task, int is_immediate)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd;
    lio_creds_t *creds;
    char *id;
    unsigned char *data;
    gop_mq_ongoing_handle_t ohandle;
    int fsize, bpos, len, id_size, i;
    int64_t timeout, n, v, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    char *path;
    char **key;
    char **val;
    int *v_size;
    os_fd_t *fd;
    intptr_t fd_key;

    log_printf(5, "Processing incoming request\n");

    key = NULL;
    val = NULL;
    v_size = NULL;
    path = NULL;
    fd = NULL;
    fd_key = 0;
    status = gop_failure_status;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    //** Get the fd handle or the fname depending on the mode
    ffd = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd, (void **)&data, &len);
    if (is_immediate) {
        path = (char *)data;
        if (len > 0) {
            if (path[len-1] != 0) path[len-1] = 0;  //** Make sure it's NULL terminated
        } else {
            if (os_notify_handle) tbx_notify_printf(os_notify_handle, 1, NULL, "osrs_set_mult_attr_fn: ERROR path_len=%d! host=%s\n", len, id);
        }
    } else {
        fd_key = *(intptr_t *)data;
    }

    fdata = mq_msg_pop(msg);  //** attr list to set
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    //** Now check if the handle is valid
    if (!is_immediate) {
        if ((fd = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key, &ohandle)) == NULL) {
          log_printf(5, "Invalid handle!\n");
            goto fail_fd;
        }
    }

    //** Parse the attr list
    bpos = 0;
    i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
    if (i<0) goto fail_fd;
    if (timeout < 0) timeout = 10;
    bpos += i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n);
    if ((i<0) || (n<=0)) goto fail_fd;
    bpos += i;
    fsize -= i;

    log_printf(5, "timeout=%" PRId64 " n=%" PRId64 "\n", timeout, n);
    tbx_type_malloc_clear(key, char *, n);
    tbx_type_malloc_clear(val, char *, n);
    tbx_type_malloc(v_size, int, n);

    for (i=0; i<n; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key[i], &(data[bpos]), v);
        key[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key=%s bpos=%d\n", i, key[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        v_size[i] = v;
        if (v > 0) {
            tbx_type_malloc(val[i], char, v+1);
            if (v > fsize) goto fail;
            memcpy(val[i], &(data[bpos]), v);
            val[i][v] = 0;
            bpos += v;
            fsize -= nbytes;
        } else {
            val[i] = NULL;
        }
        log_printf(5, "i=%d val=%s bpos=%d\n", i, val[i], bpos);

        log_printf(5, "i=%d v_size=%d bpos=%d\n", i, v_size[i], bpos);
    }

    //** Execute the get attribute call
    if (creds != NULL) {
        if (is_immediate) {
            status = gop_sync_exec_status(os_set_multiple_attrs_immediate(osrs->os_child, creds, path, key, (void **)val, v_size, n));
        } else {
            status = gop_sync_exec_status(os_set_multiple_attrs(osrs->os_child, creds, fd, key, (void **)val, v_size, n));
        }
    } else {
        status = bad_creds_status;
    }

fail_fd:
fail:
    if (fd != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(ffd);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);


    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    if (key) {
        for (i=0; i<n; i++) if (key[i]) free(key[i]);
        free(key);
    }

    if (val) {
        for (i=0; i<n; i++) if (val[i]) free(val[i]);
        free(val);
    }

    if (v_size) free(v_size);
}


//***********************************************************************
// osrs_set_mult_attr_cb - Sets the given object attributes
//***********************************************************************

void osrs_set_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;

    osrs_set_mult_attr_fn(os, task, 0);
}

//***********************************************************************
// osrs_set_mult_attr_immediate_cb - Sets the given object attributes
//***********************************************************************

void osrs_set_mult_attr_immediate_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;

    osrs_set_mult_attr_fn(os, task, 1);
}


//***********************************************************************
// osrs_abort_regex_set_muylt_attr_cb - Aborts a bulk set attr command
//***********************************************************************

void osrs_abort_regex_set_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fcred, *fspin, *fid;
    char *spin_hb;
    spin_hb_t *spin;
    lio_creds_t *creds;
    int fsize;
    gop_op_status_t status;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;

    log_printf(5, "Processing incoming request\n");

    status = gop_failure_status;  //** Store a default response

    //** Parse the command. Don't have to
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    gop_mq_frame_destroy(mq_msg_pop(msg));  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    fid = mq_msg_pop(msg);  //** Host/user ID

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fspin = mq_msg_pop(msg);  //** This has the Spin ID
    gop_mq_get_frame(fspin, (void **)&spin_hb, &fsize);

    //** Now check if the handle is valid
    apr_thread_mutex_lock(osrs->lock);
    if ((spin = apr_hash_get(osrs->spin, spin_hb, fsize)) != NULL) {
        gop = os_abort_regex_object_set_multiple_attrs(osrs->os_child, spin->gop);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        log_printf(5, "Invalid handle!\n");
    }
    apr_thread_mutex_unlock(osrs->lock);

    if (!creds) { status = bad_creds_status; }

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fspin);
    gop_mq_frame_destroy(fcred);


    log_printf(5, "END\n");
}

//***********************************************************************
// osrs_regex_set_set_mult_attr_cb - Processes the regex based object attrribute setting
//***********************************************************************

void osrs_regex_set_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *fcid, *hid;
    unsigned char *buffer;
    unsigned char tbuf[32];
    int *v_size;
    char **key;
    char **val;
    char *call_id;
    spin_hb_t spin;
    apr_time_t expire;
    lio_os_regex_table_t *path, *object_regex;
    lio_creds_t *creds;
    int fsize, bpos, n, i;
    int64_t recurse_depth, obj_types, timeout, hb_timeout, n_attrs, len, loop;
    mq_msg_t *msg;
    gop_op_generic_t *g;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    status = gop_failure_status;
    memset(&spin, 0, sizeof(spin));
    key = NULL;
    val = NULL, v_size = NULL;
    mqs = NULL;
    n_attrs = 0;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fcid = mq_msg_pop(msg);  //** This has the call ID
    call_id = gop_mq_frame_strdup(fcid);

    fdata = mq_msg_pop(msg);  //** This has the data
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);

    //** Parse the buffer
    path = NULL;
    object_regex = NULL;
    bpos = 0;

    //** Get the timeout
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
    if (n < 0) {
        timeout = 60;
        goto fail;
    }
    bpos += n;

    //** Get the spin heartbeat handle ID
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
    if (n < 0)  goto fail;
    bpos += n;

    if ((bpos+len) > fsize) goto fail;
    tbx_type_malloc(spin.key, char, len+1);
    memcpy(spin.key, &(buffer[bpos]), len);
    spin.key[len] = 0;
    spin.key_len = len;
    spin.last_hb = apr_time_now();
    bpos += len;
    apr_thread_mutex_lock(osrs->lock);
    apr_hash_set(osrs->spin, spin.key, spin.key_len, &spin);
    apr_thread_mutex_unlock(osrs->lock);

    //** Spin Heartbeat timeout
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &hb_timeout);
    if (n < 0) goto fail;
    bpos += n;

    //** Get the other params
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
    if (n < 0) goto fail;
    bpos += n;

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, (recurse_depth > 0) ? 1 : 0);

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
    if (n < 0) goto fail;
    bpos += n;

    path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &n_attrs);
    if (n < 0) goto fail;
    bpos += n;

    tbx_type_malloc_clear(key, char *, n_attrs);
    tbx_type_malloc_clear(val, char *, n_attrs);
    tbx_type_malloc_clear(v_size, int, n_attrs);

    for (i=0; i<n_attrs; i++) {
        n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
        if (n < 0)  goto fail;
        bpos += n;

        if ((bpos+len) > fsize) goto fail;
        tbx_type_malloc(key[i], char, len+1);
        memcpy(key[i], &(buffer[bpos]), len);
        key[i][len] = 0;
        bpos += len;

        n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
        if (n < 0)  goto fail;
        bpos += n;

        v_size[i] = len;
        if ((len > 0) && ((bpos+len) > fsize)) goto fail;
        if (len > 0) {
            tbx_type_malloc(val[i], char, len+1);
            memcpy(val[i], &(buffer[bpos]), len);
            val[i][len] = 0;
        } else {
            val[i] = NULL;
        }
        bpos += len;
        log_printf(15, "i=%d key=%s val=%s bpos=%d\n", i, key[i], val[i], bpos);
    }

    log_printf(5, "hb_abort_timeout=%" PRId64 "\n", hb_timeout);

    //** run the task
    if (creds != NULL) {
        spin.gop = os_regex_object_set_multiple_attrs(osrs->os_child, creds, call_id, path, object_regex, obj_types, recurse_depth, key, (void **)val, v_size, n_attrs);

        loop= 0;
        while ((g = gop_waitany_timed(spin.gop, 1)) == NULL) {
            if ((loop%10) == 0) osrs_update_active_table(os, hid);
            loop++;

            expire = apr_time_now() - apr_time_from_sec(hb_timeout);
            apr_thread_mutex_lock(osrs->lock);
            n = ((expire > spin.last_hb) || (spin.abort > 0)) ? 1 : 0;
            log_printf(5, "n=%d spin.abort=%d expire=" TT " spin.last_hb=" TT "\n", n, spin.abort, expire, spin.last_hb);
            apr_thread_mutex_unlock(osrs->lock);

            if (n == 1) { //** Kill the gop
                log_printf(1, "Aborting gop=%d\n", gop_id(spin.gop));
                g = os_abort_regex_object_set_multiple_attrs(osrs->os_child, spin.gop);
                gop_waitall(g);
                gop_free(g, OP_DESTROY);
                break;
            }
        }

        gop_waitall(spin.gop);
        status = gop_get_status(spin.gop);
    } else {
        status = bad_creds_status;
    }

fail:
    if (spin.key != NULL) {
        apr_thread_mutex_lock(osrs->lock);
        apr_hash_set(osrs->spin, spin.key, spin.key_len, NULL);
        free(spin.key);
        if (spin.gop != NULL) gop_free(spin.gop, OP_DESTROY);
        apr_thread_mutex_unlock(osrs->lock);
    }

    if (call_id != NULL) free(call_id);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);
    gop_mq_frame_destroy(fcid);

    if (path != NULL) lio_os_regex_table_destroy(path);
    if (object_regex != NULL) lio_os_regex_table_destroy(object_regex);

    if (key != NULL) {
        for (i=0; i<n_attrs; i++) {
            if (key[i] != NULL) free(key[i]);
            if (val[i] != NULL) free(val[i]);
        }
        free(key);
        free(val);
        free(v_size);
    }

    log_printf(5, "END status=%d n_errs=%d\n", status.op_status, status.error_code);
    //** Send the response
    if (mqs == NULL) mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);
    gop_mq_stream_destroy(mqs);
}

//***********************************************************************
// osrs_copy_mult_attr_cb - Copies multiple attributes between objects
//***********************************************************************

void osrs_copy_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_src, *ffd_dest;
    lio_creds_t *creds;
    char *id;
    unsigned char *data;
    gop_op_generic_t *gop;
    int fsize, bpos, len, id_size, i;
    int64_t timeout, n, v, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    char **key_src;
    char **key_dest;
    os_fd_t *fd_src, *fd_dest;
    gop_mq_ongoing_handle_t ohandle_src;
    gop_mq_ongoing_handle_t ohandle_dest;
    intptr_t fd_key_src, fd_key_dest;

    log_printf(5, "Processing incoming request\n");

    fd_src = NULL;
    fd_dest = NULL;
    key_src = NULL;
    key_dest = NULL;
    status = gop_failure_status;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    //** Get the fd handles
    ffd_src = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd_src, (void **)&data, &len);
    fd_key_src = *(intptr_t *)data;

    ffd_dest = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd_dest, (void **)&data, &len);
    fd_key_dest = *(intptr_t *)data;

    log_printf(5, "PTR key_src=%" PRIdPTR " len=%d\n", fd_key_src, len);

    fdata = mq_msg_pop(msg);  //** attr list to set
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    log_printf(5, "PTR key_src=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_src, len, id, id_size);

    //** Now check if the handles are valid
    if ((fd_src = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_src, &ohandle_src)) == NULL) {
        log_printf(5, "Invalid SOURCE handle!\n");
        goto fail_fd;
    }
    if ((fd_dest = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_dest, &ohandle_dest)) == NULL) {
        log_printf(5, "Invalid DEST handle!\n");
        goto fail_fd;
    }

    //** Parse the attr list
    bpos = 0;
    i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
    if (i<0) goto fail_fd;
    if (timeout < 0) timeout = 10;
    bpos += i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n);
    if ((i<0) || (n<=0)) goto fail_fd;
    bpos += i;
    fsize -= i;

    log_printf(5, "timeout=%" PRId64 " n=%" PRId64 "\n", timeout, n);
    tbx_type_malloc_clear(key_src, char *, n);
    tbx_type_malloc_clear(key_dest, char *, n);

    for (i=0; i<n; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key_src[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_src[i], &(data[bpos]), v);
        key_src[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;


        tbx_type_malloc(key_dest[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_dest[i], &(data[bpos]), v);
        key_dest[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
    }

    //** Execute the get attribute call
    if (creds != NULL) {
        gop = os_copy_multiple_attrs(osrs->os_child, creds, fd_src, key_src, fd_dest, key_dest, n);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

fail_fd:
fail:

    if (fd_src != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle_src);
    if (fd_dest != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle_dest);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(ffd_src);
    gop_mq_frame_destroy(ffd_dest);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);


    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    if (key_src) {
        for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
        free(key_src);
    }

    if (key_dest) {
        for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
        free(key_dest);
    }
}


//***********************************************************************
// osrs_move_mult_attr_cb - Moves multiple attributes
//***********************************************************************

void osrs_move_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_src;
    lio_creds_t *creds;
    char *id;
    unsigned char *data;
    gop_op_generic_t *gop;
    gop_mq_ongoing_handle_t ohandle;
    int fsize, bpos, len, id_size, i;
    int64_t timeout, n, v, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    char **key_src;
    char **key_dest;
    os_fd_t *fd_src;
    intptr_t fd_key_src;

    log_printf(5, "Processing incoming request\n");

    key_src = NULL;
    key_dest = NULL;
    status = gop_failure_status;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    //** Get the fd handles
    ffd_src = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd_src, (void **)&data, &len);
    fd_key_src = *(intptr_t *)data;

    log_printf(5, "PTR key_src=%" PRIdPTR " len=%d\n", fd_key_src, len);

    fdata = mq_msg_pop(msg);  //** attr list to set
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    log_printf(5, "PTR key_src=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_src, len, id, id_size);

    //** Now check if the handles are valid
    if ((fd_src = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_src, &ohandle)) == NULL) {
        log_printf(5, "Invalid SOURCE handle!\n");
        goto fail_fd;
    }

    //** Parse the attr list
    bpos = 0;
    i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
    if (i<0) goto fail_fd;
    if (timeout < 0) timeout = 10;
    bpos += i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n);
    if ((i<0) || (n<=0)) goto fail_fd;
    bpos += i;
    fsize -= i;

    log_printf(5, "timeout=%" PRId64 " n=%" PRId64 "\n", timeout, n);
    tbx_type_malloc_clear(key_src, char *, n);
    tbx_type_malloc_clear(key_dest, char *, n);

    for (i=0; i<n; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key_src[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_src[i], &(data[bpos]), v);
        key_src[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;


        tbx_type_malloc(key_dest[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_dest[i], &(data[bpos]), v);
        key_dest[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
    }

    //** Execute the get attribute call
    if (creds != NULL) {
        gop = os_move_multiple_attrs(osrs->os_child, creds, fd_src, key_src, key_dest, n);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

fail_fd:
fail:

    if (fd_src != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(ffd_src);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);


    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    if (key_src) {
        for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
        free(key_src);
    }

    if (key_dest) {
        for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
        free(key_dest);
    }
}

//***********************************************************************
// osrs_symlink_mult_attr_cb - Symlinks multiple attributes between objects
//***********************************************************************

void osrs_symlink_mult_attr_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fuid, *fcred, *fdata, *ffd_dest;
    lio_creds_t *creds;
    char *id;
    unsigned char *data;
    gop_op_generic_t *gop;
    gop_mq_ongoing_handle_t ohandle;
    int fsize, bpos, len, id_size, i;
    int64_t timeout, n, v, nbytes;
    mq_msg_t *msg, *response;
    gop_op_status_t status;
    char **src_path;
    char **key_src;
    char **key_dest;
    os_fd_t *fd_dest;
    intptr_t fd_key_dest;

    log_printf(5, "Processing incoming request\n");

    key_src = NULL;
    key_dest = NULL;
    src_path = NULL;
    status = gop_failure_status;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &id_size);

    //** Get the fd handles
    ffd_dest = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(ffd_dest, (void **)&data, &len);
    fd_key_dest = *(intptr_t *)data;

    log_printf(5, "PTR key_dest=%" PRIdPTR " len=%d\n", fd_key_dest, len);

    fdata = mq_msg_pop(msg);  //** attr list to set
    gop_mq_get_frame(fdata, (void **)&data, &fsize);

    log_printf(5, "PTR key_dest=%" PRIdPTR " len=%d id=%s id_len=%d\n", fd_key_dest, len, id, id_size);

    //** Now check if the handles are valid
    if ((fd_dest = gop_mq_ongoing_get(osrs->ongoing, (char *)id, id_size, fd_key_dest, &ohandle)) == NULL) {
        log_printf(5, "Invalid SOURCE handle!\n");
        goto fail_fd;
    }

    //** Parse the attr list
    bpos = 0;
    i = tbx_zigzag_decode(&(data[bpos]), fsize, &timeout);
    if (i<0) goto fail_fd;
    if (timeout < 0) timeout = 10;
    bpos += i;
    fsize -= i;

    i = tbx_zigzag_decode(&(data[bpos]), fsize, &n);
    if ((i<0) || (n<=0)) goto fail_fd;
    bpos += i;
    fsize -= i;

    log_printf(5, "timeout=%" PRId64 " n=%" PRId64 "\n", timeout, n);
    tbx_type_malloc_clear(key_src, char *, n);
    tbx_type_malloc_clear(key_dest, char *, n);
    tbx_type_malloc_clear(src_path, char *, n);

    for (i=0; i<n; i++) {
        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d slen=" XOT " bpos=%d\n", i, v, bpos);
        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(src_path[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(src_path[i], &(data[bpos]), v);
        src_path[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d src_path=%s bpos=%d\n", i, src_path[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);
        if ((nbytes<0) || (v<=0)) goto fail;
        bpos += nbytes;
        fsize -= nbytes;

        tbx_type_malloc(key_src[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_src[i], &(data[bpos]), v);
        key_src[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_src=%s bpos=%d\n", i, key_src[i], bpos);

        nbytes = tbx_zigzag_decode(&(data[bpos]), fsize, &v);
        log_printf(5, "i=%d klen=" XOT " bpos=%d\n", i, v, bpos);

        if (nbytes<0) goto fail;
        bpos += nbytes;
        fsize -= nbytes;


        tbx_type_malloc(key_dest[i], char, v+1);
        if (v > fsize) goto fail;
        memcpy(key_dest[i], &(data[bpos]), v);
        key_dest[i][v] = 0;
        bpos += v;
        fsize -= nbytes;
        log_printf(5, "i=%d key_dest=%s bpos=%d\n", i, key_dest[i], bpos);
    }

    //** Execute the get attribute call
    if (creds != NULL) {
        gop = os_symlink_multiple_attrs(osrs->os_child, creds, src_path, key_src, fd_dest, key_dest, n);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    } else {
        status = bad_creds_status;
    }

fail_fd:
fail:

    if (fd_dest != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle);

    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(ffd_dest);
    gop_mq_frame_destroy(fuid);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);


    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    log_printf(5, "status.op_status=%d\n", status.op_status);
    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    if (key_src) {
        for (i=0; i<n; i++) if (key_src[i]) free(key_src[i]);
        free(key_src);
    }

    if (key_dest) {
        for (i=0; i<n; i++) if (key_dest[i]) free(key_dest[i]);
        free(key_dest);
    }

    if (src_path) {
        for (i=0; i<n; i++) if (src_path[i]) free(src_path[i]);
        free(src_path);
    }
}

//***********************************************************************
// osrs_object_iter_alist_cb - Handles the alist object iterator
//***********************************************************************

void osrs_object_iter_alist_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *hid;
    unsigned char *buffer;
    unsigned char tbuf[32];
    int *v_size;
    char **key;
    char **val;
    char *fname;
    lio_os_regex_table_t *path, *object_regex;
    lio_creds_t *creds;
    int fsize, bpos, n, i, err, ftype, prefix_len;
    int64_t recurse_depth, obj_types, timeout, n_attrs, len;
    mq_msg_t *msg;
    os_object_iter_t *it;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    key = NULL;
    val = NULL, v_size = NULL;
    n_attrs = 0;
    it = NULL;
    mqs = NULL;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fdata = mq_msg_pop(msg);  //** This has the data
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);

    //** Parse the buffer
    path = NULL;
    object_regex = NULL;
    bpos = 0;

    //** Get the stream timeout
    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
    if (n < 0) {
        timeout = 60;
        goto fail;
    }
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
    if (n < 0) goto fail;
    bpos += n;

    //** Create the stream so we can get the heartbeating while we work.
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, (recurse_depth>0) ? 1 : 0);

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
    if (n < 0) goto fail;
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &n_attrs);
    if (n < 0) goto fail;
    bpos += n;

    tbx_type_malloc_clear(key, char *, n_attrs);
    tbx_type_malloc_clear(val, char *, n_attrs);
    tbx_type_malloc_clear(v_size, int, n_attrs);

    for (i=0; i<n_attrs; i++) {
        n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
        if (n < 0)  goto fail;
        bpos += n;

        if ((bpos+len) > fsize) goto fail;
        tbx_type_malloc(key[i], char, len+1);
        memcpy(key[i], &(buffer[bpos]), len);
        key[i][len] = 0;
        bpos += len;

        n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &len);
        if (n < 0)  goto fail;
        bpos += n;
        v_size[i] = -llabs(len);
        log_printf(15, "i=%d key=%s v_size=%" PRId64 " bpos=%d\n", i, key[i], len, bpos);
    }

    path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if ((n == 0) || (path == NULL)) {
        log_printf(0, "path=NULL\n");
        goto fail;
    }
    bpos += n;

    log_printf(15, "1. bpos=%d fsize=%d\n", bpos, fsize);

    object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) {
        log_printf(0, "object_regex=NULL n=%d", n);
        goto fail;
    }
    bpos += n;

    log_printf(15, "2. bpos=%d fsize=%d\n", bpos, fsize);

    //** run the task
    if (creds != NULL) {
        it = os_create_object_iter_alist(osrs->os_child, creds, path, object_regex, obj_types, recurse_depth, key, (void **)val, v_size, n_attrs);
    } else {
        it = NULL;
    }

fail:
    //** Encode the status
    if (mqs == NULL) mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);
    status = (it != NULL) ? gop_success_status : gop_failure_status;
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);

    //** Check if we kick out due to an error
    if (it == NULL) goto finished;

    //** Pack up the data and send it out
    err = 0;
    while (((ftype = os_next_object(osrs->os_child, it, &fname, &prefix_len)) > 0) && (err == 0)) {
        osrs_update_active_table(os, hid);  //** Update the active log

        len = strlen(fname);
        n = tbx_zigzag_encode(ftype, tbuf);
        n += tbx_zigzag_encode(prefix_len, &(tbuf[n]));
        n += tbx_zigzag_encode(len, &(tbuf[n]));
        err += gop_mq_stream_write(mqs, tbuf, n);
        err += gop_mq_stream_write(mqs, fname, len);

        log_printf(5, "ftype=%d prefix_len=%d len=%" PRId64 " fname=%s n_attrs=%" PRId64 "\n", ftype, prefix_len, len, fname, n_attrs);
        //** Now dump the attributes
        for (i=0; i<n_attrs; i++) {
            n = tbx_zigzag_encode(v_size[i], tbuf);
            err += gop_mq_stream_write(mqs, tbuf, n);
            log_printf(5, "v_size[%d]=%d\n", i, v_size[i]);
            if (v_size[i] > 0) {
                log_printf(5, "val[%d]=%s\n", i, val[i]);
                err += gop_mq_stream_write(mqs, val[i], v_size[i]);
                free(val[i]);
                val[i] = NULL;
            }
        }

        free(fname);

        if (err != 0) break;  //** Got a write error so break;
    }

    //** Flag this as the last object
    n = tbx_zigzag_encode(0, tbuf);
    gop_mq_stream_write(mqs, tbuf, n);

    //** Destroy the object iterator
    os_destroy_object_iter(osrs->os_child, it);

finished:
    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);

    //** Flush the buffer
    gop_mq_stream_destroy(mqs);

    //** Clean up
    if (path != NULL) lio_os_regex_table_destroy(path);
    if (object_regex != NULL) lio_os_regex_table_destroy(object_regex);

    if (key != NULL) {
        for (i=0; i<n_attrs; i++) {
            if (key[i] != NULL) free(key[i]);
            if (val[i] != NULL) free(val[i]);
        }
        free(key);
        free(val);
        free(v_size);
    }

}

//***********************************************************************
// osrs_object_iter_aregex_cb - Handles the attr regex object iterator
//***********************************************************************

void osrs_object_iter_aregex_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *hid;
    unsigned char *buffer;
    unsigned char tbuf[32], null[32];
    int  v_size;
    char *key;
    char *val;
    char *fname;
    lio_os_regex_table_t *path, *object_regex, *attr_regex;
    lio_creds_t *creds;
    int fsize, bpos, n, err, ftype, prefix_len, null_len;
    int64_t recurse_depth, obj_types, timeout, v_max, len;
    mq_msg_t *msg;
    os_object_iter_t *it;
    os_attr_iter_t *ait;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    mqs = NULL;
    it = NULL;
    path = object_regex = attr_regex = NULL;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    hid = mq_msg_pop(msg);  //** This is the Host ID for the ongoing stream

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fdata = mq_msg_pop(msg);  //** This has the data
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);

    //** Parse the buffer
    path = NULL;
    object_regex = NULL;
    bpos = 0;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
    if (n < 0) {
        timeout = 60;
        goto fail;
    }
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &recurse_depth);
    if (n < 0) goto fail;
    bpos += n;

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, (recurse_depth>0) ? 1 : 0);

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &obj_types);
    if (n < 0) goto fail;
    bpos += n;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &v_max);
    if (n < 0) goto fail;
    bpos += n;

    path = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    log_printf(15, "1. bpos=%d fsize=%d\n", bpos, fsize);

    object_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    log_printf(15, "2. bpos=%d fsize=%d\n", bpos, fsize);

    attr_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    log_printf(15, "2. bpos=%d fsize=%d attr_regex=%p\n", bpos, fsize, attr_regex);

    //** run the task
    if (creds != NULL) {
        v_max = -llabs(v_max);
        ait = NULL;
        it = os_create_object_iter(osrs->os_child, creds, path, object_regex, obj_types, attr_regex, recurse_depth, &ait, v_max);
    } else {
        it = NULL;
    }

fail:
    //** Encode the status
    if (mqs == NULL) mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, hid, 0);
    status = (it != NULL) ? gop_success_status : gop_failure_status;
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);

    //** Check if we kick out due to an error
    if (it == NULL) goto finished;

    null_len = tbx_zigzag_encode(0, null);

    //** Pack up the data and send it out
    err = 0;
    while (((ftype = os_next_object(osrs->os_child, it, &fname, &prefix_len)) > 0) && (err == 0)) {
        osrs_update_active_table(os, hid);  //** Update the active log

        len = strlen(fname);
        n = tbx_zigzag_encode(ftype, tbuf);
        n += tbx_zigzag_encode(prefix_len, &(tbuf[n]));
        n += tbx_zigzag_encode(len, &(tbuf[n]));
        err += gop_mq_stream_write(mqs, tbuf, n);
        err += gop_mq_stream_write(mqs, fname, len);

        log_printf(5, "ftype=%d prefix_len=%d len=%" PRId64 " fname=%s\n", ftype, prefix_len, len, fname);
        //** Now dump the attributes
        if (attr_regex != NULL) {
            v_size = v_max;
            while (os_next_attr(osrs->os_child, ait, &key, (void **)&val, &v_size) == 0) {
                log_printf(15, "key=%s v_size=%d\n", key, v_size);
                len = strlen(key);
                n = tbx_zigzag_encode(len, tbuf);
                err += gop_mq_stream_write(mqs, tbuf, n);
                err += gop_mq_stream_write(mqs, key, len);
                free(key);
                key = NULL;

                n = tbx_zigzag_encode(v_size, tbuf);
                err += gop_mq_stream_write(mqs, tbuf, n);
                if (v_size > 0) {
                    err += gop_mq_stream_write(mqs, val, v_size);
                    free(val);
                    val = NULL;
                }
                v_size = v_max;
            }

            //** Flag this as the last attr
            gop_mq_stream_write(mqs, null, null_len);
        }


        free(fname);
    }

    //** Flag this as the last object
    gop_mq_stream_write(mqs, null, null_len);

    //** Destroy the object iterator
    os_destroy_object_iter(osrs->os_child, it);

finished:
    osrs_release_creds(os, creds);

    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);

    //** Flush the buffer
    gop_mq_stream_destroy(mqs);

    //** Clean up
    if (path != NULL) lio_os_regex_table_destroy(path);
    if (object_regex != NULL) lio_os_regex_table_destroy(object_regex);
    if (attr_regex != NULL) lio_os_regex_table_destroy(attr_regex);
}

//***********************************************************************
// osrs_attr_iter_cb - Handles the attr regex iterator
//***********************************************************************

void osrs_attr_iter_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *ffd, *fhid;
    gop_mq_ongoing_handle_t ohandle;
    unsigned char *buffer;
    unsigned char tbuf[32];
    int v_size;
    char *key, *val, *id;
    void *fhandle;
    lio_os_regex_table_t *attr_regex;
    lio_creds_t *creds;
    int fsize, bpos, n, err, id_size, hsize;
    int64_t timeout, len, v_size_init;
    mq_msg_t *msg;
    intptr_t fhkey = 0;
    void *handle;
    os_attr_iter_t *it;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    key = NULL;
    val = NULL, v_size = -1;
    it = NULL;
    handle = NULL;
    attr_regex = NULL;
    mqs = NULL;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame
    fhid = mq_msg_pop(msg);  //** Host handle
    gop_mq_get_frame(fhid, (void **)&id, &id_size);

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    ffd = mq_msg_pop(msg);  //** This has the file handle
    gop_mq_get_frame(ffd, (void **)&fhandle, &hsize);

    fdata = mq_msg_pop(msg);  //** This has the data
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);

    //** Check if the file handle is the correect size
    if (hsize != sizeof(intptr_t)) {
        log_printf(6, "ERROR invalid handle size=%d\n", hsize);

        //** Create the stream so we can get the heartbeating while we work
        timeout = 60;
        osrs_update_active_table(os, fhid);  //** Update the active log

        goto fail;
    }

    //** Get the local file handle
    fhkey = *(intptr_t *)fhandle;
    log_printf(5, "PTR key=%p\n", key);

    //** Do the host lookup for the file handle
    if ((handle = gop_mq_ongoing_get(osrs->ongoing, id, id_size, fhkey, &ohandle)) == NULL) {
        log_printf(6, "ERROR missing host=%s\n", id);
    }

    //** Parse the buffer
    attr_regex = NULL;
    bpos = 0;

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &timeout);
    if (n < 0) {
        timeout = 60;
        goto fail;
    }
    bpos += n;

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, fhid, 0);
    osrs_update_active_table(os, fhid);  //** Update the active log

    n = tbx_zigzag_decode(&(buffer[bpos]), fsize-bpos, &v_size_init);
    if (n < 0) goto fail;
    bpos += n;

    attr_regex = os_regex_table_unpack(&(buffer[bpos]), fsize-bpos, &n);
    if (n == 0) goto fail;
    bpos += n;

    log_printf(15, "bpos=%d fsize=%d\n", bpos, fsize);

    //** run the task
    if (creds != NULL) {
        it = os_create_attr_iter(osrs->os_child, creds, handle, attr_regex, v_size_init);
    } else {
        it = NULL;
    }

fail:

    //** Encode the status
    if (mqs == NULL) mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, fhid, 0);
    status = (it != NULL) ? gop_success_status : gop_failure_status;
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);

    //** Check if we kick out due to an error
    if (it == NULL) goto finished;

    //** Pack up the data and send it out
    err = 0;
    v_size = v_size_init;
    while ((os_next_attr(osrs->os_child, it, &key, (void **)&val, &v_size) == 0) && (err == 0)) {
        log_printf(5, "err=%d key=%s v_size=%d\n", err, key, v_size);
        len = strlen(key);
        n = tbx_zigzag_encode(len, tbuf);
        err += gop_mq_stream_write(mqs, tbuf, n);
        err += gop_mq_stream_write(mqs, key, len);
        free(key);

        n = tbx_zigzag_encode(v_size, tbuf);
        err += gop_mq_stream_write(mqs, tbuf, n);
        if (v_size > 0) {
            err += gop_mq_stream_write(mqs, val, v_size);
            free(val);
        }

        v_size = v_size_init;
    }

    //** Flag this as the last object
    n = tbx_zigzag_encode(0, tbuf);
    gop_mq_stream_write(mqs, tbuf, n);

    //** Destroy the object iterator
    os_destroy_attr_iter(osrs->os_child, it);

finished:

    if (handle != NULL) gop_mq_ongoing_release(osrs->ongoing, &ohandle);

    //** Clean up
    osrs_release_creds(os, creds);
    gop_mq_frame_destroy(fdata);
    gop_mq_frame_destroy(fcred);
    gop_mq_frame_destroy(ffd);

    if (attr_regex != NULL) lio_os_regex_table_destroy(attr_regex);

    //** Flush the buffer
    gop_mq_stream_destroy(mqs);

}

//***********************************************************************
// osrs_fsck_iter_cb - Handles the FSCK regex iterator
//***********************************************************************

void osrs_fsck_iter_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata, *fhid;
    unsigned char *buffer;
    unsigned char tbuf[32];
    char *path, *bad_fname, *id;
    lio_creds_t *creds;
    int fsize, n, err, id_size, bad_atype, fsck_err;
    int64_t timeout, len, mode;
    mq_msg_t *msg;
    os_fsck_iter_t *it;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    it = NULL;
    path = NULL;
    err = 0;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fhid = mq_msg_pop(msg);  //** Host handle
    gop_mq_get_frame(fhid, (void **)&id, &id_size);

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fdata = mq_msg_pop(msg);  //** This has the path
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);
    if (fsize > 0) {
        tbx_type_malloc(path, char, fsize+1);
        memcpy(path, buffer, fsize);
        path[fsize] = 0; //** NULL terminate the path name
    } else {
        err = 1;
    }
    gop_mq_frame_destroy(fdata);

    fdata = mq_msg_pop(msg);  //** This has the mode and timeout
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);
    timeout = 300;
    if (fsize > 0) {
        if (err == 0) {
            n = tbx_zigzag_decode(buffer, fsize, &mode);
            tbx_zigzag_decode(&(buffer[n]), fsize-n, &timeout);
        }
    } else {
        err = 1;
    }
    gop_mq_frame_destroy(fdata);

    //** Create the stream so we can get the heartbeating while we work
    mqs = gop_mq_stream_write_create(osrs->mqc, osrs->server_portal, osrs->ongoing, MQS_PACK_COMPRESS, osrs->max_stream, timeout, msg, fid, fhid, 1);

    log_printf(5, "1.err=%d\n", err);

    if (err != 0) goto fail;

    //** Create the fsck iterator
    if (creds != NULL) {
        it = os_create_fsck_iter(osrs->os_child, creds, path, mode);
    } else {
        it = NULL;
    }

    if (it == NULL) {
        err = 1;
    }

    log_printf(5, "2.err=%d\n", err);

fail:

    //** Encode the status
    status = (err == 0) ? gop_success_status : gop_failure_status;
    n = tbx_zigzag_encode(status.op_status, tbuf);
    n = n + tbx_zigzag_encode(status.error_code, &(tbuf[n]));
    gop_mq_stream_write(mqs, tbuf, n);

    //** Check if we kick out due to an error
    if (it == NULL) goto finished;

    //** Pack up the data and send it out
    err = 0;
    while (((fsck_err = os_next_fsck(osrs->os_child, it, &bad_fname, &bad_atype)) != OS_FSCK_FINISHED) && (err == 0)) {
        osrs_update_active_table(os, fhid);  //** Update the active log

        log_printf(5, "err=%d bad_fname=%s bad_atype=%d\n", err, bad_fname, bad_atype);
        len = strlen(bad_fname);
        n = tbx_zigzag_encode(len, tbuf);
        err += gop_mq_stream_write(mqs, tbuf, n);
        err += gop_mq_stream_write(mqs, bad_fname, len);
        free(bad_fname);

        n = tbx_zigzag_encode(bad_atype, tbuf);
        n += tbx_zigzag_encode(fsck_err, &(tbuf[n]));
        err += gop_mq_stream_write(mqs, tbuf, n);
    }

    //** Flag this as the last object
    n = tbx_zigzag_encode(0, tbuf);
    gop_mq_stream_write(mqs, tbuf, n);

    //** Destroy the object iterator
    os_destroy_fsck_iter(osrs->os_child, it);

finished:

    //** Clean up
    osrs_release_creds(os, creds);
    gop_mq_frame_destroy(fcred);

    if (path != NULL) free(path);

    //** Flush the buffer
    gop_mq_stream_destroy(mqs);

}

//***********************************************************************
// osrs_fsck_object_cb - Handles the FSCK object check
//***********************************************************************

void osrs_fsck_object_cb(void *arg, gop_mq_task_t *task)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    gop_mq_frame_t *fid, *fcred, *fdata;
    unsigned char *buffer;
    char *path;
    lio_creds_t *creds;
    int fsize, n, err;
    int64_t timeout, ftype, resolution;
    mq_msg_t *msg, *response;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    err = 0;
    path = NULL;

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fcred = mq_msg_pop(msg);  //** This has the creds
    creds = osrs_get_creds(os, fcred);

    fdata = mq_msg_pop(msg);  //** This has the path
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);
    if (fsize > 0) {
        tbx_type_malloc(path, char, fsize+1);
        memcpy(path, buffer, fsize);
        path[fsize] = 0; //** NULL terminate the path name
    } else {
        err = 1;
    }
    gop_mq_frame_destroy(fdata);

    fdata = mq_msg_pop(msg);  //** This has the ftype and resolution
    gop_mq_get_frame(fdata, (void **)&buffer, &fsize);
    if (fsize > 0) {
        if (err == 0) {
            n = tbx_zigzag_decode(buffer, fsize, &ftype);
            n += tbx_zigzag_decode(&(buffer[n]), fsize-n, &resolution);
            tbx_zigzag_decode(&(buffer[n]), fsize-n, &timeout);
        }
    } else {
        err = 1;
    }
    gop_mq_frame_destroy(fdata);

    log_printf(5, "err=%d\n", err);
    if ((err != 0) || (creds == NULL)) {
        status = (creds == NULL) ? bad_creds_status : gop_failure_status;
    } else {
        gop = os_fsck_object(osrs->os_child, creds, path, ftype, resolution);
        gop_waitall(gop);
        status = gop_get_status(gop);
        gop_free(gop, OP_DESTROY);
    }

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(osrs->server_portal, gop_mq_task_new(osrs->mqc, response, NULL, NULL, 30));

    //** Clean up
    osrs_release_creds(os, creds);
    gop_mq_frame_destroy(fcred);

    if (path != NULL) free(path);
}

//***********************************************************************
// osrs_print_running_config - Prints the running config
//***********************************************************************

void osrs_print_running_config(lio_object_service_fn_t *os, FILE *fd, int print_section_heading)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", osrs->section);
    fprintf(fd, "type = %s\n", OS_TYPE_REMOTE_SERVER);
    if (osrs->hostname) {
        fprintf(fd, "address = %s\n", osrs->hostname);
        fprintf(fd, "ongoing_interval = %d #seconds\n", osrs->ongoing_interval);
    } else {
        fprintf(fd, "# Using global host portal for address\n");
        fprintf(fd, "# Using global host portal for ongoing_interval\n");
    }
    fprintf(fd, "log_activity = %s\n", osrs->fname_activity);
    fprintf(fd, "max_stream = %d\n", osrs->max_stream);
    fprintf(fd, "os_local = %s\n", osrs->os_local_section);
    fprintf(fd, "active_output = %s\n", osrs->fname_active);
    fprintf(fd, "max_active = %d\n", osrs->max_active);
    fprintf(fd, "\n");

     if (osrs->os_child) os_print_running_config(osrs->os_child, fd, 1);
}

//***********************************************************************
// os_remote_server_destroy
//***********************************************************************

void os_remote_server_destroy(lio_object_service_fn_t *os)
{
    lio_osrs_priv_t *osrs = (lio_osrs_priv_t *)os->priv;
    osrs_active_t *a;
    apr_hash_index_t *hi;
    const void *id;
    apr_ssize_t id_len;
    int *count;

    //** Remove the server portal if needed)
    if (osrs->hostname) {
        gop_mq_portal_remove(osrs->mqc, osrs->server_portal);
        gop_mq_ongoing_destroy(osrs->ongoing);  //** Shutdown the ongoing thread and task
        gop_mq_portal_destroy(osrs->server_portal);
        free(osrs->hostname);
    }

    //** Free the log string
    if (osrs->fname_activity != NULL) free(osrs->fname_activity);

    //** Cleanup the activity log
    tbx_stack_move_to_top(osrs->active_lru);
    a = tbx_stack_get_current_data(osrs->active_lru);
    while (a != NULL) {
        if (a->host_id) free(a->host_id);
        free(a);
        tbx_stack_move_down(osrs->active_lru);
        a = tbx_stack_get_current_data(osrs->active_lru);
    }
    tbx_stack_free(osrs->active_lru, 0);
    //** The active_table hash gets destroyed when the pool is destroyed.

    //** Free the pending_lock_table
    for (hi = apr_hash_first(NULL, osrs->pending_lock_table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, &id, &id_len, (void **)&count);
        if (count) free(count);
        apr_hash_set(osrs->pending_lock_table, id, id_len, NULL);
    }

    //** Shutdown the child OS
    os_destroy_service(osrs->os_child);

    //** Now do the normal cleanup
    apr_pool_destroy(osrs->mpool);

    free(osrs->section);
    free(osrs->os_local_section);

    if (osrs->fname_active) free(osrs->fname_active);
    free(osrs);
    free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

lio_object_service_fn_t *object_service_remote_server_create(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section)
{
    lio_object_service_fn_t *os;
    lio_osrs_priv_t *osrs;
    os_create_t *os_create;
    gop_mq_command_table_t *ctable;
    char *stype, *ctype;

    log_printf(0, "START\n");
    if (section == NULL) section = osrs_default_options.section;

    tbx_type_malloc_clear(os, lio_object_service_fn_t, 1);
    tbx_type_malloc_clear(osrs, lio_osrs_priv_t, 1);
    os->priv = (void *)osrs;

    osrs->section = strdup(section);
    os->print_running_config = osrs_print_running_config;

    osrs->tpc = lio_lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);FATAL_UNLESS(osrs->tpc != NULL);

    //** Make the locks and cond variables
    assert_result(apr_pool_create(&(osrs->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(osrs->lock), APR_THREAD_MUTEX_DEFAULT, osrs->mpool);
    apr_thread_mutex_create(&(osrs->abort_lock), APR_THREAD_MUTEX_DEFAULT, osrs->mpool);

    osrs->abort = apr_hash_make(osrs->mpool);
    FATAL_UNLESS(osrs->abort != NULL);

    osrs->spin = apr_hash_make(osrs->mpool);
    FATAL_UNLESS(osrs->spin != NULL);

    //** Get the host name we bind to NULL is global default
    osrs->hostname= tbx_inip_get_string(fd, section, "address", osrs_default_options.hostname);

    //** Get the activity log file
    osrs->fname_activity = tbx_inip_get_string(fd, section, "log_activity", osrs_default_options.fname_activity);
    log_printf(5, "section=%s log_activity=%s\n", section, osrs->fname_activity);

    //** Max Stream size
    osrs->max_stream = tbx_inip_get_integer(fd, section, "max_stream", osrs_default_options.max_stream);

    //** Start the child OS.
    stype = tbx_inip_get_string(fd, section, "os_local", osrs_default_options.os_local_section);
    if (stype == NULL) {  //** Oops missing child OS
        log_printf(0, "ERROR: Mising child OS  section=%s key=rs_local!\n", section);
        tbx_log_flush();
        free(stype);
        abort();
    }
    osrs->os_local_section = stype;

    //** and load it
    ctype = tbx_inip_get_string(fd, stype, "type", OS_TYPE_FILE);
    os_create = lio_lookup_service(ess, OS_AVAILABLE, ctype);
    osrs->os_child = (*os_create)(ess, fd, stype);
    if (osrs->os_child == NULL) {
        log_printf(1, "ERROR loading child OS!  type=%s section=%s\n", ctype, stype);
        tbx_log_flush();
        abort();
    }
    free(ctype);

    //** Get the AuthN server
    osrs->authn = lio_lookup_service(ess, ESS_RUNNING, ESS_AUTHN);

    //** Get the MQC
    osrs->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ); FATAL_UNLESS(osrs->mqc != NULL);

    //** Make the server portal
    if (osrs->hostname) {
        osrs->server_portal = gop_mq_portal_create(osrs->mqc, osrs->hostname, MQ_CMODE_SERVER);
    } else {
        osrs->server_portal = lio_lookup_service(ess, ESS_RUNNING, ESS_SERVER_PORTAL); FATAL_UNLESS(osrs->server_portal != NULL);
    }
    ctable = gop_mq_portal_command_table(osrs->server_portal);
    gop_mq_command_set(ctable, OSR_SPIN_HB_KEY, OSR_SPIN_HB_SIZE, os, osrs_spin_hb_cb);
    gop_mq_command_set(ctable, OSR_EXISTS_KEY, OSR_EXISTS_SIZE, os, osrs_exists_cb);
    gop_mq_command_set(ctable, OSR_REALPATH_KEY, OSR_REALPATH_SIZE, os, osrs_realpath_cb);
    gop_mq_command_set(ctable, OSR_OBJECT_EXEC_MODIFY_KEY, OSR_OBJECT_EXEC_MODIFY_SIZE, os, osrs_object_exec_modify_cb);
    gop_mq_command_set(ctable, OSR_CREATE_OBJECT_KEY, OSR_CREATE_OBJECT_SIZE, os, osrs_create_object_cb);
    gop_mq_command_set(ctable, OSR_CREATE_OBJECT_WITH_ATTRS_KEY, OSR_CREATE_OBJECT_WITH_ATTRS_SIZE, os, osrs_create_object_with_attrs_cb);
    gop_mq_command_set(ctable, OSR_REMOVE_OBJECT_KEY, OSR_REMOVE_OBJECT_SIZE, os, osrs_remove_object_cb);
    gop_mq_command_set(ctable, OSR_REMOVE_REGEX_OBJECT_KEY, OSR_REMOVE_REGEX_OBJECT_SIZE, os, osrs_remove_regex_object_cb);
    gop_mq_command_set(ctable, OSR_ABORT_REMOVE_REGEX_OBJECT_KEY, OSR_ABORT_REMOVE_REGEX_OBJECT_SIZE, os, osrs_abort_remove_regex_object_cb);
    gop_mq_command_set(ctable, OSR_MOVE_OBJECT_KEY, OSR_MOVE_OBJECT_SIZE, os, osrs_move_object_cb);
    gop_mq_command_set(ctable, OSR_SYMLINK_OBJECT_KEY, OSR_SYMLINK_OBJECT_SIZE, os, osrs_symlink_object_cb);
    gop_mq_command_set(ctable, OSR_HARDLINK_OBJECT_KEY, OSR_HARDLINK_OBJECT_SIZE, os, osrs_hardlink_object_cb);
    gop_mq_command_set(ctable, OSR_OPEN_OBJECT_KEY, OSR_OPEN_OBJECT_SIZE, os, osrs_open_object_cb);
    gop_mq_command_set(ctable, OSR_CLOSE_OBJECT_KEY, OSR_CLOSE_OBJECT_SIZE, os, osrs_close_object_cb);
    gop_mq_command_set(ctable, OSR_ABORT_OPEN_OBJECT_KEY, OSR_ABORT_OPEN_OBJECT_SIZE, os, osrs_abort_open_object_cb);
    gop_mq_command_set(ctable, OSR_LOCK_USER_OBJECT_KEY, OSR_LOCK_USER_OBJECT_SIZE, os, osrs_lock_user_object_cb);
    gop_mq_command_set(ctable, OSR_ABORT_LOCK_USER_OBJECT_KEY, OSR_ABORT_LOCK_USER_OBJECT_SIZE, os, osrs_abort_lock_user_object_cb);
    gop_mq_command_set(ctable, OSR_REGEX_SET_MULT_ATTR_KEY, OSR_REGEX_SET_MULT_ATTR_SIZE, os, osrs_regex_set_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_ABORT_REGEX_SET_MULT_ATTR_KEY, OSR_ABORT_REGEX_SET_MULT_ATTR_SIZE, os, osrs_abort_regex_set_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_GET_MULTIPLE_ATTR_KEY, OSR_GET_MULTIPLE_ATTR_SIZE, os, osrs_get_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_GET_MULTIPLE_ATTR_IMMEDIATE_KEY, OSR_GET_MULTIPLE_ATTR_IMMEDIATE_SIZE, os, osrs_get_mult_attr_immediate_cb);
    gop_mq_command_set(ctable, OSR_SET_MULTIPLE_ATTR_KEY, OSR_SET_MULTIPLE_ATTR_SIZE, os, osrs_set_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_SET_MULTIPLE_ATTR_IMMEDIATE_KEY, OSR_SET_MULTIPLE_ATTR_IMMEDIATE_SIZE, os, osrs_set_mult_attr_immediate_cb);
    gop_mq_command_set(ctable, OSR_COPY_MULTIPLE_ATTR_KEY, OSR_COPY_MULTIPLE_ATTR_SIZE, os, osrs_copy_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_MOVE_MULTIPLE_ATTR_KEY, OSR_MOVE_MULTIPLE_ATTR_SIZE, os, osrs_move_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_SYMLINK_MULTIPLE_ATTR_KEY, OSR_SYMLINK_MULTIPLE_ATTR_SIZE, os, osrs_symlink_mult_attr_cb);
    gop_mq_command_set(ctable, OSR_OBJECT_ITER_ALIST_KEY, OSR_OBJECT_ITER_ALIST_SIZE, os, osrs_object_iter_alist_cb);
    gop_mq_command_set(ctable, OSR_OBJECT_ITER_AREGEX_KEY, OSR_OBJECT_ITER_AREGEX_SIZE, os, osrs_object_iter_aregex_cb);
    gop_mq_command_set(ctable, OSR_ATTR_ITER_KEY, OSR_ATTR_ITER_SIZE, os, osrs_attr_iter_cb);
    gop_mq_command_set(ctable, OSR_FSCK_ITER_KEY, OSR_FSCK_ITER_SIZE, os, osrs_fsck_iter_cb);
    gop_mq_command_set(ctable, OSR_FSCK_OBJECT_KEY, OSR_FSCK_OBJECT_SIZE, os, osrs_fsck_object_cb);

     //** Make the ongoing checker if needed or use the global one
     if (osrs->hostname) {
        osrs->ongoing = gop_mq_ongoing_create(osrs->mqc, osrs->server_portal, osrs->ongoing_interval, ONGOING_SERVER, osrs->tpc);
        FATAL_UNLESS(osrs->ongoing != NULL);

        //** This is to handle client stream responses
        gop_mq_command_set(ctable, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, osrs->ongoing, gop_mqs_server_more_cb);
    } else {
        osrs->ongoing = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_SERVER); FATAL_UNLESS(osrs->ongoing != NULL);
    }

    //** Set up the fn ptrs.  This is just for shutdown
    //** so very little is implemented
    os->destroy_service = os_remote_server_destroy;

    os->type = OS_TYPE_REMOTE_SERVER;

    //** This is for the active tables
    osrs->fname_active = tbx_inip_get_string(fd, section, "active_output", osrs_default_options.fname_active);
    osrs->max_active = tbx_inip_get_integer(fd, section, "active_size", osrs_default_options.max_active);
    osrs->pending_lock_table = apr_hash_make(osrs->mpool);
    osrs->active_lru = tbx_stack_new();
    osrs->active_table = apr_hash_make(osrs->mpool);

    _os_global = os;
    tbx_siginfo_handler_add(SIGUSR1, osrs_siginfo_fn, os);

    //** Start it if needed
    if (osrs->hostname) gop_mq_portal_install(osrs->mqc, osrs->server_portal);

    log_printf(0, "END\n");

    return(os);
}

