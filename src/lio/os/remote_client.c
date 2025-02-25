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
// Remote OS implementation for the client side
//***********************************************************************

#define _log_module_index 213

#include <apr_network_io.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <execinfo.h>
#include <gop/gop.h>
#include <gop/mq_helpers.h>
#include <gop/mq_ongoing.h>
#include <gop/mq.h>
#include <gop/mq_stream.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/append_printf.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#include "authn.h"
#include "authn/fake.h"
#include "ex3/system.h"
#include "os.h"
#include "os/remote.h"
#include "service_manager.h"

static lio_osrc_priv_t osrc_default_options = {
    .section = "os_remote_client",
    .temp_section = NULL,
    .timeout = 60,
    .heartbeat = 600,
    .remote_host_string = "${osrc_host}",
    .max_stream = 10*1024*1024,
    .stream_timeout = 65,
    .spin_interval = 1,
    .spin_fail = 4
};

#define OSRS_HANDLE(ofd) (void *)(*(intptr_t *)(ofd)->data)

#define OSRC_ITER_ALIST  0
#define OSRC_ITER_AREGEX 1

extern char *_lio_exe_name;  // ** This is defined in lio_config.c and is set before we would ever be called.

typedef struct {
    lio_object_service_fn_t *os;
    void *data;
    int size;
} osrc_object_fd_t;

typedef struct {
    lio_object_service_fn_t *os;
    char *realpath;
} osrc_realpath_t;

typedef struct {
    uint64_t id;
    lio_object_service_fn_t *os;
} osrc_arg_t;

typedef struct {
    lio_object_service_fn_t *os;
//  void *it;
    gop_mq_stream_t *mqs;
    mq_msg_t *response;
    int v_max;
    int is_sub_iter;
    int no_more_attr;
} osrc_attr_iter_t;

typedef struct {
    lio_object_service_fn_t *os;
    gop_mq_stream_t *mqs;
    mq_msg_t *response;
    int mode;
    int finished;
} osrc_fsck_iter_t;

typedef struct {
    lio_object_service_fn_t *os;
    os_attr_iter_t **ait;
    void **val;
    int *v_size;
    int *v_size_initial;
    int n_keys;
    int v_max;
    int iter_type;
    int finished;
    gop_mq_stream_t *mqs;
    mq_msg_t *response;
} osrc_object_iter_t;

typedef struct {
    char handle[1024];
    osrc_object_fd_t **pfd;
    lio_object_service_fn_t *os;
} osrc_open_t;

typedef struct {
    char handle[1024];
    lio_object_service_fn_t *os;
} osrc_lock_user_t;

typedef struct {
    lio_object_service_fn_t *os;
    os_fd_t *fd;
    os_fd_t *fd_dest;
    char *path;
    char **src_path;
    char **key;
    char **key_dest;
    void **val;
    char *key_tmp;
    char *src_tmp;
    void *val_tmp;
    int *v_size;
    int v_tmp;
    int n;
} osrc_mult_attr_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    lio_os_regex_table_t *path;
    lio_os_regex_table_t *object_regex;
    int obj_types;
    int recurse_depth;
    uint64_t my_id;
} osrc_remove_regex_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    char *id;
    lio_os_regex_table_t *path;
    lio_os_regex_table_t *object_regex;
    int obj_types;
    int recurse_depth;
    char **key;
    void **val;
    int *v_size;
    int n_attrs;
    uint64_t my_id;
} osrc_set_regex_t;

#define OSRC_DEBUG(...) __VA_ARGS__
#define OSRC_DEBUG_NOTIFY(fmt, ...) if (os_notify_handle) _tbx_notify_printf(os_notify_handle, 1, NULL, __func__, __LINE__, fmt, ## __VA_ARGS__)
#define OSRC_DEBUG_MQ_PRINTF(msg, fmt, ...) char _b64[1024]; if (os_notify_handle) _tbx_notify_printf(os_notify_handle, 1, NULL, __func__, __LINE__, "OSRC_GOP id=%s " fmt, _print_mq_id(msg, _b64, 1024), ## __VA_ARGS__)

//***********************************************************************
//  _print_mq_id - Just gets the ID and size
//***********************************************************************

char *_print_mq_id(mq_msg_t *msg, char *buffer, int blen)
{
    char *id;
    int len;
    gop_mq_msg_iter_t *curr;
    gop_mq_frame_t *f;

    //** Skip the address
    for (curr = gop_mq_msg_iter_first(msg); curr != NULL; curr = gop_mq_msg_iter_next(curr)) {
        f = gop_mq_msg_iter_frame(curr);
        gop_mq_get_frame(f, (void **)&id, &len);
        if (len == 0) break;
    }

    //** Skip over he other frames
    curr = gop_mq_msg_iter_next(curr);   //** Version frame
    curr = gop_mq_msg_iter_next(curr);   //** MQ command frame

    //** This is the ID frame
    curr = gop_mq_msg_iter_next(curr);
    f = gop_mq_msg_iter_frame(curr);
    gop_mq_get_frame(f, (void **)&id, &len);

    return(gop_mq_id2str(id, len, buffer, blen));
}

//***********************************************************************
// osrc_add_creds - Adds the creds to the message
//***********************************************************************

int osrc_add_creds(lio_object_service_fn_t *os, lio_creds_t *creds, mq_msg_t *msg)
{
    void *chandle;
    int len;

    chandle = an_cred_get_handle(creds, &len);
    gop_mq_msg_append_mem(msg, chandle, len, MQF_MSG_KEEP_DATA);

    return(0);
}

//***********************************************************************
// osrc_response_status - Handles a response that just returns the status
//***********************************************************************

gop_op_status_t osrc_response_status(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    gop_op_status_t status;

    log_printf(5, "START\n");

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);
    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    if ((status.op_status == OP_STATE_FAILURE) && (status.error_code == -ENOKEY)) {
        notify_printf(osrc->notify, 1, NULL, "ERROR: osrc_response_status -- Invalid creds!\n");
    }

    return(status);
}

//***********************************************************************
// osrc_response_stream_status - Handles getting a stream status response
//***********************************************************************

gop_op_status_t osrc_response_stream_status(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;
    int err;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    mqs = gop_mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, gop_mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

    //** Parse the status
    status.op_status = gop_mq_stream_read_varint(mqs, &err);
    log_printf(15, "op_status=%d\n", status.op_status);
    status.error_code = gop_mq_stream_read_varint(mqs, &err);
    log_printf(15, "error_code%d\n", status.error_code);

    if ((status.op_status == OP_STATE_FAILURE) && (status.error_code == -ENOKEY)) {
        notify_printf(osrc->notify, 1, NULL, "ERROR: osrc_response_stream_status -- Invalid creds!\n");
    }

    gop_mq_stream_destroy(mqs);

    if (err != 0) status = gop_failure_status;
    log_printf(5, "END err=%d status=%d %d\n", err, status.op_status, status.error_code);

    return(status);
}

//***********************************************************************
// osrc_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************

gop_op_status_t osrc_remove_regex_object_func(void *arg, int id)
{
    osrc_remove_regex_t *op = (osrc_remove_regex_t *)arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)op->os->priv;
    int bpos, bufsize, again, n;
    unsigned char *buffer;
    mq_msg_t *msg, *spin;
    gop_op_generic_t *gop, *g;
    gop_op_status_t status;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_REMOVE_REGEX_OBJECT_KEY, OSR_REMOVE_REGEX_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(op->os, op->creds, msg);

    bufsize = 4096;
    tbx_type_malloc(buffer, unsigned char, bufsize);
    do {
        again = 0;
        bpos = 0;

        n = tbx_zigzag_encode(osrc->timeout, buffer);
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(sizeof(op->my_id), &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;
        memcpy(&(buffer[bpos]), &(op->my_id), sizeof(op->my_id));
        bpos += sizeof(op->my_id);

        n = tbx_zigzag_encode(osrc->spin_fail, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(op->recurse_depth, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(op->obj_types, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = os_regex_table_pack(op->path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        n = os_regex_table_pack(op->object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        if (again == 1) {
            bufsize = bpos + 10;
            free(buffer);
            tbx_type_malloc(buffer, unsigned char, bufsize);
        }
    } while (again == 1);


    gop_mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    log_printf(5, "END\n");

    //** Make the gop and submit it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_stream_status, op->os, NULL, osrc->timeout);
    gop_start_execution(gop);

    //** Wait for it to complete Sending hearbeats as needed
    while ((g = gop_waitany_timed(gop, osrc->spin_interval)) == NULL) {
        //** Form the spin message
        spin = gop_mq_make_exec_core_msg(osrc->remote_host, 0);
        gop_mq_msg_append_mem(spin, OSR_SPIN_HB_KEY, OSR_SPIN_HB_SIZE, MQF_MSG_KEEP_DATA);
        gop_mq_msg_append_mem(spin, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
        osrc_add_creds(op->os, op->creds, spin);
        gop_mq_msg_append_mem(spin, &(op->my_id), sizeof(op->my_id), MQF_MSG_KEEP_DATA);

        //** And send it
        g = gop_mq_op_new(osrc->mqc, spin, NULL, NULL, NULL, osrc->timeout);
        log_printf(5, "spin hb sent. gid=%d\n", gop_id(g));
        gop_set_auto_destroy(g, 1);
        gop_start_execution(g);
    }

    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    log_printf(5, "END status=%d\n", status.op_status);

    return(status);
}


//***********************************************************************
// osrc_remove_regex_object - Does a bulk regex remove.
//     Each matching object is removed.  If the object is a directory
//     then the system will recursively remove it's contents up to the
//     recursion depth.  Setting recurse_depth=0 will only remove the dir
//     if it is empty.
//***********************************************************************

gop_op_generic_t *osrc_remove_regex_object(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_remove_regex_t *op;
    gop_op_generic_t *gop;

    tbx_type_malloc(op, osrc_remove_regex_t, 1);
    op->os = os;
    op->creds = creds;
    op->path = path;
    op->object_regex = object_regex;
    op->obj_types = obj_types;
    op->recurse_depth = recurse_depth;
    op->my_id = 0;
    tbx_random_get_bytes(&(op->my_id), sizeof(op->my_id));

    gop = gop_tp_op_new(osrc->tpc, NULL, osrc_remove_regex_object_func, (void *)op, free, 1);
    return(gop);
}

//***********************************************************************
// osrc_abort_remove_regex_object - Aborts a bulk remove call
//***********************************************************************

gop_op_generic_t *osrc_abort_remove_regex_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    unsigned char buf[512];
    int bpos;
    gop_op_generic_t *g;
    osrc_set_regex_t *op;

    log_printf(5, "START\n");
    op = gop_get_private(gop);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_ABORT_REMOVE_REGEX_OBJECT_KEY, OSR_ABORT_REMOVE_REGEX_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, op->creds, msg);

    bpos = tbx_zigzag_encode(sizeof(op->my_id), buf);
    memcpy(&(buf[bpos]), &(op->my_id), sizeof(op->my_id));
    bpos += sizeof(op->my_id);
    gop_mq_msg_append_mem(msg, buf, bpos, MQF_MSG_KEEP_DATA);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    g = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    log_printf(5, "END\n");

    return(g);
}

//***********************************************************************
// osrc_remove_object - Makes a remove object operation
//***********************************************************************

gop_op_generic_t *osrc_remove_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START fname=%s\n", path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_REMOVE_OBJECT_KEY, OSR_REMOVE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

gop_op_status_t osrc_regex_object_set_multiple_attrs_func(void *arg, int id)
{
    osrc_set_regex_t *op = (osrc_set_regex_t *)arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)op->os->priv;
    int bpos, bufsize, again, n, i, len;
    unsigned char *buffer;
    mq_msg_t *msg, *spin;
    gop_op_generic_t *gop, *g;
    gop_op_status_t status;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_REGEX_SET_MULT_ATTR_KEY, OSR_REGEX_SET_MULT_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(op->os, op->creds, msg);
    if (op->id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, op->id, strlen(op->id)+1, MQF_MSG_KEEP_DATA);
    }

    bufsize = 4096;
    tbx_type_malloc(buffer, unsigned char, bufsize);
    do {
        again = 0;
        bpos = 0;

        n = tbx_zigzag_encode(osrc->timeout, buffer);
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(sizeof(op->my_id), &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;
        memcpy(&(buffer[bpos]), &(op->my_id), sizeof(op->my_id));
        bpos += sizeof(op->my_id);

        n = tbx_zigzag_encode(osrc->spin_fail, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(op->recurse_depth, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = tbx_zigzag_encode(op->obj_types, &(buffer[bpos]));
        if (n<0) {
            again = 1;
            n=4;
        }
        bpos += n;

        n = os_regex_table_pack(op->path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        n = os_regex_table_pack(op->object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        if (again == 1) {
            bufsize = bpos + 10;
            free(buffer);
            tbx_type_malloc(buffer, unsigned char, bufsize);
        }

        bpos += tbx_zigzag_encode(op->n_attrs, (unsigned char *)&(buffer[bpos]));

        for (i=0; i<op->n_attrs; i++) {
            log_printf(15, "i=%d key=%s val=%p bpos=%d\n", i, op->key[i], op->val[i], bpos);
            len = strlen(op->key[i]);
            n = (again == 0) ? tbx_zigzag_encode(len, (unsigned char *)&(buffer[bpos])) : 4;
            if (n<0) {
                again = 1;
                n=4;
            }
            bpos += n;
            if (again == 0) memcpy(&(buffer[bpos]), op->key[i], len);
            bpos += len;

            len = op->v_size[i];
            n = (again == 0) ? tbx_zigzag_encode(len, (unsigned char *)&(buffer[bpos])) : 4;
            if (n<0) {
                again = 1;
                n=4;
            }
            bpos += n;
            if (len > 0) {
                if (again == 0) memcpy(&(buffer[bpos]), op->val[i], len);
                bpos += len;
            }
        }
    } while (again == 1);


    gop_mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


    //** Make the gop and submit it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_stream_status, op->os, NULL, osrc->timeout);
    gop_start_execution(gop);

    //** Wait for it to complete Sending hearbeats as needed
    while ((g = gop_waitany_timed(gop, osrc->spin_interval)) == NULL) {
        //** Form the spin message
        spin = gop_mq_make_exec_core_msg(osrc->remote_host, 0);
        gop_mq_msg_append_mem(spin, OSR_SPIN_HB_KEY, OSR_SPIN_HB_SIZE, MQF_MSG_KEEP_DATA);
        gop_mq_msg_append_mem(spin, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
        osrc_add_creds(op->os, op->creds, spin);
        gop_mq_msg_append_mem(spin, &(op->my_id), sizeof(op->my_id), MQF_MSG_KEEP_DATA);

        //** And send it
        g = gop_mq_op_new(osrc->mqc, spin, NULL, NULL, NULL, osrc->timeout);
        log_printf(5, "spin hb sent. gid=%d\n", gop_id(g));
        gop_set_auto_destroy(g, 1);
        gop_start_execution(g);
    }

    gop_waitall(gop);
    status = gop_get_status(gop);
    gop_free(gop, OP_DESTROY);

    log_printf(5, "END status=%d\n", status.op_status);


    return(status);
}

//***********************************************************************
// osrc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

gop_op_generic_t *osrc_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char *id, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_set_regex_t *op;
    gop_op_generic_t *gop;

    tbx_type_malloc(op, osrc_set_regex_t, 1);
    op->os = os;
    op->creds = creds;
    op->id = id;
    op->path = path;
    op->object_regex = object_regex;
    op->obj_types = object_types;
    op->recurse_depth = recurse_depth;
    op->key = key;
    op->val = val;
    op->v_size= v_size;
    op->n_attrs = n_attrs;
    op->my_id = 0;
    tbx_random_get_bytes(&(op->my_id), sizeof(op->my_id));

    gop = gop_tp_op_new(osrc->tpc, NULL, osrc_regex_object_set_multiple_attrs_func, (void *)op, free, 1);
    gop_set_private(gop, op);

    return(gop);
}

//***********************************************************************
// osrc_abort_regex_object_set_multiple_attrs - Aborts a bulk attr call
//***********************************************************************

gop_op_generic_t *osrc_abort_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    unsigned char buf[512];
    int bpos;
    gop_op_generic_t *g;
    osrc_set_regex_t *op;

    log_printf(5, "START\n");
    op = gop_get_private(gop);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_ABORT_REGEX_SET_MULT_ATTR_KEY, OSR_ABORT_REGEX_SET_MULT_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, op->creds, msg);

    bpos = tbx_zigzag_encode(sizeof(op->my_id), buf);
    memcpy(&(buf[bpos]), &(op->my_id), sizeof(op->my_id));
    bpos += sizeof(op->my_id);
    gop_mq_msg_append_mem(msg, buf, bpos, MQF_MSG_KEEP_DATA);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    g = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    log_printf(5, "END\n");

    return(g);
}

//***********************************************************************
//  osrc_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

gop_op_generic_t *osrc_exists(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START fname=%s\n", path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_EXISTS_KEY, OSR_EXISTS_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:EXISTS path=%s", path);

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
// osrc_response_realpath - Handles the response for a realpath call
//***********************************************************************

gop_op_status_t osrc_response_realpath(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_realpath_t *arg = (osrc_realpath_t *)task->arg;
    gop_op_status_t status;
    char *data;
    int n;

    log_printf(5, "START\n");

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);
    if (status.op_status == OP_STATE_SUCCESS) {
        gop_mq_get_frame(gop_mq_msg_next(task->response), (void **)&data, &n);
        memcpy(arg->realpath, data, n);
        arg->realpath[n] = '\0';
    } else {
        arg->realpath[0] = '\0';
    }
    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
//  osrc_realpath - Returns the real path of the object type
//***********************************************************************

gop_op_generic_t *osrc_realpath(lio_object_service_fn_t *os, lio_creds_t *creds, const char *path, char *realpath)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    osrc_realpath_t *arg;

    log_printf(5, "START fname=%s\n", path);

    if (strcmp(path, "/") == 0) {
        log_printf(5, "Short circuiting the root path\n");
        strncpy(realpath, path, 2);
        return(gop_dummy(gop_success_status));
    }

    tbx_type_malloc(arg, osrc_realpath_t, 1);
    arg->os = os;
    arg->realpath = realpath;

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_REALPATH_KEY, OSR_REALPATH_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, (char *)path, strlen(path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_realpath, arg, free, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:REALPATH path=%s", path);

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
// osrc_object_exec_modify - Updates eh executable flag
//***********************************************************************

gop_op_generic_t *osrc_object_exec_modify(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    unsigned char buffer[10], *sent;
    int n;

    log_printf(5, "START fname=%s\n", path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_OBJECT_EXEC_MODIFY_KEY, OSR_OBJECT_EXEC_MODIFY_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

    n = tbx_zigzag_encode(type, buffer);
    tbx_type_malloc(sent, unsigned char, n);
    memcpy(sent, buffer, n);
    gop_mq_msg_append_frame(msg, gop_mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:EXEC_MODIFY path=%s", path);

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
// osrc_create_object - Creates an object
//***********************************************************************

gop_op_generic_t *osrc_create_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type, char *id)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    unsigned char buffer[10], *sent;
    int n;

    log_printf(5, "START fname=%s\n", path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_CREATE_OBJECT_KEY, OSR_CREATE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

    n = tbx_zigzag_encode(type, buffer);
    tbx_type_malloc(sent, unsigned char, n);
    memcpy(sent, buffer, n);
    gop_mq_msg_append_frame(msg, gop_mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

    if (id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:CREATE_OBJECT type=%d path=%s", type, path);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_create_object_with_attrs - Creates an object with initial attrs
//***********************************************************************

gop_op_generic_t *osrc_create_object_with_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type, char *id, char **key, void **val, int *v_size, int n_keys)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    unsigned char buffer[10], *sent;
    char *data;
    int i, bpos, len, nmax, n;

    log_printf(5, "START fname=%s\n", path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_CREATE_OBJECT_WITH_ATTRS_KEY, OSR_CREATE_OBJECT_WITH_ATTRS_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

    n = tbx_zigzag_encode(type, buffer);
    tbx_type_malloc(sent, unsigned char, n);
    memcpy(sent, buffer, n);
    gop_mq_msg_append_frame(msg, gop_mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

    if (id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
    }

    //** Form the attribute frame
    nmax = 8;  //** Add just a little extra
    for (i=0; i<n_keys; i++) {
        nmax += strlen(key[i]) + 4 + v_size[i] + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = 0;
    bpos += tbx_zigzag_encode(n_keys, (unsigned char *)&(data[bpos]));
    for (i=0; i<n_keys; i++) {
        len = strlen(key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), key[i], len);
        bpos += len;

        bpos += tbx_zigzag_encode(v_size[i], (unsigned char *)&(data[bpos]));
        if (v_size[i] > 0) {
            memcpy(&(data[bpos]), val[i], v_size[i]);
            bpos += v_size[i];
        }
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:CREATE_OBJECT type=%d path=%s", type, path);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_symlink_object - Generates a symbolic link object operation
//***********************************************************************

gop_op_generic_t *osrc_symlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START src_fname=%s\n", src_path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_SYMLINK_OBJECT_KEY, OSR_SYMLINK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
    if (id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "src=%s dest=%s\n", src_path, dest_path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:SYMLINK src_path=%s dest_path=%s", src_path, dest_path);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_hardlink_object - Generates a hard link object operation
//***********************************************************************

gop_op_generic_t *osrc_hardlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START src_fname=%s\n", src_path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_HARDLINK_OBJECT_KEY, OSR_HARDLINK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
    if (id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "src=%s dest=%s\n", src_path, dest_path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:HARDLINK src_path=%s dest_path=%s", src_path, dest_path);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_move_object - Generates a move object operation
//***********************************************************************

gop_op_generic_t *osrc_move_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START src_fname=%s\n", src_path);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_MOVE_OBJECT_KEY, OSR_MOVE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, src_path, strlen(src_path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, dest_path, strlen(dest_path)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "src=%s dest=%s\n", src_path, dest_path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:MOVE src_path=%s dest_path=%s", src_path, dest_path);

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
// osrc_copy_mult_attrs_internal - Copies multiple object attributes between
//    objects
//***********************************************************************

gop_op_generic_t *osrc_copy_mult_attrs_internal(lio_object_service_fn_t *os, osrc_mult_attr_t *ma, lio_creds_t *creds)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *sfd = (osrc_object_fd_t *)ma->fd;
    osrc_object_fd_t *dfd = (osrc_object_fd_t *)ma->fd_dest;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int i, bpos, len, nmax;
    char *data;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_COPY_MULTIPLE_ATTR_KEY, OSR_COPY_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    //** Form the heartbeat and handle frames
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, sfd->data, sfd->size, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, dfd->data, dfd->size, MQF_MSG_KEEP_DATA);

    //** Form the attribute frame
    nmax = 12;  //** Add just a little extra
    for (i=0; i<ma->n; i++) {
        nmax += strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = 0;
    bpos += tbx_zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
    bpos += tbx_zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
    for (i=0; i<ma->n; i++) {
        len = strlen(ma->key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key[i], len);
        bpos += len;

        len = strlen(ma->key_dest[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key_dest[i], len);
        bpos += len;
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "FIXME=MISSING\n");

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:COPY_ATTRS");

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

gop_op_generic_t *osrc_copy_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd_src;
    ma->fd_dest = fd_dest;
    ma->key = key_src;
    ma->key_dest = key_dest;
    ma->n = n;

    return(osrc_copy_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_copy_attr - Generates a copy object attribute operation
//***********************************************************************

gop_op_generic_t *osrc_copy_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd_src;
    ma->fd_dest = fd_dest;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(osrc_copy_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_symlink_mult_attrs_internal - Symlinks multiple object attributes between
//    objects
//***********************************************************************

gop_op_generic_t *osrc_symlink_mult_attrs_internal(lio_object_service_fn_t *os, osrc_mult_attr_t *ma, lio_creds_t *creds)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *dfd = (osrc_object_fd_t *)ma->fd_dest;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int i, bpos, len, nmax;
    char *data;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_SYMLINK_MULTIPLE_ATTR_KEY, OSR_SYMLINK_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    //** Form the heartbeat and handle frames
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, dfd->data, dfd->size, MQF_MSG_KEEP_DATA);

    //** Form the attribute frame
    nmax = 12;  //** Add just a little extra
    for (i=0; i<ma->n; i++) {
        nmax += strlen(ma->src_path[i]) + 4 + strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = 0;
    bpos += tbx_zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
    bpos += tbx_zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
    for (i=0; i<ma->n; i++) {
        len = strlen(ma->src_path[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->src_path[i], len);
        bpos += len;

        len = strlen(ma->key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key[i], len);
        bpos += len;

        len = strlen(ma->key_dest[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key_dest[i], len);
        bpos += len;
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "FIXME=MISSING\n");

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

gop_op_generic_t *osrc_symlink_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->src_path = src_path;
    ma->key = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = key_dest;
    ma->n = n;

    return(osrc_symlink_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_symlink_attr - Generates a link attribute operation
//***********************************************************************

gop_op_generic_t *osrc_symlink_attr(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->src_path = &(ma->src_tmp);
    ma->src_tmp = src_path;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(osrc_symlink_mult_attrs_internal(os, ma, creds));
}

//***********************************************************************
// osrc_move_mult_attrs_internal - Renames multiple object attributes
//***********************************************************************

gop_op_generic_t *osrc_move_mult_attrs_internal(lio_object_service_fn_t *os, osrc_mult_attr_t *ma, lio_creds_t *creds)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *sfd = (osrc_object_fd_t *)ma->fd;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int i, bpos, len, nmax;
    char *data;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_MOVE_MULTIPLE_ATTR_KEY, OSR_MOVE_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    //** Form the heartbeat and handle frames
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, sfd->data, sfd->size, MQF_MSG_KEEP_DATA);

    //** Form the attribute frame
    nmax = 12;  //** Add just a little extra
    for (i=0; i<ma->n; i++) {
        nmax += strlen(ma->key[i]) + 4 + strlen(ma->key_dest[i]) + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = 0;
    bpos += tbx_zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
    bpos += tbx_zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
    for (i=0; i<ma->n; i++) {
        len = strlen(ma->key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key[i], len);
        bpos += len;

        len = strlen(ma->key_dest[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key_dest[i], len);
        bpos += len;
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

gop_op_generic_t *osrc_move_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = key_old;
    ma->key_dest = key_new;
    ma->n = n;

    return(osrc_move_mult_attrs_internal(os, ma, creds));
}


//***********************************************************************
// osrc_move_attr - Generates a move object attribute operation
//***********************************************************************

gop_op_generic_t *osrc_move_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_old;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_new;

    ma->n = 1;

    return(osrc_move_mult_attrs_internal(os, ma, creds));
}


//*************************************************************
//  osf_store_val - Stores the return attribute value
//*************************************************************

int osrc_store_val(gop_mq_stream_t *mqs, int src_size, void **dest, int *v_size)
{
    char *buf;

    if (*v_size >= 0) {
        if (src_size < 0) {    //** Doesn't exist so just store the size
            *v_size = src_size;
        } else if (*v_size < src_size) {
            *v_size = -src_size;
            gop_mq_stream_read(mqs, NULL, src_size);  //** This drops the values
            return(1);
        } else if (dest && (*v_size > src_size)) {
            buf = *dest;
            buf[src_size] = 0;  //** IF have the space NULL terminate
        }
    } else {
        if (dest && (src_size > 0)) {
            *dest = malloc(src_size+1);
            buf = *dest;
            buf[src_size] = 0;  //** IF have the space NULL terminate
        } else {
            *v_size = src_size;
            if (dest) *dest = NULL;
            return(0);
        }
    }

    *v_size = src_size;
    if (dest) {
        gop_mq_stream_read(mqs, *dest, src_size);
    } else {
        gop_mq_stream_read(mqs, NULL, src_size);  //** Just drop it
    }

    return(0);
}

//***********************************************************************
// osrc_response_get_multiple_attrs - Handles a get multiple attr response
//***********************************************************************

gop_op_status_t osrc_response_get_multiple_attrs(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_mult_attr_t *ma = task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)ma->os->priv;
    gop_mq_stream_t *mqs;
    gop_op_status_t status;
    int len, err, i;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    mqs = gop_mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, gop_mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

    //** Parse the status
    status.op_status = gop_mq_stream_read_varint(mqs, &err);
    log_printf(15, "op_status=%d\n", status.op_status);
    status.error_code = gop_mq_stream_read_varint(mqs, &err);
    log_printf(15, "error_code%d\n", status.error_code);

    if (err != 0) {
        status.op_status= OP_STATE_FAILURE;    //** Trigger a failure if error reading from the stream
    }
    if (status.op_status == OP_STATE_FAILURE) goto fail;

    //** Not get the attributes
    for (i=0; i < ma->n; i++) {
        len = gop_mq_stream_read_varint(mqs, &err);
        if (err != 0) {
            status = gop_failure_status;
            goto fail;
        }

        osrc_store_val(mqs, len, &(ma->val[i]), &(ma->v_size[i]));
        log_printf(15, "val[%d]=%s\n", i, (char *)ma->val[i]);
    }

fail:
    gop_mq_stream_destroy(mqs);

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
// osrc_get_mult_attrs_internal - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osrc_get_mult_attrs_internal(lio_object_service_fn_t *os, osrc_mult_attr_t *ma, lio_creds_t *creds, int is_immediate)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *ofd = (osrc_object_fd_t *)ma->fd;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int i, bpos, len, nmax;
    char *data;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    if (is_immediate) {
        gop_mq_msg_append_mem(msg, OSR_GET_MULTIPLE_ATTR_IMMEDIATE_KEY, OSR_GET_MULTIPLE_ATTR_IMMEDIATE_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, OSR_GET_MULTIPLE_ATTR_KEY, OSR_GET_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    //** Form the heartbeat and handle frames
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    if (is_immediate) {
        OSRC_DEBUG_MQ_PRINTF(msg, "OSRC: GET_ATTR: IMMEDIATE fname=%s\n", ma->path);
        gop_mq_msg_append_mem(msg, ma->path, strlen(ma->path)+1, MQF_MSG_KEEP_DATA);
    } else {
        OSRC_DEBUG_MQ_PRINTF(msg, "OSRC: GET_ATTR: fd=%" PRIdPTR "\n", *(intptr_t *)ofd->data);
        gop_mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);
    }

    //** Form the attribute frame
    nmax = 12;  //** Add just a little extra
    for (i=0; i<ma->n; i++) {
        nmax += strlen(ma->key[i]) + 4 + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = tbx_zigzag_encode(osrc->max_stream, (unsigned char *)data);
    bpos += tbx_zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
    bpos += tbx_zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
    for (i=0; i<ma->n; i++) {
        len = strlen(ma->key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key[i], len);
        bpos += len;
        bpos += tbx_zigzag_encode(ma->v_size[i], (unsigned char *)&(data[bpos]));
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_get_multiple_attrs, ma, free, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:GET_ATTRS");

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
// osrc_get_multiple_attrs_immediate - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osrc_get_multiple_attrs_immediate(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, char **key, void **val, int *v_size, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;
    ma->path = path;

    return(osrc_get_mult_attrs_internal(os, ma, creds, 1));
}

//***********************************************************************
// osrc_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osrc_get_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;

    return(osrc_get_mult_attrs_internal(os, ma, creds, 0));
}

//***********************************************************************
// osrc_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *osrc_get_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = 1;

    return(osrc_get_mult_attrs_internal(os, ma, creds, 0));
}


//***********************************************************************
// osrc_set_mult_attrs_internal - Sets multiple object attributes
//***********************************************************************

gop_op_generic_t *osrc_set_mult_attrs_internal(lio_object_service_fn_t *os, osrc_mult_attr_t *ma, lio_creds_t *creds, int is_immediate)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *ofd = (osrc_object_fd_t *)ma->fd;
    mq_msg_t *msg;
    gop_op_generic_t *gop;
    int i, bpos, len, nmax;
    char *data;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    if (is_immediate) {
        gop_mq_msg_append_mem(msg, OSR_SET_MULTIPLE_ATTR_IMMEDIATE_KEY, OSR_SET_MULTIPLE_ATTR_IMMEDIATE_SIZE, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, OSR_SET_MULTIPLE_ATTR_KEY, OSR_SET_MULTIPLE_ATTR_SIZE, MQF_MSG_KEEP_DATA);
    }
    osrc_add_creds(os, creds, msg);

    //** Form the heartbeat and handle frames
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    if (is_immediate) {
        OSRC_DEBUG_MQ_PRINTF(msg, "OSRC: SET_ATTR: IMMEDIATE fname=%s\n", ma->path);
        gop_mq_msg_append_mem(msg, ma->path, strlen(ma->path)+1, MQF_MSG_KEEP_DATA);
    } else {
        OSRC_DEBUG_MQ_PRINTF(msg, "OSRC: SET_ATTR: fd=%" PRIdPTR "\n", *(intptr_t *)ofd->data);
        gop_mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);
    }

    //** Form the attribute frame
    nmax = 8;  //** Add just a little extra
    for (i=0; i<ma->n; i++) {
        nmax += strlen(ma->key[i]) + 4 + ma->v_size[i] + 4;
    }
    tbx_type_malloc(data, char, nmax);
    bpos = 0;
    bpos += tbx_zigzag_encode(osrc->timeout, (unsigned char *)&(data[bpos]));
    bpos += tbx_zigzag_encode(ma->n, (unsigned char *)&(data[bpos]));
    for (i=0; i<ma->n; i++) {
        len = strlen(ma->key[i]);
        bpos += tbx_zigzag_encode(len, (unsigned char *)&(data[bpos]));
        memcpy(&(data[bpos]), ma->key[i], len);
        bpos += len;

        bpos += tbx_zigzag_encode(ma->v_size[i], (unsigned char *)&(data[bpos]));
        if (ma->v_size[i] > 0) {
            memcpy(&(data[bpos]), ma->val[i], ma->v_size[i]);
            bpos += ma->v_size[i];
        }
    }
    gop_mq_msg_append_mem(msg, data, bpos, MQF_MSG_AUTO_FREE);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, ma, free, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:SET_ATTRS");

    log_printf(5, "END\n");

    return(gop);

}

//***********************************************************************
// osrc_set_multiple_attrs_immediate - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

gop_op_generic_t *osrc_set_multiple_attrs_immediate(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, char **key, void **val, int *v_size, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->path = path;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;

    return(osrc_set_mult_attrs_internal(os, ma, creds, 1));
}

//***********************************************************************
// osrc_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

gop_op_generic_t *osrc_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;

    return(osrc_set_mult_attrs_internal(os, ma, creds, 0));
}


//***********************************************************************
// osrc_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

gop_op_generic_t *osrc_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
    osrc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, osrc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = &(ma->val_tmp);
    ma->val_tmp = val;
    ma->v_size = &(ma->v_tmp);
    ma->v_tmp = v_size;
    ma->n = 1;


    return(osrc_set_mult_attrs_internal(os, ma, creds, 0));
}


//***********************************************************************
// osrc_next_attr - Returns the next matching attribute
//***********************************************************************

int osrc_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
    osrc_attr_iter_t *it = (osrc_attr_iter_t *)oit;
    int n, err;

    //** Init the return variables
    if (key != NULL) *key = NULL;
    *v_size = -1;
    if ((*v_size <= 0) && (val != NULL)) *val = NULL;

    log_printf(5, "START\n");

    //** Check if already read the last attr
    if (it->no_more_attr == 1) {
        log_printf(5, "Finished No more attrs\n");
        return(-1);
    }

    //** Read the key len
    n = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading key len!\n");
        *v_size = -1;
        it->no_more_attr = 1;
        return(-2);
    }

    //** Check if Last attr. If so return
    if (n <= 0) {
        log_printf(5, "Finished No more attrs\n");
        *v_size = -1;
        it->no_more_attr = 1;
        return(-1);
    }

    //** Valid key so read it
    if (key != NULL) {
        tbx_type_malloc(*key, char, n+1);
        (*key)[n] = 0;
        err = gop_mq_stream_read(it->mqs, *key, n);
    } else {
        err = gop_mq_stream_read(it->mqs, NULL, n); //** Want to drop the key name
    }
    if (err != 0) {
        log_printf(5, "ERROR reading key!");
        if (key != NULL) {
            free(*key);
            *key = NULL;
        }
        *v_size = -1;
        it->no_more_attr = 1;
        return(-2);
    }

    //** Read the value len
    n = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading prefix_len!");
        it->no_more_attr = 1;
        return(-1);
    }

    *v_size = it->v_max;
    osrc_store_val(it->mqs, n, val, v_size);

    //** Do some key/val ptr manging for reporting
    char *k, *v;
    k = (key == NULL) ? NULL : *key;
    v = (val == NULL) ? NULL : (char *)(*val);
    log_printf(5, "key=%s val=%s v_size=%d\n", k, v, *v_size);
    log_printf(5, "END\n");

    return(0);
}

//***********************************************************************
// osrc_response_attr_iter - Handles the create_attr_iter() response
//***********************************************************************

gop_op_status_t osrc_response_attr_iter(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_attr_iter_t *it = (osrc_attr_iter_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)it->os->priv;
    gop_op_status_t status;
    int err;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    it->mqs = gop_mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, gop_mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

    //** Parse the status
    status.op_status = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "op_status=%d\n", status.op_status);
    status.error_code = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "error_code%d\n", status.error_code);

    if (err != 0) {
        status.op_status= OP_STATE_FAILURE;    //** Trigger a failure if error reading from the stream
    }
    if (status.op_status == OP_STATE_FAILURE) {
        gop_mq_stream_destroy(it->mqs);
    } else {
        //** Remove the response from the task to keep it from being freed.
        //** We'll do it manually
        it->response = task->response;
        task->response = NULL;
    }

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}

//***********************************************************************
// osrc_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//***********************************************************************

os_attr_iter_t *osrc_create_attr_iter(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, lio_os_regex_table_t *attr, int v_max)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *ofd = (osrc_object_fd_t *)fd;
    osrc_attr_iter_t *it;
    int bpos, bufsize, again, n, err;
    unsigned char *buffer;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_ATTR_ITER_KEY, OSR_ATTR_ITER_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, ofd->data, ofd->size, MQF_MSG_KEEP_DATA);

    bufsize = 4096;
    tbx_type_malloc(buffer, unsigned char, bufsize);
    do {
        again = 0;
        bpos = 0;

        bpos += tbx_zigzag_encode(osrc->timeout, buffer);
        bpos += tbx_zigzag_encode(v_max, &(buffer[bpos]));

        n = os_regex_table_pack(attr, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        if (again == 1) {
            bufsize = bpos + 10;
            free(buffer);
            tbx_type_malloc(buffer, unsigned char, bufsize);
        }
    } while (again == 1);

    gop_mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


    //** Make the iterator handle
    tbx_type_malloc_clear(it, osrc_attr_iter_t, 1);
    it->os = os;
    it->v_max = v_max;

    //** Make the gop and execute it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_attr_iter, it, NULL, osrc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(5, "ERROR status=%d\n", err);
        free(it);
        return(NULL);
    }
    gop_free(gop, OP_DESTROY);

    log_printf(5, "END\n");

    return(it);
}


//***********************************************************************
// osrc_destroy_attr_iter - Destroys an attribute iterator
//***********************************************************************

void osrc_destroy_attr_iter(os_attr_iter_t *oit)
{
    osrc_attr_iter_t *it = (osrc_attr_iter_t *)oit;

    if (it->mqs != NULL) gop_mq_stream_destroy(it->mqs);
    if ((it->response != NULL) && (it->is_sub_iter == 0)) gop_mq_msg_destroy(it->response);

    free(it);
}


//***********************************************************************
// osrc_next_object - Returns the iterators next matching object
//***********************************************************************

int osrc_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    osrc_object_iter_t *it = (osrc_object_iter_t *)oit;
    osrc_attr_iter_t *ait;
    int i, n, err, ftype;

    log_printf(5, "START\n");

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return(-2);
    } else if (it->finished == 1) {
        *fname = NULL;
        *prefix_len = -1;
        log_printf(5, "No more objects\n");
        return(0);
    }

    ait = NULL;

    //** If a regex attr iter make sure and flush any remaining attrs
    // from the previous object
    if (it->ait != NULL) {
        ait = *(osrc_attr_iter_t **)it->ait;
        if (ait->no_more_attr == 0) {
            do {
                n = os_next_attr(it->os, ait, NULL, NULL, 0);
            } while (n == 0);
        }
    }

    //** Read the object type
    ftype = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading object type!\n");
        return(0);
    }

    //** Last object so return
    if (ftype <= 0) {
        *fname = NULL;
        *prefix_len = -1;
        it->finished = 1;
        log_printf(5, "No more objects\n");
        return(ftype);
    }

    //** Read the prefix len
    *prefix_len = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading prefix_len!");
        return(-1);
    }

    //** Read the object name
    n = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading object len!");
        return(-1);
    }
    tbx_type_malloc(*fname, char, n+1);
    (*fname)[n] = 0;
    err = gop_mq_stream_read(it->mqs, *fname, n);
    if (err != 0) {
        log_printf(5, "ERROR reading fname!");
        free(*fname);
        *fname = NULL;
        return(-1);
    }

    log_printf(5, "ftype=%d fname=%s prefix_len=%d\n", ftype, *fname, *prefix_len);

    if (it->iter_type == OSRC_ITER_ALIST) { //** Now load the fixed attribute list
        for (i=0; i < it->n_keys; i++) {
            if (it->v_size[i] < 0) it->val[i] = NULL;
        }

        for (i=0; i < it->n_keys; i++) {
            n = gop_mq_stream_read_varint(it->mqs, &err);
            if (err != 0) {
                log_printf(5, "ERROR reading attribute #%d!", i);
                return(-1);
            }

            it->v_size[i] = it->v_size_initial[i];
            osrc_store_val(it->mqs, n, &(it->val[i]), &(it->v_size[i]));
            log_printf(15, "val[%d]=%s\n", i, (char *)it->val[i]);
        }
    } else if (it->ait != NULL) {  //It's a regex for the attributes so reset the attr iter
        ait->no_more_attr = 0;
        log_printf(5, "Resetting att iter\n");
    }

    log_printf(5, "END\n");

    return(ftype);
}

//***********************************************************************
// osrc_response_object_iter - Handles a alist/regex iter response
//***********************************************************************

gop_op_status_t osrc_response_object_iter(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_object_iter_t *it = (osrc_object_iter_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)it->os->priv;
    gop_op_status_t status;
    int err;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    it->mqs = gop_mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, gop_mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

    //** Parse the status
    status.op_status = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "op_status=%d\n", status.op_status);
    status.error_code = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "error_code%d\n", status.error_code);

    if (err != 0) {
        status.op_status= OP_STATE_FAILURE;    //** Trigger a failure if error reading from the stream
    }
    if (status.op_status == OP_STATE_FAILURE) {
        gop_mq_stream_destroy(it->mqs);
    } else {
        //** Remove the response from the task to keep it from being freed.
        //** We'll do it manually
        it->response = task->response;
        task->response = NULL;
    }

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
// osrc_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//***********************************************************************

os_object_iter_t *osrc_create_object_iter(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        lio_os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_iter_t *it;
    osrc_attr_iter_t *ait;
    int bpos, bufsize, again, n, err;
    unsigned char *buffer;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_OBJECT_ITER_AREGEX_KEY, OSR_OBJECT_ITER_AREGEX_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    bufsize = 4096;
    tbx_type_malloc(buffer, unsigned char, bufsize);
    do {
        again = 0;
        bpos = 0;

        bpos += tbx_zigzag_encode(osrc->timeout, buffer);
        bpos += tbx_zigzag_encode(recurse_depth, &(buffer[bpos]));
        bpos += tbx_zigzag_encode(object_types, &(buffer[bpos]));
        bpos += tbx_zigzag_encode(v_max, &(buffer[bpos]));

        n = os_regex_table_pack(path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        n = os_regex_table_pack(object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        n = os_regex_table_pack(attr, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;


        if (again == 1) {
            bufsize = bpos + 10;
            free(buffer);
            tbx_type_malloc(buffer, unsigned char, bufsize);
        }
    } while (again == 1);

    gop_mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


    //** Make the iterator handle
    tbx_type_malloc_clear(it, osrc_object_iter_t, 1);
    it->iter_type = OSRC_ITER_AREGEX;
    it->os = os;
    it->v_max = v_max;
    it->ait = it_attr;

    //** Make the gop and execute it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_object_iter, it, NULL, osrc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(5, "ERROR status=%d\n", err);
        free(it);
        return(NULL);
    }
    gop_free(gop, OP_DESTROY);

    //** Go ahead and make the regex attr iter if needed
    if (it_attr != NULL) {
        tbx_type_malloc_clear(ait, osrc_attr_iter_t, 1);
        ait->os = os;
        ait->v_max = v_max;
        ait->is_sub_iter = 1;
        ait->no_more_attr = 1;
        ait->response = it->response;
        ait->mqs = it->mqs;
        *it_attr = ait;
    }

    log_printf(5, "END\n");

    return(it);
}

//***********************************************************************
// osrc_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *osrc_create_object_iter_alist(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_iter_t *it;

    int bpos, bufsize, again, n, i, err;
    unsigned char *buffer;
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_OBJECT_ITER_ALIST_KEY, OSR_OBJECT_ITER_ALIST_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);

    //** Estimate the size of the keys
    n = 0;
    for (i=0; i<n_keys; i++) {
        n += strlen(key[i]);
    }

    bufsize = 4096 + n_keys + 2*n + 1;
    tbx_type_malloc(buffer, unsigned char, bufsize);
    do {
        again = 0;
        bpos = 0;

        bpos += tbx_zigzag_encode(osrc->timeout, buffer);
        bpos += tbx_zigzag_encode(recurse_depth, &(buffer[bpos]));
        bpos += tbx_zigzag_encode(object_types, &(buffer[bpos]));

        bpos += tbx_zigzag_encode(n_keys, &(buffer[bpos]));
        for (i=0; i< n_keys; i++) {
            n = strlen(key[i]);
            bpos += tbx_zigzag_encode(n, &(buffer[bpos]));
            memcpy(&(buffer[bpos]), key[i], n);
            bpos += n;
            bpos += tbx_zigzag_encode(v_size[i], &(buffer[bpos]));
        }

        n = os_regex_table_pack(path, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;

        n = os_regex_table_pack(object_regex, &(buffer[(again==0) ? bpos : 0]), bufsize-bpos);
        if (n < 0) {
            again = 1;
            n = -n;
        }
        bpos += n;


        if (again == 1) {
            bufsize = bpos + 10;
            free(buffer);
            tbx_type_malloc(buffer, unsigned char, bufsize);
        }
    } while (again == 1);

    gop_mq_msg_append_mem(msg, buffer, bpos, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);


    //** Make the iterator handle
    tbx_type_malloc_clear(it, osrc_object_iter_t, 1);
    it->iter_type = OSRC_ITER_ALIST;
    it->os = os;
    it->val = val;
    it->v_size = v_size;
    it->n_keys = n_keys;
    tbx_type_malloc(it->v_size_initial, int, n_keys);
    memcpy(it->v_size_initial, it->v_size, n_keys*sizeof(int));

    //** Make the gop and execute it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_object_iter, it, NULL, osrc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(5, "ERROR status=%d\n", err);
        free(it);
        return(NULL);
    }
    gop_free(gop, OP_DESTROY);

    log_printf(5, "END\n");

    return(it);
}

//***********************************************************************
// osrc_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void osrc_destroy_object_iter(os_object_iter_t *oit)
{
    osrc_object_iter_t *it = (osrc_object_iter_t *)oit;

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return;
    }

    if (it->mqs != NULL) gop_mq_stream_destroy(it->mqs);
    if (it->response != NULL) gop_mq_msg_destroy(it->response);
    if (it->v_size_initial != NULL) free(it->v_size_initial);
    if (it->ait != NULL) free(*(it->ait));

    free(it);
}

//***********************************************************************
// osrc_response_open - Handles an open request response
//***********************************************************************

gop_op_status_t osrc_response_open(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_open_t *arg = (osrc_open_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)arg->os->priv;
    gop_op_status_t status;
    void *data;
    osrc_object_fd_t *fd;

    log_printf(5, "START\n");

    OSRC_DEBUG_MQ_PRINTF(task->response, "MISSING\n");

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);
    if (status.op_status == OP_STATE_SUCCESS) {
        tbx_type_malloc_clear(fd, osrc_object_fd_t, 1);
        fd->os = arg->os;
        gop_mq_get_frame(gop_mq_msg_next(task->response), (void **)&data, &(fd->size));
        tbx_type_malloc(fd->data, char, fd->size);
        memcpy(fd->data, data, fd->size);
        OSRC_DEBUG_NOTIFY("OSRC_GOP: id=%s OPEN: fd=%" PRIdPTR " \n", _b64, *(intptr_t *)fd->data);

        *(arg->pfd) = fd;
        gop_mq_ongoing_host_inc(osrc->ongoing, osrc->remote_host, osrc->host_id, osrc->host_id_len, osrc->heartbeat);
    } else {
        *(arg->pfd) = NULL;
    }

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
//  osrc_open_object - Makes the open file op
//***********************************************************************

gop_op_generic_t *osrc_open_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_open_t *arg;
    gop_op_generic_t *gop;
    mq_msg_t *msg;
    unsigned char buffer[1024];
    unsigned char *sent;
    int n, hlen;

    log_printf(5, "START fname=%s id=%s\n", path, id);
    tbx_type_malloc(arg, osrc_open_t, 1);
    arg->os = os;
    arg->pfd = (osrc_object_fd_t **)pfd;
    hlen = snprintf(arg->handle, 1024, "%s:%d", osrc->host_id, tbx_atomic_global_counter());

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_OPEN_OBJECT_KEY, OSR_OPEN_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    if (id == NULL) {
        gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    } else {
        gop_mq_msg_append_mem(msg, id, strlen(id)+1, MQF_MSG_KEEP_DATA);
    }
    gop_mq_msg_append_mem(msg, path, strlen(path)+1, MQF_MSG_KEEP_DATA);

    //** Same for the mode
    n = tbx_zigzag_encode(mode, buffer);
    n += tbx_zigzag_encode(max_wait, &(buffer[n]));
    tbx_type_malloc(sent, unsigned char, n);
    memcpy(sent, buffer, n);
    gop_mq_msg_append_frame(msg, gop_mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

    //** Form the heartbeat and handle frames
    log_printf(5, "host_id=%s\n", osrc->host_id);
    log_printf(5, "handle=%s hlen=%d\n", arg->handle, hlen);

    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, arg->handle, hlen+1, MQF_MSG_KEEP_DATA);

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "fname=%s\n", path);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_open, arg, free, max_wait);
    gop_set_private(gop, arg);

    tbx_monitor_obj_label(gop_mo(gop), "OS:OPEN path=%s", path);

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
//  osrc_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

gop_op_generic_t *osrc_abort_open_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_open_t *arg = (osrc_open_t *)gop_get_private(gop);
    mq_msg_t *msg;

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_ABORT_OPEN_OBJECT_KEY, OSR_ABORT_OPEN_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, arg->handle, strlen(arg->handle)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "FIXME=MISSING\n");

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    log_printf(5, "END\n");

    return(gop);
}


//***********************************************************************
// osrc_response_close_object - Handles the response to a clos_object call
//***********************************************************************

gop_op_status_t osrc_response_close_object(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_object_fd_t *fd = (osrc_object_fd_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)fd->os->priv;

    gop_op_status_t status;

    log_printf(5, "START\n");

    OSRC_DEBUG_MQ_PRINTF(task->response, "CLOSE: fd=%"PRIdPTR "\n", *(intptr_t *)fd->data);

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);

    //** Quit tracking it
    gop_mq_ongoing_host_dec(osrc->ongoing, osrc->remote_host, osrc->host_id, osrc->host_id_len);

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    free(fd->data);
    free(fd);

    return(status);
}

//***********************************************************************
//  osrc_close_object - Closes the object
//***********************************************************************

gop_op_generic_t *osrc_close_object(lio_object_service_fn_t *os, os_fd_t *ofd)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *fd = (osrc_object_fd_t *)ofd;
    gop_op_generic_t *gop;
    mq_msg_t *msg;

    log_printf(5, "START fd->size=%d\n", fd->size);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_CLOSE_OBJECT_KEY, OSR_CLOSE_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, fd->data, fd->size, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "CLOSE: fd=%"PRIdPTR "\n", *(intptr_t *)fd->data);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_close_object, fd, NULL, osrc->timeout);
    tbx_monitor_obj_label(gop_mo(gop), "OS:CLOSE");

    log_printf(5, "END\n");

    return(gop);
}

//***********************************************************************
//  osrc_lock_user_object - Applies a user lock to the object
//***********************************************************************

gop_op_generic_t *osrc_lock_user_object(lio_object_service_fn_t *os, os_fd_t *ofd, int rw_mode, int max_wait)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_object_fd_t *fd = (osrc_object_fd_t *)ofd;
    osrc_lock_user_t *arg;
    gop_op_generic_t *gop;
    mq_msg_t *msg;
    unsigned char buffer[1024];
    unsigned char *sent;
    int hlen;
    int n;

    tbx_type_malloc(arg, osrc_lock_user_t, 1);
    arg->os = os;
    hlen = snprintf(arg->handle, 1024, "%s:%d", osrc->host_id, tbx_atomic_global_counter());

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_LOCK_USER_OBJECT_KEY, OSR_LOCK_USER_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, osrc->host_id_len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, arg->handle, hlen+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, fd->data, fd->size, MQF_MSG_KEEP_DATA);

    //** Add the mode and wait time
    n = tbx_zigzag_encode(rw_mode, buffer);
    n += tbx_zigzag_encode(max_wait, &(buffer[n]));
    tbx_type_malloc(sent, unsigned char, n);
    memcpy(sent, buffer, n);
    gop_mq_msg_append_frame(msg, gop_mq_frame_new(sent, n, MQF_MSG_AUTO_FREE));

    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "OSRC: FLOCK: fd=%" PRIdPTR " mode=%d max_wait=%d\n", *(intptr_t *)fd->data, rw_mode, max_wait);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, arg, free, max_wait + 10);
    gop_set_private(gop, arg);

    return(gop);
}

//***********************************************************************
//  osrc_abort_lock_user_object - Aborts user lock operation
//***********************************************************************

gop_op_generic_t *osrc_abort_lock_user_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_lock_user_t *arg = (osrc_lock_user_t *)gop_get_private(gop);
    mq_msg_t *msg;

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_ABORT_LOCK_USER_OBJECT_KEY, OSR_ABORT_LOCK_USER_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, arg->handle, strlen(arg->handle)+1, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    OSRC_DEBUG_MQ_PRINTF(msg, "FIXME=MISSING\n");

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    return(gop);
}

//***********************************************************************
//  osrc_fsck_object - Allocates space for the object check
//***********************************************************************

gop_op_generic_t *osrc_fsck_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int ftype, int resolution)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    int n;
    unsigned char buf[32];
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_FSCK_OBJECT_KEY, OSR_FSCK_OBJECT_SIZE, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, fname, strlen(fname), MQF_MSG_KEEP_DATA);

    n = tbx_zigzag_encode(ftype, buf);
    n += tbx_zigzag_encode(resolution, &(buf[n]));
    n += tbx_zigzag_encode(osrc->timeout, &(buf[n]));
    gop_mq_msg_append_mem(msg, buf, n, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_status, os, NULL, osrc->timeout);

    return(gop);
}


//***********************************************************************
// osrc_next_fsck - Returns the next problem object
//***********************************************************************

int osrc_next_fsck(lio_object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)oit;
    int n, err, fsck_err;

    *bad_fname = NULL;
    *bad_atype = 0;
    if (it->finished == 1) return(OS_FSCK_FINISHED);

    //** Read the bad fname len
    n = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading key len!\n");
        it->finished = 1;
        return(OS_FSCK_ERROR);
    }

    //** Check if Last bad object. If so return
    if (n <= 0) {
        log_printf(5, "Finished No more bad objects found\n");
        it->finished = 1;
        return(OS_FSCK_FINISHED);
    }

    //** Valid object name so read it
    tbx_type_malloc(*bad_fname, char, n+1);
    (*bad_fname)[n] = 0;
    err = gop_mq_stream_read(it->mqs, *bad_fname, n);
    if (err != 0) {
        log_printf(5, "ERROR reading key!");
        free(*bad_fname);
        *bad_fname = NULL;
        it->finished = 1;
        return(OS_FSCK_ERROR);
    }

    //** Read the object type
    *bad_atype = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading object type!");
        it->finished = 1;
        return(OS_FSCK_ERROR);
    }

    //** Read the  FSCK error
    fsck_err = gop_mq_stream_read_varint(it->mqs, &err);
    if (err != 0) {
        log_printf(5, "ERROR reading FSCK error!");
        it->finished = 1;
        return(OS_FSCK_ERROR);
    }

    return(fsck_err);
}


//***********************************************************************
// osrc_response_fsck_iter - Handles the create_fsck_iter() response
//***********************************************************************

gop_op_status_t osrc_response_fsck_iter(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)task->arg;
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)it->os->priv;
    gop_op_status_t status;
    int err;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    it->mqs = gop_mq_stream_read_create(osrc->mqc, osrc->ongoing, osrc->host_id, osrc->host_id_len, gop_mq_msg_first(task->response), osrc->remote_host, osrc->stream_timeout);

    //** Parse the status
    status.op_status = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "op_status=%d\n", status.op_status);
    status.error_code = gop_mq_stream_read_varint(it->mqs, &err);
    log_printf(15, "error_code%d\n", status.error_code);

    if (err != 0) {
        status.op_status= OP_STATE_FAILURE;    //** Trigger a failure if error reading from the stream
    }
    if (status.op_status == OP_STATE_FAILURE) {
        gop_mq_stream_destroy(it->mqs);
    } else {
        //** Remove the response from the task to keep it from being freed.
        //** We'll do it manually
        it->response = task->response;
        task->response = NULL;
    }

    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);

    return(status);
}


//***********************************************************************
// osrc_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *osrc_create_fsck_iter(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    osrc_fsck_iter_t *it;
    int err, n;
    unsigned char buf[16];
    mq_msg_t *msg;
    gop_op_generic_t *gop;

    log_printf(5, "START\n");

    //** Form the message
    msg = gop_mq_make_exec_core_msg(osrc->remote_host, 1);
    gop_mq_msg_append_mem(msg, OSR_FSCK_ITER_KEY, OSR_FSCK_ITER_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, osrc->host_id, strlen(osrc->host_id)+1, MQF_MSG_KEEP_DATA);
    osrc_add_creds(os, creds, msg);
    gop_mq_msg_append_mem(msg, path, strlen(path), MQF_MSG_KEEP_DATA);

    n = tbx_zigzag_encode(mode, buf);
    n += tbx_zigzag_encode(osrc->timeout, &(buf[n]));
    gop_mq_msg_append_mem(msg, buf, n, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the iterator handle
    tbx_type_malloc_clear(it, osrc_fsck_iter_t, 1);
    it->os = os;
    it->mode = mode;

    //** Make the gop and execute it
    gop = gop_mq_op_new(osrc->mqc, msg, osrc_response_fsck_iter, it, NULL, osrc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(5, "ERROR status=%d\n", err);
        free(it);
        return(NULL);
    }
    gop_free(gop, OP_DESTROY);

    return(it);
}


//***********************************************************************
// osrc_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void osrc_destroy_fsck_iter(lio_object_service_fn_t *os, os_fsck_iter_t *oit)
{
    osrc_fsck_iter_t *it = (osrc_fsck_iter_t *)oit;

    if (it->mqs != NULL) gop_mq_stream_destroy(it->mqs);
    if (it->response != NULL) gop_mq_msg_destroy(it->response);

    free(it);
}


//***********************************************************************
// osrc_print_running_config - Prints the running config
//***********************************************************************

void osrc_print_running_config(lio_object_service_fn_t *os, FILE *fd, int print_section_heading)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;
    char  text[1024];

    if (print_section_heading) fprintf(fd, "[%s]\n", osrc->section);
    fprintf(fd, "type = %s\n", OS_TYPE_REMOTE_CLIENT);
    fprintf(fd, "os_temp = %s\n", osrc->temp_section);
    fprintf(fd, "remote_address = %s\n", osrc->remote_host_string);
    fprintf(fd, "timeout = %d #seconds\n", osrc->timeout);
    fprintf(fd, "heartbeat = %d #seconds\n", osrc->heartbeat);
    fprintf(fd, "stream_timeout = %d #seconds\n", osrc->stream_timeout);
    fprintf(fd, "spin_interval = %d #seconds\n", osrc->spin_interval);
    fprintf(fd, "spin_fail = %d\n", osrc->spin_fail);
    fprintf(fd, "max_stream = %s\n", tbx_stk_pretty_print_int_with_scale(osrc->max_stream, text));
    fprintf(fd, "\n");

     if (osrc->os_remote) os_print_running_config(osrc->os_remote, fd, 1);
}

//***********************************************************************
// os_remote_client_destroy
//***********************************************************************

void osrc_destroy(lio_object_service_fn_t *os)
{
    lio_osrc_priv_t *osrc = (lio_osrc_priv_t *)os->priv;

    if (osrc->os_remote != NULL) {
        os_destroy(osrc->os_remote);
    }

    gop_mq_msg_destroy(osrc->remote_host);
    free(osrc->section);
    if (osrc->temp_section) free(osrc->temp_section);
    free(osrc->remote_host_string);
    free(osrc);
    free(os);
}


//***********************************************************************
//  object_service_remote_client_create - Creates a remote client OS
//***********************************************************************

lio_object_service_fn_t *object_service_remote_client_create(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section)
{
    lio_object_service_fn_t *os;
    lio_osrc_priv_t *osrc;
    char *str;
    int error;

    log_printf(10, "START\n");
    if (section == NULL) section = osrc_default_options.section;

    tbx_type_malloc_clear(os, lio_object_service_fn_t, 1);
    tbx_type_malloc_clear(osrc, lio_osrc_priv_t, 1);
    os->priv = (void *)osrc;

    osrc->section = strdup(section);

    str = tbx_inip_get_string(fd, section, "os_temp", osrc_default_options.temp_section);
    if (str != NULL) {  //** Running in test/temp
        log_printf(0, "NOTE: Running in debug mode by loading Remote server locally!\n");
        osrc->os_remote = object_service_remote_server_create(ess, fd, str);
        FATAL_UNLESS(osrc->os_remote != NULL);
        osrc->os_temp = ((lio_osrs_priv_t *)(osrc->os_remote->priv))->os_child;
        free(str);
    } else {
        osrc->authn = lio_lookup_service(ess, ESS_RUNNING, ESS_AUTHN);
    }

    osrc->timeout = tbx_inip_get_integer(fd, section, "timeout", osrc_default_options.timeout);
    osrc->heartbeat = tbx_inip_get_integer(fd, section, "heartbeat", osrc_default_options.heartbeat);
    osrc->remote_host_string = tbx_inip_get_string_full(fd, section, "remote_address", osrc_default_options.remote_host_string, &error);
    osrc->remote_host = gop_mq_string_to_address(osrc->remote_host_string);

    osrc->max_stream = tbx_inip_get_integer(fd, section, "max_stream", osrc_default_options.max_stream);
    osrc->stream_timeout = tbx_inip_get_integer(fd, section, "stream_timeout", osrc_default_options.stream_timeout);
    osrc->spin_interval = tbx_inip_get_integer(fd, section, "spin_interval", osrc_default_options.spin_interval);
    osrc->spin_fail = tbx_inip_get_integer(fd, section, "spin_fail", osrc_default_options.spin_fail);

    apr_pool_create(&osrc->mpool, NULL);
    apr_thread_mutex_create(&(osrc->lock), APR_THREAD_MUTEX_DEFAULT, osrc->mpool);
    apr_thread_cond_create(&(osrc->cond), osrc->mpool);

    //** Get the host ID
    osrc->host_id = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_HOST_ID);FATAL_UNLESS(osrc->host_id != NULL);
    osrc->host_id_len = strlen(osrc->host_id)+1;

    //** Get the MQC
    osrc->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ);FATAL_UNLESS(osrc->mqc != NULL);

    //** Get the Global ongoing handle
    osrc->ongoing = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_CLIENT);FATAL_UNLESS(osrc->ongoing != NULL);

    //** Get the thread pool to use
    osrc->tpc = lio_lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);FATAL_UNLESS(osrc->tpc != NULL);

    //** Get the notify handle
    osrc->notify = lio_lookup_service(ess, ESS_RUNNING, ESS_NOTIFY);FATAL_UNLESS(osrc->notify != NULL);

    //** Set up the fn ptrs
    os->type = OS_TYPE_REMOTE_CLIENT;

    os->print_running_config = osrc_print_running_config;
    os->destroy_service = osrc_destroy;
    os->exists = osrc_exists;
    os->realpath = osrc_realpath;
    os->exec_modify = osrc_object_exec_modify;
    os->create_object = osrc_create_object;
    os->create_object_with_attrs = osrc_create_object_with_attrs;
    os->remove_object = osrc_remove_object;
    os->remove_regex_object = osrc_remove_regex_object;
    os->abort_remove_regex_object = osrc_abort_remove_regex_object;
    os->move_object = osrc_move_object;
    os->symlink_object = osrc_symlink_object;
    os->hardlink_object = osrc_hardlink_object;
    os->create_object_iter = osrc_create_object_iter;
    os->create_object_iter_alist = osrc_create_object_iter_alist;
    os->next_object = osrc_next_object;
    os->destroy_object_iter = osrc_destroy_object_iter;
    os->open_object = osrc_open_object;
    os->close_object = osrc_close_object;
    os->abort_open_object = osrc_abort_open_object;

    os->lock_user_object = osrc_lock_user_object;
    os->abort_lock_user_object = osrc_abort_lock_user_object;

    os->get_attr = osrc_get_attr;
    os->set_attr = osrc_set_attr;
    os->symlink_attr = osrc_symlink_attr;
    os->copy_attr = osrc_copy_attr;
    os->get_multiple_attrs = osrc_get_multiple_attrs;
    os->get_multiple_attrs_immediate = osrc_get_multiple_attrs_immediate;
    os->set_multiple_attrs = osrc_set_multiple_attrs;
    os->set_multiple_attrs_immediate = osrc_set_multiple_attrs_immediate;
    os->copy_multiple_attrs = osrc_copy_multiple_attrs;
    os->symlink_multiple_attrs = osrc_symlink_multiple_attrs;
    os->move_attr = osrc_move_attr;
    os->move_multiple_attrs = osrc_move_multiple_attrs;
    os->regex_object_set_multiple_attrs = osrc_regex_object_set_multiple_attrs;
    os->abort_regex_object_set_multiple_attrs = osrc_abort_regex_object_set_multiple_attrs;
    os->create_attr_iter = osrc_create_attr_iter;
    os->next_attr = osrc_next_attr;
    os->destroy_attr_iter = osrc_destroy_attr_iter;

    os->create_fsck_iter = osrc_create_fsck_iter;
    os->destroy_fsck_iter = osrc_destroy_fsck_iter;
    os->next_fsck = osrc_next_fsck;
    os->fsck_object = osrc_fsck_object;

    log_printf(10, "END\n");

    return(os);
}

