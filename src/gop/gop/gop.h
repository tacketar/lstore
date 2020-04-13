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

/** \file
* Autogenerated public API
*/

#ifndef ACCRE_GOP_GOP_H_INCLUDED
#define ACCRE_GOP_GOP_H_INCLUDED

#include <apr_time.h>
#include <gop/callback.h>
#include <gop/visibility.h>
#include <gop/types.h>
#include <stdbool.h>
#include <tbx/atomic_counter.h>
#include <tbx/network.h>
#include <tbx/stack.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef void (*gop_op_free_fn_t)(gop_op_generic_t *d, int mode);
typedef gop_op_status_t (*gop_op_send_command_fn_t)(gop_op_generic_t *gop, tbx_ns_t *ns);
typedef gop_op_status_t (*gop_op_send_phase_fn_t)(gop_op_generic_t *gop, tbx_ns_t *ns);
typedef gop_op_status_t (*gop_op_recv_phase_fn_t)(gop_op_generic_t *gop, tbx_ns_t *ns);
typedef int (*gop_op_on_submit_fn_t)(tbx_stack_t *stack, tbx_stack_ele_t *gop_ele);
typedef int (*gop_op_before_exec_fn_t)(gop_op_generic_t *gop);
typedef int (*gop_op_destroy_command_fn_t)(gop_op_generic_t *gop);
typedef enum gop_op_state_t gop_op_state_t;
enum gop_op_state_t {
    OP_STATE_SUCCESS = 10,
    OP_STATE_FAILURE = 20,
    OP_STATE_RETRY = 30,
    OP_STATE_DEAD = 40,
    OP_STATE_TIMEOUT = 50,
    OP_STATE_INVALID_HOST = 60,
    OP_STATE_CANT_CONNECT = 70,
    OP_STATE_ERROR = 80,
};
typedef enum gop_op_type_t gop_op_type_t;
enum gop_op_type_t {
    Q_TYPE_OPERATION = 50,
    Q_TYPE_QUE = 51,
};

typedef enum gop_op_free_mode_t gop_op_free_mode_t;
enum gop_op_free_mode_t {
    OP_FINALIZE = -10,
    OP_DESTROY = -20,
};
typedef enum gop_fm_t gop_fm_t;
enum gop_fm_t {
    OP_FM_FORCED = 11,
    OP_FM_GET_END = 22,
};

typedef enum gop_op_exec_mode_t gop_op_exec_mode_t;
//enum gop_op_exec_mode_t {
//    OP_EXEC_QUEUE,
//    OP_EXEC_DIRECT,
//};

// Functions
GOP_API void gop_mark_completed(gop_op_generic_t *gop, gop_op_status_t status);
GOP_API void gop_callback_append(gop_op_generic_t *q, gop_callback_t *cb);
GOP_API int gop_completed_successfully(gop_op_generic_t *gop);
GOP_API gop_op_generic_t *gop_dummy(gop_op_status_t state);
GOP_API apr_time_t gop_time_exec(gop_op_generic_t *gop);
GOP_API void gop_finished_submission(gop_op_generic_t *gop);
GOP_API void gop_free(gop_op_generic_t *gop, gop_op_free_mode_t mode);
GOP_API void gop_generic_free(gop_op_generic_t *gop, gop_op_free_mode_t mode);
GOP_API gop_op_generic_t *gop_get_next_failed(gop_op_generic_t *gop);
GOP_API gop_op_generic_t *gop_get_next_finished(gop_op_generic_t *gop);
GOP_API void gop_init(gop_op_generic_t *gop);
GOP_API void gop_reset(gop_op_generic_t *gop);
GOP_API void gop_set_auto_destroy(gop_op_generic_t *gop, int val);
GOP_API void gop_start_execution(gop_op_generic_t *gop);
GOP_API void gop_auto_destroy_exec(gop_op_generic_t *gop);
GOP_API int gop_sync_exec(gop_op_generic_t *gop);
GOP_API gop_op_status_t gop_sync_exec_status(gop_op_generic_t *gop);
GOP_API int gop_tasks_failed(gop_op_generic_t *gop);
GOP_API int gop_tasks_finished(gop_op_generic_t *gop);
GOP_API int gop_tasks_left(gop_op_generic_t *gop);
GOP_API int gop_waitall(gop_op_generic_t *gop);
GOP_API gop_op_generic_t *gop_waitany(gop_op_generic_t *gop);
GOP_API gop_op_generic_t *gop_waitany_timed(gop_op_generic_t *g, int dt);
GOP_API apr_time_t gop_time_start(gop_op_generic_t *gop);
GOP_API apr_time_t gop_time_end(gop_op_generic_t *gop);
GOP_API apr_time_t gop_time_exec(gop_op_generic_t *gop);

// Preprocessor constants
// Global constants
GOP_API extern gop_op_status_t gop_success_status;
GOP_API extern gop_op_status_t gop_failure_status;
extern gop_op_status_t op_retry_status;
extern gop_op_status_t op_dead_status;
extern gop_op_status_t op_timeout_status;
extern gop_op_status_t op_invalid_host_status;
extern gop_op_status_t op_cant_connect_status;
GOP_API extern gop_op_status_t gop_error_status;

// Preprocessor macros
#define _op_set_status(v, opstat, errcode) { (v).op_status = opstat; (v).error_code = errcode; }
#define lock_gop(gop)   log_printf(15, "lock_gop: gid=%d\n", (gop)->base.id); apr_thread_mutex_lock((gop)->base.ctl->lock)
#define unlock_gop(gop) log_printf(15, "unlock_gop: gid=%d\n", (gop)->base.id); apr_thread_mutex_unlock((gop)->base.ctl->lock)
#define gop_id(gop) (gop)->base.id
#define gop_get_auto_destroy(gop) (gop)->base.auto_destroy
#define gop_get_private(gop) (gop)->base.user_priv
#define gop_set_private(gop, newval) (gop)->base.user_priv = newval
#define gop_get_id(gop) (gop)->base.id
#define gop_set_id(gop, newval) (gop)->base.id = newval
#define gop_get_myid(gop) (gop)->base.my_id
#define gop_set_myid(gop, newval) (gop)->base.my_id = newval
#define gop_get_type(gop) (gop)->type
#define gop_get_status(gop) (gop)->base.status
#define gop_set_status(gop, val) (gop)->base.status = val

// Exported types. To be obscured.
struct gop_op_status_t {       //** Generic opcode status
    gop_op_state_t op_status;          //** Simplified operation status, OP_SUCCESS or OP_FAILURE
    int error_code;         //** Low level op error code
};

struct gop_op_common_t {
    gop_callback_t *cb;        //** Optional callback
    gop_opque_t *parent_q;     //** Parent que attached to
    gop_op_status_t status;    //** Command result
    gop_fm_t failure_mode;      //** Used via the callbacks to force a failure, even on success
    int retries;           //** Upon failure how many times we've retried
    int id;                //** Op's global id.  Can be changed by use but generally should use my_id
    int my_id;             //** User/Application settable id.  Defaults to id.
    bool state;             //** Command state 0=submitted 1=completed
    bool started_execution; //** If 1 the tasks have already been submitted for execution
    bool auto_destroy;      //** If 1 then automatically call the free fn to destroy the object
    gop_control_t *ctl;    //** Lock and condition struct
    void *user_priv;           //** Optional user supplied handle
    gop_op_free_fn_t free;
    gop_portal_context_t *pc;
};

struct gop_op_generic_t {
    gop_op_type_t type;
    void *free_ptr;
    gop_op_common_t base;
    gop_que_data_t   *q;
    gop_op_data_t   *op;
};

struct gop_command_op_t {   //** Command operation
    char *hostport; //** Depot hostname:port:type:...  Unique string for host/connect_context
    void *connect_context;   //** Private information needed to make a host connection
    int  cmp_size;  //** Used for ordering commands within the same host
    apr_time_t timeout;    //** Command timeout
    apr_time_t retry_wait; //** How long to wait in case of a dead socket, if 0 then retry immediately
    int64_t workload;   //** Workload for measuring channel usage
    int retry_count;//** Number of times retried
    gop_op_send_command_fn_t send_command;
    gop_op_send_phase_fn_t send_phase;
    gop_op_recv_phase_fn_t recv_phase;
    gop_op_on_submit_fn_t on_submit;
    gop_op_before_exec_fn_t before_exec;
    gop_op_destroy_command_fn_t destroy_command;
    tbx_stack_t  *coalesced_ops;                                  //** Stores any other coalesced ops
    tbx_atomic_int_t on_top;
    apr_time_t start_time;
    apr_time_t end_time;
};

struct gop_op_data_t {
    gop_portal_context_t *pc;
    gop_command_op_t cmd;
    void *priv;
};


#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_GOP_GOP_H_INCLUDED ^ */ 
