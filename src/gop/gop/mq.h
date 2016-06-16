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

#ifndef ACCRE_GOP_MQ_PORTAL_H_INCLUDED
#define ACCRE_GOP_MQ_PORTAL_H_INCLUDED

#include <apr_hash.h>
#include <apr_thread_proc.h>
#include <gop/gop.h>
#include <gop/visibility.h>
#include <gop/types.h>
#include <tbx/iniparse.h>
#include <tbx/stack.h>
#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct mq_command_stats_t mq_command_stats_t;
typedef struct mq_command_t mq_command_t;
typedef struct mq_command_table_t mq_command_table_t;
typedef struct mq_conn_t mq_conn_t;
typedef struct mq_context_t mq_context_t;
typedef struct mq_frame_t mq_frame_t;
typedef struct mq_heartbeat_entry_t mq_heartbeat_entry_t;
typedef struct mq_msg_hash_t mq_msg_hash_t;
typedef struct mq_portal_t mq_portal_t;
typedef struct mq_socket_context_t mq_socket_context_t;
typedef struct mq_socket_t mq_socket_t;
typedef struct mq_task_monitor_t mq_task_monitor_t;
typedef struct mq_task_t mq_task_t;
typedef tbx_stack_t mq_msg_t;
typedef int mq_pipe_t;       //** Event notification FD
typedef void (gop_mq_exec_fn_t)(void *arg, mq_task_t *task);
typedef void (*gop_mq_task_arg_free_fn_t)(void *arg);  //** Function for cleaning up the GOP arg. (GOP)

// Functions
GOP_API void gop_mq_apply_return_address_msg(mq_msg_t *msg, mq_msg_t *raw_address, int dup_frames);
GOP_API void gop_mq_command_set(mq_command_table_t *table, void *cmd, int cmd_size, void *arg, gop_mq_exec_fn_t *fn);
GOP_API void gop_mq_command_table_set_default(mq_command_table_t *table, void *arg, gop_mq_exec_fn_t *fn);
GOP_API mq_context_t *gop_mq_create_context(tbx_inip_file_t *ifd, char *section);
GOP_API void gop_mq_destroy_context(mq_context_t *mqp);
GOP_API void gop_mq_frame_destroy(mq_frame_t *f);
GOP_API mq_frame_t *gop_mq_frame_new(void *data, int len, int auto_free);
GOP_API void gop_mq_frame_set(mq_frame_t *f, void *data, int len, int auto_free);
GOP_API char *gop_mq_frame_strdup(mq_frame_t *f);
GOP_API int gop_mq_get_frame(mq_frame_t *f, void **data, int *size);
GOP_API char *gop_mq_id2str(char *id, int id_len, char *str, int str_len);
GOP_API void gop_mq_msg_append_frame(mq_msg_t *msg, mq_frame_t *f);
GOP_API void gop_mq_msg_append_mem(mq_msg_t *msg, void *data, int len, int auto_free);
GOP_API void gop_mq_msg_append_msg(mq_msg_t *msg, mq_msg_t *extra, int mode);
GOP_API mq_frame_t *gop_mq_msg_current(mq_msg_t *msg);
GOP_API void gop_mq_msg_destroy(mq_msg_t *msg);
GOP_API mq_frame_t *gop_mq_msg_first(mq_msg_t *msg);
GOP_API mq_frame_t *gop_mq_msg_last(mq_msg_t *msg);
GOP_API mq_msg_t *gop_mq_msg_new();
GOP_API mq_frame_t *gop_mq_msg_next(mq_msg_t *msg);
GOP_API mq_frame_t *gop_mq_msg_pluck(mq_msg_t *msg, int move_up);
GOP_API op_generic_t *gop_mq_op_new(mq_context_t *ctx, mq_msg_t *msg, op_status_t (*fn_response)(void *arg, int id), void *arg, gop_mq_task_arg_free_fn_t my_arg_free, int dt);
GOP_API mq_command_table_t *gop_mq_portal_command_table(mq_portal_t *portal);
GOP_API mq_portal_t *gop_mq_portal_create(mq_context_t *mqc, char *host, int connect_mode);
GOP_API void gop_mq_portal_destroy(mq_portal_t *p);
GOP_API int gop_mq_portal_install(mq_context_t *mqc, mq_portal_t *p);
GOP_API mq_portal_t *gop_mq_portal_lookup(mq_context_t *mqc, char *host, int connect_mode);
GOP_API void gop_mq_portal_remove(mq_context_t *mqc, mq_portal_t *p);
GOP_API int gop_mq_submit(mq_portal_t *p, mq_task_t *task);
GOP_API mq_task_t *gop_mq_task_new(mq_context_t *ctx, mq_msg_t *msg, op_generic_t *gop, void *arg, int dt);

// Preprocessor constants
#define MQF_MSG_AUTO_FREE     0  //** Auto free data on destroy
#define MQF_MSG_KEEP_DATA     1  //** Skip free'ing of data on destroy.  App is responsible.
#define MQF_MSG_INTERNAL_FREE 2  //** The msg routines are responsible for free'ing the data. Used on mq_recv().

#define MQ_CMODE_CLIENT  0     //** Normal outgoing connection
#define MQ_CMODE_SERVER  1     //** USed by servers for incoming connections

#define MQF_VERSION_KEY        "LMQv100"
#define MQF_VERSION_SIZE       7
#define MQF_PING_KEY           "\001"
#define MQF_PING_SIZE          1
#define MQF_PONG_KEY           "\002"
#define MQF_PONG_SIZE          1
#define MQF_EXEC_KEY           "\003"
#define MQF_EXEC_SIZE          1
#define MQF_TRACKEXEC_KEY      "\004"
#define MQF_TRACKEXEC_SIZE     1
#define MQF_TRACKADDRESS_KEY   "\005"
#define MQF_TRACKADDRESS_SIZE  1
#define MQF_RESPONSE_KEY       "\006"
#define MQF_RESPONSE_SIZE      1

// Preprocessor macros
#define mq_data_compare(A, sA, B, sB) (((sA) == (sB)) ? memcmp(A, B, sA) : 1)
#define mq_msg_pop(A) (mq_frame_t *)tbx_stack_pop(A)

// Exported types. To be obscured.
struct mq_task_t {      //** Generic containter for MQ messages for both the server and GOP (or client). If the variable is not used it's value is NULL.
    mq_msg_t *msg;          //** Actual message to send with address (Server+GOP)
    mq_msg_t *response;     //** Response message (GOP)
    op_generic_t *gop;      //** GOP corresponding to the task.  This could be NULL if a direct submission is used (GOP)
    mq_context_t *ctx;      //** Portal context for sending responses. (Server+GOP)
    void *arg;              //** Optional argument when calling mq_command_add() or gop_mq_op_new() (server+GOP)
    apr_time_t timeout;     //** Initially the DT in sec for the command to complete and converted to abs timeout when sent
    gop_mq_task_arg_free_fn_t my_arg_free;
    int pass_through;       //** Flag to set when a task is only used to pass a message; no heartbeating necessary
};

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_GOP_MQ_PORTAL_H_INCLUDED ^ */ 
