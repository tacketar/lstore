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

#ifndef ACCRE_GOP_PORTAL_H_INCLUDED
#define ACCRE_GOP_PORTAL_H_INCLUDED

#include <apr_hash.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/visibility.h>
#include <gop/types.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/log.h>
#include <tbx/network.h>
#include <tbx/pigeon_coop.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

#ifdef __cplusplus
extern "C" {
#endif

//Separator between the host and the port
#define HP_HOSTPORT_SEPARATOR "|"

// Typedefs
typedef void *(*gop_portal_dup_fn_t)(void *connect_context);  //** Duplicates a ccon
typedef void (*gop_portal_destroy_fn_t)(void *connect_context);
typedef int (*gop_portal_connect_fn_t)(tbx_ns_t *ns, void *connect_context, char *host, int port, tbx_ns_timeout_t timeout);
typedef void (*gop_portal_close_fn_t)(tbx_ns_t *ns);
typedef void (*gop_portal_sort_fn_t)(void *arg, gop_opque_t *q);        //** optional
typedef void (*gop_portal_submit_fn_t)(void *arg, gop_op_generic_t *op);
typedef void *(*gop_portal_exec_fn_t)(void *arg, gop_op_generic_t *op);   //** optional

struct gop_portal_fn_t {
    gop_portal_dup_fn_t dup_connect_context;
    gop_portal_destroy_fn_t destroy_connect_context;
    gop_portal_connect_fn_t connect;
    gop_portal_close_fn_t close_connection;
    gop_portal_sort_fn_t sort_tasks;
    gop_portal_submit_fn_t submit;
    gop_portal_exec_fn_t sync_exec;
};

struct gop_portal_context_t;
typedef struct gop_host_portal_t gop_host_portal_t;

// Functions
GOP_API int gop_hp_que_op_submit(gop_portal_context_t *hpc, gop_op_generic_t *op);
GOP_API gop_portal_context_t *gop_hp_context_create(gop_portal_fn_t *hpi, char *name);
GOP_API void gop_hp_context_destroy(gop_portal_context_t *hpc);
GOP_API void gop_hpc_load(gop_portal_context_t *hpc, tbx_inip_file_t *fd, char *section);
GOP_API gop_portal_fn_t *gop_hp_fn_get(gop_portal_context_t *hpc);
GOP_API void gop_hp_fn_set(gop_portal_context_t *hpc, gop_portal_fn_t *fn);
GOP_API int gop_hp_que_op_submit(gop_portal_context_t *hpc, gop_op_generic_t *op);
GOP_API void gop_hp_shutdown(gop_portal_context_t *hpc);
GOP_API int gop_hp_submit(gop_host_portal_t *dp, gop_op_generic_t *op, bool addtotop, bool release_master);
GOP_API void gop_hpc_print_running_config(gop_portal_context_t *hpc, FILE *fd, int print_section_heading);

// tunable accessors
GOP_API void gop_hpc_dead_dt_set(gop_portal_context_t *hpc, apr_time_t dt);
GOP_API apr_time_t gop_hpct_dead_dt_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_dead_check_set(gop_portal_context_t *hpc, apr_time_t dt);
GOP_API apr_time_t gop_hpc_dead_check_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_max_idle_set(gop_portal_context_t *hpc, apr_time_t dt);
GOP_API apr_time_t gop_hpc_max_idle_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_wait_stable_set(gop_portal_context_t *hpc, apr_time_t dt);
GOP_API apr_time_t gop_hpc_wait_stable_get(gop_portal_context_t *hpc);

GOP_API void gop_hpc_max_total_conn_set(gop_portal_context_t *hpc, int n);
GOP_API int gop_hpc_max_total_conn_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_min_host_conn_set(gop_portal_context_t *hpc, int n);
GOP_API int gop_hpc_min_host_conn_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_max_host_conn_set(gop_portal_context_t *hpc, int n);
GOP_API int gop_hpc_max_host_conn_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_max_workload_set(gop_portal_context_t *hpc, int64_t n);
GOP_API int64_t gop_hpc_max_workload_get(gop_portal_context_t *hpc);

GOP_API void gop_hpc_min_bw_fraction_set(gop_portal_context_t *hpc, double d);
GOP_API double gop_hpc_min_bw_fraction_get(gop_portal_context_t *hpc);
GOP_API void gop_hpc_mex_latest_fraction_set(gop_portal_context_t *hpc, double d);
GOP_API double gop_hpc_mix_latest_fraction_get(gop_portal_context_t *hpc);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_GOP_PORTAL_H_INCLUDED ^ */
