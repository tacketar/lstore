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

#ifndef ACCRE_IBP_IBP_OP_H_INCLUDED
#define ACCRE_IBP_IBP_OP_H_INCLUDED

#include <ibp/visibility.h>
#include <ibp/types.h>
#include <tbx/iniparse.h>
#include <tbx/pigeon_coop.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct ibp_context_t ibp_context_t;
typedef struct ibp_op_alloc_t ibp_op_alloc_t;
typedef struct ibp_op_copy_t ibp_op_copy_t;
typedef struct ibp_op_depot_inq_t ibp_op_depot_inq_t;
typedef struct ibp_op_depot_modify_t ibp_op_depot_modify_t;
typedef struct ibp_op_get_chksum_t ibp_op_get_chksum_t;
typedef struct ibp_op_merge_alloc_t ibp_op_merge_alloc_t;
typedef struct ibp_op_modify_alloc_t ibp_op_modify_alloc_t;
typedef struct ibp_op_probe_t ibp_op_probe_t;
typedef struct ibp_op_rid_inq_t ibp_op_rid_inq_t;
typedef struct ibp_op_rw_t ibp_op_rw_t;
typedef struct ibp_op_t ibp_op_t;
typedef struct ibp_op_validate_chksum_t ibp_op_validate_chksum_t;
typedef struct ibp_op_version_t ibp_op_version_t;
typedef struct ibp_rw_buf_t ibp_rw_buf_t;

// Functions
IBP_API gop_op_generic_t *ibp_proxy_modify_count_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int mode, int captype, int timeout);
IBP_API gop_op_generic_t *ibp_depot_modify_gop(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_off_t hard, ibp_off_t soft, int duration, int timeout);
IBP_API gop_op_generic_t *ibp_rename_gop(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, int timeout);
IBP_API gop_op_generic_t *ibp_merge_alloc_gop(ibp_context_t *ic, ibp_cap_t *mcap, ibp_cap_t *ccap, int timeout);
IBP_API gop_op_generic_t *ibp_split_alloc_gop(ibp_context_t *ic, ibp_cap_t *mcap, ibp_capset_t *caps, ibp_off_t size, ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
IBP_API gop_op_generic_t *ibp_proxy_alloc_gop(ibp_context_t *ic, ibp_capset_t *caps, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration, int timeout);
IBP_API gop_op_generic_t *ibp_proxy_modify_alloc_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, ibp_off_t offset, ibp_off_t size, int duration, int timeout);
IBP_API gop_op_generic_t *ibp_proxy_remove_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_cap_t *mcap, int timeout);
IBP_API gop_op_generic_t *ibp_proxy_probe_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_proxy_capstatus_t *probe, int timeout);
IBP_API gop_op_generic_t *ibp_alloc_gop(ibp_context_t *ic, ibp_capset_t *caps, ibp_off_t size, ibp_depot_t *depot, ibp_attributes_t *attr, int disk_cs_type, ibp_off_t disk_blocksize, int timeout);
IBP_API gop_op_generic_t *ibp_append_gop(ibp_context_t *ic, ibp_cap_t *cap, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API int ibp_cc_type(ibp_connect_context_t *cc);
IBP_API int ibp_chksum_set(ibp_context_t *ic, tbx_ns_chksum_t *ncs);
IBP_API int ibp_config_load(ibp_context_t *ic, tbx_inip_file_t *ifd, char *section);
IBP_API int ibp_config_load_file(ibp_context_t *ic, char *fname, char *section);
IBP_API void ibp_print_running_config( ibp_context_t *ic, FILE *fd, int print_section_heading);
IBP_API ibp_context_t *ibp_context_create();
IBP_API void ibp_context_destroy(ibp_context_t *ic);
IBP_API gop_op_generic_t *ibp_copy_gop(ibp_context_t *ic, int mode, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t dest_offset, ibp_off_t size, int src_timeout, int dest_timeout, int dest_client_timeout);
IBP_API gop_op_generic_t *ibp_copyappend_gop(ibp_context_t *ic, int ns_type, char *path, ibp_cap_t *srccap, ibp_cap_t *destcap, ibp_off_t src_offset, ibp_off_t size, int src_timeout, int  dest_timeout, int dest_client_timeout);
IBP_API gop_op_generic_t *ibp_depot_inq_gop(ibp_context_t *ic, ibp_depot_t *depot, char *password, ibp_depotinfo_t *di, int timeout);
IBP_API gop_op_generic_t *ibp_modify_alloc_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int duration, int reliability, int timeout);
IBP_API gop_op_generic_t *ibp_modify_count_gop(ibp_context_t *ic, ibp_cap_t *cap, int mode, int captype, int timeout);
IBP_API void ibp_op_cc_set(gop_op_generic_t *gop, ibp_connect_context_t *cc);
IBP_API void ibp_op_init(ibp_context_t *ic, ibp_op_t *op);
IBP_API void ibp_op_ncs_set(gop_op_generic_t *gop, tbx_ns_chksum_t *ncs);
IBP_API gop_op_generic_t *ibp_probe_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_capstatus_t *probe, int timeout);
IBP_API gop_op_generic_t *ibp_query_resources_gop(ibp_context_t *ic, ibp_depot_t *depot, ibp_ridlist_t *rlist, int timeout);
IBP_API void ibp_read_cc_set(ibp_context_t *ic, ibp_connect_context_t *cc);
IBP_API gop_op_generic_t *ibp_read_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API gop_op_generic_t *ibp_remove_gop(ibp_context_t *ic, ibp_cap_t *cap, int timeout);
IBP_API gop_op_generic_t *ibp_rw_gop(ibp_context_t *ic, int rw_type, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API void ibp_tcpsize_set(ibp_context_t *ic, int n);
IBP_API gop_op_generic_t *ibp_truncate_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t size, int timeout);
IBP_API gop_op_generic_t *ibp_validate_chksum_gop(ibp_context_t *ic, ibp_cap_t *mcap, int correct_errors, int *n_bad_blocks, int timeout);
IBP_API gop_op_generic_t *ibp_vec_read_gop(ibp_context_t *ic, ibp_cap_t *cap, int n_vec, ibp_tbx_iovec_t *vec, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API gop_op_generic_t *ibp_vec_write_gop(ibp_context_t *ic, ibp_cap_t *cap, int n_iovec, ibp_tbx_iovec_t *iovec, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API char *ibp_version();
IBP_API gop_op_generic_t *ibp_version_gop(ibp_context_t *ic, ibp_depot_t *depot, char *buffer, int buffer_size, int timeout);
IBP_API void ibp_write_cc_set(ibp_context_t *ic, ibp_connect_context_t *cc);
IBP_API gop_op_generic_t *ibp_write_gop(ibp_context_t *ic, ibp_cap_t *cap, ibp_off_t offset, tbx_tbuf_t *buffer, ibp_off_t boff, ibp_off_t len, int timeout);
IBP_API void ibp_set_sync_context(ibp_context_t *ic);
IBP_API gop_op_generic_t *ibp_alloc_chksum_get_gop(ibp_context_t *ic, ibp_cap_t *mcap, int chksum_info_only,
        int *cs_type, int *cs_size, ibp_off_t *blocksize, ibp_off_t *nblocks, ibp_off_t *n_chksumbytes, char *buffer, ibp_off_t bufsize,
        int timeout);
// Config accessor functions
IBP_API int ibp_context_chksum_set(ibp_context_t *ic, tbx_ns_chksum_t *ncs);
IBP_API void ibp_context_chksum_get(ibp_context_t *ic, tbx_ns_chksum_t *ncs);
IBP_API int  ibp_context_tcpsize_get(ibp_context_t *ic);
IBP_API void ibp_context_max_host_conn_set(ibp_context_t *ic, int n);
IBP_API int  ibp_context_max_host_conn_get(ibp_context_t *ic);
IBP_API void ibp_context_command_weight_set(ibp_context_t *ic, int n);
IBP_API int  ibp_context_command_weight_get(ibp_context_t *ic);
IBP_API void ibp_context_max_coalesce_workload_set(ibp_context_t *ic, int64_t n);
IBP_API int64_t  ibp_context_max_coalesce_workload_get(ibp_context_t *ic);
IBP_API void ibp_context_transfer_rate_set(ibp_context_t *ic, double rate);
IBP_API double ibp_context_transfer_rate_get(ibp_context_t *ic);
IBP_API void ibp_context_connection_mode_set(ibp_context_t *ic, int mode);
IBP_API int  ibp_context_connection_mode_get(ibp_context_t *ic);

// Preprocessor constants
#define MAX_KEY_SIZE 256

// Preprocessor macros
#define ibp_get_iop(a) (a)->op->priv

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_IBP_IBP_OP_H_INCLUDED ^ */ 
