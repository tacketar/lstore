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

#ifndef _SEGMENT_H_INCLUDED
#define _SEGMENT_H_INCLUDED

#include <sodium.h>
#include <lio/segment.h>

typedef struct {
    ex_off_t chunk_size;
    ex_off_t stripe_size;
    ex_off_t crypt_chunk_scale;
    char *crypt_nonce;
    char *crypt_key;
} crypt_info_t;

typedef struct {
    crypt_info_t *info;
    ex_tbx_iovec_t *ex_iov;
    char *crypt_buffer;
    tbx_tbuf_t tbuf_crypt;
    int n_ex;
    int crypt_flush;
    ex_off_t prev_bufoff;
    ex_off_t prev_lunoff;
    ex_off_t slot_total_pos;
    ex_off_t curr_slot;
    ex_off_t *lun_offset;
} crypt_rw_t;

typedef struct {
    lio_data_block_t *data;    //** Data block
    ex_off_t cap_offset;  //** Starting location to use data in the cap
    ex_off_t block_len;
} segment_block_inspect_t;

typedef ssize_t (*segment_copy_read_fn_t)(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset);
typedef ssize_t (*segment_copy_write_fn_t)(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset);

gop_op_generic_t *segment_put_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, segment_copy_read_fn_t cp_read, FILE *fd, lio_segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout);
gop_op_generic_t *segment_get_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, segment_copy_write_fn_t cp_write, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout);
lio_segment_t *load_segment(lio_service_manager_t *ess, ex_id_t id, lio_exnode_exchange_t *ex);

//** Data placement helpers
int segment_placement_check(lio_resource_service_fn_t *rs, data_attr_t *da, segment_block_inspect_t *block, int *block_status, int n_blocks, int soft_error_fail, rs_query_t *query, lio_inspect_args_t *args, int timeout);
int segment_placement_fix(lio_resource_service_fn_t *rs, data_attr_t *da, segment_block_inspect_t *block, int *block_status, int n_blocks, lio_inspect_args_t *args, int timeout, tbx_stack_t **db_cleanup);

ex_off_t math_gcd(ex_off_t a, ex_off_t b);
ex_off_t math_lcm(ex_off_t a, ex_off_t b);

//** Encryption at rest helpers
#define SEGMENT_CRYPT_KEY_LEN   crypto_stream_xchacha20_KEYBYTES
#define SEGMENT_CRYPT_NONCE_LEN crypto_stream_xchacha20_NONCEBYTES

void crypt_newkeys(char **key, char **nonce);
char *crypt_bin2etext(char *bin, int len);
char *crypt_etext2bin(char *etext, int len);
int crypt_loadkeys(crypt_info_t *cinfo, tbx_inip_file_t *fd, const char *grp, int ok_to_generate_keys, ex_off_t chunk_size, ex_off_t stripe_size);
void crypt_destroykeys(crypt_info_t *cinfo);
void crypt_regenkeys(crypt_info_t *cinfo);

int crypt_read_op_next_block(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv);
int crypt_write_op_next_block(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv);

// Preprocessor macros
#define lio_segment_type(s) (s)->header.type
#define segment_clone(s, da, clone_ex, mode, attr, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->clone(s, da, clone_ex, mode, attr, to)  //** FIXME after segment hints added
#define segment_deserialize(s, id, exp) ((lio_segment_vtable_t *)(s)->obj.vtable)->deserialize(s, id, exp)
#define segment_get_gop_header(seg) &((seg)->header)
#define segment_lock(s) apr_thread_mutex_lock((s)->lock)
#define segment_remove(s, da, to) ((lio_segment_vtable_t *)(s)->obj.vtable)->remove(s, da, to) //** FIXME after segment hints added
#define segment_serialize(s, exp) ((lio_segment_vtable_t *)(s)->obj.vtable)->serialize(s, exp)
#define segment_set_header(seg, new_head) (seg)->header = *(new_head)
#define segment_unlock(s) apr_thread_mutex_unlock((s)->lock)

#endif
