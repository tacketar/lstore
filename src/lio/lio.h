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
// Generic LIO functionality
//***********************************************************************

#ifndef _LIO_ABSTRACT_H_
#define _LIO_ABSTRACT_H_

#include <gop/mq.h>
#include <lio/lio.h>
#include <lio/visibility.h>
#include <sys/stat.h>
#include <tbx/lio_monitor.h>
#include <tbx/log.h>
#include <zlib.h>

#include "blacklist.h"

#ifdef __cplusplus
extern "C" {
#endif

void lio_open_files_info_fn(void *arg, FILE *fd);

extern char *_lio_stat_keys[];
#define  _lio_stat_key_size 7

extern char *_lio_exe_name;  //** Executable name

struct lio_unified_object_iter_t {
    lio_path_tuple_t tuple;
    os_object_iter_t *oit;
    local_object_iter_t *lit;
};

#define LIO_WRITE_MODE     2
#define LIO_TRUNCATE_MODE  4
#define LIO_CREATE_MODE    8
#define LIO_APPEND_MODE   16
#define LIO_EXCL_MODE     32
#define LIO_RW_MODE       (LIO_READ_MODE|LIO_WRITE_MODE)

#define lio_lock(s) apr_thread_mutex_lock((s)->lock)
#define lio_unlock(s) apr_thread_mutex_unlock((s)->lock)

struct wq_context_s;
typedef struct wq_context_s wq_context_t;

typedef struct {  //** This is used for small I/O buffering to minimize having to hit the cache which is designed for multi-megabyte I/O
    tbx_tbuf_t tbuf;
    ex_off_t max_size;
    ex_off_t used;
    ex_off_t offset;
    ex_off_t offset_end;
    int is_dirty;
    char buf[];
} stream_buf_t;

struct lio_file_handle_t {  //** Shared file handle
    lio_exnode_t *ex;
    lio_segment_t *seg;
    lio_config_t *lc;
    wq_context_t *wq_ctx;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
    stream_buf_t *stream;
    tbx_mon_object_t mo;
    char *fname;         //** This is just used for dumping info and represents the name used for adding the FH
    char *data;          //** This holds small file data which gets stored as metadata
    ex_off_t data_size;  //** And if this is the corresponding size in data
    ex_off_t max_data_allocated;  //** This is how much is allocated already
    ex_id_t ino;
    ex_id_t sid;
    int in_flight;
    int adjust_tier_pending;
    int ref_count;
    int ref_write;
    int ref_read;
    int remove_on_close;
    int is_special;
    ex_off_t readahead_end;
    tbx_atomic_int_t modified;
    tbx_list_t *write_table;
};

typedef struct {
    ex_off_t offset;
    ex_off_t len;
    uLong adler32;
} lfs_adler32_t;

typedef struct {
    lio_fd_t *fd;
    int n_iov;
    int skip_in_flight_update;
    ex_tbx_iovec_t *iov;
    tbx_tbuf_t *buffer;
    lio_segment_rw_hints_t *rw_hints;
    ex_tbx_iovec_t iov_dummy;
    tbx_tbuf_t buffer_dummy;
    ex_off_t boff;
} lio_rw_op_t;

typedef gop_op_generic_t *(lio_rw_ex_gop_t)(lio_rw_op_t *op);

struct lio_fd_t {  //** Individual file descriptor
    lio_config_t *lc;
    lio_file_handle_t *fh;  //** Shared handle
    tbx_stack_t *wq;
    lio_creds_t *creds;
    char *path;
    lio_rw_ex_gop_t *read_gop;
    lio_rw_ex_gop_t *write_gop;
    os_fd_t *ofd;     //** Only used for locking
    int mode;         //** Full LIO mode
    int rw_mode;      //** This just let's me know if it's a special file(0) or it's for R/W
    int sfd;          //** Special FD for FIFO or SOCKET objects
    int ftype;        //** File type
    ex_id_t  id;
    ex_off_t curr_offset;
    ex_off_t tally_ops[2];
    ex_off_t tally_bytes[2];
    ex_off_t tally_error_ops[2];
    ex_off_t tally_error_bytes[2];
};

wq_context_t *wq_context_create(lio_fd_t *fd, int max_tasks);
void wq_context_destroy(wq_context_t *ctx);
void wq_backend_shutdown(wq_context_t *ctx);
gop_op_generic_t *wq_op_new(wq_context_t *ctx, lio_rw_op_t *rw_op, int rw_mode);

extern tbx_sl_compare_t ex_id_compare;

mode_t ftype_lio2posix(int ftype);
void lio_parse_stat_vals(char *fname, struct stat *stat, char **val, int *v_size, char **flink, int *lio_ftype);
int lio_get_symlink_inode(lio_config_t *lc, lio_creds_t *creds, const char *fname, char *rpath, int fetch_realpath, ex_id_t *ino);

int lio_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode);
int lio_update_exnode_attrs(lio_fd_t *fd, lio_segment_errors_t *serr);

ex_off_t lio_size_fh(lio_file_handle_t *fh);

void lio_set_timestamp(char *id, char **val, int *v_size);
void lio_get_timestamp(char *val, int *timestamp, char **id);
gop_op_status_t cp_lio2lio(lio_cp_file_t *cp);
gop_op_status_t cp_local2lio(lio_cp_file_t *cp);
gop_op_status_t cp_lio2local(lio_cp_file_t *cp);
int lio_cp_create_dir(tbx_list_t *table, lio_path_tuple_t tuple);

void lc_object_remove_unused(int remove_all_unused);
lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *ltuple);
LIO_API int lio_parse_path_test();
lio_fn_t *lio_core_create();
void lio_core_destroy(lio_config_t *lio);
const char *lio_client_version();

void lio_wq_shutdown();
void lio_wq_startup(int n_parallel);

#ifdef __cplusplus
}
#endif

#endif
