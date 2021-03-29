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

#ifndef ACCRE_LIO_LIO_ABSTRACT_H_INCLUDED
#define ACCRE_LIO_LIO_ABSTRACT_H_INCLUDED

#include <gop/mq.h>
#include <lio/visibility.h>
#include <lio/authn.h>
#include <lio/blacklist.h>
#include <lio/cache.h>
#include <lio/ex3.h>
#include <lio/os.h>
#include <lio/rs.h>
#include <tbx/list.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct lio_config_t lio_config_t;
typedef struct lio_cp_file_t lio_cp_file_t;
typedef struct lio_cp_path_t lio_cp_path_t;
typedef struct lio_fd_t lio_fd_t;
typedef struct lio_file_handle_t lio_file_handle_t;
typedef struct lio_fn_t lio_fn_t;
typedef struct lio_fsck_iter_t lio_fsck_iter_t;
typedef struct lio_path_tuple_t lio_path_tuple_t;
typedef struct lio_unified_object_iter_t lio_unified_object_iter_t;
struct stat;

// Functions
LIO_API gop_op_generic_t *lio_abort_regex_object_set_multiple_attrs_gop(lio_config_t *lc, gop_op_generic_t *gop);
LIO_API gop_op_generic_t *lio_symlink_object_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path, char *id);
LIO_API gop_op_generic_t *lio_read_gop(lio_fd_t *fd, char *buf, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_readv_gop(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_read_ex_gop(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_write_gop(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_writev_gop(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_write_ex_gop(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);

LIO_API int lio_read(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_readv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_read_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_writev(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_write_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_stat(lio_config_t *lc, lio_creds_t *creds, char *fname, struct stat *stat, char *mount_prefix, char **readlink);
LIO_API int lio_wq_enable(lio_fd_t *fd, int max_in_flight);

LIO_API void lio_get_timestamp(char *val, int *timestamp, char **id);
LIO_API ex_off_t lio_seek(lio_fd_t *fd, ex_off_t offset, int whence);
LIO_API ex_off_t lio_tell(lio_fd_t *fd);
LIO_API ex_off_t lio_size(lio_fd_t *fd);
LIO_API ex_off_t lio_block_size(lio_fd_t *fd, int block_type);
LIO_API int lio_cache_pages_drop(lio_fd_t *fd, ex_off_t lo, ex_off_t hi);
LIO_API gop_op_generic_t *lio_truncate_gop(lio_fd_t *fd, ex_off_t new_size);
LIO_API gop_op_generic_t *lio_flush_gop(lio_fd_t *fd, ex_off_t lo, ex_off_t hi);

LIO_API gop_op_generic_t *lio_get_multiple_attrs_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
LIO_API gop_op_generic_t *lio_multiple_setattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
LIO_API int lio_get_multiple_attrs(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
LIO_API gop_op_generic_t *lio_move_object_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path);

LIO_API os_attr_iter_t *lio_create_attr_iter(lio_config_t *lc, lio_creds_t *creds, const char *path, lio_os_regex_table_t *attr, int v_max);
LIO_API void lio_destroy_attr_iter(lio_config_t *lc, os_attr_iter_t *it);

LIO_API gop_op_generic_t *lio_close_gop(lio_fd_t *fd);
LIO_API gop_op_generic_t *lio_cp_lio2lio_gop(lio_fd_t *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, int hints, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_cp_lio2local_gop(lio_fd_t *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_cp_local2lio_gop(FILE *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len, int truncate, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_cp_local2local_gop(FILE *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, int truncate, lio_segment_rw_hints_t *rw_hints, int which_align);
LIO_API lio_fsck_iter_t *lio_create_fsck_iter(lio_config_t *lc, lio_creds_t *creds, char *path, int owner_mode, char *owner, int exnode_mode);
LIO_API os_object_iter_t *lio_create_object_iter(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *obj_regex, int object_types, lio_os_regex_table_t *attr, int recurse_dpeth, os_attr_iter_t **it, int v_max);
LIO_API os_object_iter_t *lio_create_object_iter_alist(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *obj_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_keys);
LIO_API gop_op_generic_t *lio_create_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int type, char *ex, char *id);
LIO_API void lio_destroy_fsck_iter(lio_config_t *lc, lio_fsck_iter_t *oit);
LIO_API void lio_destroy_object_iter(lio_config_t *lc, os_object_iter_t *it);
LIO_API int lio_encode_error_counts(lio_segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode);
LIO_API int lio_exists(lio_config_t *lc, lio_creds_t *creds, char *path);
LIO_API gop_op_generic_t *lio_exists_gop(lio_config_t *lc, lio_creds_t *creds, char *path);
LIO_API int lio_realpath(lio_config_t *lc, lio_creds_t *creds, const char *path, char *realpath);
LIO_API gop_op_generic_t *lio_realpath_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *realpath);
LIO_API gop_op_status_t lio_file_copy_op(void *arg, int id);
LIO_API int lio_fopen_flags(char *sflags);
LIO_API gop_op_generic_t *lio_fsck_gop(lio_config_t *lc, lio_creds_t *creds, char *fname, int ftype, int owner_mode, char *owner, int exnode_mode);
LIO_API ex_off_t lio_fsck_visited_count(lio_config_t *lc, lio_fsck_iter_t *oit);
LIO_API void lio_get_error_counts(lio_config_t *lc, lio_segment_t *seg, lio_segment_errors_t *serr);
LIO_API int lio_getattr(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size);
LIO_API gop_op_generic_t *lio_getattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void **val, int *v_size);
LIO_API gop_op_generic_t *lio_hardlink_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path, char *id);
LIO_API int lio_init(int *argc, char ***argvp);
LIO_API gop_op_generic_t *lio_link_gop(lio_config_t *lc, lio_creds_t *creds, int symlink, char *src_path, char *dest_path, char *id);
LIO_API gop_op_generic_t *lio_move_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path);
LIO_API int lio_multiple_setattr_op(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char **key, void **val, int *v_size, int n);
LIO_API int lio_next_attr(lio_config_t *lc, os_attr_iter_t *it, char **key, void **val, int *v_size);
LIO_API int lio_next_fsck(lio_config_t *lc, lio_fsck_iter_t *oit, char **bad_fname, int *bad_atype);
LIO_API int lio_next_object(lio_config_t *lc, os_object_iter_t *it, char **fname, int *prefix_len);
LIO_API gop_op_generic_t *lio_open_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int mode, char *id, lio_fd_t **fd, int max_wait);
LIO_API int lio_parse_path_options(int *argc, char **argv, int auto_mode, lio_path_tuple_t *tuple, lio_os_regex_table_t **rp, lio_os_regex_table_t **ro);
LIO_API gop_op_status_t lio_path_copy_op(void *arg, int id);
LIO_API void lio_path_local_make_absolute(lio_path_tuple_t *tuple);
LIO_API void lio_path_release(lio_path_tuple_t *tuple);
LIO_API lio_path_tuple_t lio_path_tuple_copy(lio_path_tuple_t *curr, char *fname);
LIO_API lio_path_tuple_t lio_path_resolve(int auto_fuse_convert, char *lpath);
LIO_API lio_path_tuple_t lio_path_resolve_base(char *lpath);
LIO_API int lio_path_wildcard_auto_append(lio_path_tuple_t *tuple);
LIO_API void lio_print_options(FILE *fd);
LIO_API void lio_print_path_options(FILE *fd);
LIO_API void lio_print_object_type_options(FILE *fd, int obj_type_default);
LIO_API gop_op_generic_t *lio_regex_object_set_multiple_attrs_gop(lio_config_t *lc, lio_creds_t *creds, char *id, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n);
LIO_API gop_op_generic_t *lio_remove_gop(lio_config_t *lc, lio_creds_t *creds, char *path, char *ex_optional, int ftype_optional);
LIO_API gop_op_generic_t *lio_remove_regex_gop(lio_config_t *lc, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int obj_types, int recurse_depth, int np);
LIO_API int lio_setattr(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void *val, int v_size);
LIO_API gop_op_generic_t *lio_setattr_gop(lio_config_t *lc, lio_creds_t *creds, const char *path, char *id, char *key, void *val, int v_size);
LIO_API gop_op_generic_t *lio_move_object_gop(lio_config_t *lc, lio_creds_t *creds, char *src_path, char *dest_path);
LIO_API int lio_shutdown();
LIO_API int lio_stat(lio_config_t *lc, lio_creds_t *creds, char *fname, struct stat *stat, char *mount_prefix, char **readlink);
LIO_API gop_op_generic_t *lio_truncate_op(lio_fd_t *fd, ex_off_t new_size);
LIO_API int lio_unified_next_object(lio_unified_object_iter_t *it, char **fname, int *prefix_len);
LIO_API lio_unified_object_iter_t *lio_unified_object_iter_create(lio_path_tuple_t tuple, lio_os_regex_table_t *path_regex, lio_os_regex_table_t *obj_regex, int obj_types, int rd);
LIO_API void lio_unified_object_iter_destroy(lio_unified_object_iter_t *it);
LIO_API void *lio_stdinlist_iter_create(int argc, const char **argv);
LIO_API void lio_stdinlist_iter_destroy(void *ptr);
LIO_API char *lio_stdinlist_iter_next(void *ptr);

LIO_API int lio_rw_test_exec(int rw_mode, char *section);

// Preprocessor constants
typedef enum lio_fsck_repair_t lio_fsck_repair_t;
enum lio_fsck_repair_t {
    LIO_FSCK_MANUAL =    0,
    LIO_FSCK_PARENT =    (1 << 0),
    LIO_FSCK_DELETE =    (1 << 1),
    LIO_FSCK_USER   =    (1 << 2),
    LIO_FSCK_SIZE_REPAIR  = (1 << 3),
};

typedef enum lio_fsck_error_flags_t lio_fsck_error_flags_t;
enum lio_fsck_error_flags_t {
    LIO_FSCK_FINISHED          =  (-1),
    LIO_FSCK_GOOD              =  (0),
    LIO_FSCK_MISSING_OWNER     =  (1 << 0),
    LIO_FSCK_MISSING_EXNODE    =  (1 << 1),
    LIO_FSCK_MISSING_EXNODE_SIZE = (1 << 2),
    LIO_FSCK_MISSING_INODE     =  (1 << 3),
    LIO_FSCK_MISSING           =  (1 << 4),
};

#define LIO_READ_MODE      1
#define LIO_WRITE_MODE     2
#define LIO_TRUNCATE_MODE  4
#define LIO_CREATE_MODE    8
#define LIO_APPEND_MODE   16
#define LIO_EXCL_MODE     32
#define LIO_RW_MODE       (LIO_READ_MODE|LIO_WRITE_MODE)

typedef enum lio_copy_hint_t lio_copy_hint_t;
enum lio_copy_hint_t {
    LIO_COPY_DIRECT   = 0,
    LIO_COPY_INDIRECT = 1,
};

// Global variables
LIO_API extern lio_config_t *lio_gc;
LIO_API extern tbx_log_fd_t *lio_ifd;
LIO_API extern int lio_parallel_task_count;
LIO_API extern FILE *_lio_ifd;  // ** Default information log device

// Exported structures. To be obscured
struct lio_config_t {
    lio_data_service_fn_t *ds;
    lio_object_service_fn_t *os;
    lio_resource_service_fn_t *rs;
    lio_authn_t *authn;
    gop_thread_pool_context_t *tpc_unlimited;
    gop_thread_pool_context_t *tpc_cache;
    gop_mq_context_t *mqc;
    lio_service_manager_t *ess;
    lio_service_manager_t *ess_nocache;  // ** Copy of ess but missing cache.  Kind of a kludge...
    tbx_stack_t *plugin_stack;
    lio_cache_t *cache;
    data_attr_t *da;
    tbx_inip_file_t *ifd;
    tbx_list_t *open_index;
    lio_creds_t *creds;
    apr_thread_mutex_t *lock;
    apr_pool_t *mpool;
    char *obj_name;
    char *server_address;
    char *section_name;
    char *ds_section;
    char *mq_section;
    char *authn_section;
    char *os_section;
    char *rs_section;
    char *tpc_unlimited_section;
    char *tpc_cache_section;
    char *cache_section;
    char *creds_name;
    char *creds_user;
    char *blacklist_section;
    char *exe_name;
    char *rc_section;
    lio_blacklist_t *blacklist;
    ex_off_t readahead;
    ex_off_t readahead_trigger;
    ex_off_t jerase_max_parity_on_stack;
    int calc_adler32;
    int timeout;
    int max_attr;
    int anonymous_creation;
    int auto_translate;
    int jerase_paranoid;
    int tpc_unlimited_count;
    int tpc_cache_count;
    int tpc_max_recursion;
    int ref_cnt;
};

struct lio_path_tuple_t {
    lio_creds_t *creds;
    lio_config_t *lc;
    char *path;
    int is_lio;
};

struct lio_cp_file_t {
    lio_segment_rw_hints_t *rw_hints;
    lio_path_tuple_t src_tuple;
    lio_path_tuple_t dest_tuple;
    ex_off_t bufsize;
    int slow;
    int enable_local;
};

struct lio_cp_path_t {
    lio_path_tuple_t src_tuple;
    lio_path_tuple_t dest_tuple;
    lio_os_regex_table_t *path_regex;
    lio_os_regex_table_t *obj_regex;
    int recurse_depth;
    int dest_type;
    int obj_types;
    int max_spawn;
    int slow;
    int enable_local;
    int force_dest_create;
    ex_off_t bufsize;
};
#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_LIO_ABSTRACT_H_INCLUDED ^ */ 
