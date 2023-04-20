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

// *** XSTDIO **

// *****************************************************************
// Contains some of the core LIO abstract types
// *****************************************************************

#ifndef ACCRE_LIO_FS_H_INCLUDED
#define ACCRE_LIO_FS_H_INCLUDED

#include <sys/statvfs.h>
#include <tbx/iniparse.h>
#include <lio/core.h>
#include <lio/ex3_fwd.h>
#include <lio/visibility.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lio_fs_t lio_fs_t;
typedef struct lio_fs_dir_iter_t lio_fs_dir_iter_t;

LIO_API void lio_fs_hint_release(lio_fs_t *fs, lio_os_authz_local_t *ug);
LIO_API lio_os_authz_local_t *lio_fs_new_os_authz_local(lio_fs_t *fs, uid_t uid, gid_t gid);
LIO_API void lio_fs_destroy_os_authz_local(lio_fs_t *fs, lio_os_authz_local_t *ug);
LIO_API void lio_fs_fill_os_authz_local(lio_fs_t *fs, lio_os_authz_local_t *ug, uid_t uid, gid_t gid);
LIO_API int lio_fs_same_namespace(lio_fs_t *fs1, lio_fs_t *fs2);
LIO_API int lio_fs_stat(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct stat *stat, char **symlink, int stat_symlink, int no_cache_stat_if_file);
LIO_API int lio_fs_fstat(lio_fs_t *fs, lio_fd_t *fd, struct stat *sbuf);
LIO_API lio_fs_dir_iter_t *lio_fs_opendir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname);
LIO_API int lio_fs_realpath(lio_fs_t *fs, const char *path, char *realpath);
LIO_API int lio_fs_exists(lio_fs_t *fs, const char *path);
LIO_API int lio_fs_access(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int mode);
LIO_API int lio_fs_fadvise(lio_fs_t *fs, lio_fd_t *fd, off_t offset, off_t len, int advice);
LIO_API int lio_fs_readdir(lio_fs_dir_iter_t *dit, char **dentry, struct stat *stat, char **symlink, int stat_symlink);
LIO_API int lio_fs_closedir(lio_fs_dir_iter_t *dit);
LIO_API int lio_fs_dir_is_empty(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path);
LIO_API int lio_fs_object_create(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode, int mkpath);
LIO_API int lio_fs_mknod(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode, dev_t rdev);
LIO_API int lio_fs_chmod(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode);
LIO_API int lio_fs_mkdir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode);
LIO_API int lio_fs_mkpath(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode, int mkpath);
LIO_API int lio_fs_object_remove(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int ftype);
LIO_API int lio_fs_unlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname);
LIO_API int lio_fs_rmdir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname);
LIO_API int lio_fs_flock(lio_fs_t *fs, lio_fd_t *fd, int lock_type);
LIO_API lio_fd_t *lio_fs_open(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int lflags);
LIO_API int lio_fs_close(lio_fs_t *fs, lio_fd_t *fd);
LIO_API off_t lio_fs_seek(lio_fs_t *fs, lio_fd_t *fd, off_t offset, int whence);
LIO_API off_t lio_fs_tell(lio_fs_t *fs, lio_fd_t *fd);
LIO_API off_t lio_fs_size(lio_fs_t *fs, lio_fd_t *fd);
LIO_API ssize_t lio_fs_pread(lio_fs_t *fs, lio_fd_t *fd, char *buf, size_t size, off_t off);
LIO_API ssize_t lio_fs_read(lio_fs_t *fs, lio_fd_t *fd, char *buf, size_t size);
LIO_API ssize_t lio_fs_readv(lio_fs_t *fs, lio_fd_t *fd, const struct iovec *iov, int iovcnt, off_t offset);
LIO_API int lio_fs_read_ex(lio_fs_t *fs, lio_fd_t *fd, int n_ex_iov, ex_tbx_iovec_t *ex_iov, const struct iovec *iov, int iovcnt, size_t iov_nbytes, off_t iov_off);
LIO_API ssize_t lio_fs_pwrite(lio_fs_t *fs, lio_fd_t *fd, const char *buf, size_t size, off_t off);
LIO_API ssize_t lio_fs_write(lio_fs_t *fs, lio_fd_t *fd, const char *buf, size_t size);
LIO_API ssize_t lio_fs_writev(lio_fs_t *fs, lio_fd_t *fd, const struct iovec *iov, int iovcnt, off_t offset);
LIO_API int lio_fs_write_ex(lio_fs_t *fs, lio_fd_t *fd, int n_ex_iov, ex_tbx_iovec_t *ex_iov, const struct iovec *iov, int iovcnt, size_t iov_nbytes, off_t iov_off);
LIO_API int lio_fs_flush(lio_fs_t *fs, lio_fd_t *fd);
LIO_API ex_off_t lio_fs_copy_file_range(lio_fs_t *fs, lio_fd_t *fd_in, off_t offset_in, lio_fd_t *fd_out, off_t offset_out, size_t size);
LIO_API int lio_fs_rename(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *oldname, const char *newname);
LIO_API int lio_fs_copy_lio2local(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *src_lio_fname, const char *dest_local_fname, int bufsize, char *buffer, ex_off_t offset, ex_off_t len, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_fs_copy_local2lio(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *src_local_fname, const char *dest_lio_fname, int bufsize, char *buffer, ex_off_t offset, ex_off_t len, int do_truncate, lio_segment_rw_hints_t *rw_hints);
LIO_API gop_op_generic_t *lio_fs_copy_gop(lio_fs_t *fs, lio_os_authz_local_t *ug, int src_is_lio, const char *src_fname, int dest_is_lio, const char *dest_fname, ex_off_t bufsize, int do_slow_copy, int allow_local2local, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_fs_copy(lio_fs_t *fs, lio_os_authz_local_t *ug, int src_is_lio, const char *src_fname, int dest_is_lio, const char *dest_fname, ex_off_t bufsize, int do_slow_copy, int allow_local2local, lio_segment_rw_hints_t *rw_hints);
LIO_API int lio_fs_truncate(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, off_t new_size);
LIO_API int lio_fs_ftruncate(lio_fs_t *fs, lio_fd_t *fd, off_t new_size);
LIO_API int lio_fs_utimens(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const struct timespec tv[2]);
LIO_API int lio_fs_listxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char *list, size_t size);
LIO_API void lio_fs_set_tape_attr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *mytape_val, int tape_size);
LIO_API void lio_fs_get_tape_attr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char **tape_val, int *tape_size);
LIO_API int lio_fs_getxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name, char *buf, size_t size);
LIO_API int lio_fs_setxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name, const char *fval, size_t size, int flags);
LIO_API int lio_fs_removexattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name);
LIO_API int lio_fs_hardlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *oldname, const char *newname);
LIO_API int lio_fs_readlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char *buf, size_t bsize);
LIO_API int lio_fs_symlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *link, const char *newname);
LIO_API int lio_fs_statvfs(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct statvfs *sfs);
LIO_API int lio_fs_statfs(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct statfs *sfs);

LIO_API lio_fs_t *lio_fs_create(tbx_inip_file_t *fd, const char *fs_section, lio_config_t *lc, uid_t uid, gid_t gid);
LIO_API void lio_fs_destroy(lio_fs_t *fs);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_FS_H_INCLUDED ^ */