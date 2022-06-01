/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License") TBX_IO_DECLARE_END
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

//**********************************************************************************************
//  This file just contains the fn definitions for declarations and needs TBX_IO_DECLARE_BEGIN
//  and TBX_IO_DECLARE_END defined for this to make sense
//**********************************************************************************************

//**These are actually 64-bit interfaces
TBX_IO_DECLARE_BEGIN FILE *(*TBX_IO_WRAP_NAME(fopen64))(const char *pathname, const char *mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(open64))(const char *pathname, int flags, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(openat64))(int dirfd, const char *pathname, int flags, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fadvise64))(int fd, off_t offset, off_t len, int advice) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fcntl64))(int fd, int cmd, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(getrlimit64))(__rlimit_resource_t resource, struct rlimit64 *rlim) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(setrlimit64))(__rlimit_resource_t resource, const struct rlimit64 *rlim) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pread64))(int fd, void *buf, size_t count, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pwrite64))(int fd, const void *buf, size_t count, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__xstat64))(int __ver, const char *pathname, struct stat64 *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN struct dirent64 *(*TBX_IO_WRAP_NAME(readdir64))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__fxstat64))(int __ver, int fd, struct stat64 *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__fxstatat64))(int __ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__lxstat64))(int __ver, const char *pathname, struct stat64 *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(statvfs64))(const char *path, struct statvfs64 *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fstatvfs64))(int fd, struct statvfs64 *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(statfs64))(const char *path, struct statfs64 *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fstatfs64))(int fd, struct statfs64 *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(truncate64))(const char *path, off_t length) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(ftruncate64))(int fd, off_t length) TBX_IO_DECLARE_END

//These are normal version of those above
TBX_IO_DECLARE_BEGIN FILE *(*TBX_IO_WRAP_NAME(fopen))(const char *pathname, const char *mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(open))(const char *pathname, int flags, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(openat))(int dirfd, const char *pathname, int flags, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fcntl))(int fd, int cmd, ...) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pread))(int fd, void *buf, size_t count, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pwrite))(int fd, const void *buf, size_t count, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__xstat))(int __ver, const char *pathname, struct stat *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN struct dirent *(*TBX_IO_WRAP_NAME(readdir))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__fxstatat))(int __ver, int dirfd, const char *pathname, struct stat *statbuf, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(statvfs))(const char *path, struct statvfs *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fstatvfs))(int fd, struct statvfs *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(statfs))(const char *path, struct statfs *buf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fstatfs))(int fd, struct statfs *buf) TBX_IO_DECLARE_END

//** These only have a single variant
TBX_API char *(*TBX_IO_WRAP_NAME(get_current_dir_name))(void) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(chdir))(const char *path) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fchdir))(int fd) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(chmod))(const char *pathname, mode_t mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fchmod))(int fd,  mode_t mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fchmodat))(int fd,  const char *pathname, mode_t mode, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN char *(*TBX_IO_WRAP_NAME(getcwd))(char *buf, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(symlinkat))(const char *target, int newdirfd, const char *linkpath) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(symlink))(const char *target, const char *linkpath) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(linkat))(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(link))(const char *oldpath, const char *newpath) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(creat))(const char *pathname, mode_t mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(posix_fadvise))(int fd, off_t offset, off_t len, int advice) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(rename))(const char *oldpath, const char *newpath) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(renameat))(int olddirfd, const char *oldpath, int newdirfd, const char *newpath) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(renameat2))(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags) TBX_IO_DECLARE_END

TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fclose))(FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(close))(int fd) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(select))(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(pselect))(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timespec *timeout, const sigset_t *sigmask) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(poll))(struct pollfd *fds, nfds_t nfds, int timeout) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(ppoll))(struct pollfd *fds, nfds_t nfds, const struct timespec *tmo_p, const sigset_t *sigmask) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN size_t (*TBX_IO_WRAP_NAME(fread))(void *ptr, size_t size, size_t nmemb, FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(read))(int fd, void *buf, size_t count) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(readv))(int fd, const struct iovec *iov, int iovcnt) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(preadv))(int fd, const struct iovec *iov, int iovcnt, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(preadv2))(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN size_t (*TBX_IO_WRAP_NAME(fwrite))(const void *ptr, size_t size, size_t nmemb, FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(write))(int fd, const void *buf, size_t count) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(writev))(int fd, const struct iovec *iov, int iovcnt) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pwritev))(int fd, const struct iovec *iov, int iovcnt, off_t offset) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(pwritev2))(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN off_t (*TBX_IO_WRAP_NAME(lseek))(int fd, off_t offset, int whence) TBX_IO_DECLARE_END

TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fseek))(FILE *stream, long offset, int whence) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN long (*TBX_IO_WRAP_NAME(ftell))(FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN void (*TBX_IO_WRAP_NAME(rewind))(FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fseeko))(FILE *stream, off_t offset, int whence) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN off_t (*TBX_IO_WRAP_NAME(ftello))(FILE *stream) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fgetpos))(FILE *stream, fpos_t *pos);
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fsetpos))(FILE *stream, const fpos_t *pos);
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(truncate))(const char *path, off_t length) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(ftruncate))(int fd, off_t length) TBX_IO_DECLARE_END

TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(dup))(int oldfd) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(dup2))(int oldfd, int newfd) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(readlinkat))(int dirfd, const char *pathname, char *buf, size_t bufsiz) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(readlink))(const char *pathname, char *buf, size_t bufsiz) TBX_IO_DECLARE_END
#ifdef HAS_STATX
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(statx))(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *statxbuf) TBX_IO_DECLARE_END
#endif
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__fxstat))(int __ver, int fd, struct stat *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(__lxstat))(int __ver, const char *pathname, struct stat *statbuf) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(access))(const char *pathname, int mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(faccessat))(int dirfd, const char *pathname, int mode, int flags) TBX_IO_DECLARE_END

TBX_IO_DECLARE_BEGIN DIR *(*TBX_IO_WRAP_NAME(opendir))(const char *name) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(readdir_r))(DIR *dirp, struct dirent *dentry, struct dirent **result) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(closedir))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(dirfd))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN void (*TBX_IO_WRAP_NAME(rewinddir))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN void (*TBX_IO_WRAP_NAME(seekdir))(DIR *dirp, long loc) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN long (*TBX_IO_WRAP_NAME(telldir))(DIR *dirp) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN DIR *(*TBX_IO_WRAP_NAME(fdopendir))(int fd) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(getxattr))(const char *path, const char *name, void *value, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(lgetxattr))(const char *path, const char *name, void *value, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(fgetxattr))(int fd, const char *name, void *value, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(setxattr))(const char *path, const char *name, const void *value, size_t size, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(lsetxattr))(const char *path, const char *name, const void *value, size_t size, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fsetxattr))(int fd, const char *name, const void *value, size_t size, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(removexattr))(const char *path, const char *name) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(lremovexattr))(const char *path, const char *name) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(fremovexattr))(int fd, const char *name) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(listxattr))(const char *path, char *list, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(llistxattr))(const char *path, char *list, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN ssize_t (*TBX_IO_WRAP_NAME(flistxattr))(int fd, char *list, size_t size) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(futimens))(int fd, const struct timespec times[2]) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(unlink))(const char *pathname) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(unlinkat))(int dirfd, const char *pathname, int flags) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(remove))(const char *pathname) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(rmdir))(const char *pathname) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(mkdirat))(int dirfd, const char *pathname, mode_t mode) TBX_IO_DECLARE_END
TBX_IO_DECLARE_BEGIN int (*TBX_IO_WRAP_NAME(mkdir))(const char *pathname, mode_t mode) TBX_IO_DECLARE_END
