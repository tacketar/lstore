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

#ifndef ACCRE_LIO_STDIO_WRAPPER_ABSTRACT_H_INCLUDED
#define ACCRE_LIO_STDIO_WRAPPER_ABSTRACT_H_INCLUDED

#include <lio_stdio_wrapper/visibility.h>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef WRAPPER_PREFIX
#define WRAPPER_PREFIX(a) a
//#define WRAPPER_PREFIX(a) xio_ ## a
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ** 64-bit renamed interfaces
LIO_STDIO_WRAPPER_API FILE *WRAPPER_PREFIX(fopen64)(const char *pathname, const char *mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(open64)(const char *pathname, int flags, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(openat64)(int dirfd, const char *pathname, int flags, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__xstat64)(int __ver, const char *pathname, struct stat64 *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__lxstat64)(int __ver, const char *pathname, struct stat64 *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__fxstat64)(int __ver, int fd, struct stat64 *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__fxstatat64)(int __ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pread64)(int fd, void *buf, size_t count, off_t offset);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pwrite64)(int fd, const void *buf, size_t count, off_t offset);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fadvise64)(int fd, off_t offset, off_t len, int advice);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(statvfs64)(const char *pathname, struct statvfs64 *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fstatvfs64)(int fd, struct statvfs64 *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(statfs64)(const char *pathname, struct statfs64 *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fstatfs64)(int fd, struct statfs64 *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(ftruncate64)(int fd, off_t length);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(truncate64)(const char *path, off_t length);

// ** Everything else
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(chdir)(const char *path);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fchdir)(int fd);
LIO_STDIO_WRAPPER_API char *WRAPPER_PREFIX(getcwd)(char *buf, size_t size);
LIO_STDIO_WRAPPER_API char *WRAPPER_PREFIX(get_current_dir_name)(void);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(chmod)(const char *pathname, mode_t mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fchmod)(int fd, mode_t mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fchmodat)(int dirfd, const char *pathname, mode_t mode, int flags);

LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(symlinkat)(const char *target, int newdirfd, const char *linkpath);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(symlink)(const char *target, const char *linkpath);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(linkat)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(link)(const char *oldpath, const char *newpath);
LIO_STDIO_WRAPPER_API FILE *WRAPPER_PREFIX(fopen)(const char *pathname, const char *mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(creat)(const char *pathname, mode_t mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(open)(const char *pathname, int flags, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(openat)(int dirfd, const char *pathname, int flags, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(posix_fadvise)(int fd, off_t offset, off_t len, int advice);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(rename)(const char *oldpath, const char *newpath);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(renameat)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(renameat2)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags);

LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fclose)(FILE *stream);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(close)(int fd);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(select)(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(pselect)(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timespec *timeout, const sigset_t *sigmask);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(poll)(struct pollfd *fds, nfds_t nfds, int timeout);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(ppoll)(struct pollfd *fds, nfds_t nfds, const struct timespec *tmo_p, const sigset_t *sigmask);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fcntl64)(int fd, int cmd, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fcntl)(int fd, int cmd, ...);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(getrlimit64)(__rlimit_resource_t  resource, struct rlimit64 *rlim);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(setrlimit64)(__rlimit_resource_t  resource, const struct rlimit64 *rlim);
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream);

//** Some OSes have this defined as a macro like: __extension__ fread_unlocked(...) which trips up the WRAPPER_PREFIX macro
#ifdef fread_unlocked
#pragma push_macro("fread_unlocked")
#undef fread_unlocked
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fread_unlocked)(void *ptr, size_t size, size_t nmemb, FILE *stream);
#pragma push_macro("fread_unlocked")
#else
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fread_unlocked)(void *ptr, size_t size, size_t nmemb, FILE *stream);
#endif

LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pread)(int fd, void *buf, size_t count, off_t offset);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(read)(int fd, void *buf, size_t count);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(readv)(int fd, const struct iovec *iov, int iovcnt);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(preadv)(int fd, const struct iovec *iov, int iovcnt, off_t offset);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(preadv2)(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags);
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream);

//** Some OSes have this defined as a macro like: __extension__ fwrite_unlocked(...) which trips up the WRAPPER_PREFIX macro
#ifdef fwrite_unlocked
#pragma push_macro("fwrite_unlocked")
#undef fwrite_unlocked
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fwrite_unlocked)(const void *ptr, size_t size, size_t nmemb, FILE *stream);
#pragma push_macro("fwrite_unlocked")
#else
LIO_STDIO_WRAPPER_API size_t WRAPPER_PREFIX(fwrite_unlocked)(const void *ptr, size_t size, size_t nmemb, FILE *stream);
#endif

LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(write)(int fd, const void *buf, size_t count);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(writev)(int fd, const struct iovec *iov, int iovcnt);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pwritev)(int fd, const struct iovec *iov, int iovcnt, off_t offset);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pwritev2)(int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(pwrite)(int fd, const void *buf, size_t count, off_t offset);
LIO_STDIO_WRAPPER_API off_t WRAPPER_PREFIX(lseek)(int fd, off_t offset, int whence);

LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(ftruncate)(int fd, off_t length);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(truncate)(const char *path, off_t length);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fseek)(FILE *stream, long offset, int whence);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fseeko)(FILE *stream, long offset, int whence);
LIO_STDIO_WRAPPER_API off_t  WRAPPER_PREFIX(ftello)(FILE *stream);
LIO_STDIO_WRAPPER_API long WRAPPER_PREFIX(ftell)(FILE *stream);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fgetpos)(FILE *stream, fpos_t *pos);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fsetpos)(FILE *stream, const fpos_t *pos);
LIO_STDIO_WRAPPER_API void WRAPPER_PREFIX(rewind)(FILE *stream);

LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(dup)(int oldfd);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(dup2)(int oldfd, int newfd);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(readlinkat)(int dirfd, const char *pathname, char *buf, size_t bufsiz);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(readlink)(const char *pathname, char *buf, size_t bufsiz);
#ifdef HAS_STATX
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(statx)(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *statxbuf);
#endif
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__fxstatat)(int __ver, int dirfd, const char *pathname, struct stat *statbuf, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__fxstat)(int __ver, int fd, struct stat *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__lxstat)(int __ver, const char *pathname, struct stat *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(__xstat)(int __ver, const char *pathname, struct stat *statbuf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(access)(const char *pathname, int mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(faccessat)(int dirfd, const char *pathname, int mode, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(statvfs)(const char *pathname, struct statvfs *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fstatvfs)(int fd, struct statvfs *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(statfs)(const char *pathname, struct statfs *buf);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fstatfs)(int fd, struct statfs *buf);

LIO_STDIO_WRAPPER_API DIR *WRAPPER_PREFIX(opendir)(const char *name);
LIO_STDIO_WRAPPER_API struct dirent *WRAPPER_PREFIX(readdir)(DIR *dirp);
LIO_STDIO_WRAPPER_API struct dirent64 *WRAPPER_PREFIX(readdir64)(DIR *dirp);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(readdir_r)(DIR *dirp, struct dirent *dentry, struct dirent **result);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(closedir)(DIR *dirp);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(dirfd)(DIR *dirp);
LIO_STDIO_WRAPPER_API void WRAPPER_PREFIX(rewinddir)(DIR *dirp);
LIO_STDIO_WRAPPER_API void WRAPPER_PREFIX(seekdir)(DIR *dirp, long loc);
LIO_STDIO_WRAPPER_API long WRAPPER_PREFIX(telldir)(DIR *dirp);
LIO_STDIO_WRAPPER_API DIR *WRAPPER_PREFIX(fdopendir)(int fd);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(getxattr)(const char *path, const char *name, void *value, size_t size);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(lgetxattr)(const char *path, const char *name, void *value, size_t size);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(fgetxattr)(int fd, const char *name, void *value, size_t size);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(setxattr)(const char *path, const char *name, const void *value, size_t size, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(lsetxattr)(const char *path, const char *name, const void *value, size_t size, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fsetxattr)(int fd, const char *name, const void *value, size_t size, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(removexattr)(const char *path, const char *name);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(fremovexattr)(int fd, const char *name);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(lremovexattr)(const char *path, const char *name);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(listxattr)(const char *path, char *list, size_t size);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(llistxattr)(const char *path, char *list, size_t size);
LIO_STDIO_WRAPPER_API ssize_t WRAPPER_PREFIX(flistxattr)(int fd, char *list, size_t size);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(futimens)(int fd, const struct timespec times[2]);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(unlink)(const char *pathname);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(unlinkat)(int dirfd, const char *pathname, int flags);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(remove)(const char *pathname);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(rmdir)(const char *pathname);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(mkdirat)(int dirfd, const char *pathname, mode_t mode);
LIO_STDIO_WRAPPER_API int WRAPPER_PREFIX(mkdir)(const char *pathname, mode_t mode);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_STDIO_WRAPPER__ABSTRACT_H_INCLUDED ^ */
