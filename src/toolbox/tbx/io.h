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

#ifndef __IO_H__
#define __IO_H__

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <poll.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/xattr.h>
#include <tbx/visibility.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef TBX_IO_WRAP_NAME
#define TBX_IO_WRAP_NAME(a) tbx_io_ ## a
#endif

//** This is used to declare the fn variables as externs
#pragma push_macro("TBX_IO_DECLARE_BEGIN")
#pragma push_macro("TBX_IO_DECLARE_END")
#define TBX_IO_DECLARE_BEGIN TBX_API extern
#define TBX_IO_DECLARE_END ;
#include <tbx/io_declare.h>
#pragma pop_macro("TBX_IO_DECLARE_END")
#pragma pop_macro("TBX_IO_DECLARE_BEGIN")

//** And now the functions
TBX_API void tbx_io_init(void *handle, int do_overwrite_fn);  //** This sets up all the fn pointers

#define tbx_io_stat(path, sbuf) TBX_IO_WRAP_NAME(__xstat)(_STAT_VER, path, sbuf)
#define tbx_io_fstat(fd, sbuf) TBX_IO_WRAP_NAME(__fxstat)(_STAT_VER, fd, sbuf)
#define tbx_io_fstat64(fd, sbuf) TBX_IO_WRAP_NAME(__fxstat64)(_STAT_VER, fd, sbuf)
#define tbx_io_fstatat(dirfd, pathname, sbuf, flags) TBX_IO_WRAP_NAME(__fxstatat)(_STAT_VER, dirfd, pathname, sbuf, flags)
#define tbx_io_lstat(path, sbuf) TBX_IO_WRAP_NAME(__lxstat)(_STAT_VER, path, sbuf)
#define tbx_posix_fadvise(fd, offset, len, advice) TBX_IO_WRAP_NAME(fadvise64)(fd, offset, len, advice)
#define tbx_io_mknod(fname, mode, dev) TBX_IO_WRAP_NAME(__xmknod)(_MKNOD_VER, fname, mode, dev)
#define tbx_io_mknodat(dirfd, fname, mode, dev) TBX_IO_WRAP_NAME(__xmknodat)(_MKNOD_VER, dirfd, fname, mode, dev)

#ifdef __cplusplus
}
#endif

#endif