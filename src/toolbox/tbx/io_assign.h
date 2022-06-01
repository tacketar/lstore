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
// This is just used as a template for assigning the functions and is intended to be included
// as code in an existing routine.
//**********************************************************************************************


    //** These are actually 64-bit routines
    ASSERT_EXISTS(fopen64);
    ASSERT_EXISTS(open64);
    ASSERT_EXISTS(openat64);
    OK_IF_MISSING(fcntl64);
    ASSERT_EXISTS(getrlimit64);
    ASSERT_EXISTS(setrlimit64);
    ASSERT_EXISTS(pread64);
    ASSERT_EXISTS(pwrite64);
    ASSERT_EXISTS(__xstat64);
    ASSERT_EXISTS(__lxstat64);
    ASSERT_EXISTS(__fxstat64);
    ASSERT_EXISTS(__fxstatat64);
    ASSERT_EXISTS(readdir64);
    ASSERT_EXISTS(statvfs64);
    ASSERT_EXISTS(fstatvfs64);
    ASSERT_EXISTS(statfs64);
    ASSERT_EXISTS(fstatfs64);
    ASSERT_EXISTS(truncate);
    ASSERT_EXISTS(ftruncate);

    //** These are the normal interfaces
    ASSERT_EXISTS(fopen);
    ASSERT_EXISTS(open);
    ASSERT_EXISTS(openat);
    ASSERT_EXISTS(fcntl);
    ASSERT_EXISTS(pread);
    ASSERT_EXISTS(pwrite);
    ASSERT_EXISTS(__xstat);
    ASSERT_EXISTS(readdir);

    //** Everything else has a single interface
    ASSERT_EXISTS(chdir);
    ASSERT_EXISTS(fchdir);
    ASSERT_EXISTS(chmod);
    ASSERT_EXISTS(fchmod);
    ASSERT_EXISTS(fchmodat);
    ASSERT_EXISTS(getcwd);
    ASSERT_EXISTS(get_current_dir_name);

    ASSERT_EXISTS(symlinkat);
    ASSERT_EXISTS(symlink);
    ASSERT_EXISTS(linkat);
    ASSERT_EXISTS(link);
    ASSERT_EXISTS(creat);
    ASSERT_EXISTS(posix_fadvise);
    ASSERT_EXISTS(rename);
    ASSERT_EXISTS(renameat);
    OK_IF_MISSING(renameat2);

    ASSERT_EXISTS(fclose);
    ASSERT_EXISTS(close);
    ASSERT_EXISTS(select);
    ASSERT_EXISTS(pselect);
    ASSERT_EXISTS(poll);
    ASSERT_EXISTS(ppoll);
    ASSERT_EXISTS(fread);
    ASSERT_EXISTS(preadv);
    ASSERT_EXISTS(preadv2);
    ASSERT_EXISTS(read);
    ASSERT_EXISTS(readv);
    ASSERT_EXISTS(fwrite);
    ASSERT_EXISTS(pwritev);
    ASSERT_EXISTS(pwritev2);
    ASSERT_EXISTS(write);
    ASSERT_EXISTS(writev);
    ASSERT_EXISTS(lseek);

    ASSERT_EXISTS(fseek);
    ASSERT_EXISTS(ftell);
    ASSERT_EXISTS(rewind);
    ASSERT_EXISTS(fseeko);
    ASSERT_EXISTS(ftello);
    ASSERT_EXISTS(fgetpos);
    ASSERT_EXISTS(fsetpos);
    ASSERT_EXISTS(truncate);
    ASSERT_EXISTS(ftruncate);

    ASSERT_EXISTS(dup);
    ASSERT_EXISTS(dup2);
    ASSERT_EXISTS(readlinkat);
    ASSERT_EXISTS(readlink);

    //**stat's go here
    ASSERT_EXISTS(__xstat);
#ifdef HAS_STATX
    ASSERT_EXISTS(statx);
#endif
    ASSERT_EXISTS(__fxstat);
    ASSERT_EXISTS(__fxstatat);
    ASSERT_EXISTS(__lxstat);
    ASSERT_EXISTS(statvfs);
    ASSERT_EXISTS(fstatvfs);
    ASSERT_EXISTS(statfs);
    ASSERT_EXISTS(fstatfs);

    //** And everything else
    ASSERT_EXISTS(access);
    ASSERT_EXISTS(faccessat);

    ASSERT_EXISTS(opendir);
    ASSERT_EXISTS(readdir_r);
    ASSERT_EXISTS(closedir);
    ASSERT_EXISTS(dirfd);
    ASSERT_EXISTS(rewinddir);
    ASSERT_EXISTS(seekdir);
    ASSERT_EXISTS(telldir);
    ASSERT_EXISTS(fdopendir);

    ASSERT_EXISTS(getxattr);
    ASSERT_EXISTS(lgetxattr);
    ASSERT_EXISTS(fgetxattr);
    ASSERT_EXISTS(setxattr);
    ASSERT_EXISTS(lsetxattr);
    ASSERT_EXISTS(fsetxattr);
    ASSERT_EXISTS(removexattr);
    ASSERT_EXISTS(lremovexattr);
    ASSERT_EXISTS(fremovexattr);
    ASSERT_EXISTS(listxattr);
    ASSERT_EXISTS(llistxattr);
    ASSERT_EXISTS(flistxattr);

    ASSERT_EXISTS(futimens);
    ASSERT_EXISTS(unlink);
    ASSERT_EXISTS(unlinkat);
    ASSERT_EXISTS(remove);
    ASSERT_EXISTS(rmdir);
    ASSERT_EXISTS(mkdir);
    ASSERT_EXISTS(mkdirat);
