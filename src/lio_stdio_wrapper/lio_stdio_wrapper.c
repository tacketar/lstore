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
// These rotuines are designed to be used as a LD_PRELOAD to allow
// existing applications to natively use LIO files
//
//  In order to meet the POXIS assumptions about lowest fd numbers
//  we create a dummy fd and then dup it to (n_fd_max-1) and keep that as
//  a placeholder for LIO files.  Anytime a LIO file is created we then
//  use the FD returned from dup(n_fd_max-1) as the slot
//***********************************************************************

#define _log_module_index 185

#include <lio/lio.h>
#include <tbx/normalize_path.h>
#include <tbx/io.h>
#include <lio/fs.h>
#include <tbx/assert_result.h>
#include <tbx/constructor_wrapper.h>
#include <tbx/type_malloc.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <dlfcn.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <lio_stdio_wrapper/lio_stdio_wrapper.h>

#ifndef WRAPPER_PREFIX
#define WRAPPER_PREFIX(a) a
#endif

//#define FPRINTF(...) fprintf(stderr, "WRAP: " __VA_ARGS__); fflush(stderr)
#define FPRINTF(...)

//***********************************************************************
// Define the internal data types
//***********************************************************************

#define FD_MODE_NONE   0
#define FD_MODE_FILENO 1
#define FD_MODE_STD    2
#define FD_MODE_LIO    3

#define FDMAX_HARD_LIMIT (100*1024)       //** MAx number of open files allowed

#define _STDIO_PROTO(fn, ret, ...) ret fn (__VA_ARGS__)
#define _STDIO_VAR_DECLARE(fn, ret, ...) ret (*fn ## _stdio)(__VA_ARGS__)
#define STDIO_VAR_DLSYM(fn)  assert_result_not_null(fn ## _stdio = dlsym(RTLD_NEXT, #fn))
#define STDIO_VAR_DECLARE(...) _STDIO_VAR_DECLARE(__VA_ARGS__)

#define SANITY_CHECK_FD(fd, ret_val) if (((fd)<0) || ((fd)>=n_fd_max2)) return(ret_val)
#define CFD_IS_STD(fd, cfd) ((fd<n_fd_max) && (((cfd) == NULL) || ((cfd)->mode == FD_MODE_STD)))

typedef struct {  //** Base FD
    lio_fd_t *lfd;
    FILE *sfd;
    DIR *sdir;
    lio_fs_dir_iter_t *ldir;
    char *lname_dir;  //** LStore directory name
    char *sname_dir;  //** STDIO path
    struct dirent ldentry;
    struct dirent64 ldentry64;
    int fileno;
    int mode;
    int flags;
    int dup_count;
    int prefix_index;
} fd_core_t;

typedef struct {  //** FD used for the stdio routines
    fd_core_t *cfd;
    FILE dfd;
} fd_stdio_t;

typedef struct {
    char *prefix;
    char *link;
    int len;
    int link_len;
} prefix_t;

regex_t *np_regex = NULL;
int next_slot = 0;
int fd_dummy = -1;
int n_fd_max = 0;
int n_fd_max2 = -1;
fd_stdio_t *fd_table = NULL;
int n_prefix = 0;
prefix_t *prefix_table = NULL;
apr_thread_mutex_t *lock = NULL;
apr_pool_t *mpool = NULL;
lio_fs_t *fs = NULL;
char cwd[PATH_MAX-1];
int cwd_len = -1;

//** This is my name mangler
#define STDIO_WRAP_NAME(a) a ## _stdio

//** This is the template for all the fd based passthrough routines
#define FD_TEMPLATE(name, result, proto, stdio, lio) \
result WRAPPER_PREFIX(name)proto \
{ \
    fd_core_t *cfd; \
\
    FPRINTF(#name ": start fd=%d\n", fd); \
    SANITY_CHECK_FD(fd, -1); \
\
    cfd = fd_table[fd].cfd; \
    if (CFD_IS_STD(fd, cfd)) { \
        if (STDIO_WRAP_NAME(name) == NULL) {  STDIO_VAR_DLSYM(name); } \
        return(stdio); \
    } \
\
    return(lio); \
}
//------------------------------------------------


//** Pull in all the fn symbols and declare my version of them
#pragma push_macro("TBX_IO_DECLARE_BEGIN")
#pragma push_macro("TBX_IO_DECLARE_END")
#pragma push_macro("TBX_IO_WRAP_NAME")
#define TBX_IO_DECLARE_BEGIN
#define TBX_IO_DECLARE_END = NULL;
#ifdef TBX_IO_WRAP_NAME
#undef TBX_IO_WRAP_NAME
#endif
#define TBX_IO_WRAP_NAME(a) STDIO_WRAP_NAME(a)
#include <tbx/io_declare.h>
#pragma pop_macro("TBX_IO_WRAP_NAME")
#pragma pop_macro("TBX_IO_DECLARE_END")
#pragma pop_macro("TBX_IO_DECLARE_BEGIN")

//***********************************************************************
// Helper functions
//***********************************************************************

//***********************************************************************

void get_stdio_fn()
{
#define ASSERT_EXISTS(name) assert_result_not_null(STDIO_WRAP_NAME(name) = dlsym(RTLD_NEXT, #name)); TBX_IO_WRAP_NAME(name) = STDIO_WRAP_NAME(name);
#include <tbx/io_assign.h>
#undef ASSERT_EXISTS
}

//***********************************************************************

const char *path_map(int dirfd, const char *path, int *index, int *len, char *buffer, char *fbuffer, char **fullpath)
{
    fd_core_t *cfd;
    int i, n;
    char buf2[PATH_MAX];
    const char *mypath = path;
    char *npath = NULL;

    FPRINTF("path_map: dirfd=%d cwd=%s path=%s\n", dirfd, cwd, path);

    buffer[0] = '\0';
    if (path[0] != '/') { //**Relative path so convert it
        if (lock == NULL) return(NULL);  //** Still in startup mode
        apr_thread_mutex_lock(lock);
        if ((dirfd > 0) &&  (dirfd <= n_fd_max2)) { //** Got a directory prefix so use it
            cfd = fd_table[dirfd].cfd;
            if ((cfd) && (cfd->sname_dir)) {
                snprintf(buffer, PATH_MAX, "%s/%s", cfd->sname_dir, path);
                FPRINTF("path_map: dirfd=%d path=%s dir_path=%s fullpath=%s\n", dirfd, path, cfd->sname_dir, buffer);
            } else {
                FPRINTF("path_map: dirfd=%d path=%s MISSING directory!\n", dirfd, path);
                apr_thread_mutex_unlock(lock);
                if (fullpath != NULL)  *fullpath = (char *)path;  //** Don't have a dir fd so this is actually an error
                return(NULL);
            }
        } else {
            snprintf(buffer, PATH_MAX, "%s/%s", cwd, path);
            FPRINTF("path_map: dirfd=%d CWD adjustment cwd=%s path=%s\n", dirfd, cwd, buffer);
        }
        apr_thread_mutex_unlock(lock);
        mypath = buffer;
        FPRINTF("path_map: dirfd=%d pretest path=%s\n", dirfd, mypath);
        if (fullpath != NULL) {
            *fullpath = fbuffer;
            strncpy(fbuffer, mypath, PATH_MAX);
        }
    } else if (fullpath) {
        if (fullpath != NULL)  *fullpath = (char *)path;
    }

    //** Se if we need to normalize the path
    if ((np_regex) && (tbx_path_is_normalized(np_regex, mypath) == 0)) {
        npath = tbx_normalize_path(mypath, fbuffer);
        if (npath == NULL) return(NULL);
        strncpy(buffer, fbuffer, PATH_MAX);
        mypath = buffer;
        if (fullpath) *fullpath = fbuffer;
    }


    FPRINTF("path_map: path=%s n_prefix=%d\n", mypath, n_prefix);
    for (i=0; i<n_prefix; i++) {
        FPRINTF("path_map: path=%s prefix=%s i=%d\n", mypath, prefix_table[i].prefix, i);
        if (strncmp(prefix_table[i].prefix, mypath, prefix_table[i].len) == 0) {
            *index = i;
            if (prefix_table[i].link) { //** Got a link
                *len = 0;
                if (buffer == mypath) {
                    n = snprintf(buf2, PATH_MAX, "%s%s", prefix_table[i].link, mypath + prefix_table[i].len);
                    memcpy(buffer, buf2, n+1);
                } else {
                    snprintf(buffer, PATH_MAX, "%s%s", prefix_table[i].link, mypath + prefix_table[i].len);
                    mypath = buffer;
                }
                FPRINTF("path_map: MATCH-LINK path=%s prefix=%s i=%d\n", mypath, prefix_table[i].prefix, i);
            } else {
                *len = prefix_table[i].len;
                FPRINTF("path_map: MATCH path=%s prefix=%s i=%d\n", mypath, prefix_table[i].prefix, i);
            }
            return(mypath);
        }
    }

    return(NULL);
}

//***********************************************************************

fd_core_t *corefd_new(int mode, int flags, int fileno, FILE *sfd, lio_fd_t *lfd, DIR *sdir, lio_fs_dir_iter_t *ldir)
{
    fd_core_t *cfd;

    tbx_type_malloc_clear(cfd, fd_core_t, 1);
    cfd->mode = mode;
    cfd->flags = flags;
    cfd->fileno = fileno;
    cfd->sfd = sfd;
    cfd->lfd = lfd;
    cfd->sdir = sdir;
    cfd->ldir = ldir;
    cfd->dup_count = 1;

    return(cfd);
}

//***********************************************************************

int get_free_slot(int dup_fd, fd_core_t *cfd, int get_lowest)
{
    int i, slot, start;

    apr_thread_mutex_lock(lock);
    start = (get_lowest) ? 0 : next_slot;
    for (i=0; i<n_fd_max; i++) {
        slot = ((i+start) % n_fd_max) + n_fd_max;
        FPRINTF("GET_FREE_SLOT: returning %d\n", slot);
        if (fd_table[slot].cfd == NULL) {
            if (get_lowest == 0) next_slot = slot + 1;
            if (dup_fd >= 0) {
                fd_table[dup_fd].cfd->dup_count++;
                fd_table[slot].cfd = fd_table[dup_fd].cfd;
            } else {
                fd_table[slot].cfd = cfd;
            }
            apr_thread_mutex_unlock(lock);
            return(slot);
        }
    }
    apr_thread_mutex_unlock(lock);

    //** We should never make it here
    return(-1);
}

//***********************************************************************

int close_and_reserve(int fd, FILE *stream, fd_core_t *cfd_new, int delta)
{
    fd_stdio_t *jfd;
    fd_core_t *cfd;
    int close_parent, ret;

    FPRINTF("CAR: n_fd_max=%d fd=%d stream=%p cfd_new=%p delta=%d\n", n_fd_max, fd, stream, cfd_new, delta);

    ret = 0;

    close_parent = 0;
    if ((fd<0) || (fd>=n_fd_max2)) {
        return(-1);
    }

    jfd = &(fd_table[fd]);
    cfd = jfd->cfd;

    if (fd<n_fd_max) {
        if ((cfd) && (cfd->mode == FD_MODE_LIO)) {  //** This is actually an LStore file
            apr_thread_mutex_lock(lock);
            cfd->dup_count--;
            if (cfd->dup_count == 0) { close_parent = 1; }
            jfd->cfd = cfd_new;
            if (cfd_new) jfd->cfd->dup_count += delta;
            apr_thread_mutex_unlock(lock);
        } else {    //** Normal file
            if (stream) {
                ret = fclose_stdio(stream);
            } else {
                ret = close_stdio(fd);
            }
            if (cfd_new) {  //** See if we link it to an LStore file
                apr_thread_mutex_lock(lock);
                jfd->cfd = cfd_new;
                if (cfd_new) cfd_new->dup_count += delta;
                apr_thread_mutex_lock(lock);
            }
        }
    } else if (cfd) {   //** LStore file for sure
        apr_thread_mutex_lock(lock);
        cfd->dup_count--;
        if (cfd->dup_count == 0) { close_parent = 1; }
        jfd->cfd = cfd_new;
        if (cfd_new) jfd->cfd->dup_count += delta;
        apr_thread_mutex_unlock(lock);
    } else {   //** Invalid FD so throw an error
        return(-1);
    }

    //** See if we close and cleanup
    if (close_parent == 1) {
        if (cfd->lfd) {
            ret = lio_fs_close(fs, cfd->lfd);
        }
        if (cfd->lname_dir) free(cfd->lname_dir);
        if (cfd->sname_dir) free(cfd->sname_dir);
        free(cfd);
    }

    return(ret);
}

//***********************************************************************

int local_dir_is_empty(const char *path)
{
    DIR *dir;
    struct dirent *dentry;

    dir = opendir_stdio(path);
    if (!dir) return(-1);
    dentry = readdir_stdio(dir);  //** This should be "."
    if (dentry == NULL) { closedir_stdio(dir); return(1); }
    dentry = readdir_stdio(dir);  //** This should be ".."
    if (dentry == NULL) { closedir_stdio(dir); return(1); }

    dentry = readdir_stdio(dir);  //** This should be the 1st actual object
    if (dentry == NULL) { closedir_stdio(dir); return(1); }

    closedir_stdio(dir);
    return(0);
}


//***********************************************************************

int lio_dir_is_empty(const char *path)
{
    lio_fs_dir_iter_t *dir;
    char *dname;
    struct stat stat;

    //** Make sure the directory exists and we can open it
    dir = lio_fs_opendir(fs, NULL, path);
    if (!dir) return(-1);

    //** This should be the 1st entry of "."
    if (lio_fs_readdir(dir, &dname, &stat, NULL, 1) != 0) { lio_fs_closedir(dir); return(1); }
    if (dname) free(dname);

    //** This is ".."
    if (lio_fs_readdir(dir, &dname, &stat, NULL, 1) != 0) { lio_fs_closedir(dir); return(1); }
    if (dname) free(dname);

    //** This should be the 1st real entry
    if (lio_fs_readdir(dir, &dname, &stat, NULL, 1) != 0) { lio_fs_closedir(dir); return(1); }
    if (dname) free(dname);

    lio_fs_closedir(dir);
    return(0);
}

//***********************************************************************
// STDIO wrapper functions follow
//***********************************************************************

int chdir_lio(const char *path)
{
    char buffer[PATH_MAX];
    const char *mycwd;

    mycwd = path;
    apr_thread_mutex_lock(lock);
    FPRINTF("chdir_lio: START cwd=%s path=%s\n", cwd, path);
    if (path[0] != '/') { //**Relative path so convert it
        snprintf(buffer, PATH_MAX, "%s/%s", cwd, path);
        mycwd = buffer;
    }

    cwd_len = strlen(mycwd) + 1;
    memcpy(cwd, mycwd, cwd_len); //** Get the NULL terminator
    if (cwd[cwd_len-2] == '/') {
        cwd[cwd_len-2] = '\0';
        cwd_len--;
    }

    FPRINTF("chdir_lio: END cwd=%s\n", cwd);

    apr_thread_mutex_unlock(lock);

    return(0);
}

//***********************************************************************

int WRAPPER_PREFIX(chdir)(const char *path)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    char *fullpath;
    const char *fname;
    int index, len, err;

    FPRINTF("chdir. path=%s\n", path);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("chdir. normal path=%s\n", fullpath);
        apr_thread_mutex_lock(lock);
        err = chdir(fullpath);
        if (err == 0) {
            getcwd_stdio(cwd, sizeof(cwd));
            cwd_len = strlen(cwd) + 1;
        }
        apr_thread_mutex_unlock(lock);
        return(err);
    }

    //** We're changing to an LStore path
    //** Make sure it exists first
    if ((lio_fs_exists(fs, fname + len) && OS_OBJECT_DIR_FLAG) == 0) {
        errno = ENOTDIR;
        return(-1);
    }

    err = chdir_lio(path);
    FPRINTF("chdir. lio newcwd=%s\n", cwd);

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(fchdir)(int fd)
{
    fd_core_t *cfd;
    int err;

    FPRINTF("fchdir=%d\n", fd);
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) {
        apr_thread_mutex_lock(lock);
        err = fchdir_stdio(fd);
        if (err == 0) {
            getcwd_stdio(cwd, sizeof(cwd));
            cwd_len = strlen(cwd) + 1;
        }
        apr_thread_mutex_unlock(lock);
        return(err);
    }

    if (cfd->sname_dir) {
        return(chdir_lio(cfd->sname_dir));
    }

    return(-1);
}

//***********************************************************************

char *WRAPPER_PREFIX(getcwd)(char *buf, size_t size)
{
    char *ptr;
    int n = size;

    ptr = NULL;
    apr_thread_mutex_lock(lock);
    FPRINTF("getcwd: cwd=%s buf=%p size=%d\n", cwd, buf, n);
    if (buf) {
        if (n < cwd_len) {
            errno = ERANGE;
        } else {
            memcpy(buf, cwd, cwd_len);
            ptr = buf;
       }
    } else {
        if (n == 0) {
            tbx_type_malloc(ptr, char, cwd_len);
            memcpy(ptr, cwd, cwd_len);
        } else if (n >= cwd_len) {
            tbx_type_malloc(ptr, char, n);
            memcpy(ptr, cwd, cwd_len);
        } else {
            errno = ERANGE;
        }
    }
    apr_thread_mutex_unlock(lock);

    FPRINTF("getcwd: cwd=%s ptr=%s errno=%d\n", cwd, ptr, errno);

    return(ptr);
}

//***********************************************************************

char *WRAPPER_PREFIX(get_current_dir_name)(void)
{
    char *ptr;

    apr_thread_mutex_lock(lock);
    tbx_type_malloc(ptr, char, cwd_len);
    memcpy(ptr, cwd, cwd_len);
    apr_thread_mutex_unlock(lock);

    return(ptr);
}

//***********************************************************************

int WRAPPER_PREFIX(fchmodat)(int dirfd, const char *pathname, mode_t mode, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("fchmodat: dirfd=%d pathname=%s\n", dirfd, pathname);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(fchmodat_stdio(dirfd, fullpath, mode, flags));
    }

    //** It's a LIO file
    err = lio_fs_chmod(fs, NULL, fname, mode);
    if (err != 0) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(fchmod)(int fd, mode_t mode)
{
    fd_core_t *cfd;
    int err;

    FPRINTF("fchmod: start fd=%d\n", fd);
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) {
        if (STDIO_WRAP_NAME(fchmod) == NULL) {  STDIO_VAR_DLSYM(fchmod); }
        return(fchmod_stdio(fd, mode));
     }

    err = lio_fs_chmod(fs, NULL, lio_fd_path(cfd->lfd), mode);
    if (err != 0) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(chmod)(const char *pathname, mode_t mode)
{
    return(WRAPPER_PREFIX(fchmodat)(AT_FDCWD, pathname, mode, 0));
}


//***********************************************************************
// renameat2 - NOTE: we don't support a non-0 flags option! for LIO file combos
//***********************************************************************

int WRAPPER_PREFIX(renameat2)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, unsigned int flags)
{
    char newbuf[PATH_MAX];
    char oldbuf[PATH_MAX];
    char fnewbuf[PATH_MAX];
    char foldbuf[PATH_MAX];
    const char *fnew, *fold;
    char *newfull, *oldfull;
    int index_old, index_new, len_old, len_new, err, ftype, is_dir, ftype_old;
    struct stat sbuf;

    FPRINTF("renameat2: old=%s new=%s\n", oldpath, newpath);

    //** See if we handle it or pass it thru
    fold = path_map(olddirfd, oldpath, &index_old, &len_old, oldbuf, foldbuf, &oldfull);
    fnew = path_map(newdirfd, newpath, &index_new, &len_new, newbuf, fnewbuf, &newfull);

    err = 0;
    if (fold && fnew) { //** Both are LIO paths
        if (flags) {errno = EINVAL; return(-1); }
        return(lio_fs_rename(fs, NULL, fold, fnew));
    } else if (fold) { //** Source is LIO and dest is local
        if (flags) {errno = EINVAL; return(-1); }
        ftype_old = lio_fs_exists(fs, fold);
        is_dir = ftype_old & OS_OBJECT_DIR_FLAG;
        FPRINTF("renameat2: lio2local: ftype_old=%d is_dir=%d\n", ftype_old, is_dir);
        if (is_dir) { //** It's a directory rename on different mounts.. So only works with empty dirs
            //** MAke sure the source is empty
            if (lio_dir_is_empty(fold) == 1) {
                ftype = lio_os_local_filetype(newfull);
                FPRINTF("renameat2: lio2local: ftype_new=%d lio_dir_is_empty=1\n", ftype);
                if (ftype == 0) { //** Dest doesn't exist so create it
                    lio_fs_stat(fs, NULL, fold, &sbuf, NULL, 1);
                    err = mkdirat_stdio(newdirfd, newpath, sbuf.st_mode);
                } else {  //** Destination already exists
                    if ((ftype & OS_OBJECT_DIR_FLAG) == 0) {
                        errno = ENOTDIR;
                        err = -1;
                    }
                }
            } else { //** It's not empty so no simple rename
                FPRINTF("renameat2: lio2local: lio_dir_is_empty=0\n");
                errno = EXDEV;
                err = -1;
            }
        } else { //** It's a normal file
            FPRINTF("renameat2: lio2local: %s -> %s\n", fold, newfull);
            ftype = lio_os_local_filetype(newfull);
            if (ftype & OS_OBJECT_DIR_FLAG) { //** It's a directory so kick out
                errno = EXDEV;
                err = -1;
            } else {
                err = lio_fs_copy(fs, NULL, 1, fold, 0, newfull, -1, 0, 0, NULL);
            }
            FPRINTF("renameat2: lio2local: %s -> %s  err=%d\n", fold, newfull, err);
        }

        FPRINTF("renameat2: lio2local: err=%d errno=%d\n", err, errno);
        if (err == 0) lio_fs_object_remove(fs, NULL, fold, ftype);
        return(err);
    } if (fnew) { //** Source is local and dest is LIO
        if (flags) {errno = EINVAL; return(-1); }
        ftype_old = lio_os_local_filetype(oldfull);
        is_dir = ftype_old & OS_OBJECT_DIR_FLAG;
        if (is_dir) { //** It's a directory rename which only works with empty dirs across mounts
            //** Make sure the source is empty
            if (local_dir_is_empty(oldfull) == 1) {
                ftype = lio_fs_exists(fs, fnew);
                if (ftype == 0) { //** Dest doesn't exist so create it
                    sbuf.st_mode = 0;
                    __xstat_stdio(_STAT_VER, fold, &sbuf);
                    err = lio_fs_object_create(fs, NULL, fnew, sbuf.st_mode, ftype_old);
                } else {  //** Destination already exists
                    if ((ftype & OS_OBJECT_DIR_FLAG) == 0) {
                        errno = ENOTDIR;
                        err = -1;
                    }
                }
            } else { //** It's not empty so no simple rename
                errno = EXDEV;
                err = -1;
            }
        } else { //** It's a normal file
            FPRINTF("renameat2: local2lio: %s -> %s\n", oldfull, fnew);
            ftype = lio_fs_exists(fs, fnew);
            if (ftype & OS_OBJECT_DIR_FLAG) { //** It's a directory so kick out
                errno = EXDEV;
                err = -1;
            } else {
                err = lio_fs_copy(fs, NULL, 0, oldfull, 1, fnew, -1, 0, 0, NULL);
            }
            FPRINTF("renameat2: local2lio: %s -> %s err=%d\n", oldfull, fnew, err);
        }

        FPRINTF("renameat2: local2lio: err=%d errno=%d\n", err, errno);
        if (err == 0) remove_stdio(oldfull);
        return(err);
    }

    //** Both the source and dest are local
    err = renameat2_stdio(olddirfd, oldpath, newdirfd, newpath, flags);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(renameat)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath)
{
    return(WRAPPER_PREFIX(renameat2)(olddirfd, oldpath, newdirfd, newpath, 0));
}

//***********************************************************************

int WRAPPER_PREFIX(rename)(const char *oldpath, const char *newpath)
{
    return(WRAPPER_PREFIX(renameat2)(AT_FDCWD, oldpath, AT_FDCWD, newpath, 0));
}

//***********************************************************************

int WRAPPER_PREFIX(symlinkat)(const char *target, int newdirfd, const char *linkpath)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("symlinkat: dirfd=%d target=%s linkpath=%s\n", newdirfd, target, linkpath);

    //** See if we handle it or pass it thru
    fname = path_map(newdirfd, linkpath, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(symlinkat_stdio(target, newdirfd, linkpath));
    }

    //** It's a LIO file
    err = lio_fs_symlink(fs, NULL, target, fname);
    if (err != 0) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(symlink)(const char *target, const char *linkpath)
{
    return(symlinkat(target, AT_FDCWD, linkpath));
}

//***********************************************************************

int WRAPPER_PREFIX(linkat)(int olddirfd, const char *oldpath, int newdirfd, const char *newpath, int flags)
{
    char obuf[PATH_MAX], nbuf[PATH_MAX];
    char fobuf[PATH_MAX], fnbuf[PATH_MAX];
    const char *ofname, *nfname;
    char *ofull, *nfull;
    int oindex, olen, nindex, nlen, err;

    FPRINTF("linkat: olddirfd=%d oldpath=%s newdirfd=%d newpath=%s flags=%d\n", olddirfd, oldpath, newdirfd, newpath, flags);

    //** See if we handle it or pass it thru. You can only hardlink within LStore or external. No mixing
    //** So we have to check both the source and destination paths
    ofname = path_map(olddirfd, oldpath, &oindex, &olen, obuf, fobuf, &ofull);
    nfname = path_map(newdirfd, newpath, &nindex, &nlen, nbuf, fnbuf, &nfull);

    if ((ofname == NULL) && (nfname == NULL)) {
        FPRINTF("linkat: normal olddirfd=%d oldpath=%s newdirfd=%d newpath=%s flags=%d\n", olddirfd, oldpath, newdirfd, newpath, flags);
        return(linkat_stdio(olddirfd, oldpath, newdirfd, newpath, flags));
    } else if ((ofname == NULL) || (nfname == NULL)) { //** Mix case so throw an error
        FPRINTF("linkat: ERROR mixed olddirfd=%d oldpath=%s newdirfd=%d newpath=%s flags=%d\n", olddirfd, oldpath, newdirfd, newpath, flags);
        errno = EXDEV;
        return(-1);
    }

    FPRINTF("linkat: lio ofname=%s nfname=%s\n", ofname, nfname);

    //** If we make it here then both paths are on LStore
    err = lio_fs_hardlink(fs, NULL, ofname, nfname);
    FPRINTF("linkat: lio ofname=%s nfname=%s err=%d\n", ofname, nfname, err);
    if (err != 0) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(link)(const char *oldpath, const char *newpath)
{
    return(linkat(AT_FDCWD, oldpath, AT_FDCWD, newpath, 0));
}

//***********************************************************************
// fopen2open_flags - Helper to convert fopen flags to normal open flags
//***********************************************************************

int fopen2open_flags(const char *sflags)
{
    int mode = -1;

    if (strcmp(sflags, "r") == 0) {
        mode = O_RDONLY;
    } else if (strcmp(sflags, "r+") == 0) {
        mode = O_RDWR;
    } else if (strcmp(sflags, "w") == 0) {
        mode = O_WRONLY | O_TRUNC |  O_CREAT;
    } else if (strcmp(sflags, "w+") == 0 ) {
        mode = O_RDWR | O_TRUNC | O_CREAT;
    } else if (strcmp(sflags, "a") == 0) {
        mode = O_WRONLY | O_CREAT | O_APPEND;
    } else if (strcmp(sflags, "a+") == 0) {
        mode = O_RDWR | O_CREAT | O_APPEND;
    }

    mode |= O_LARGEFILE;  //** We always support this

    return(mode);
}

//***********************************************************************

FILE *WRAPPER_PREFIX(fopen64)(const char *pathname, const char *mode)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, slot;
    lio_fd_t *lfd;

    FPRINTF("fopen64. pathname=%s, mode=%s\n", pathname, mode);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("fopen64. normal file fname=%s\n", pathname);
        return(fopen64_stdio(fullpath, mode));
    }

    FPRINTF("fopen64. lio file fname=%s index=%d len=%d\n", fname, index, len);

    //** It's a LIO file
    lfd = lio_fs_open(fs, NULL, fname + len, lio_fopen_flags(mode));
    if (lfd) {
        slot = get_free_slot(-1, corefd_new(FD_MODE_LIO, fopen2open_flags(mode), -1, NULL, lfd, NULL, NULL), 0);
        FPRINTF("fopen64 lio slot=%d _fileno=%d\n", slot, fd_table[slot].dfd._fileno);
        fd_table[slot].cfd->prefix_index = index;
        return(&(fd_table[slot].dfd));
    }

    return(NULL);
}

//***********************************************************************

FILE *WRAPPER_PREFIX(fopen)(const char *pathname, const char *mode)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    char *fullpath;
    const char *fname;
    int index, len, slot;
    lio_fd_t *lfd;

    FPRINTF("fopen. pathname=%s mode=%s\n", pathname, mode); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("fopen. normal file fname=%s fopen_stdio=%p\n", pathname, fopen_stdio);
        if (!fopen_stdio) {   //** This can occur during startup when loading other libraries
            assert_result_not_null(fopen_stdio = dlsym(RTLD_NEXT, "fopen"));
        }
        return(fopen_stdio(fullpath, mode));
    }

    FPRINTF("fopen. lio file fname=%s index=%d len=%d\n", fname, index, len);

    //** It's a LIO file
    lfd = lio_fs_open(fs, NULL, fname + len, lio_fopen_flags(mode));
    if (lfd) {
        slot = get_free_slot(-1, corefd_new(FD_MODE_LIO, fopen2open_flags(mode), -1, NULL, lfd, NULL, NULL), 0);
        FPRINTF("fopen lio slot=%d _fileno=%d\n", slot, fd_table[slot].dfd._fileno);
        fd_table[slot].cfd->prefix_index = index;
        return(&(fd_table[slot].dfd));
    }

    return(NULL);

}

//***********************************************************************

int vopenat64(int dirfd, const char *pathname, int flags, va_list ap)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, slot, fd, n, ftype;
    lio_fd_t *lfd;
    mode_t mode;

    mode = (O_CREAT|O_TMPFILE) ? va_arg(ap, int) : 0;
    slot = mode;
    FPRINTF("vopenat64=%s flags=%d mode=%o O_DIRECTORY=%d O_PATH=%d flags&O_DIR=%d\n", pathname, flags, slot, O_DIRECTORY, O_PATH, (flags&O_DIRECTORY));

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("vopenat64=%s normal\n", pathname);
        if (openat64_stdio == NULL) { assert_result_not_null(openat64_stdio = dlsym(RTLD_NEXT, "openat64")); }
        if (mode == 0) {
            fd = openat64_stdio(dirfd, pathname, flags);
        } else {
            fd = openat64_stdio(dirfd, pathname, flags, mode);
        }

        if (fd == -1) return(fd);

        ftype = lio_os_local_filetype(fullpath);
        if (ftype & OS_OBJECT_DIR_FLAG) { //** It's a direcroy so cache the path
            apr_thread_mutex_unlock(lock);
            fd_table[fd].cfd = corefd_new(FD_MODE_STD, flags, -1, NULL, NULL, NULL, NULL);
            fd_table[fd].cfd->lname_dir = NULL;
            fd_table[fd].cfd->sname_dir = strdup(fullpath);
            n = strlen(fullpath);
            if (fullpath[n-1] == '/') fd_table[fd].cfd->sname_dir[n-1] = '\0';
            FPRINTF("vopenat64 normal O_DIR slot=%d _fileno=%d\n", fd, fd_table[fd].dfd._fileno);
            fd_table[fd].cfd->prefix_index = index;
            apr_thread_mutex_unlock(lock);
            return(fd_table[fd].dfd._fileno);
        }

        return(fd);
    }

    ftype = lio_fs_exists(fs, fname + len);
    if (ftype & OS_OBJECT_DIR_FLAG) { //** This is directory and not a file so fake an FD for a subsequent fdopendir
        slot = get_free_slot(-1, corefd_new(FD_MODE_LIO, flags, -1, NULL, NULL, NULL, NULL), 0);
        fd_table[slot].cfd->lname_dir = strdup(fname + len);
        fd_table[slot].cfd->sname_dir = strdup(fullpath);
        FPRINTF("vopenat64 lio O_DIR slot=%d _fileno=%d\n", slot, fd_table[slot].dfd._fileno);
        fd_table[slot].cfd->prefix_index = index;
        return(fd_table[slot].dfd._fileno);
    } else if (flags & O_DIRECTORY) {
        FPRINTF("vopenat64 lio O_DIR set and it's not a directory fname=%s\n", fname + len);
        errno = ENOTDIR;
        return(-1);
    }


    //** It's a LIO file
    lfd = lio_fs_open(fs, NULL, fname + len, lio_open_flags(flags, mode));
    FPRINTF("vopenat64. lio fname=%s lfd=%p\n", fname + len, lfd);
    if (lfd) {
        slot = get_free_slot(-1, corefd_new(FD_MODE_LIO, flags, -1, NULL, lfd, NULL, NULL), 0);
        FPRINTF("vopenat64. lio slot=%d _fileno=%d\n", slot, fd_table[slot].dfd._fileno);
        fd_table[slot].cfd->prefix_index = index;
        return(fd_table[slot].dfd._fileno);
    }

    return(-1);
}

//***********************************************************************

int vcreat(const char *pathname, ...)
{
    va_list ap;
    int n;

    FPRINTF("vcreat=%s CALLING vopen64\n", pathname);

    va_start(ap, pathname);
    n = vopenat64(AT_FDCWD, pathname, O_CREAT, ap);
    va_end(ap);

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(creat)(const char *pathname, mode_t mode)
{
    FPRINTF("creat=%s CALLING vcreat\n", pathname);
    return(vcreat(pathname, mode));
}

//***********************************************************************

int WRAPPER_PREFIX(open64)(const char *pathname, int flags, ...)
{
    va_list ap;
    int n;

    FPRINTF("open64=%s flags=%d CALLING vopen64\n", pathname, flags);

    va_start(ap, flags);
    n = vopenat64(AT_FDCWD, pathname, flags, ap);
    va_end(ap);

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(open)(const char *pathname, int flags, ...)
{
    va_list ap;
    int n;

    FPRINTF("open=%s flags=%d CALLING vopen64\n", pathname, flags);

    va_start(ap, flags);
    n = vopenat64(AT_FDCWD, pathname, flags, ap);
    va_end(ap);

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(openat64)(int dirfd, const char *pathname, int flags, ...)
{
    va_list ap;
    int n;

    FPRINTF("openat64=%s dirfd=%d flags=%d CALLING vopen64\n", pathname, dirfd, flags);

    va_start(ap, flags);
    n = WRAPPER_PREFIX(vopenat64)(dirfd, pathname, flags, ap);
    va_end(ap);

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(openat)(int dirfd, const char *pathname, int flags, ...)
{
    va_list ap;
    int n;

    FPRINTF("openat=%s dirfd=%d flags=%d CALLING vopen64\n", pathname, dirfd, flags);

    va_start(ap, flags);
    n = WRAPPER_PREFIX(vopenat64)(dirfd, pathname, flags, ap);
    va_end(ap);

    return(n);
}


//***********************************************************************

int WRAPPER_PREFIX(fclose)(FILE *fd)
{
    FPRINTF("fclose=%d\n", fd->_fileno);

    if (!fd) return(-1);
    return(close_and_reserve(fd->_fileno, fd, NULL, 0));
}

//***********************************************************************

int WRAPPER_PREFIX(close)(int fd)
{
    FPRINTF("close=%d\n", fd);
    return(close_and_reserve(fd, NULL, NULL, 0));
}

//***********************************************************************

int WRAPPER_PREFIX(unlink)(const char *pathname)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("unlink: fname=%s\n", pathname);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("unlink. normal file fname=%s\n", pathname);
        return(unlink_stdio(fullpath));
    }

    return(lio_fs_object_remove(fs, NULL, fname + len, OS_OBJECT_FILE_FLAG));
}

//***********************************************************************

int WRAPPER_PREFIX(remove)(const char *pathname)
{
    FPRINTF("remove: fname=%s\n", pathname);
    return(WRAPPER_PREFIX(unlink)(pathname));
}

//***********************************************************************

int WRAPPER_PREFIX(unlinkat)(int dirfd, const char *pathname, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, ftype;

    FPRINTF("unlinkat: fname=%s dirfd=%d AT_FDCWD=%d\n", pathname, dirfd, AT_FDCWD);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("unlinkat. normal file fname=%s\n", pathname);
        return(unlinkat_stdio(dirfd, fullpath, flags));
    }

    ftype = (flags & AT_REMOVEDIR) ? OS_OBJECT_FILE_FLAG : OS_OBJECT_DIR_FLAG;
    return(lio_fs_object_remove(fs, NULL, fname + len, ftype));
}

//***********************************************************************

int WRAPPER_PREFIX(rmdir)(const char *pathname)
{
    FPRINTF("rmdir: fname=%s\n", pathname);
    return(WRAPPER_PREFIX(unlinkat)(AT_FDCWD, pathname, AT_REMOVEDIR));
}

//***********************************************************************

int WRAPPER_PREFIX(mkdirat)(int dirfd, const char *pathname, mode_t mode)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("mkdirat: fname=%s dirfd=%d AT_FDCWD=%d\n", pathname, dirfd, AT_FDCWD);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("mkdirat. normal file fname=%s\n", pathname);
        return(mkdirat_stdio(dirfd, fullpath, mode));
    }

    return(lio_fs_mkdir(fs, NULL, fname + len, mode));
}

//***********************************************************************

int WRAPPER_PREFIX(mkdir)(const char *pathname, mode_t mode)
{
    FPRINTF("mkdir: fname=%s. calling mkdirat\n", pathname);
    return(WRAPPER_PREFIX(mkdirat)(AT_FDCWD, pathname,mode));
}

//***********************************************************************

FD_TEMPLATE(futimens, int, (int fd, const struct timespec times[2]), futimens_stdio(fd, times), lio_fs_utimens(fs, NULL, lio_fd_path(cfd->lfd), times))
FD_TEMPLATE(lseek, off_t, (int fd, off_t offset, int whence), lseek_stdio(fd, offset, whence), lio_fs_seek(fs, cfd->lfd, offset, whence))
FD_TEMPLATE(ftruncate, int, (int fd, off_t length), ftruncate_stdio(fd, length), lio_fs_ftruncate(fs, cfd->lfd, length))
FD_TEMPLATE(ftruncate64, int, (int fd, off_t length), ftruncate64_stdio(fd, length), lio_fs_ftruncate(fs, cfd->lfd, length))

//***********************************************************************

int WRAPPER_PREFIX(truncate)(const char *path, off_t length)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("truncate: fname=%s\n", path);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(truncate_stdio(fullpath, length));
    }

    return(lio_fs_truncate(fs, NULL, fname + len, length));
}

//***********************************************************************

int WRAPPER_PREFIX(truncate64)(const char *path, off_t length)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("truncate64: fname=%s\n", path);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(truncate64_stdio(fullpath, length));
    }

    return(lio_fs_truncate(fs, NULL, fname + len, length));
}

//***********************************************************************

int WRAPPER_PREFIX(fgetpos)(FILE *stream, fpos_t *pos)
{
    fd_core_t *cfd;
    int fd;
    ex_off_t nbytes;

    FPRINTF("fgetpos: slot=%d\n", stream->_fileno);

    fd = stream->_fileno;
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fgetpos_stdio(stream, pos));

    nbytes = lio_fs_seek(fs, cfd->lfd, 0, SEEK_CUR);
    if (nbytes >= 0) {
        errno = -nbytes;
        return(-1);
    }

    pos->__pos = nbytes;
    return(0);
}

//***********************************************************************

int WRAPPER_PREFIX(fsetpos)(FILE *stream, const fpos_t *pos)
{
    fd_core_t *cfd;
    int fd;
    ex_off_t nbytes;

    FPRINTF("fsetpos: slot=%d\n", stream->_fileno);

    fd = stream->_fileno;
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fsetpos_stdio(stream, pos));

    nbytes = lio_fs_seek(fs, cfd->lfd, pos->__pos, SEEK_SET);
    if (nbytes >= 0) {
        errno = -nbytes;
        return(-1);
    }

    return(0);
}


//***********************************************************************

int WRAPPER_PREFIX(fseek)(FILE *stream, long offset, int whence)
{
    return(WRAPPER_PREFIX(lseek)(fileno(stream), offset, whence));
}

//***********************************************************************

long WRAPPER_PREFIX(ftell)(FILE *stream)
{
    return(WRAPPER_PREFIX(lseek)(fileno(stream), 0, SEEK_CUR));
}

//***********************************************************************

int WRAPPER_PREFIX(fseeko)(FILE *stream, long offset, int whence)
{
    return(WRAPPER_PREFIX(lseek)(fileno(stream), offset, whence));
}

//***********************************************************************

off_t  WRAPPER_PREFIX(ftello)(FILE *stream)
{
    return(WRAPPER_PREFIX(lseek)(fileno(stream), 0, SEEK_CUR));
}

//***********************************************************************

void WRAPPER_PREFIX(rewind)(FILE *stream)
{
    WRAPPER_PREFIX(lseek)(fileno(stream), 0, SEEK_SET);
    return;
}

//***********************************************************************

size_t WRAPPER_PREFIX(fread)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    fd_core_t *cfd;
    int fd;
    size_t got, nbytes;

    FPRINTF("fread: slot=%d\n", stream->_fileno);

    fd = stream->_fileno;
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fread_stdio(ptr, size, nmemb, stream));

    nbytes = size * nmemb;
    got = lio_fs_read(fs, cfd->lfd, ptr, nbytes);
    FPRINTF("fread. lio read nmemb=" ST " n=" ST "\n", nmemb, got);
    return(got / size);
}

//***********************************************************************

size_t WRAPPER_PREFIX(fread_unlocked_UNUSED)(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    FPRINTF("fread_unlocked\n");
    return(fread_unlocked_stdio(ptr, size, nmemb, stream));
}


//***********************************************************************

FD_TEMPLATE(read, ssize_t, (int fd, void *ptr, size_t count), read_stdio(fd, ptr, count), lio_fs_read(fs, cfd->lfd, ptr, count))
FD_TEMPLATE(readv, ssize_t, (int fd, const struct iovec *iov, int iovcnt), readv_stdio(fd, iov, iovcnt), lio_fs_readv(fs, cfd->lfd, iov, iovcnt, -1))
FD_TEMPLATE(pread64, ssize_t, (int fd, void *ptr, size_t count, off_t offset), pread64_stdio(fd, ptr, count, offset), lio_fs_pread(fs, cfd->lfd, ptr, count, offset))
FD_TEMPLATE(preadv, ssize_t, (int fd, const struct iovec *iov, int iovcnt, off_t offset), preadv_stdio(fd, iov, iovcnt, offset), lio_fs_readv(fs, cfd->lfd, iov, iovcnt, offset))
FD_TEMPLATE(preadv2, ssize_t, (int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags), preadv2_stdio(fd, iov, iovcnt, offset, flags), lio_fs_readv(fs, cfd->lfd, iov, iovcnt, offset))
FD_TEMPLATE(pread, ssize_t, (int fd, void *ptr, size_t count, off_t offset), pread_stdio(fd, ptr, count, offset), lio_fs_pread(fs, cfd->lfd, ptr, count, offset))

//***********************************************************************

size_t WRAPPER_PREFIX(fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    fd_core_t *cfd;
    int fd;
    size_t got, nbytes;

    FPRINTF("fwrite: slot=%d\n", stream->_fileno);

    fd = stream->_fileno;

    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fwrite_stdio(ptr, size, nmemb, stream));

    nbytes = size * nmemb;
    got = lio_fs_write(fs, cfd->lfd, ptr, nbytes);
    FPRINTF("fwrite. lio read nmemb=" ST " n=" ST "\n", nmemb, got);
    return(got / size);
}

//***********************************************************************

size_t WRAPPER_PREFIX(fwrite_unlocked_UNUSED)(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    return(fwrite_unlocked_stdio(ptr, size, nmemb, stream));
}

//***********************************************************************

FD_TEMPLATE(write, ssize_t, (int fd, const void *ptr, size_t count), write_stdio(fd, ptr, count), lio_fs_write(fs, cfd->lfd, ptr, count))
FD_TEMPLATE(writev, ssize_t, (int fd, const struct iovec *iov, int iovcnt), writev_stdio(fd, iov, iovcnt), lio_fs_writev(fs, cfd->lfd, iov, iovcnt, -1))
FD_TEMPLATE(pwrite64, ssize_t, (int fd, const void *ptr, size_t count, off_t offset), pwrite64_stdio(fd, ptr, count, offset), lio_fs_pwrite(fs, cfd->lfd, ptr, count, offset))
FD_TEMPLATE(pwritev, ssize_t, (int fd, const struct iovec *iov, int iovcnt, off_t offset), pwritev_stdio(fd, iov, iovcnt, offset), lio_fs_writev(fs, cfd->lfd, iov, iovcnt, offset))
FD_TEMPLATE(pwritev2, ssize_t, (int fd, const struct iovec *iov, int iovcnt, off_t offset, int flags), pwritev2_stdio(fd, iov, iovcnt, offset, flags), lio_fs_writev(fs, cfd->lfd, iov, iovcnt, offset))

//***********************************************************************
// posix_fadvise
//***********************************************************************
FD_TEMPLATE(posix_fadvise, int, (int fd, off_t offset, off_t len, int advice), posix_fadvise_stdio(fd, offset, len, advice), lio_fs_fadvise(fs, cfd->lfd, offset, len, advice))

//**** Dup and dup2 commands.  We currently don't honor the "lowest fd" request in dup

FD_TEMPLATE(dup, int, (int fd), dup_stdio(fd), get_free_slot(fd, NULL, 1))

//***********************************************************************

int WRAPPER_PREFIX(dup2)(int oldfd, int newfd)
{
    fd_stdio_t *jfd_old, *jfd_new;

    FPRINTF("dup2 old=%d new=%d\n", oldfd, newfd);

    //** Sanity check the value
    SANITY_CHECK_FD(oldfd, -1);
    jfd_old = &(fd_table[oldfd]);

    //** Sanity check the newFD also
    SANITY_CHECK_FD(newfd, -1);
    jfd_new = &(fd_table[newfd]);

    FPRINTF("dup2 old=%d new=%d\n", oldfd, newfd);

    if ((jfd_old->cfd) && (jfd_old->cfd->mode != FD_MODE_STD)) {  //** Got an LStore file for the source
        apr_thread_mutex_lock(lock);
        if ((jfd_new->cfd) && (jfd_new->cfd->mode != FD_MODE_STD)) {
            FPRINTF("dup2 old=%d new=%d LIO with close\n", oldfd, newfd);
            apr_thread_mutex_unlock(lock);
            close_and_reserve(newfd, NULL, jfd_old->cfd, 1);
            return(newfd);
        } else {
            FPRINTF("dup2 old=%d new=%d LIO dup\n", oldfd, newfd);
            jfd_new->cfd = jfd_old->cfd;
            jfd_new->cfd->dup_count++;
        }
        apr_thread_mutex_unlock(lock);
        return(newfd);
    }

    //** Not an LStore file for the source
    apr_thread_mutex_lock(lock);
    if ((jfd_new->cfd) && (jfd_new->cfd->mode != FD_MODE_STD)) {
        FPRINTF("dup2 old=%d new=%d LIO-src\n", oldfd, newfd);
        close_and_reserve(newfd, NULL, NULL, 0);
    }
    apr_thread_mutex_unlock(lock);

    FPRINTF("dup2 old=%d new=%d STDIO\n", oldfd, newfd);

    return(dup2_stdio(oldfd, newfd));
}

//***********************************************************************
// select and poll routines
//***********************************************************************

int fd_set_is_lio_prep(int nfds, fd_set *fds, int *fd_lio, int *fd_max)
{
    int i, n;
    fd_core_t *cfd;

    *fd_max = 0;
    if (fds == NULL) return(0);

    n = 0;
    for (i=0; i<nfds; i++) {
        if (FD_ISSET(i, fds)) {
            cfd = fd_table[i].cfd;
            if ((cfd != NULL) && (cfd->mode == FD_MODE_LIO)) {
                FD_CLR(i, fds);  //** We clear them for the actual select and then add them back
                fd_lio[n] = i;
                n++;
            } else {
                *fd_max = i;
            }
        }
    }

    return(n);
}

//***********************************************************************

void fd_set_add_lio(fd_set *fds, int *fd_lio, int fd_used)
{
    int i;

    if ((fds == NULL) || (fd_used == 0)) return;

    for (i=0; i<fd_used; i++) {
        FD_SET(fd_lio[i], fds);
    }
}

//***********************************************************************

int WRAPPER_PREFIX(select)(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, struct timeval *timeout)
{
    int n, max_fd;
    int read_lio[n_fd_max2], n_read_lio, read_max;
    int write_lio[n_fd_max2], n_write_lio, write_max;
    int except_lio[n_fd_max2], n_except_lio, except_max;
    struct timeval to_zero;

    to_zero.tv_sec = 0; to_zero.tv_usec = 0;

FPRINTF("select: nfds=%d r=%p w=%p e=%p\n", nfds, readfds, writefds, exceptfds);

    //** See if any of the fd's are LIO and also clear out any of them that are LIO fd's
    n_read_lio = fd_set_is_lio_prep(nfds, readfds, read_lio, &read_max);
    n_write_lio = fd_set_is_lio_prep(nfds, writefds, write_lio, &write_max);
    n_except_lio = fd_set_is_lio_prep(nfds, exceptfds, except_lio, &except_max);

    FPRINTF("select: rl=%d wl=%d el=%d\n", n_read_lio, n_write_lio, n_except_lio);

    if ((n_read_lio+n_write_lio+n_except_lio) == 0) {  //** No LIO Fds
        return(select_stdio(nfds, readfds, writefds, exceptfds, timeout));
    }

    //** Got some LIO FDs so first handle the normal STDIO files with 0 time
    //** Figoure out the new max fd without the LIO fds
    max_fd = (read_max > write_max) ? read_max : write_max;
    if (max_fd < except_max) max_fd = except_max;

    //** Do the normal select() call but don't wait
    n = select_stdio(max_fd, (read_max ? readfds : NULL), (write_max ? writefds : NULL), (except_max ? exceptfds : NULL), &to_zero);

    //** Now merge back in the LIO FDs
    fd_set_add_lio(readfds, read_lio, n_read_lio);
    fd_set_add_lio(writefds, write_lio, n_write_lio);
//    fd_set_add_lio(exceptfds, except_lio, n_except_lio);

    n += n_read_lio + n_write_lio;

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(pselect)(int nfds, fd_set *readfds, fd_set *writefds, fd_set *exceptfds, const struct timespec *timeout, const sigset_t *sigmask)
{
    int n, max_fd;
    int read_lio[n_fd_max2], n_read_lio, read_max;
    int write_lio[n_fd_max2], n_write_lio, write_max;
    int except_lio[n_fd_max2], n_except_lio, except_max;
    struct timespec to_zero;

    to_zero.tv_sec = 0; to_zero.tv_nsec = 0;

    FPRINTF("pselect: nfds=%d r=%p w=%p e=%p\n", nfds, readfds, writefds, exceptfds);

    //** See if any of the fd's are LIO and also clear out any of them that are LIO fd's
    n_read_lio = fd_set_is_lio_prep(nfds, readfds, read_lio, &read_max);
    n_write_lio = fd_set_is_lio_prep(nfds, writefds, write_lio, &write_max);
    n_except_lio = fd_set_is_lio_prep(nfds, exceptfds, except_lio, &except_max);

    FPRINTF("pselect: rl=%d wl=%d el=%d\n", n_read_lio, n_write_lio, n_except_lio);

    if ((n_read_lio+n_write_lio+n_except_lio) == 0) {  //** No LIO Fds
        return(pselect_stdio(nfds, readfds, writefds, exceptfds, timeout, sigmask));
    }

    //** Got some LIO FDs so first handle the normal STDIO files with 0 time
    //** Figoure out the new max fd without the LIO fds
    max_fd = (read_max > write_max) ? read_max : write_max;
    if (max_fd < except_max) max_fd = except_max;

    //** Do the normal select() call but don't wait
    n = pselect_stdio(max_fd, (read_max ? readfds : NULL), (write_max ? writefds : NULL), (except_max ? exceptfds : NULL), &to_zero, sigmask);

    //** Now merge back in the LIO FDs
    fd_set_add_lio(readfds, read_lio, n_read_lio);
    fd_set_add_lio(writefds, write_lio, n_write_lio);
//    fd_set_add_lio(exceptfds, except_lio, n_except_lio);

    n += n_read_lio + n_write_lio;

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(poll)(struct pollfd *fds, nfds_t nfds, int timeout)
{
    long unsigned i, j, fd, n, nlio;
    fd_core_t *cfd;
    int lio_fds[n_fd_max2], lslot[n_fd_max2];
    struct pollfd *myfd;

    FPRINTF("poll: nfds=%lu\n", nfds);

    //** See if any of the fd's are LIO and also clear out any of them that are LIO fd's
    nlio = 0;
    for (i=0; i<nfds; i++) {
        fd = fds[i].fd;
        cfd = fd_table[fd].cfd;
        if ((cfd != NULL) && (cfd->mode == FD_MODE_LIO)) {
            lio_fds[i] = fd;
            lslot[nlio] = i;
            nlio++;
            fds[i].fd = -1;
        }
    }

    FPRINTF("poll: nlio=%lu\n", nlio);

    if (nlio == 0) {  //** No LIO FDs
        return(poll_stdio(fds, nfds, timeout));
    }

    //** Do a 0-wait poll on the STD FDs
    n = poll_stdio(fds, nfds, 0);

    //** Now merge back in the LIO FDs
    for (i=0; i<nlio; i++) {
        j = lslot[i];
        myfd = &fds[j];
        myfd->fd = lio_fds[j];
        myfd->revents = myfd->events & (POLLIN|POLLOUT|POLLRDNORM|POLLWRNORM);
    }
    n += nlio;

    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(ppoll)(struct pollfd *fds, nfds_t nfds, const struct timespec *tmo_p, const sigset_t *sigmask)
{
    long unsigned i, j, fd, n, nlio;
    fd_core_t *cfd;
    int lio_fds[n_fd_max2], lslot[n_fd_max2];
    struct pollfd *myfd;
    struct timespec to_zero;

    to_zero.tv_sec = 0; to_zero.tv_nsec = 0;
    FPRINTF("ppoll: nfds=%lu\n", nfds);

    //** See if any of the fd's are LIO and also clear out any of them that are LIO fd's
    nlio = 0;
    for (i=0; i<nfds; i++) {
        fd = fds[i].fd;
        cfd = fd_table[fd].cfd;
        if ((cfd != NULL) && (cfd->mode == FD_MODE_LIO)) {
            lio_fds[i] = fd;
            lslot[nlio] = i;
            nlio++;
            fds[i].fd = -1;
        }
    }

    FPRINTF("ppoll: nlio=%lu\n", nlio);

    if (nlio == 0) {  //** No LIO FDs
        return(ppoll_stdio(fds, nfds, tmo_p, sigmask));
    }

    //** Do a 0-wait poll on the STD FDs
    n = ppoll_stdio(fds, nfds, &to_zero, sigmask);

    //** Now merge back in the LIO FDs
    for (i=0; i<nlio; i++) {
        j = lslot[i];
        myfd = &fds[j];
        myfd->fd = lio_fds[j];
        myfd->revents = myfd->events & (POLLIN|POLLOUT|POLLRDNORM|POLLWRNORM);
    }
    n += nlio;

    return(n);
}


//***********************************************************************

ssize_t WRAPPER_PREFIX(readlinkat)(int dirfd, const char *pathname, char *buf, size_t bufsiz)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    char flink[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err, n;

    FPRINTF("readlinkat:  pathname=%s\n", pathname);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(readlinkat_stdio(dirfd, fullpath, buf, bufsiz));
    }

    FPRINTF("readlinkat: before pathname=%s fname=%s len=%d\n", pathname, fname, len);
    err = lio_fs_readlink(fs, NULL, fname + len, buf, bufsiz);
    if (err) {
        errno = -err;
        err = -1;
        return(err);
    }

    if (buf[0] == '/') { //** Absolute path so need to prepend the mount path
        flink[PATH_MAX-1] = 0;
        n = snprintf(flink, PATH_MAX, "%s%s", prefix_table[index].prefix, buf + len);
        memcpy(buf, flink, n+1);
    } else {
        n = strlen(buf);
    }

    FPRINTF("readlinkat: after pathname=%s readlink=%s n=%d\n", pathname, buf, n);
    return(n);

}

//***********************************************************************

ssize_t WRAPPER_PREFIX(readlink)(const char *pathname, char *buf, size_t bufsiz)
{
    return(WRAPPER_PREFIX(readlinkat(AT_FDCWD, pathname, buf, bufsiz)));
}

//***********************************************************************

int WRAPPER_PREFIX(__xstat)(int __ver, const char *pathname, struct stat *statbuf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("xstat: fname=%s\n", pathname);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(__xstat_stdio(__ver, fullpath, statbuf));
    }

    err = lio_fs_stat(fs, NULL, fname + len, statbuf, NULL, 0);
    FPRINTF("xstat: LIO fname=%s err=%d\n", pathname, err);
    if (err) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__xstat64)(int __ver, const char *pathname, struct stat64 *statbuf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("xstat64: fname=%s\n", pathname);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(__xstat64_stdio(__ver, fullpath, statbuf));
    }

    err = lio_fs_stat(fs, NULL, fname + len, (struct stat *)statbuf, NULL, 0);
    FPRINTF("xstat64: LIO fname=%s err=%d ino=" XIDT "\n", pathname, err, statbuf->st_ino);
    if (err) {
        errno = -err;
        err = -1;
    }

    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__lxstat)(int __ver, const char *pathname, struct stat *statbuf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("lxstat: pathname=%s\n", pathname); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(__lxstat_stdio(__ver, fullpath, statbuf));
    }

    FPRINTF("lxstat: before pathname=%s fname=%s len=%d\n", pathname, fname, len); fflush(stderr);
    err = lio_fs_stat(fs, NULL, fname + len, statbuf, NULL, 1);
    if (err) {
        errno = -err;
        err = -1;
    }
    FPRINTF("lxstat: after pathname=%s fname=%s len=%d lio_fs_stat=%d\n", pathname, fname, len, err); fflush(stderr);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__lxstat64)(int __ver, const char *pathname, struct stat64 *statbuf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("lxstat64: pathname=%s\n", pathname); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        if (__lxstat64_stdio == NULL) { assert_result_not_null(__lxstat64_stdio = dlsym(RTLD_NEXT, "access")); }
        return(__lxstat64_stdio(__ver, fullpath, statbuf));
    }

    FPRINTF("lxstat64: before pathname=%s fname=%s len=%d\n", pathname, fname, len); fflush(stderr);
    err = lio_fs_stat(fs, NULL, fname + len, (struct stat *)statbuf, NULL, 1);
    if (err) {
        errno = -err;
        err = -1;
    }
    FPRINTF("lxstat64: after pathname=%s fname=%s len=%d lio_fs_stat=%d\n", pathname, fname, len, err); fflush(stderr);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__fxstat64)(int __ver, int fd, struct stat64 *statbuf)
{
    fd_core_t *cfd;
    int err;

    FPRINTF("fxstat64: slot=%d\n", fd);
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(__fxstat64_stdio(__ver, fd, statbuf));

    FPRINTF("fxstat64: slot=%d before lio call lfd=%p\n", fd, cfd->lfd);
    err = lio_fs_stat(fs, NULL, lio_fd_path(cfd->lfd), (struct stat *)statbuf, NULL, 0);
    FPRINTF("fxstat64: slot=%d err=%d\n", fd, err);
    if (err) {
        errno = -err;
        err = -1;
    }

    FPRINTF("fxstat64: slot=%d errno=%d\n", fd, errno);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__fxstat)(int __ver, int fd, struct stat *statbuf)
{
    fd_core_t *cfd;
    int err;

    FPRINTF("fxstat: slot=%d\n", fd);
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(__fxstat_stdio(__ver, fd, statbuf));

    err = lio_fs_stat(fs, NULL, lio_fd_path(cfd->lfd), statbuf, NULL, 0);
    if (err) {
        errno = -err;
        err = -1;
    }
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__fxstatat)(int __ver, int dirfd, const char *pathname, struct stat *statbuf, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("fxstatat: fname=%s dirfd=%d flags=%d AT_EMPTY_PATH=%d AT_SYMLINK_NOFOLLOW=%d\n", pathname, dirfd, flags, AT_EMPTY_PATH, AT_SYMLINK_NOFOLLOW);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        FPRINTF("fxstatat: fname=%s normal file\n", pathname);
        return(__fxstatat_stdio(__ver, dirfd, fullpath, statbuf, flags));
    }

    err = lio_fs_stat(fs, NULL, fname + len, statbuf, NULL, 0);
    if (err) {
        errno = -err;
        err = -1;
    } else {
        errno = 0;
    }

    FPRINTF("fxstatat: fname=%s err=%d errno=%d\n", pathname, err, errno);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(__fxstatat64)(int __ver, int dirfd, const char *pathname, struct stat64 *statbuf, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, err;

    FPRINTF("fxstatat: fname=%s dirfd=%d flags=%d AT_EMPTY_PATH=%d AT_SYMLINK_NOFOLLOW=%d\n", pathname, dirfd, flags, AT_EMPTY_PATH, AT_SYMLINK_NOFOLLOW);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(__fxstatat64_stdio(__ver, dirfd, fullpath, statbuf, flags));
    }

    err = lio_fs_stat(fs, NULL, fname + len, (struct stat *)statbuf, NULL, 0);
    if (err) {
        errno = -err;
        err = -1;
    } else {
        errno = 0;
    }

    FPRINTF("fxstatat: fname=%s err=%d errno=%d\n", pathname, err, errno);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(statx)(int dirfd, const char *pathname, int flags, unsigned int mask, struct statx *statxbuf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    struct stat statbuf;
    int index, len, err;
    int stat_symlink;

    FPRINTF("statx: fname=%s dirfd=%d flags=%d AT_EMPTY_PATH=%d AT_SYMLINK_NOFOLLOW=%d\n", pathname, dirfd, flags, AT_EMPTY_PATH, AT_SYMLINK_NOFOLLOW);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(statx_stdio(dirfd, fullpath, flags, mask, statxbuf));
    }

    //** It's an LStore file so do a stat
    stat_symlink = (flags & AT_SYMLINK_NOFOLLOW) ? 1 : 0;
    err = lio_fs_stat(fs, NULL, fname + len, &statbuf, NULL, stat_symlink);
    if (err == 0) {  //** Convert it to a statx struct
        memset(statxbuf, 0, sizeof(struct statx));
        statxbuf->stx_ino = statbuf.st_ino;
        statxbuf->stx_mode = statbuf.st_mode;
        statxbuf->stx_nlink = statbuf.st_nlink;
        statxbuf->stx_uid = statbuf.st_uid;
        statxbuf->stx_gid = statbuf.st_gid;
        statxbuf->stx_size = statbuf.st_size;
        statxbuf->stx_blksize = statbuf.st_blksize;
        statxbuf->stx_blocks = statbuf.st_blocks;
        statxbuf->stx_atime.tv_sec = statbuf.st_atim.tv_sec; statxbuf->stx_atime.tv_nsec = statbuf.st_atim.tv_nsec;
        statxbuf->stx_mtime.tv_sec = statbuf.st_mtim.tv_sec; statxbuf->stx_mtime.tv_nsec = statbuf.st_mtim.tv_nsec;
        statxbuf->stx_ctime.tv_sec = statbuf.st_ctim.tv_sec; statxbuf->stx_ctime.tv_nsec = statbuf.st_ctim.tv_nsec;
    } else {
        errno = -err;
        err = -1;
    }

    FPRINTF("statx: fname=%s errno=%d\n", pathname, errno);
    return(err);
}

//***********************************************************************

FD_TEMPLATE(fstatvfs, int, (int fd, struct statvfs *buf), fstatvfs_stdio(fd, buf), lio_fs_statvfs(fs, NULL, lio_fd_path(cfd->lfd), buf))
FD_TEMPLATE(fstatvfs64, int, (int fd, struct statvfs64 *buf), fstatvfs64_stdio(fd, buf), lio_fs_statvfs(fs, NULL, lio_fd_path(cfd->lfd), (struct statvfs *)buf))

//***********************************************************************

int WRAPPER_PREFIX(statvfs)(const char *pathname, struct statvfs *buf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("statvfs: fname=%s\n", pathname);

    if (access_stdio == NULL) {
        assert_result_not_null(statvfs_stdio = dlsym(RTLD_NEXT, "statvfs"));
    }

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(statvfs_stdio(fullpath, buf));
    }

    return(lio_fs_statvfs(fs, NULL, fname + len, buf));
}

//***********************************************************************

int WRAPPER_PREFIX(statvfs64)(const char *pathname, struct statvfs64 *buf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("statvfs64: fname=%s\n", pathname);

    if (access_stdio == NULL) {
        assert_result_not_null(statvfs64_stdio = dlsym(RTLD_NEXT, "statvfs64"));
    }

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(statvfs64_stdio(fullpath, buf));
    }

    return(lio_fs_statvfs(fs, NULL, fname + len, (struct statvfs *)buf));
}

//***********************************************************************

FD_TEMPLATE(fstatfs, int, (int fd, struct statfs *buf), fstatfs_stdio(fd, buf), lio_fs_statfs(fs, NULL, lio_fd_path(cfd->lfd), buf))
FD_TEMPLATE(fstatfs64, int, (int fd, struct statfs64 *buf), fstatfs64_stdio(fd, buf), lio_fs_statfs(fs, NULL, lio_fd_path(cfd->lfd), (struct statfs *)buf))

//***********************************************************************

int WRAPPER_PREFIX(statfs)(const char *pathname, struct statfs *buf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("statfs: fname=%s\n", pathname);

    if (statfs_stdio == NULL) {
        assert_result_not_null(statfs_stdio = dlsym(RTLD_NEXT, "statfs"));
    }

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(statfs_stdio(fullpath, buf));
    }

    return(lio_fs_statfs(fs, NULL, fname + len, buf));
}

//***********************************************************************

int WRAPPER_PREFIX(statfs64)(const char *pathname, struct statfs64 *buf)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("statfs64: fname=%s\n", pathname);

    if (statfs64_stdio == NULL) {
        assert_result_not_null(statfs64_stdio = dlsym(RTLD_NEXT, "statfs64"));
    }

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(statfs64_stdio(fullpath, buf));
    }

    return(lio_fs_statfs(fs, NULL, fname + len, (struct statfs *)buf));
}

//***********************************************************************

int WRAPPER_PREFIX(access)(const char *pathname, int mode)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("access: fname=%s\n", pathname);

    if (access_stdio == NULL) {
        assert_result_not_null(access_stdio = dlsym(RTLD_NEXT, "access"));
    }

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(access_stdio(fullpath, mode));
    }

    return(lio_fs_access(fs, NULL, fname + len, mode));
}

//***********************************************************************

int WRAPPER_PREFIX(faccessat)(int dirfd, const char *pathname, int mode, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("faccessat: fname=%s mode=%d flags=%d F_OK=%d R_OK=%d W_OK=%d X_OK=%d\n", pathname, mode, flags, F_OK, R_OK, W_OK, X_OK);

    //** See if we handle it or pass it thru
    fname = path_map(dirfd, pathname, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(faccessat_stdio(dirfd, fullpath, mode, flags));
    }

    return(lio_fs_access(fs, NULL, fname + len, mode));
}

//***********************************************************************
// These are the opendir/readdir/closedir wrappers
//***********************************************************************

//***********************************************************************

DIR *WRAPPER_PREFIX(fdopendir)(int fd)
{
    fd_core_t *cfd;
    DIR *sdir;

    FPRINTF("fdopendir fd=%d\n", fd);
    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) {
        sdir = fdopendir_stdio(fd);
        fd_table[fd].cfd = corefd_new(FD_MODE_STD, 0, fd, NULL, NULL, sdir, NULL);
        return((DIR *)&(fd_table[fd].dfd._fileno));
    }

    //** If we are here then it's an LStore file
    cfd->ldir = lio_fs_opendir(fs, NULL, cfd->lname_dir);
    FPRINTF("fdopendir  pathname=%s ldir=%p\n", cfd->lname_dir, cfd->ldir);
    if (cfd->ldir == NULL) {
        errno = EACCES;
        return(NULL);
    }

    FPRINTF("fdopendir fd=%d is_lio\n", fd);
    return((DIR *)&(fd_table[fd].dfd._fileno));
}

//***********************************************************************

DIR *WRAPPER_PREFIX(opendir)(const char *name)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len, slot;
    DIR *sdir;
    lio_fs_dir_iter_t *ldir;

    FPRINTF("opendir  pathname=%s\n", name);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, name, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        sdir = opendir_stdio(fullpath);
        if (sdir == NULL) return(NULL);
        index = dirfd_stdio(sdir);
        FPRINTF("opendir pathname=%s slot=%d STD\n", name, index);
        fd_table[index].cfd = corefd_new(FD_MODE_STD, 0, index, NULL, NULL, sdir, NULL);
        return((DIR *)&(fd_table[index].dfd._fileno));
    }

    //** If we are here then it's an LStore file
    ldir = lio_fs_opendir(fs, NULL, fname + len);
    FPRINTF("opendir  pathname=%s ldir=%p\n", fname, ldir);
    if (ldir == NULL) {
        errno = EACCES;
        return(NULL);
    }

    slot = get_free_slot(-1, corefd_new(FD_MODE_LIO, 0, -1, NULL, NULL, NULL, ldir), 0);
    FPRINTF("opendir lio slot=%d _fileno=%d\n", slot, fd_table[slot].dfd._fileno);
    fd_table[slot].cfd->prefix_index = index;
    return((DIR *)&(fd_table[slot].dfd._fileno));
}

//***********************************************************************

int WRAPPER_PREFIX(closedir)(DIR *dirp)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;

    FPRINTF("closedir: slot=%d\n", fd);
    SANITY_CHECK_FD(fd, -1);

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(closedir_stdio(cfd->sdir));

    FPRINTF("closedir: slot=%d is_lio\n", fd);
    return(lio_fs_closedir(cfd->ldir));
}

//***********************************************************************

struct dirent64 *WRAPPER_PREFIX(readdir64)(DIR *dirp)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;
    struct dirent64 *dentry;
    char *dname;
    struct stat stat;
    int err;

    FPRINTF("readdir64: slot=%d START\n", fd); fflush(stderr);

    if ((fd<0) || (fd>=n_fd_max2)) {
        errno = EBADF;
        return(NULL);
    }

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(readdir64_stdio(cfd->sdir));

    dentry = &(cfd->ldentry64);
    err = lio_fs_readdir(cfd->ldir, &dname, &stat, NULL, 1);
    FPRINTF("readdir64: slot=%d err=%d\n", fd, err); fflush(stderr);
    if (err != 0) {
        if (err < 0) errno = err;
        return(NULL);
    }

    FPRINTF("readdir64: dname=%s ino=%lu\n", dname, stat.st_ino); fflush(stderr);

    //** Form the dentry
    memset(dentry, 0, sizeof(struct dirent64));
    strncpy(dentry->d_name, dname, sizeof(dentry->d_name)-1);
    dentry->d_ino = stat.st_ino;
    dentry->d_reclen = sizeof(struct dirent);
    if (stat.st_mode & S_IFREG) {
        dentry->d_type = DT_REG;
    } else if (stat.st_mode & S_IFDIR) {
        dentry->d_type = DT_DIR;
    } else if (stat.st_mode & S_IFLNK) {
        dentry->d_type = DT_LNK;
    }

    return(dentry);
}

//***********************************************************************

struct dirent *WRAPPER_PREFIX(readdir)(DIR *dirp)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;
    struct dirent *dentry;
    char *dname;
    struct stat stat;
    int err;

    FPRINTF("readdir: slot=%d START\n", fd); fflush(stderr);

    if ((fd<0) || (fd>=n_fd_max2)) {
        errno = EBADF;
        return(NULL);
    }

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(readdir_stdio(cfd->sdir));

    dentry = &(cfd->ldentry);
    err = lio_fs_readdir(cfd->ldir, &dname, &stat, NULL, 1);
    FPRINTF("readdir: slot=%d err=%d\n", fd, err); fflush(stderr);
    if (err != 0) {
        if (err < 0) errno = err;
        return(NULL);
    }

    FPRINTF("readdir: dname=%s ino=%lu\n", dname, stat.st_ino); fflush(stderr);

    //** Form the dentry
    memset(dentry, 0, sizeof(struct dirent));
    strncpy(dentry->d_name, dname, sizeof(dentry->d_name)-1);
    dentry->d_ino = stat.st_ino;
    dentry->d_reclen = sizeof(struct dirent);
    if (stat.st_mode & S_IFREG) {
        dentry->d_type = DT_REG;
    } else if (stat.st_mode & S_IFDIR) {
        dentry->d_type = DT_DIR;
    } else if (stat.st_mode & S_IFLNK) {
        dentry->d_type = DT_LNK;
    }

    return(dentry);
}

//***********************************************************************

int WRAPPER_PREFIX(readdir_r)(DIR *dirp, struct dirent *dentry, struct dirent **result)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;
    char *dname;
    struct stat stat;
    int err;

    FPRINTF("readdir_r: slot=%d START\n", fd); fflush(stderr);

    if ((fd<0) || (fd>=n_fd_max2)) {
        return(EBADF);
    }

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(readdir_r_stdio(cfd->sdir, dentry, result));

    err = lio_fs_readdir(cfd->ldir, &dname, &stat, NULL, 1);
    FPRINTF("readdir_r: slot=%d err=%d\n", fd, err); fflush(stderr);
    if (err != 0) {
        *result = NULL;
        return(0);
    }

    FPRINTF("readdir_r: dname=%s ino=%lu\n", dname, stat.st_ino); fflush(stderr);

    //** Form the dentry
    memset(dentry, 0, sizeof(struct dirent));
    strncpy(dentry->d_name, dname, sizeof(dentry->d_name)-1);
    dentry->d_ino = stat.st_ino;
    dentry->d_reclen = sizeof(struct dirent);
    if (stat.st_mode & S_IFREG) {
        dentry->d_type = DT_REG;
    } else if (stat.st_mode & S_IFDIR) {
        dentry->d_type = DT_DIR;
    } else if (stat.st_mode & S_IFLNK) {
        dentry->d_type = DT_LNK;
    }

    *result = dentry;
    return(0);

}


//***********************************************************************

int WRAPPER_PREFIX(dirfd)(DIR *dirp)
{
    int fd = *(int *)dirp;

    FPRINTF("dirfd slot=%d\nm", fd);
    return(fd);
}

//***********************************************************************

void WRAPPER_PREFIX(rewinddir)(DIR *dirp)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;

    FPRINTF("rewinddir: slot=%d\n", fd);
    if ((fd<0) || (fd>=n_fd_max2)) return;

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(rewinddir_stdio(cfd->sdir));

    fprintf(stderr, "UNSUPPORTED: rewinddir on an LIO object\n");
}

//***********************************************************************

void WRAPPER_PREFIX(seekdir)(DIR *dirp, long loc)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;

    FPRINTF("seekdir: slot=%d off=%ld\n", fd, loc);
    if ((fd<0) || (fd>=n_fd_max2)) return;

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(seekdir_stdio(cfd->sdir, loc));

    fprintf(stderr, "UNSUPPORTED: seekdir on an LIO object\n");
}

//***********************************************************************

long WRAPPER_PREFIX(telldir)(DIR *dirp)
{
    int fd = *(int *)dirp;
    fd_core_t *cfd;

    FPRINTF("telldir: slot=%d\n", fd);
    if ((fd<0) || (fd>=n_fd_max2)) {
        errno = EBADF;
        return(-1);
    }

    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(telldir_stdio(cfd->sdir));

    fprintf(stderr, "UNSUPPORTED: seekdir on an LIO object\n");
    errno = EINVAL;
    return(-1);
}

//***********************************************************************
// getxattr/setxattr/listxattr routines
//***********************************************************************

ssize_t WRAPPER_PREFIX(getxattr)(const char *path, const char *name, void *value, size_t size)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;


    FPRINTF("getxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(getxattr_stdio(fullpath, name, value, size));
    }

    return(lio_fs_getxattr(fs, NULL, fname + len, name, value, size));
}

//***********************************************************************

ssize_t WRAPPER_PREFIX(lgetxattr)(const char *path, const char *name, void *value, size_t size)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;


    FPRINTF("lgetxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(lgetxattr_stdio(fullpath, name, value, size));
    }

    return(lio_fs_getxattr(fs, NULL, fname + len, name, value, size));
}

//***********************************************************************

ssize_t WRAPPER_PREFIX(fgetxattr)(int fd, const char *name, void *value, size_t size)
{
    fd_core_t *cfd;

    FPRINTF("fgetxattr: slot=%d\n", fd);

    SANITY_CHECK_FD(fd, -1);
    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fgetxattr_stdio(fd, name, value, size));

    return(lio_fs_getxattr(fs, NULL, lio_fd_path(cfd->lfd), name, value, size));
}

//***********************************************************************

int WRAPPER_PREFIX(setxattr)(const char *path, const char *name, const void *value, size_t size, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("setxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(setxattr_stdio(fullpath, name, value, size, flags));
    }

    return(lio_fs_setxattr(fs, NULL, fname + len, name, value, size, flags));
}

//***********************************************************************

int WRAPPER_PREFIX(lsetxattr)(const char *path, const char *name, const void *value, size_t size, int flags)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("lsetxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(lsetxattr_stdio(fullpath, name, value, size, flags));
    }

    return(lio_fs_setxattr(fs, NULL, fname + len, name, value, size, flags));
}


//***********************************************************************

int WRAPPER_PREFIX(fsetxattr)(int fd, const char *name, const void *value, size_t size, int flags)
{
    fd_core_t *cfd;

    FPRINTF("fgetxattr: slot=%d\n", fd);

    SANITY_CHECK_FD(fd, -1);
    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) return(fsetxattr_stdio(fd, name, value, size, flags));

    return(lio_fs_setxattr(fs, NULL, lio_fd_path(cfd->lfd), name, value, size, flags));
}

//***********************************************************************

int WRAPPER_PREFIX(removexattr)(const char *path, const char *name)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;


    FPRINTF("removexattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(removexattr_stdio(fullpath, name));
    }

    return(lio_fs_removexattr(fs, NULL, fname + len, name));
}

//***********************************************************************

int WRAPPER_PREFIX(lremovexattr)(const char *path, const char *name)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;


    FPRINTF("removexattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(lremovexattr_stdio(fullpath, name));
    }

    return(lio_fs_removexattr(fs, NULL, fname + len, name));
}

//***********************************************************************

FD_TEMPLATE(fremovexattr, int, (int fd, const char *name), fremovexattr_stdio(fd, name), lio_fs_removexattr(fs, NULL, lio_fd_path(cfd->lfd), name))

//***********************************************************************

ssize_t WRAPPER_PREFIX(listxattr)(const char *path, char *list, size_t size)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("listxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(listxattr_stdio(fullpath, list, size));
    }

    return(lio_fs_listxattr(fs, NULL, fname + len, list, size));
}

//***********************************************************************

ssize_t WRAPPER_PREFIX(llistxattr)(const char *path, char *list, size_t size)
{
    char buffer[PATH_MAX];
    char fbuffer[PATH_MAX];
    const char *fname;
    char *fullpath;
    int index, len;

    FPRINTF("listxattr:  pathname=%s\n", path); fflush(stderr);

    //** See if we handle it or pass it thru
    fname = path_map(AT_FDCWD, path, &index, &len, buffer, fbuffer, &fullpath);
    if (fname == NULL) {
        return(llistxattr_stdio(fullpath, list, size));
    }

    return(lio_fs_listxattr(fs, NULL, fname + len, list, size));
}

//***********************************************************************

FD_TEMPLATE(flistxattr, ssize_t, (int fd, char *list, size_t size), flistxattr_stdio(fd, list, size), lio_fs_listxattr(fs, NULL, lio_fd_path(cfd->lfd), list, size))

//***********************************************************************
// fcntl wrappers
//***********************************************************************

int my_vfcntl64_stdio(int fd, int cmd, va_list ap)
{
    switch(cmd) {
        case F_DUPFD:  goto int_arg;
        case F_DUPFD_CLOEXEC:  goto int_arg;
        case F_GETFD:  goto void_arg;
        case F_SETFD:  goto int_arg;
        case F_GETFL:  goto void_arg;
        case F_SETFL:  goto int_arg;
        case F_SETLK:  goto flock_arg;
        case F_SETLKW: goto flock_arg;
        case F_GETLK:  goto flock_arg;
        case F_OFD_SETLK:  goto flock_arg;
        case F_OFD_SETLKW: goto flock_arg;
        case F_OFD_GETLK:  goto flock_arg;
        case F_GETOWN: goto void_arg;
        case F_SETOWN: goto int_arg;
        case F_GETOWN_EX: goto f_owner_ex_arg;
        case F_SETOWN_EX: goto f_owner_ex_arg;
        case F_GETSIG: goto void_arg;
        case F_SETSIG: goto int_arg;
        case F_SETLEASE: goto int_arg;
        case F_GETLEASE: goto void_arg;
        case F_NOTIFY: goto int_arg;
        case F_SETPIPE_SZ: goto int_arg;
        case F_GETPIPE_SZ: goto void_arg;
        case F_ADD_SEALS: goto int_arg;
        case F_GET_SEALS: goto void_arg;
        case F_GET_RW_HINT: goto uint64_ptr_arg;
        case F_SET_RW_HINT: goto uint64_ptr_arg;
        case F_GET_FILE_RW_HINT: goto uint64_ptr_arg;
        case F_SET_FILE_RW_HINT: goto uint64_ptr_arg;
    }

    return(-1);  //** We should never get here

void_arg:
    return(fcntl64_stdio(fd, cmd));

int_arg:
    return(fcntl64_stdio(fd, cmd, va_arg(ap, int)));

flock_arg:
    return(fcntl64_stdio(fd, cmd, va_arg(ap, struct flock *)));

f_owner_ex_arg:
    return(fcntl64_stdio(fd, cmd, va_arg(ap, struct f_owner_ex *)));

uint64_ptr_arg:
    return(fcntl64_stdio(fd, cmd, va_arg(ap, uint64_t *)));
}

//***********************************************************************

int my_vfcntl64_lio(int fd, int cmd, va_list ap)
{
    fd_core_t *cfd;
    int n;

    FPRINTF("my_vfcntl64_lio: fd=%d cmd=%d F_DUPFD=%d F_DUPFD_CLOEXEC=%d F_GETFD=%d F_SETFD=%d F_GETFL=%d F_SETFL=%d\n", fd, cmd, F_DUPFD, F_DUPFD_CLOEXEC, F_GETFD, F_SETFD, F_GETFL, F_SETFL);
    switch(cmd) {
        case F_DUPFD:  return(dup(fd));
        case F_DUPFD_CLOEXEC:
            n = dup(fd);
            FPRINTF("my_vfcntl64_lio: fd=%d dup=%d\n", fd, n);
            return(n);
        case F_GETFD:  return(fd_table[fd].cfd->flags & O_CLOEXEC);
        case F_SETFD:
            cfd = fd_table[fd].cfd;
            n = va_arg(ap, int);
            if (n > 0) { //** Set O_CLOEXEC
                cfd->flags |= O_CLOEXEC;
            } else if (cfd->flags & O_CLOEXEC) { //** Clear it
                cfd->flags ^= O_CLOEXEC;
            }
            return(0);
        case F_GETFL:  return(fd_table[fd].cfd->flags);
        case F_SETFL:
            cfd = fd_table[fd].cfd;
            cfd->flags &= ~(O_APPEND | O_ASYNC | O_DIRECT | O_NOATIME | O_NONBLOCK);
            cfd->flags ^= (va_arg(ap, int) & (O_APPEND | O_ASYNC | O_DIRECT | O_NOATIME | O_NONBLOCK));
            return(0);
        default:
            FPRINTF("my_vfcntl64_lio: fd=%d ERROR unsupported command. cmd=%d\n", fd, cmd);
            return(-1);
    }

    return(-1);  //** We should never get here
}

//***********************************************************************

int WRAPPER_PREFIX(fcntl64)(int fd, int cmd, ...)
{
    int n;
    fd_core_t *cfd;
    va_list ap;

    FPRINTF("fcntl64: slot=%d cmd=%d\n", fd, cmd);

    SANITY_CHECK_FD(fd, -1);
    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) {
        va_start(ap, cmd);
        n = my_vfcntl64_stdio(fd, cmd, ap);
        va_end(ap);
        return(n);

    }

    //** This is an LStore file and we only handle a small subset of functionality
    va_start(ap, cmd);
    n = my_vfcntl64_lio(fd, cmd, ap);
    va_end(ap);
    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(fcntl)(int fd, int cmd, ...)
{
    int n;
    fd_core_t *cfd;
    va_list ap;

    FPRINTF("fcntl: CALLING fcntl64 handlers! slot=%d cmd=%d\n", fd, cmd);

    SANITY_CHECK_FD(fd, -1);
    cfd = fd_table[fd].cfd;
    if (CFD_IS_STD(fd, cfd)) {
        va_start(ap, cmd);
        n = my_vfcntl64_stdio(fd, cmd, ap);
        va_end(ap);
        return(n);

    }

    //** This is an LStore file and we only handle a small subset of functionality
    va_start(ap, cmd);
    n = my_vfcntl64_lio(fd, cmd, ap);
    va_end(ap);
    return(n);
}

//***********************************************************************
//  fdmax_suggested - Suggested n_fdmax* values
//***********************************************************************

int fdmax_suggested(struct rlimit64 *rl)
{
    struct rlimit64 rlim;
    struct rlimit64 *myrl;
    int n;

    myrl = (rl) ? rl : &rlim;

    if (getrlimit64_stdio == NULL) { assert_result_not_null(getrlimit64_stdio = dlsym(RTLD_NEXT, "getrlimit64")); }

    getrlimit64_stdio(RLIMIT_NOFILE, myrl);

    n = myrl->rlim_max/2;
    if (n > FDMAX_HARD_LIMIT) n = FDMAX_HARD_LIMIT;
    return(n);
}

//***********************************************************************

int WRAPPER_PREFIX(getrlimit64)(__rlimit_resource_t resource, struct rlimit64 *rlim)
{
    int err, n;
    struct rlimit64 rl;

    if (getrlimit64_stdio == NULL) { assert_result_not_null(getrlimit64_stdio = dlsym(RTLD_NEXT, "getrlimit64")); }

    if (resource == RLIMIT_NOFILE) {
        n = fdmax_suggested(&rl);
        FPRINTF("getrlimit: override resource=%d curr=%lu max=%lu returning=%d\n", resource, rlim->rlim_cur, rlim->rlim_max, n);
        rl.rlim_max = n;
        *rlim = rl;
        return(0);
    }

    err = getrlimit64_stdio(resource, rlim);
    FPRINTF("getrlimit: err=%d resource=%d curr=%lu max=%lu\n", err, resource, rlim->rlim_cur, rlim->rlim_max);
    return(err);
}

//***********************************************************************

int WRAPPER_PREFIX(setrlimit64)(__rlimit_resource_t  resource, const struct rlimit64 *rlim)
{
    int err;

    if (setrlimit64_stdio == NULL) { assert_result_not_null(setrlimit64_stdio = dlsym(RTLD_NEXT, "setrlimit64")); }

    err = setrlimit64_stdio(resource, rlim);
    FPRINTF("setrlimit: err=%d resource=%d curr=%lu max=%lu\n", err, resource, rlim->rlim_cur, rlim->rlim_max);
    return(err);
}

//***********************************************************************
//  These are the .SO constructur/destructor functions
//***********************************************************************

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(lio_stdio_wrapper_construct_fn)
#endif
ACCRE_DEFINE_CONSTRUCTOR(lio_stdio_wrapper_construct_fn)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(lio_stdio_wrapper_construct_fn)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(lio_stdio_wrapper_destruct_fn)
#endif
ACCRE_DEFINE_DESTRUCTOR(lio_stdio_wrapper_destruct_fn)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(lio_std_wrapper_destruct_fn)
#endif

static void lio_stdio_wrapper_construct_fn()
{
    int argc, i;
    char **argv;
    char *link;
    char *pwd;

    FPRINTF("STARTING\n");

    //** Get the max number of open files
    n_fd_max = fdmax_suggested(NULL);
    n_fd_max2 = 2*n_fd_max;

    tbx_type_malloc_clear(fd_table, fd_stdio_t, n_fd_max2);
    for (i=0; i<n_fd_max2; i++) {
        fd_table[i].dfd._fileno = i;
    }

    //** Fill all the stdio symbols for passthru
    get_stdio_fn();

    //** Get the arguments from the environment either LIO_OPTIONS_STDIO_WRAPPER or LIO_OPTIONS
    argc = 1;
    tbx_type_malloc(argv, char *, 1);
    argv[0] = "lio_stdio_wrapper";
    lio_init(&argc, &argv);

    //**Make the lock
    assert_result(apr_pool_create(&mpool, NULL), APR_SUCCESS);
    apr_thread_mutex_create(&lock, APR_THREAD_MUTEX_DEFAULT, mpool);

    //**  The remaining args should be the prefixes we are mapping
    if (argc <= 1) { //** No explicit prefix supplied so use the default of /lio/lfs
        n_prefix = 1;
        tbx_type_malloc(prefix_table, prefix_t, 1);
        prefix_table[0].prefix = "/lio/lfs";
        prefix_table[0].len = strlen(prefix_table[0].prefix);
    } else {
        n_prefix = argc-1;
        tbx_type_malloc_clear(prefix_table, prefix_t, n_prefix);
        for (i=1; i<argc; i++) {
            link = strchr(argv[i], ':');
            if (link) {
                link[0] = '\0';
                prefix_table[i-1].link = link + 1;
                prefix_table[i-1].link_len = strlen(prefix_table[i-1].link);
            }
            prefix_table[i-1].prefix = argv[i];
            prefix_table[i-1].len = strlen(prefix_table[i-1].prefix);
        }
    }

    np_regex = tbx_normalize_check_make();  //** Make the regex for seeing if we need to simplify the path

    //** Create teh file system handler
    fs = lio_fs_create(lio_gc->ifd, "lfs", lio_gc, getuid(), getgid());

    //** Now see what our current directory is
    //** We prefer to get it from the environment for working with scripts
    pwd = getenv("PWD");
    if (pwd) {
        cwd_len = strlen(pwd) + 1;
        memcpy(cwd, pwd, cwd_len);
    } else {
        getcwd_stdio(cwd, sizeof(cwd));
        cwd_len = strlen(cwd) + 1;
    }
    FPRINTF("INIT: starting cwd=%s PWD=%s\n", cwd, getenv("PWD"));
}

//***********************************************************************

static void lio_stdio_wrapper_destruct_fn() {
    int i;
    fd_core_t *cfd;

    //** Close anything still open
    for (i=0; i<n_fd_max; i++) {
        cfd = fd_table[i].cfd;
        if (cfd) {
            if (cfd->dup_count) cfd->dup_count--;
            if (cfd->mode == FD_MODE_FILENO) {
                tbx_io_close(cfd->fileno);
            } else if ((cfd->mode == FD_MODE_STD) && (cfd->sfd)) {
                tbx_io_fclose(cfd->sfd);
            } else if ((cfd->mode == FD_MODE_LIO) && (cfd->lfd)){
                lio_fs_close(fs, cfd->lfd);
            }
        }
    }

    apr_thread_mutex_destroy(lock);
    apr_pool_destroy(mpool);

    tbx_normalize_check_destroy(np_regex);

    lio_fs_destroy(fs);

    //** And terminate
    lio_shutdown();
}


