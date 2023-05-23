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

#define _log_module_index 212

#include "config.h"

#include <sys/types.h>
#include <sys/acl.h>

#include <apr_hash.h>
#include <apr_network_io.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <grp.h>
#include <lio/segment.h>
#include <lio/core.h>
#include <lio/fs.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <dirent.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include "blacklist.h"
#include "osaz/fake.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/types.h"
#include "lio.h"
#include "lio_fuse.h"
#include "os.h"
#include "rs.h"

#if defined(HAVE_SYS_XATTR_H)
#include <sys/xattr.h>
#elif defined(HAVE_ATTR_XATTR_H)
#include <attr/xattr.h>
#endif

#ifdef HAS_FUSE3
    #define FILLER(fn, buf, dentry, stat, off)  fn(buf, dentry, stat, off, FUSE_FILL_DIR_PLUS)
    #define LFS_INIT() void *lfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg)
    #define LFS_READDIR() int lfs_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
#else
    #define FILLER(fn, buf, dentry, stat, off)  fn(buf, dentry, stat, off)
    #define LFS_INIT() void *lfs_init(struct fuse_conn_info *conn)
    #define LFS_READDIR() int lfs_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi)
#endif

//** These sre for using a shadow file system for sanity checking LFS results
//#define LFS_SHADOW
#ifdef LFS_SHADOW
    typedef struct {
        lio_fd_t *fd;
        int sfd;
    } shadow_fd_t;

    typedef struct {
        char *dentry;
        struct stat sbuf;
        int matched;
    } shadow_dentry_t;

    typedef struct {
        DIR *dir;
        tbx_stack_t *stack;
        char *fname;
    } shadow_dir_t;

    #define SHADOW_MIRROR_PREFIX "/accre/new_lserver/alan/minio"
    #define SHADOW_MIRROR_PREFIX_LEN strlen(SHADOW_MIRROR_PREFIX)
    #define SHADOW_MOUNT_PREFIX "/tmp/shadow"
    #define SHADOW_MANGLE_FNAME(sfname, fname, is_shadow) \
        char sfname[OS_PATH_MAX]; \
        int is_shadow = 0; \
        if (strncmp(SHADOW_MIRROR_PREFIX, fname, SHADOW_MIRROR_PREFIX_LEN) == 0) { \
            is_shadow = 1; \
            snprintf(sfname, OS_PATH_MAX, SHADOW_MOUNT_PREFIX "%s", fname); \
        } \
        log_printf(0, "SHADOW: is_shadow=%d fname=%s sfname=%s\n", is_shadow, fname, sfname)
    #define SHADOW_GET_FD(handle) ((shadow_fd_t *)(handle))->fd
    #define SHADOW_CREATE_FD(handle) tbx_type_malloc_clear(handle, shadow_fd_t, 1)
    #define SHADOW_CODE(code) code
    #define SHADOW_ERROR(...) log_printf(0, "SHADOW_ERROR: " __VA_ARGS__)
    #define  SHADOW_GENERIC_COMPARE(fname, lerr, shadow_cmd) \
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow); \
        int serr = shadow_cmd;              \
        if (is_shadow) { \
            if ((lerr != 0) && (serr == -1)) {   \
                if (-lerr != errno) { SHADOW_ERROR("fname=%s lerr=%d serrno=%d\n", fname, lerr, errno); } \
            } else if ((lerr != 0) || (serr != 0)) { \
                SHADOW_ERROR("fname=%s MISMATCH lerr=%d serr=%d serrno=%d\n", fname, lerr, serr, errno); \
            } \
        }
    #define  SHADOW_GENERIC_COMPARE_DUAL(fname1, fname2, lerr, shadow_cmd) \
        SHADOW_MANGLE_FNAME(sfname1, fname1, is_shadow1); \
        SHADOW_MANGLE_FNAME(sfname2, fname2, is_shadow2); \
        int is_shadow12 = is_shadow1 + is_shadow2; \
        int serr; \
        if (is_shadow12 == 0) { \
            serr = shadow_cmd;              \
            if ((lerr != 0) && (serr == -1)) {   \
                if (-lerr != errno) { SHADOW_ERROR("fname1=%s fname2=%s lerr=%d serrno=%d\n", fname1, fname2, lerr, errno); } \
            } else if ((lerr != 0) || (serr != 0)) { \
                SHADOW_ERROR("fname1=%s fname2=%s MISMATCH lerr=%d serr=%d serrno=%d\n", fname1, fname2, lerr, serr, errno); \
            } \
        } else if (is_shadow12 == 1) { \
                SHADOW_ERROR("WARNING(is_shadow12=1 SKIPPING) fname1=%s fname2=%s\n", fname1, fname2); \
        }
#else
    #define SHADOW_GET_FD(handle) (lio_fd_t *)handle
    #define SHADOW_CREATE_FD(handle)
    #define SHADOW_ERROR(...)
    #define SHADOW_CODE(code)
    #define SHADOW_GENERIC_COMPARE(fname, lerr, shadow_cmd)
    #define SHADOW_GENERIC_COMPARE_DUAL(fname1, fname2, lerr, shadow_cmd)
#endif

typedef struct {
    char *dentry;
    struct stat stat;
} lfs_dir_entry_t;

typedef struct {
    char *fname;
    ex_id_t sid;
    int ref_count;
    int remove_on_close;
}  lio_fuse_open_file_t;

typedef struct {
    lio_fuse_t *lfs;
    lio_fs_dir_iter_t *fsit;
    tbx_stack_t *stack;
    int state;
    SHADOW_CODE(shadow_dir_t *sdir;)
} lfs_dir_iter_t;

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t vid);


//***********************************************************************
//  Shadow Helper routines
//***********************************************************************

SHADOW_CODE(
    //***********************************************************************

    void shadow_stat_compare(const char *fname, int serrno, int serr, struct stat *shadow_stat, int err, struct stat *sbuf)
    {
        //** Check if an error occurred and if so compare them
        if ((serr<0) && (err<0)) {
            if (-serrno != err) { SHADOW_ERROR("fname=%s lerr=%d serr=%d errno=%d\n", fname, err, serr, errno); }
            return;
        }

        //** No error so compare the structs
        if ((shadow_stat->st_mode & S_IFMT) != (sbuf->st_mode & S_IFMT)) { SHADOW_ERROR("fname=%s st_mode: shadow=%o lio=%o\n", fname, (shadow_stat->st_mode & S_IFMT), (sbuf->st_mode & S_IFMT)); }
        if (shadow_stat->st_mode & S_IFREG) { //** It's a normal file so compare the sizes
            if (shadow_stat->st_size != sbuf->st_size) { SHADOW_ERROR("fname=%s st_size: shadow=%lu lio=%lu\n", fname, shadow_stat->st_size, sbuf->st_size); }
        }
    }

    //***********************************************************************

    void shadow_closedir(lfs_dir_iter_t *ldir)
    {
        shadow_dir_t *sdir = ldir->sdir;
        shadow_dentry_t *dentry;

        if (!sdir) return;

        closedir(sdir->dir);
        tbx_stack_move_to_top(sdir->stack);
        while ((dentry = tbx_stack_pop(sdir->stack)) != NULL) {
            if (dentry->matched != 1) { SHADOW_ERROR("prefix_fname=%s dentry=%s MISSING from LIO\n", sdir->fname, dentry->dentry); }
            if (dentry->dentry) free(dentry->dentry);
            free(dentry);
        }
        tbx_stack_free(sdir->stack, 1);

        free(sdir->fname);
        free(sdir);
    }

    //***********************************************************************

    void shadow_opendir(const char *fname, lfs_dir_iter_t *ldir)
    {
        shadow_dir_t *sdir;
        DIR *d;
        struct dirent *de;
        shadow_dentry_t *dentry;
        char dename[OS_PATH_MAX];
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);

        if (is_shadow == 0) return;

        //** Try and open the directory and sanity check they both got the same initial result
        d = opendir(sfname);
        if (ldir->fsit == NULL) {
            if (d) {
                SHADOW_ERROR("fname=%s ldir=NULL and sdir!=NULL\n", fname);
                closedir(d);
            }
            return;
        } else if (d == NULL) {
            SHADOW_ERROR("fname=%s ldir!=NULL and sdir=NULL\n", fname);
            return;
        }

        //** If we made it here then both LIO and shadow can open the directory so we need to slurp in all the entries
        //** since the order returned is random for comparison via lfs_readdir() calls
        tbx_type_malloc_clear(sdir, shadow_dir_t, 1);
        ldir->sdir = sdir;
        sdir->dir = d;
        sdir->stack = tbx_stack_new();
        sdir->fname = strdup(fname);

        while ((de = readdir(d)) != NULL) {
            tbx_type_malloc_clear(dentry, shadow_dentry_t, 1);
            dentry->dentry = strdup(de->d_name);
            snprintf(dename, OS_PATH_MAX, "%s/%s", sfname, de->d_name);
            stat(dename, &(dentry->sbuf));
            tbx_stack_push(sdir->stack, dentry);
        }
    }

    //***********************************************************************

    void shadow_readdir(lfs_dir_iter_t *ldir, lfs_dir_entry_t *lde)
    {
        shadow_dir_t *sdir = ldir->sdir;
        shadow_dentry_t *sde;
        char dename[OS_PATH_MAX];

        if (!sdir) return;

        tbx_stack_move_to_top(sdir->stack);
        while ((sde = tbx_stack_get_current_data(sdir->stack)) != NULL) {
            if (strcmp(sde->dentry, lde->dentry) == 0) { //** Got a match
                sde->matched = 1;
                snprintf(dename, OS_PATH_MAX, "%s/%s(READDIR)", sdir->fname, sde->dentry);
                shadow_stat_compare(dename, 0, 0, &(sde->sbuf), 0, &(lde->stat));
                return;
            }
            tbx_stack_move_down(sdir->stack);
        }

        //** If we made it here the dentry was missing
        SHADOW_ERROR("MISSING dentry from shadow! fname=%s/%s\n", sdir->fname, lde->dentry);
    }

    //***********************************************************************

    void shadow_listxattr(const char *fname, char *list, size_t size, int err)
    {

        char slist[2*size];
        char *sattr;
        int nl;
        int pos;
        int len;
        int ssize;
        int i;
        char *lattr[2048];  //** A hack buf this is for testing only
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);

        if (is_shadow == 0) return;

        //** Do the shadow call
        ssize = listxattr(sfname, slist, 2*size);

        //** Check for errors
        if ((ssize == -1) && (err < 0)) {
            if (errno != -err) { SHADOW_ERROR("fname=%s MISMATCH errno lerr=%d ssize=%d serrno=%d\n", fname, err, ssize, errno); }
        } else if ((ssize == -1) || (err<0)) {
            SHADOW_ERROR("fname=%s MISMATCH lerr=%d ssize=%d serrno=%d\n", fname, err, ssize, errno);
        }

        //** Both have results so need to do a comparision
        //** Scan the LIO list and determine the offsets
        nl = 0;
        for (pos = 0; pos<(int)size; pos++) {
            lattr[nl] = list + pos;
            nl++;
            len = strlen(list + pos);
            pos += len + 1;
        }

        //** Do the same for the shadow but do a lio check.  Yes it's O(n) but it's a test.
        for (pos = 0; pos<ssize; pos++) {
            sattr = slist + pos;
            len = strlen(sattr);
            pos += len + 1;

            for (i=0; i<nl; i++) {
                if ((lattr[i]) && (strcmp(lattr[i], sattr) == 0)) {
                    lattr[i] = NULL;
                    goto match;
                }
            }
            SHADOW_ERROR("fname=%s MISSING sattr=%s\n", fname, sattr);
    match: continue;
        }

        //** Now see if any were missing from LIO
        for (i=0; i<nl; i++) {
            if (lattr[i]) { SHADOW_ERROR("fname=%s MISSING lattr=%s\n", fname, lattr[i]); }
        }
    }
)

//***********************************************************************

lio_os_authz_local_t *_get_fuse_ug(lio_fuse_t *lfs, lio_os_authz_local_t *ug, struct fuse_context *fc)
{

    if (lfs->fs_checks_acls == 0) {
        ug->valid_guids = 0;
    } else {
        lio_fs_fill_os_authz_local(lfs->fs, ug, fc->uid, fc->gid);
    }
    return(ug);
}

//*************************************************************************

void _lfs_hint_release(lio_fuse_t *lfs, lio_os_authz_local_t *ug)
{
    if (lfs->fs_checks_acls == 0) return;

    lio_fs_hint_release(lfs->fs, ug);
}

//*************************************************************************
// lfs_get_context - Returns the LFS context.  If none is available it aborts
//*************************************************************************

lio_fuse_t *lfs_get_context()
{
    lio_fuse_t *lfs;
    struct fuse_context *ctx;
    ctx = fuse_get_context();

   FATAL_UNLESS(NULL != ctx);

    lfs = (lio_fuse_t*)ctx->private_data;
   FATAL_UNLESS(NULL != lfs);

    return(lfs);
}

//*************************************************************************
// lfs_stat - Does a stat on the file/dir
//*************************************************************************

int lfs_stat(const char *fname, struct stat *sbuf, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;
    char *flink;

    flink = NULL;
    err = lio_fs_stat(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, sbuf, &flink, 1, lfs->no_cache_stat_if_file);
    _lfs_hint_release(lfs, &ug);

    if (err == 0) {
        if (flink) {
            if (flink[0] == '/') {
                sbuf->st_size += lfs->mount_point_len;
            } else if (strncmp(flink, OS_SL_OOB_MAGIC, OS_SL_OOB_MAGIC_LEN) == 0) {
                sbuf->st_size -= OS_SL_OOB_MAGIC_LEN;
            }
            free(flink);
        }
    }

    SHADOW_CODE(
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);
        if (is_shadow) {
            struct stat shadow_stat;
            int serr = stat(sfname, &shadow_stat);
            shadow_stat_compare(fname, errno, serr, &shadow_stat, err, sbuf);
        }
    )

    return(err);
}

//*************************************************************************

int lfs_stat2(const char *fname, struct stat *stat)
{
    return(lfs_stat(fname, stat, NULL));
}

//*************************************************************************
// lfs_closedir - Closes the opendir file handle
//*************************************************************************

int lfs_closedir(const char *fname, struct fuse_file_info *fi)
{
    lfs_dir_iter_t *dit = (lfs_dir_iter_t *)fi->fh;
    lfs_dir_entry_t *de;

    if (dit == NULL) return(-EBADF);

    SHADOW_CODE(shadow_closedir(dit);)

    if (dit->stack) {
        //** Cyle through releasing all the entries
        while ((de = (lfs_dir_entry_t *)tbx_stack_pop(dit->stack)) != NULL) {
            log_printf(15, "fname=%s\n", de->dentry);
            tbx_log_flush();
            free(de->dentry);
            free(de);
        }

        tbx_stack_free(dit->stack, 0);
    }

    if (dit->fsit) lio_fs_closedir(dit->fsit);
    free(dit);

    return(0);
}

//*************************************************************************
// lfs_opendir - FUSE opendir call
//*************************************************************************

int lfs_opendir(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lfs_dir_iter_t *dit;
    lio_os_authz_local_t ug;


    tbx_type_malloc_clear(dit, lfs_dir_iter_t, 1);
    dit->lfs = lfs;
    dit->fsit = lio_fs_opendir(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname);
    _lfs_hint_release(lfs, &ug);

    SHADOW_CODE(shadow_opendir(fname, dit);)

    if (dit->fsit == NULL) {
        free(dit);
        return(-EACCES);
    }

    dit->stack = tbx_stack_new();
    dit->state = 0;

    //** Compose our reply
    fi->fh = (uint64_t)dit;
    return(0);
}

//*************************************************************************
// lfs_readdir - Returns the next file in the directory
//*************************************************************************

LFS_READDIR()
{
    lfs_dir_iter_t *dit = (lfs_dir_iter_t *)fi->fh;
    lfs_dir_entry_t *de;
    int n, i, err;
    struct stat stbuf;
    apr_time_t now;
    double dt;
    int off2 = off;

    log_printf(1, "dname=%s off=%d stack_size=%d\n", dname, off2, tbx_stack_count(dit->stack));
    tbx_log_flush();
    now = apr_time_now();

    if (dit == NULL) {
        return(-EBADF);
    }

    off++;  //** This is the *next* slot to get where the stack top is off=1

    memset(&stbuf, 0, sizeof(stbuf));
    n = tbx_stack_count(dit->stack);
    tbx_stack_move_to_bottom(dit->stack);  //** Go from the bottom up.
    if (n>=off) { //** Rewind
        for (i=n; i>off; i--) tbx_stack_move_up(dit->stack);

        de = tbx_stack_get_current_data(dit->stack);
        while (de != NULL) {
            log_printf(2, "dname=%s off=" XOT "\n", de->dentry, off);
            if (FILLER(filler, buf, de->dentry, &(de->stat), off) == 1) {
                dt = apr_time_now() - now;
                dt /= APR_USEC_PER_SEC;
                log_printf(1, "dt=%lf\n", dt);
                return(0);
            }

            SHADOW_CODE(shadow_readdir(dit, de);)
            off++;
            tbx_stack_move_down(dit->stack);
            de = tbx_stack_get_current_data(dit->stack);
        }
    }

    log_printf(15, "dname=%s switching to iter\n", dname);

    for (;;) {
        //** If we made it here then grab the next file and look it up.
        tbx_type_malloc(de, lfs_dir_entry_t, 1);
        err = lio_fs_readdir(dit->fsit, &(de->dentry), &(de->stat), NULL, 1);
        if (err != 0) {   //** Nothing left to process
            free(de);
            return((err == 1) ? 0 : -EIO);
        }

        tbx_stack_move_to_bottom(dit->stack);
        tbx_stack_insert_below(dit->stack, de);

        if (FILLER(filler, buf, de->dentry, &(de->stat), off) == 1) {
            dt = apr_time_now() - now;
            dt /= APR_USEC_PER_SEC;
            log_printf(15, "BUFFER FULL dt=%lf\n", dt);
            return(0);
        }

        SHADOW_CODE(shadow_readdir(dit, de);)
        off++;
    }

    return(0);
}

//*************************************************************************
// lfs_mknod - Makes a regular file
//*************************************************************************

int lfs_mknod(const char *fname, mode_t mode, dev_t rdev)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_mknod(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, mode, rdev);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, mknod(sfname, mode, rdev));
    return(err);
}

//*************************************************************************
// lfs_chmod - Currently this only changes the exec bit for a FILE
//*************************************************************************

#ifdef HAS_FUSE3
int lfs_chmod(const char *fname, mode_t mode, struct fuse_file_info *fi)
#else
int lfs_chmod(const char *fname, mode_t mode)
#endif
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_chmod(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, mode);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, chmod(sfname, mode));

    return(err);
}

//*************************************************************************
// lfs_mkdir - Makes a directory
//*************************************************************************

int lfs_mkdir(const char *fname, mode_t mode)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_mkdir(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, mode);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, mkdir(sfname, mode));

    return(err);
}

//*****************************************************************
//  lfs_unlink - Remove a file
//*****************************************************************

int lfs_unlink(const char *fname)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_object_remove(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, OS_OBJECT_FILE_FLAG);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, unlink(sfname));

    return(err);
}

//*****************************************************************
//  lfs_rmdir - Remove a directory
//*****************************************************************

int lfs_rmdir(const char *fname)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_object_remove(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, OS_OBJECT_DIR_FLAG);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, rmdir(sfname));

    return(err);
}

//*****************************************************************
// lfs_flock - Performas a lock operation on the file
//*****************************************************************

int lfs_flock(const char *fname, struct fuse_file_info *fi, int lock_type)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);
    int err;

    err = lio_fs_flock(lfs->fs, fd, lock_type);
    return(err);
}

//*****************************************************************
// lfs_open - Opens a file for I/O
//*****************************************************************

int lfs_open(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd;
    lio_os_authz_local_t ug;

    fd = lio_fs_open(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, lio_open_flags(fi->flags, 0));
    _lfs_hint_release(lfs, &ug);
    fi->fh = (uint64_t)fd;

    SHADOW_CODE(
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);
        shadow_fd_t *sfd;
        int lerr = errno;
        tbx_type_malloc_clear(sfd, shadow_fd_t, 1);
        sfd->sfd = -1;

        if (is_shadow) {
            sfd->sfd = open(sfname, fi->flags);
            sfd->fd = fd;
            fi->fh = (uint64_t)sfd;

            if ((fd == NULL) && (sfd->sfd == -1)) { //** Both failed so check the errno
                if (lerr != errno) {
                    SHADOW_ERROR("fname=%s flags=%d lfd=%p sfd=%d lerr=%d serr=%d\n", fname, fi->flags, fd, sfd->sfd, lerr, errno);
                }
            } else if (!(fd && sfd->fd)) { //** 1 failed but not both
               SHADOW_ERROR("fname=%s flags=%d lfd=%p sfd=%d lerr=%d serr=%d\n", fname, fi->flags, fd, sfd->sfd, lerr, errno);
            }
            errno = lerr;
        }
    )

    if (!fd) return(-errno);  //On error lio_fs_open sets the error code in errno
    return(0);
}

//*****************************************************************
// lfs_release - Closes a file
//*****************************************************************

int lfs_release(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);
    int err;

    err = lio_fs_close(lfs->fs, fd);

    SHADOW_CODE(
        shadow_fd_t *sfd = (shadow_fd_t *)fi->fh;
        int serr = close(sfd->sfd);
        if (sfd->sfd) {
            if ((serr == -1) && (err != 0)) {
                SHADOW_ERROR("fname=%s lfd=%p sfd=%d lerr=%d serr=%d\n", fname, fd, sfd->sfd, err, errno);
            } else if (!((serr == 0) && (err == 0))) {
                SHADOW_ERROR("fnam=%s lfd=%p sfd=%d lerr=%d serr=%d\n", fname, fd, sfd->sfd, err, errno);
            }
        }
        free(sfd);
    )

    return(err);
}

//*****************************************************************
// lfs_read - Reads data from a file
//    NOTE: Uses the LFS readahead hints
//*****************************************************************

int lfs_read(const char *fname, char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);
    int err;

    SHADOW_CODE( //** We do the read 1st so the user gets the data from LStore
        int serrno;
        int serr;
        shadow_fd_t *sfd = (shadow_fd_t *)fi->fh;
        if (sfd->sfd >= 0) {
            serr = pread(sfd->sfd, buf, size, off);
            serrno = errno;
        }
    )
    err = lio_fs_pread(lfs->fs, fd, buf, size, off);

    SHADOW_CODE(
        if ((sfd->sfd >= 0) && (serr != err)) {
            SHADOW_ERROR("fname=%s lfd=%p sfd=%d off=" XOT " size=" ST " lerr=%d serr=%d serrno=%d\n", fname, fd, sfd->sfd, off, size, err, serr, serrno);
        }
    )

    return(err);
}

//*****************************************************************
// lfs_write - Writes data to a file
//*****************************************************************

int lfs_write(const char *fname, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);
    int err;

    err = lio_fs_pwrite(lfs->fs, fd, buf, size, off);

    SHADOW_CODE(
        shadow_fd_t *sfd = (shadow_fd_t *)fi->fh;
        if (sfd->sfd >= 0) {
            int serr = pwrite(sfd->sfd, buf, size, off);
            if (serr != err) {
                SHADOW_ERROR("fname=%s lfd=%p sfd=%d off=" XOT " size=" ST " lerr=%d serr=%d serrno=%d\n", fname, fd, sfd->sfd, off, size, err, serr, errno);
            }
        }
    )

    return(err);
}

//*****************************************************************
// lfs_flush - Flushes any data to backing store
//*****************************************************************

int lfs_flush(const char *fname, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);   //** There is no flush for a open() call just for fopen() calls

    return(lio_fs_flush(lfs->fs, fd));
}


//*****************************************************************
// lfs_copy_file_range - Copies data between files
//*****************************************************************

//** SHADOW_UNSUPPORTED
ssize_t lfs_copy_file_range(const char *path_in,  struct fuse_file_info *fi_in,  off_t offset_in,
                            const char *path_out, struct fuse_file_info *fi_out, off_t offset_out, size_t size, int flags)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd_in, *fd_out;
    int err;

    log_printf(1, "START copy_file_range src=%s dest=%s\n", path_in, path_out);

    SHADOW_ERROR("WARNING: UNSUPPORTED call! fin=%s fout=%s\n", path_in, path_out);

    fd_in = SHADOW_GET_FD(fi_in->fh);
    if (fd_in == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_in handle\n");
        return(-EBADF);
    }

    fd_out = SHADOW_GET_FD(fi_out->fh);
    if (fd_out == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_out handle\n");
        return(-EBADF);
    }

    err = lio_fs_copy_file_range(lfs->fs, fd_in, offset_in, fd_out, offset_out, size);

    return(err);
}

//*****************************************************************
// lfs_fsync - Flushes any data to backing store
//*****************************************************************

int lfs_fsync(const char *fname, int datasync, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = SHADOW_GET_FD(fi->fh);
    int err;

    err = lio_fs_flush(lfs->fs, fd);
    SHADOW_GENERIC_COMPARE(fname, err, fsync(((shadow_fd_t *)fi->fh)->sfd));

    return(err);
}

//*************************************************************************
// lfs_rename - Renames a file
//*************************************************************************

int lfs_rename(const char *oldname, const char *newname, unsigned int flags)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_rename(lfs->fs,  _get_fuse_ug(lfs, &ug, fuse_get_context()), oldname, newname);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE_DUAL(oldname, newname, err, rename(sfname1, sfname2));

    return(err);
}

//*****************************************************************

int lfs_rename2(const char *oldname, const char *newname)
{
    return(lfs_rename(oldname, newname, 0));
}

//*****************************************************************
// lfs_truncate - Truncate the file
//*****************************************************************

int lfs_truncate(const char *fname, off_t new_size)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_truncate(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, new_size);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, truncate(sfname, new_size));

    return(err);
}

//*****************************************************************
// lfs_ftruncate - Truncate the file associated with the FD
//*****************************************************************

int lfs_ftruncate(const char *fname, off_t new_size, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;

    err = lio_fs_ftruncate(lfs->fs, fd, new_size);

    SHADOW_GENERIC_COMPARE(fname, err, ftruncate(((shadow_fd_t *)fi->fh)->sfd, new_size));

    return(err);
}


//*****************************************************************
// lfs_utimens - Sets the access and mod times in ns
//*****************************************************************

//** SHADOW_UNSUPPORTED
int lfs_utimens(const char *fname, const struct timespec tv[2], struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    SHADOW_ERROR("WARNING: UNSUPPORTED call! fname=%s\n", fname);

    err = lio_fs_utimens(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, tv);
    _lfs_hint_release(lfs, &ug);

    return(err);
}

//*****************************************************************

int lfs_utimens2(const char *fname, const struct timespec tv[2])
{
    return(lfs_utimens(fname, tv, NULL));
}

//*****************************************************************
// lfs_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

int lfs_listxattr(const char *fname, char *list, size_t size)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_listxattr(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, list, size);
    _lfs_hint_release(lfs, &ug);

    SHADOW_CODE(shadow_listxattr(fname, list, size, err);)
    return(err);
}

//*****************************************************************
// lfs_getxattr - Gets an extended attribute
//*****************************************************************

#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int lfs_getxattr(const char *fname, const char *name, char *buf, size_t size)
#  else
int lfs_getxattr(const char *fname, const char *name, char *buf, size_t size, uint32_t dummy)
#  endif
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_getxattr(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, name, buf, size);
    _lfs_hint_release(lfs, &ug);

    SHADOW_CODE(
        char sbuf[size+1];
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);
        if (is_shadow) {
            int serr = getxattr(sfname, name, sbuf, size+1);
            if ((serr == -1) && (err < 0)) {
                if (errno != -err) { SHADOW_ERROR("fname=%s attr=%s lerr=%d serr=%d\n", fname, name, err, errno); }
            } else if ((serr == -1) || (err < 0)) {
                SHADOW_ERROR("fname=%s mismatch attr=%s lerr=%d serr=%d serrno=%d\n", fname, name, err, serr, errno);
            } else if (serr != err) {
                SHADOW_ERROR("fname=%s size mismatch attr=%s lsize=%d ssize=%d\n", fname, name, err, serr);
            } else if (serr > 0) {
                if (memcmp(buf, sbuf, serr) != 0) { SHADOW_ERROR("fname=%s value mismatch attr=%s lval=%s sval=%s\n", fname, name, buf, sbuf); }
            }
        }
    )

    return(err);
}
#endif //HAVE_XATTR

//*****************************************************************
// lfs_setxattr - Sets a extended attribute
//*****************************************************************

#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int lfs_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags)
#  else
int lfs_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags, uint32_t dummy)
#  endif
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_setxattr(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, name, fval, size, flags);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, setxattr(sfname, name, fval, size, flags));

    return(err);
}

//*****************************************************************
// lfs_removexattr - Removes an extended attribute
//*****************************************************************

int lfs_removexattr(const char *fname, const char *name)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_removexattr(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, name);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE(fname, err, removexattr(sfname, name));

    return(err);
}
#endif //HAVE_XATTR

//*************************************************************************
// lfs_hardlink - Creates a hardlink to an existing file
//*************************************************************************

int lfs_hardlink(const char *oldname, const char *newname)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    err = lio_fs_hardlink(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), oldname, newname);
    _lfs_hint_release(lfs, &ug);

    SHADOW_GENERIC_COMPARE_DUAL(oldname, newname, err, link(sfname1, sfname2));

    return(err);
}

//*****************************************************************
//  lfs_readlink - Reads the object symlink
//*****************************************************************

int lfs_readlink(const char *fname, char *buf, size_t bsize)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int n, err;
    char flink[OS_PATH_MAX];

    err = lio_fs_readlink(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, buf, bsize);
    _lfs_hint_release(lfs, &ug);

    SHADOW_CODE(
        SHADOW_MANGLE_FNAME(sfname, fname, is_shadow);
        char sbuf[OS_PATH_MAX];
        int serr;

        if (is_shadow) {
            serr = readlink(sfname, sbuf, OS_PATH_MAX);
            if ((serr == -1) && (err < 0)) {
                if (errno != -err) { SHADOW_ERROR("fname=%s lerr=%d serr=%d\n", fname, err, errno); }
            } else if ((serr == -1) || (err < 0)) {
                SHADOW_ERROR("fname=%s mismatch lerr=%d serr=%d\n", fname, err, errno);
            } else if (serr != err) {
                SHADOW_ERROR("fname=%s size mismatch lsize=%d ssize=%d\n", fname, err, serr);
            } else if (serr > 0) {
                if ((buf[0] == '/') && (sbuf[0] == '/')) { //** Absolute paths
                    if (strcmp(buf, sbuf + strlen(SHADOW_MOUNT_PREFIX)) != 0) { SHADOW_ERROR("fname=%s link mismatch llink=%s slink=%s\n", fname, buf, sbuf); }
                } else if (strcmp(buf, sbuf) != 0) {
                    SHADOW_ERROR("fname=%s link mismatch llink=%s slink=%s\n", fname, buf, sbuf);
                }
            }
        }
    )

    if (err < 0) return(err);

    if (buf[0] == '/') { //** Absolute path so need to prepend the mount path
        if (lfs->mount_point_len != 0) {
            flink[OS_PATH_MAX-1] = 0;
            n = snprintf(flink, OS_PATH_MAX, "%s%s", lfs->mount_point, buf);
            memcpy(buf, flink, n+1);
        }
    } else if (strncmp(buf, OS_SL_OOB_MAGIC, OS_SL_OOB_MAGIC_LEN) == 0) {
        strcpy(flink, buf + OS_SL_OOB_MAGIC_LEN);
        strcpy(buf, flink);
    }

    return(0);
}

//*****************************************************************
//  lfs_symlink - Makes a symbolic link
//*****************************************************************

int lfs_symlink(const char *link, const char *newname)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    const char *link2;
    char abslink[OS_PATH_MAX];
    int err;

    log_printf(1, "link=%s newname=%s\n", link, newname);
    tbx_log_flush();

    //** If the link is an absolute path we need to peel off the mount point to the get attribs to link correctly
    //** We only support symlinks within LFS
    link2 = link;
    if (link[0] == '/') { //** Got an abs symlink
        if (strncmp(link, lfs->mount_point, lfs->mount_point_len) == 0) { //** abs symlink w/in LFS
            link2 = &(link[lfs->mount_point_len]);
        } else {  //** It's out of bounds of LFS so need to munge it to let FS know
            snprintf(abslink, sizeof(abslink)-1, "%s%s", OS_SL_OOB_MAGIC, link); abslink[sizeof(abslink)-1] = '\0';
            link2 = abslink;
        }
    }

    err = lio_fs_symlink(lfs->fs, _get_fuse_ug(lfs, &ug, fuse_get_context()), link2, newname);
    _lfs_hint_release(lfs, &ug);

    SHADOW_CODE(
        SHADOW_MANGLE_FNAME(snewname, newname, is_shadow);
        char slink[OS_PATH_MAX];
        int serr;

        if (is_shadow) {
            if (link[0] == '/') { //** Got an abs symlink
                snprintf(slink, OS_PATH_MAX, SHADOW_MOUNT_PREFIX "/%s", link2);
                serr = symlink(slink, snewname);
            } else {
                serr = symlink(link2, snewname);
            }

            if (!((serr == 0) && (err == 0))) { SHADOW_ERROR("link=%s newname=%s lerr=%d serr=%d\n", link, newname, err, errno); }
        }
    )

    return(err);
}

//*************************************************************************
// lfs_statfs - Returns the files system size
//*************************************************************************

//** SHADOW_UNSUPPORTED
int lfs_statvfs(const char *fname, struct statvfs *sfs)
{
    lio_fuse_t *lfs = lfs_get_context();
    lio_os_authz_local_t ug;
    int err;

    SHADOW_ERROR("WARNING: UNSUPPORTED call! fname=%s\n", fname);

    err =  lio_fs_statvfs(lfs->fs,  _get_fuse_ug(lfs, &ug, fuse_get_context()), fname, sfs);
    _lfs_hint_release(lfs, &ug);
    return(err);
}

//*************************************************************************
// lio_fuse_info_fn - Signal handler to dump info
//*************************************************************************

void lio_fuse_info_fn(void *arg, FILE *fd)
{
    lio_fuse_t *lfs = arg;
    char ppbuf[100];

    fprintf(fd, "---------------------------------- LFS config start --------------------------------------------\n");
    fprintf(fd, "[%s]\n", lfs->lfs_section);
    fprintf(fd, "mount_point = %s\n", lfs->mount_point);
    fprintf(fd, "enable_osaz_acl_mappings = %d\n", lfs->enable_osaz_acl_mappings);
    fprintf(fd, "fs_checks_acls = %d  # Should only be 0 if running lio_fuse since the kernel handles ACL checking\n", lfs->fs_checks_acls);
    fprintf(fd, "no_cache_stat_if_file = %d\n", lfs->no_cache_stat_if_file);
    if (lfs->enable_flock) {
        fprintf(fd, "# flock() is ENABLED\n");
    } else {
        fprintf(fd, "# flock() is DISABLED\n");
    }
    fprintf(fd, "max_write = %s\n", tbx_stk_pretty_print_double_with_scale(1024, lfs->conn->max_write, ppbuf));
#ifdef HAS_FUSE3
    fprintf(fd, "max_read = %s\n", tbx_stk_pretty_print_double_with_scale(1024, lfs->conn->max_read, ppbuf));
#endif
    fprintf(fd, "max_readahead = %s\n", tbx_stk_pretty_print_double_with_scale(1024, lfs->conn->max_readahead, ppbuf));
    fprintf(fd, "max_background = %d\n", lfs->conn->max_background);
    fprintf(fd, "congestion_threshold = %d\n", lfs->conn->congestion_threshold);
    fprintf(fd, "\n");
}

//*************************************************************************
//  lio_fuse_init - Creates a lowlevel fuse handle for use
//     Note that this function should be called by FUSE and the return value of this function
//     overwrites the .private_data field of the fuse context. This function returns the
//     lio fuse handle (lio_fuse_t *lfs) on success and NULL on failure.
//
//     This function calls lio_init(...) itself, no need to call it beforehand.
//
//*************************************************************************

void *lfs_init_real(struct fuse_conn_info *conn,
                    int argc,
                    char **argv,
                    const char *mount_point)
{
    lio_fuse_t *lfs;
    char *section =  "lfs";
    ex_off_t n;
    lio_fuse_init_args_t *init_args;
    lio_fuse_init_args_t real_args;

    // Retrieve the fuse_context, the last argument of fuse_main(...) is passed in the private_data field for use as a generic user arg. We pass the mount point in it.
    struct fuse_context *ctx;
    if ((argc == 0) && (argv == NULL) && (mount_point == NULL)) {
        ctx = fuse_get_context();
        if (NULL == ctx || NULL == ctx->private_data) {
            log_printf(0, "ERROR_CTX:  unable to access fuse context or context is invalid. (Hint: last arg of fuse_main(...) must be lio_fuse_init_args_t* and have the mount point set)");
            return(NULL); //TODO: what is the best way to signal failure in the init function? Note that the return value of this function overwrites the .private_data field of the fuse context
        } else {
            init_args = (lio_fuse_init_args_t*)ctx->private_data;
        }
    } else {
        // We weren't called by fuse, so the args are function arguments
        // AMM - 9/23/13
        init_args = &real_args;
        init_args->lio_argc = argc;
        init_args->lio_argv = argv;
        init_args->mount_point = (char *)mount_point;
    }

    lio_init(&init_args->lio_argc, &init_args->lio_argv); //This sets the global lio_gc, it also uses a reference count to safely handle extra calls to init
    init_args->lc = lio_gc;

    log_printf(15, "START mount=%s\n", init_args->mount_point);

    //** See if we need to change the CWD
    if (init_args->lio_argc > 1) {
        if (strcmp(init_args->lio_argv[1], "-C") == 0) {
            if (chdir(init_args->lio_argv[2]) != 0) {
                fprintf(stderr, "ERROR setting CWD=%s.  errno=%d\n", init_args->lio_argv[2], errno);
                log_printf(0, "ERROR setting CWD=%s.  errno=%d\n", init_args->lio_argv[2], errno);
            } else {
                log_printf(0, "Setting CWD=%s\n", init_args->lio_argv[2]);
            }
        }
    }

    tbx_type_malloc_clear(lfs, lio_fuse_t, 1);

    lfs->lc = init_args->lc;
    lfs->conn = conn;
    lfs->lfs_section = strdup(section);
    lfs->mount_point = strdup(init_args->mount_point);
    lfs->mount_point_len = strlen(init_args->mount_point);

    //** Most of the heavylifting is done in the filesystem object
    lfs->fs = lio_fs_create(lfs->lc->ifd, section, lfs->lc, getuid(), getgid());
log_printf(0, "lfs->fs=%p\n", lfs->fs);
    lfs->fs_checks_acls = tbx_inip_get_integer(lfs->lc->ifd, section, "fs_checks_acls", 1);
    lfs->enable_osaz_acl_mappings = tbx_inip_get_integer(lfs->lc->ifd, section, "enable_osaz_acl_mappings", 0);
    lfs->no_cache_stat_if_file = tbx_inip_get_integer(lfs->lc->ifd, section, "no_cache_stat_if_file", 1);
    lfs->enable_flock = (lfs_fops.flock == NULL) ? 0 : 1;

#ifdef FUSE_CAP_POSIX_ACL
    if (lfs->enable_osaz_acl_mappings) {
        conn->capable |= FUSE_CAP_POSIX_ACL;  //** enable POSIX ACLs
        conn->want |= FUSE_CAP_POSIX_ACL;  //** enable POSIX ACLs
    }
#endif

    n = tbx_inip_get_integer(lfs->lc->ifd, section, "max_write", -1);
    if (n > -1) conn->max_write = n;
    n = tbx_inip_get_integer(lfs->lc->ifd, section, "congestion_threshold", -1);
    if (n > -1) conn->congestion_threshold = n;
    n = tbx_inip_get_integer(lfs->lc->ifd, section, "max_background", -1);
    if (n > -1) conn->max_background = n;

#ifdef HAS_FUSE3
    n = tbx_inip_get_integer(lfs->lc->ifd, section, "max_read", -1);
    if (n > -1) conn->max_read = n;
#endif
    n = tbx_inip_get_integer(lfs->lc->ifd, section, "max_readahead", -1);
    if (n > -1) conn->max_readahead = n;

    apr_pool_create(&(lfs->mpool), NULL);

    //** Get the default host ID for opens
    char hostname[1024];
    apr_gethostname(hostname, sizeof(hostname), lfs->mpool);
    lfs->id = strdup(hostname);

    // TODO: find a cleaner way to get fops here
    //lfs->fops = ctx->fuse->fuse_fs->op;
    lfs->fops = lfs_fops;

    tbx_siginfo_handler_add(SIGUSR1, lio_fuse_info_fn, lfs);

    log_printf(15, "END\n");
    return(lfs); //
}

//** See macro for actual definition
LFS_INIT()
{
#ifdef HAS_FUSE3
    cfg->use_ino = 0;
#endif
    return lfs_init_real(conn,0,NULL,NULL);
}

//*************************************************************************
// lfs_destroy - Destroy a fuse object
//
//    (handles shuting down lio as appropriate, no need to call lio_shutdown() externally)
//
//*************************************************************************

void lfs_destroy(void *private_data)
{
    lio_fuse_t *lfs;

    log_printf(0, "shutting down\n");
    tbx_log_flush();

    lfs = (lio_fuse_t*)private_data;
    if (lfs == NULL) {
        log_printf(0,"lio_fuse_destroy: Error, the lfs handle is null, unable to shutdown cleanly. Perhaps lfs creation failed?");
        return;
    }

    tbx_siginfo_handler_remove(SIGUSR1, lio_fuse_info_fn, lfs);

    lio_fs_destroy(lfs->fs);  //** Destryo the file system handler

    //** Clean up everything else
    if (lfs->lfs_section) free(lfs->lfs_section);
    if (lfs->id) free (lfs->id);
    free(lfs->mount_point);
    apr_pool_destroy(lfs->mpool);
    free(lfs);

    lio_shutdown(); // Reference counting in this function protects against shutdown if lio is still in use elsewhere
}

//********************************************************
// Here's the FUSE operatsions structure
//********************************************************

struct fuse_operations lfs_fops = { //All lfs instances should use the same functions so statically initialize
    .init = lfs_init,
    .destroy = lfs_destroy,

    .opendir = lfs_opendir,
    .releasedir = lfs_closedir,
    .readdir = lfs_readdir,
#ifdef HAS_FUSE3
    .truncate = lfs_ftruncate,
    .getattr = lfs_stat,
    .utimens = lfs_utimens,
    .rename = lfs_rename,
#if FUSE_MINOR_VERSION > 4
    .copy_file_range = lfs_copy_file_range,
#endif
#else
    .truncate = lfs_truncate,
    .ftruncate = lfs_ftruncate,
    .getattr = lfs_stat2,
    .utimens = lfs_utimens2,
    .rename = lfs_rename2,
#endif
    .chmod = lfs_chmod,
    .mknod = lfs_mknod,
    .mkdir = lfs_mkdir,
    .unlink = lfs_unlink,
    .rmdir = lfs_rmdir,
    .flock = lfs_flock,
    .open = lfs_open,
    .release = lfs_release,
    .read = lfs_read,
    .write = lfs_write,
    .flush = lfs_flush,
    .fsync = lfs_fsync,
    .link = lfs_hardlink,
    .readlink = lfs_readlink,
    .symlink = lfs_symlink,
    .statfs = lfs_statvfs,

#ifdef HAVE_XATTR
    .listxattr = lfs_listxattr,
    .getxattr = lfs_getxattr,
    .setxattr = lfs_setxattr,
    .removexattr = lfs_removexattr,
#endif
};

