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

//******************************************************************************
//
//  fs_core provides the core logic for implementing a file system type interface
//  It's deisgned to be foundation for the FUSE library and other 3rd party APIs
//  providing similar functionality.
//
//  NOTE: It assumes all paths are LIO relative so any mount prefix should be
//        removed before being passed in.  There are just a handful of calls
//        that require need further filename processing and all relate to
//        absolute symbolic link paths.  The API calls needing further processing
//        are: lio_fs_stat, lio_fs_readlink, lio_fs_symlink, lio_fs_readdir
//
//        lio_fs_stat, lio_fs_readlink, and lio_fs_readdir all need to have the
//        symlink that's returned checked if it's an absolute path.  If so the
//        the mount point needs to be prepended and additionally for lio_fs_stat
//        the stat.st_size needs to include the new prepended link.
//
//        lio_fs_symlink needs to have the mount point removed if it's an
//        absolute path.
//
//******************************************************************************

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
#include <lio/visibility.h>
#include <lio/fs.h>
#include <lio/segment.h>
#include <pwd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/fsuid.h>
#include <tbx/append_printf.h>
#include <tbx/atomic_counter.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/lio_monitor.h>
#include <tbx/io.h>
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
#include "ex3/system.h"
#include "lio.h"
#include "os.h"
#include "rs.h"

#if defined(HAVE_SYS_XATTR_H)
#include <sys/xattr.h>
#elif defined(HAVE_ATTR_XATTR_H)
#include <attr/xattr.h>
#endif

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t ino);

//#define fs_lock(fs)  log_printf(0, "fs_lock\n"); tbx_log_flush(); apr_thread_mutex_lock((fs)->lock)
//#define fs_unlock(fs) log_printf(0, "fs_unlock\n");  tbx_log_flush(); apr_thread_mutex_unlock((fs)->lock)
#define fs_lock(fs)    apr_thread_mutex_lock((fs)->lock)
#define fs_unlock(fs)  apr_thread_mutex_unlock((fs)->lock)

#define LIO_FS_TAPE_ATTR "system.tape"

static tbx_atomic_int_t _fs_atomic_counter = 0;

#define FS_MON_OBJ_CREATE(...) tbx_mon_object_t mo; \
                               tbx_monitor_obj_create(tbx_monitor_object_fill(&mo, MON_INDEX_FS, tbx_atomic_counter(&_fs_atomic_counter)), __VA_ARGS__); \
                               tbx_monitor_thread_group(&mo, MON_MY_THREAD)
#define FS_MON_OBJ_CREATE_IRATE(size, ...) tbx_mon_object_t mo; \
                               tbx_monitor_obj_create_irate(tbx_monitor_object_fill(&mo, MON_INDEX_FS, tbx_atomic_counter(&_fs_atomic_counter)), size, __VA_ARGS__); \
                               tbx_monitor_thread_group(&mo, MON_MY_THREAD)
#define FS_MON_OBJ_DESTROY() tbx_monitor_thread_ungroup(&mo, MON_MY_THREAD); tbx_monitor_obj_destroy(&mo)
#define FS_MON_OBJ_MESSAGE(...) tbx_monitor_obj_message(&mo, __VA_ARGS__)
#define FS_MON_OBJ_DESTROY_MESSAGE(...) tbx_monitor_thread_ungroup(&mo, MON_MY_THREAD); tbx_monitor_obj_destroy_message(&mo, __VA_ARGS__)

#define _inode_key_size_core 8
#define _inode_key_size_security 11
#define _inode_key_os_realpath_index 7
static char *_inode_keys[] = { "system.inode", "system.modify_data", "system.modify_attr", "system.exnode.size", "os.type", "os.link_count", "os.link",  "os.realpath", "system.posix_acl_default", "security.selinux", "system.posix_acl_access" };

#define _tape_key_size  2
static char *_tape_keys[] = { "system.owner", "system.exnode" };

typedef struct {
    char *dentry;
    struct stat stat;
} fs_dir_entry_t;

typedef struct {
    char *fname;
    ex_id_t sid;
    int ref_count;
    int remove_on_close;
}  fs_open_file_t;

struct lio_fs_dir_iter_t {
    lio_fs_t *fs;
    os_object_iter_t *it;
    lio_os_regex_table_t *path_regex;
    char *val[_inode_key_size_security];
    int v_size[_inode_key_size_security];
    char *dot_path;
    char *dotdot_path;
    fs_dir_entry_t dot_de;
    fs_dir_entry_t dotdot_de;
    tbx_mon_object_t mo;
    int state;
};

#define UG_GLOBAL 0
#define UG_UID    1
#define UG_FSUID  2

//** This is the 1-liner to relase the creds if not local
#define _fs_release_ug(fs, ug_used, ug_local) if ((ug_used) == (ug_local)) osaz_ug_hint_release(fs->osaz, fs->lc->creds, ug_used)

char *_ug_mode_string[] = { "global", "uid", "fsuid" };

struct lio_fs_t {
    int enable_tape;
    int enable_osaz_acl_mappings;
    int enable_osaz_secondary_gids;
    int enable_fuse_hacks;
    int enable_rw_locks;
    int rw_locks_max_wait;
    int xattr_error_for_hard_errors;
    int ug_mode;
    int shutdown;
    int n_merge;
    int enable_security_attr_checks;
    int _inode_key_size;
    int enable_fifo;
    int enable_socket;
    int os_read_mode;
    int os_write_mode;
    ex_off_t copy_bufsize;
    tbx_atomic_int_t read_cmds_inflight;
    tbx_atomic_int_t read_bytes_inflight;
    tbx_atomic_int_t write_cmds_inflight;
    tbx_atomic_int_t write_bytes_inflight;
    lio_config_t *lc;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_hash_t *open_files;
    char *id;
    lio_segment_rw_hints_t *rw_hints;
    lio_os_authz_t *osaz;
    char *authz_section;
    char *fs_section;
    char *rw_lock_attr_string;
    regex_t rw_lock_attr_regex;
    lio_os_authz_local_t ug;
};


//*************************************************************************
// fs OSAZ Wrapper routines.  Basically they just force getting the
//    realpath before passing to the OSAZ
//*************************************************************************

void fs_osaz_attr_filter_apply(lio_fs_t *fs, const char *key, int mode, char **value, int *len, osaz_attr_filter_t filter)
{
    void *v_out;
    int len_out;

    if (filter == NULL) return;

    filter(fs->osaz, (char *)key, mode, *value, *len, &v_out, &len_out);
    free(*value);
    *value = v_out;
    *len = len_out;
    return;
}

//***********************************************************************

void lio_fs_hint_release(lio_fs_t *fs, lio_os_authz_local_t *ug)
{
    osaz_ug_hint_release(fs->osaz, fs->lc->creds, ug);
}

//***********************************************************************

void lio_fs_fill_os_authz_local(lio_fs_t *fs, lio_os_authz_local_t *ug, uid_t uid, gid_t gid)
{
    struct passwd pwd;
    struct passwd *result;
    char buf[32*1024];
    int blen = sizeof(buf);

    ug->uid = uid; //** This is needed for checking for a hint

    log_printf(10, "uid=%d gid=%d\n", uid, gid);

    //** check if we just want to use the primary GID
    if (fs->enable_osaz_secondary_gids == 0) {
oops:
        ug->gid[0] = gid; ug->n_gid = 1;

        //** See if we've already done the mapping
        if (osaz_ug_hint_get(fs->osaz, fs->lc->creds, ug) == 0) return;
        osaz_ug_hint_set(fs->osaz, fs->lc->creds, ug);  //** Make the hint
        return;
    }

    //** See if we've already done the mapping
    if (osaz_ug_hint_get(fs->osaz, fs->lc->creds, ug) == 0) return;

    //** If we made it here then no hint exists so we have to make it
    //** we're using the all the groups the user is a member of
    if (getpwuid_r(uid, &pwd, buf, blen, &result) != 0) {
        goto oops;  //** Buffer was to small or the UID wasn't found so do the fallback
    }
    ug->n_gid = OS_AUTHZ_MAX_GID;
    if (getgrouplist(result->pw_name, result->pw_gid, ug->gid, &(ug->n_gid)) < 0) goto oops;

    osaz_ug_hint_set(fs->osaz, fs->lc->creds, ug);  //** Make the hint
}

//***********************************************************************

lio_os_authz_local_t *lio_fs_new_os_authz_local(lio_fs_t *fs, uid_t uid, gid_t gid)
{
    lio_os_authz_local_t *ug;

    tbx_type_malloc_clear(ug, lio_os_authz_local_t, 1);
    lio_fs_fill_os_authz_local(fs, ug, uid, gid);
    return(ug);
}

//***********************************************************************

void lio_fs_destroy_os_authz_local(lio_fs_t *fs, lio_os_authz_local_t *ug)
{
    if (ug) free(ug);
}

//***********************************************************************

lio_os_authz_local_t *_fs_get_ug(lio_fs_t *fs, lio_os_authz_local_t *my_ug, lio_os_authz_local_t *dummy_ug)
{
    gid_t fsuid;

    if (my_ug) return(my_ug);

    switch (fs->ug_mode) {
    case UG_GLOBAL:
        lio_fs_fill_os_authz_local(fs, dummy_ug, fs->ug.uid, fs->ug.gid[0]);
        break;
    case UG_UID:
        lio_fs_fill_os_authz_local(fs, dummy_ug, getuid(), getgid());
        break;
    case UG_FSUID:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-result"
        fsuid = setfsuid(-1); setfsuid(fsuid);
#pragma GCC diagnostic pop
        lio_fs_fill_os_authz_local(fs, dummy_ug, fsuid, getgid());
        break;
    }
    return(dummy_ug);
}


//***********************************************************************

int lio_fs_realpath(lio_fs_t *fs, const char *path, char *realpath)
{
    int err, v_size;

    v_size = OS_PATH_MAX;
    err = lio_getattr(fs->lc, fs->lc->creds, (char *)path, NULL, "os.realpath", (void **)&realpath, &v_size);
    if (err != OP_STATE_SUCCESS) {
        return(-ENOATTR);
    }

    return(0);
}

//***********************************************************************

int lio_fs_exists(lio_fs_t *fs, const char *path)
{
    return(lio_exists(fs->lc, fs->lc->creds, (char *)path));
}

//***********************************************************************

int fs_osaz_object_create(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path)
{
    char realpath[OS_PATH_MAX];
    char *parent_dir, *file;
    lio_os_authz_local_t dug, *ug_used;
    int err;

    //** The object shouldn't exist so make sure we can access the parent
    lio_os_path_split(path, &parent_dir, &file);
    if (file) free(file);

    if (lio_fs_realpath(fs, parent_dir, realpath) != 0) {
        if (parent_dir) free(parent_dir);
        return(0);
    }
    if (parent_dir) free(parent_dir);

    ug_used = _fs_get_ug(fs, ug, &dug);
    err = osaz_object_create(fs->osaz, fs->lc->creds, ug_used, realpath);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//***********************************************************************

int fs_osaz_object_remove(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path)
{
    char realpath[OS_PATH_MAX];
    lio_os_authz_local_t dug, *ug_used;
    int err;

    if (lio_fs_realpath(fs, path, realpath) != 0) return(0);

    ug_used = _fs_get_ug(fs, ug, &dug);
    err =osaz_object_remove(fs->osaz, fs->lc->creds, ug_used, path);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//***********************************************************************

int fs_osaz_object_access(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path, int mode)
{
    char realpath[OS_PATH_MAX];
    lio_os_authz_local_t dug, *ug_used;
    int err;

    if (lio_fs_realpath(fs, path, realpath) != 0) return(0);

    ug_used = _fs_get_ug(fs, ug, &dug);
    err = osaz_object_access(fs->osaz, fs->lc->creds, ug_used, path, mode);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//***********************************************************************

int fs_osaz_attr_create(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path, const char *key)
{
    char realpath[OS_PATH_MAX];
    lio_os_authz_local_t dug, *ug_used;
    int err;

    if (lio_fs_realpath(fs, path, realpath) != 0) return(0);

    ug_used = _fs_get_ug(fs, ug, &dug);
    err = osaz_attr_create(fs->osaz, fs->lc->creds, ug_used, path, key);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//***********************************************************************

int fs_osaz_attr_remove(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path, const char *key)
{
    char realpath[OS_PATH_MAX];
    lio_os_authz_local_t dug, *ug_used;
    int err;

    if (lio_fs_realpath(fs, path, realpath) != 0) return(0);

    ug_used = _fs_get_ug(fs, ug, &dug);
    err = osaz_attr_remove(fs->osaz, fs->lc->creds, ug_used, path, key);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//***********************************************************************

int fs_osaz_attr_access(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path, const char *key, int mode, osaz_attr_filter_t *filter)
{
    char realpath[OS_PATH_MAX];
    lio_os_authz_local_t dug, *ug_used;
    int err;

    if (lio_fs_realpath(fs, path, realpath) != 0) return(0);

    *filter = NULL;
    ug_used = _fs_get_ug(fs, ug, &dug);
    err = osaz_attr_access(fs->osaz, fs->lc->creds, ug_used, path, key, mode, filter);
    _fs_release_ug(fs, ug_used, &dug);
    return(err);
}

//*************************************************************************
// _fs_parse_inode_vals - Parses the inode values received
//   NOTE: All the val[*] strings are free'ed!
//*************************************************************************

int _fs_parse_stat_vals(lio_fs_t *fs, char *fname, struct stat *stat, char **val, int *v_size, char **symlink, int stat_symlink, int get_lock)
{
    fs_open_file_t *fop;
    char *slink;
    char rpath[OS_PATH_MAX];
    int ftype, i;
    ex_id_t ino;

    //** Do the normal parse
    slink = NULL;
    lio_parse_stat_vals(fname, stat, val, v_size, &slink, &ftype);

    //** Free up the extra attributes used to seed the OS cache
    for (i=7; i<fs->_inode_key_size; i++) {
        if (val[i]) free(val[i]);
    }

    //** Now update the fields based on the requested symlink behavior
    if (slink) {
        if (stat_symlink == 1) {
            stat->st_size = strlen(slink);
            osaz_posix_acl(fs->osaz, fs->lc->creds, fname, OS_OBJECT_FILE_FLAG, NULL, 0, &(stat->st_uid), &(stat->st_gid), &(stat->st_mode));
        } else { //** Get the symlink target values
            ino = 0;
            if (lio_get_symlink_inode(fs->lc, fs->lc->creds, fname, rpath, 1, &ino) == 0) {
                stat->st_ino = ino;

                //** Get the UID/GID from the target
                osaz_posix_acl(fs->osaz, fs->lc->creds, rpath, ftype, NULL, 0, &(stat->st_uid), &(stat->st_gid), &(stat->st_mode));
            } else {
                //** Get the UID/GID from the symlink path
                osaz_posix_acl(fs->osaz, fs->lc->creds, fname, ftype, NULL, 0, &(stat->st_uid), &(stat->st_gid), &(stat->st_mode));
            }
       }
    } else {
        //** Get the UID/GID from the fname
        osaz_posix_acl(fs->osaz, fs->lc->creds, fname, ftype, NULL, 0, &(stat->st_uid), &(stat->st_gid), &(stat->st_mode));
    }

    if (symlink) {
        *symlink = slink;
    } else if (slink) {
        free(slink);
    }

    //** Size
    if (ftype & OS_OBJECT_DIR_FLAG) return(ftype);  //** No need to update the size on a directory
    if (stat_symlink && (ftype & OS_OBJECT_SYMLINK_FLAG)) return(ftype);  //** No need to update the size if they don't want it

    //** Normal file so see if it's open
    ino = stat->st_ino;
    if (get_lock == 1) fs_lock(fs);
    fop = apr_hash_get(fs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop != NULL) {  //** The file is open so need to override the size
        ino = fop->sid;
        lio_update_stat_open_file_size(fs->lc, ino, stat, 1);
    }
    if (get_lock == 1) fs_unlock(fs);

    return(ftype);
}

//*************************************************************************
// lio_fs_same_namespace - checks 2 fs structs and returns if they are use
//     the same LStore namespace.  Returns 1 if they are and 0 if they differ
//*************************************************************************

int lio_fs_same_namespace(lio_fs_t *fs1, lio_fs_t *fs2)
{
    int same;

    same = (fs1->lc->uuid == fs2->lc->uuid) ? 1 : 0;
    return(same);
}

//*************************************************************************
// lio_fs_access - Returns if the uid/gid has perms to perform the operation
//*************************************************************************

int lio_fs_access(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int mode)
{
    int n, os_mode;

    //** See if it's a simple object existence check
    if (mode == F_OK) {
        n = lio_fs_exists(fs, fname);
        return(((n>0) ? 0 : -1));
    }

    //** Got to actually check the perms
    os_mode = 0;
    if (mode & R_OK) os_mode |= OS_MODE_READ_IMMEDIATE;
    if (mode & W_OK) os_mode |= OS_MODE_WRITE_IMMEDIATE;
    n = (os_mode) ? fs_osaz_object_access(fs, ug, fname, os_mode) : 2;
    if (n == 2) {
        if (mode & X_OK) {
           n = lio_fs_exists(fs, fname);
           if ((n & OS_OBJECT_EXEC_FLAG) == 0) n = 0;
        }
    }

    return(((n>0) ? 0 : -1));
}

//*************************************************************************
// lio_fs_stat - Does a stat on the file/dir.
//*************************************************************************

int lio_fs_stat(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct stat *stat, char **symlink, int stat_symlink)
{
    char *val[_inode_key_size_security];
    int v_size[_inode_key_size_security], i, err;

    FS_MON_OBJ_CREATE("FS_STAT: fname=%s", fname);

    if (fs_osaz_object_access(fs, ug, fname, OS_MODE_READ_IMMEDIATE) == 0) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES/ENOENT");
        if (lio_fs_exists(fs, fname) > 0) return(-EACCES);
        return(-ENOENT);
    }

    for (i=0; i<fs->_inode_key_size; i++) v_size[i] = -fs->lc->max_attr;
    err = lio_get_multiple_attrs(fs->lc, fs->lc->creds, fname, NULL, _inode_keys, (void **)val, v_size, fs->_inode_key_size);

    if (err != OP_STATE_SUCCESS) {
        FS_MON_OBJ_DESTROY_MESSAGE("ENOENT");
        return(-ENOENT);
    }

    //** The whole remote fetch and merging with open files is locked to
    //** keep quickly successive stat calls to not get stale information
    _fs_parse_stat_vals(fs, (char *)fname, stat, val, v_size, symlink, stat_symlink, 1);

    FS_MON_OBJ_DESTROY_MESSAGE("size=" XOT, stat->st_size);

    return(0);
}

//*************************************************************************
// lio_fs_fadvise - Same as posix_fadvise()
//   Currently this does nothing.
//*************************************************************************

int lio_fs_fadvise(lio_fs_t *fs, lio_fd_t *fd, off_t offset, off_t len, int advice)
{
    return(0);
}

//*************************************************************************
// lio_fs_dir_is_empty - Determines if the directory is empty
//    Returns  1 if the directory is empty
//             0 contains objects
//            -1 Can't access
//*************************************************************************

int lio_fs_dir_is_empty(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *path)
{
    lio_os_regex_table_t *rp;
    os_object_iter_t *it;
    char wpath[PATH_MAX];
    char *fname;
    int prefix_len, is_empty;
    int obj_types = OS_OBJECT_FILE_FLAG | OS_OBJECT_DIR_FLAG | OS_OBJECT_SYMLINK_FLAG;

    FS_MON_OBJ_CREATE("FS_DIR_IS_EMPTY: fname=%s", path);

    if (fs_osaz_object_access(fs, ug, path, OS_MODE_READ_IMMEDIATE) != 2) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-1);
    }

    snprintf(wpath, PATH_MAX, "%s/*", path);
    rp = lio_os_path_glob2regex(wpath);
    it = lio_create_object_iter(fs->lc, fs->lc->creds, rp, NULL, obj_types, NULL, 1,  NULL, 0);
    if (it == NULL) {
        FS_MON_OBJ_DESTROY_MESSAGE("ERROR: iter create failed");
        return(-1);
    };

    fname = NULL;
    is_empty = 0;
    if (lio_next_object(fs->lc, it, &fname, &prefix_len) == 0) is_empty = 1;
    if (fname) free(fname);

    lio_destroy_object_iter(fs->lc, it);
    lio_os_regex_table_destroy(rp);

    FS_MON_OBJ_DESTROY();
    return(is_empty);
}

//*************************************************************************
// lio_fs_closedir - Closes the opendir file handle
//*************************************************************************

int lio_fs_closedir(lio_fs_dir_iter_t *dit)
{
    if (dit == NULL) return(-EBADF);

    tbx_monitor_obj_destroy(&(dit->mo));

    if (dit->dot_path) free(dit->dot_path);
    if (dit->dotdot_path) free(dit->dotdot_path);

    if (dit->it) lio_destroy_object_iter(dit->fs->lc, dit->it);
    lio_os_regex_table_destroy(dit->path_regex);
    free(dit);

    return(0);
}

//*************************************************************************
// lio_fs_opendir - opendir call
//*************************************************************************

lio_fs_dir_iter_t *lio_fs_opendir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname)
{
    lio_fs_dir_iter_t *dit;
    char path[OS_PATH_MAX];
    char *dir, *file;
    int i;

    tbx_log_flush();
log_printf(0, "START fname=%s\n", fname);

    if (fs_osaz_object_access(fs, ug, fname, OS_MODE_READ_IMMEDIATE) != 2) {
        return(NULL);
    }
log_printf(0, "CACN_ACCESS=2 fname=%s\n", fname);

    tbx_type_malloc_clear(dit, lio_fs_dir_iter_t, 1);

    for (i=0; i<fs->_inode_key_size; i++) {
        dit->v_size[i] = -fs->lc->max_attr;
        dit->val[i] = NULL;
    }

    dit->fs = fs;
    snprintf(path, OS_PATH_MAX, "%s/*", fname);
    dit->path_regex = lio_os_path_glob2regex(path);

    tbx_monitor_obj_create(tbx_monitor_object_fill(&(dit->mo), MON_INDEX_FS, tbx_atomic_counter(&_fs_atomic_counter)), "FS_OPENDIR: fname=%s", fname);
    tbx_monitor_thread_group(&(dit->mo), MON_MY_THREAD);

    dit->it = lio_create_object_iter_alist(dit->fs->lc, dit->fs->lc->creds, dit->path_regex, NULL, OS_OBJECT_ANY_FLAG, 0, _inode_keys, (void **)dit->val, dit->v_size, fs->_inode_key_size);
    if (dit->it == NULL) {
        tbx_monitor_thread_ungroup(&(dit->mo), MON_MY_THREAD);
        lio_fs_closedir(dit);
        return(NULL);
    }

    dit->state = 0;

    //** Add "."
    dit->dot_path = strdup(fname);
    if (lio_fs_stat(fs, ug, fname, &(dit->dot_de.stat), NULL, 1) != 0) {
        lio_fs_closedir(dit);
        tbx_monitor_thread_ungroup(&(dit->mo), MON_MY_THREAD);
        return(NULL);
    }

    //** And ".."
    if (strcmp(fname, "/") != 0) {
        lio_os_path_split((char *)fname, &dir, &file);
        dit->dotdot_path = dir;
        free(file);
    } else {
        dit->dotdot_path = strdup(fname);
    }

    log_printf(1, "dot=%s dotdot=%s\n", dit->dot_path, dit->dotdot_path);

    if (lio_fs_stat(fs, ug, dit->dotdot_path, &(dit->dotdot_de.stat), NULL, 1) != 0) {
        lio_fs_closedir(dit);
        tbx_monitor_thread_ungroup(&(dit->mo), MON_MY_THREAD);
        return(NULL);
    }

    tbx_monitor_thread_ungroup(&(dit->mo), MON_MY_THREAD);

    return(dit);
}

//*************************************************************************
// lio_fs_readdir - Returns the next file in the directory
//*************************************************************************

int lio_fs_readdir(lio_fs_dir_iter_t *dit, char **dentry, struct stat *stat, char **symlink, int stat_symlink)
{
    int ftype, prefix_len;
    char *fname;

    if (dit == NULL) {
        return(-EBADF);
    }

    //** See if we are dealing with "." or ".."
    if (dit->state == 0) {
        *dentry = strdup(".");
        *stat = dit->dot_de.stat;
    } else if (dit->state == 1) {
        *dentry = strdup("..");
        *stat = dit->dotdot_de.stat;
    }

    tbx_monitor_obj_message(&(dit->mo), "FS_READDIR");
    dit->state++;
    if (dit->state <= 2) return(0);  //** See if we have an early kickout

    //** If we made it here then grab the next file and look it up.
    tbx_monitor_thread_group(&(dit->mo), MON_MY_THREAD);
    ftype = lio_next_object(dit->fs->lc, dit->it, &fname, &prefix_len);
    tbx_monitor_thread_ungroup(&(dit->mo), MON_MY_THREAD);
    if (ftype <= 0) { //** No more files
        return((ftype == 0) ? 1 : -EIO);
    }

    *dentry = strdup(fname+prefix_len+1);
    _fs_parse_stat_vals(dit->fs, fname, stat, dit->val, dit->v_size, symlink, stat_symlink, 1);
    free(fname);

    return(0);
}

//*************************************************************************
// lio_fs_object_create
//*************************************************************************

int lio_fs_object_create(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode)
{
    char fullname[OS_PATH_MAX];
    int err, n, exec_mode;
    gop_op_status_t status;
    int os_mode = lio_mode2os_flags(mode);

    FS_MON_OBJ_CREATE("FS_OBJECT_CREATE: fname=%s mode=%d", fname, mode);

    if ((os_mode & OS_OBJECT_FIFO_FLAG) && (fs->enable_fifo == 0)) { FS_MON_OBJ_DESTROY_MESSAGE("EOPNOTSUPP: FIFO"); return(-EOPNOTSUPP); }
    if ((os_mode & OS_OBJECT_SOCKET_FLAG) && (fs->enable_socket == 0)) { FS_MON_OBJ_DESTROY_MESSAGE("EOPNOTSUPP: SOCKET"); return(-EOPNOTSUPP); }

    //** Make sure it doesn't exists
    n = lio_fs_exists(fs, fname);
    if (n != 0) {  //** File already exists
        log_printf(15, "File already exist! fname=%s\n", fullname);
        FS_MON_OBJ_DESTROY_MESSAGE("EEXIST");
        return(-EEXIST);
    }

    if (fs_osaz_object_create(fs, ug, fname) != 1) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** If we made it here it's a new file or dir
    //** Create the new object
    err = gop_sync_exec(lio_create_gop(fs->lc, fs->lc->creds, (char *)fname, os_mode, NULL, fs->id));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Error creating object! fname=%s\n", fullname);
        if (strlen(fullname) > 3900) {  //** Probably a path length issue
            FS_MON_OBJ_DESTROY_MESSAGE("ENAMETOOLONG");
            return(-ENAMETOOLONG);
        }
        FS_MON_OBJ_DESTROY_MESSAGE("EREMOTEIO");
        return(-EREMOTEIO);
    }

    err = 0;
    exec_mode = ((S_IXUSR|S_IXGRP|S_IXOTH) & mode) ? 1 : 0;
    if (exec_mode) {
        status = gop_sync_exec_status(os_object_exec_modify(fs->lc->os, fs->lc->creds, (char *)fname, exec_mode));
        if (status.op_status != OP_STATE_SUCCESS) {
            FS_MON_OBJ_DESTROY_MESSAGE("EACCES-EXEC");
            return(-EACCES);
        }
    }

    FS_MON_OBJ_DESTROY();

    return(err);
}

//*************************************************************************
// lio_fs_mknod - Makes a regular file
//*************************************************************************

int lio_fs_mknod(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode, dev_t rdev)
{
    return(lio_fs_object_create(fs, ug, fname, mode));
}

//*************************************************************************
// lio_fs_chmod - Currently this only changes the exec bit for a FILE
//*************************************************************************

int lio_fs_chmod(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode)
{
    gop_op_status_t status;
    int exec_mode, err;

    FS_MON_OBJ_CREATE("FS_CHMOD: fname=%s mode=%d", fname, mode);

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_WRITE_IMMEDIATE)) {
        log_printf(0, "Invalid access: path=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    exec_mode = ((S_IXUSR|S_IXGRP|S_IXOTH) & mode) ? 1 : 0;
    status = gop_sync_exec_status(os_object_exec_modify(fs->lc->os, fs->lc->creds, (char *)fname, exec_mode));
    err = (status.op_status == OP_STATE_SUCCESS) ? 0 : -EACCES;

    FS_MON_OBJ_DESTROY();

    return(err);
}

//*************************************************************************
// lio_fs_mkdir - Makes a directory
//*************************************************************************

int lio_fs_mkdir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, mode_t mode)
{
    mode |= S_IFDIR;
    return(lio_fs_object_create(fs, ug, fname, mode));
}

//*****************************************************************
// fs_actual_remove - Does the actual removal
//*****************************************************************

int fs_actual_remove(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int ftype)
{
    int err;
    err = gop_sync_exec(lio_remove_gop(fs->lc, fs->lc->creds, (char *)fname, NULL, 0));

    if (err == OP_STATE_SUCCESS) {
        return(0);
    } else if ((ftype & OS_OBJECT_DIR_FLAG) > 0) { //** Most likey the dirs not empty
        return(-ENOTEMPTY);
    }

    return(-EREMOTEIO);  //** Probably an expired exnode but through an error anyway
}

//*****************************************************************
//  lio_fs_object_remove - Removes a file or directory
//*****************************************************************

int lio_fs_object_remove(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int ftype)
{
    fs_open_file_t *fop;
    int err;

    FS_MON_OBJ_CREATE("FS_OBJECT_REMOVE: fname=%s ftype=%d", fname,ftype);

    //** Make sure we can access it
    if (!fs_osaz_object_remove(fs, ug, fname)) {
        log_printf(0, "Invalid access: path=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** Check if it's open.  If so do a delayed removal
    fs_lock(fs);
    fop = apr_hash_get(fs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop != NULL) {
        fop->remove_on_close = 1;
        fs_unlock(fs);
        FS_MON_OBJ_DESTROY_MESSAGE("In use. Remove on close");
        return(0);
    }
    fs_unlock(fs);

    err = fs_actual_remove(fs, ug, fname, ftype);
    if (err) {
        FS_MON_OBJ_DESTROY_MESSAGE("error=%d", err);
    } else {
        FS_MON_OBJ_DESTROY();
    }
    return(err);
}


//*****************************************************************
//  lio_fs_unlink - Remove a file
//*****************************************************************

int lio_fs_unlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname)
{
    return(lio_fs_object_remove(fs, ug, fname, OS_OBJECT_FILE_FLAG));
}

//*****************************************************************
//  lio_fs_rmdir - Remove a directory
//*****************************************************************

int lio_fs_rmdir(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname)
{
    return(lio_fs_object_remove(fs, ug, fname, OS_OBJECT_DIR_FLAG));
}

//*****************************************************************
// lio_fs_open - Opens a file for I/O
//*****************************************************************

lio_fd_t *lio_fs_open(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, int lflags)
{
    lio_fd_t *fd;
    fs_open_file_t *fop;
    int os_mode;

    log_printf(10, "fname=%s lflags=%d O_RDONLY=%d O_WRONLY=%d O_RDWR=%d LIO_READ=%d LIO_WRITE=%d\n", fname, lflags, O_RDONLY, O_WRONLY, O_RDWR, LIO_READ_MODE, LIO_WRITE_MODE);

    FS_MON_OBJ_CREATE("FS_OPEN: fname=%s lflags=%d", fname, lflags);

    //** Make sure we can access it
    if (lflags & LIO_CREATE_MODE) {
        if (!fs_osaz_object_create(fs, ug, fname)) {
            log_printf(0, "Invalid access for create: path=%s\n", fname);
            FS_MON_OBJ_DESTROY_MESSAGE("EACCESS: CREATE");
            errno = EACCES;
            return(NULL);  //EACCESS
        }
    } else {
        os_mode = (lflags & LIO_WRITE_MODE) ? OS_MODE_WRITE_IMMEDIATE : OS_MODE_READ_IMMEDIATE;
        if (!fs_osaz_object_access(fs, ug, fname, os_mode)) {
            log_printf(0, "Invalid access: path=%s\n", fname);
            FS_MON_OBJ_DESTROY_MESSAGE("EACCESS");
            errno = EACCES;
            return(NULL);  //EACCESS
        }
    }

    if (fs->enable_rw_locks) lflags |= LIO_LOCK_MODE;

    //** Ok we can access the file if we made it here
    gop_sync_exec(lio_open_gop(fs->lc, fs->lc->creds, (char *)fname, lflags, fs->id, &fd, fs->rw_locks_max_wait));
    log_printf(2, "fname=%s fd=%p\n", fname, fd);
    if (fd == NULL) {
        log_printf(0, "Failed opening file!  path=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("EREMOTEIO: open failed");
        errno = EREMOTEIO;
        return(NULL);  //EREMOTEIO
    }

    fs_lock(fs);
    fop = apr_hash_get(fs->open_files, fname, APR_HASH_KEY_STRING);
    if (fop == NULL) {
        tbx_type_malloc_clear(fop, fs_open_file_t, 1);
        fop->fname = strdup(fd->path);
        fop->sid = segment_id(fd->fh->seg);
        apr_hash_set(fs->open_files, fop->fname, APR_HASH_KEY_STRING, fop);
    }
    fop->ref_count++;
    fs_unlock(fs);

    //** See if we have WQ enabled
    if (fs->n_merge > 0) lio_wq_enable(fd, fs->n_merge);

    FS_MON_OBJ_DESTROY();
    errno = 0;
    return(fd);
}

//*****************************************************************
// lio_fs_close - Closes a file
//*****************************************************************

int lio_fs_close(lio_fs_t *fs, lio_fd_t *fd)
{
    fs_open_file_t *fop;
    int err, remove_on_close;

    remove_on_close = 0;

    FS_MON_OBJ_CREATE("FS_CLOSE: fname=%s", fd->path);

    //** We lock overthe whole close process to make sure an immediate stat call
    //** doesn't get stale information.
    fs_lock(fs);
    fop = apr_hash_get(fs->open_files, fd->path, APR_HASH_KEY_STRING);
    if (fop) {
        remove_on_close = fop->remove_on_close;
        fop->ref_count--;
        if (fop->ref_count <= 0) {  //** Last one so remove it.
            apr_hash_set(fs->open_files, fd->path, APR_HASH_KEY_STRING, NULL);
            free(fop->fname);
            free(fop);
        }
    }
    fs_unlock(fs);

    //** See if we need to remove it
    if (remove_on_close == 1) {
        segment_lock(fd->fh->seg);
        fd->fh->remove_on_close = 1;
        segment_unlock(fd->fh->seg);
    }

    err = gop_sync_exec(lio_close_gop(fd)); // ** Close it but keep track of the error

    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "Failed closing file!\n");
        FS_MON_OBJ_DESTROY_MESSAGE("EREMOREIO");
        return(-EREMOTEIO);
    }

    FS_MON_OBJ_DESTROY();
    return(0);
}

//***********************************************************************
// lio_fs_seek - Sets the file position
//***********************************************************************

off_t lio_fs_seek(lio_fs_t *fs, lio_fd_t *fd, off_t offset, int whence)
{
    return(lio_seek(fd, offset, whence));
}

//***********************************************************************
// lio_fs_tell - Return the current position
//***********************************************************************

off_t lio_fs_tell(lio_fs_t *fs, lio_fd_t *fd)
{
    return(lio_tell(fd));
}

//***********************************************************************
// lio_fs_size - Return the file size
//***********************************************************************

off_t lio_fs_size(lio_fs_t *fs, lio_fd_t *fd)
{
    return(lio_size_fh(fd->fh));
}

//*****************************************************************
// lio_fs_pread - Reads data from a file using the give offset
//    NOTE: Uses the FS readahead hints
//          if off < 0 then the current buffer position is used
//*****************************************************************

ssize_t lio_fs_pread(lio_fs_t *fs, lio_fd_t *fd, char *buf, size_t size, off_t off)
{
    ex_off_t nbytes;
    apr_time_t now;
    double dt;

    ex_off_t t1, t2;
    t1 = size;
    t2 = off;

    FS_MON_OBJ_CREATE_IRATE(size, "FS_PREAD: off=" OT " size=" ST, off, size);
    log_printf(1, "fname=%s size=" XOT " off=" XOT " fd=%p\n", fd->path, t1, t2, fd);
    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null file desriptor\n");
        FS_MON_OBJ_DESTROY_MESSAGE("EDADF");
        return(-EBADF);
    }

    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    now = apr_time_now();

    //** Do the read op
    tbx_atomic_inc(fs->read_cmds_inflight);
    tbx_atomic_add(fs->read_bytes_inflight, size);
    nbytes = lio_read(fd, buf, size, off, fs->rw_hints);
    tbx_atomic_dec(fs->read_cmds_inflight);
    tbx_atomic_sub(fs->read_bytes_inflight, size);

    if (tbx_log_level() > 0) {
        t2 = size+off-1;
        log_printf(1, "LFS_READ:START " XOT " %zu\n", off, size);
        log_printf(1, "LFS_READ:END " XOT "\n", t2);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s seg=" XIDT " size=" XOT " off=%zu nbytes=" XOT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), t1, t2, nbytes, dt);
    tbx_log_flush();

    FS_MON_OBJ_DESTROY();
    return(nbytes);
}

//*****************************************************************
// lio_fs_readv - Does a IOvec Read operation
//*****************************************************************

ssize_t lio_fs_readv(lio_fs_t *fs, lio_fd_t *fd, const struct iovec *iov, int iovcnt, off_t offset)
{
    ex_off_t n, nbytes, ret;
    int i;

    nbytes = 0;
    for (i=0; i<iovcnt; i++) nbytes += iov[i].iov_len;

    FS_MON_OBJ_CREATE_IRATE(nbytes, "FS_READV: n_iov=%d off=" OT " size=" XOT, iovcnt, offset, nbytes);
    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    tbx_atomic_inc(fs->read_cmds_inflight);
    tbx_atomic_add(fs->read_bytes_inflight, nbytes);
    n = lio_readv(fd, (struct iovec *)iov, iovcnt, nbytes, offset, NULL);
    tbx_atomic_dec(fs->read_cmds_inflight);
    tbx_atomic_sub(fs->read_bytes_inflight, nbytes);

    ret = (n == nbytes) ? nbytes : 0;

    FS_MON_OBJ_DESTROY();

    return(ret);
}

//*****************************************************************
// lio_fs_read - Normal reading from the current file position
//*****************************************************************

ssize_t lio_fs_read(lio_fs_t *fs, lio_fd_t *fd, char *buf, size_t size)
{
    return(lio_fs_pread(fs, fd, buf, size, -1));
}

//*****************************************************************
// lio_fs_read_ex - Performs a multi-offset, ie gather, read
//*****************************************************************

int lio_fs_read_ex(lio_fs_t *fs, lio_fd_t *fd, int n_ex_iov, ex_tbx_iovec_t *ex_iov, const struct iovec *iov, int iovcnt, size_t iov_nbytes, off_t iov_off)
{
    tbx_tbuf_t tbuf;
    int err;

    FS_MON_OBJ_CREATE_IRATE(iov_nbytes, "FS_READ_EX: n_ex=%d n_iov=%d off[0]=" OT " size=" XOT, n_ex_iov, iovcnt, ex_iov[0].offset, iov_nbytes);
    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    tbx_tbuf_vec(&tbuf, iov_nbytes, iovcnt, (struct iovec *)iov);

    tbx_atomic_inc(fs->read_cmds_inflight);
    tbx_atomic_add(fs->read_bytes_inflight, iov_nbytes);
    err = gop_sync_exec(lio_read_ex_gop(fd, n_ex_iov, ex_iov, &tbuf, iov_off, NULL));
    tbx_atomic_dec(fs->read_cmds_inflight);
    tbx_atomic_sub(fs->read_bytes_inflight, iov_nbytes);

    FS_MON_OBJ_DESTROY();

    return(((err == OP_STATE_SUCCESS) ? 0 : -EIO));
}

//*****************************************************************
// lio_fs_pwrite - Writes data to a file
//    NOTE: Uses the FS readahead hints
//          if off < 0 then the current buffer position is used
//*****************************************************************

ssize_t lio_fs_pwrite(lio_fs_t *fs, lio_fd_t *fd, const char *buf, size_t size, off_t off)
{
    ex_off_t nbytes;
    apr_time_t now;
    double dt;

    ex_off_t t1, t2;
    t1 = size;
    t2 = off;

    FS_MON_OBJ_CREATE_IRATE(size, "FS_PWRITE: off=" OT " size=" ST, off, size);

    log_printf(1, "fname=%s size=" XOT " off=" XOT " fd=%p\n", fd->path, t1, t2, fd);
    tbx_log_flush();
    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null LFS handle\n");
        return(-EBADF);
    }

    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    now = apr_time_now();

    //** Do the write op
    tbx_atomic_inc(fs->write_cmds_inflight);
    tbx_atomic_add(fs->write_bytes_inflight, size);
    nbytes = lio_write(fd, (char *)buf, size, off, fs->rw_hints);
    tbx_atomic_dec(fs->write_cmds_inflight);
    tbx_atomic_sub(fs->write_bytes_inflight, size);

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s seg=" XIDT " size=" XOT " off=" XOT " nbytes=" XOT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), t1, t2, nbytes, dt);
    tbx_log_flush();

    FS_MON_OBJ_DESTROY();

    return(nbytes);
}

//*****************************************************************
// lio_fs_write - Normal writinging from the current file position
//*****************************************************************

ssize_t lio_fs_write(lio_fs_t *fs, lio_fd_t *fd, const char *buf, size_t size)
{
    return(lio_fs_pwrite(fs, fd, buf, size, -1));
}

//*****************************************************************
// lio_fs_writev - Does a IOvec write operation
//*****************************************************************

ssize_t lio_fs_writev(lio_fs_t *fs, lio_fd_t *fd, const struct iovec *iov, int iovcnt, off_t offset)
{
    ex_off_t n, nbytes, ret;
    int i;

    nbytes = 0;
    for (i=0; i<iovcnt; i++) nbytes += iov[i].iov_len;

    FS_MON_OBJ_CREATE_IRATE(nbytes, "FS_WRITEV: n_iov=%d off=" OT " size=" XOT, iovcnt, offset, nbytes);
    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    tbx_atomic_inc(fs->write_cmds_inflight);
    tbx_atomic_add(fs->write_bytes_inflight, nbytes);
    n = lio_writev(fd, (struct iovec *)iov, iovcnt, nbytes, offset, NULL);
    tbx_atomic_dec(fs->write_cmds_inflight);
    tbx_atomic_sub(fs->write_bytes_inflight, nbytes);

    FS_MON_OBJ_DESTROY();

    ret = (n == nbytes) ? nbytes : 0;

    return(ret);
}

//*****************************************************************
// lio_fs_write_ex - Performs a multi-offset, ie scatter,  write
//*****************************************************************

int lio_fs_write_ex(lio_fs_t *fs, lio_fd_t *fd, int n_ex_iov, ex_tbx_iovec_t *ex_iov, const struct iovec *iov, int iovcnt, size_t iov_nbytes, off_t iov_off)
{
    tbx_tbuf_t tbuf;
    int err;

    FS_MON_OBJ_CREATE_IRATE(iov_nbytes, "FS_WRITE_EX: n_ex=%d n_iov=%d off[0]=" OT " size=" XOT, n_ex_iov, iovcnt, ex_iov[0].offset, iov_nbytes);
    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    tbx_tbuf_vec(&tbuf, iov_nbytes, iovcnt, (struct iovec *)iov);

    tbx_atomic_inc(fs->write_cmds_inflight);
    tbx_atomic_add(fs->write_bytes_inflight, iov_nbytes);
    err = gop_sync_exec(lio_write_ex_gop(fd, n_ex_iov, ex_iov, &tbuf, iov_off, NULL));
    tbx_atomic_dec(fs->write_cmds_inflight);
    tbx_atomic_sub(fs->write_bytes_inflight, iov_nbytes);

    FS_MON_OBJ_DESTROY();

    return(((err == OP_STATE_SUCCESS) ? 0 : -EIO));
}

//*****************************************************************
// lio_fs_flush - Flushes any data to backing store
//*****************************************************************

int lio_fs_flush(lio_fs_t *fs, lio_fd_t *fd)
{
    int err;
    apr_time_t now;
    double dt;

    now = apr_time_now();

    if (fd == NULL) {
        return(-EBADF);
    }

    log_printf(1, "START fname=%s\n", fd->path);

    FS_MON_OBJ_CREATE("FS_FLUSH");
    tbx_monitor_obj_reference(&mo, &(fd->fh->mo));

    err = gop_sync_exec(lio_flush_gop(fd, 0, -1));
    if (err != OP_STATE_SUCCESS) {
        FS_MON_OBJ_DESTROY_MESSAGE("EIO");
        return(-EIO);
    }

    FS_MON_OBJ_DESTROY();

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "END fname=%s dt=%lf\n", fd->path, dt);
    tbx_log_flush();

    return(0);
}


//*****************************************************************
// lio_fs_copy_file_range - Copies data between files
//*****************************************************************

ex_off_t lio_fs_copy_file_range(lio_fs_t *fs, lio_fd_t *fd_in, off_t offset_in, lio_fd_t *fd_out, off_t offset_out, size_t size)
{
    int err;

    if (fd_in == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_in handle\n");
        return(-EBADF);
    }

    if (fd_out == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_out handle\n");
        return(-EBADF);
    }

    log_printf(1, "START copy_file_range src=%s dest=%s\n", fd_in->path, fd_out->path);

    FS_MON_OBJ_CREATE_IRATE(size, "FS_COPY_FILE_RANGE: fin=%s fout=%s off_in=" OT " off_out=" OT " size=" ST, fd_in->fh->fname, fd_out->fh->fname, offset_in, offset_out, size);
    tbx_monitor_obj_reference(&mo, &(fd_in->fh->mo));
    tbx_monitor_obj_reference(&mo, &(fd_out->fh->mo));

    //** Do the copy op
    err = gop_sync_exec(lio_cp_lio2lio_gop(fd_in, fd_out, 0, NULL, offset_in, offset_out, size, 0, fs->rw_hints));
    if (err != OP_STATE_SUCCESS) {
        FS_MON_OBJ_DESTROY_MESSAGE("EIO");
        return(-EIO);
    }

    FS_MON_OBJ_DESTROY();
    return(size);

}

//*************************************************************************
// lio_fs_rename - Renames a file
//*************************************************************************

int lio_fs_rename(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *oldname, const char *newname)
{
    fs_open_file_t *fop;
    gop_op_status_t status;

    log_printf(1, "oldname=%s newname=%s\n", oldname, newname);

    FS_MON_OBJ_CREATE("FS_RENAME: old=%s new=%s", oldname, newname);

    //** Make sure we can access it
    if (!(fs_osaz_object_remove(fs, ug, oldname) && fs_osaz_object_create(fs, ug, newname))) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    fs_lock(fs);
    fop = apr_hash_get(fs->open_files, oldname, APR_HASH_KEY_STRING);
    if (fop) {  //** Got an open file so need to mve the entry there as well.
        apr_hash_set(fs->open_files, oldname, APR_HASH_KEY_STRING, NULL);
        free(fop->fname);
        fop->fname = strdup(newname);
        apr_hash_set(fs->open_files, fop->fname, APR_HASH_KEY_STRING, fop);
    }
    fs_unlock(fs);

    //** Do the move
    status = gop_sync_exec_status(lio_move_object_gop(fs->lc, fs->lc->creds, (char *)oldname, (char *)newname));
    if (status.op_status != OP_STATE_SUCCESS) {
        FS_MON_OBJ_DESTROY_MESSAGE("ERROR");
        return((status.error_code != 0) ? -status.error_code : -EREMOTEIO);
    }

    FS_MON_OBJ_DESTROY();
    return(0);
}

//*****************************************************************
// lio_fs_lio2local - Copy a local file to LStore
//*****************************************************************

int lio_fs_copy_lio2local(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *src_lio_fname, const char *dest_local_fname, int bufsize, char *buffer, ex_off_t offset, ex_off_t len, lio_segment_rw_hints_t *rw_hints)
{
    lio_fd_t *sfd;
    FILE *dfd;
    char *buf2;
    int err, ftype;
    struct stat sbuf;


    //** Set things up
    sfd = lio_fs_open(fs, ug, src_lio_fname, LIO_READ_MODE);
    if (!sfd) {
        log_printf(1, "ERROR: Failed opening lio file: %s\n", src_lio_fname);
        return(-1);
    }

    dfd = tbx_io_fopen(dest_local_fname, "w");
    if (!dfd) {
        lio_fs_close(fs, sfd);
        log_printf(1, "ERROR: Failed opening destination local file: %s\n", dest_local_fname);
        return(-1);
    }

    //** Check the buffer
    buf2 = buffer;
    if (buffer == NULL) { //** Need to make our own buffer
        if (bufsize <= 0) bufsize = fs->copy_bufsize;
        tbx_fudge_align_size(bufsize, 2*getpagesize());
        tbx_malloc_align(buf2, getpagesize(), bufsize);
    }

    //** Now do the actual copy
    err = gop_sync_exec(lio_cp_lio2local_gop(sfd, dfd, bufsize, buf2, offset, len, rw_hints));
    if (err == OP_STATE_SUCCESS) { //** See if we need to set the exec bits
        err = 0;
        ftype = lio_fs_exists(fs, src_lio_fname);
        stat(dest_local_fname, &sbuf);
        if (ftype & OS_OBJECT_EXEC_FLAG)  {
            if (sbuf.st_mode & S_IRUSR) sbuf.st_mode |= S_IXUSR;
            if (sbuf.st_mode & S_IRGRP) sbuf.st_mode |= S_IXGRP;
            if (sbuf.st_mode & S_IROTH) sbuf.st_mode |= S_IXOTH;
            chmod(dest_local_fname, sbuf.st_mode);
        } else {
            if (sbuf.st_mode & S_IRUSR) sbuf.st_mode ^= S_IXUSR;
            if (sbuf.st_mode & S_IRGRP) sbuf.st_mode ^= S_IXGRP;
            if (sbuf.st_mode & S_IROTH) sbuf.st_mode ^= S_IXOTH;
            chmod(dest_local_fname, sbuf.st_mode);
        }
    } else {
        err = -1;
    }

    //** Clean up
    if (buf2 != buffer) free(buf2);
    lio_fs_close(fs, sfd);
    tbx_io_fclose(dfd);

    return(err);
}

//*****************************************************************
// lio_fs_local2lio - Copy an LStore file to the local file system
//*****************************************************************

int lio_fs_copy_local2lio(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *src_local_fname, const char *dest_lio_fname, int bufsize, char *buffer, ex_off_t offset, ex_off_t len, int do_truncate, lio_segment_rw_hints_t *rw_hints)
{
    lio_fd_t *dfd;
    FILE *sfd;
    char *buf2;
    int err, exec_mode;
    struct stat sbuf;

    //** Set things up
    sfd = tbx_io_fopen(src_local_fname, "r");
    if (!sfd) {
        log_printf(1, "ERROR: Failed opening local src file: %s\n", src_local_fname);
        return(-1);
    }

    dfd = lio_fs_open(fs, ug, dest_lio_fname, LIO_READ_MODE);
    if (!dfd) {
        tbx_io_fclose(sfd);
        log_printf(1, "ERROR: Failed opening destination lio file: %s\n", dest_lio_fname);
        return(-1);
    }

    //** Check the buffer
    buf2 = buffer;
    if (buffer == NULL) { //** Need to make our own buffer
        if (bufsize <= 0) bufsize = fs->copy_bufsize;
        tbx_fudge_align_size(bufsize, 2*getpagesize());
        tbx_malloc_align(buf2, getpagesize(), bufsize);
    }

    //** Now do the actual copy
    err = gop_sync_exec(lio_cp_local2lio_gop(sfd, dfd, bufsize, buf2, offset, len, do_truncate, rw_hints));
    if (err == OP_STATE_SUCCESS) { //** See if we need to set the exec bits
        err = 0;
        stat(src_local_fname, &sbuf);
        exec_mode = (sbuf.st_mode & (S_IXUSR|S_IXGRP|S_IXOTH)) ? 1 : 0;
        gop_sync_exec(os_object_exec_modify(fs->lc->os, fs->lc->creds, (char *)dest_lio_fname, exec_mode));
    } else {
        err = -1;
    }

    //** Clean up
    if (buf2 != buffer) free(buf2);
    tbx_io_fclose(sfd);
    lio_fs_close(fs, dfd);

    return(err);
}

//*****************************************************************
// lio_fs_lio2lio - Copy an LStore file to another LStore file
//*****************************************************************

int lio_fs_copy_lio2lio(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *src_lio_fname, const char *dest_lio_fname, int bufsize, char *buffer, ex_off_t offset, ex_off_t len, lio_segment_rw_hints_t *rw_hints)
{
    lio_fd_t *dfd;
    lio_fd_t *sfd;
    char *buf2;
    int err, exec_mode, ftype;

    //** Set things up
    sfd = lio_fs_open(fs, ug, dest_lio_fname, LIO_READ_MODE);
    if (!sfd) {
        log_printf(1, "ERROR: Failed opening liosrc file: %s\n", src_lio_fname);
        return(-1);
    }

    dfd = lio_fs_open(fs, ug, dest_lio_fname, LIO_READ_MODE);
    if (!dfd) {
        lio_fs_close(fs, sfd);
        log_printf(1, "ERROR: Failed opening destination lio file: %s\n", dest_lio_fname);
        return(-1);
    }

    //** Check the buffer
    buf2 = buffer;
    if (buffer == NULL) { //** Need to make our own buffer
        if (bufsize <= 0) bufsize = fs->copy_bufsize;
        tbx_fudge_align_size(bufsize, 2*getpagesize());
        tbx_malloc_align(buf2, getpagesize(), bufsize);
    }

    //** Now do the actual copy
    err = gop_sync_exec(lio_cp_lio2lio_gop(sfd, dfd, bufsize, buf2, 0, offset, len, LIO_COPY_INDIRECT, rw_hints));
    if (err == OP_STATE_SUCCESS) { //** See if we need to set the exec bits
        err = 0;
        ftype = lio_fs_exists(fs, src_lio_fname);
        exec_mode = (ftype & OS_OBJECT_EXEC_FLAG) ? 1 : 0;
        gop_sync_exec(os_object_exec_modify(fs->lc->os, fs->lc->creds, (char *)dest_lio_fname, exec_mode));
    } else {
        err = -1;
    }

    //** Clean up
    if (buf2 != buffer) free(buf2);
    lio_fs_close(fs, sfd);
    lio_fs_close(fs, dfd);

    return(err);
}

//*****************************************************************
// lio_fs_copy_gop - Copy file operation
//*****************************************************************

gop_op_generic_t *lio_fs_copy_gop(lio_fs_t *fs, lio_os_authz_local_t *ug, int src_is_lio, const char *src_fname, int dest_is_lio, const char *dest_fname, ex_off_t bufsize, int do_slow_copy, int enable_local2local, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_file_t *cp;
    gop_op_generic_t *gop;

    //** Go ahead and sanity check that the user has the appropriate perms.
    //** Ideally this would be done at the time of the copy to get better parallel performance.
    if (src_is_lio) { //** Check if we have read access
        if (fs_osaz_object_access(fs, ug, src_fname, OS_MODE_READ_IMMEDIATE) == 0) {
            log_printf(1, "ERROR: Read access denied for src_fname=%s\n", src_fname);
            return(gop_dummy(gop_failure_status));
        }
    }
    if (dest_is_lio) { //** Check if we have read access
        if (fs_osaz_object_create(fs, ug, dest_fname) == 0) {
            log_printf(1, "ERROR: Create access denied for dest_fname=%s\n", dest_fname);
            return(gop_dummy(gop_failure_status));
        }
    }

    //** Now make the copy struct
    tbx_type_malloc_clear(cp, lio_cp_file_t, 1);
    cp->src_tuple.creds = fs->lc->creds;
    cp->src_tuple.lc = fs->lc;
    cp->src_tuple.is_lio = src_is_lio;
    cp->src_tuple.path = (char *)src_fname;
    cp->dest_tuple.creds = fs->lc->creds;
    cp->dest_tuple.lc = fs->lc;
    cp->dest_tuple.is_lio = dest_is_lio;
    cp->dest_tuple.path = (char *)dest_fname;
    cp->bufsize = (bufsize <= 0) ? fs->copy_bufsize : bufsize;
    cp->slow = do_slow_copy;
    cp->enable_local = enable_local2local;
    cp->rw_hints = rw_hints;
    gop = gop_tp_op_new(fs->lc->tpc_unlimited, NULL, lio_file_copy_op, (void *)cp, NULL, 1);

    return(gop);
}

//*****************************************************************
// lio_fs_copy - Copy a file
//*****************************************************************

int lio_fs_copy(lio_fs_t *fs, lio_os_authz_local_t *ug, int src_is_lio, const char *src_fname, int dest_is_lio, const char *dest_fname, ex_off_t bufsize, int do_slow_copy, int enable_local2local, lio_segment_rw_hints_t *rw_hints)
{
    int err;

    FS_MON_OBJ_CREATE("FS_COPY: fsrc=%s src_is_lio=%d fdest=%s dest_is_lio=%d slow_copy=%d bufsize=" XOT, src_fname, src_is_lio, dest_fname, dest_is_lio, do_slow_copy, bufsize);
    err = gop_sync_exec(lio_fs_copy_gop(fs, ug, src_is_lio, src_fname, dest_is_lio, dest_fname, bufsize, do_slow_copy, enable_local2local, rw_hints));
    FS_MON_OBJ_DESTROY();
    return((err == OP_STATE_SUCCESS) ? 0 : 1);
}

//*****************************************************************
// lio_fs_truncate - Truncate the file
//*****************************************************************

int lio_fs_truncate(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, off_t new_size)
{
    lio_fd_t *fd;
    ex_off_t ts;
    int result;

    FS_MON_OBJ_CREATE("FS_TRUNCATE: fname=%s new_size=" OT, fname, new_size);
    ts = new_size;
    log_printf(15, "fname=%s adjusting size=" XOT "\n", fname, ts);

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_WRITE_IMMEDIATE)) {
        log_printf(0, "Invalid access: path=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    gop_sync_exec(lio_open_gop(fs->lc, fs->lc->creds, (char *)fname, LIO_RW_MODE, NULL, &fd, 60));
    if (fd == NULL) {
        log_printf(0, "Failed opening file!  path=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("EIO: open");
        return(-EIO);
    }

    result = 0;
    if (gop_sync_exec(lio_truncate_gop(fd, new_size)) != OP_STATE_SUCCESS) {
        log_printf(0, "Failed truncating file!  path=%s\n", fname);
        result = -EIO;
    }

    if (gop_sync_exec(lio_close_gop(fd)) != OP_STATE_SUCCESS) {
        log_printf(0, "Failed closing file!  path=%s\n", fname);
        result = -EIO;
    }

    FS_MON_OBJ_DESTROY();
    return(result);
}

//*****************************************************************
// lio_fs_ftruncate - Truncate the file associated with the FD
//*****************************************************************

int lio_fs_ftruncate(lio_fs_t *fs, lio_fd_t *fd, off_t new_size)
{
    int err;

    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null FS fd handle\n");
        return(-EBADF);
    }

    FS_MON_OBJ_CREATE("FS_FTRUNCATE: fname=%s new_size=" OT, fd->fh->fname, new_size);

    if (fd->mode & LIO_WRITE_MODE) {
        err = gop_sync_exec(lio_truncate_gop(fd, new_size));
        FS_MON_OBJ_DESTROY();
        return((err == OP_STATE_SUCCESS) ? 0 : -EIO);
    }

    FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
    return(-EACCES);
}


//*****************************************************************
// lio_fs_utimens - Sets the access and mod times in ns
//*****************************************************************

int lio_fs_utimens(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const struct timespec tv[2])
{
    char buf[1024];
    char *key;
    char *val;
    int v_size;
    ex_off_t ts;
    int err;
    osaz_attr_filter_t filter;

    key = "system.modify_attr";
    if (fs_osaz_attr_access(fs, ug, fname, key, OS_MODE_WRITE_IMMEDIATE, &filter) != 2) {
        log_printf(0, "ERROR accessing system.modify_attr fname=%s\n", fname);
        return(-EACCES);
    }

    ts = (tv) ? tv[1].tv_sec : time(NULL);
    snprintf(buf, 1024, XOT "|%s", ts, fs->id);
    val = buf;
    v_size = strlen(buf);

    err = lio_setattr(fs->lc, fs->lc->creds, (char *)fname, NULL, key, (void *)val, v_size);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating stat! fname=%s\n", fname);
        return(-EBADE);
    }

    return(0);
}

//*****************************************************************
// lio_fs_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

int lio_fs_listxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char *list, size_t size)
{
    char *buf, *key, *val;
    int bpos, bufsize, v_size, n, i, err;
    lio_os_regex_table_t *attr_regex;
    os_attr_iter_t *it;
    os_fd_t *fd;

    bpos= size;

    FS_MON_OBJ_CREATE("FS_LISTXATTR: fname=%s", fname);

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_READ_IMMEDIATE)) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** Make an iterator
    attr_regex = lio_os_path_glob2regex("user.*");
    err = gop_sync_exec(os_open_object(fs->lc->os, fs->lc->creds, (char *)fname, OS_MODE_READ_IMMEDIATE, fs->id, &fd, fs->lc->timeout));
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "ERROR: opening file: %s err=%d\n", fname, err);
        FS_MON_OBJ_DESTROY_MESSAGE("ENOENT");
        return(-ENOENT);
    }
    it = os_create_attr_iter(fs->lc->os, fs->lc->creds, fd, attr_regex, 0);
    if (it == NULL) {
        log_printf(15, "ERROR creating iterator for fname=%s\n", fname);
        FS_MON_OBJ_DESTROY_MESSAGE("ENOENT");
        return(-ENOENT);
    }

    //** Cycle through the keys
    bufsize = 10*1024;
    tbx_type_malloc_clear(buf, char, bufsize);
    val = NULL;
    bpos = 0;

    if (fs->enable_tape == 1)  { //** Add the tape attribute
        strcpy(buf, LIO_FS_TAPE_ATTR);
        bpos = strlen(buf) + 1;
    }
    while (os_next_attr(fs->lc->os, it, &key, (void **)&val, &v_size) == 0) {
        n = strlen(key);
        if ((n+bpos) > bufsize) {
            bufsize = bufsize + n + 10*1024;
            buf = realloc(buf, bufsize);
        }

        log_printf(15, "adding key=%s bpos=%d\n", key, bpos);
        for (i=0; ; i++) {
            buf[bpos] = key[i];
            bpos++;
            if (key[i] == 0) break;
        }
        free(key);

        v_size = 0;
    }

    os_destroy_attr_iter(fs->lc->os, it);
    gop_sync_exec(os_close_object(fs->lc->os, fd));
    lio_os_regex_table_destroy(attr_regex);

    if (size == 0) {
        log_printf(15, "SIZE bpos=%d buf=%s\n", bpos, buf);
    } else if ((int)size > bpos) {
        log_printf(15, "FULL bpos=%d buf=%s\n", bpos, buf);
        memcpy(list, buf, bpos);
    } else {
        log_printf(15, "ERANGE bpos=%d buf=%s\n", bpos, buf);
    }
    free(buf);

    FS_MON_OBJ_DESTROY();

    return(bpos);
}

//*****************************************************************
// lio_fs_set_tape_attr - Disburse the tape attribute
//*****************************************************************

void lio_fs_set_tape_attr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *mytape_val, int tape_size)
{
    char *val[_tape_key_size], *tape_val, *bstate, *tmp;
    int v_size[_tape_key_size];
    int n, i, fin, ex_key, err, ftype, nkeys;;
    lio_exnode_exchange_t *exp;
    lio_exnode_t *ex, *cex;

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_WRITE_IMMEDIATE)) {
        log_printf(5, "Can't access! fname=%s\n", fname);
        return;
    }

    tbx_type_malloc(tape_val, char, tape_size+1);
    memcpy(tape_val, mytape_val, tape_size);
    tape_val[tape_size] = 0;  //** Just to be safe with the string/prints routines

    log_printf(15, "fname=%s tape_size=%d\n", fname, tape_size);
    log_printf(15, "Tape attribute follows:\n%s\n", tape_val);

    ftype = lio_fs_exists(fs, fname);
    if (ftype <= 0) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        return;
    }

    nkeys = (ftype & OS_OBJECT_SYMLINK_FLAG) ? 1 : _tape_key_size;

    //** The 1st key should be n_keys
    tmp = tbx_stk_string_token(tape_val, "=\n", &bstate, &fin);
    if (strcmp(tmp, "n_keys") != 0) { //*
        log_printf(0, "ERROR parsing tape attribute! Missing n_keys! fname=%s\n", fname);
        log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
        free(tape_val);
        return;
    }

    n = -1;
    sscanf(tbx_stk_string_token(NULL, "=\n", &bstate, &fin), "%d", &n);
    log_printf(15, "fname=%s n=%d nkeys=%d ftype=%d\n", fname, n, nkeys, ftype);
    if (n != nkeys) {
        log_printf(0, "ERROR parsing n_keys size fname=%s\n", fname);
        log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
        free(tape_val);
        return;
    }

    log_printf(15, "nkeys=%d fname=%s ftype=%d\n", nkeys, fname, ftype);

    //** Set all of them to 0 cause the size is used to see if the key was loaded
    for (i=0; i<_tape_key_size; i++) {
        v_size[i] = 0;
    }

    //** Parse the sizes
    for (i=0; i<nkeys; i++) {
        tmp = tbx_stk_string_token(NULL, "=\n", &bstate, &fin);
        if (strcmp(tmp, _tape_keys[i]) == 0) {
            sscanf(tbx_stk_string_token(NULL, "=\n", &bstate, &fin), "%d", &(v_size[i]));
            if (v_size[i] < 0) {
                log_printf(0, "ERROR parsing key=%s size=%d fname=%s\n", tmp, v_size[i], fname);
                log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
                free(tape_val);
                return;
            }
        } else {
            log_printf(0, "ERROR Missing key=%s\n", _tape_keys[i]);
            log_printf(0, "Tape attribute follows:\n%s\n", mytape_val);
            free(tape_val);
            return;
        }
    }

    //** Split out the attributes
    n = 0;
    for (i=0; i<nkeys; i++) {
        val[i] = NULL;
        if (v_size[i] > 0) {
            tbx_type_malloc(val[i], char, v_size[i]+1);
            memcpy(val[i], &(bstate[n]), v_size[i]);
            val[i][v_size[i]] = 0;
            n = n + v_size[i];
            log_printf(15, "fname=%s key=%s val=%s\n", fname, _tape_keys[i], val[i]);
        }
    }

    //** Just need to process the exnode
    ex_key = 1;  //** tape_key index for exnode
    if (v_size[ex_key] > 0) {
        //** If this has a caching segment we need to disable it from being adding
        //** to the global cache table cause there could be multiple copies of the
        //** same segment being serialized/deserialized.
        //** Deserialize it
        exp = lio_exnode_exchange_text_parse(val[ex_key]);
        ex = lio_exnode_create();
        err = lio_exnode_deserialize(ex, exp, fs->lc->ess_nocache);
        exnode_exchange_free(exp);
        val[ex_key] = NULL;

        if (err != 0) {
            log_printf(1, "ERROR parsing parent exnode fname=%s\n", fname);
            lio_exnode_exchange_destroy(exp);
            lio_exnode_destroy(ex);
        }

        //** Execute the clone operation
        err = gop_sync_exec(lio_exnode_clone_gop(fs->lc->tpc_unlimited, ex, fs->lc->da, &cex, NULL, CLONE_STRUCTURE, fs->lc->timeout));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR cloning parent fname=%s\n", fname);
        }

        //** Serialize it for storage
        lio_exnode_serialize(cex, exp);
        val[ex_key] = exp->text.text;
        v_size[ex_key] = strlen(val[ex_key]);
        exp->text.text = NULL;
        lio_exnode_exchange_destroy(exp);
        lio_exnode_destroy(ex);
        lio_exnode_destroy(cex);
    }

    //** Store them
    err = lio_multiple_setattr_op(fs->lc, fs->lc->creds, (char *)fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating exnode! fname=%s\n", fname);
    }

    //** Clean up
    free(tape_val);
    for (i=0; i<nkeys; i++) {
        if (val[i] != NULL) free(val[i]);
    }

    return;
}


//*****************************************************************
// lio_fs_get_tape_attr - Retreives the tape attribute
//*****************************************************************

void lio_fs_get_tape_attr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char **tape_val, int *tape_size)
{
    char *val[_tape_key_size];
    int v_size[_tape_key_size];
    int n, i, j, used, ftype, nkeys;
    int hmax= 1024;
    char *buffer, header[hmax];

    *tape_val = NULL;
    *tape_size = 0;

    log_printf(15, "START fname=%s\n", fname);

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_READ_IMMEDIATE)) {
        log_printf(5, "Can't access. fname=%s\n",fname);
        return;
    }

    ftype = lio_fs_exists(fs, fname);
    if (ftype <= 0) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        return;
    }

    for (i=0; i<_tape_key_size; i++) {
        val[i] = NULL;
        v_size[i] = -fs->lc->max_attr;
    }

    log_printf(15, "fname=%s ftype=%d\n", fname, ftype);
    nkeys = (ftype & OS_OBJECT_SYMLINK_FLAG) ? 1 : _tape_key_size;
    i = lio_get_multiple_attrs(fs->lc, fs->lc->creds, fname, NULL, _tape_keys, (void **)val, v_size, nkeys);
    if (i != OP_STATE_SUCCESS) {
        log_printf(15, "Failed retrieving file info!  path=%s\n", fname);
        return;
    }

    //** Figure out how much space we need
    n = 0;
    used = 0;
    tbx_append_printf(header, &used, hmax, "n_keys=%d\n", nkeys);
    for (i=0; i<nkeys; i++) {
        j = (v_size[i] > 0) ? v_size[i] : 0;
        n = n + 1 + j;
        tbx_append_printf(header, &used, hmax, "%s=%d\n", _tape_keys[i], j);
    }

    //** Copy all the data into the buffer;
    n = n + used;
    tbx_type_malloc_clear(buffer, char, n);
    n = used;
    memcpy(buffer, header, used);
    for (i=0; i<nkeys; i++) {
        if (v_size[i] > 0) {
            memcpy(&(buffer[n]), val[i], v_size[i]);
            n = n + v_size[i];
            free(val[i]);
        }
    }

    log_printf(15, "END fname=%s\n", fname);

    *tape_val = buffer;
    *tape_size = n;
    return;
}

//*****************************************************************
// lio_fs_getxattr - Gets an extended attribute
//*****************************************************************

int lio_fs_getxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name, char *buf, size_t size)
{
    osaz_attr_filter_t filter;
    fs_open_file_t *fop;
    os_fd_t *ofd;
    lio_file_handle_t *fh;
    char *val[2];
    char *attrs[2];
    int v_size[2], err, ftype, na;
    ex_id_t ino;
    uid_t uid;
    gid_t gid;
    mode_t mode;
    gop_op_status_t status;
    int n_readers, n_writers;

    v_size[0] = size;
    FS_MON_OBJ_CREATE("FS_GETXATTR: fname=%s aname=%s", fname, name);

    if (fs->enable_osaz_acl_mappings) {
        if (strcmp("system.posix_acl_access", name) == 0) {
            ftype = lio_fs_exists(fs, (char *)fname);
            if (ftype <= 0) {
                log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
                FS_MON_OBJ_DESTROY_MESSAGE("ENODATA");
                return(-ENODATA);
            }
            err = osaz_posix_acl(fs->osaz, fs->lc->creds, fname, ftype, buf, size, &uid, &gid, &mode);
            FS_MON_OBJ_DESTROY();
            return(err);
        }
    }

    //** See if this are always empty attrs
    if (fs->enable_security_attr_checks == 0) {
        if (strncmp("system.posix_", name, 17) == 0) {
            if ((strcmp("access", name + 17) == 0) || (strcmp("default", name + 17) == 0)) {
                FS_MON_OBJ_DESTROY_MESSAGE("ENODATA");
                return(-ENODATA);
            }
       } else if (strcmp("security.selinux", name) == 0) {
          FS_MON_OBJ_DESTROY_MESSAGE("ENODATA");
          return(-ENODATA);
       }
    }

    na = 1;
    v_size[0] = (size == 0) ? -fs->lc->max_attr : -(int)size;
    attrs[0] = (char *)name;
    val[0] = NULL;
    if ((fs->enable_tape == 1) && (strcmp(name, LIO_FS_TAPE_ATTR) == 0)) {  //** Want the tape backup attr
        //** Make sure we can access it
        if (fs_osaz_attr_access(fs, ug, fname, name, OS_MODE_READ_IMMEDIATE, &filter)) {
            FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
            return(-EACCES);
        }
        lio_fs_get_tape_attr(fs, ug, fname, &val[0], &v_size[0]);
        fs_osaz_attr_filter_apply(fs, name, LIO_READ_MODE, &val[0], &v_size[0], filter);
    } else {
        //** Short circuit the Linux Security ACLs we don't support
        if ((strcmp(name, "security.capability") == 0) || (strcmp(name, "security.selinux") == 0)) {
            FS_MON_OBJ_DESTROY_MESSAGE("ENODATA");
            return(-ENODATA);
        }

        //** Make sure we can access it
        if (!fs_osaz_attr_access(fs, ug, fname, name, OS_MODE_READ_IMMEDIATE, &filter)) {
            FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
            return(-EACCES);
        }

        //** See if we throw other errors for files  with unrecoverable blocks
        if (fs->xattr_error_for_hard_errors) {
            //** 1st check if the file is already open. If so snag the hard_errors from there
            fs_lock(fs);
            fop = apr_hash_get(fs->open_files, fname, APR_HASH_KEY_STRING);
            if (fop != NULL) {  //** The file is open so check here 1st
                ino = fop->sid;
                fs_unlock(fs);

                lio_lock(fs->lc);
                fh = _lio_get_file_handle(fs->lc, ino);
                if (fh) {
                    status = gop_sync_exec_status(segment_inspect(fh->seg, fs->lc->da, lio_ifd, INSPECT_HARD_ERRORS, 0, NULL, 1));
                    if (status.error_code > 0) { //** Got a hard error so kick out
                        lio_unlock(fs->lc);
                        FS_MON_OBJ_DESTROY_MESSAGE("EHARD=%d", fs->xattr_error_for_hard_errors);
                        return(-fs->xattr_error_for_hard_errors);
                    }
                }
                lio_unlock(fs->lc);
            }
            fs_unlock(fs);

            //** No errors from a possibly open file so check the attribute
            na = 2;
            attrs[1] = "system.hard_errors";
            val[1] = NULL;
            v_size[1] = -fs->lc->max_attr;
        }

        //** If needed get a read lock
        ofd = NULL;
        if ((fs->enable_rw_locks) && (fs->rw_lock_attr_string) && (regexec(&(fs->rw_lock_attr_regex), name, 0, NULL, 0) == 0)) {
            if (fs->enable_fuse_hacks) {  //** See if we need to hack around FUSE oddities on order of operations
                if (lio_open_file_check(fs->lc, fname, &n_readers, &n_writers)) { //** We already have it open in some form with a lock
                    goto already_have_a_lock;
                }
            }
            err = gop_sync_exec(os_open_object(fs->lc->os, fs->lc->creds, (char *)fname, OS_MODE_READ_BLOCKING, fs->id, &ofd, fs->rw_locks_max_wait));
            if (err != OP_STATE_SUCCESS) {
                log_printf(15, "ERROR opening os object fname=%s attr=%s\n", fname, name);
                FS_MON_OBJ_DESTROY_MESSAGE("ENOLCK");
                return(-ENOLCK);
            }
        }

already_have_a_lock:
        err = lio_get_multiple_attrs(fs->lc, fs->lc->creds, (char *)fname, NULL, attrs, (void **)val, v_size, na);
        if (err != OP_STATE_SUCCESS) {
            FS_MON_OBJ_DESTROY_MESSAGE("ENODATA");
            return(-ENODATA);
        }
        if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed

        if ((na>1) && (v_size[1] > 0)) { //** We have hard errors so throw an error
            free(val[1]);
            if (v_size[0] > 0) free(val[0]);
            FS_MON_OBJ_DESTROY_MESSAGE("EHARD=%d", fs->xattr_error_for_hard_errors);
            return(-fs->xattr_error_for_hard_errors);
        }
        fs_osaz_attr_filter_apply(fs, name, OS_MODE_READ_IMMEDIATE, &val[0], &v_size[0], filter);
    }

    err = 0;
    if (v_size[0] < 0) {
        v_size[0] = 0;  //** No attribute
        err = -ENODATA;
    }

    if (size == 0) {
        log_printf(1, "SIZE bpos=%d buf=%.*s\n", v_size[0], v_size[0], val[0]);
    } else if ((int)size >= v_size[0]) {
        log_printf(1, "FULL bpos=%d buf=%.*s\n",v_size[0], v_size[0], val[0]);
        memcpy(buf, val[0], v_size[0]);
    } else {
        log_printf(1, "ERANGE bpos=%d buf=%.*s\n", v_size[0], v_size[0], val[0]);
    }

    FS_MON_OBJ_DESTROY();

    if (val[0] != NULL) free(val[0]);
    return((v_size[0] == 0) ? err : v_size[0]);
}

//*****************************************************************
// lio_fs_setxattr - Sets a extended attribute
//*****************************************************************

int lio_fs_setxattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name, const char *fval, size_t size, int flags)
{
    osaz_attr_filter_t filter;
    char *val;
    os_fd_t *ofd;
    int v_size, err;
    int n_readers, n_writers;

    v_size= size;

    if (strcmp("system.posix_acl_access", name) == 0) return(0);  //** We don't allow setting that now
    if (strcmp("system.exnode", name) == 0) return(0);  //** We don't allow setting the exnode from FUSE
    if (strcmp("system.exnode.data", name) == 0) return(0);  //** We don't allow setting the exnode data from FUSE either

    FS_MON_OBJ_CREATE("FS_SETXATTR: fname=%s aname=%s", fname, name);

    //** If needed get a read lock
    ofd = NULL;
    if ((fs->enable_rw_locks) && (fs->rw_lock_attr_string) && (regexec(&(fs->rw_lock_attr_regex), name, 0, NULL, 0) == 0)) {
        if (fs->enable_fuse_hacks) {  //** See if we need to hack around FUSE oddities on order of operations
            if (lio_open_file_check(fs->lc, fname, &n_readers, &n_writers)) { //** We already have it open in some form with a lock
                goto already_have_a_lock;
            }
        }

        err = gop_sync_exec(os_open_object(fs->lc->os, fs->lc->creds, (char *)fname, OS_MODE_WRITE_BLOCKING, fs->id, &ofd, fs->rw_locks_max_wait));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR opening os object fname=%s attr=%s\n", fname, name);
            FS_MON_OBJ_DESTROY_MESSAGE("ENOLCK");
            return(-ENOLCK);
        }
    }

already_have_a_lock:
    if (flags != 0) { //** Got an XATTR_CREATE/XATTR_REPLACE
        v_size = 0;
        val = NULL;
        err = lio_getattr(fs->lc, fs->lc->creds, (char *)fname, NULL, (char *)name, (void **)&val, &v_size);
        if (flags == XATTR_CREATE) {
            if (err == OP_STATE_SUCCESS) {
                if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
                FS_MON_OBJ_DESTROY_MESSAGE("EEXIST");
                return(-EEXIST);
            }
        } else if (flags == XATTR_REPLACE) {
            if (err != OP_STATE_SUCCESS) {
                if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
                FS_MON_OBJ_DESTROY_MESSAGE("ENOATTR");
                return(-ENOATTR);
            }
        }
    }

    v_size = size;
    if ((fs->enable_tape == 1) && (strcmp(name, LIO_FS_TAPE_ATTR) == 0)) {  //** Got the tape attribute
        //** Make sure we can access it
        if (fs_osaz_attr_access(fs, ug, fname, name, OS_MODE_WRITE_IMMEDIATE, &filter)) {
            if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
            FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
            return(-EACCES);
        }
        lio_fs_set_tape_attr(fs, ug, fname, fval, v_size);
        return(0);
    } else {
        //** Make sure we can access it
        if (!fs_osaz_attr_access(fs, ug, fname, name, OS_MODE_WRITE_IMMEDIATE, &filter)) {
            if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
            FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
            return(-EACCES);
        }

        err = lio_setattr(fs->lc, fs->lc->creds, (char *)fname, NULL, (char *)name, (void *)fval, v_size);
        if (err != OP_STATE_SUCCESS) {
            if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
            FS_MON_OBJ_DESTROY_MESSAGE("ENOENT");
            return(-ENOENT);
        }
    }

    if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed

    FS_MON_OBJ_DESTROY();

    return(0);
}

//*****************************************************************
// lio_fs_removexattr - Removes an extended attribute
//*****************************************************************

int lio_fs_removexattr(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, const char *name)
{
    int v_size, err;
    int n_readers, n_writers;
    os_fd_t *ofd;

    FS_MON_OBJ_CREATE("FS_SETXATTR: fname=%s aname=%s", fname, name);

    //** We don't allow setting these attributes from the FS
    if ((strcmp("system.posix_acl_access", name) == 0) ||
        (strcmp("system.exnode", name) == 0) ||
        (strcmp("system.exnode.data", name) == 0)) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }


    if ((fs->enable_tape == 1) && (strcmp(name, LIO_FS_TAPE_ATTR) == 0)) {
        FS_MON_OBJ_DESTROY();
        return(0);
    }

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_WRITE_IMMEDIATE)) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** If needed get a read lock
    ofd = NULL;
    if ((fs->enable_rw_locks) && (fs->rw_lock_attr_string) && (regexec(&(fs->rw_lock_attr_regex), name, 0, NULL, 0) == 0)) {
        if (fs->enable_fuse_hacks) {  //** See if we need to hack around FUSE oddities on order of operations
            if (lio_open_file_check(fs->lc, fname, &n_readers, &n_writers)) { //** We already have it open in some form with a lock
                goto already_have_a_lock;
            }
        }

        err = gop_sync_exec(os_open_object(fs->lc->os, fs->lc->creds, (char *)fname, OS_MODE_WRITE_BLOCKING, fs->id, &ofd, fs->rw_locks_max_wait));
        if (err != OP_STATE_SUCCESS) {
            log_printf(15, "ERROR opening os object fname=%s attr=%s\n", fname, name);
            FS_MON_OBJ_DESTROY_MESSAGE("ENOLCK");
            return(-ENOLCK);
        }
    }

already_have_a_lock:
    v_size = -1;
    err = lio_setattr(fs->lc, fs->lc->creds, (char *)fname, NULL, (char *)name, NULL, v_size);
    if (err != OP_STATE_SUCCESS) {
        if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed
        FS_MON_OBJ_DESTROY_MESSAGE("ENOENT");
        return(-ENOENT);
    }

    if (ofd) gop_sync_exec(os_close_object(fs->lc->os, ofd));  //** Close it if needed

    FS_MON_OBJ_DESTROY();

    return(0);
}

//*************************************************************************
// lio_fs_hardlink - Creates a hardlink to an existing file
//*************************************************************************

int lio_fs_hardlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *oldname, const char *newname)
{
    int err;

    FS_MON_OBJ_CREATE("FS_HARDLINK: oldname=%s newname=%s", oldname, newname);

    //** Make sure we can access it
    if (!(fs_osaz_object_remove(fs, ug, oldname) && fs_osaz_object_create(fs, ug, newname))) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** Now do the hard link
    err = gop_sync_exec(lio_link_gop(fs->lc, fs->lc->creds, 0, (char *)oldname, (char *)newname, fs->id));
    if (err != OP_STATE_SUCCESS) {
        FS_MON_OBJ_DESTROY_MESSAGE("EIO");
        return(-EIO);
    }

    FS_MON_OBJ_DESTROY();

    return(0);
}

//*****************************************************************
//  lio_fs_readlink - Reads the object symlink
//*****************************************************************

int lio_fs_readlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, char *buf, size_t bsize)
{
    int v_size, err, i;
    char *val;

    log_printf(15, "fname=%s\n", fname);

    FS_MON_OBJ_CREATE("FS_READLINK: fname=%s", fname);

    //** Make sure we can access it
    if (!fs_osaz_object_access(fs, ug, fname, OS_MODE_READ_IMMEDIATE)) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    v_size = -fs->lc->max_attr;
    val = NULL;
    err = lio_getattr(fs->lc, fs->lc->creds, (char *)fname, NULL, "os.link", (void **)&val, &v_size);
    if (err != OP_STATE_SUCCESS) {
        buf[0] = 0;
        FS_MON_OBJ_DESTROY_MESSAGE("EIO");
        return(-EIO);
    } else if (v_size <= 0) {
        buf[0] = 0;
        FS_MON_OBJ_DESTROY_MESSAGE("EINVAL");
        return(-EINVAL);
    }

    if (val) {
        snprintf(buf, bsize, "%s", (char *)val);
        free(val);
    } else {
        buf[0] = 0;
    }
    buf[bsize] = 0;

    i=bsize;
    log_printf(15, "fname=%s bsize=%d link=%s\n", fname, i, buf);
    tbx_log_flush();

    FS_MON_OBJ_DESTROY();

    return(0);
}

//*****************************************************************
//  lio_fs_symlink - Makes a symbolic link
//*****************************************************************

int lio_fs_symlink(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *link, const char *newname)
{
    int err;

    FS_MON_OBJ_CREATE("FS_SYMLINK: link=%s newname=%s", link, newname);

    //** Make sure we can access it
    if (!fs_osaz_object_create(fs, ug, newname)) {
        FS_MON_OBJ_DESTROY_MESSAGE("EACCES");
        return(-EACCES);
    }

    //** Now do the sym link
    err = gop_sync_exec(lio_link_gop(fs->lc, fs->lc->creds, 1, (char *)link, (char *)newname, fs->id));
    if (err != OP_STATE_SUCCESS) {
        if (lio_fs_exists(fs, newname) != 0) {
            FS_MON_OBJ_DESTROY_MESSAGE("EEXIST");
            return(-EEXIST);
        }

        FS_MON_OBJ_DESTROY_MESSAGE("EIO");
        return(-EIO);
    }

    FS_MON_OBJ_DESTROY();

    return(0);
}

//*************************************************************************
// lio_fs_statvfs - Returns the files system size
//*************************************************************************

int lio_fs_statvfs(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct statvfs *sfs)
{
    lio_rs_space_t space;
    char *config;

    FS_MON_OBJ_CREATE("FS_STATVFS");

    memset(sfs, 0, sizeof(struct statvfs));

    //** Get the config
    config = rs_get_rid_config(fs->lc->rs);

    //** And parse it
    space = rs_space(config);
    free(config);

    sfs->f_bsize = 4096;
    sfs->f_blocks = space.total_up / sfs->f_bsize;;
    sfs->f_bfree = space.free_up / sfs->f_bsize;
    sfs->f_bavail = sfs->f_bfree;
    sfs->f_files = 1;
    sfs->f_ffree = 10*1024*1024;
    sfs->f_namemax = 4096 - 100;

    FS_MON_OBJ_DESTROY();
    return(0);
}

//*************************************************************************
// lio_fs_statfs - Returns the files system size
//*************************************************************************

int lio_fs_statfs(lio_fs_t *fs, lio_os_authz_local_t *ug, const char *fname, struct statfs *sfs)
{
    lio_rs_space_t space;
    char *config;

    FS_MON_OBJ_CREATE("FS_STATFS");

    memset(sfs, 0, sizeof(struct statvfs));

    //** Get the config
    config = rs_get_rid_config(fs->lc->rs);

    //** And parse it
    space = rs_space(config);
    free(config);

    sfs->f_bsize = 4096;
    sfs->f_blocks = space.total_up / sfs->f_bsize;;
    sfs->f_bfree = space.free_up / sfs->f_bsize;
    sfs->f_bavail = sfs->f_bfree;
    sfs->f_files = 1;
    sfs->f_ffree = 10*1024*1024;
    sfs->f_frsize = 6*16*1024;
    sfs->f_namelen = 4096 - 100;

    FS_MON_OBJ_DESTROY();
    return(0);
}

//*************************************************************************
// lio_fs_info_fn - Signal handler to dump info
//*************************************************************************

void lio_fs_info_fn(void *arg, FILE *fd)
{
    lio_fs_t *fs = arg;
    int n;
    char ppbuf[100];

    fprintf(fd, "---------------------------------- FS config start --------------------------------------------\n");
    fprintf(fd, "[%s]\n", fs->fs_section);
    fprintf(fd, "authz = %s\n", fs->authz_section);
    fprintf(fd, "enable_tape = %d\n", fs->enable_tape);
    fprintf(fd, "enable_osaz_acl_mappings = %d\n", fs->enable_osaz_acl_mappings);
    fprintf(fd, "enable_osaz_secondary_gids = %d\n", fs->enable_osaz_secondary_gids);
    fprintf(fd, "enable_security_attr_checks = %d\n", fs->enable_security_attr_checks);
    fprintf(fd, "enable_fuse_hacks = %d\n", fs->enable_fuse_hacks);
    fprintf(fd, "enable_rw_locks = %d\n", fs->enable_rw_locks);
    if (fs->enable_rw_locks) fprintf(fd, "rw_locks_max_wait = %d\n", fs->rw_locks_max_wait);
    if (fs->rw_lock_attr_string) fprintf(fd, "rw_lock_attrs = %s\n", fs->rw_lock_attr_string);
    fprintf(fd, "enable_fifo = %d\n", fs->enable_fifo);
    fprintf(fd, "enable_socket = %d\n", fs->enable_socket);
    fprintf(fd, "xattr_error_for_hard_errors = %d\n", fs->xattr_error_for_hard_errors);
    fprintf(fd, "ug_mode = %s\n", _ug_mode_string[fs->ug_mode]);
    fprintf(fd, "n_merge = %d\n", fs->n_merge);
    fprintf(fd, "copy_bufsize = %s\n", tbx_stk_pretty_print_double_with_scale(1024, fs->copy_bufsize, ppbuf));

    n = tbx_atomic_get(fs->write_cmds_inflight);  fprintf(fd, "#write_cmds_inflight = %d\n", n);
    n = tbx_atomic_get(fs->write_bytes_inflight); fprintf(fd, "#write_bytes_inflight = %s\n", tbx_stk_pretty_print_double_with_scale(1024, n, ppbuf));
    n = tbx_atomic_get(fs->read_cmds_inflight);   fprintf(fd, "#read_cmds_inflight = %d\n", n);
    n = tbx_atomic_get(fs->read_bytes_inflight);  fprintf(fd, "#read_bytes_inflight = %s\n", tbx_stk_pretty_print_double_with_scale(1024, n, ppbuf));
    fprintf(fd, "\n");

    //** Print the AuthZ configuration
    osaz_print_running_config(fs->osaz, fd, 1);
}

//*************************************************************************
//  lio_fs_create - Creates a file system handle for use.
//     NOTE: This assumes that lio_init() has already been called
//
//     fd          - INI file to use for getting options.
//     fs_section  - Section from INI for parsing. Defaults to "fs" if NULL
//     lc          - LIO context. if NULL then the global lio_gc is used
//
//*************************************************************************

lio_fs_t *lio_fs_create(tbx_inip_file_t *fd, char *fs_section, lio_config_t *lc, uid_t uid, gid_t gid)
{
    lio_fs_t *fs;
    char *atype;
    osaz_create_t *osaz_create;

    tbx_type_malloc_clear(fs, lio_fs_t, 1);

    fs->lc = (lc) ? lc : lio_gc;
    fs->fs_section = (fs_section) ? strdup(fs_section) : strdup("fs");
log_printf(0, "fs->fs_section=%s fs=>lc=%p\n", fs->fs_section, fs->lc);

    fs->enable_tape = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "enable_tape", 0);
    fs->enable_osaz_acl_mappings = tbx_inip_get_integer(fd, fs->fs_section, "enable_osaz_acl_mappings", 0);
    fs->enable_osaz_secondary_gids = tbx_inip_get_integer(fd, fs->fs_section, "enable_osaz_secondary_gids", 0);
    fs->enable_security_attr_checks = tbx_inip_get_integer(fd, fs->fs_section, "enable_security_attr_checks", 0);
    fs->enable_fifo = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "enable_fifo", 0);
    fs->enable_socket = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "enable_socket", 0);
    fs->xattr_error_for_hard_errors = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "xattr_error_for_hard_errors", 0);
    atype = tbx_inip_get_string(fd, fs->fs_section, "ug_mode", _ug_mode_string[UG_GLOBAL]);
    fs->ug_mode = UG_GLOBAL;
    if (strcmp(atype, _ug_mode_string[UG_UID]) == 0) {
        fs->ug_mode = UG_UID;
    } else if (strcmp(atype, _ug_mode_string[UG_FSUID]) == 0) {
        fs->ug_mode = UG_FSUID;
    }
    free(atype);
    fs->_inode_key_size = (fs->enable_security_attr_checks) ? _inode_key_size_security : _inode_key_size_core;

    fs->enable_fuse_hacks = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "enable_fuse_hacks", 0);
    fs->enable_rw_locks = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "enable_rw_locks", 0);
    fs->os_read_mode = OS_MODE_READ_IMMEDIATE;
    fs->os_write_mode = OS_MODE_WRITE_IMMEDIATE;
    fs->rw_locks_max_wait = 60;     //** Default is to wait 60s if not using locks. It should be immediate in this case
    if (fs->enable_rw_locks) {
        fs->rw_locks_max_wait = tbx_inip_get_integer(fs->lc->ifd, fs->fs_section, "rw_locks_max_wait", 600);
        fs->os_read_mode = OS_MODE_READ_BLOCKING;
        fs->os_write_mode = OS_MODE_WRITE_BLOCKING;
    }

    //** See if there are any attributes to not cache (and optionally protect with RW locks if enabled
    fs->rw_lock_attr_string = tbx_inip_get_string(fd, fs->fs_section, "rw_lock_attrs", NULL);
    if (fs->rw_lock_attr_string) {
        if (lio_os_globregex_parse(&(fs->rw_lock_attr_regex), fs->rw_lock_attr_string) != 0) {
            log_printf(0, "ERROR: Failed parsing rw_lock_attr glob/regex! string=%s\n", fs->rw_lock_attr_string);
            fprintf(stderr, "ERROR: Failed parsing rw_lock_attr glob/regex! string=%s\n", fs->rw_lock_attr_string);
            fflush(stderr);
            abort();
        }
    }

    fs->n_merge = tbx_inip_get_integer(fd, fs->fs_section, "n_merge", 0);
    fs->copy_bufsize = tbx_inip_get_integer(fd, fs->fs_section, "copy_bufsize", 10*1024*1024);

    apr_pool_create(&(fs->mpool), NULL);
    apr_thread_mutex_create(&(fs->lock), APR_THREAD_MUTEX_DEFAULT, fs->mpool);
    fs->open_files = apr_hash_make(fs->mpool);

    //** Load the OS AuthZ framework
    fs->authz_section = tbx_inip_get_string(fd, fs->fs_section, "authz", OSAZ_TYPE_FAKE);
    atype = tbx_inip_get_string(fs->lc->ifd, fs->authz_section, "type", OSAZ_TYPE_FAKE);
    osaz_create = lio_lookup_service(fs->lc->ess, OSAZ_AVAILABLE, atype);
    fs->osaz = (*osaz_create)(fs->lc->ess, fd, fs->authz_section, NULL);
    free(atype);

    //** Get the default host ID for opens
    fs->id = lio_lookup_service(fs->lc->ess, ESS_RUNNING, ESS_ONGOING_HOST_ID);

    //** Make the AuthZ hint.  We really only care about the uid/gids which is a byproduct of setting things up
    lio_fs_fill_os_authz_local(fs, &(fs->ug), uid, gid);
    osaz_ug_hint_release(fs->osaz, fs->lc->creds, &(fs->ug)); //** Got ahead and free the hint since the ugi/gids are in the actual ug struct

    tbx_siginfo_handler_add(SIGUSR1, lio_fs_info_fn, fs);

    log_printf(15, "END\n");
    return(fs);
}

//*************************************************************************
// lio_fs_destroy - Destroy a file system object
//*************************************************************************

void lio_fs_destroy(lio_fs_t *fs)
{
    fs_open_file_t *fop;
    apr_hash_index_t *hi;

    log_printf(0, "shutting down\n");
    tbx_log_flush();

    tbx_siginfo_handler_remove(SIGUSR1, lio_fs_info_fn, fs);

    //** Cleanup the open file hash
    for (hi=apr_hash_first(fs->mpool, fs->open_files); hi; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&fop);
        log_printf(0, "ERROR: LFS_OPEN_FILE: fname=%s sid= " XIDT " ref=%d remove=%d\n", fop->fname, fop->sid, fop->ref_count, fop->remove_on_close);
//        lio_fs_close(fs, fop->fd);
        free(fop->fname);
        free(fop);
    }

    //** Release our global hint if used
    osaz_ug_hint_release(fs->osaz, fs->lc->creds, &(fs->ug));

    //** Destroy the OSAZ
    osaz_destroy(fs->osaz);

    //** Clean up everything else
    if (fs->authz_section) free(fs->authz_section);
    if (fs->fs_section) free(fs->fs_section);
    if (fs->rw_lock_attr_string) {
        regfree(&(fs->rw_lock_attr_regex));
        free(fs->rw_lock_attr_string);
    }

    apr_thread_mutex_destroy(fs->lock);
    apr_pool_destroy(fs->mpool);

    if (fs->rw_hints) free(fs->rw_hints);
    free(fs);
}
