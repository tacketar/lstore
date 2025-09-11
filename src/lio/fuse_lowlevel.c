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

#include <strings.h>
#include <lio/lio.h>
#include <lio/lio_fuse.h>
#include <tbx/append_printf.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#include "lio_fuse.h"

typedef struct {
    char *dentry;
    struct fuse_entry_param fe;
    int ftype;
} ll_dir_entry_t;

typedef struct {
    lio_fuse_t *lfs;
    lio_fs_dir_iter_t *fsit;
    tbx_stack_t *stack;
    int state;
    ex_id_t parent;
} ll_dir_iter_t;

lio_fuse_t *glfs = NULL;  //** Global LFS handle
#define _get_lfs_context() glfs

//********************************************************
// _ll_get_fuse_ug - Gets the UID/GID info
//********************************************************

lio_os_authz_local_t *_ll_get_fuse_ug(lio_fuse_t *lfs, lio_os_authz_local_t *ug, const struct fuse_ctx *fc)
{
    if (lfs->fs_checks_acls == 0) {
        ug->valid_guids = 0;  //** FIXME
        ug->uid = fc->uid; ug->gid[0] = fc->gid; ug->n_gid = 1;
    } else {
        lio_fs_fill_os_authz_local(lfs->fs, ug, fc->uid, fc->gid);
    }
    return(ug);
}


//********************************************************
//  ll_inode2path_from_ilut - Converts an inode to a path only used the iLUT
//     The path should be allocated to OS_PATH_MAX
//     On success 0 is returned otherwise 1 if the inode can't be located
//********************************************************

int ll_inode2path_from_ilut(lio_fuse_t *lfs, ex_id_t inode_target, char *path, int *ftype_target)
{
    char *dentry[OS_PATH_MAX];
    ex_id_t inode_curr, parent;
    int f, len, i, err, n_dirs;
    char *de;

    inode_curr = inode_target;
    i = 0;
    *ftype_target = 0;
    while ((err = os_inode_lut_get(lfs->ilut, 1, inode_curr, &parent, &f, &len, &de)) == 0) {
        if (*ftype_target == 0) *ftype_target = f;
        dentry[i] = de;
        inode_curr = parent;
        i++;

        if (parent == 0) break;  //** We've reached the root directory
        if (i == OS_PATH_MAX-1) {  //** We have a loop so flag it
            fprintf(stderr, "ERROR: Hit recursion max! inode_target=" XIDT " curr_parent=" XIDT " cuff_ftype=%d curr_dentry=%s\n", inode_target, parent, f, de);
            break;
        }
    }

    n_dirs = i;

    if (err == 0) {
        //** Got a valid set of inodes so convert it to a path
        os_inode_lookup2path(path, n_dirs, NULL, dentry);
    }

    //** Cleanup up the dentry
    for (i=0; i<n_dirs; i++) {
        if (dentry[i]) free(dentry[i]);
    }

    return(err);
}

//********************************************************
//  ll_inode2path_from_os - Converts an inode to a path using the OS
//     The path should be allocated to OS_PATH_MAX
//     On success 0 is returned otherwise 1 if the inode can't be located
//********************************************************

int ll_inode2path_from_os(lio_fuse_t *lfs, ex_id_t inode_target, char *path, int *ftype_target)
{
    int err, v_size, i, len, n, used, ftype, k;
    ex_id_t inode, parent;
    int64_t v;
    char attr[1024];
    char *blob, *de;

log_printf(0, "QWERT: START inode=" XIDT "\n", inode_target);
    //** We are going to look it up in binary format
    snprintf(attr, sizeof(attr), "os.inode.lookup.binary." XIDT, inode_target);
    v_size = -lfs->lc->max_attr;
    blob = NULL;
    err = lio_getattr(lfs->lc, lfs->lc->creds, "/", NULL, attr, (void **)&blob, &v_size);

    //** Kick out if nothing
    if (err != OP_STATE_SUCCESS) return(1);

    //** We got a valid object so parse and update the iLUT
    used = tbx_zigzag_decode((uint8_t *)blob, v_size, &v); n = v;

    ftype = 0;
    path[0] = '\0';
    k = 0;
    parent = 0;  //** This is the root inode who's parent is always 0
    for (i=0; i<n; i++) {
        used += tbx_zigzag_decode((uint8_t *)blob + used, v_size, (int64_t *)&inode);
        used += tbx_zigzag_decode((uint8_t *)blob + used, v_size, &v); ftype = v;
        used += tbx_zigzag_decode((uint8_t *)blob + used, v_size, &v); len = v;
        de = blob + used;
        used += len;
//log_printf(0, "QWERT: n=%d i=%d inode=" XIDT " ftype=%d len=%d de=%s path=%s\n", n, i, inode, ftype, len, de, path);
        os_inode_lut_put(lfs->ilut, 1, inode, parent, ftype, len-1, de);
        parent = inode;
        if (i > 0) tbx_append_printf(path, &k, OS_PATH_MAX, "/%s", de);
//        if (i == 0) {
//            tbx_append_printf(path, &k, OS_PATH_MAX, "/");
//        } else {
//            tbx_append_printf(path, &k, OS_PATH_MAX, "/%s", de);
//        }
    }

    if (n == 1) sprintf(path, "/");

log_printf(0, "QWERT: END inode=" XIDT " path=%s\n", inode_target, path);

    if (blob) free(blob);

    *ftype_target = ftype;
    return(0);
}

//********************************************************
// ll_inode2path - Converts the inode to a path
//********************************************************

int ll_inode2path(lio_fuse_t *lfs, ex_id_t ino, char *fname, int *ftype, int force_os)
{
log_printf(0, "force_os=%d ino=" XIDT "\n", force_os, ino);
    if (force_os) {
        if (ll_inode2path_from_os(lfs, ino, fname, ftype) != 0) {
            return(-ENOENT);
        }
    } else if (ll_inode2path_from_ilut(lfs, ino, fname, ftype) != 0) {
        if (ll_inode2path_from_os(lfs, ino, fname, ftype) != 0) {
            return(-ENOENT);
        }
    }
log_printf(0, "force_os=%d ino=" XIDT " fname=%s\n", force_os, ino, fname);

    return(0);
}

//********************************************************
// ll_new_object_prep - Does the preamble lookup for creating a new object
//********************************************************

int ll_new_object_prep(lio_fuse_t *lfs, fuse_ino_t parent, const char *name, char *path, int force_os)
{
    int len, ftype;

    //** See if we have a valid parent
    if (ll_inode2path(lfs, parent, path, &ftype, force_os) != 0) {
        if (force_os == 0) { //** Let's retry but force the data from the LServer
            if (ll_inode2path(lfs, parent, path, &ftype, 1) != 0) {
                return(1);
            }
        }
    }

    //** We do so now add the dentry
    len = strlen(path);
    if ((len == 1) && (path[0] == '/')) {
        snprintf(path+len, OS_PATH_MAX-len, "%s", name);
    } else {
        snprintf(path+len, OS_PATH_MAX-len, "/%s", name);
    }
    return(0);
}

//********************************************************
// ll_inode2path_dentry - Uses the iLUT dentry table for the lookup
//   and returns the path along with the child inode
//********************************************************

int ll_inode2path_dentry(lio_fuse_t *lfs, lio_os_authz_local_t *ug, fuse_ino_t parent, const char *name, char *path, ex_id_t *inode, int *ftype, int force_os)
{
    struct fuse_entry_param fe;
    int err, len;
    ex_id_t pinode;

    //** See if the object already exists
    if (os_inode_lut_dentry_get(lfs->ilut, 1, parent, name, inode, &pinode, ftype, &len, NULL) == 0) {
        return(ll_new_object_prep(lfs, parent, name, path, force_os));  //** It does so make the path using the dentry in case of hardlinks
    }

    //** No internal match so do the fallback
    if (ll_new_object_prep(lfs, parent, name, path, force_os) != 0) return(1);

    //** Do the FS stat call
    err = lio_fs_stat_full(lfs->fs, ug, path, &(fe.attr), ftype, NULL, 0, lfs->no_cache_stat_if_file);
    if (err != 0) return(1);  //** No matching entry

    //** Add the inode
    os_inode_lut_put(lfs->ilut, 1, fe.attr.st_ino, parent, *ftype, strlen(name), name);  //**FIXME  name could be fifferent for hardlinks

    *inode = fe.attr.st_ino;
    return(0);
}

//********************************************************
// ll_object_reply_entry - Adds the object to inode LUT and does he reply
//********************************************************

void ll_object_reply_entry(lio_fuse_t *lfs, fuse_req_t req, fuse_ino_t parent, const char *name, const char *path, int retry)
{
    lio_os_authz_local_t ug;
    struct fuse_entry_param fe;
    ex_id_t hparent;
    int err, len, ftype, hftype, hlen;
    char fname[OS_PATH_MAX];
    char *hde;

    //** Do the FS stat call
    ftype = 0;
    err = lio_fs_stat_full(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, &(fe.attr), &ftype, NULL, 0, lfs->no_cache_stat_if_file);
//log_printf(0, "QWERT: A.fs_stat=%d parent=" XIDT " name=%s fname=%s\n", err, parent, name, path);
    if (err != 0) {  //** We may have a stale entry so try again but go direct to the LServer
//log_printf(0, "QWERT: Trying a fresh lookup parent=" XIDT " name=%s\n", parent, name);
        if ((retry == 0) || (ll_inode2path_from_os(lfs, parent, fname, &ftype) != 0)) {
            _lfs_hint_release(lfs, &ug);
            fuse_reply_err(req, -err);
            return;
        }

        //** Add the dentry
        len = strlen(fname);
        snprintf(fname+len, OS_PATH_MAX-len, "/%s", name);
        err = lio_fs_stat_full(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, &(fe.attr), &ftype, NULL, 0, lfs->no_cache_stat_if_file);
//log_printf(0, "QWERT: B.fs_stat=%d parent=" XIDT " name=%s fname=%s\n", err, parent, name, fname);
    }
    _lfs_hint_release(lfs, &ug);
    if (err != 0) {
        fuse_reply_err(req, -err);
        return;
    }

    //** If  we have a hardlink sanity check the preferred dentry still exists
    if (ftype & OS_OBJECT_HARDLINK_FLAG) {
        if (ll_inode2path(lfs, fe.attr.st_ino, fname, &hftype, 0) == 0) {  //** This generates the primary hardlink path
            if (lio_fs_exists(lfs->fs, fname) != 0) goto hl_exists;
        }

log_printf(0, "QWERT: CHECKING primary HARDLINK inode=" XIDT " parent=" XIDT " fname=%s\n", fe.attr.st_ino, hparent,fname);

        //** If we made it here something is not right so remove the dentry
        if (os_inode_lut_get(lfs->ilut, 1, fe.attr.st_ino, &hparent, &hftype, &hlen, &hde) == 0) { //** Get the inode record
log_printf(0, "QWERT: REMOVING primary HARDLINK hparent=" XIDT " hde=%s fname=%s\n", hparent, hde, fname);
            os_inode_lut_dentry_del(lfs->ilut, 1, hparent, hde);
            if (hde) free(hde);
        }
hl_exists:  //** We've done any cleanup so continue as normal
    }

    //** Add the inodeq
    os_inode_lut_put(lfs->ilut, 1, fe.attr.st_ino, parent, ftype, strlen(name), name);

    //** Form the reply
    fe.ino = fe.attr.st_ino;
    fe.generation = 0;
    fe.attr_timeout = lfs->ll_stat_timeout;
    fe.entry_timeout = lfs->ll_entry_timeout;
    fuse_reply_entry(req, &fe);
}

//********************************************************
// ll_lookup - FUSE lowlevel lookup inode routine
//********************************************************

void ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    lio_fuse_t *lfs = _get_lfs_context();
    char path[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    struct fuse_entry_param fe;
    int err, ftype;
    char *ptr;

//log_printf(0, "QWERT: parent=" XIDT " name=%s\n", parent, name);
    //** See if we have a valid parent
    if ((strcmp(".", name) == 0) || (strcmp("..", name) == 0)) goto direct_lookup;

    if (ll_new_object_prep(lfs, parent, name, path, 0) != 0) {
log_printf(0, "QWERT: OOPS parent=" XIDT " name=%s\n", parent, name);
        fuse_reply_err(req, ENOENT);
        return;
    }

log_printf(0, "QWERT: parent=" XIDT " name=%s path=%s\n", parent, name, path);

    ll_object_reply_entry(lfs, req, parent, name, path, 0);
    return;

direct_lookup:
log_printf(0, "QWERT: DIRECT LOOKUP parent=" XIDT " name=%s\n", parent, name);

    //** If we made it here we are doing a direct lookup so we need to get the path and then split it
    if (ll_inode2path(lfs, parent, path, &ftype, 0) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

log_printf(0, "QWERT: DIRECT LOOKUP parent=" XIDT " name=%s FULL fname=%s\n", parent, name, path);
    if (strcmp("..", name) == 0) { //** Looking up the directories parent's so peel off the last entry
        ptr = rindex(path, '/');
        if (!ptr) {  //** If not found throw an error
log_printf(0, "QWERT: DIRECT LOOKUP OOPS! rindex = NULL parent=" XIDT " name=%s FULL fname=%s\n", parent, name, path);
            fuse_reply_err(req, ENOENT);
            return;
        }

        *ptr = '\0';
log_printf(0, "QWERT: DIRECT LOOKUP parent=" XIDT " name=%s TRUNCATED fname=%s\n", parent, name, path);
    }

    //** Do the FS stat call
    err = lio_fs_stat_full(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, &(fe.attr), &ftype, NULL, 0, lfs->no_cache_stat_if_file);
    _lfs_hint_release(lfs, &ug);
log_printf(0, "QWERT: DIRECT LOOKUP path=%s err=%d\n", path, err);
    if (err != 0) {
        fuse_reply_err(req, -err);
        return;
    }

    //** Form the reply
    fe.ino = fe.attr.st_ino;
    fe.generation = 0;
    fe.attr_timeout = lfs->ll_stat_timeout;
    fe.entry_timeout = lfs->ll_entry_timeout;
    fuse_reply_entry(req, &fe);
}

//********************************************************
// ll_forget - Does nothing since the LServer tracks all inodes
//********************************************************

void ll_forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup)
{
    fuse_reply_none(req);
}

//********************************************************
// ll_forget - Does nothing since the LServer tracks all inodes
//********************************************************

void ll_forget_multi(fuse_req_t req, size_t count, struct fuse_forget_data *forgets)
{
    fuse_reply_none(req);
}

//********************************************************
// ll_stat - FUSE lowlevel stat inode routine
//********************************************************

void ll_stat(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (fi) ? (lio_fd_t *)fi->fh : NULL;
    int ftype, err;
    struct stat sbuf;
    char path[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    int force_os = 0;

    if (fd) {
        err = lio_fs_fstat(lfs->fs, fd, &sbuf);
    } else { //** Got to do an inode lookup
        //** See if we have a valid inode
retry:
        if (ll_inode2path(lfs, ino, path, &ftype, force_os) != 0) {
            fuse_reply_err(req, ENOENT);
            return;
        }

        //** Do the FS stat call
        err = lio_fs_stat_full(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, &sbuf, &ftype, NULL, 0, lfs->no_cache_stat_if_file);
        _lfs_hint_release(lfs, &ug);
        if (err != 0) {
            if (force_os == 0) { force_os = 1; goto retry; }
            fuse_reply_err(req, -err);
            return;
        }
    }

    fuse_reply_attr(req, &sbuf, lfs->ll_stat_timeout);
}

//*****************************************************************
// ll_setattr Set file attributes
// NOTE: We don't support setting atime an mtime directly
//*****************************************************************

void ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (fi) ? (lio_fd_t *)fi->fh : NULL;
    char path[OS_PATH_MAX];
    char *fname;
    lio_os_authz_local_t ug;
    uid_t uid;
    gid_t gid;
    struct stat sbuf;
    int err, ftype;
    int force_os = 0;

    if (fd) {
        fname = fd->fh->fname;
    } else {
        //** See if we have a valid inode
retry:
        if (ll_inode2path(lfs, ino, path, &ftype, force_os) != 0) {
            fuse_reply_err(req, ENOENT);
            return;
        }
        fname = path;
    }

    _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req));

    if (to_set & FUSE_SET_ATTR_MODE) {
        err = lio_fs_chmod(lfs->fs, &ug, fname, attr->st_mode);
        if (err) goto oops;
    }

	if (to_set & (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)) {
        uid = (to_set & FUSE_SET_ATTR_UID) ? attr->st_uid : (uid_t) -1;
        gid = (to_set & FUSE_SET_ATTR_GID) ? attr->st_gid : (gid_t) -1;
        err = lio_fs_chown(lfs->fs, &ug, fname, uid, gid);
        if (err) goto oops;
	}

	if (to_set & FUSE_SET_ATTR_SIZE) {
        err = lio_fs_truncate(lfs->fs, &ug, fname, attr->st_size);
        if (err) goto oops;
	}

    err = lio_fs_stat_full(lfs->fs, &ug, fname, &sbuf, &ftype, NULL, 0, lfs->no_cache_stat_if_file);
    if (err != 0) goto oops;

    _lfs_hint_release(lfs, &ug);
    fuse_reply_attr(req, &sbuf, lfs->ll_stat_timeout);
    return;

oops:
    _lfs_hint_release(lfs, &ug);
    if (force_os == 0) { force_os = 1; goto retry; }
    fuse_reply_err(req, -err);
}


//*****************************************************************
// ll_listxattr - Lists the extended attributes
//    These are currently defined as the user.* attributes
//*****************************************************************

void ll_listxattr(fuse_req_t req, fuse_ino_t ino,  size_t size)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    char fname[OS_PATH_MAX];
    char tmpbuf[32*1024];
    char *buf;
    int err, ftype;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, fname, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    //** Adjust the buffer
    buf = tmpbuf;
    if (size > sizeof(tmpbuf)) {
        tbx_type_malloc(buf, char, size);
    }

    err = lio_fs_listxattr(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, buf, size);
    _lfs_hint_release(lfs, &ug);

    if (err < 0) {  //** On error err is negative
        if (buf != tmpbuf) free(buf);
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

    //** On success err is the number of bytes in the list
    if (size == 0) {
        fuse_reply_xattr(req, err);
    } else if ((int)size >= err) {
        fuse_reply_buf(req, buf, err);
    } else {
        fuse_reply_err(req, ERANGE);
    }

    if (buf != tmpbuf) free(buf);
}

//*****************************************************************
// ll_getxattr - Rerturns an extended attribute
//*****************************************************************

void ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    char fname[OS_PATH_MAX];
    char tmpbuf[32*1024];
    char *buf;
    int err, ftype;
    int force_os = 0;

//log_printf(0, "QWERT: inode=" XIDT " attr=%s\n", ino, name);
    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, fname, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

//log_printf(0, "QWERT: inode=" XIDT " attr=%s fname=%s\n", ino, name, fname);

    //** Adjust the buffer
    buf = tmpbuf;
    if (size > sizeof(tmpbuf)) {
        tbx_type_malloc(buf, char, size);
    }
    err = lio_fs_getxattr(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, name, buf, size);
    _lfs_hint_release(lfs, &ug);

//log_printf(0, "QWERT: inode=" XIDT " attr=%s fname=%s err=%d\n", ino, name, fname, err);

    if (err < 0) {  //** On error err is negative
        if (buf != tmpbuf) free(buf);
        if (err != -ENODATA) {  //** No need to rety if the data is missing
            if (force_os == 0) { force_os = 1; goto retry; }
        }
        fuse_reply_err(req, -err);
        return;
    }

    //** On success err is the number of bytes in the list
    if (size == 0) {
        fuse_reply_xattr(req, err);
    } else if ((int)size >= err) {
        fuse_reply_buf(req, buf, err);
    } else {
        fuse_reply_err(req, ERANGE);
    }

    if (buf != tmpbuf) free(buf);
}

//*****************************************************************
// ll_setxattr - Sets an xattr
//*****************************************************************

void ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    char fname[OS_PATH_MAX];
    int err, ftype;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, fname, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_setxattr(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, name, value, size, flags);
    _lfs_hint_release(lfs, &ug);

    if (force_os == 0) { force_os = 1; goto retry; }

    fuse_reply_err(req, -err);
}

//*****************************************************************
// ll_removexattr - Removes the xattr
//*****************************************************************

void ll_removexattr(fuse_req_t req, fuse_ino_t ino, const char *name)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    char fname[OS_PATH_MAX];
    int err, ftype;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, fname, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_removexattr(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, name);
    _lfs_hint_release(lfs, &ug);

    if (force_os == 0) { force_os = 1; goto retry; }

    fuse_reply_err(req, -err); //** fuse_reply_err expects a positive error
}

//*****************************************************************
// ll_opendir - Opens the directory for reading
//*****************************************************************

void ll_opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    ll_dir_iter_t *dit;
    char fname[OS_PATH_MAX];
    int ftype;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, fname, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    tbx_type_malloc_clear(dit, ll_dir_iter_t, 1);
    dit->lfs = lfs;
    dit->parent = ino;
    dit->fsit = lio_fs_opendir(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, 1);
    _lfs_hint_release(lfs, &ug);

    if (dit->fsit == NULL) {
        free(dit);
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, ENOENT);
        return;
    }

    dit->stack = tbx_stack_new();
    dit->state = 0;

    //** Compose our reply
    fi->fh = (uint64_t)dit;
    fuse_reply_open(req, fi);
}

//*****************************************************************
// ll_releasedir - Closes an open directory
//*****************************************************************

void ll_releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    ll_dir_iter_t *dit = (ll_dir_iter_t *)fi->fh;
    ll_dir_entry_t *de;

    if (dit == NULL) {
        fuse_reply_err(req, EBADF);
        return;
    }

    if (dit->stack) {
        //** Cyle through releasing all the entries
        while ((de = (ll_dir_entry_t *)tbx_stack_pop(dit->stack)) != NULL) {
            log_printf(15, "fname=%s\n", de->dentry);
            tbx_log_flush();
            free(de->dentry);
            free(de);
        }

        tbx_stack_free(dit->stack, 0);
    }

    if (dit->fsit) lio_fs_closedir(dit->fsit);
    free(dit);

    fuse_reply_err(req, 0);
}

//*****************************************************************
// ll_readdirplus - Adds dentries to the buffer
//*****************************************************************

void ll_readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
    ll_dir_iter_t *dit = (ll_dir_iter_t *)fi->fh;
    lio_fuse_t *lfs = _get_lfs_context();
    ll_dir_entry_t *de;
    int n, i, err, ftype;
    struct stat stbuf;
    int bleft, bused, be, bmax;
    char buf[1024*1024];

    if (dit == NULL) {
        fuse_reply_err(req, EBADF);
        return;
    }

    off++;  //** This is the *next* slot to get where the stack top is off=1
    bmax = (size > sizeof(buf)) ? sizeof(buf) : size;
    bused = 0; bleft = bmax;

//log_printf(0, "QWERT: START inode=" XIDT " bmax=%d size=" ST " sizeof=" ST "\n", ino, bmax, size, sizeof(buf));

    memset(&stbuf, 0, sizeof(stbuf));
    n = tbx_stack_count(dit->stack);
    tbx_stack_move_to_bottom(dit->stack);  //** Go from the bottom up.
    if (n>=off) { //** Rewind
        for (i=n; i>off; i--) tbx_stack_move_up(dit->stack);

        de = tbx_stack_get_current_data(dit->stack);
        while (de != NULL) {
            log_printf(2, "dname=%s off=" XOT "\n", de->dentry, off);
            be = fuse_add_direntry_plus(req, buf + bused, bleft, de->dentry, &(de->fe), off);
//log_printf(0, "QWERT: be=%d bleft=%d off=%lu inode=" XIDT " parent=" XIDT " ftype=%d de=%s\n", be, bleft, off, de->fe.attr.st_ino, dit->parent, ftype, de->dentry);
            if (be > bleft) {
                fuse_reply_buf(req, buf, bused);
                bused = 0; bleft = bmax; be = 0;
                return;
            }
            bused += be;
            bleft -= be;

            off++;
            tbx_stack_move_down(dit->stack);
            de = tbx_stack_get_current_data(dit->stack);
        }
    }

    for (;;) {
        //** If we made it here then grab the next file and look it up.
        tbx_type_malloc(de, ll_dir_entry_t, 1);
        ftype = 0;
        err = lio_fs_readdir(dit->fsit, &(de->dentry), &(de->fe.attr), NULL, &ftype);
//log_printf(0, "QWERT: err=%d\n", err);
        if (err != 0) {   //** Nothing left to process
            free(de);
            if (err == 1) {
                fuse_reply_buf(req, buf, bused);  //** Flush the buffer if data
                return;
            } else {
                fuse_reply_err(req, EIO);
                return;
            }
        }

        tbx_stack_move_to_bottom(dit->stack);
        tbx_stack_insert_below(dit->stack, de);

        //** Fill in the rest of the structure
        de->ftype = ftype;
        de->fe.ino = de->fe.attr.st_ino;
        de->fe.generation = 0;
        de->fe.attr_timeout = lfs->ll_stat_timeout;
        de->fe.entry_timeout = lfs->ll_entry_timeout;

        //** Add it to the ilut if not a special entry
        if ((strcmp(de->dentry, ".") != 0) && (strcmp(de->dentry, "..") != 0)) {
            os_inode_lut_put(lfs->ilut, 1, de->fe.attr.st_ino, dit->parent, de->ftype, strlen(de->dentry), de->dentry);
        }

        be = fuse_add_direntry_plus(req, buf + bused, bleft, de->dentry, &(de->fe), off);
//log_printf(0, "QWERT: be=%d bleft=%d off=%lu inode=" XIDT " parent=" XIDT " ftype=%d de=%s\n", be, bleft, off, de->fe.attr.st_ino, dit->parent, ftype, de->dentry);
        if (be > bleft) {
            fuse_reply_buf(req, buf, bused);  //** Buf is full
            bused = 0; bleft = bmax; be = 0;  //** Reset it
            return;
        }
        bused += be;
        bleft -= be;

        off++;
    }

    return;
}

//********************************************************
// ll_mkdir - Makes a directory
//********************************************************


void ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err;
    char path[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    int force_os = 0;

    //** See if we have a valid parent
retry:
    if (ll_new_object_prep(lfs, parent, name, path, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_mkdir(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, mode);
    _lfs_hint_release(lfs, &ug);

    if (err != 0) {  //** Kick out on error
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

    ll_object_reply_entry(lfs, req, parent, name, path, 1);
}

//********************************************************
// ll_mknod -Create a regular or special file
//********************************************************

void ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err;
    char path[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    int force_os = 0;

    //** See if we have a valid parent
retry:
    if (ll_new_object_prep(lfs, parent, name, path, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_mknod(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, mode, rdev);
    _lfs_hint_release(lfs, &ug);

    if (err != 0) {  //** Kick out on error
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

    ll_object_reply_entry(lfs, req, parent, name, path, 1);
}

//********************************************************
// ll_symlink - Create a symbolic link
//********************************************************

void ll_symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *newname)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err;
    char path[OS_PATH_MAX];
    char abslink[OS_PATH_MAX];
    const char *link2;
    lio_os_authz_local_t ug;
    int force_os = 0;

//log_printf(0, "QWERT: inode=" XIDT " newname=%s link=%s\n", parent, newname, link);
    //** See if we have a valid parent
retry:
    if (ll_new_object_prep(lfs, parent, newname, path, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

//log_printf(0, "QWERT: inode=" XIDT " newname=%s link=%s fname=%s\n", parent, newname, link, path);

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

//log_printf(0, "QWERT: inode=" XIDT " newname=%s tweaked link2=%s fname=%s\n", parent, newname, link2, path);

    err = lio_fs_symlink(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), link2, path);
    _lfs_hint_release(lfs, &ug);

//log_printf(0, "QWERT: inode=" XIDT " newname=%s tweaked link2=%s fname=%s err=%d\n", parent, newname, link2, path, err);

    if (err != 0) {  //** Kick out on error
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

    ll_object_reply_entry(lfs, req, parent, newname, path, 1);
}

//********************************************************
// ll_hardlink - Creates a hardlink
//********************************************************

void ll_hardlink(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *newname)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err, ftype;
    char oldpath[OS_PATH_MAX];
    char newpath[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, oldpath, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    //** See if we have a valid parent
    if (ll_new_object_prep(lfs, newparent, newname, newpath, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_hardlink(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), oldpath, newpath);
    _lfs_hint_release(lfs, &ug);

    if (err != 0) {  //** Kick out on error
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

    //** This is odd that they want a new entry considering the objects are identical. **QWERT Test this**
    ll_object_reply_entry(lfs, req, newparent, newname, newpath, 1);
}

//********************************************************
// ll_unlink - Remove a file
//********************************************************

void ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err, ftype;
    char fname[OS_PATH_MAX];
    char *cptr;
    lio_os_authz_local_t ug;
    ex_id_t inode;
    int force_os = 0;

    //** See if we have a valid object
retry:
    if (ll_inode2path_dentry(lfs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), parent, name, fname, &inode, &ftype, force_os) != 0) {
        _lfs_hint_release(lfs, &ug);
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_object_remove(lfs->fs, &ug, fname, OS_OBJECT_FILE_FLAG);
log_printf(0, "REMOVE: inode=" XIDT " parent=" XIDT " dentry=%s fname=%s err=%d\n", inode, parent, name, fname, err);
    if (err != 0) { //** We have an error so let's see if it could be a race issue with a .fuse_hidden file
        cptr = rindex(fname, '/');
        if (strncmp(cptr, "/.fuse_hidden", 13) == 0) {
            err = 0;   //** Probably a false positive so go ahead and clear it
            notify_printf(lfs->lc->notify, 1, lfs->lc->creds, "SOFT_ERROR: Most likely the .fuse_hidden file was deleted on close before FUSE tried. Ignoring. fname=%s\n", fname);
        }
    } else {
        os_inode_lut_dentry_del(lfs->ilut, 1, parent, name);
    }

    _lfs_hint_release(lfs, &ug);

    if (err) {
        if (force_os == 0) { force_os = 1; goto retry; }
    }
    fuse_reply_err(req, -err);
}

//********************************************************
// ll_rmdir - REmove a directory
//********************************************************

void ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
    lio_fuse_t *lfs = _get_lfs_context();
    int err, ftype;
    char fname[OS_PATH_MAX];
    lio_os_authz_local_t ug;
    ex_id_t inode;
    int force_os = 0;

    //** See if we have a valid object
retry:
    if (ll_inode2path_dentry(lfs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), parent, name, fname, &inode, &ftype, force_os) != 0) {
        _lfs_hint_release(lfs, &ug);
        fuse_reply_err(req, ENOENT);
        return;
    }

    err = lio_fs_object_remove(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), fname, OS_OBJECT_DIR_FLAG);
    _lfs_hint_release(lfs, &ug);

    if (err) {
        if (force_os == 0) { force_os = 1; goto retry; }
    }

    //** Remove the entry and return
    os_inode_lut_dentry_del(lfs->ilut, 1, parent, name);
    fuse_reply_err(req, -err);
}

//********************************************************
// ll_rename - Rename an object
//********************************************************

void ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    int err, ftype;
    char *cptr;
    char oldpath[OS_PATH_MAX];
    char newpath[OS_PATH_MAX];
    ex_id_t inode;
    int force_os = 0;

    //** See if we have a valid object
retry:
    if (ll_inode2path_dentry(lfs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), parent, name, oldpath, &inode, &ftype, force_os) != 0) {
        _lfs_hint_release(lfs, &ug);
        fuse_reply_err(req, ENOENT);
        return;
    }

    //** See if we have a valid parent
    if (ll_new_object_prep(lfs, newparent, newname, newpath, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

log_printf(0, "inode=" XIDT " oldparent=" XIDT " oldname=%s newparent=" XIDT " newname=%s oldpath=%s newpath=%s flags=%u EXCHANGE=%u NOREPLACE=%u\n", inode, parent, name, newparent, newname, oldpath, newpath, flags, RENAME_EXCHANGE, RENAME_NOREPLACE);

    //**FIXME  Not sure how this works with the low-level drivers
    //** See if this is a .fuse_hidden rename
    cptr = rindex(newpath, '/');
    if ((lfs->enable_pending_delete_relocate == 1) && (strncmp(cptr, "/.fuse_hidden", 13) == 0)) {
        err = lfs_pending_delete_relocate_object(lfs, oldpath, newpath);
    } else {
        err = lio_fs_rename(lfs->fs,  _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), oldpath, newpath);
        _lfs_hint_release(lfs, &ug);
    }

    if (err == 0) os_inode_lut_dentry_rename(lfs->ilut, 1, inode, ftype, parent, name, newparent, newname);

    if (err) {
        if (force_os == 0) { force_os = 1; goto retry; }
    }
    fuse_reply_err(req, -err);
}

//********************************************************
// l_readlink - Reads the link associated with the symlink
//********************************************************

void ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    int n, err, ftype, bsize;
    char buf[OS_PATH_MAX];
    char path[OS_PATH_MAX];
    char flink[OS_PATH_MAX];
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, path, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    bsize = sizeof(buf);
    err = lio_fs_readlink(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, buf, bsize);
    _lfs_hint_release(lfs, &ug);
//log_printf(0, "QWERT: inode=" XIDT " fname=%s link=%s err=%d\n", ino, path, buf, err);

    if (err < 0) {
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, -err);
        return;
    }

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

    fuse_reply_readlink(req, buf);
}

//********************************************************
// ll_fsyncdir - Sync directory contents
//********************************************************

void ll_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    fuse_reply_err(req, 0);
}

//********************************************************
// ll_create - Create and open a file
//********************************************************

void ll_create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi)
{
    fuse_reply_err(req, ENOSYS);  //** This tells the kernel to use ll_mknod() and ll_open()
}


//********************************************************
// ll_open - Opne a file
//********************************************************

void ll_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    char path[OS_PATH_MAX];
    int ftype;
    lio_fd_t *fd;
    int force_os = 0;

    //** See if we have a valid inode
retry:
    if (ll_inode2path(lfs, ino, path, &ftype, force_os) != 0) {
        fuse_reply_err(req, ENOENT);
        return;
    }

    fd = lio_fs_open(lfs->fs, _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), path, lio_open_flags(fi->flags, 0));
//log_printf(0, "QWERT: fd=%p path=%s ino=" XIDT " force_os=%d\n", fd, path, ino, force_os);
    _lfs_hint_release(lfs, &ug);
    fi->fh = (uint64_t)fd;

    if (fd == NULL) {
        if (force_os == 0) { force_os = 1; goto retry; }
        fuse_reply_err(req, EREMOTEIO);
        return;
    }

    fuse_reply_open(req, fi);
}

//********************************************************
// ll_release - Close a file
//********************************************************

void ll_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;

    //** Check if we need to flag it as to remove on close by looking to see if it's in the trash LFS prefix
    //** FUSE will abandon removing it if the parent directory is remoaved first.
    if (strncmp(lfs->pending_delete_prefix, fd->fh->fname, lfs->pending_delete_prefix_len) == 0) {
        if (fd->fh->fname[lfs->pending_delete_prefix_len] == '/') {
            lio_fs_object_remove(lfs->fs, NULL, fd->fh->fname, OS_OBJECT_FILE_FLAG);
            lfs_pending_delete_mapping_remove(lfs, fd->fh->fname);
        }
    }

    err = lio_fs_close(lfs->fs, fd);

    fuse_reply_err(req, -err);
}


//********************************************************
// ll_flush - Flush data on an open file
//********************************************************

void ll_flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;

    err = lio_fs_flush(lfs->fs, fd);

    fuse_reply_err(req, -err);
}

//********************************************************
// ll_fsync - Does a metdata and data flush
//   NOTE: We always sync metdata so this is the same as a flush
//********************************************************

void ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
    ll_flush(req, ino, fi);
}

//********************************************************
// ll_read - Read data from an open file
//********************************************************

void ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;
    char tmpbuf[1024*1024];
    char *buf;

    buf = tmpbuf;
    if (size > sizeof(tmpbuf)) {
        tbx_type_malloc(buf, char, size);
    }

    err = lio_fs_pread(lfs->fs, fd, buf, size, off);

    if (err < 0) {
        fuse_reply_err(req, -err);
        if (buf != tmpbuf) free(buf);
        return;
    }

    fuse_reply_buf(req, buf, err);

    if (buf != tmpbuf) free(buf);
}

//********************************************************
// ll_write -Writes data to a file
//********************************************************

void ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;

    err = lio_fs_pwrite(lfs->fs, fd, buf, size, off);

    if (err < 0) {
        fuse_reply_err(req, -err);
        return;
    }

    fuse_reply_write(req, err);

}

//********************************************************
// ll_write_buf - Writes data to a file
//********************************************************

void ll_write_buf(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec *bufv, off_t off, struct fuse_file_info *fi)
{
//    lio_fuse_t *lfs = _get_lfs_context();
//    lio_fd_t *fd = (lio_fd_t *)fi->fh;
//    int err;
}


//********************************************************
// ll_statfs - Get FS stats
//********************************************************

void ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_os_authz_local_t ug;
    int err;
    struct statvfs sfs;

    err =  lio_fs_statvfs(lfs->fs,  _ll_get_fuse_ug(lfs, &ug, fuse_req_ctx(req)), NULL, &sfs);
    _lfs_hint_release(lfs, &ug);

    if (err < 0) {
        fuse_reply_err(req, -err);
        return;
    }

    fuse_reply_statfs(req, &sfs);
}

//********************************************************
// ll_flock = BSD file locks
//********************************************************

void ll_flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, int op)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd = (lio_fd_t *)fi->fh;
    int err;

    err = lio_fs_flock(lfs->fs, fd, op);
    fuse_reply_err(req, -err);
}

//********************************************************
// ll_fallocate - Allocate file space.  NOT implemented
//********************************************************

void ll_fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi)
{
    fuse_reply_err(req, ENOSYS);
}

//********************************************************
// ll_copy_file_range - Copies data between 2 open files
//********************************************************

void ll_copy_file_range(fuse_req_t req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info *fi_in, fuse_ino_t ino_out, off_t off_out, struct fuse_file_info *fi_out, size_t len, int flags)
{
    lio_fuse_t *lfs = _get_lfs_context();
    lio_fd_t *fd_in = (lio_fd_t *)fi_in->fh;
    lio_fd_t *fd_out = (lio_fd_t *)fi_out->fh;
    int err;

    if (fd_in == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_in handle\n");
        fuse_reply_err(req, EBADF);
        return;
    }

    if (fd_out == NULL) {
        log_printf(0, "ERROR: Got a null LFS fd_out handle\n");
        fuse_reply_err(req, EBADF);
        return;
    }

    err = lio_fs_copy_file_range(lfs->fs, fd_in, off_in, fd_out, off_out, len);

    if (err < 0) {
        fuse_reply_err(req, -err);
        return;
    }

    fuse_reply_write(req, err);
}

//********************************************************
// ll_lseek - Skips to the next data block. NOT SUPPORTED
//********************************************************

void ll_lseek(fuse_req_t req, fuse_ino_t ino, off_t off, int whence, struct fuse_file_info *fi)
{
    fuse_reply_err(req, ENOSYS);
}


//********************************************************
// lfs_ll_print_fuse_info - Prints the extra bits realated to using the LowLevel API
//********************************************************

void lfs_ll_print_fuse_info(void *arg, FILE *fd)
{
    lio_fuse_t *lfs = arg;

    fprintf(fd, "ll_stat_timeout = %lf\n", lfs->ll_stat_timeout);
    fprintf(fd, "ll_entry_timeout = %lf\n", lfs->ll_entry_timeout);

    fprintf(fd, "ilut_section = %s\n\n", lfs->ilut_section);

    fprintf(fd, "[%s]\n", lfs->ilut_section);
    os_inode_lut_print_running_config(lfs->ilut, fd);
}

//********************************************************
// lfs_ll_init - Low-level init routines
//********************************************************

void lfs_ll_init(void *userdata, struct fuse_conn_info *conn)
{
    lio_fuse_init_args_t *init_args = (lio_fuse_init_args_t *)userdata;

    //** This does the bulk of the init
    init_args->lfs = lfs_init_real(NULL, conn, init_args);

    glfs = init_args->lfs;

    glfs->use_lowlevel_api = 1;

    //** This is required for Inode LUT
    CAP_SET_WANT(conn, FUSE_CAP_EXPORT_SUPPORT);

    //** Get the stat/attr and entry timeouts
    glfs->ll_stat_timeout = tbx_inip_get_double(glfs->lc->ifd, glfs->lfs_section, "ll_stat_timeout", 5);
    glfs->ll_entry_timeout = tbx_inip_get_double(glfs->lc->ifd, glfs->lfs_section, "ll_stat_timeout", 1000);

    //** Load the LUT
    glfs->ilut_section = tbx_inip_get_string(glfs->lc->ifd, glfs->lfs_section, "ilut_section", "ilut");
    glfs->ilut = os_inode_lut_load(glfs->lc->ess, glfs->lc->ifd, glfs->ilut_section);
    if (glfs->ilut == NULL) {
        log_printf(0, "ERROR: Unable to create Inode LUT ctx! section=%s\n", glfs->ilut_section);
    }
    return;
}

//********************************************************
// lfs_ll_destroy
//********************************************************

void lfs_ll_destroy(void *private_data)
{
    lio_fuse_t *lfs = _get_lfs_context();

    if (lfs->ilut_section) free(lfs->ilut_section);
    if (lfs->ilut) os_inode_lut_destroy(lfs->ilut);

    lfs_destroy(lfs);
}


//********************************************************
// Here's the Low-Level FUSE operations structure
//********************************************************

struct fuse_lowlevel_ops lfs_ll_ops = { //All lfs instances should use the same functions so statically initialize
    .init = lfs_ll_init,
    .destroy = lfs_ll_destroy,
    .lookup = ll_lookup,
    .forget = ll_forget,
    .forget_multi = ll_forget_multi,
    .getattr = ll_stat,
	.setattr = ll_setattr,
    .listxattr = ll_listxattr,
	.getxattr = ll_getxattr,
	.setxattr = ll_setxattr,
	.removexattr = ll_removexattr,

	.opendir = ll_opendir,
	.readdir = NULL,   //** Leaving blank since we implemented readdirplus
	.readdirplus = ll_readdirplus,
	.releasedir = ll_releasedir,

    .access = NULL,
    .getlk = NULL,
    .setlk = NULL,
    .bmap = NULL,
    .ioctl = NULL,
    .poll = NULL,
    .retrieve_reply = NULL,
	.mkdir = ll_mkdir,
	.mknod = ll_mknod,
	.symlink = ll_symlink,
	.link = ll_hardlink,
	.unlink = ll_unlink,
	.rmdir = ll_rmdir,
	.rename	= ll_rename,
	.readlink = ll_readlink,
	.fsyncdir = ll_fsyncdir,
	.create = ll_create,
//TONEW	.tmpfile = NULL,
	.open = ll_open,
	.release = ll_release,
	.flush = ll_flush,
	.fsync = ll_fsync,
	.read = ll_read,
	.write = ll_write,
	.write_buf = NULL,
	.statfs = ll_statfs,
	.fallocate = ll_fallocate,
	.flock = ll_flock,
	.copy_file_range = ll_copy_file_range,
	.lseek = ll_lseek,
//TONEW	.statx = NULL
};

