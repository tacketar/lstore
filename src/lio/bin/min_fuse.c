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

#define _log_module_index 211

#include "config.h"

#ifdef HAS_FUSE3
    #define FUSE_USE_VERSION (3*100 + 16)
    #include <fuse3/fuse.h>
#else
    #define FUSE_USE_VERSION 26
    #include <fuse.h>
#endif

#include <stdio.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <unistd.h>
#include <apr_hash.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include <tbx/type_malloc.h>

// *************************************************************************
// *************************************************************************

#define OBJECT_ROOT "/"
#define OBJECT_DIR  "/dir"
#define OBJECT_FILE "/file"

#define OBJECT_ROOT_DENTRY "."
#define OBJECT_DIR_DENTRY  "dir"
#define OBJECT_FILE_DENTRY "file"

#define LOG_FNAME "/tmp/min.log"

typedef struct {
    const char *fname;
    const char *dentry;
    struct stat sinfo;
    apr_hash_t *xattrs;
    apr_hash_t *xattrs_query;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
} min_object_t;

typedef struct {
    char *key;
    char *val;
    int v_size;
    int n_read;
    int n_write;
} xattr_t;

min_object_t *_object_root = NULL;
min_object_t *_object_dir = NULL;
min_object_t *_object_file = NULL;

FILE *log_fd = NULL;
int _ignore_xattr_replace = 0;

//*************************************************************************
//  ns_object_init - Init an object
//*************************************************************************

min_object_t *ns_object_init(const char *fname, const char *dentry, int ino, mode_t mode)
{
    min_object_t *mo;

    tbx_type_malloc_clear(mo, min_object_t, 1);
    assert_result(apr_pool_create(&(mo->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(mo->lock), APR_THREAD_MUTEX_DEFAULT, mo->mpool);
    mo->xattrs = apr_hash_make(mo->mpool);
    mo->xattrs_query = apr_hash_make(mo->mpool);

    mo->fname = fname;
    mo->dentry = dentry;
    mo->sinfo.st_uid = getuid();
    mo->sinfo.st_gid = getgid();
    mo->sinfo.st_ino = ino;
    mo->sinfo.st_mode = mode;

    return(mo);
}

//*************************************************************************
// ns_object_destroy
//*************************************************************************

void ns_object_destroy(min_object_t *mo)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    xattr_t *attr;

    //** Remove all the attrs
    for (hi=apr_hash_first(NULL, mo->xattrs_query); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&attr);
        if (attr->key) free(attr->key);
        if (attr->val) free(attr->val);
        free(attr);
    }

    for (hi=apr_hash_first(NULL, mo->xattrs); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&attr);
        if (attr->key) free(attr->key);
        if (attr->val) free(attr->val);
        free(attr);
    }

    //* Now clean up the object
    apr_pool_destroy(mo->mpool);
    free(mo);
}

//*************************************************************************
//  ns_init - Initializes the namespace
//*************************************************************************

int ns_init()
{
    //** Make the objects
    _object_root = ns_object_init(OBJECT_ROOT, OBJECT_ROOT_DENTRY, 1, S_IFDIR|0755);
    _object_dir = ns_object_init(OBJECT_DIR, OBJECT_DIR_DENTRY, 2, S_IFDIR|0755);
    _object_file = ns_object_init(OBJECT_FILE, OBJECT_FILE_DENTRY, 3, S_IFREG|0666);

    return(0);
}

//*************************************************************************
// printf_attr
//*************************************************************************

void printf_attr(FILE *fd, const char *val, int size)
{
    int i;
    unsigned int x;

    if (size <= 0) return;
    for (i=0; i<size; i++) {
        x = (unsigned  char)val[i];
        fprintf(fd, "%x:", x);
    }
}


//*************************************************************************
// dump_object
//*************************************************************************

void dump_object(min_object_t *mo)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    xattr_t *attr;

    fprintf(log_fd, "object=%s\n", mo->fname);
    for (hi=apr_hash_first(NULL, mo->xattrs_query); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&attr);
        fprintf(log_fd, "    attr=%s n_read=%d n_write=%d\n", attr->key, attr->n_read, attr->n_write);
    }
}


//*************************************************************************
//  ns_destroy - Destroys the namespace and dumps all the attrs queried
//*************************************************************************

void ns_destroy()
{

    dump_object(_object_root);
    dump_object(_object_dir);
    dump_object(_object_file);

    ns_object_destroy(_object_root);
    ns_object_destroy(_object_dir);
    ns_object_destroy(_object_file);
}

//*************************************************************************
//  _get_object - Returns the object
//*************************************************************************

min_object_t *_get_object(const char *fname)
{
    if (strcmp(fname, OBJECT_ROOT) == 0) {
        return(_object_root);
    } else if (strcmp(fname, OBJECT_DIR) == 0) {
        return(_object_dir);
    } else if (strcmp(fname, OBJECT_FILE) == 0) {
        return(_object_file);
    }

    return(NULL);
}

//*************************************************************************
// min_stat
//*************************************************************************

int min_stat(const char *fname, struct stat *sbuf, struct fuse_file_info *fi)
{
    min_object_t *mo;

    mo = _get_object(fname);
    if (mo == NULL) return(-ENOENT);

    *sbuf = mo->sinfo;
    return(0);
}

//*************************************************************************
//  xattr_create
//*************************************************************************

xattr_t *xattr_create(const char *name, const char *val, int v_size)
{
    xattr_t *attr;

    tbx_type_malloc_clear(attr, xattr_t, 1);

    attr->key = strdup(name);
    attr->v_size = v_size;
    if (v_size > 0) {
        tbx_type_malloc_clear(attr->val, char, v_size);
        memcpy(attr->val, val, v_size);
    }

    return(attr);
}

//*************************************************************************
// xattr_destroy
//*************************************************************************

void xattr_destroy(xattr_t *attr)
{
    if (attr->key) free(attr->key);
    if (attr->val) free(attr->val);
    free(attr);
}

//*************************************************************************
// min_chmod
//*************************************************************************

#ifdef HAS_FUSE3
int min_chmod(const char *fname, mode_t mode, struct fuse_file_info *fi)
#else
int min_chmod(const char *fname, mode_t mode)
#endif
{
    min_object_t *mo;

    mo = _get_object(fname);
    if (mo == NULL) return(-ENOENT);

    mo->sinfo.st_mode = mode;
    return(0);
}

//*************************************************************************
// min_opendir
//*************************************************************************

int min_opendir(const char *fname, struct fuse_file_info *fi)
{
    if (strcmp(fname, "/") != 0) return(-EACCES);

    return(0);
}

//*************************************************************************
// min_closedir
//*************************************************************************

int min_closedir(const char *fname, struct fuse_file_info *fi)
{
    return(0);
}

//*************************************************************************
// min_readdir
//*************************************************************************

#ifdef HAS_FUSE3
    #define FILLER(fn, buf, dentry, stat, off)  fn(buf, dentry, stat, off, FUSE_FILL_DIR_PLUS)
    int min_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
#else
    #define FILLER(fn, buf, dentry, stat, off)  fn(buf, dentry, stat, off)
    int min_readdir(const char *dname, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi)
#endif
{

    FILLER(filler, buf, _object_root->dentry, &(_object_root->sinfo), 0);
    FILLER(filler, buf, _object_dir->dentry, &(_object_dir->sinfo), 0);
    FILLER(filler, buf, _object_file->dentry, &(_object_file->sinfo), 0);

    return(0);
}

//*************************************************************************
// min_setxattr
//*************************************************************************

#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int min_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags)
#  else
int min_setxattr(const char *fname, const char *name, const char *fval, size_t size, int flags, uint32_t dummy)
#  endif
{
    min_object_t *mo;
    xattr_t *attr;

    mo = _get_object(fname);
    if (mo == NULL) return(-ENOENT);

    apr_thread_mutex_lock(mo->lock);

    //** 1st update the query hash
    attr = apr_hash_get(mo->xattrs_query, name, APR_HASH_KEY_STRING);
    if (attr == NULL) {
        attr = xattr_create(name, NULL, -1);
        apr_hash_set(mo->xattrs_query, attr->key, APR_HASH_KEY_STRING, attr);
    }
    attr->n_write++;

    //** Now do the operation
    attr = apr_hash_get(mo->xattrs, name, APR_HASH_KEY_STRING);
    if (flags != 0) { //** Got an XATTR_CREATE/XATTR_REPLACE
        if (flags == XATTR_CREATE) {
            apr_thread_mutex_unlock(mo->lock);
            return(-EEXIST);
        } else if (flags == XATTR_REPLACE) {
            if (attr == NULL) {
               if (_ignore_xattr_replace == 0) {
                   apr_thread_mutex_unlock(mo->lock);
                   return(-ENOATTR);
               }
            }
        }
    }

    //** If we made it here then we just store it
    if (size == 0) {
        if (attr) {
            if (attr->v_size > 0) {
                free(attr->val);
                attr->val = NULL; attr->v_size = 0;
            }
        } else {
            attr = xattr_create(name, fval, size);
            apr_hash_set(mo->xattrs, attr->key, APR_HASH_KEY_STRING, attr);
        }
    } else {
        if (attr) {
            apr_hash_set(mo->xattrs, attr->key, APR_HASH_KEY_STRING, NULL);
            xattr_destroy(attr);
        }

        attr = xattr_create(name, fval, size);
        apr_hash_set(mo->xattrs, attr->key, APR_HASH_KEY_STRING, attr);
    }

    apr_thread_mutex_unlock(mo->lock);

    return(0);
}

#endif //HAVE_XATTR

//*************************************************************************
//  min_getxattr
//*************************************************************************

#if defined(HAVE_XATTR)
#  if ! defined(__APPLE__)
int min_getxattr(const char *fname, const char *name, char *buf, size_t size)
#  else
int min_getxattr(const char *fname, const char *name, char *buf, size_t size, uint32_t dummy)
#  endif
{
    min_object_t *mo;
    xattr_t *attr;
    int err = 0;

    mo = _get_object(fname);
    if (mo == NULL) return(-ENOENT);

    apr_thread_mutex_lock(mo->lock);

    //** 1st update the query hash
    attr = apr_hash_get(mo->xattrs_query, name, APR_HASH_KEY_STRING);
    if (attr == NULL) {
        attr = xattr_create(name, NULL, -1);
        apr_hash_set(mo->xattrs_query, attr->key, APR_HASH_KEY_STRING, attr);
    }
    attr->n_read++;

    attr = apr_hash_get(mo->xattrs, name, APR_HASH_KEY_STRING);
    if (attr) {
        err = attr->v_size;
        if (attr->v_size <= (int)size) {
            memcpy(buf, attr->val, attr->v_size);
        } else if (size != 0) {
            fprintf(stderr, "ERROR: getxattr Buffer to small fname=%s attr=%s v_size=%d buf_size=%ld\n", fname, name, attr->v_size, size);
            err = -ENOBUFS;
        }
    } else {
        err = -ENODATA;
    }
    apr_thread_mutex_unlock(mo->lock);

    return(err);
}

#endif //HAVE_XATTR


//*************************************************************************
//*************************************************************************

static const struct fuse_operations min_ops = {
    .chmod = min_chmod,
    .opendir = min_opendir,
    .readdir = min_readdir,
    .releasedir = min_closedir,
    .getattr = min_stat,
    .getxattr = min_getxattr,
    .setxattr = min_setxattr
};


void print_usage()
{
    printf("\n"
           "min_fuse [FUSE_OPTIONS] <mount-point>\n"
           "       -h   --help            print this help\n"
           "       -ho                    print FUSE mount options help\n"
           "       -V   --version         print FUSE version\n"
           "       -d   -o debug          enable debug output (implies -f)\n"
           "       -s                     disable multi-threaded operation\n"
           "       -f                     foreground operation\n"
           "       -o OPT[,OPT...]        mount options\n"
           "                                (for possible values of OPT see 'man mount.fuse' or see 'min_fuse -o -h')\n"
           "\n"
           "This creates a minimum FUSE mount point with 2 hardcoded objects inside: file and dir.\n"
           "The mount supports the ability to stat, setxattr, and getxattr on the objects. Nothing else.\n"
           "If the environment varialbe MIN_IGNORE_XATTR_REPLACE is set then then XATTR_REPLACE flag is ignored\n"
           "If the environment variable MIN_LOG is set then operations are also recorded to stderr\n"
           "\n"
           "Logging goes to %s. This can be overriden by setting the MIN_LOG environment variabl\n", LOG_FNAME);

}

int main(int argc, char **argv)
{
    int err = 0;
    char *log_fname;

    if (argc < 2 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        print_usage();
        return(1);
    }


    log_fname = getenv("MIN_LOG");
    if (log_fname == NULL) log_fname = LOG_FNAME;
    log_fd = fopen(log_fname, "w");
    if (log_fd == NULL) {
        fprintf(stderr, "ERROR: Unable to create log file: %s\n", log_fname);
        exit(1);
    }

    if (getenv("MIN_IGNORE_XATTR_REPLACE") != NULL) _ignore_xattr_replace = 1;

    ns_init();

    err = fuse_main(argc, argv, &min_ops, NULL);

    ns_destroy();

    fclose(log_fd);

    return(err);
}
