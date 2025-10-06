/*
   Copyright 2025 Vanderbilt University

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
//  This module provides low-level tooling to track LStore inodes and perform
//  inode -> path lookups and management for when it gets out of sync.
//******************************************************************************

#define _log_module_index 250

#include <lio/ex3_fwd.h>
#include <lio/ex3.h>
#include <lio/os.h>
#include <tbx/type_malloc.h>
#include <tbx/append_printf.h>
#include <tbx/string_token.h>

#include "inode.h"
#include "inode_lut.h"

#include <sys/stat.h>
#include <sys/types.h>

//******************************************************************************
// Inode DB overview. There are 2 DB's -- inode and dir.  The inode DB has all
// contains all objects and "dir" just contains directory entries.  The purpose
// for having 2 is that the "dir" DB should be much smaller and is used as a
// cache front end to the larger inode DB.
//
// The records are the same for both DBs:
//     key: inode
//     value: dentry, ftype, parent_inode
//
// Each DB can be sharded across multiple drives to increase performance at creation.
// By default all the shards are created on the same drive if given a top level prefix
// to use.  In order to perform sharding across multiple physical devices a prefix
// skeleton can be constructed a priori at the prefix using symlinks for the shards.
// This skeleton will be used as is.
//
// Skeleton format:
//     <prefix>/
//              inode/
//                    n_shards
//                    shard_1
//                    ..
//                    shard_N
//              dir/
//                    n_shards
//                    shard_1
//                    ..
//                    shard_N
//               scan_prefix    - This is mainly used for testing to know the "OS" path for generating the DB
//******************************************************************************

//** These are used for lookup's and DB generation
walk_create_fn_t walk_create = NULL;
walk_destroy_fn_t walk_destroy = NULL;
walk_next_fn_t walk_next = NULL;
walk_object_info_t walk_info = NULL;
void *walk_arg = NULL;

void os_inode_close_a_db(os_inode_db_t *idb);

//****************************************************************************
// inode_compare_op - Comparator routine for the inode and dir DB
//****************************************************************************

static int inode_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    ex_id_t aid = *(ex_id_t *)a;
    ex_id_t bid = *(ex_id_t *)b;

    if (aid > bid) {
        return(1);
    } else if (aid < bid) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

static void default_compare_destroy(void *arg) { return; }
static const char *inode_compare_name(void *arg) { return("INODE"); }


//****************************************************************************
// get_shards - Retreives the partition info for the DB
//****************************************************************************

static int get_shards(const char *prefix)
{
    int n;
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/n_shards", prefix);

    fd = fopen(path, "r");
    if (fd == NULL) {
        return(-1);
    }

    n = -1;
    if (fscanf(fd, "%d", &n) == 0) n = -1;
    fclose(fd);

    return(n);
}

//****************************************************************************
// put_shards - Stores the partition info for the DB
//****************************************************************************

static int put_shards(const char *prefix, int n)
{
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/n_shards", prefix);

    fd = fopen(path, "w");
    if (fd == NULL) {
        fprintf(stderr, "ERROR: Unable to create partitions file!. path=%s\n", path); fflush(stderr);
        return(-1);
    }

    fprintf(fd, "%d", n);
    fclose(fd);

    return(0);
}


//******************************************************************************
// os_inode_open_a_shard - Opens an inode/dir DB shard
//******************************************************************************

os_inode_shard_t *os_inode_open_a_shard(const char *db_path, rocksdb_comparator_t *cmp, int mode)
{
    os_inode_shard_t *db;
    rocksdb_options_t *opts, *opts2;
    char *errstr = NULL;

    tbx_type_malloc_clear(db, os_inode_shard_t, 1);

    if (mode & OS_INODE_OPEN_CREATE_ONLY) { //** Throw an error if it already exists
        opts2 = rocksdb_options_create();
        rocksdb_options_set_error_if_exists(opts2, 1);
        rocksdb_options_set_create_if_missing(opts2, 1);

        db->db = rocksdb_open(opts2, db_path, &errstr);
        if (errstr != NULL) {  //** It already exists
            rocksdb_options_destroy(opts2);
            free(db);
            free(errstr);
            errstr = NULL;

            fprintf(stderr, "ERROR: Already exists and OS_INODE_OPEN_CREATE_ONLY is set!  DB:%s\n", db_path); fflush(stderr);
            return(NULL);
        }

        if (db->db) {  //** Close it if needed
            rocksdb_cancel_all_background_work(db->db, 1);
            rocksdb_close(db->db);
        }

        rocksdb_options_destroy(opts2);
    }

    //** Try opening it for real
    opts = rocksdb_options_create();
    if (cmp) rocksdb_options_set_comparator(opts, cmp);
    if (mode & (OS_INODE_OPEN_CREATE_ONLY|OS_INODE_OPEN_CREATE_IF_NEEDED)) rocksdb_options_set_create_if_missing(opts, 1);

    errstr = NULL;
    if (mode & OS_INODE_OPEN_READ_ONLY) {  //** Open Read only
        db->db = rocksdb_open_for_read_only(opts, db_path, 0, &errstr);
    } else {  //** OPen R/W
        db->db = rocksdb_open(opts, db_path, &errstr);
    }

    if ((errstr != NULL) || (db->db == NULL)) {  //** An Error occured
        fprintf(stderr, "ERROR: Failed Opening/Creating %s. DB error:%s mode=%d\n", db_path, errstr, mode);
        free(errstr);
        rocksdb_options_destroy(opts);
        free(db);
        return(NULL);
    }

    rocksdb_options_destroy(opts);

    db->wopt = rocksdb_writeoptions_create();
    db->ropt = rocksdb_readoptions_create();
    db->cmp = cmp;

    return(db);
}


//******************************************************************************
//  os_inode_open_a_db - Open's either the inode or dir db and all shards
//******************************************************************************

os_inode_db_t *os_inode_open_a_db(const char *db_path, rocksdb_comparator_t *cmp, int mode, int n_shards)
{
    int i, ftype;
    os_inode_db_t *idb;
    char path[OS_PATH_MAX];

    tbx_type_malloc_clear(idb, os_inode_db_t, 1);

    //** Make the skeleton if needed
    //** See if the top level dir exists
    ftype = lio_os_local_filetype(db_path);
    if (! OS_INODE_IS_DIR(ftype)) {
        if (mode & OS_INODE_OPEN_EXISTS) {
            fprintf(stderr, "ERROR: Missing base path! directory: %s mode=%d\bn", db_path, mode); fflush(stderr);
            return(NULL);
        } else  if (mkdir(db_path, 0) != 0) {   //** Kick out if we fail
            fprintf(stderr, "ERROR: Unable to make base path! directory: %s mode=%d\n", db_path, mode); fflush(stderr);
            return(NULL);
        }
    }

    //** Now see if we already have a n_shards file
    idb->n_shards = get_shards(db_path);
    if (idb->n_shards == -1) {
        if (mode & OS_INODE_OPEN_EXISTS) {
            fprintf(stderr, "ERROR: Missing shards file! directory: %s mode=%d\bn", db_path, mode); fflush(stderr);
            return(NULL);
        } else {
            put_shards(db_path, n_shards);
            idb->n_shards = n_shards;
        }
    }

    //** Now cycle through looking for the shard directories
    for (i=0; i<idb->n_shards; i++) {
        sprintf(path, "%s/shard_%d", db_path, i);
        ftype = lio_os_local_filetype(path);
        if (! OS_INODE_IS_DIR(ftype)) {
            if (mode & OS_INODE_OPEN_EXISTS) {
                fprintf(stderr, "ERROR: Missing shard path! directory: %s mode=%d\bn", path, mode); fflush(stderr);
                return(NULL);
            } else  if (mkdir(path, 0) != 0) {   //** Kick out if we fail
                fprintf(stderr, "ERROR: Unable to make shard directory! directory: %s mode=%d\bn", path, mode); fflush(stderr);
                return(NULL);
            }
        }
    }

    //** If we made it here then the skeleton exists so go ahead an open the shards
    //** The os_inod_open_a_shard aborts on an error.
    tbx_type_malloc_clear(idb->shard, os_inode_shard_t *, idb->n_shards);
    for (i=0; i<idb->n_shards; i++) {
        sprintf(path, "%s/shard_%d", db_path, i);
        idb->shard[i] = os_inode_open_a_shard(path, cmp, mode);
        if (idb->shard[i] == NULL) {
            fprintf(stderr, "ERROR: Unable to open shard! directory: %s mode=%d\n", path, mode); fflush(stderr);
            os_inode_close_a_db(idb);
            return(NULL);
        }
    }

    idb->prefix = strdup(db_path);
    return(idb);
}

//******************************************************************************
// os_inode_open_ctx - Open the inode DB sets
//******************************************************************************

os_inode_ctx_t *os_inode_open_ctx(const char *prefix, int mode, int n_shards)
{
    os_inode_ctx_t *ctx;
    char path[OS_PATH_MAX];
    int ftype;

    //** See if the top level dir exists
    ftype = lio_os_local_filetype(prefix);
    if (!OS_INODE_IS_DIR(ftype)) {
        if (mode & OS_INODE_OPEN_EXISTS) {
            fprintf(stderr, "ERROR: Missing base path! directory: %s mode=%d\bn", prefix, mode); fflush(stderr);
            return(NULL);
        } else  if (mkdir(prefix, 0) != 0) {   //** Kick out if we fail
            fprintf(stderr, "ERROR: Unable to make base path! directory: %s mode=%d\n", prefix, mode); fflush(stderr);
            return(NULL);
        }
    }

    tbx_type_malloc_clear(ctx, os_inode_ctx_t, 1);
    ctx->prefix = strdup(prefix);
    ctx->inode_cmp = rocksdb_comparator_create(ctx, default_compare_destroy, inode_compare_op, inode_compare_name);

    //** NOTE that if the DB open failes the program aborts
    sprintf(path, "%s/inode", prefix);
    ctx->inode = os_inode_open_a_db(path, ctx->inode_cmp, mode, n_shards);
    if (ctx->inode == NULL) {
        fprintf(stderr, "ERROR: Unable to make inode DB! directory: %s mode=%d n_shards=%d\n", prefix, mode, n_shards); fflush(stderr);
        os_inode_close_ctx(ctx);
        return(NULL);
    }

    sprintf(path, "%s/dir", prefix);
    ctx->dir = os_inode_open_a_db(path, ctx->inode_cmp, mode, n_shards);
    if (ctx->dir == NULL) {
        fprintf(stderr, "ERROR: Unable to make dir DB! directory: %s mode=%d n_shards=%d\n", prefix, mode, n_shards); fflush(stderr);
        os_inode_close_ctx(ctx);
        return(NULL);
    }

    return(ctx);
}

//****************************************************************************z
// os_inode_close_a_shared - Closes either an inode or dir shard
//****************************************************************************

void os_inode_close_a_shard(os_inode_shard_t *db)
{
    if (db == NULL) return;

    if (db->db) {
        rocksdb_cancel_all_background_work(db->db, 1);
        rocksdb_close(db->db);
    }

    if (db->wopt) rocksdb_writeoptions_destroy(db->wopt);
    if (db->ropt) rocksdb_readoptions_destroy(db->ropt);

    free(db);
}

//****************************************************************************
// os_inode_close_a_db
//****************************************************************************

void os_inode_close_a_db(os_inode_db_t *idb)
{
    int i;

    if (idb == NULL) return;
    for (i=0; i<idb->n_shards; i++) {
        os_inode_close_a_shard(idb->shard[i]);
    }

    if (idb->shard) free(idb->shard);
    if (idb->prefix) free(idb->prefix);
    free(idb);
}

//****************************************************************************
// os_inode_close_ctx - Close a inode context
//****************************************************************************

void os_inode_close_ctx(os_inode_ctx_t *ctx)
{

    if (ctx == NULL) return;
    os_inode_close_a_db(ctx->inode);
    os_inode_close_a_db(ctx->dir);

    rocksdb_comparator_destroy(ctx->inode_cmp);

    if (ctx->prefix) free(ctx->prefix);
    free(ctx);
}

//****************************************************************************
// os_inode_compare_shard_count - Compares the shard counts between the 2 DBs
//    and returns an error if they are different
//****************************************************************************

int os_inode_compare_shard_count(os_inode_ctx_t *ctx1, os_inode_ctx_t *ctx2)
{
    int err = 0;

    if (ctx1->inode->n_shards != ctx2->inode->n_shards) {
        err = 1;
        fprintf(stderr, "ERROR: OOPS! inode shard counts differ: db1=%d db2=%d\n", ctx1->inode->n_shards, ctx2->inode->n_shards);
    }

    if (ctx1->dir->n_shards != ctx2->dir->n_shards) {
        err = 1;
        fprintf(stderr, "ERROR: OOPS! dir shard counts differ: db1=%d db2=%d\n", ctx1->dir->n_shards, ctx2->dir->n_shards);
    }

    return(err);
}

//****************************************************************************
// os_inode_put - Stores a record in the inode DBs
//****************************************************************************

int os_inode_put(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry)
{
    os_inode_rec_t *r;
    os_inode_shard_t *s;
    int n, nbytes, err;
    char *errstr = NULL;
    char buf[OS_PATH_MAX + sizeof(os_inode_rec_t) + 1];

//fprintf(stderr, "os_inode_put: INODE inode=" XIDT " ftype=%d\n", inode, ftype);
    err = 0;
    r = (os_inode_rec_t *)buf;
    r->inode = inode;
    r->ftype = ftype;
    r->dentry.parent_inode = parent_inode;
    r->dentry.len = len;
    strncpy(r->dentry.dentry, dentry, len+1);
    nbytes = sizeof(os_inode_rec_t) + len + 1;

    n = inode % ctx->inode->n_shards;
    s = ctx->inode->shard[n];
    rocksdb_put(s->db, s->wopt, (const char *)&inode, sizeof(ex_id_t), buf, nbytes, &errstr);
    if (errstr != NULL) {
        log_printf(0, "ERROR: inode prefix=%s shard=%d err=%s\n", ctx->prefix, n, errstr);
        free(errstr);
        err = 1;
    }

    if (OS_INODE_IS_DIR(ftype)) {
        n = inode % ctx->dir->n_shards;
        s = ctx->dir->shard[n];
        rocksdb_put(s->db, s->wopt, (const char *)&inode, sizeof(ex_id_t), buf, nbytes, &errstr);
        if (errstr != NULL) {
            log_printf(0, "ERROR: dir prefix=%s shard=%d err=%s\n", ctx->prefix, n, errstr);
            free(errstr);
            err = 1;
        }
    }

    return(err);
}

//****************************************************************************
// os_inode_put - Stores a record in the inode DBs
//****************************************************************************

int os_inode_put_with_lut(os_inode_ctx_t *ctx,  os_inode_lut_t *ilut, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry)
{
    os_inode_put(ctx, inode, parent_inode, ftype, len, dentry);
    os_inode_lut_put(ilut, 1, inode, parent_inode, ftype, len, dentry);
    return(0);
}

//****************************************************************************
//  os_inode_shard_get - Fetch a record from the shard
//****************************************************************************

int os_inode_shard_get(os_inode_shard_t *s, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    os_inode_rec_t *r;
    size_t nbytes;
    char *errstr = NULL;
    char *buf;

    nbytes = 0;
    *dentry = NULL;
    *parent_inode = OS_INODE_MISSING;
    buf = (char *)rocksdb_get(s->db, s->ropt, (const char *)&inode, sizeof(ex_id_t), &nbytes, &errstr);
    if (nbytes == 0) {
        return(1);
    }
    r = (os_inode_rec_t *)buf;
    *ftype = r->ftype;
    *parent_inode = r->dentry.parent_inode;
    *len = r->dentry.len;
    if (dentry) *dentry = strdup(r->dentry.dentry);

    free(buf);

    return(0);
}

//****************************************************************************
// os_inode_db_get - Routine to fetch  a record in the inode or dir DBs
//    inode - Inode to look up
//    parent_inode - Returned parent inode value
//    len   - On exit it contains the size of the dentry used without the training NUL byte
//    dentry- object name in the directory. The space is allocated and the caller should free it if non-NULL.
//            IF dentry == NULL then no dentry is returned.
//
//  Return values
//    If the inode is found 0 is returned otherwise 1 is returned if missing
//****************************************************************************

int os_inode_db_get(os_inode_db_t *db, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    int n;

    n = inode  % db->n_shards;
    return(os_inode_shard_get(db->shard[n], inode, parent_inode, ftype, len, dentry));
}

//****************************************************************************
// os_inode_db_get_with_lut - Same as os_inode_db_get but with a LUT in front of it
//****************************************************************************

int os_inode_db_get_with_lut(os_inode_db_t *db, os_inode_lut_t *ilut, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    int err;

    if (os_inode_lut_get(ilut, 1, inode, parent_inode, ftype, len, dentry) != 0) {
        err = os_inode_db_get(db, inode, parent_inode, ftype, len, dentry);
        if (err != 0) return(1);  //** If not found kick out
        os_inode_lut_put(ilut, 1, inode, *parent_inode, *ftype, *len, *dentry); //** Got it from DD so add it to the LUT
    }

    return(0);
}

//****************************************************************************
// os_inode_get - Fetches a record in the inode DBs
//****************************************************************************

int os_inode_get(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    return(os_inode_db_get(ctx->inode, inode, parent_inode, ftype, len, dentry));
}

//****************************************************************************
// os_inode_get_with_lut - Same as os_inode_db_get but with a LUT in front of it
//****************************************************************************

int os_inode_get_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    int err;

    if (os_inode_lut_get(ilut, 1, inode, parent_inode, ftype, len, dentry) != 0) {
        err = os_inode_get(ctx, inode, parent_inode, ftype, len, dentry);
        if (err != 0) return(1);  //** If not found kick out
        os_inode_lut_put(ilut, 1, inode, *parent_inode, *ftype, *len, *dentry); //** Got it from DD so add it to the LUT
    }

    return(0);
}


//****************************************************************************
// os_inode_dir_get - Fetches a record in the dir DBs
//****************************************************************************

int os_inode_dir_get(os_inode_ctx_t *ctx, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    return(os_inode_db_get(ctx->dir, inode, parent_inode, ftype, len, dentry));
}

//****************************************************************************
// os_inode_db_del - Removes an inode from either the inode or dir DBs
//   inode - Inode to remove
//
// Return value
//   On success 0 is returned otherwise 1 is returned if missing
//****************************************************************************

int os_inode_db_del(os_inode_db_t *db, ex_id_t inode)
{
    os_inode_shard_t *s;
    char *errstr = NULL;
    int n;

    //** Remove it from the inode DB
    n = inode % db->n_shards;
    s = db->shard[n];
    rocksdb_delete(s->db, s->wopt, (const char *)&inode, sizeof(ex_id_t), &errstr);
    if (errstr) free(errstr);

    return(0);
}

//****************************************************************************
// os_inode_del - Removes an inode from the inode and dir DBs
//   inode - Inode to remove
//   ftype - inode type if known and 0 otherwise.
//
// Return value
//   On success 0 is returned otherwise 1 is returned if missing
//****************************************************************************

int os_inode_del(os_inode_ctx_t *ctx, ex_id_t inode, int ftype)
{
    //** Remove it from the inode DB

//fprintf(stderr, "os_inode_del: INODE inode=" XIDT "\n", inode);
    os_inode_db_del(ctx->inode, inode);

    //** See if we also need to delete from the dir DB
    if ((ftype == 0) || OS_INODE_IS_DIR(ftype)) { //** Unknown type of a dir so purge from the dir DB
        os_inode_db_del(ctx->dir, inode);
    }

    return(0);
}

//****************************************************************************
// os_inode_del_with_lut - Same as os_inode_del but with a LUT in front of it
//****************************************************************************

int os_inode_del_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode, int ftype)
{
    os_inode_lut_del(ilut, 1, inode);
    os_inode_del(ctx, inode, ftype);

    return(0);
}


//****************************************************************************
// os_inode_lookup2path - Converts info from a lookup into a path using the supplied buffer
//****************************************************************************

void os_inode_lookup2path(char *path, int n, ex_id_t *inode, char **dentry)
{
    int i, pos, nbytes;
    char *etext;

    pos = 0;
    nbytes = OS_PATH_MAX;
    for (i=n-1; i>=0; i--) {
        if (i >= n-2) {
            tbx_append_printf(path, &pos, nbytes, "%s", dentry[i]);
        } else {
            etext = tbx_stk_escape_text("/", '\\', dentry[i]);
            tbx_append_printf(path, &pos, nbytes, "/%s", etext);
            if (etext) free(etext);
        }
    }
}


//****************************************************************************
// os_inode_lookup_path_db - Takes an inode and does a reverse lookup
//    It returns arrays of inodes and dentries that can be used to reconstruct the path.
//    The size of both arrays should be OS_PATH_MAX.  The number of entries
//    isreturned in len.
//
//    NOTE: The entries are in reverse order!!!!!
//
//    If the inode is missing the 1 is returned otherwise 0
//****************************************************************************

int os_inode_lookup_path_db(os_inode_db_t *db, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs)
{
    ex_id_t inode_curr, parent;
    int f, len, i, err;
    char *de;

    *n_dirs = 0;
    inode_curr = inode_target;
    i = 0;
    while ((err = os_inode_db_get(db, inode_curr, &parent, &f, &len, &de)) == 0) {
        inode[i] = inode_curr;
        ftype[i] = f;
        dentry[i] = de;
        inode_curr = parent;

        i++;

        if (parent == 0) break;  //** We've reached the root directory
        if (i == OS_PATH_MAX-1) {  //** We have a loop so flag it
            fprintf(stderr, "ERROR: Hit recursion max! inode_target=" XIDT " curr_inode=" XIDT " curr_parent=" XIDT " cuff_ftype=%d curr_dentry=%s\n", inode_target, inode[i-1], parent, f, de);
            break;
        }
    }

    *n_dirs = i;

    if (err) {  //** Didn't make it to root so clean up
        for (i=0; i<*n_dirs; i++) {
            if (dentry[i]) {
                free(dentry[i]);
                dentry[i] = NULL;
            }
        }

        *n_dirs = 0;
        return(1);
    }

    return(0);
}

//****************************************************************************
// os_inode_lookup_path_db_with_lut - Same as os_inode_lookup_path_db
//    but with an LUT in front of it.
//****************************************************************************

int os_inode_lookup_path_db_with_lut(os_inode_db_t *db, os_inode_lut_t *ilut, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs)
{
    ex_id_t inode_curr, parent;
    int f, len, i, err;
    char *de;

    *n_dirs = 0;
    inode_curr = inode_target;
    i = 0;
    while ((err = os_inode_db_get_with_lut(db, ilut, inode_curr, &parent, &f, &len, &de)) == 0) {
        inode[i] = inode_curr;
        ftype[i] = f;
        dentry[i] = de;
        inode_curr = parent;
        i++;

        if (parent == 0) break;  //** We've reached the root directory
        if (i == OS_PATH_MAX-1) {  //** We have a loop so flag it
            fprintf(stderr, "ERROR: Hit recursion max! inode_target=" XIDT " curr_inode=" XIDT " curr_parent=" XIDT " cuff_ftype=%d curr_dentry=%s\n", inode_target,  inode[i-1], parent, f, de);
            break;
        }
    }

    *n_dirs = i;

    if (err) {  //** Didn't make it to root so clean up
        for (i=0; i<*n_dirs; i++) {
            if (dentry[i]) {
                free(dentry[i]);
                dentry[i] = NULL;
            }
        }

        *n_dirs = 0;
        return(1);
    }

    return(0);
}

//****************************************************************************
// os_inode_lookup_path - Takes an inode and does a reverse lookup
//    It returnsarrays of inodes and dentries that can be used to reconstruct the path.
//    The size of both arrays should be OS_PATH_MAX.  The number of entries
//    isreturned in len.
//
//    NOTE: The entries are in reverse order!!!!!
//
//    If the inode is missing the 1 is returned otherwise 0
//****************************************************************************

int os_inode_lookup_path(os_inode_ctx_t *ctx, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs)
{
    ex_id_t parent;
    int f, len;
    char *de;

    //** Get the terminal from the inode
    if (os_inode_db_get(ctx->inode, inode_target, &parent, &f, &len, &de) == 0) {
        //** The rest of the entries can come from the smaller dir DB
        inode[0] = inode_target;
        ftype[0] = f;
        dentry[0] = de;
        if (parent == 0) {  //** We are looking up '/'
            *n_dirs = 1;
            return(0);
        }
        if (os_inode_lookup_path_db(ctx->dir, parent, inode + 1, ftype + 1, dentry + 1, n_dirs) != 0) {
            if (de) free(de);
            *n_dirs = 0;
            return(1);
        }
        (*n_dirs)++;  //** This accounts for the initial entry, ie 0.
    } else {
        *n_dirs = 0;
        return(1);
    }

    return(0);
}



//****************************************************************************
// os_inode_lookup_path_with_lut - Takes an inode and does a reverse lookup
//    using an in-memory LUT.  Otherwise it's the same as os_inode_lookup_path.
//****************************************************************************

int os_inode_lookup_path_with_lut(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t inode_target, ex_id_t *inode, int *ftype, char **dentry, int *n_dirs)
{
    ex_id_t parent;
    int f, len;
    char *de;

    //** Get the terminal from the inode
    if (os_inode_db_get_with_lut(ctx->inode, ilut, inode_target, &parent, &f, &len, &de) == 0) {
        //** The rest of the entries can come from the smaller dir DB
        inode[0] = inode_target;
        ftype[0] = f;
        dentry[0] = de;
        if (parent == 0) {  //** We are looking up '/'
            (*n_dirs)++;
            return(0);
        }
        if (os_inode_lookup_path_db_with_lut(ctx->dir, ilut, parent, inode + 1, ftype + 1, dentry + 1, n_dirs) != 0) {
            if (de) free(de);
            return(1);
        }
        (*n_dirs)++;
    } else {
        return(1);
    }

    return(0);
}
