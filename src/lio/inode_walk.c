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

#include <lio/ex3_fwd.h>
#include <lio/lio.h>
#include <tbx/stack.h>
#include <tbx/append_printf.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>
#include "os.h"
#include "inode_lut.h"
#include "inode.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

//*************************************************************************
// Routines for walking LStore. Supports recustion but not globbing.
//
// NOTE: Assumes a valid LStore context has been stored in lio_gc.
//*************************************************************************

#define _lio_walk_n_keys 1
char *_lio_walk_keys[] = { "system.inode" };
const int _lio_object_types = OS_OBJECT_FILE_FLAG|OS_OBJECT_DIR_FLAG|OS_OBJECT_SYMLINK_FLAG|OS_OBJECT_HARDLINK_FLAG|OS_OBJECT_FIFO_FLAG|OS_OBJECT_SOCKET_FLAG;

char *_lio_info_keys[] = { "system.inode", "os.type" };

typedef struct {
    os_object_iter_t *it;
    char *prefix;
    char *de_fixed;
    ex_id_t inode_fixed;
    int ftype_fixed;
    int len_fixed;
    int prefix_len;
    int is_fixed;
    int processed;
    ex_id_t dir_inode;
    char *val[_lio_walk_n_keys];
    int v_size[_lio_walk_n_keys];
} walk_lio_dir_t;

typedef struct {
    char *prefix;
    int max_recurse_depth;
    int recurse_depth;
    tbx_stack_t *stack;
    lio_os_regex_table_t *rp_single;
    walk_lio_dir_t *curr;
    lio_path_tuple_t tuple;
} walk_lio_t;


//*************************************************************************

int walk_lio_info(void *arg, char *fname, ex_id_t *inode, int *ftype)
{
    char *val[2];
    int v_size[2];
    int err;

//fprintf(stderr, "walk_lio_info: START fname=%s lio_gc=%p, lio_gc->creds=%p\n", fname, lio_gc, lio_gc->creds);
    err = 0;
    val[0] = val[1] = NULL;
    v_size[0] = v_size[1] = -lio_gc->max_attr;
    lio_get_multiple_attrs(lio_gc, lio_gc->creds, fname, NULL, _lio_info_keys, (void **)val, v_size, 2, 0);

//fprintf(stderr, "walk_lio_info: fname=%s inode=%s ftype=%s\n", fname, val[0], val[1]); fflush(stderr);

    if (val[0] != NULL) {
        sscanf(val[0], XIDT, inode);
    } else {
        err = 1;
        *inode = 0;
    }

    if (val[0] != NULL) {
        sscanf(val[1], "%d", ftype);
    } else {
        err = 1;
        *ftype = 0;
    }

//fprintf(stderr, "walk_lio_info: fname=%s inode=" XIDT " ftype=%d err=%d\n", fname, *inode, *ftype, err);

    if (val[0]) free(val[0]);
    if (val[1]) free(val[1]);

    return(err);
}

//*************************************************************************

int walk_lio_next(void *arg, char **fname, ex_id_t *inode, int *ftype, ex_id_t *parent, int *prefix_len, int *len)
{
    walk_lio_t *w = (walk_lio_t *)arg;
    walk_lio_dir_t *wdir;
    int ftype2, plen;
    char path[OS_PATH_MAX];
    char *dir, *base;

//fprintf(stderr, "walk_lio_next: w=%p w->curr=%p\n", w, w->curr);
    if ((w == NULL) || (w->curr == NULL)) return(1);  //** Kick out if no iterator

    //** See if we are on the fixed prefix
again:
//fprintf(stderr, "AAA: walk_lio_next: prefix=%s is_fixed=%d processed=%d\n", w->curr->prefix, w->curr->is_fixed, w->curr->processed);
    if ((w->curr) && w->curr->is_fixed) {
//fprintf(stderr, "walk_lio_next: is_fixed=%d processed=%d\n", w->curr->is_fixed, w->curr->processed);
        if (w->curr->processed) {
            free(w->curr->de_fixed);
            free(w->curr->prefix);
            free(w->curr);
            w->curr = tbx_stack_pop(w->stack);
            if (w->curr == NULL) {
                return(1);
            }
            goto again;
        } else {
            w->curr->processed = 1;

            if ((strcmp(w->curr->de_fixed, "/") == 0) || (strcmp(w->curr->prefix, "/") == 0)) {
                *len = snprintf(path, OS_PATH_MAX-1, "%s%s", w->curr->prefix, w->curr->de_fixed);
            } else {
                *len = snprintf(path, OS_PATH_MAX-1, "%s/%s", w->curr->prefix, w->curr->de_fixed);
            }

            *fname = strdup(path);
            *prefix_len = (w->curr->prefix_len > 1) ? w->curr->prefix_len + 1 : w->curr->prefix_len;
            *inode = w->curr->inode_fixed;
            *parent =  w->curr->dir_inode;
            *ftype = w->curr->ftype_fixed;
//fprintf(stderr, "walk_lio_next: FIXED fname=%s ftype=%d inode=" XIDT " parent=" XIDT "\n", *fname, *ftype, *inode, *parent);

            return(0);
        }
    }

    //** On the iterator
    //** Grab the next entry
    *ftype = lio_next_object(lio_gc, w->curr->it, fname, &plen);
//fprintf(stderr, "walk_lio_next: fname=%s ftype=%d prefix_len=%d v_size=%d\n", *fname, *ftype, plen, w->curr->v_size[0]);

    if (*ftype <= 0) {
        if (w->curr->it) lio_destroy_object_iter(lio_gc, w->curr->it);
        if (w->curr->de_fixed) free(w->curr->de_fixed);
        if (w->curr->prefix) free(w->curr->prefix);
        free(w->curr);
        w->curr = NULL;
        return(1);  //** finished;
    }

    //** If we made it here we have a valid object and we've already stored the ftype, and fname
    //** We are just missing the inode, parent and the lens
    //** Get the inode
    *len = strlen(*fname);

    wdir = w->curr;
    if (wdir->val[0] != NULL) {
        if (sscanf(wdir->val[0], XIDT, inode) != 1) *inode = OS_INODE_MISSING;
//fprintf(stderr, "walk_lio_next: fname=%s inode=" XIDT "\n", *fname, *inode);
        free(wdir->val[0]);
        wdir->v_size[0] = -lio_gc->max_attr;
        wdir->val[0] = NULL;
    } else {
        wdir->v_size[0] = -lio_gc->max_attr;
        wdir->val[0] = NULL;
        if (*fname) free(*fname);
        return(1);
    }

    //** The only thing left is the parent inode
    //** See if the new fname has the same prefix we used last time
    lio_os_path_split(*fname, &dir, &base);
//fprintf(stderr, "walk_lio_next: prefix=%s dir=%s base=%s\n", wdir->prefix, dir, base);
    if (strcmp(wdir->prefix, dir) == 0) {
        //** It's the same directory
        *parent = wdir->dir_inode;
        *prefix_len = wdir->prefix_len;
        free(dir);
        free(base);
        return(0);
    }

    //** It's a different directory so we have to fetch the parent manually
    if (wdir->prefix) free(wdir->prefix);
    wdir->prefix = dir;
    wdir->prefix_len = (strcmp("/", dir)!=0) ? strlen(dir) + 1 : 1;
    *prefix_len = wdir->prefix_len;
    if (base) free(base);
    if (walk_lio_info(walk_arg, wdir->prefix, parent, &ftype2) != 0) {
//fprintf(stderr, "walk_lio_next: ERROR getting parent. fname=%s dir=%s\n", *fname, wdir->prefix);
        if (*fname) free(*fname);
        return(1);
    }
//fprintf(stderr, "walk_lio_next: fname=%s parent=" XIDT "\n", *fname, *parent);
    wdir->dir_inode = *parent;

    return(0);
}

//*************************************************************************

void walk_lio_add_fixed_prefix(walk_lio_t *w, char *path)
{
    walk_lio_dir_t *wdir;
    char *dir, *old, *base;
    int ftype2, added_root;
    ex_id_t ino;

    added_root = 0;
    old = strdup(path);
    lio_os_path_split(old, &dir, &base);
    walk_info(walk_arg, old, &ino, &ftype2);
//fprintf(stderr, "walk_lio_add_fixed_prefix: START old=%s prefix=%s base=%s inode=" XIDT " ftype=%d\n", old, dir, base, ino, ftype2);
    while (added_root != 1) {
        if (strcmp(base, "/") == 0) added_root++;
        tbx_type_malloc_clear(wdir, walk_lio_dir_t, 1);
        wdir->is_fixed = 1;
        wdir->prefix = (added_root != 1) ? strdup(dir) : strdup("");
        wdir->prefix_len = strlen(wdir->prefix);

//fprintf(stderr, "walk_lio_add_fixed_prefix: old=%s prefix=%s base=%s inode=" XIDT " ftype=%d prefix_len=%d\n", old, dir, base, ino, ftype2, wdir->prefix_len);

        //** Store the subdir dentry
        wdir->inode_fixed = ino;
        wdir->ftype_fixed = ftype2;
        wdir->len_fixed = strlen(base);
        wdir->de_fixed = base;

        //** Need to get and store the parent dir inode
        if (added_root != 1) {
            walk_info(walk_arg, dir, &ino, &ftype2);
            wdir->dir_inode = ino;
//fprintf(stderr, "walk_lio_add_fixed_prefix: prefix=%s dir_inode=" XIDT " ftype=%d\n", wdir->prefix, wdir->dir_inode, ftype2);
        } else {
            wdir->dir_inode = 0;
        }
        tbx_stack_push(w->stack, wdir);
//fprintf(stderr, "walk_lio_add_fixed_prefix: adding prefix=%s dir_inode=" XIDT " de_fixed=%s fixed_ino=" XIDT "\n", wdir->prefix, wdir->dir_inode, wdir->de_fixed, wdir->inode_fixed);

        if (old) free(old);
        old = dir;
        lio_os_path_split(old, &dir, &base);
        walk_info(walk_arg, old, &ino, &ftype2);
//fprintf(stderr, "walk_lio_add_fixed_prefix: LOOP old=%s prefix=%s base=%s inode=" XIDT " ftype=%d\n", old, dir, base, ino, ftype2);
    }

    if (old) free(old);
    if (dir) free(dir);
    if (base) free(base);

    //** Swap out w->curr
    tbx_stack_move_to_bottom(w->stack);
    tbx_stack_insert_below(w->stack, w->curr);
    w->curr = tbx_stack_pop(w->stack);
//fprintf(stderr, "walk_lio_add_fixed_prefix: stack_count=%d curr->prefix=%s curr->is_fixed=%d curr->de_fixed.d_name=%s\n", tbx_stack_count(w->stack), w->curr->prefix, w->curr->is_fixed, w->curr->de_fixed);

    return;
}

//*************************************************************************

void *walk_lio_create(const char *prefix, int max_recurse_depth)
{
    walk_lio_t *w;
    walk_lio_dir_t *wdir;
    int ftype;

    //** Allocate space for the iterator
    tbx_type_malloc_clear(w, walk_lio_t, 1);

    //** Create the tuple and add the wildcard
    w->tuple = lio_path_resolve(lio_gc->auto_translate, (char *)prefix);
    if (w->tuple.is_lio < 0) {
        fprintf(stderr, "Unable to parse path: %s\n", prefix);
        lio_path_release(&(w->tuple));
        free(w);
        return(NULL);
    }
    lio_path_wildcard_auto_append(&(w->tuple));
    w->rp_single = lio_os_path_glob2regex(w->tuple.path);
    if (!w->rp_single) {  //** Got a bad path
        fprintf(stderr,  "ERROR: processing path=%s\n", w->tuple.path);
        lio_path_release(&(w->tuple));
        free(w);
        return(NULL);
    }

    //** Now make the actual iterator
    tbx_type_malloc_clear(wdir, walk_lio_dir_t, 1);
    w->curr = wdir;

    wdir->prefix = strdup(prefix);
    wdir->prefix_len = strlen(wdir->prefix);
    if (wdir->prefix_len < 1) wdir->prefix_len++;
    walk_info(walk_arg, wdir->prefix, &(wdir->dir_inode), &ftype);

    wdir->val[0] = wdir->val[1] = NULL;
    wdir->v_size[0] = wdir->v_size[1] = -lio_gc->max_attr;
    wdir->it = lio_create_object_iter_alist(w->tuple.lc, w->tuple.creds, w->rp_single, NULL, _lio_object_types, max_recurse_depth, _lio_walk_keys, (void **)(wdir->val), wdir->v_size, _lio_walk_n_keys);
    if (wdir->it == NULL) { //**Bad iterator
        fprintf(stderr,  "ERROR: creating iterator! path=%s\n", w->tuple.path);
        lio_path_release(&(w->tuple));
        free(wdir);
        free(w);
        return(NULL);
    }

    //** If we made it here then we have a valid iterator so add the preifx in
    w->stack = tbx_stack_new();
    walk_lio_add_fixed_prefix(w, (char *)prefix);

    return(w);
}

//*************************************************************************

void walk_lio_destroy(void *arg)
{
    walk_lio_t *w = (walk_lio_t *)arg;
    walk_lio_dir_t *wdir;

    if (w->stack) {
        while ((wdir = tbx_stack_pop(w->stack)) != NULL) {
            if (wdir->it) lio_destroy_object_iter(lio_gc, wdir->it);
            if (wdir->de_fixed) free(wdir->de_fixed);
            if (wdir->prefix) free(wdir->prefix);
            free(wdir);
        }
        tbx_stack_free(w->stack, 1);
    }

    if (w->curr) {
        wdir = w->curr;
        if (wdir->it) lio_destroy_object_iter(lio_gc, wdir->it);
        if (wdir->de_fixed) free(wdir->de_fixed);
        if (wdir->prefix) free(wdir->prefix);
        free(wdir);
    }

    lio_path_release(&(w->tuple));

    if (w->rp_single) lio_os_regex_table_destroy(w->rp_single);

    free(w);
}

//*************************************************************************
// Routines for walking the local OS.  Only supports recursion and no globbing
// This is used to test the inode mgmt routines and is more limited than the
// version used for LIO
//*************************************************************************

typedef struct {
    DIR *dir;
    char *prefix;
    struct dirent de_fixed;
    int prefix_len;
    int is_fixed;
    int processed;
    ex_id_t dir_inode;
} walk_local_dir_t;

typedef struct {
    char *prefix;
    int max_recurse_depth;
    int recurse_depth;
    tbx_stack_t *stack;
    walk_local_dir_t *curr;
} walk_local_t;

//*************************************************************************

int walk_local_next(void *arg, char **fname, ex_id_t *inode, int *ftype, ex_id_t *parent, int *prefix_len, int *len)
{
    walk_local_t *w = (walk_local_t *)arg;
    walk_local_dir_t *wdir;
    struct stat link_stat;
    struct stat  obj_stat;
    struct dirent *de;
    char path[OS_PATH_MAX];

//fprintf(stderr, "walk_local_next: w=%p w->curr=%p\n", w, w->curr);
    if ((w == NULL) || (w->curr == NULL)) return(1);  //** Kick out if no iterator

again:
//fprintf(stderr, "AAA: walk_local_next: prefix=%s is_fixed=%d processed=%d\n", w->curr->prefix, w->curr->is_fixed, w->curr->processed);
    if ((w->curr) && w->curr->is_fixed) {
//fprintf(stderr, "walk_local_next: is_fixed=%d processed=%d\n", w->curr->is_fixed, w->curr->processed);
        if (w->curr->processed) {
            free(w->curr->prefix);
            free(w->curr);
            w->curr = tbx_stack_pop(w->stack);
            if (w->curr == NULL) {
                return(1);
            }
            goto again;
        } else {
            de = &(w->curr->de_fixed);
            w->curr->processed = 1;
        }
    } else {
        while ((de = readdir(w->curr->dir)) == NULL) {
            w->recurse_depth--;
            closedir(w->curr->dir);
            free(w->curr->prefix);
            free(w->curr);
            w->curr = tbx_stack_pop(w->stack);
//if (w->curr) {
//    fprintf(stderr, "walk_local_next: prefix=%s is_fixed=%d processed=%d\n", w->curr->prefix, w->curr->is_fixed, w->curr->processed);
//} else {
//    fprintf(stderr, "walk_local_next: curr=NULL\n");
//}
            if (w->curr == NULL) {
                return(1);
            } else if (w->curr->is_fixed) {
                de = &(w->curr->de_fixed);
                w->curr->processed = 1;
                break;
            }
        }
    }

    //** We skip the special entries
    if ((strcmp(de->d_name, "..") == 0) || (strcmp(de->d_name, ".") == 0)) goto again;

    if ((strcmp(de->d_name, "/") == 0) || (strcmp(w->curr->prefix, "/") == 0)) {
        *len = snprintf(path, OS_PATH_MAX-1, "%s%s", w->curr->prefix, de->d_name);
    } else {
        *len = snprintf(path, OS_PATH_MAX-1, "%s/%s", w->curr->prefix, de->d_name);
    }
//fprintf(stderr, "walk_local_next: path=%s curr->prefix=%s d_name=%s\n", path, w->curr->prefix, de->d_name);
    *fname = strdup(path);
    *prefix_len = (w->curr->prefix_len > 1) ? w->curr->prefix_len + 1 : w->curr->prefix_len;
    *inode = de->d_ino;
    *parent =  w->curr->dir_inode;
    *ftype = os_local_filetype_stat(path, &link_stat, &obj_stat);
//fprintf(stderr, "walk_local_next: path=%s curr->prefix=%s d_name=%s ftype=%d rd=%d mrdc=%d is_fixed=%d\n", path, w->curr->prefix, de->d_name, *ftype, w->recurse_depth, w->max_recurse_depth, w->curr->is_fixed);

    if ((w->curr->is_fixed == 0) && OS_INODE_IS_DIR(*ftype) && (w->recurse_depth < w->max_recurse_depth)) { //** Got a directory so recurse into it.
//fprintf(stderr, "walk_local_next: path=%s curr->prefix=%s d_name=%s RECURSING\n", path, w->curr->prefix, de->d_name);
        tbx_type_malloc_clear(wdir, walk_local_dir_t, 1);
        wdir->dir = opendir(*fname);
        if (wdir->dir == NULL) {
            fprintf(stderr, "ERROR: Unable to opendir! path=%s errno=%d\n", *fname, errno);
            free(wdir);
            return(1);
        }

        w->recurse_depth++;
        wdir->prefix = strdup(*fname);
        wdir->prefix_len = strlen(wdir->prefix);
        wdir->dir_inode = *inode;
        tbx_stack_push(w->stack, w->curr);
        w->curr = wdir;
    }

    return(0);
}

//*************************************************************************

void walk_local_add_fixed_prefix(walk_local_t *w, char *path)
{
    walk_local_dir_t *wdir;
    struct stat link_stat;
    struct stat  obj_stat;
    char *dir, *old, *base;
    int ftype2, added_root;
    ex_id_t ino;

    added_root = 0;
    old = strdup(path);
    lio_os_path_split(old, &dir, &base);
    walk_info(walk_arg, old, &ino, &ftype2);
    while (added_root != 1) {
        if (strcmp(base, "/") == 0) added_root++;
        tbx_type_malloc_clear(wdir, walk_local_dir_t, 1);
        wdir->is_fixed = 1;
        wdir->prefix = (added_root != 1) ? strdup(dir) : strdup("");
        wdir->prefix_len = strlen(wdir->prefix);

//fprintf(stderr, "walk_local_add_fixed_prefix: old=%s prefix=%s base=%s\n", old, dir, base);

        //** Store the subdir dentry
        wdir->de_fixed.d_ino = ino;
        wdir->de_fixed.d_type = os_local_filetype_stat(old, &link_stat, &obj_stat);
        wdir->de_fixed.d_reclen = strlen(base);
        strncpy(wdir->de_fixed.d_name, base, sizeof(wdir->de_fixed.d_name));
        free(base);

        //** Need to get and store the parent dir inode
        if (added_root != 1) {
            walk_info(walk_arg, dir, &ino, &ftype2);
            wdir->dir_inode = ino;
//fprintf(stderr, "walk_local_add_fixed_prefix: prefix=%s dir_inode=" XIDT " ftype=%d\n", wdir->prefix, wdir->dir_inode, ftype2);
        } else {
            wdir->dir_inode = 0;
        }
        tbx_stack_push(w->stack, wdir);
//fprintf(stderr, "walk_local_add_fixed_prefix: adding prefix=%s dir_inode=" XIDT " de_fixed=%s fixed_ino=" XIDT "\n", wdir->prefix, wdir->dir_inode, wdir->de_fixed.d_name, wdir->de_fixed.d_ino);

        if (old) free(old);
        old = dir;
        lio_os_path_split(old, &dir, &base);
        walk_info(walk_arg, old, &ino, &ftype2);
    }

    if (old) free(old);
    if (dir) free(dir);
    if (base) free(base);

    //** Swap out w->curr
    tbx_stack_move_to_bottom(w->stack);
    tbx_stack_insert_below(w->stack, w->curr);
    w->curr = tbx_stack_pop(w->stack);
//fprintf(stderr, "walk_local_add_fixed_prefix: stack_count=%d curr->prefix=%s curr->is_fixed=%d curr->de_fixed.d_name=%s\n", tbx_stack_count(w->stack), w->curr->prefix, w->curr->is_fixed, w->curr->de_fixed.d_name);

    return;
}

//*************************************************************************

void *walk_local_create(const char *prefix, int max_recurse_depth)
{
    walk_local_t *w;
    walk_local_dir_t *wdir;
    DIR *dir;
    struct stat link_stat;
    struct stat  obj_stat;

    //** Make sure we can open the directory
    tbx_type_malloc_clear(wdir, walk_local_dir_t, 1);
    dir = opendir(prefix);
    if (dir == NULL) {
        fprintf(stderr, "ERROR: Unable to opendir! path=%s errno=%d\n", prefix, errno);
        free(wdir);
        return(NULL);
    }

    //** Ok now the iterator should be good
    tbx_type_malloc_clear(w, walk_local_t, 1);

    w->prefix = strdup(prefix);
    w->max_recurse_depth = max_recurse_depth;
    w->stack = tbx_stack_new();

    wdir->dir = dir;
    wdir->prefix = strdup(w->prefix);
    wdir->prefix_len = strlen(wdir->prefix);

    os_local_filetype_stat(wdir->prefix, &link_stat, &obj_stat);
    wdir->dir_inode = obj_stat.st_ino;

    w->curr = wdir;
    walk_local_add_fixed_prefix(w, wdir->prefix);

    return(w);
}

//*************************************************************************

void walk_local_destroy(void *arg)
{
    walk_local_t *w = (walk_local_t *)arg;
    walk_local_dir_t *wdir;

    if (w == NULL) return;  //** Kick out if no iterator

    if (w->curr) {
        wdir = w->curr;
        if (wdir->prefix) free(wdir->prefix);
        if (wdir->dir) closedir(wdir->dir);
        free(wdir);
    }
    while ((wdir = tbx_stack_pop(w->stack)) != NULL) {
        if (wdir->prefix) free(wdir->prefix);
        if (wdir->dir) closedir(wdir->dir);
        free(wdir);
    }
    tbx_stack_free(w->stack, 1);

    free(w->prefix);
    free(w);
}

//*************************************************************************

int walk_local_info(void *arg, char *fname, ex_id_t *inode, int *ftype)
{
    struct stat link_stat;
    struct stat  obj_stat;

    *ftype = os_local_filetype_stat(fname, &link_stat, &obj_stat);
//fprintf(stderr, "walk_local_info: fname=%s ftype=%d lino=" XIDT " oino=" XIDT "\n", fname, *ftype, link_stat.st_ino, obj_stat.st_ino);
    if (*ftype == 0) return(1);
    if ((*ftype & OS_OBJECT_SYMLINK_FLAG) == 0) {
        *inode = obj_stat.st_ino;  //** This is the stat for the fname be it a symlink or a normal object
    } else {
        *inode = link_stat.st_ino;  //** This is the stat for the fname be it a symlink or a normal object
    }
    return(0);
}


//*************************************************************************
// os_inode_walk_setup - Sets up the internal routines used to walk the FS
//*************************************************************************

void os_inode_walk_setup(int is_lio)
{
    if (is_lio) {
        walk_create = walk_lio_create;
        walk_destroy = walk_lio_destroy;
        walk_next = walk_lio_next;
        walk_info = walk_lio_info;
    } else {
        walk_create = walk_local_create;
        walk_destroy = walk_local_destroy;
        walk_next = walk_local_next;
        walk_info = walk_local_info;
    }
}

//*************************************************************************
//  The routines below of for merging the changelog
//*************************************************************************



//*************************************************************************
// repair_inode2path - Takes an inode path to an object and checks that
//    it exists in the OS.  If it doesn't it walks the OS looking for the
//    break and corrects the inode and dir DBs as needed
//*************************************************************************

void repair_inode2path(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, int use_dir, ex_id_t *inode, int *ft, char **de, int n_dirs, FILE *fdout)
{
    int i, oops, offset, len, ftype;
    ex_id_t ino;
    os_inode_db_t *db;
    char fname[OS_PATH_MAX];

    db = (use_dir) ? ctx->dir : ctx->inode;

//fprintf(stderr, "repair_inode2path: START n_dirs=%d use_dir=%d\n", n_dirs, use_dir);
    offset = 0;
    oops = -1;
    for (i=n_dirs-1; i>=0; i--) {
        len = strlen(de[i]);
        if (i<(n_dirs-2)) {
            fname[offset] = '/';
            offset++;
        }
        strcpy(fname + offset, de[i]);
        offset += len;

//fprintf(stderr, "repair_inode2path: i=%d de=%s fname=%s\n", i, de[i], fname);
        if (walk_info(walk_arg, fname, &ino, &ftype) != 0) {  //** Stat failed:(
            fprintf(fdout, "repair_inode2path: A. REMOVING inode=" XIDT " de=%s\n", inode[i], de[i]);
            os_inode_db_del(db, inode[i]);
            if (ilut) os_inode_lut_del(ilut, 1, inode[i]);
            oops = i-1;
            break;
        }
    }

//fprintf(stderr, "repair_inode2path: oops=%d\n", oops);

    //** The rest of the entries are unlinked so remove them
    for (i=oops; i>0; i--) {
        os_inode_db_del(db, inode[i]);
        if (ilut) os_inode_lut_del(ilut, 1, inode[i]);
        fprintf(fdout, "repair_inode2path: B. REMOVING inode=" XIDT " de=%s\n", inode[i], de[i]);
    }

//fprintf(stderr, "repair_inode2path: END n_dirs=%d use_dir=%d\n", n_dirs, use_dir);

    return;
}

//*************************************************************************
//  repair_path2inode - Takes an OS path that exists and makes sure all the
//      inode and dir entries exist and are correct
//*************************************************************************

void repair_path2inode(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, char *path, int ftype, FILE *fdout)
{
    int ft[OS_PATH_MAX];
    char *de[OS_PATH_MAX];
    ex_id_t inode[OS_PATH_MAX];
    char *dir, *old, *base;
    int i, n_dir, ftype2;
    ex_id_t ino, parent;

//fprintf(stderr, "repair_path2inode: START: fname=%s\n", path); fflush(stderr);

    i = 0;
    old = NULL;
    lio_os_path_split(path, &dir, &base);
//fprintf(stderr, "repair_path2inode: fname=%s dir=%s file=%s\n", path, dir, base); fflush(stderr);
    walk_info(walk_arg, path, &ino, &ftype2);
//fprintf(stderr, "repair_path2inode: start: i=%d inode=" XIDT " de=%s  path=%s dir=%s base=%s\n", i, ino, base, path, dir, base);
    while ((strcmp(base, "/") != 0) && (ino != 0)) {
        de[i] = base;
        inode[i] = ino;
        ft[i] = ftype2;
        if (old) free(old);
        old = dir;
        i++;
        lio_os_path_split(old, &dir, &base);
        walk_info(walk_arg, old, &ino, &ftype2);
//fprintf(stderr, "repair_path2inode: i=%d inode=" XIDT " de=%s  old=%s dir=%s base=%s\n", i, ino, base, old, dir, base);
    }

    if (dir) free(dir);
    if (old) free(old);

    //** Add the root entry
    de[i] = base;
    inode[i] = ino;
    ft[i] = ftype2;
    n_dir = i+1;

//for (i=0; i<n_dir; i++) {
//    fprintf(stderr, "repair_path2inode: loop: i=%d inode=" XIDT " de=%s\n", i, inode[i], de[i]);
//}

    //** Now go through adding or updating the entries
    if (n_dir == 1) {
        fprintf(fdout, "repair_path2inode: ADDING ROOT path=%s inode=" XIDT " parent=0 de=%s\n", path, inode[0], de[0]);
        os_inode_put(ctx, inode[0], 0, ft[0], strlen(de[0]), de[0]);
        if (ilut) os_inode_lut_put(ilut, 1, inode[0], 0, ft[0], strlen(de[0]), de[0]);
        if (de[0]) free(de[0]);
    } else {
        for (i=n_dir-1; i>=0; i--) {
            parent = (i==(n_dir-1)) ? 0 : inode[i+1];
            fprintf(fdout, "repair_path2inode: ADDING path=%s inode=" XIDT " parent=" XIDT " de=%s\n", path, inode[i], parent, de[i]);
            os_inode_put(ctx, inode[i], parent, ft[i], strlen(de[i]), de[i]);
            if (ilut) os_inode_lut_put(ilut, 1, inode[i], parent, ft[i], strlen(de[i]), de[i]);
            if (de[i]) free(de[i]);
        }
    }

    return;
}

//*************************************************************************
// inode_fsck_path - Checks and makes any corrections needed to find the fname
//    in the inode DB's.  If the path doesn't resolve then 1 is returned
//    otherwise 0 and the inode of the path object is returned
//*************************************************************************

static int inode_fsck_path(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, char *path, ex_id_t *path_inode, FILE *fdout)
{
    int ftype, n, start, ftype2, len, i, ran_path2inode;
    int ft[OS_PATH_MAX];
    ex_id_t inode[OS_PATH_MAX];
    ex_id_t parent_inode, pino;
    char *dentry;
    char *de[OS_PATH_MAX];
    char fname[OS_PATH_MAX];

//fprintf(stderr, "inode_fsck_path: START path=%s walk_info=%p\n", path, walk_info); fflush(stderr);

    //** See if the path exists
    if (walk_info(walk_arg, path, path_inode, &ftype) != 0) return(1);

//fprintf(stderr, "inode_fsck_path: after walk_info path=%s inode=" XIDT " ftype=%d\n", path, *path_inode, ftype); fflush(stderr);
    n = 0;
    ran_path2inode = 0;
    dentry = NULL;

    //** If we made it here the path exists so now check that it's in the DBs
    //** Check the inode 1st since it will have the terminal if it's a file
//fprintf(stderr, "inode_fsck_path: AAAAAAA\n");

    if (os_inode_lookup_path_db(ctx->inode, *path_inode, inode, ft, de, &n) == 0) {
//fprintf(stderr, "inode_fsck_path: BBBBB\n");
        os_inode_lookup2path(fname, n, inode, de);
//fprintf(stderr, "inode_fsck_path: strcmp path=%s fname=%s ftype=%d ft[0]=%d\n", path, fname, ftype, ft[0]);
        if (strcmp(fname, path) != 0) { //** Path mismatch
            ran_path2inode = 1;
            repair_path2inode(ctx, ilut, path, ftype, fdout);   //** This will add missing entries
            repair_inode2path(ctx, ilut, 0, inode, ft, de, n, fdout);  //** And remove orphans
        } else if (ftype != ft[0]) { //** ftype mismatch
            pino = (n == 1) ? 0 : inode[1];
            os_inode_put(ctx, inode[0], pino, ftype, strlen(de[0]), de[0]);
            if (ilut) os_inode_lut_put(ilut, 1, inode[0], pino, ftype, strlen(de[0]), de[0]);
            fprintf(fdout, "inode_fsck_path: REPAIRING ftype path=%s inode=" XIDT " de=%s ftype=%d\n", fname, inode[0], de[0], ftype);
        }
    } else {  //** We failed the inode lookup but we know the path exists so add it
//fprintf(stderr, "inode_fsck_path: MISSING INODE path=%s inode=" XIDT "\n", path, *path_inode); fflush(stderr);
        ran_path2inode = 1;
        repair_path2inode(ctx, ilut, path, ftype, fdout);   //** This will add missing entries
//fprintf(stderr, "inode_fsck_path: MISSING INODE 1111111 path=%s inode=" XIDT "\n", path, *path_inode); fflush(stderr);
        os_inode_lookup_path_db(ctx->inode, *path_inode, inode, ft, de, &n); //** This will work now
//fprintf(stderr, "inode_fsck_path: MISSING INODE 222222 path=%s inode=" XIDT "\n", path, *path_inode); fflush(stderr);
    }

//fprintf(stderr, "inode_fsck_path: CCCCCCCC n=%d\n", n); fflush(stderr);

    //** Check the dir DB
    //** The dir DB only has directories so adjust as needed
    if (OS_INODE_IS_DIR(ftype)) {
        start = 0;
        parent_inode = inode[0];
    } else {
        start = 1;
        if (os_inode_get(ctx, *path_inode, &parent_inode, &ftype2, &len, &dentry) != 0) {
            if (dentry) free(dentry);
            goto finished;
        }
    }
//fprintf(stderr, "inode_fsck_path: DDDDDDDDDD start=%d n=%d\n", start, n); fflush(stderr);
//for (i=0; i<n; i++) {
//  if (de[i]) { fprintf(stderr, "inode_fsck_path: start=%d n=%d i=%d de=%s\n", start, n, i, de[i]); }
//}
//fflush(stderr);

    //** Clean up the old dentry strings
    if (dentry) free(dentry);
    for (i=start; i<n; i++) {
        if (de[i]) { free(de[i]); de[i] = NULL; }
    }

    n = 0;

    //** Now do the check
    if (os_inode_lookup_path_db(ctx->dir, parent_inode, inode + start, ft + start, de + start, &n) == 0) {
        n = n + start;  //** Adjust the number of entries
        os_inode_lookup2path(fname, n, inode, de);
        if (strcmp(fname, path) != 0) { //** Mismatch which needs to be corrected
            if (!ran_path2inode) repair_path2inode(ctx, ilut, path, ftype, fdout);
            repair_inode2path(ctx, ilut, 1, inode, ft, de, n, fdout);
        }
    } else {
        n = start;
    }

finished:
    for (i=0; i<n; i++) {
        if (de[i]) free(de[i]);
    }

//fprintf(stderr, "inode_fsck_path: END path=%s inode=" XIDT "\n", path, *path_inode);

    return(0);
}

//*************************************************************************
// inode_fsck_inode - Checks and makes any corrections needed to find the inode
//*************************************************************************

static int inode_fsck_inode(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, ex_id_t changelog_inode, int ftype, FILE *fdout)
{
    int n, ftype2, len, i;
    int ft[OS_PATH_MAX];
    ex_id_t inode[OS_PATH_MAX];
    ex_id_t parent_inode, path_inode;
    char *dentry;
    char *de[OS_PATH_MAX];
    char fname[OS_PATH_MAX];

    //** See if we have an entry
    if (os_inode_get(ctx, changelog_inode, &parent_inode, &ftype2, &len, &dentry) != 0) {
        goto check_dir;
    }

    //** We have an entry so convert it to a path just using the inode DB
    if (os_inode_lookup_path_db(ctx->inode, changelog_inode, inode, ft, de, &n) != 0) {
        os_inode_del(ctx, changelog_inode, ftype);  //** It failed so remove it
        if (ilut) os_inode_lut_del(ilut, 1, changelog_inode);  //** It failed so remove it
        goto check_dir;
    }

    //** See if the object exists
    os_inode_lookup2path(fname, n, inode, de);
    if (walk_info(walk_arg, fname, &path_inode, &ftype) != 0) { //** It's broken
        repair_inode2path(ctx, ilut, 0, inode, ft, de, n, fdout);  //** And remove orphans
    }

    //** Clean up the old dentry strings
    for (i=0; i<n; i++) {
        if (de[i]) free(de[i]);
    }

check_dir:
    if (dentry) free(dentry);

    //** Check the dir DB
    //** The dir DB only has directories so kick out if not a directory
    if (!OS_INODE_IS_DIR(ftype)) return(0);

    //** See if we have an entry
    if (os_inode_db_get(ctx->dir, changelog_inode, &parent_inode, &ftype2, &len, &dentry) != 0) {
        return(0);
    }

    //** We have an entry so convert it to a path just using the dir DB
    if (os_inode_lookup_path_db(ctx->dir, changelog_inode, inode, ft, de, &n) != 0) {
        os_inode_del(ctx, changelog_inode, ftype);  //** It failed so remove it
        return(0);
    }

    //** See if the object exists
    os_inode_lookup2path(fname, n, inode, &dentry);
    if (walk_info(walk_arg, fname, &path_inode, &ftype) != 0) { //** It's broken
        repair_inode2path(ctx, ilut, 1, inode, ft, de, n, fdout);  //** And remove orphans
    }

    //** Clean up the old dentry strings
    for (i=0; i<n; i++) {
        if (de[i]) free(de[i]);
    }

    return(0);
}

//*************************************************************************
// parse_inode_entry - Parses the changelog entry
//   fname and msg are preallocated buffers which upon return contains the
//   changelog msg and the fname.
//   If no errors are encountered 0 is returned otherwise 1.
//*************************************************************************

int parse_inode_entry(FILE *fd, char *entry, char *msg, ex_id_t *inode, int *ftype, char *fname)
{
    int n;
    msg[0] = fname[0] = '\0';
    n = sscanf(entry, "%s " XIDT " %d %[^\n]s\n", msg, inode, ftype, fname);
    if (strcmp(msg, "ERROR-LUT") == 0) {         //** These we skip
        fprintf(fd, "SKIPPING: %s\n", msg);
        return(1);
    }
//fprintf(stderr, "QWERT: parse_inode_entry: n=%d\n", n);
    if (n != 4) {  //** Malformed line
        fprintf(fd, "MALFORMED-ENTRY: sscanf=%d\n", n);
        return(2);
    }
    return(0);
}

//*************************************************************************
// process_entry - Process a changelog merge entry
//*************************************************************************

void os_inode_process_entry(os_inode_ctx_t *ctx, os_inode_lut_t *ilut, char *entry, FILE *fdout)
{
    char msg[OS_PATH_MAX], fname[OS_PATH_MAX];
    ex_id_t inode, path_inode;
    int ftype;

    //** Parse entry
    if (parse_inode_entry(fdout, entry, msg, &inode, &ftype, fname) != 0) return;

//fprintf(stderr, "process_entry: START fname=%s inode=" XIDT " ftype=%d\n", fname, inode, ftype); fflush(stderr);
    if (inode == OS_INODE_MISSING) {
        fprintf(fdout, "process_entry: %s " XIDT " %s\n", OS_MSG_NO_INODE, inode, fname);
    }
    if (strcmp(msg, OS_MSG_DELETE_INODE) == 0) {
        os_inode_del(ctx, inode, 0);
        if (ilut) os_inode_lut_del(ilut, 1, inode);
        fprintf(fdout, "process_entry: %s " XIDT "\n", OS_MSG_DELETE_INODE, inode);
        return;
    }

//fprintf(stderr, "process_entry: before fsck_path fname=%s inode=" XIDT " ftype=%d\n", fname, inode, ftype); fflush(stderr);

    //** Check the fname is good
    if (inode_fsck_path(ctx, ilut, fname, &path_inode, fdout) == 0) {
//        if (path_inode == inode) return;  //*If the path and entry inode are the same then nothing left
//FIXME or KEEPME???
if (path_inode == inode) {
    fprintf(stderr, "process_entry: END A fname=%s inode=" XIDT " ftype=%d\n", fname, inode, ftype); fflush(stderr);
    return;
}

    }

//fprintf(stderr, "process_entry: before fsck_inode fname=%s inode=" XIDT " ftype=%d\n", fname, inode, ftype); fflush(stderr);
    inode_fsck_inode(ctx, ilut, inode, ftype, fdout);
//fprintf(stderr, "process_entry: END B fname=%s inode=" XIDT " ftype=%d\n", fname, inode, ftype);
}
