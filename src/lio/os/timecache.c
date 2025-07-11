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
// Remote OS implementation for the client side
//***********************************************************************

//#define _log_module_index 213

#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "authn.h"
#include "ex3/system.h"
#include "os.h"
#include "os/file.h"
#include "os/remote.h"
#include "os/timecache.h"

#define OSTC_LOCK(ostc); log_printf(5, "LOCK\n"); apr_thread_mutex_lock(ostc->lock)
#define OSTC_UNLOCK(ostc) log_printf(5, "UNLOCK\n"); apr_thread_mutex_unlock(ostc->lock)

#define OSTC_NOCACHE_MATCH(key) ((strncmp((key), "os.lock", 7) == 0) && ( (*((key)+7) == '\0') || (strcmp((key)+7, ".user") == 0) ) ) || ((strncmp((key), "system.exnode", 13) == 0) && ( (*((key)+13) == '\0') || (strcmp((key)+13, ".data") == 0) ) )

#define OSTC_ITER_ALIST  0
#define OSTC_ITER_AREGEX 1

#define OSTC_MAX_RECURSE 500

#define OS_ATTR_LINK "os.attr_link"
#define OS_ATTR_LINK_LEN 12
#define OS_LINK "os.link"

typedef struct {
    int ftype;
    int link_count;
    ex_id_t inode;
} ostc_base_object_info_t;

typedef struct {
    char *key;
    void *val;
    char *link;
    int v_size;
    int type;
    apr_time_t expire;
} ostcdb_attr_t;

typedef struct ostcb_object_s {
    char *fname;
    struct ostcb_object_s *hard_obj;
    int ftype;
    int link_count;
    ex_id_t inode;
    int n_refs;
    char *link;
    char *realpath;
    int rp_len;
    apr_pool_t *mpool;
    apr_hash_t *objects;
    apr_hash_t *attrs;
    apr_time_t expire;
} ostcdb_object_t;

typedef struct {
    char *fname;
    int mode;
    int mflags;
    int max_wait;
    lio_creds_t *creds;
    char *id;
    os_fd_t *fd_child;
} ostc_fd_t;

typedef struct {
    int n_keys;
    int n_keys_total;
    int ftype_index;
    int link_count_index;
    int inode_index;
    char **key;
    void **val;
    int *v_size;
} ostc_cacheprep_t;


typedef struct {
    lio_object_service_fn_t *os;
    os_attr_iter_t *it;
} ostc_attr_iter_t;

typedef struct {
    lio_object_service_fn_t *os;
    gop_op_generic_t *gop;
} ostc_remove_regex_t;

typedef struct {
    lio_object_service_fn_t *os;
    os_object_iter_t **it_child;
    ostc_attr_iter_t it_attr;
    lio_os_regex_table_t *attr;
    ostc_fd_t fd;
    lio_creds_t *creds;
    void **val;
    int *v_size;
    int *v_size_initial;
    int n_keys;
    int v_max;
    ostc_cacheprep_t cp;
    int iter_type;
} ostc_object_iter_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    ostc_fd_t *fd;
    ostc_fd_t *fd_dest;
    char *path;
    char **src_path;
    char **key;
    char **key_dest;
    void **val;
    char *key_tmp;
    char *src_tmp;
    void *val_tmp;
    int *v_size;
    int v_tmp;
    int n;
    int is_immediate;
} ostc_mult_attr_t;

typedef struct {
    lio_object_service_fn_t *os;
    lio_creds_t *creds;
    char *src_path;
    char *dest_path;
    int ftype;
    char *id;
    char **key;
    void **val;
    int *v_size;
    int n_keys;
} ostc_object_op_t;

typedef struct {
    char *string;
    regex_t regex;
    int timeout;
} limit_cache_t;

typedef struct {
    char *section;
    char *os_child_section;
    limit_cache_t *limit_cache;
    lio_object_service_fn_t *os_child;//** child OS which does the heavy lifting
    apr_thread_mutex_t *lock;
    apr_thread_mutex_t *delayed_lock;
    apr_thread_cond_t *cond;
    apr_pool_t *mpool;
    gop_thread_pool_context_t *tpc;
    ostcdb_object_t *cache_root;
    apr_hash_t *hardlink_objects;
    apr_time_t entry_timeout;
    apr_time_t cleanup_interval;
    apr_thread_t *cleanup_thread;
    ex_off_t n_objects_created;
    ex_off_t n_objects_removed;
    ex_off_t n_attrs_created;
    ex_off_t n_attrs_removed;
    ex_off_t n_attrs_hit;
    ex_off_t n_attrs_miss;
    int n_limit_cache;
    int shutdown;
} ostc_priv_t;

typedef struct {
    lio_object_service_fn_t *os;
    os_fd_t **fd;
    os_fd_t *close_fd;
    lio_creds_t *creds;
    gop_op_generic_t *gop;
    os_fd_t *cfd;
    char *path;
    char *id;
    int mode;
    int max_wait;
} ostc_open_op_t;

static ostc_priv_t ostc_default_options = {
    .section = "os_timecache",
    .os_child_section = "os_remote_client",
    .entry_timeout = 60,
    .cleanup_interval = 120
};

void _ostc_update_link_count_object(lio_object_service_fn_t *os,  ostcdb_object_t *obj, int delta);
gop_op_status_t ostc_close_object_fn(void *arg, int tid);
gop_op_status_t ostc_delayed_open_object(lio_object_service_fn_t *os, ostc_fd_t *fd);

//***********************************************************************
// _ostc_count - Counts the objects and attributes
//     NOTE: ostc->lock must be held by the calling process
//***********************************************************************

void _ostc_count(ostcdb_object_t *obj, ex_off_t *n_objs, ex_off_t *n_attrs)
{
    ostcdb_object_t *o;
    apr_hash_index_t *hi;

    if (obj == NULL) return;  //** Nothing to do so return

    //** Recursively count the objects
    if (obj->objects) {
        for (hi = apr_hash_first(NULL, obj->objects); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, NULL, (void **) &o);
            _ostc_count(o, n_objs, n_attrs);
        }
    }

    //** Count my attributes and myself
    (*n_objs)++;
    if (obj->attrs) (*n_attrs) += apr_hash_count(obj->attrs);
}

//***********************************************************************
// _ostc_hard_link_count - Counts the hardlink objects and attributes
//     NOTE: ostc->lock must be held by the calling process
//***********************************************************************

void _ostc_hardlink_count(apr_hash_t *hardlink_objs, ex_off_t *n_objs, ex_off_t *n_attrs)
{
    ostcdb_object_t *obj;
    apr_hash_index_t *hi;

    *n_objs += apr_hash_count(hardlink_objs);
    for (hi = apr_hash_first(NULL, hardlink_objs); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **) &obj);
        if (obj->attrs) *n_attrs += apr_hash_count(obj->attrs);
    }
}


//***********************************************************************
// ostc_info_fn - Dumps the time cache info
//***********************************************************************

void ostc_info_fn(void *arg, FILE *fd)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ex_off_t no, na, co, ca, wo, wa, ho, ha;
    double d;

    fprintf(fd, "OS Timecache Usage------------------------\n");
    OSTC_LOCK(ostc);
    fprintf(fd, "Created           -- n_objects: " XOT " n_attrs: " XOT "\n", ostc->n_objects_created, ostc->n_attrs_created);
    fprintf(fd, "Removed           -- n_objects: " XOT " n_attrs: " XOT "\n", ostc->n_objects_removed, ostc->n_attrs_removed);

    co = ostc->n_objects_created - ostc->n_objects_removed;
    ca = ostc->n_attrs_created - ostc->n_attrs_removed;
    fprintf(fd, "Calculated        -- n_objects: " XOT " n_attrs: " XOT "\n", co, ca);

    wo = wa = 0;
    _ostc_count(ostc->cache_root, &wo, &wa);
    fprintf(fd, "Walked            -- n_objects: " XOT " n_attrs: " XOT "\n", wo, wa);

    ho = ha = 0;
    _ostc_hardlink_count(ostc->hardlink_objects, &ho, &ha);
    fprintf(fd, "Walked Hard Links -- n_objects: " XOT " n_attrs: " XOT "\n", ho, ha);

    no = wo+ho-co; na = wa+ha-ca;
    if ((no != 0) || (na != 0)) {
        fprintf(fd, "ERROR(c-w)        -- n_objects: " XOT " n_attrs: " XOT "\n", no, na);
    }

    d = ostc->n_attrs_hit;
    no = ostc->n_attrs_hit + ostc->n_attrs_miss;
    if (no > 0) d /= no;
    fprintf(fd, "Attr Cache        -- hits: " XOT " miss: " XOT " hit/miss ratio: %lf\n", ostc->n_attrs_hit, ostc->n_attrs_miss, d);
    fprintf(fd, "\n");
    OSTC_UNLOCK(ostc);
}

//***********************************************************************
// free_ostcdb_attr - Destroys a cached attribute
//***********************************************************************

void free_ostcdb_attr(ostcdb_attr_t *attr)
{
    log_printf(5, "removing\n");
    if (attr->key) free(attr->key);
    if (attr->val) free(attr->val);
    if (attr->link) free(attr->link);
    free(attr);
}

//***********************************************************************
// _new_ostcdb_attr - Creates a new attr object
//***********************************************************************

ostcdb_attr_t *new_ostcdb_attr(char *key, void *val, int v_size, apr_time_t expire)
{
    ostcdb_attr_t *attr;

    log_printf(5, "adding key=%s size=%d\n", key, v_size);
    tbx_type_malloc_clear(attr, ostcdb_attr_t, 1);

    attr->key = strdup(key);
    if (v_size > 0) {
        tbx_type_malloc(attr->val, void, v_size+1);
        memcpy(attr->val, val, v_size);
        ((char *)(attr->val))[v_size] = 0;  //** NULL terminate

    }
    attr->v_size = v_size;
    attr->expire = expire;

    return(attr);
}

//***********************************************************************
// free_ostcdb_object - Destroys a cache object
//    Accumulates the number of objects and attrs removed in the
//    provided counters.
//***********************************************************************

void free_ostcdb_object(lio_object_service_fn_t *os, ostcdb_object_t *obj, ex_off_t *n_objs, ex_off_t *n_attrs)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostcdb_object_t *o;
    ostcdb_attr_t *a;
    apr_hash_index_t *ohi;
    apr_hash_index_t *ahi;

    //** Free my attributes
    if (obj->attrs) {
        for (ahi = apr_hash_first(NULL, obj->attrs); ahi != NULL; ahi = apr_hash_next(ahi)) {
            apr_hash_this(ahi, NULL, NULL, (void **) &a);
            free_ostcdb_attr(a);
            (*n_attrs)++;
        }
    }

    //** Now free all the children objects
    if (obj->objects != NULL) {
        for (ohi = apr_hash_first(NULL, obj->objects); ohi != NULL; ohi = apr_hash_next(ohi)) {
            apr_hash_this(ohi, NULL, NULL, (void **) &o);
            free_ostcdb_object(os, o, n_objs, n_attrs);
        }
    }

    //** See if we free up the hardlink is used
    if (obj->hard_obj) {
        obj->hard_obj->n_refs--;
        if (obj->hard_obj->n_refs == 0) {
            apr_hash_set(ostc->hardlink_objects, &(obj->hard_obj->inode), sizeof(ex_id_t), NULL);
            free_ostcdb_object(os, obj->hard_obj, n_objs, n_attrs);
        }
    }

    if (obj->realpath != NULL) free(obj->realpath);
    if (obj->fname != NULL) free(obj->fname);
    if (obj->link != NULL) free(obj->link);
    if (obj->mpool) apr_pool_destroy(obj->mpool);
    free(obj);
    (*n_objs)++;
}

//***********************************************************************
// new_ostcdb_object - Creates a new cache object
//***********************************************************************

ostcdb_object_t *new_ostcdb_object_base(apr_time_t expire)
{
    ostcdb_object_t *obj;

    tbx_type_malloc_clear(obj, ostcdb_object_t, 1);

    obj->expire = expire;

    return(obj);
}

//***********************************************************************

ostcdb_object_t *new_ostcdb_object(char *entry, ostc_base_object_info_t *base, apr_time_t expire, apr_pool_t *mpool)
{
    ostcdb_object_t *obj;

    obj = new_ostcdb_object_base(expire);

    obj->fname = entry;
    obj->ftype = base->ftype;
    obj->inode = base->inode;
    obj->link_count = base->link_count;
    obj->link = NULL;
    apr_pool_create(&(obj->mpool), NULL);
    obj->objects = (base->ftype & OS_OBJECT_DIR_FLAG) ? apr_hash_make(obj->mpool) : NULL;  //** FIXME - properly handle symlinks
    obj->attrs = apr_hash_make(obj->mpool);

    return(obj);
}

//***********************************************************************
// _ostc_cleanup - Clean's out the cache of expired objects/attributes
//     NOTE: ostc->lock must be held by the calling process
//***********************************************************************

int _ostc_cleanup(lio_object_service_fn_t *os, ostcdb_object_t *obj, apr_time_t expired)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostcdb_object_t *o;
    ostcdb_attr_t *a;
    apr_hash_index_t *hi;
    int okept, akept, result;

    if (obj == NULL) return(0);  //** Nothing to do so return

    //** Recursively prune the objects
    okept = 0;
    if (obj->objects) {
        for (hi = apr_hash_first(NULL, obj->objects); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, NULL, (void **) &o);
            result = _ostc_cleanup(os, o, expired);
            okept += result;
            if (result == 0) {
                apr_hash_set(obj->objects, o->fname, APR_HASH_KEY_STRING, NULL);
                free_ostcdb_object(os, o, &ostc->n_objects_removed, &ostc->n_attrs_removed);
            }
        }
    }

    //** Free my expired attributes
    akept = 0;
    if (obj->hard_obj == NULL) {
        for (hi = apr_hash_first(NULL, obj->attrs); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, NULL, (void **) &a);
            log_printf(5, "fname=%s attr=%s a->expire=" TT " expired=" TT "\n", obj->fname, a->key, a->expire, expired);
            if (a->expire < expired) {
                apr_hash_set(obj->attrs, a->key, APR_HASH_KEY_STRING, NULL);
                free_ostcdb_attr(a);
                ostc->n_attrs_removed++;
            } else {
                akept++;
            }
        }
    }

    log_printf(5, "fname=%s akept=%d okept=%d o+a=%d\n", obj->fname, akept, okept, akept+okept);
    tbx_monitor_thread_message(MON_MY_THREAD, "_ostc_cleanup: fname=%s akept=%d okept=%d o+a=%d", obj->fname, akept, okept, akept+okept);

    return(akept + okept);
}

//***********************************************************************
// ostc_cache_compact_thread - Thread for cleaning out the cache
//***********************************************************************

void *ostc_cache_compact_thread(apr_thread_t *th, void *data)
{
    lio_object_service_fn_t *os = (lio_object_service_fn_t *)data;
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    tbx_monitor_thread_create(MON_MY_THREAD, "ostc_cache_compact_thread");
    OSTC_LOCK(ostc);
    while (ostc->shutdown == 0) {
        apr_thread_cond_timedwait(ostc->cond, ostc->lock, ostc->cleanup_interval);

        log_printf(5, "START: Running an attribute cleanup\n");
        _ostc_cleanup(os, ostc->cache_root, apr_time_now());
        log_printf(5, "END: cleanup finished\n");
    }
    OSTC_UNLOCK(ostc);
    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
// merge_prefix_with_link - Merges an existing prefix with a link and returns
//      a new string and stores it in merged_path.
//***********************************************************************

void merge_prefix_with_link(char *merged_path, char *prefix, int n_prefix, char *link)
{
    int i;

    if (link[0] == '/') { //**  Absolute path so just copy it and return
        i = strlen(link) + 1;
        memcpy(merged_path, link, i);
    } else {  //** Relative path so have to do a merge
        memcpy(merged_path, prefix, n_prefix);
        merged_path[n_prefix] = '/';
        i = strlen(link) + 1;
        memcpy(merged_path + n_prefix+1, link, i);
    }

    return;
}

//***********************************************************************
// ostc_attr_cacheprep_setup - Sets up the arrays for storing the attributes
//    in the time cache.
//***********************************************************************

void ostc_attr_cacheprep_setup(ostc_cacheprep_t *cp, int n_keys, char **key_src, void **val_src, int *v_size_src)
{
    int i, j, n;
    void **vd;
    char **kd;
    int *vsd;

    cp->ftype_index = -1;
    cp->link_count_index = -1;
    cp->inode_index = -1;

    //** Make the full list of attrs to get
    tbx_type_malloc(kd, char *, 2*n_keys+4);
    tbx_type_malloc_clear(vd, void *, 2*n_keys+4);
    tbx_type_malloc_clear(vsd, int, 2*n_keys+4);
    memcpy(kd, key_src, sizeof(char *) * n_keys);
    memcpy(vsd, v_size_src, sizeof(int) * n_keys);
    memcpy(vd, val_src, sizeof(void *) * n_keys);

    for (i=n_keys; i<2*n_keys; i++) {
        n = strlen(key_src[i-n_keys]);
        j = n + OS_ATTR_LINK_LEN + 1 + 1;
        tbx_type_malloc(kd[i], char, n + OS_ATTR_LINK_LEN + 1 + 1);
        snprintf(kd[i], j, "%s.%s", OS_ATTR_LINK, kd[i-n_keys]);
        vsd[i] = -OS_PATH_MAX;
        log_printf(5, "key[%d]=%s\n", i, kd[i]);
    }
    kd[2*n_keys] = strdup(OS_LINK);
    vsd[2*n_keys] = -OS_PATH_MAX;

    cp->n_keys = n_keys;
    cp->n_keys_total = 2*n_keys+1;
    cp->key = kd;
    cp->val = vd;
    cp->v_size = vsd;

    for (i=0; i<n_keys; i++) {
        if (strcmp(key_src[i], "os.type") == 0) {
            cp->ftype_index = i;
            break;
        } else if (strcmp(key_src[i], "os.link_count") == 0) {
            cp->link_count_index = i;
            break;
        } else if (strcmp(key_src[i], "system.inode") == 0) {
            cp->inode_index = i;
            break;
        }
    }

    n = 2*n_keys;
    if (cp->ftype_index == -1) {
        n++;
        cp->ftype_index = n;;
        cp->key[cp->ftype_index] = strdup("os.type");
        cp->v_size[cp->ftype_index] = -OS_PATH_MAX;
    }
    if (cp->link_count_index == -1) {
        n++;
        cp->link_count_index = n;;
        cp->key[cp->link_count_index] = strdup("os.link_count");
        cp->v_size[cp->link_count_index] = -OS_PATH_MAX;
    }
    if (cp->inode_index == -1) {
        n++;
        cp->inode_index = n;;
        cp->key[cp->inode_index] = strdup("system.inode");
        cp->v_size[cp->inode_index] = -OS_PATH_MAX;
    }

    cp->n_keys_total = n+1;
}

//***********************************************************************
// ostc_attr_cacheprep_destroy - Destroys the structures created in the setup routine
//***********************************************************************

void ostc_attr_cacheprep_destroy(ostc_cacheprep_t *cp)
{
    int i;

    for (i=cp->n_keys; i<cp->n_keys_total; i++) {
        if (cp->key != NULL) {
            if (cp->key[i] != NULL) free(cp->key[i]);
        }
        if (cp->val != NULL) {
            if (cp->val[i] != NULL) free(cp->val[i]);
        }
    }

    if (cp->key != NULL) free(cp->key);
    if (cp->val != NULL) free(cp->val);
    if (cp->v_size != NULL) free(cp->v_size);
}

//***********************************************************************
//  ostc_attr_cacheprep_copy - Copies the user attributes back out
//***********************************************************************

void ostc_attr_cacheprep_copy(ostc_cacheprep_t *cp, void **val, int *v_size)
{
    //** Copy the data
    memcpy(val, cp->val, sizeof(char *) * cp->n_keys);
    memcpy(v_size, cp->v_size, sizeof(int) * cp->n_keys);
}


//***********************************************************************
//  ostc_attr_cacheprep_ftype - Retreives the file type from the
//  attributes
//***********************************************************************

ostc_base_object_info_t ostc_attr_cacheprep_info_process(ostc_cacheprep_t *cp, int *err)
{
    ostc_base_object_info_t base;

    *err = OP_STATE_SUCCESS;
    memset(&base, 0, sizeof(base));
    if ((cp->val[cp->ftype_index] == NULL) || (cp->val[cp->link_count_index] == NULL) || (cp->val[cp->inode_index] == NULL)) {
        *err = OP_STATE_FAILURE;
        return(base);
    }

    sscanf((char *)cp->val[cp->ftype_index], "%d", &(base.ftype));
    sscanf((char *)cp->val[cp->link_count_index], "%d", &(base.link_count));
    sscanf((char *)cp->val[cp->inode_index], XIDT, &(base.inode));
    return(base);
}

//***********************************************************************
// _ostc_lio_cache_tree_walk - Walks the cache tree using the provided path
//   and optionally creates the target node if needed.
//
//   Returns 0 on success or a positive value representing the prefix that could
//      be mapped.
//
//   NOTE:  Assumes the cache lock is held
//***********************************************************************

int _ostc_lio_cache_tree_walk(lio_object_service_fn_t *os, char *fname, tbx_stack_t *tree, ostcdb_object_t *replacement_obj, ostc_base_object_info_t *add_terminal_info, int max_recurse)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    int i, n, start, end, loop, err, prefix_len;
    tbx_stack_t rtree;
    ostcdb_object_t *curr, *next, *prev, *hard;
    char merged_prefix[OS_PATH_MAX];

    if (add_terminal_info) {
        log_printf(5, "fname=%s add_terminal_ftype=%d inode=" XIDT "\n", fname, add_terminal_info->ftype, add_terminal_info->inode);
    } else {
        log_printf(5, "fname=%s add_terminal_info=NULL\n", fname);
    }

    if ((fname == NULL) || (max_recurse <= 0)) return(1);

    i=0;
    if ((tbx_stack_count(tree) == 0) || (fname[i] == '/')) {
        curr = ostc->cache_root;
        tbx_stack_empty(tree, 0);
    } else {
        curr = tbx_stack_get_current_data(tree);
    }
    tbx_stack_move_to_bottom(tree);
    tbx_stack_insert_below(tree, curr);

    loop = 0;
    while (fname[i] != 0) {
        //** Pick off any multiple /'s
        prefix_len = i;
        for (start=i; fname[start] == '/' ; start++) {}
        err = start;
        log_printf(5, "loop=%d start=%d i=%d\n", loop, start, i);
        if (fname[start] == 0) {
            if (loop == 0) {  //** This is the first (and last) token so push the root on the stack
                tbx_stack_move_to_bottom(tree);
                tbx_stack_insert_below(tree, curr);
            }
            err = 0;
            goto finished;
        }
        //** Find the trailing / or we reach the end
        for (end=start+1; fname[end] != '/' ; end++) {
            if (fname[end] == 0) break;
        }
        end--;
        i = end+1;

        //** Now do the lookup
        n = end - start +1;
        if (curr != NULL) {
            next = (curr->objects != NULL) ? apr_hash_get(curr->objects, (void *)&(fname[start]), n) : NULL;
        } else {
            next = NULL;
        }
        log_printf(5, "loop=%d start=%d end=%d i=%d next=%p end_char=%c prefix=%s\n", loop, start, end, i, next, fname[end], fname+start);

        if (next == NULL) {  //** Check if at the end
            if (fname[i] == 0) { //** Yup at the end
                if (add_terminal_info != NULL)  { //** Want to add the terminal
                    if (curr) { //** Make sure we have something to add it to
                        if ((add_terminal_info->ftype & OS_OBJECT_FILE_FLAG) && ((add_terminal_info->ftype & OS_OBJECT_SYMLINK) == 0) && (add_terminal_info->link_count > 1)) {
                            hard = apr_hash_get(ostc->hardlink_objects, &(add_terminal_info->inode), sizeof(ex_id_t));
                            if (!hard) {
                                hard = new_ostcdb_object(NULL, add_terminal_info, apr_time_now() + ostc->entry_timeout, ostc->mpool);
                                ostc->n_objects_created++;
                                apr_hash_set(ostc->hardlink_objects, &(hard->inode), sizeof(ex_id_t), hard);
                            }
                            if (replacement_obj == NULL) {
                                next = new_ostcdb_object_base(hard->expire);
                                ostc->n_objects_created++;
                            } else {
                                next = replacement_obj;
                            }
                            next->fname = strndup(&(fname[start]), n);
                            next->hard_obj = hard;
                            hard->n_refs++;
                        } else {
                            if (replacement_obj == NULL) {
                                next = new_ostcdb_object(strndup(&(fname[start]), n), add_terminal_info, apr_time_now() + ostc->entry_timeout, ostc->mpool);
                                ostc->n_objects_created++;
                            } else {
                                next = replacement_obj;
                            }
                        }

                        apr_hash_set(curr->objects, (void *)next->fname, n, next);
                        tbx_stack_move_to_bottom(tree);
                        tbx_stack_insert_below(tree, next);

                        err = 0;
                    }
                    goto finished;
                }
            } else if (fname[start] == '.') {  //** Check if it's a special entry
                if (n == 1) {//**Got a "."
                    continue;    //** Just restart the loop
                } else if (n == 2) {
                    if (fname[start+1] == '.') { //** Got a ".."
                        tbx_stack_move_to_bottom(tree);
                        tbx_stack_delete_current(tree, 1, 0);
                        curr = tbx_stack_get_current_data(tree);
                        continue;
                    }
                }
            }

            goto finished;
        } else if (next->link) {  //** Got a link
            if (fname[i] != 0) { //** If not at the end we need to follow it
                //*** Need to make a new stack and recurse it only keeping the bottom element
                merge_prefix_with_link(merged_prefix, fname, prefix_len, next->link);
                tbx_stack_init(&rtree);
                tbx_stack_dup(tree, &rtree);
                if (_ostc_lio_cache_tree_walk(os, next->link, &rtree, NULL, add_terminal_info, max_recurse-1) != 0) {
                    tbx_stack_empty(&rtree, 0);
                    err = -1;
                    goto finished;
                }
                tbx_stack_move_to_bottom(&rtree);
                next = tbx_stack_get_current_data(&rtree);  //** This will get placed as the next object on the stack
                tbx_stack_empty(&rtree, 0);
            }
        }

        loop++;
        tbx_stack_move_to_bottom(tree);
        tbx_stack_insert_below(tree, next);
        curr = next;
    }

    //** IF we made it here and this is non-NULL we need to replace the object we just located
    //** With the one provided
    if (replacement_obj != NULL) {
        tbx_stack_move_up(tree);
        prev = tbx_stack_get_current_data(tree);
        apr_hash_set(prev->objects, curr->fname, APR_HASH_KEY_STRING, NULL);
        apr_hash_set(prev->objects, replacement_obj->fname, strlen(replacement_obj->fname), replacement_obj);

        free_ostcdb_object(os, curr, &ostc->n_objects_removed, &ostc->n_attrs_removed);

        tbx_stack_move_to_bottom(tree);
        tbx_stack_delete_current(tree, 1, 0);
        tbx_stack_insert_below(tree, replacement_obj);
    }

    err = 0;
finished:
    log_printf(15, "fname=%s err=%d\n", fname, err);
    if (tbx_log_level() >= 15) {
        ostcdb_object_t *lo;
        tbx_stack_move_to_top(tree);
        log_printf(15, "stack_size=%d\n", tbx_stack_count(tree));
        while ((lo = tbx_stack_get_current_data(tree)) != NULL) {
            log_printf(15, "pwalk lo=%s ftype=%d\n", lo->fname, lo->ftype);
            tbx_stack_move_down(tree);
        }
    }

    return(err);
}

//***********************************************************************
// ostcdb_resolve_attr_link - Follows the attribute symlinks and returns
//   final object and attribute
//***********************************************************************

int _ostcdb_resolve_attr_link(lio_object_service_fn_t *os, tbx_stack_t *tree, char *alink, ostcdb_object_t **lobj, ostcdb_attr_t **lattr, int max_recurse)
{
    tbx_stack_t rtree;
    int i, n;
    char *aname;
    ostcdb_object_t *lo;
    ostcdb_attr_t *la;
    apr_hash_t *attrs;

    log_printf(5, "START alink=%s\n", alink);

    lo = NULL;
    la = NULL;

    //** 1st split the link into a path and attribute name
    n = strlen(alink);
    aname = NULL;
    for (i=n-1; i>=0; i--) {
        if (alink[i] == '/') {
            aname = alink + i + 1;
            alink[i] = 0;
            break;
        }
    }
    log_printf(5, "alink=%s aname=%s i=%d mr=%d\n", alink, aname, i, max_recurse);

    //** Copy the stack
    tbx_stack_init(&rtree);
    tbx_stack_dup(&rtree, tree);

    tbx_stack_move_to_top(&rtree);
    log_printf(5, "stack_size=%d org=%d\n", tbx_stack_count(&rtree), tbx_stack_count(tree));
    while ((lo = tbx_stack_get_current_data(&rtree)) != NULL) {
        log_printf(5, "pwalk lo=%s ftype=%d\n", lo->fname, lo->ftype);
        tbx_stack_move_down(&rtree);
    }
    lo = NULL;

    //** and pop the terminal which is up.  This will pop us up to the directory for the walk
    tbx_stack_move_to_bottom(&rtree);
    tbx_stack_delete_current(&rtree, 1, 0);
    if (_ostc_lio_cache_tree_walk(os, alink, &rtree, NULL, 0, OSTC_MAX_RECURSE) != 0) {
        if (i> -1) alink[i] = '/';
        goto finished;
    }
    if (i> -1) alink[i] = '/'; //** Undo our string munge if needed
    tbx_stack_move_to_bottom(&rtree);
    lo = tbx_stack_get_current_data(&rtree);  //** This will get placed as the next object on the stack

    if (lo == NULL) goto finished;
    attrs = (lo->hard_obj) ? lo->hard_obj->attrs : lo->attrs;
    la = apr_hash_get(attrs, aname, APR_HASH_KEY_STRING);
    log_printf(5, "alink=%s aname=%s lattr=%p mr=%d\n", alink, aname, la, max_recurse);

    if (la != NULL) {
        log_printf(5, "alink=%s aname=%s lo=%s la->link=%s\n", alink, aname, lo->fname, la->link);
        if (la->link) {  //** Got to recurse
            aname = strdup(la->link);
            _ostcdb_resolve_attr_link(os, &rtree, aname, &lo, &la, max_recurse-1);
            free(aname);
        }
    }

finished:
    tbx_stack_empty(&rtree, 0);
    *lattr = la;
    *lobj = lo;

    return(((la != NULL) && (lo != NULL)) ? 0 : 1);
}


//***********************************************************************
// _ostc_cache_purge_realpath - Purges the realpath variable in cache for all
//          underlying files/dirs
//***********************************************************************

void _ostc_cache_purge_realpath(lio_object_service_fn_t *os, ostcdb_object_t *parent)
{
    ostcdb_object_t *obj;
    apr_hash_index_t *hi;

    for (hi = apr_hash_first(NULL, parent->objects); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **) &obj);
        if (obj->ftype & OS_OBJECT_DIR_FLAG) {
            _ostc_cache_purge_realpath(os, obj);
        } else if (obj->realpath) {
            free(obj->realpath);
            obj->realpath = NULL;
        }
     }

    if (parent->realpath) {
        free(parent->realpath);
        parent->realpath = NULL;
    }
}

//***********************************************************************
//  ostc_cache_move_object - Moves an existing cache object within the cache
//***********************************************************************

void ostc_cache_move_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj, *parent;
    ostc_base_object_info_t base;
    int i;

    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, src_path, &tree, NULL, 0, OSTC_MAX_RECURSE) == 0) {
        tbx_stack_move_to_bottom(&tree);
        obj = tbx_stack_get_current_data(&tree);  //** Snag what we want to move

        tbx_stack_move_up(&tree);
        parent = tbx_stack_get_current_data(&tree);
        apr_hash_set(parent->objects, obj->fname, APR_HASH_KEY_STRING, NULL);  //** Delete it from it's old location
        if ((obj->ftype & OS_OBJECT_DIR_FLAG) && ((obj->ftype & OS_OBJECT_SYMLINK_FLAG) == 0)) {
            _ostc_update_link_count_object(os, parent, -1);
        }

        //** Peel off the new fname
        for (i = strlen(dest_path); i>0; i--) {
            if (dest_path[i] == '/') break;
        }
        if (dest_path[i] == '/') i++;
        free(obj->fname);
        obj->fname = strdup(&(dest_path[i]));
        log_printf(0, "src=%s dest=%s dname=%s\n", src_path, dest_path, obj->fname);

        //** Purge the os.realpath variable for the object and any files underneath
        if (obj->ftype & OS_OBJECT_DIR_FLAG) {
            _ostc_cache_purge_realpath(os, obj);
        } else if (obj->realpath) {
            free(obj->realpath);
            obj->realpath = NULL;
        }

        //** Do the walk and add it back
        tbx_stack_empty(&tree, 0);
        base.ftype = obj->ftype;
        base.inode = obj->inode;
        base.link_count = (obj->hard_obj) ? obj->hard_obj->link_count : obj->link_count;
        if (_ostc_lio_cache_tree_walk(os, dest_path, &tree, obj, &base, OSTC_MAX_RECURSE) != 0) {
            free_ostcdb_object(os, obj, &ostc->n_objects_removed, &ostc->n_attrs_removed);  //**Failed to walk the destination path
        } else if ((obj->ftype & OS_OBJECT_DIR_FLAG) && ((obj->ftype & OS_OBJECT_SYMLINK_FLAG) == 0)) {
            tbx_stack_move_to_bottom(&tree);
            tbx_stack_move_up(&tree);
            parent = tbx_stack_get_current_data(&tree);
            _ostc_update_link_count_object(os, parent, 1);
        }
    }

    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}

//***********************************************************************
//  ostc_cache_remove_object - Removes a cache object
//***********************************************************************

void ostc_cache_remove_object(lio_object_service_fn_t *os, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj, *parent;

    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, path, &tree, NULL, 0, OSTC_MAX_RECURSE) == 0) {
        tbx_stack_move_to_bottom(&tree);
        obj = tbx_stack_get_current_data(&tree);
        tbx_stack_move_up(&tree);
        parent = tbx_stack_get_current_data(&tree);
        apr_hash_set(parent->objects, obj->fname, APR_HASH_KEY_STRING, NULL);
        if (obj->hard_obj) {
            _ostc_update_link_count_object(os, obj->hard_obj, -1);
        }
        if ((obj->ftype & OS_OBJECT_DIR_FLAG) && ((obj->ftype & OS_OBJECT_SYMLINK_FLAG) == 0)) {
            _ostc_update_link_count_object(os, parent, -1);
        }

        free_ostcdb_object(os, obj, &ostc->n_objects_removed, &ostc->n_attrs_removed);
    }
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}

//***********************************************************************
//  ostc_remove_attr - Removes the given attributes from the object
//***********************************************************************

void ostc_cache_remove_attrs(lio_object_service_fn_t *os, char *fname, char **key, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr;

    int i;

    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    tbx_stack_move_to_bottom(&tree);
    obj = tbx_stack_get_current_data(&tree);
    if (obj->hard_obj) obj = obj->hard_obj;
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key[i], APR_HASH_KEY_STRING);
        if (attr != NULL) {
            apr_hash_set(obj->attrs, key[i], APR_HASH_KEY_STRING, NULL);
            free_ostcdb_attr(attr);
            ostc->n_attrs_removed++;
        }
    }
finished:
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}


//***********************************************************************
//  ostc_move_attr - Renames the given attributes from the object
//***********************************************************************

void ostc_cache_move_attrs(lio_object_service_fn_t *os, char *fname, char **key_old, char **key_new, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr, *attr2;
    int i;

    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    tbx_stack_move_to_bottom(&tree);
    obj = tbx_stack_get_current_data(&tree);
    if (obj->hard_obj) obj = obj->hard_obj;
    for (i=0; i<n; i++) {
        attr = apr_hash_get(obj->attrs, key_old[i], APR_HASH_KEY_STRING);
        if (attr != NULL) {
            apr_hash_set(obj->attrs, key_old[i], APR_HASH_KEY_STRING, NULL);
            attr2 = apr_hash_get(obj->attrs, key_new[i], APR_HASH_KEY_STRING);
            if (attr2 != NULL) {  //** Already have something by that name so delete it
                apr_hash_set(obj->attrs, key_new[i], APR_HASH_KEY_STRING, NULL);
                free_ostcdb_attr(attr2);
                ostc->n_attrs_removed++;
            }

            if (attr->key) free(attr->key);
            attr->key = strdup(key_new[i]);
            apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}


//***********************************************************************
// lut_limit_cache_timeout - returns the timeout for the attr or -1 if not found
//***********************************************************************

int lut_limit_cache_timeout(int n, limit_cache_t *lca, const char *key)
{
    int i;

    for (i=0; i<n; i++) {
        if (regexec(&(lca[i].regex), key, 0, NULL, 0) == 0) {
            return(lca[i].timeout);
        }
    }

    return(-1);
}

//***********************************************************************
//  ostc_cache_process_attrs - Merges the attrs into the cache
//***********************************************************************

void ostc_cache_process_attrs(lio_object_service_fn_t *os, char *fname, ostc_base_object_info_t *base, char **key_list, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *sobj, *obj, *aobj;
    ostcdb_attr_t *attr;
    char *key, *lkey;
    int i, timeout, to;

    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, base, OSTC_MAX_RECURSE) != 0) goto finished;

    log_printf(5, "fname=%s stack_size=%d\n", fname, tbx_stack_count(&tree));

    tbx_stack_move_to_bottom(&tree);
    sobj = tbx_stack_get_current_data(&tree);
    obj = sobj;
    if (obj->hard_obj) obj = obj->hard_obj;

    if (obj->link) {
        free(obj->link);
        obj->link = NULL;
    }
    if (val[2*n]) { //** got a symlink
        obj->link = (char *)val[2*n];
        val[2*n] = NULL;
    }
    for (i=0; i<n; i++) {
        timeout = ostc->entry_timeout;
        key = key_list[i];
        if (OSTC_NOCACHE_MATCH(key)) continue;  //** These we don't cache
        if (ostc->n_limit_cache) {
            to = lut_limit_cache_timeout(ostc->n_limit_cache, ostc->limit_cache, key);
            if (to != -1) {
                if (to == 0) continue;
                timeout = to;
            }
        }
        if (strcmp(key, "os.realpath") == 0) {
            if (v_size[i] > 0) {
                if (sobj->realpath) free(sobj->realpath);
                sobj->realpath = strdup((char *)val[i]);
                sobj->rp_len = v_size[i];
                continue;
            }
        }

        lkey = val[n+i];
        if (lkey != NULL) {  //** Got to find the linked attribute
            _ostcdb_resolve_attr_link(os, &tree, lkey, &aobj, &attr, OSTC_MAX_RECURSE);

            if (attr == NULL) continue;  //** If no atribute then skip updating
            log_printf(5, "TARGET obj=%s key=%s\n", aobj->fname, attr->key);

            //** Make the attr on the target link
            if (attr->val) { free(attr->val); attr->val = NULL; }
            if (v_size[i] > 0) {
                tbx_type_malloc(attr->val, void, v_size[i]+1);
                memcpy(attr->val, val[i], v_size[i]);
                ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
            }
            attr->v_size = v_size[i];

            //** Now make the pointer on the source
            attr = apr_hash_get(obj->attrs, key, APR_HASH_KEY_STRING);
            if (attr == NULL) {
                log_printf(5, "NEW obj=%s key=%s link=%s\n", obj->fname, key, lkey);
                attr = new_ostcdb_attr(key, NULL, -1234, apr_time_now() + timeout);
                apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
                ostc->n_attrs_created++;
            } else {
                log_printf(5, "OLD obj=%s key=%s link=%s\n", obj->fname, key, lkey);
                if (attr->link) free(attr->link);
                if (attr->val) free(attr->val);
                attr->v_size = -1234;
                attr->val = NULL;
            }
            attr->link = lkey;
            val[n+i] = NULL;
            v_size[n+i] = 0;
        } else {
            attr = apr_hash_get(obj->attrs, key, APR_HASH_KEY_STRING);
            if (attr == NULL) {
                attr = new_ostcdb_attr(key, val[i], v_size[i], apr_time_now() + timeout);
                apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, attr);
                ostc->n_attrs_created++;
            } else {
                if (attr->link) {
                    free(attr->link);
                    attr->link = NULL;
                }
                if (attr->val) free(attr->val);
                if (v_size[i] > 0) {
                    tbx_type_malloc(attr->val, void, v_size[i]+1);
                    memcpy(attr->val, val[i], v_size[i]);
                    ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
                } else {
                    attr->val = NULL;
                }
                attr->v_size = v_size[i];
            }
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}


//***********************************************************************
// ostc_cache_fetch - Attempts to process the attribute request from cached data
//***********************************************************************

gop_op_status_t ostc_cache_fetch(lio_object_service_fn_t *os, char *fname, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *sobj, *obj, *lobj;
    ostcdb_attr_t *attr;
    gop_op_status_t status = gop_failure_status;
    void *va[n];
    char *skey;
    int vs[n];
    int i, oops;

    tbx_stack_init(&tree);
    oops = 1;

    i=0;
    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    tbx_stack_move_to_bottom(&tree);
    sobj = tbx_stack_get_current_data(&tree);
    obj = sobj;
    if (obj->hard_obj) obj = obj->hard_obj;
    for (i=0; i<n; i++) {
        skey = key[i];
        if (strcmp(skey, "os.realpath") == 0) {
            if (sobj->realpath) {
                osf_store_val(sobj->realpath, sobj->rp_len, &(val[i]), &(v_size[i]));
                continue;
            } else {
                goto finished;
            }
        }

        attr = apr_hash_get(obj->attrs, skey, APR_HASH_KEY_STRING);
        if (attr == NULL) goto finished;  //** Not in cache so need to pull it

        log_printf(5, "BEFORE obj=%s key=%s val=%s v_size=%d alink=%s olink=%s\n", obj->fname, attr->key, (char *)attr->val, attr->v_size, attr->link, obj->link);

        if (attr->link != NULL) {  //** Got to resolve the link
            _ostcdb_resolve_attr_link(os, &tree, attr->link, &lobj, &attr, OSTC_MAX_RECURSE);
            if (attr == NULL) goto finished;  //** Can't follow the link
        }
        log_printf(5, "AFTER obj=%s key=%s val=%s v_size=%d alink=%s olink=%s\n", obj->fname, attr->key, (char *)attr->val, attr->v_size, attr->link, obj->link);

        //** Store the original values in case we need to rollback
        vs[i] = v_size[i];
        va[i] = val[i];

        log_printf(5, "key=%s val=%s v_size=%d\n", attr->key, (char *)attr->val, attr->v_size);
        osf_store_val(attr->val, attr->v_size, &(val[i]), &(v_size[i]));
    }

    oops = 0;
    status = gop_success_status;

finished:
    if (oops == 0) {
        ostc->n_attrs_hit += n;
    } else {
        ostc->n_attrs_miss += n;
    }

    OSTC_UNLOCK(ostc);

    if (oops == 1) { //** Got to unroll the values stored
        oops = i;
        for (i=0; i<oops; i++) {
            if (vs[i] < 0) {
                if (val[i] != NULL) free(val[i]);
            }
            v_size[i] = vs[i];
            val[i] = va[i];
        }
    }

    log_printf(5, "fname=%s n=%d key[0]=%s status=%d\n", fname, n, key[0], status.op_status);
    tbx_stack_empty(&tree, 0);

    return(status);
}


//***********************************************************************
// ostc_cache_update_attrs - Updates the attributes already cached.
//     Attributes that aren't cached are ignored.
//***********************************************************************


void ostc_cache_update_attrs(lio_object_service_fn_t *os, char *fname, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj, *sobj;
    ostcdb_attr_t *attr;
    char *skey;
    int i;

    tbx_stack_init(&tree);

    log_printf(15, "fname=%s n=%d key[0]=%s\n", fname, n, key[0]);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    tbx_stack_move_to_bottom(&tree);
    sobj = tbx_stack_get_current_data(&tree);
    obj = sobj;
    if (obj->hard_obj) obj = obj->hard_obj;

    for (i=0; i<n; i++) {
        skey = (strncmp(key[i], "os.timestamp.", 13) == 0) ? key[i] + 13 : key[i];
        if (strcmp(skey, "os.realpath") == 0) {
            if (sobj->realpath) free(sobj->realpath);
            sobj->realpath = strdup((char *)val[i]);
            sobj->rp_len = v_size[i];
            continue;
        }
        attr = apr_hash_get(obj->attrs, skey, APR_HASH_KEY_STRING);
        if (attr == NULL) continue;  //** Not in cache so ignore updating it

        if (attr->link != NULL) {  //** Got to resolve the link
            _ostcdb_resolve_attr_link(os, &tree, attr->link, &obj, &attr, OSTC_MAX_RECURSE);
            if (attr == NULL) continue;  //** Can't follow the link
        }

        //** Delete the attirbute since this is a timestamp
        if (strncmp(key[i], "os.timestamp.", 13) == 0) {
            apr_hash_set(obj->attrs, attr->key, APR_HASH_KEY_STRING, NULL);
            free_ostcdb_attr(attr);
            ostc->n_attrs_removed++;
            continue;
        }

        attr->v_size = v_size[i];
        if (attr->val) {
            free(attr->val);
            attr->val = NULL;
        }
        if (val) {
            if (val[i]) {
                tbx_type_malloc(attr->val, void, attr->v_size+1);
                memcpy(attr->val, val[i], attr->v_size);
                ((char *)(attr->val))[v_size[i]] = 0;  //** NULL terminate
            }
        }
    }

finished:
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}

//***********************************************************************
// ostc_cache_update_exec - Updates the attributes alredy cached for the file type
//     Attributes that aren't cached are ignored.
//***********************************************************************

void ostc_cache_update_exec(lio_object_service_fn_t *os, char *fname, int exec_mode)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostcdb_object_t *obj;
    ostcdb_attr_t *attr;
    int ftype;
    char buffer[128];
    tbx_stack_init(&tree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(os, fname, &tree, NULL, 0, OSTC_MAX_RECURSE) != 0) goto finished;

    tbx_stack_move_to_bottom(&tree);
    obj = tbx_stack_get_current_data(&tree);
    if (obj->hard_obj) obj = obj->hard_obj;
    if (exec_mode) {
        ftype = obj->ftype|OS_OBJECT_EXEC_FLAG;
    } else {
        if (obj->ftype & OS_OBJECT_EXEC_FLAG) {
            ftype = obj->ftype ^ OS_OBJECT_EXEC_FLAG;
        } else {
            ftype = obj->ftype;
        }
    }
    obj->ftype = ftype;

    attr = apr_hash_get(obj->attrs, "os.type", APR_HASH_KEY_STRING);
    if (attr == NULL) goto finished;  //** Not in cache so ignore updating it

    if (attr->link != NULL) {  //** Got to resolve the link
        _ostcdb_resolve_attr_link(os, &tree, attr->link, &obj, &attr, OSTC_MAX_RECURSE);
        if (attr == NULL) goto finished;  //** Can't follow the link
    }

    if (attr->val) {
        free(attr->val);
        attr->v_size = snprintf(buffer, sizeof(buffer)-1, "%d", ftype);
        buffer[attr->v_size] = 0;
        attr->val = strdup(buffer);
    }

finished:
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&tree, 0);
}

//***********************************************************************
// ostc_cache_populate_prefix - Recursively populates the prefix with a
//    minimal set of cache entries.
//***********************************************************************

int ostc_cache_populate_prefix(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int prefix_len, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_stack_t tree;
    ostc_cacheprep_t cp;
    os_fd_t *fd = NULL;
    char *key_array[1], *val_array[1];
    char *fname, *key, *val;
    int v_size[1];
    int err, start, end, len;
    int max_wait = 10;
    gop_op_status_t status;
    ostc_base_object_info_t base;

    len = strlen(path);
    if (len == 1) return(0);  //** Nothing to do.  Just a '/'

    tbx_stack_init(&tree);
    OSTC_LOCK(ostc);
    err = _ostc_lio_cache_tree_walk(os, path, &tree, NULL, 0, OSTC_MAX_RECURSE);
    OSTC_UNLOCK(ostc);
    tbx_stack_empty(&tree, 0);
    if (err <= 0)  return(err);

    start = err;
    for (end=start; path[end] != '/' ; end++) {  //** Find the terminal end
        if (path[end] == 0) break;
    }

    fname = strndup(path, end);

    if (end <= prefix_len) {
        log_printf(0, "ERROR: Unable to make progress in recursion. end=%d prefix_len=%d fname=%s full_path=%s\n", end, prefix_len, fname, path);
        free(fname);
        return(-1234);
    }

    log_printf(5, "path=%s prefix=%s len=%d end=%d\n", path, fname, len, end);

    key = "system.inode";
    val = NULL;
    key_array[0] = key;
    val_array[0] = val;
    v_size[0] = -100;
    ostc_attr_cacheprep_setup(&cp, 1, key_array, (void **)val_array, v_size);

    err = gop_sync_exec(os_open_object(ostc->os_child, creds, fname, OS_MODE_READ_IMMEDIATE, id, &fd, max_wait));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR opening object=%s\n", path);
        goto OOPS;
    }

    //** IF the attribute doesn't exist *val == NULL an *v_size = 0
    status = gop_sync_exec_status(os_get_multiple_attrs(ostc->os_child, creds, fd, cp.key, cp.val, cp.v_size, cp.n_keys_total));

    //** Close the parent
    err = gop_sync_exec(os_close_object(ostc->os_child, fd));
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR closing object=%s\n", path);
    }

    //** Store them in the cache on success
    if (status.op_status == OP_STATE_SUCCESS) {
        base = ostc_attr_cacheprep_info_process(&cp, &err);
        log_printf(1, "storing=%s ftype=%d end=%d len=%d v_size[0]=%d err=%d\n", fname, base.ftype, end, len, cp.v_size[0], err);
        if (err == OP_STATE_SUCCESS) {
            ostc_cache_process_attrs(os, fname, &base, cp.key, cp.val, cp.v_size, cp.n_keys);
            ostc_attr_cacheprep_copy(&cp, (void **)val_array, v_size);
            if (end < (len-1)) { //** Recurse and add the next layer
                log_printf(1, "recursing object=%s\n", path);
                err = ostc_cache_populate_prefix(os, creds, path, end, id);
            }
        }
    }

OOPS:
    ostc_attr_cacheprep_destroy(&cp);
    free(fname);
    if (v_size[0] > 0) free(val_array[0]);

    return(err);
}

//***********************************************************************
// _ostc_update_link_count_object - Updates the os.link_count for the object
//***********************************************************************

void _ostc_update_link_count_object(lio_object_service_fn_t *os,  ostcdb_object_t *obj, int delta)
{
    tbx_stack_t tree;
    ostcdb_object_t *aobj;
    ostcdb_attr_t *attr;
    char count[128];
    int i, n;

    obj->link_count += delta;

    attr = apr_hash_get(obj->attrs, "os.link_count", APR_HASH_KEY_STRING);
    if (attr) {
        if (attr->link != NULL) {  //** Got to resolve the link
            tbx_stack_init(&tree);
            _ostcdb_resolve_attr_link(os, &tree, attr->link, &aobj, &attr, OSTC_MAX_RECURSE);
        }

        if (attr) {
            if (attr->val) {
                n = atoi(attr->val);
                n = n + delta;
                i = snprintf(count, sizeof(count)-1, "%d", n);
                count[sizeof(count)-1] = 0;
                free(attr->val);
                attr->val = strdup(count);
                attr->v_size = i;
            }
        }
    }

}

//***********************************************************************
// _ostc_update_link_count - Updates the os.link_count for the file
//***********************************************************************

void _ostc_update_link_count(lio_object_service_fn_t *os, char *fname, int delta)
{
    tbx_stack_t tree;
    ostcdb_object_t *obj;
    char pname[OS_PATH_MAX];
    int i, n;

    //** Find the parent directory
    n = strlen(fname)-1;
    for (i=n; i!=0; i--) {
        if (fname[i] == '/') {
            break;
        }
    }
    if (i == 0) {
        pname[0] = '/';
        i = 1;
    } else {
        strncpy(pname, fname, i);
    }
    pname[i] = 0;

    //** See if it exists
    tbx_stack_init(&tree);
    if (_ostc_lio_cache_tree_walk(os, pname, &tree, NULL, 0, OSTC_MAX_RECURSE) == 0) {
        tbx_stack_move_to_bottom(&tree);
        obj = tbx_stack_get_current_data(&tree);
        if (obj->hard_obj) obj = obj->hard_obj;
        _ostc_update_link_count_object(os,  obj, delta);
    }
    tbx_stack_empty(&tree, 0);
}

//***********************************************************************
// ostc_remove_regex_object_fn - Simple passthru with purging of my cache.
//      This command is rarely used.  Hence the simple purging.
//***********************************************************************

gop_op_status_t ostc_remove_regex_object_fn(void *arg, int tid)
{
    ostc_remove_regex_t *op = (ostc_remove_regex_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;

    status = gop_sync_exec_status(op->gop);
    op->gop = NULL;  //** This way we don't accidentally clean it up again

    //** Since we don't know what was removed we're going to purge everything to make life easy.
    if (status.op_status == OP_STATE_SUCCESS) {
        apr_thread_mutex_lock(ostc->lock);
        _ostc_cleanup(op->os, ostc->cache_root, apr_time_now() + 4*ostc->entry_timeout);
        apr_thread_mutex_unlock(ostc->lock);
    }

    return(status);
}

//***********************************************************************
//  free_remove_regex - Frees a regex remove object
//***********************************************************************

void free_remove_regex(void *arg)
{
    ostc_remove_regex_t *op = (ostc_remove_regex_t *)arg;

    if (op->gop) gop_free(op->gop, OP_DESTROY);
    free(op);
}

//***********************************************************************
// ostc_remove_regex_object - Simple passthru with purging of my cache.
//      This command is rarely used.  Hence the simple purging.
//***********************************************************************

gop_op_generic_t *ostc_remove_regex_object(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int obj_types, int recurse_depth)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_remove_regex_t *op;

    tbx_type_malloc(op, ostc_remove_regex_t, 1);
    op->os = os;
    op->gop = os_remove_regex_object(ostc->os_child, creds, path, object_regex, obj_types, recurse_depth);
    gop_set_private(op->gop, op);

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_remove_regex_object_fn, (void *)op, free_remove_regex, 1));
}

//***********************************************************************
// ostc_abort_remove_regex_object - Aborts a bulk remove call
//***********************************************************************

gop_op_generic_t *ostc_abort_remove_regex_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_remove_regex_t *op;

    op = (ostc_remove_regex_t *)gop_get_private(gop);
    return(os_abort_remove_regex_object(ostc->os_child, op->gop));
}

//***********************************************************************
// ostc_remove_object_fn - Handles the actual object removal
//***********************************************************************

gop_op_status_t ostc_remove_object_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;

    status = gop_sync_exec_status(os_remove_object(ostc->os_child, op->creds, op->src_path));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    ostc_cache_remove_object(op->os, op->src_path);

    return(status);
}


//***********************************************************************
// ostc_remove_object - Generates a remove object operation
//***********************************************************************

gop_op_generic_t *ostc_remove_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = path;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_remove_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_regex_object_set_multiple_attrs - Does a bulk regex change.
//     Each matching object's attr are changed.  If the object is a directory
//     then the system will recursively change it's contents up to the
//     recursion depth.
//***********************************************************************

gop_op_generic_t *ostc_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char *id, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types, int recurse_depth, char **key, void **val, int *v_size, int n_attrs)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_regex_object_set_multiple_attrs(ostc->os_child, creds, id, path, object_regex, object_types, recurse_depth, key, val, v_size, n_attrs));
}

//***********************************************************************
// ostc_abort_regex_object_set_multiple_attrs - Aborts a bulk attr call
//***********************************************************************

gop_op_generic_t *ostc_abort_regex_object_set_multiple_attrs(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_abort_regex_object_set_multiple_attrs(ostc->os_child, gop));
}


//***********************************************************************
//  ostc_exists - Returns the object type  and 0 if it doesn't exist
//***********************************************************************

gop_op_generic_t *ostc_exists(lio_object_service_fn_t *os, lio_creds_t *creds, char *path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_exists(ostc->os_child, creds, path));
}

//***********************************************************************
//  ostc_realpath - Returns the real path of the object type
//***********************************************************************

gop_op_generic_t *ostc_realpath(lio_object_service_fn_t *os, lio_creds_t *creds, const char *path, char *realpath)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_realpath(ostc->os_child, creds, path, realpath));
}

//***********************************************************************
// ostc_object_exec_modify - Sets/cleares the executable bit
//***********************************************************************

gop_op_status_t ostc_object_exec_modify_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;

    status = gop_sync_exec_status(os_object_exec_modify(ostc->os_child, op->creds, op->src_path, op->ftype));

    if (status.op_status != OP_STATE_SUCCESS) return(status); //** Failed so kick out

    //** If we made it here it was successful so try and update the os.type
    ostc_cache_update_exec(op->os, op->src_path, op->ftype);
    return(status);
}

//***********************************************************************

gop_op_generic_t *ostc_object_exec_modify(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int exec_state)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc_clear(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = path;
    op->ftype = exec_state;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_object_exec_modify_fn, (void *)op, free, 1));
}

//***********************************************************************

gop_op_status_t ostc_create_object_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;

    status = gop_sync_exec_status(os_create_object(ostc->os_child, op->creds, op->src_path, op->ftype, op->id));

    if (status.op_status != OP_STATE_SUCCESS) return(status); //** Failed so kick out

    //** If we made it here it was successful so try and update the parent os.link_count
    if (op->ftype & OS_OBJECT_DIR_FLAG) {
        OSTC_LOCK(ostc);
        _ostc_update_link_count(op->os, op->src_path, 1);
        OSTC_UNLOCK(ostc);
    }
    return(status);
}

//***********************************************************************
// ostc_create_object - Creates an object
//***********************************************************************

gop_op_generic_t *ostc_create_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc_clear(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = path;
    op->ftype = type;
    op->id = id;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_create_object_fn, (void *)op, free, 1));
}


//***********************************************************************

gop_op_status_t ostc_create_object_with_attrs_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;

    //** We don't try and cache anything in case of a failure.  A subsequent open call will properly populate the cache.
    status = gop_sync_exec_status(os_create_object_with_attrs(ostc->os_child, op->creds, op->src_path, op->ftype, op->id, op->key, op->val, op->v_size, op->n_keys));

    if (status.op_status != OP_STATE_SUCCESS) return(status); //** Failed so kick out

    //** If we made it here it was successful so try and update the parent os.link_count
    if (op->ftype & OS_OBJECT_DIR_FLAG) {
        OSTC_LOCK(ostc);
        _ostc_update_link_count(op->os, op->src_path, 1);
        OSTC_UNLOCK(ostc);
    }
    return(status);
}

//***********************************************************************
// ostc_create_object_with_attrs - Creates an object with attrs
//***********************************************************************

gop_op_generic_t *ostc_create_object_with_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int type, char *id, char **key, void **val, int *v_size, int n_keys)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc_clear(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = path;
    op->ftype = type;
    op->id = id;
    op->key = key;
    op->val = val;
    op->v_size = v_size;
    op->n_keys = n_keys;
    return(gop_tp_op_new(ostc->tpc, NULL, ostc_create_object_with_attrs_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_symlink_object_fn - Handles the actual object symlink
//***********************************************************************

gop_op_status_t ostc_symlink_object_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;


    status = gop_sync_exec_status(os_symlink_object(ostc->os_child, op->creds, op->src_path, op->dest_path, op->id));

    return(status);
}

//***********************************************************************
// ostc_symlink_object - Generates a symbolic link object operation
//***********************************************************************

gop_op_generic_t *ostc_symlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc_clear(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = src_path;
    op->dest_path = dest_path;
    op->id = id;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_symlink_object_fn, (void *)op, free, 1));
}


//***********************************************************************
// ostc_hardlink_object_fn - Handles the actual object hardlink
//***********************************************************************

gop_op_status_t ostc_hardlink_object_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;
    tbx_stack_t stree, dtree;
    ostcdb_object_t *sobj, *hobj, *parent;
    ostc_base_object_info_t  base;

    status = gop_sync_exec_status(os_hardlink_object(ostc->os_child, op->creds, op->src_path, op->dest_path, op->id));
    if (status.op_status != OP_STATE_SUCCESS) return(status);

    //** If needed move the current src cache object to a hardlink

    tbx_stack_init(&stree);
    tbx_stack_init(&dtree);

    OSTC_LOCK(ostc);
    if (_ostc_lio_cache_tree_walk(op->os, op->src_path, &stree, NULL, NULL, OSTC_MAX_RECURSE) == 0) {
        tbx_stack_move_to_bottom(&stree);
        sobj = tbx_stack_get_current_data(&stree);

        tbx_stack_move_up(&stree);
        parent = tbx_stack_get_current_data(&stree);

        if (sobj->hard_obj) {  //** Already a hardlinked object
            hobj = sobj->hard_obj;
        } else {  //** Need to move the existing object to the hardlinks and replace it
            hobj = sobj;
            hobj->ftype |= OS_OBJECT_HARDLINK_FLAG;
            hobj->n_refs++;
            sobj = new_ostcdb_object_base(hobj->expire);
            ostc->n_objects_created++;
            sobj->hard_obj = hobj;
            sobj->fname = hobj->fname;
            hobj->fname = NULL;
            apr_hash_set(parent->objects, sobj->fname, APR_HASH_KEY_STRING, sobj);
            apr_hash_set(ostc->hardlink_objects, &(hobj->inode), sizeof(ex_id_t), hobj);
        }

        //** Now make the destination object and link it
        _ostc_update_link_count_object(op->os, hobj, 1);

        base.ftype = hobj->ftype;
        base.link_count = hobj->link_count;
        base.inode = hobj->inode;
        _ostc_lio_cache_tree_walk(op->os, op->dest_path, &dtree, NULL, &base, OSTC_MAX_RECURSE);
    }
    OSTC_UNLOCK(ostc);

    tbx_stack_empty(&stree, 0);
    tbx_stack_empty(&dtree, 0);

    return(status);
}

//***********************************************************************
// ostc_hardlink_object - Generates a hard link object operation
//***********************************************************************

gop_op_generic_t *ostc_hardlink_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path, char *id)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc_clear(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = src_path;
    op->dest_path = dest_path;
    op->id = id;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_hardlink_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_move_object_fn - Handles the actual object move
//***********************************************************************

gop_op_status_t ostc_move_object_fn(void *arg, int tid)
{
    ostc_object_op_t *op = (ostc_object_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;


    status = gop_sync_exec_status(os_move_object(ostc->os_child, op->creds, op->src_path, op->dest_path));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    ostc_cache_move_object(op->os, op->creds, op->src_path, op->dest_path);

    return(status);
}

//***********************************************************************
// ostc_delayed_open_object - Actual opens the object for defayed opens
//***********************************************************************

gop_op_status_t ostc_delayed_open_object(lio_object_service_fn_t *os, ostc_fd_t *fd)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    gop_op_status_t status;
    os_fd_t *cfd;

    log_printf(5, "DELAYED_OPEN fd=%s\n", fd->fname);
    status = gop_sync_exec_status(os_open_object(ostc->os_child, fd->creds, fd->fname, fd->mode, fd->id, &cfd, fd->max_wait));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    apr_thread_mutex_lock(ostc->delayed_lock);
    if (fd->fd_child == NULL) {
        fd->fd_child = cfd;
        apr_thread_mutex_unlock(ostc->delayed_lock);
    } else { //** Somebody else beat us to it so close mine and throw it away
        apr_thread_mutex_unlock(ostc->delayed_lock);

        gop_sync_exec(os_close_object(ostc->os_child, cfd));
    }

    return(status);
}

//***********************************************************************
// ostc_move_object - Generates a move object operation
//***********************************************************************

gop_op_generic_t *ostc_move_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *dest_path)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_op_t *op;

    tbx_type_malloc(op, ostc_object_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->src_path = src_path;
    op->dest_path = dest_path;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_move_object_fn, (void *)op, free, 1));
}

//***********************************************************************
// ostc_copy_multiple_attrs_fn - Handles the actual attribute copy operation
//***********************************************************************

gop_op_status_t ostc_copy_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    if (ma->fd_dest->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd_dest);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_copy_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->fd_dest->fd_child, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** We're just going to remove the destination attributes and rely on a refetch to get the changes
    ostc_cache_remove_attrs(ma->os, ma->fd_dest->fname, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_copy_multiple_attrs - Generates a copy object multiple attribute operation
//***********************************************************************

gop_op_generic_t *ostc_copy_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->fd = (ostc_fd_t *)fd_src;
    ma->fd_dest = (ostc_fd_t *)fd_dest;
    ma->key = key_src;
    ma->key_dest = key_dest;
    ma->n = n;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_copy_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_copy_attr - Generates a copy object attribute operation
//***********************************************************************

gop_op_generic_t *ostc_copy_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd_src, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->fd = fd_src;
    ma->fd_dest = fd_dest;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_copy_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_symlink_attrs_fn - Handles the actual attribute symlinking
//***********************************************************************

gop_op_status_t ostc_symlink_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;

    if (ma->fd_dest->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd_dest);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_symlink_multiple_attrs(ostc->os_child, ma->creds, ma->src_path, ma->key, ma->fd_dest->fd_child, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** We're just going to remove the destination attributes and rely on a refetch to get the changes
    ostc_cache_remove_attrs(ma->os, ma->fd_dest->fname, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_symlink_multiple_attrs - Generates a link multiple attribute operation
//***********************************************************************

gop_op_generic_t *ostc_symlink_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, char **src_path, char **key_src, os_fd_t *fd_dest, char **key_dest, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->src_path = src_path;
    ma->key = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = key_dest;
    ma->n = n;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_symlink_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_symlink_attr - Generates a link attribute operation
//***********************************************************************

gop_op_generic_t *ostc_symlink_attr(lio_object_service_fn_t *os, lio_creds_t *creds, char *src_path, char *key_src, os_fd_t *fd_dest, char *key_dest)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->creds = creds;
    ma->src_path = &(ma->src_tmp);
    ma->src_tmp = src_path;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_src;
    ma->fd_dest = fd_dest;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_dest;

    ma->n = 1;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_symlink_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_move_attrs_fn - Handles the actual attribute move operation
//***********************************************************************

gop_op_status_t ostc_move_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;

    if (ma->fd->fd_child == NULL) {
        status = ostc_delayed_open_object(ma->os, ma->fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    status = gop_sync_exec_status(os_move_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->key_dest, ma->n));

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

    //** Move them in the cache
    ostc_cache_move_attrs(ma->os, ma->fd->fname, ma->key, ma->key_dest, ma->n);
    return(status);
}


//***********************************************************************
// ostc_move_multiple_attrs - Generates a move object attributes operation
//***********************************************************************

gop_op_generic_t *ostc_move_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key_old, char **key_new, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key_old;
    ma->key_dest = key_new;
    ma->n = n;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_move_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_move_attr - Generates a move object attribute operation
//***********************************************************************

gop_op_generic_t *ostc_move_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key_old, char *key_new)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key_old;
    ma->key_dest = (char **)&(ma->val_tmp);
    ma->val_tmp = key_new;

    ma->n = 1;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_move_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// get_attrs_sanity_check - Used for debugging only to compare with what's in the cache
//***********************************************************************

int get_attrs_sanity_check(ostc_mult_attr_t *ma)
{
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;
    ostc_cacheprep_t cp;
    int i, err, do_fix, n;
    os_fd_t *cfd;

    do_fix = 0;  //** Set to 1 for auto correcting

    //** Init everything
    n = ma->n+1;
    tbx_type_malloc_clear(cp.key, char *, n); memcpy(cp.key, ma->key, sizeof(char *)*ma->n);
    tbx_type_malloc_clear(cp.val, void *, n);
    tbx_type_malloc_clear(cp.v_size, int, n);
    for (i=0; i<n; i++) cp.v_size[i] = -10*1024*1024;
    err = 0;

    //** Open the file
    status = gop_sync_exec_status(os_open_object(ostc->os_child, ma->fd->creds, ma->fd->fname, ma->fd->mode, ma->fd->id, &cfd, ma-> fd->max_wait));
    if (status.op_status != OP_STATE_SUCCESS) {
        err = -1;
        log_printf(0, "ERROR: Unable to open fname=%s\n", ma->fd->fname);
        goto finished;
    }

    //** Fetch the attributes
    status = gop_sync_exec_status(os_get_multiple_attrs(ostc->os_child, ma->creds, cfd, cp.key, cp.val, cp.v_size, ma->n));


    //** And compare them
    if (status.op_status != OP_STATE_SUCCESS) {
        err = -2;
        log_printf(0, "MISMATCH-ERROR get child->get_attr.  fname=%s\n", ma->fd->fname);
    } else {
        for (i=0; i<ma->n; i++) {
            if (cp.v_size[i] == ma->v_size[i]) {
                if (cp.v_size[i] > 0) {
                    if (memcmp(cp.val[i], ma->val[i], ma->v_size[i]) != 0) {
                        err++;
                        log_printf(0, "MISMATCH-VAL: fname=%s inode=%s key=%s vsize=%d child=%s tc=%s\n", ma->fd->fname, (char *)cp.val[n-1], ma->key[i], ma->v_size[i], (char *)cp.val[i], (char *)ma->val[i]);
                        if (do_fix) {
                            if (ma->val[i]) free(ma->val[i]);
                            ma->v_size[i] = cp.v_size[i];
                            ma->val[i] = cp.val[i];
                            cp.v_size[i] = 0;
                        }
                    }
                }
            } else {
                err++;
                log_printf(0, "MISMATCH-SIZE: fname=%s key=%s vsize: child=%d tc=%d ---- val: child=%s tc=%s\n", ma->fd->fname, ma->key[i], ma->v_size[i], cp.v_size[i], (char *)cp.val[i], (char *)ma->val[i]);
                if (do_fix) {
                    if (ma->val[i]) free(ma->val[i]);
                    ma->v_size[i] = cp.v_size[i];
                    ma->val[i] = cp.val[i];
                    cp.v_size[i] = 0;
                }
            }

            if (cp.v_size[i] > 0) free(cp.val[i]);
        }

        if (err != ma->n) {
            i = ma->n - err;
            log_printf(0, "MATCH-COUNT: fname=%s good=%d bad=%d\n", ma->fd->fname, i, err);
        }
    }

    //** Close the file
    gop_sync_exec(os_close_object(ostc->os_child, cfd));

finished:
    free(cp.key);
    free(cp.val);
    free(cp.v_size);

    return(err);
}

//***********************************************************************
// ostc_get_attrs_fn - Handles the actual attribute get operation
//***********************************************************************

gop_op_status_t ostc_get_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;
    ostc_cacheprep_t cp;
    ostc_base_object_info_t base;
    int err;
    char *fname, *id;
    int mflags;

    //** Set up the vars needed
    if (ma->is_immediate) {
        fname = ma->path;
        id = an_cred_get_descriptive_id(ma->creds, NULL);
        mflags = OS_MODE_READ_IMMEDIATE;
    } else {
        fname = ma->fd->fname;
        id = ma->fd->id;
        mflags = ma->fd->mflags;
    }

    //** See if we can satisfy everything from cache
    if ((mflags & OS_MODE_NO_CACHE_INFO_IF_FILE) == 0) {
        status = ostc_cache_fetch(ma->os, fname, ma->key, ma->val, ma->v_size, ma->n);
    } else {
        status = gop_failure_status;
    }

    //** Uncomment line below to verify the cached attrs match what is in the LServer.
    //** Changes made to directories, hard links, etc outside the calling program won't get reflected perfectly
    //** but it should make things like git and other programs running locally happy by reflecting their changes appropriately
    //if (status.op_status == OP_STATE_SUCCESS) get_attrs_sanity_check(ma);

    if (status.op_status == OP_STATE_SUCCESS) {
        log_printf(10, "ATTR_CACHE_HIT: fname=%s key[0]=%s n_keys=%d no_cache_if_file_flag=%d\n", fname, ma->key[0], ma->n, (mflags & OS_MODE_NO_CACHE_INFO_IF_FILE));
    } else {
        log_printf(10, "ATTR_CACHE_MISS fname=%s key[0]=%s n_keys=%d no_cache_flig_if_file_flag=%d\n", fname, ma->key[0], ma->n, (mflags & OS_MODE_NO_CACHE_INFO_IF_FILE));
    }
    if (status.op_status == OP_STATE_SUCCESS) return(status);

    ostc_cache_populate_prefix(ma->os, ma->creds, fname, 0, id);

    ostc_attr_cacheprep_setup(&cp, ma->n, ma->key, ma->val, ma->v_size);

    if (ma->is_immediate) {
        status = gop_sync_exec_status(os_get_multiple_attrs_immediate(ostc->os_child, ma->creds, fname, cp.key, cp.val, cp.v_size, cp.n_keys_total));
    } else {
        if (ma->fd->fd_child == NULL) {
            status = ostc_delayed_open_object(ma->os, ma->fd);
            if (status.op_status == OP_STATE_FAILURE) goto failed;
        }

        status = gop_sync_exec_status(os_get_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, cp.key, cp.val, cp.v_size, cp.n_keys_total));
    }


    //** Store them in the cache on success
    if (status.op_status == OP_STATE_SUCCESS) {
        base = ostc_attr_cacheprep_info_process(&cp, &err);
        if (err == OP_STATE_SUCCESS) {
            ostc_cache_process_attrs(ma->os, fname, &base, cp.key, cp.val, cp.v_size, cp.n_keys);
            ostc_attr_cacheprep_copy(&cp, ma->val, ma->v_size);
        } else {
            status.op_status = OP_STATE_FAILURE;
        }
    }

failed:
    ostc_attr_cacheprep_destroy(&cp);

    return(status);
}

//***********************************************************************
// ostc_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *ostc_get_multiple_attrs_immediate(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->path = path;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;
    ma->is_immediate = 1;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_get_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_get_multiple_attrs - Retreives multiple object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *ostc_get_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;
    ma->is_immediate = 0;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_get_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_get_attr - Retreives a single object attribute
//   If *v_size < 0 then space is allocated up to a max of abs(v_size)
//   and upon return *v_size contains the bytes loaded
//***********************************************************************

gop_op_generic_t *ostc_get_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void **val, int *v_size)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = 1;
    ma->is_immediate = 0;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_get_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_set_attrs_fn - Handles the actual attribute set operation
//***********************************************************************

gop_op_status_t ostc_set_attrs_fn(void *arg, int tid)
{
    ostc_mult_attr_t *ma = (ostc_mult_attr_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)ma->os->priv;
    gop_op_status_t status;

    if (ma->is_immediate) {
        status = gop_sync_exec_status(os_set_multiple_attrs_immediate(ostc->os_child, ma->creds, ma->path, ma->key, ma->val, ma->v_size, ma->n));
    } else {
        if (ma->fd->fd_child == NULL) {
            status = ostc_delayed_open_object(ma->os, ma->fd);
            if (status.op_status == OP_STATE_FAILURE) return(status);
        }

        status = gop_sync_exec_status(os_set_multiple_attrs(ostc->os_child, ma->creds, ma->fd->fd_child, ma->key, ma->val, ma->v_size, ma->n));
    }

    //** Failed just return
    if (status.op_status != OP_STATE_SUCCESS) return(status);

    //** Update the cache on the keys we know about
    ostc_cache_update_attrs(ma->os, ((ma->fd) ? ma->fd->fname : ma->path), ma->key, ma->val, ma->v_size, ma->n);

    return(status);
}

//***********************************************************************
// ostc_set_multiple_attrs_immeidate - Sets multiple object attributes for a quick setting
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

gop_op_generic_t *ostc_set_multiple_attrs_immediate(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->path = path;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;
    ma->is_immediate = 1;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_set_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************
// ostc_set_multiple_attrs - Sets multiple object attributes
//   If val[i] == NULL for the attribute is deleted
//***********************************************************************

gop_op_generic_t *ostc_set_multiple_attrs(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char **key, void **val, int *v_size, int n)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = key;
    ma->val = val;
    ma->v_size = v_size;
    ma->n = n;
    ma->is_immediate = 0;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_set_attrs_fn, (void *)ma, free, 1));
}


//***********************************************************************
// ostc_set_attr - Sets a single object attribute
//   If val == NULL the attribute is deleted
//***********************************************************************

gop_op_generic_t *ostc_set_attr(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *fd, char *key, void *val, int v_size)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_mult_attr_t *ma;

    tbx_type_malloc_clear(ma, ostc_mult_attr_t, 1);
    ma->os = os;
    ma->fd = fd;
    ma->creds = creds;
    ma->key = &(ma->key_tmp);
    ma->key_tmp = key;
    ma->val = &(ma->val_tmp);
    ma->val_tmp = val;
    ma->v_size = &(ma->v_tmp);
    ma->v_tmp = v_size;
    ma->n = 1;
    ma->is_immediate = 0;

    return(gop_tp_op_new(ostc->tpc, NULL, ostc_set_attrs_fn, (void *)ma, free, 1));
}

//***********************************************************************

//***********************************************************************
// ostc_next_attr - Returns the next matching attribute
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

int ostc_next_attr(os_attr_iter_t *oit, char **key, void **val, int *v_size)
{
    ostc_attr_iter_t *it = (ostc_attr_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    return(os_next_attr(ostc->os_child, it->it, key, val, v_size));
}

//***********************************************************************
// ostc_create_attr_iter - Creates an attribute iterator
//   Each entry in the attr table corresponds to a different regex
//   for selecting attributes
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

os_attr_iter_t *ostc_create_attr_iter(lio_object_service_fn_t *os, lio_creds_t *creds, os_fd_t *ofd, lio_os_regex_table_t *attr, int v_max)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_attr_iter_t *it;
    ostc_fd_t *fd = (ostc_fd_t *)ofd;
    gop_op_status_t status;

    if (fd->fd_child == NULL) {
        status = ostc_delayed_open_object(os, fd);
        if (status.op_status == OP_STATE_FAILURE) return(NULL);
    }

    tbx_type_malloc(it, ostc_attr_iter_t, 1);

    it->it = os_create_attr_iter(ostc->os_child, creds, fd->fd_child, attr, v_max);
    if (it == NULL) {
        free(it);
        return(NULL);
    }

    it->os = os;

    return(it);
}


//***********************************************************************
// ostc_destroy_attr_iter - Destroys an attribute iterator
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

void ostc_destroy_attr_iter(os_attr_iter_t *oit)
{
    ostc_attr_iter_t *it = (ostc_attr_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    os_destroy_attr_iter(ostc->os_child, it->it);
    free(it);
}

//***********************************************************************
// ostc_next_object - Returns the iterators next matching object
//***********************************************************************

int ostc_next_object(os_object_iter_t *oit, char **fname, int *prefix_len)
{
    ostc_object_iter_t *it = (ostc_object_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;
    ostc_base_object_info_t base;
    int ftype, i, err, start_index;

    log_printf(5, "START\n");

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return(-2);
    }

    ftype = os_next_object(ostc->os_child, it->it_child, fname, prefix_len);
    //** Last object so return
    if (ftype <= 0) {
        *fname = NULL;
        *prefix_len = -1;
        log_printf(5, "No more objects\n");
        return(ftype);
    }

    if (it->iter_type == OSTC_ITER_ALIST) {
        //** Copy any results back
        ostc_attr_cacheprep_copy(&(it->cp), it->val, it->v_size);
        base = ostc_attr_cacheprep_info_process(&(it->cp), &err);
        start_index = it->cp.n_keys;
        if (err == OP_STATE_SUCCESS) {
            ostc_cache_process_attrs(it->os, *fname, &base, it->cp.key, it->cp.val, it->cp.v_size, it->n_keys);
        } else {
            ftype = -1;
            if (*fname) free(*fname);
            *fname = NULL;
            start_index = 0;  //** Also free the user requested attributes if they exist
        }
        //** We have to do a manual cleanup and can't call the CP destroy method
        for (i=start_index; i<it->cp.n_keys_total; i++) {
            if (it->cp.val[i] != NULL) {
                free(it->cp.val[i]);
                it->cp.val[i] = NULL;
            }
        }
    }

    log_printf(5, "END\n");

    return(ftype);
}

//***********************************************************************
// ostc_destroy_object_iter - Destroy the object iterator
//***********************************************************************

void ostc_destroy_object_iter(os_object_iter_t *oit)
{
    ostc_object_iter_t *it = (ostc_object_iter_t *)oit;
    ostc_priv_t *ostc = (ostc_priv_t *)it->os->priv;

    if (it == NULL) {
        log_printf(0, "ERROR: it=NULL\n");
        return;
    }

    if (it->it_child != NULL) os_destroy_object_iter(ostc->os_child, it->it_child);
    if (it->iter_type == OSTC_ITER_ALIST) ostc_attr_cacheprep_destroy(&(it->cp));

    if (it->v_size_initial != NULL) free(it->v_size_initial);

    free(it);
}

//***********************************************************************
// ostc_create_object_iter - Creates an object iterator to selectively
//  retreive object/attribute combinations
//
//   NOTE: We don't do any caching on regex attr lists so this is a simple
//     passthru
//***********************************************************************

os_object_iter_t *ostc_create_object_iter(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        lio_os_regex_table_t *attr, int recurse_depth, os_attr_iter_t **it_attr, int v_max)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_iter_t *it;
    os_attr_iter_t **achild;

    log_printf(5, "START\n");


    //** Make the iterator handle
    tbx_type_malloc_clear(it, ostc_object_iter_t, 1);
    it->iter_type = OSTC_ITER_AREGEX;
    it->os = os;
    it->attr = attr;

    //** Set up the FD for the iterator
    it->fd.creds = creds;
    it->fd.fname = NULL;
    it->fd.mode = OS_MODE_READ_IMMEDIATE;
    it->fd.id = NULL;
    it->fd.max_wait = 60;

    achild = NULL;
    if (it_attr != NULL) {
        it->it_attr.os = os;
        achild = &(it->it_attr.it);
        *it_attr = (os_attr_iter_t *)&(it->it_attr);
    }

    //** Make the gop and execute it
    it->it_child = os_create_object_iter(ostc->os_child, creds,  path, object_regex, object_types, attr, recurse_depth, achild, v_max);

    log_printf(5, "it_child=%p\n", it->it_child);

    //** Clean up if an error occurred
    if (it->it_child == NULL) {
        ostc_destroy_object_iter(it);
        it = NULL;
    }

    log_printf(5, "END\n");

    return(it);
}

//***********************************************************************
// ostc_create_object_iter_alist - Creates an object iterator to selectively
//  retreive object/attribute from a fixed attr list
//
//***********************************************************************

os_object_iter_t *ostc_create_object_iter_alist(lio_object_service_fn_t *os, lio_creds_t *creds, lio_os_regex_table_t *path, lio_os_regex_table_t *object_regex, int object_types,
        int recurse_depth, char **key, void **val, int *v_size, int n_keys)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_object_iter_t *it;

    log_printf(5, "START\n");


    //** Make the iterator handle
    tbx_type_malloc_clear(it, ostc_object_iter_t, 1);
    it->iter_type = OSTC_ITER_ALIST;
    it->os = os;
    it->val = val;
    it->v_size = v_size;
    it->n_keys = n_keys;
    ostc_attr_cacheprep_setup(&(it->cp), it->n_keys, key, val, v_size);

    tbx_type_malloc(it->v_size_initial, int, n_keys);
    memcpy(it->v_size_initial, it->v_size, n_keys*sizeof(int));

    //** Make the gop and execute it
    it->it_child = os_create_object_iter_alist(ostc->os_child, creds, path, object_regex, object_types,
                   recurse_depth, it->cp.key, it->cp.val, it->cp.v_size, it->cp.n_keys_total);

    log_printf(5, "it_child=%p\n", it->it_child);
    //** Clean up if an error occurred
    if (it->it_child == NULL) {
        ostc_destroy_object_iter(it);
        it = NULL;
    }

    log_printf(5, "END\n");

    return(it);
}

//***********************************************************************
// ostc_open_object_fn - Handles the actual object open
//***********************************************************************

gop_op_status_t ostc_open_object_fn(void *arg, int tid)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    ostcdb_object_t *oo;
    gop_op_status_t status;
    ostc_fd_t *fd;
    tbx_stack_t tree;
    int err, rw_mode;

    log_printf(5, "mode=%d OS_MODE_READ_IMMEDIATE=%d fname=%s\n", op->mode, OS_MODE_READ_IMMEDIATE, op->path);

    if (op->mode & OS_MODE_READ_IMMEDIATE) { //** Can use a delayed open if the object is in cache
        tbx_stack_init(&tree);
        OSTC_LOCK(ostc);
        err = _ostc_lio_cache_tree_walk(op->os, op->path, &tree, NULL, 0, OSTC_MAX_RECURSE);
        if ((err == 0) && (op->mode & OS_MODE_NO_CACHE_INFO_IF_FILE)) { //** Not supposed to use the cache if this is a file
            tbx_stack_move_to_bottom(&tree);
            oo = tbx_stack_get_current_data(&tree);
            if (oo->ftype & OS_OBJECT_FILE_FLAG) {  // ** It's a file so don't use the cache
                err = 1;  //** Throw an error to force the file opening
                if (op->mode & OS_MODE_BLOCK_ONLY_IF_FILE) {  //** Force a blocking R/W open
                    rw_mode = op->mode & (OS_MODE_READ_IMMEDIATE|OS_MODE_READ_BLOCKING|OS_MODE_WRITE_IMMEDIATE|OS_MODE_WRITE_BLOCKING);
                    op->mode ^= rw_mode;  //** Clear the mode bits
                    if (rw_mode & (OS_MODE_READ_IMMEDIATE|OS_MODE_READ_BLOCKING)) {
                        op->mode |= OS_MODE_READ_BLOCKING;
                    } else if (rw_mode & (OS_MODE_WRITE_IMMEDIATE|OS_MODE_WRITE_BLOCKING)) {
                        op->mode |= OS_MODE_WRITE_BLOCKING;
                    }
                }
            } else { //** Not a file so clear the flag
                op->mode ^= OS_MODE_NO_CACHE_INFO_IF_FILE;
            }
        }
        OSTC_UNLOCK(ostc);
        tbx_stack_empty(&tree, 0);
        if (err == 0) goto finished;
    }

    //** Force an immediate file open
    op->gop = os_open_object(ostc->os_child, op->creds, op->path, op->mode & OS_MODE_BASE_MODES, op->id, &(op->cfd), op->max_wait);
    log_printf(5, "forced open of file. fname=%s gid=%d\n", op->path, gop_id(op->gop));
    status = gop_sync_exec_status(op->gop);
    op->gop = NULL;

    //** If it failed just return
    if (status.op_status == OP_STATE_FAILURE) return(status);

finished:
    //** Make my version of the FD
    tbx_type_malloc(fd, ostc_fd_t, 1);
    fd->fname = op->path;
    op->path = NULL;
    fd->fd_child = op->cfd;
    *op->fd = fd;
    fd->mode = op->mode & OS_MODE_BASE_MODES;
    fd->mflags = op->mode & OS_MODE_FLAGS;
    fd->creds = op->creds;
    fd->id = op->id;
    fd->max_wait = op->max_wait;

    return(gop_success_status);
}


//***********************************************************************
//  ostc_open_free - Frees an open op structure
//***********************************************************************

void ostc_open_free(void *arg)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;

    if (op->path != NULL) free (op->path);
    if (op->gop != NULL) gop_free(op->gop, OP_DESTROY);
    free(op);
}

//***********************************************************************
//  ostc_open_object - Makes the open file op
//***********************************************************************

gop_op_generic_t *ostc_open_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode, char *id, os_fd_t **pfd, int max_wait)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op;
    gop_op_generic_t *gop;

    tbx_type_malloc(op, ostc_open_op_t, 1);
    op->os = os;
    op->creds = creds;
    op->path = strdup(path);
    op->mode = mode;
    op->fd = pfd;
    op->id = id;
    op->max_wait = max_wait;
    op->cfd = NULL;

    gop = gop_tp_op_new(ostc->tpc, NULL, ostc_open_object_fn, (void *)op, ostc_open_free, 1);

    gop_set_private(gop, op);
    return(gop);
}

//***********************************************************************
//  ostc_abort_open_object - Aborts an ongoing open file op
//***********************************************************************

gop_op_generic_t *ostc_abort_open_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op = (ostc_open_op_t *)gop_get_private(gop);

    return(os_abort_open_object(ostc->os_child, op->gop));
}


//***********************************************************************
// ostc_close_object_fn - Handles the actual object close
//***********************************************************************

gop_op_status_t ostc_close_object_fn(void *arg, int tid)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;
    ostc_fd_t *fd = (ostc_fd_t *)op->close_fd;

    status = (fd->fd_child != NULL) ? gop_sync_exec_status(os_close_object(ostc->os_child, fd->fd_child)) : gop_success_status;

    if (fd->fname != NULL) free(fd->fname);
    free(fd);

    return(status);
}

//***********************************************************************
//  ostc_close_object - Closes the object
//***********************************************************************

gop_op_generic_t *ostc_close_object(lio_object_service_fn_t *os, os_fd_t *ofd)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op;

    tbx_type_malloc(op, ostc_open_op_t, 1);
    op->os = os;
    op->close_fd = ofd;
    return(gop_tp_op_new(ostc->tpc, NULL, ostc_close_object_fn, (void *)op, free, 1));
}

//***********************************************************************
//  ostc_lock_user_object - Applies a user lock to the object
//***********************************************************************

gop_op_status_t ostc_lock_user_object_fn(void *arg, int tid)
{
    ostc_open_op_t *op = (ostc_open_op_t *)arg;
    ostc_priv_t *ostc = (ostc_priv_t *)op->os->priv;
    gop_op_status_t status;
    ostc_fd_t *fd = (ostc_fd_t *)op->close_fd;

    if (fd->fd_child == NULL) {
        status = ostc_delayed_open_object(op->os, fd);
        if (status.op_status == OP_STATE_FAILURE) return(status);
    }

    return(gop_sync_exec_status(os_lock_user_object(ostc->os_child, fd->fd_child, op->mode, op->max_wait)));

}

//***********************************************************************

gop_op_generic_t *ostc_lock_user_object(lio_object_service_fn_t *os, os_fd_t *ofd, int rw_mode, int max_wait)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    ostc_open_op_t *op;

    //** This is a bit ofa struct reuse hack
    tbx_type_malloc_clear(op, ostc_open_op_t, 1);
    op->os = os;
    op->close_fd = ofd;
    op->mode = rw_mode;
    op->max_wait = max_wait;
    return(gop_tp_op_new(ostc->tpc, NULL, ostc_lock_user_object_fn, (void *)op, free, 1));
}

//***********************************************************************
//  ostc_abort_lock_user_object - Aborts user lock operation
//***********************************************************************

gop_op_generic_t *ostc_abort_lock_user_object(lio_object_service_fn_t *os, gop_op_generic_t *gop)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_abort_lock_user_object(ostc->os_child, gop));
}

//***********************************************************************
//  ostc_fsck_object - Allocates space for the object check
//***********************************************************************

gop_op_generic_t *ostc_fsck_object(lio_object_service_fn_t *os, lio_creds_t *creds, char *fname, int ftype, int resolution)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_fsck_object(ostc->os_child, creds, fname, ftype, resolution));
}


//***********************************************************************
// ostc_next_fsck - Returns the next problem object
//***********************************************************************

int ostc_next_fsck(lio_object_service_fn_t *os, os_fsck_iter_t *oit, char **bad_fname, int *bad_atype)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_next_fsck(ostc->os_child, oit, bad_fname, bad_atype));
}

//***********************************************************************
// ostc_create_fsck_iter - Creates an fsck iterator
//***********************************************************************

os_fsck_iter_t *ostc_create_fsck_iter(lio_object_service_fn_t *os, lio_creds_t *creds, char *path, int mode)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    return(os_create_fsck_iter(ostc->os_child, creds, path, mode));
}


//***********************************************************************
// ostc_destroy_fsck_iter - Destroys an fsck iterator
//***********************************************************************

void ostc_destroy_fsck_iter(lio_object_service_fn_t *os, os_fsck_iter_t *oit)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;

    os_destroy_fsck_iter(ostc->os_child, oit);
}

//***********************************************************************
// ostc_print_running_config - Prints the running config
//***********************************************************************

void ostc_print_running_config(lio_object_service_fn_t *os, FILE *fd, int print_section_heading)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    int i;

    if (print_section_heading) fprintf(fd, "[%s]\n", ostc->section);
    fprintf(fd, "type = %s\n", OS_TYPE_TIMECACHE);
    fprintf(fd, "os_child = %s\n", ostc->os_child_section);
    fprintf(fd, "entry_timeout = %ld #seconds\n", apr_time_sec(ostc->entry_timeout));
    fprintf(fd, "cleanup_interval = %ld #seconds\n", apr_time_sec(ostc->cleanup_interval));
    fprintf(fd, "# n_limit_cache = %d\n", ostc->n_limit_cache);
    fprintf(fd, "# limit_cache = timeout_in_us:[glob|regex]:<attr_string>  -- String format.  Multiple limit_cache keys are supported\n");
    for (i=0; i<ostc->n_limit_cache; i++) {
        fprintf(fd, "limit_cache = %s\n", ostc->limit_cache[i].string);
    }
    fprintf(fd, "\n");

    os_print_running_config(ostc->os_child, fd, 1);
}

//***********************************************************************
// parse_limit_cache_attrs - Adds the limited caching attrs
//***********************************************************************

void parse_limit_cache_attrs(lio_object_service_fn_t *os, tbx_inip_file_t *ifd, const char *section)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *s, *e, *key, *val;
    tbx_stack_t *stack;
    int n, to;

    //** Find our group
    ig = tbx_inip_group_first(ifd);
    while ((ig != NULL) && (strcmp(section, tbx_inip_group_get(ig)) != 0)) {
        ig = tbx_inip_group_next(ig);
    }

    if (ig == NULL) return;  //** Nothing to do so kick out

    //**Cycle through the tags
    ele = tbx_inip_ele_first(ig);
    stack = NULL;
    for (ele = tbx_inip_ele_first(ig); ele != NULL; ele = tbx_inip_ele_next(ele)) {
        //** Check if this is a limit_cache key
        key = tbx_inip_ele_get_key(ele);
        if (strcmp("limit_cache", key) != 0) continue;

        //** Got a match if we made it here
        s = tbx_inip_ele_get_value(ele);
        if (!s) continue;  //** No value so skip

        //** Add it to the stack
        if (stack == NULL) stack = tbx_stack_new();
        tbx_stack_push(stack, strdup(s));
    }

    if (stack == NULL) return;  //Nothing found so kick out

    //** If we made it here then we have some objects to parse
    ostc->n_limit_cache = tbx_stack_count(stack);
    tbx_type_malloc_clear(ostc->limit_cache, limit_cache_t, ostc->n_limit_cache);
    n = 0;
    while ((val = tbx_stack_pop(stack)) != NULL) {
        ostc->limit_cache[n].string = strdup(val);  //** Store the string

        //** Peel off the timeout and store it
        e = index(val, ':');
        s = val;
        to = 0;
        if (e) {
            e[0] = '\0';
            to = tbx_stk_string_get_integer(s);
            s = e + 1;
        }
        ostc->limit_cache[n].timeout = to;

        //** Now parse the glob or regex
        if (lio_os_globregex_parse(&(ostc->limit_cache[n].regex), s) != 0) {
            log_printf(0, "ERROR: Failed parsing nocache glob/regex! string=%s\n", ostc->limit_cache[n].string);
            fprintf(stderr, "ERROR: Failed parsing nocache glob/regex! string=%s\n", ostc->limit_cache[n].string);
            fflush(stderr);
            abort();
        }

        free(val);
        n++;
    }

    tbx_stack_free(stack, 1);
}

//***********************************************************************
// ostc_destroy
//***********************************************************************

void ostc_destroy(lio_object_service_fn_t *os)
{
    ostc_priv_t *ostc = (ostc_priv_t *)os->priv;
    apr_status_t value;
    limit_cache_t *lca;
    int i;

    tbx_siginfo_handler_remove(SIGUSR1, ostc_info_fn, os);

    //** Shut the child down
    if (ostc->os_child != NULL) {
        os_destroy(ostc->os_child);
    }

    //** Signal we're shutting down
    OSTC_LOCK(ostc);
    ostc->shutdown = 1;
    apr_thread_cond_signal(ostc->cond);
    OSTC_UNLOCK(ostc);

    //** Wait for the cleanup thread to complete
    apr_thread_join(&value, ostc->cleanup_thread);

    //** Dump the cache 1 last time just to be safe. The hardlink table should have been cleared out when the ref counts hit zero
    _ostc_cleanup(os, ostc->cache_root, apr_time_now() + 4*ostc->entry_timeout);
    free_ostcdb_object(os, ostc->cache_root, &ostc->n_objects_removed, &ostc->n_attrs_removed);

    //** Do the final cleanup
    apr_thread_mutex_destroy(ostc->lock);
    apr_thread_cond_destroy(ostc->cond);
    apr_pool_destroy(ostc->mpool);   //** This also destroys the hardlink hash

    if (ostc->n_limit_cache > 0) {
        for (i=0; i<ostc->n_limit_cache; i++) {
            lca = &(ostc->limit_cache[i]);
            free(lca->string);
            regfree(&(lca->regex));
        }

        free(ostc->limit_cache);
    }

    free(ostc->section);
    free(ostc->os_child_section);
    free(ostc);
    free(os);
}

//***********************************************************************
// ostc_make_root - Makes the root node for the cache
//***********************************************************************

void ostc_make_root(lio_object_service_fn_t *os)
{
    ostc_priv_t *ostc = os->priv;
    ostc_base_object_info_t base;

    memset(&base, 0, sizeof(base));
    base.ftype = OS_OBJECT_DIR_FLAG;
    base.link_count = 1;
    ostc->cache_root = new_ostcdb_object(strdup("/"), &base, 0, ostc->mpool);
    ostc->n_objects_created++;

}

//***********************************************************************
//  object_service_timecache_create - Creates a remote client OS
//***********************************************************************

lio_object_service_fn_t *object_service_timecache_create(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section)
{
    lio_object_service_fn_t *os;
    ostc_priv_t *ostc;
    os_create_t *os_create;
    char *str, *ctype;

    log_printf(10, "START\n");
    if (section == NULL) section = ostc_default_options.section;

    tbx_type_malloc_clear(os, lio_object_service_fn_t, 1);
    tbx_type_malloc_clear(ostc, ostc_priv_t, 1);
    os->priv = (void *)ostc;

    ostc->section = strdup(section);
    str = tbx_inip_get_string(fd, section, "os_child", ostc_default_options.os_child_section);
    if (str != NULL) {  //** Running in test/temp
        ctype = tbx_inip_get_string(fd, str, "type", OS_TYPE_REMOTE_CLIENT);
        os_create = lio_lookup_service(ess, OS_AVAILABLE, ctype);
        ostc->os_child = (*os_create)(ess, fd, str);
        if (ostc->os_child == NULL) {
            log_printf(1, "Error loading object service!  type=%s section=%s\n", ctype, str);
            fprintf(stderr, "Error loading object service!  type=%s section=%s\n", ctype, str);
            fflush(stderr);
            abort();
        }
        free(ctype);
    } else {
        log_printf(0, "ERROR:  Missing child OS!\n");
        abort();
    }
    ostc->os_child_section = str;

    //** See if there are any attributes to not cache
    parse_limit_cache_attrs(os, fd, section);

    ostc->entry_timeout = apr_time_from_sec(tbx_inip_get_integer(fd, section, "entry_timeout", ostc_default_options.entry_timeout));
    ostc->cleanup_interval = apr_time_from_sec(tbx_inip_get_integer(fd, section, "cleanup_interval",ostc_default_options.cleanup_interval));

    apr_pool_create(&ostc->mpool, NULL);
    apr_thread_mutex_create(&(ostc->lock), APR_THREAD_MUTEX_DEFAULT, ostc->mpool);
    apr_thread_mutex_create(&(ostc->delayed_lock), APR_THREAD_MUTEX_DEFAULT, ostc->mpool);
    apr_thread_cond_create(&(ostc->cond), ostc->mpool);
    ostc->hardlink_objects = apr_hash_make(ostc->mpool);

    //** Get the thread pool to use
    ostc->tpc = lio_lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED);FATAL_UNLESS(ostc->tpc != NULL);

    //** Make the root node
    ostc_make_root(os);

    //** Set up the fn ptrs
    os->type = OS_TYPE_TIMECACHE;

    os->print_running_config = ostc_print_running_config;
    os->destroy_service = ostc_destroy;
    os->exists = ostc_exists;
    os->realpath = ostc_realpath;
    os->exec_modify = ostc_object_exec_modify;
    os->create_object = ostc_create_object;
    os->create_object_with_attrs = ostc_create_object_with_attrs;
    os->remove_object = ostc_remove_object;
    os->remove_regex_object = ostc_remove_regex_object;
    os->abort_remove_regex_object = ostc_abort_remove_regex_object;
    os->move_object = ostc_move_object;
    os->symlink_object = ostc_symlink_object;
    os->hardlink_object = ostc_hardlink_object;
    os->create_object_iter = ostc_create_object_iter;
    os->create_object_iter_alist = ostc_create_object_iter_alist;
    os->next_object = ostc_next_object;
    os->destroy_object_iter = ostc_destroy_object_iter;
    os->open_object = ostc_open_object;
    os->close_object = ostc_close_object;
    os->abort_open_object = ostc_abort_open_object;
    os->lock_user_object = ostc_lock_user_object;
    os->abort_lock_user_object = ostc_abort_lock_user_object;
    os->get_attr = ostc_get_attr;
    os->set_attr = ostc_set_attr;
    os->symlink_attr = ostc_symlink_attr;
    os->copy_attr = ostc_copy_attr;
    os->get_multiple_attrs = ostc_get_multiple_attrs;
    os->get_multiple_attrs_immediate = ostc_get_multiple_attrs_immediate;
    os->set_multiple_attrs = ostc_set_multiple_attrs;
    os->set_multiple_attrs_immediate = ostc_set_multiple_attrs_immediate;
    os->copy_multiple_attrs = ostc_copy_multiple_attrs;
    os->symlink_multiple_attrs = ostc_symlink_multiple_attrs;
    os->move_attr = ostc_move_attr;
    os->move_multiple_attrs = ostc_move_multiple_attrs;
    os->regex_object_set_multiple_attrs = ostc_regex_object_set_multiple_attrs;
    os->abort_regex_object_set_multiple_attrs = ostc_abort_regex_object_set_multiple_attrs;
    os->create_attr_iter = ostc_create_attr_iter;
    os->next_attr = ostc_next_attr;
    os->destroy_attr_iter = ostc_destroy_attr_iter;

    os->create_fsck_iter = ostc_create_fsck_iter;
    os->destroy_fsck_iter = ostc_destroy_fsck_iter;
    os->next_fsck = ostc_next_fsck;
    os->fsck_object = ostc_fsck_object;

    tbx_siginfo_handler_add(SIGUSR1, ostc_info_fn, os);
    tbx_thread_create_assert(&(ostc->cleanup_thread), NULL, ostc_cache_compact_thread, (void *)os, ostc->mpool);

    log_printf(10, "END\n");

    return(os);
}

