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

#define _log_module_index 193

//#define _GNU_SOURCE  //**Already set
#include <dlfcn.h>
#include <apr.h>
#include <apr_dso.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <ctype.h>
#include <gop/mq_ongoing.h>
#include <gop/mq_stream.h>
#include <gop/mq.h>
#include <gop/tp.h>
#include <lio/lio.h>
#include <malloc.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/constructor.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/io.h>
#include <tbx/monitor.h>
#include <tbx/skiplist.h>
#include <tbx/siginfo.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <lio/notify.h>

#include "authn.h"
#include "authn/fake.h"
#include "blacklist.h"
#include "cache.h"
#include "cache/amp.h"
#include "ds.h"
#include "ds/ibp.h"
#include "ex3.h"
#include "ex3/system.h"
#include "lio.h"
#include "remote_config.h"
#include "os.h"
#include "os/file.h"
#include "os/remote.h"
#include "rs.h"
#include "rs/simple.h"
#include "service_manager.h"
#include "segment/lun.h"

typedef struct {
    int count;
    void *object;
    char *key;
} lc_object_container_t;

typedef struct {
    char *prefix;
    int len;
    char *shortcut;
} lfs_mount_t;

int lio_parallel_task_count = 100;

lio_config_t lio_default_options = {
    .timeout = 60,  //********** FIXME should be in APR time! *************
    .max_attr = 10*1024*1024,
    .calc_adler32 = 0,
    .readahead = 0,
    .readahead_trigger = 0,
    .stream_buffer_max_size = 0,
    .stream_buffer_min_size = 0,
    .stream_buffer_total_size = 0,
    .small_files_in_metadata_max_size = 0,
    .path_is_literal = 0,
    .tpc_unlimited_count = 300,
    .tpc_max_recursion = 10,
    .tpc_cache_count = 100,
    .tpc_ongoing_count = 100,
    .blacklist_section = NULL,
    .monitor_fname = "/tmp/lio.mon",
    .monitor_enable = 0,
    .section_name = "lio",
    .mq_section = "mq_context",
    .ds_section = DS_TYPE_IBP,
    .rs_section = "rs_simple",
    .notify_section = "lio_notify",
    .os_section = "os_remote_client",
    .authn_section = "authn",
    .cache_section = "cache_amp",
    .special_file_prefix = "/tmp/lfs_special.",
    .root_prefix = NULL,
    .creds_user = NULL
};

// ** Define the global LIO config
lio_config_t *lio_gc = NULL;
lio_cache_t *_lio_cache = NULL;
tbx_log_fd_t *lio_ifd = NULL;
FILE *_lio_ifd = NULL;
char *_lio_exe_name = NULL;

int _monitoring_state = 0;
char *_monitoring_fname = NULL;

int _lfs_mount_count = -1;
lfs_mount_t *lfs_mount = NULL;

apr_pool_t *_lc_mpool = NULL;
apr_thread_mutex_t *_lc_lock = NULL;
tbx_list_t *_lc_object_list = NULL;

lio_config_t *lio_create_nl(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name, int make_monitor, lio_path_tuple_t **lc_tuple);
void lio_destroy_nl(lio_config_t *lio);

//** These are for releasing memory
typedef void *(tcfree_t)();
tcfree_t *_tcfree = NULL;

char **myargv = NULL;  // ** This is used to hold the new argv we return from lio_init so we can properly clean it up

//***************************************************************
//  memory_usage_dump - Dumps the memory usage to the FD
//***************************************************************

void memory_usage_dump(FILE *fd)
{
    int stderr_old;
    fpos_t stderr_pos, fd_pos;

    fprintf(fd, "-----------Memory usage-------------\n");
    fflush(stderr);  fflush(fd);
    fgetpos(stderr, &stderr_pos);
    stderr_old = dup(fileno(stderr));
    fgetpos(fd, &fd_pos);
    dup2(fileno(fd), fileno(stderr));
    fsetpos(stderr, &fd_pos);
    malloc_stats();
    dup2(stderr_old, fileno(stderr));
    close(stderr_old);
    fsetpos(stderr, &stderr_pos);
    fprintf(fd, "\n");
}

//***************************************************************
// memory_release_fn - Releases all the memory bask to the Kernel
//***************************************************************

void memory_release_fn(void *arg, FILE *fd)
{
    if (_tcfree) {
        fprintf(fd, "---------------------------------- Initial usage --------------------------------------------\n");
        memory_usage_dump(fd);
        _tcfree();
        fprintf(fd, "---------------------------------- After release --------------------------------------------\n");
        memory_usage_dump(fd);
    } else {
        fprintf(fd, "tcmalloc not used. Option not available\n");
    }
}

//***************************************************************
// lio_print_running_config - Prints the running config
//***************************************************************

void lio_print_running_config(FILE *fd, lio_config_t *lio)
{
    char text[1024];

    fprintf(fd, "[%s]\n", lio->section_name);
    fprintf(fd, "timeout = %d # seconds FIXME!!! Should be in APR time!\n", lio->timeout);
    fprintf(fd, "max_attr_size = %s\n", tbx_stk_pretty_print_int_with_scale(lio->max_attr, text));
    fprintf(fd, "calc_adler32 = %d\n", lio->calc_adler32);
    fprintf(fd, "readahead = %s\n", tbx_stk_pretty_print_int_with_scale(lio->readahead, text));
    fprintf(fd, "readahead_trigger = %s\n", tbx_stk_pretty_print_int_with_scale(lio->readahead_trigger, text));
    fprintf(fd, "stream_buffer_max_size = %s\n", tbx_stk_pretty_print_int_with_scale(lio->stream_buffer_max_size, text));
    fprintf(fd, "stream_buffer_min_size = %s\n", tbx_stk_pretty_print_int_with_scale(lio->stream_buffer_min_size, text));
    fprintf(fd, "stream_buffer_total_size = %s\n", tbx_stk_pretty_print_int_with_scale(lio->stream_buffer_total_size, text));
    fprintf(fd, "small_files_in_metadata_max_size = %s\n", tbx_stk_pretty_print_int_with_scale(lio->small_files_in_metadata_max_size, text));
    fprintf(fd, "tpc_unlimited = %d\n", lio->tpc_unlimited_count);
    fprintf(fd, "tpc_ongoing = %d  # Only used for ongoing in SERVER mode\n", lio->tpc_ongoing_count);
    fprintf(fd, "tpc_max_recursion = %d\n", lio->tpc_max_recursion);
    fprintf(fd, "tpc_cache = %d\n", lio->tpc_cache_count);
    fprintf(fd, "blacklist= %s\n", lio->blacklist_section);
    fprintf(fd, "open_close_lock_size = %d\n", lio->open_close_lock_size);
    fprintf(fd, "mq = %s\n", lio->mq_section);
    fprintf(fd, "ds = %s\n", lio->ds_section);
    fprintf(fd, "rs = %s\n", lio->rs_section);
    fprintf(fd, "os = %s\n", lio->os_section);
    fprintf(fd, "server_address = %s\n", (lio->server_address == NULL) ? "<CLIENT-MODE>" : lio->server_address);
    fprintf(fd, "remote_config = %s\n", lio->rc_section);
    fprintf(fd, "cache = %s\n", lio->cache_section);
    fprintf(fd, "user = %s\n", lio->creds_user);
    fprintf(fd, "notify = %s\n", lio->notify_section);
    fprintf(fd, "monitor_fname = %s\n", lio->monitor_fname);
    fprintf(fd, "monitor_enable = %d  #touch %s-enable to start logging or touch %s-disable to stop logging and then trigger a state dump\n", lio->monitor_enable, lio->monitor_fname, lio->monitor_fname);
    fprintf(fd, "#lun_max_retry_size = ....  #See the LUN global retry stats below for the value\n");
    lio_print_flag_service(lio->ess, ESS_RUNNING, fd);
    fprintf(fd, "\n");

    rc_print_running_config(fd);
    cache_print_running_config(lio->cache, fd, 1);
    gop_mq_print_running_config(lio->mqc, fd, 1);
    os_print_running_config(lio->os, fd, 1);
    ds_print_running_config(lio->ds, fd, 1);
    rs_print_running_config(lio->rs, fd, 1);
    authn_print_running_config(lio->authn, fd, 1);
    tbx_notify_print_running_config(lio->notify, fd, 1);
    lun_global_print_running_stats(NULL, fd, 1);

    memory_usage_dump(fd);
}

//***************************************************************
//** lio_dump_running_config_fn - Dumps the config
//***************************************************************

void lio_dump_running_config_fn(void *arg, FILE *fd)
{
    lio_config_t *lc = (lio_config_t *)arg;

    fprintf(fd, "---------------------------------- LIO config start --------------------------------------------\n");
    lio_print_running_config(fd, lc);
    fprintf(fd, "---------------------------------- LIO config end --------------------------------------------\n");
    fprintf(fd, "\n");
}

//***************************************************************
//** lio_monitor_fn - See if we need to adjust the monitoring state
//***************************************************************

void lio_monitor_fn(void *arg, FILE *fd)
{
    lio_config_t *lc = (lio_config_t *)arg;
    char buffer[OS_PATH_MAX];

    snprintf(buffer, sizeof(buffer), "%s-enable", lc->monitor_fname);
    buffer[OS_PATH_MAX-1] = '\0';
    if (lio_os_local_filetype(buffer)) {
        if (lc->monitor_enable == 0) {
            fprintf(fd, "-------- Enabling monitoring logs fname:%s\n", lc->monitor_fname);
            lc->monitor_enable = 1;
            tbx_monitor_set_state(1);
            return;  //** Kick out
        }
    }

    snprintf(buffer, sizeof(buffer), "%s-disable", lc->monitor_fname);
    buffer[OS_PATH_MAX-1] = '\0';
    if (lio_os_local_filetype(buffer)) {
        if (lc->monitor_enable == 1) {
            fprintf(fd, "-------- Disabling monitoring logs fname:%s\n", lc->monitor_fname);
            lc->monitor_enable = 0;
            tbx_monitor_set_state(0);
            return;  //** Kick out
        }
    }
}


//***************************************************************
// check_for_section - Checks to make sure the section
//    exists in the config file and if it doesn't it complains
//    and exists
//***************************************************************

void check_for_section(tbx_inip_file_t *fd, char *section, char *err_string)
{
    if (tbx_inip_group_find(fd, section) == NULL) {
        fprintf(stderr, "Missing section! section=%s\n", section);
        fprintf(stderr, "%s", err_string);
        fflush(stderr);
        exit(EINVAL);
    }
}

//***************************************************************
//  _lc_object_destroy - Decrements the LC object and removes it
//       if no other references exist.  It returns the number of
//       remaining references.  So it can be safely destroyed
//       when 0 is returned.
//***************************************************************

int _lc_object_destroy_ptr(char *key, void **obj)
{
    int count = 0;
    lc_object_container_t *lcc;

    lcc = tbx_list_search(_lc_object_list, key);
    if (lcc != NULL) {
        lcc->count--;
        count = lcc->count;
        log_printf(15, "REMOVE key=%s count=%d lcc=%p\n", key, count, lcc);

        *obj = NULL;
        if (lcc->count <= 0) {
            *obj = lcc->object;
            tbx_list_remove(_lc_object_list, key, lcc);
            free(lcc->key);
            free(lcc);
        }
    } else {
        *obj = NULL;
        log_printf(15, "REMOVE-FAIL key=%s\n", key);
    }

    return(count);
}

//***************************************************************
//  _lc_object_destroy - Decrements the LC object and removes it
//       if no other references exist.  It returns the number of
//       remaining references.  So it can be safely destroyed
//       when 0 is returned.
//***************************************************************

int _lc_object_destroy(char *key)
{
    void *obj;

    return(_lc_object_destroy_ptr(key, &obj));
}

//***************************************************************
//  _lc_object_get - Retrieves an LC object
//***************************************************************

void *_lc_object_get(char *key)
{
    void *obj = NULL;
    lc_object_container_t *lcc;

    lcc = tbx_list_search(_lc_object_list, key);
    if (lcc != NULL) {
        lcc->count++;
        obj = lcc->object;
    }

    if (obj != NULL) {
        log_printf(15, "GET key=%s count=%d lcc=%p\n", key, lcc->count, lcc);
    } else {
        log_printf(15, "GET-FAIL key=%s\n", key);
    }
    return(obj);
}

//***************************************************************
//  lc_object_put - Adds an LC object
//***************************************************************

void _lc_object_put(char *key, void *obj)
{
    lc_object_container_t *lcc;

    tbx_type_malloc(lcc, lc_object_container_t, 1);

    log_printf(15, "PUT key=%s count=1 lcc=%p\n", key, lcc);

    lcc->count = 1;
    lcc->object = obj;
    lcc->key = strdup(key);
    tbx_list_insert(_lc_object_list, lcc->key, lcc);

    return;
}

//***************************************************************
// _lio_load_plugins - Loads the optional plugins
//***************************************************************

void _lio_load_plugins(lio_config_t *lio, tbx_inip_file_t *fd)
{
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *key;
    apr_dso_handle_t *handle;
    int error;
    void *sym;
    char *section, *name, *library, *symbol;
    char buffer[1024];

    g = tbx_inip_group_first(fd);
    while (g) {
        if (strcmp(tbx_inip_group_get(g), "plugin") == 0) { //** Got a Plugin section
            //** Parse the plugin section
            ele = tbx_inip_ele_first(g);
            section = name = library = symbol = NULL;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                if (strcmp(key, "section") == 0) {
                    section = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "name") == 0) {
                    name = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "library") == 0) {
                    library = tbx_inip_ele_get_value(ele);
                } else if (strcmp(key, "symbol") == 0) {
                    symbol = tbx_inip_ele_get_value(ele);
                }

                ele = tbx_inip_ele_next(ele);
            }

            //** Sanity check it
            error = 1;
            if ((section == NULL) || (name == NULL) || (library == NULL) || (symbol == NULL)) goto fail;

            //** Attempt to load it
            snprintf(buffer, sizeof(buffer), "plugin_handle:%s", library);
            handle = _lc_object_get(buffer);
            if (handle == NULL) {
                if (apr_dso_load(&handle, library, _lc_mpool) != APR_SUCCESS) goto fail;
                _lc_object_put(buffer, handle);
                add_service(lio->ess, "plugin", buffer, handle);
            }
            if (apr_dso_sym(&sym, handle, symbol) != APR_SUCCESS) goto fail;

            if (lio->plugin_stack == NULL) lio->plugin_stack = tbx_stack_new();
            log_printf(5, "library=%s\n", buffer);
            tbx_stack_push(lio->plugin_stack, strdup(buffer));
            add_service(lio->ess, section, name, sym);

            error = 0;  //** Skip cleanup
fail:
            if (error != 0) {
                log_printf(0, "ERROR loading plugin!  section=%s name=%s library=%s symbol=%s\n", section, name, library, symbol);
            }
        }
        g = tbx_inip_group_next(g);
    }
}

//***************************************************************
// _lio_destroy_plugins - Destroys the optional plugins
//***************************************************************

void _lio_destroy_plugins(lio_config_t *lio)
{
    char *library_key;
    apr_dso_handle_t *handle;
    int count;

    if (lio->plugin_stack == NULL) return;

    while ((library_key = tbx_stack_pop(lio->plugin_stack)) != NULL) {
        log_printf(5, "library_key=%s\n", library_key);
        count = _lc_object_destroy(library_key);
        if (count <= 0) {
            handle = lio_lookup_service(lio->ess, "plugin", library_key);
            if (handle != NULL) apr_dso_unload(handle);
        }
        free(library_key);
    }

    tbx_stack_free(lio->plugin_stack, 0);
}


//***************************************************************
//  lio_find_lfs_mounts - Finds the LFS mounts and creates the global
//     internal table for use by the path commands
//***************************************************************

void lio_find_lfs_mounts()
{
    tbx_stack_t *stack;
    FILE *fd;
    lfs_mount_t *entry;
    char *text, *prefix, *bstate, *shortcut;
    int i, fin;
    size_t ns;

    stack = tbx_stack_new();

    fd = tbx_io_fopen("/proc/mounts", "r");
    if (fd) {
        text = NULL;
        while (getline(&text, &ns, fd) != -1) {
            log_printf(5, "getline=%s", text);
            if (strncasecmp(text, "lfs:", 4) == 0) { //** Found a match
                shortcut = tbx_stk_string_token(text, " ", &bstate, &fin);
                prefix = tbx_stk_string_token(NULL, " ", &bstate, &fin);
                if (prefix != NULL) { //** Add it
                    tbx_type_malloc_clear(entry, lfs_mount_t, 1);
                    if (shortcut) {
                        entry->shortcut = strdup(shortcut + 4);
                    }
                    entry->prefix = strdup(prefix);
                    entry->len = strlen(entry->prefix);
                    tbx_stack_push(stack, entry);
                    log_printf(5, "mount prefix=%s len=%d shortcut=%s\n", entry->prefix, entry->len, entry->shortcut);
                }
            }

            free(text);
            text = NULL;
        }
        if (text != NULL)
            free(text);  //** Getline() always returns something
    }
    //** Convert it to a simple array
    _lfs_mount_count = tbx_stack_count(stack);
    tbx_type_malloc(lfs_mount, lfs_mount_t, _lfs_mount_count);
    for (i=0; i<_lfs_mount_count; i++) {
        entry = tbx_stack_pop(stack);
        lfs_mount[i].shortcut = entry->shortcut;
        lfs_mount[i].prefix = entry->prefix;
        lfs_mount[i].len = entry->len;
        free(entry);
    }

    tbx_stack_free(stack, 0);
}

//***************************************************************
//  lio_path_release - Decrs a path tuple object
//***************************************************************

void lio_path_release(lio_path_tuple_t *tuple)
{
    char buffer[1024];
    lio_path_tuple_t *tuple2;
    lio_config_t *lc2 = NULL;
    int anon = 0;

    if (tuple->path != NULL) {
        if (strcmp(tuple->path, "ANONYMOUS") == 1) {
            anon = 1;
        } else {
            free(tuple->path);
        }
    }
    if (tuple->lc == NULL) return;

    apr_thread_mutex_lock(_lc_lock);
    snprintf(buffer, sizeof(buffer), "%s", tuple->lc->creds_name);

    log_printf(15, "START object=%s\n", buffer); tbx_log_flush();
    if (_lc_object_destroy_ptr(buffer, (void **)&tuple2) == 0) {
        if (tuple2) {
            if (_lc_object_destroy_ptr(tuple2->lc->obj_name, (void **)&lc2) == 0) {
                lio_destroy_nl(lc2);
            }
            free(tuple2);
            if (anon) free(tuple);
        }
    }

    tuple->lc = NULL;
    log_printf(15, "END object=%s\n", buffer);

    apr_thread_mutex_unlock(_lc_lock);

    return;
}

//***************************************************************
// lio_path_local_make_absolute - Converts the local relative
//     path to an absolute one.
//***************************************************************

void lio_path_local_make_absolute(lio_path_tuple_t *tuple)
{
    char *p, *rp, *pp;
    int i, n, last_slash, glob_index;
    char path[OS_PATH_MAX];
    char c;

    log_printf(5, "initial path=%s\n", tuple->path);
    if (tuple->path == NULL) return;

    p = tuple->path;
    n = strlen(p);
    last_slash = -1;
    glob_index = -1;
    if ((p[0] == '*') || (p[0] == '?') || (p[0] == '[')) goto wildcard;

    for (i=0; i<n; i++) {
        if (p[i] == '/') last_slash = i;
        if ((p[i] == '*') || (p[i] == '?') || (p[i] == '[')) {
            if (p[i-1] != '\\') {
                glob_index = i;
                break;
            }
        }
    }

    log_printf(5, "last_slash=%d\n", last_slash);

wildcard:
    if (last_slash == -1) {  //** Just using the CWD as the base for the glob
        if (strcmp(p, ".") == 0) {
            pp = realpath(".", path);
            last_slash = n;
        } else if (strcmp(p, "..") == 0) {
            pp = realpath("..", path);
            last_slash = n;
        } else {
            pp = realpath(".", path);
            last_slash = 0;
        }

        if (last_slash != n) {
            if (pp == NULL) i = 0;  //** This is a NULL statement but it makes the compiler happy about not using the return of realpath
            i = strlen(path);
            path[i] = '/';  //** Need to add the trailing / to the path
            path[i+1] = 0;
        }
        rp = strdup(path);
    } else {
        if (last_slash == -1) last_slash = n;
        c = p[last_slash];
        p[last_slash] = 0;
        rp = realpath(p, NULL);
        p[last_slash] = c;
        if ((p[n-1] == '/') && (last_slash == n)) last_slash--;  //** '/' terminator so preserve it
    }

    log_printf(5, "p=%s realpath=%s last_slash=%d n=%d glob_index=%d\n", p, rp, last_slash, n, glob_index);

    if (rp != NULL) {
        if (last_slash == n) {
            tuple->path = rp;
        } else {
            snprintf(path, sizeof(path), "%s%s", rp, &(p[last_slash]));
            tuple->path = strdup(path);
            free(rp);
        }
        free(p);
    }
}

//***************************************************************
// lio_path_wildcard_auto_append - Auto appends a "*" if the path
//      ends in "/".
//     Returns 0 if no change and 1 if a wildcard was added.
//***************************************************************

int lio_path_wildcard_auto_append(lio_path_tuple_t *tuple)
{
    int n;

    if (tuple->path == NULL) return(0);

    n = strlen(tuple->path);
    if (tuple->path[n-1] == '/') {
        tuple->path = realloc(tuple->path, n+1+1);
        tuple->path[n] = '*';
        tuple->path[n+1] = 0;

        log_printf(5, " tweaked tuple=%s\n", tuple->path);
        return(1);
    }

    return(0);
}

//*************************************************************************
// lio_path_tuple_copy - Returns a new tuple with just the path different
//*************************************************************************

lio_path_tuple_t lio_path_tuple_copy(lio_path_tuple_t *curr, char *fname)
{
    lio_path_tuple_t tuple;
    lio_path_tuple_t *t2;
    char buffer[4096];

    tuple = *curr;
    tuple.path = fname;    //** We don't use the helper because this routine would be called after the tuple parsing

    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_account(curr->creds, NULL), curr->lc->obj_name);
    apr_thread_mutex_lock(_lc_lock);
    t2 = _lc_object_get(buffer);
    apr_thread_mutex_unlock(_lc_lock);

    if (t2 == NULL) {
        log_printf(0, "ERROR: missing tuple! obj=%s\n", buffer);
        fprintf(stderr, "ERROR: missing tuple! obj=%s\n", buffer);
        abort();
    }
    return(tuple);
}

//***************************************************************
// tuple_fname_helper
//***************************************************************

char *tuple_fname_helper(lio_config_t *lc, char *fname)
{
    char buf[OS_PATH_MAX];
    char *new_fname;

    if (lc->root_prefix && fname) {
        snprintf(buf, sizeof(buf), "%s%s", lc->root_prefix, fname);
        new_fname = strdup(buf);
        free(fname);
        return(new_fname);
    }

    return(fname);
}

//***************************************************************
//  lio_path_resolve_base - Returns a  path tuple object
//      containing the cred, lc, and path
//***************************************************************

lio_path_tuple_t lio_path_resolve_base_full(char *lpath, int path_is_literal)
{
    char *userid, *pp_section, *fname, *pp_mq, *pp_host, *pp_cfg, *config, *obj_name, *hints_string;
    void *cred_args[2];
    lio_path_tuple_t tuple, *tuple2;
    tbx_inip_file_t *ifd;
    char uri[1024];
    char buffer[2048];
    int n, is_lio, pp_port;
    time_t ts;

    ts = 0;
    userid = NULL;
    pp_mq = NULL;
    pp_host = NULL;
    pp_port = 0;
    pp_cfg = NULL;
    pp_section = NULL;
    hints_string = NULL;
    fname = NULL;
    memset(&tuple, 0, sizeof(tuple));

    is_lio = lio_parse_path(lpath, &userid, &pp_mq, &pp_host, &pp_port, &pp_cfg, &pp_section, NULL, &fname, path_is_literal);
    if (is_lio == -1) { //** Can't parse the path
        memset(&tuple, 0, sizeof(tuple));
        goto finished;
    }

    if (!fname) {
        memset(&tuple, 0, sizeof(tuple));
        is_lio = 0;
        fprintf(stderr, "lio_path_resolve_base:  ERROR: fname=NULL lpath=%s\n", lpath);
        goto finished;
    }

    if ((lio_gc) && (!pp_mq) && (!pp_host) && (!pp_cfg) && (!pp_section) && (pp_port == 0)) { //** Check if we just have defaults if so use the global context
        strncpy(uri, lio_gc->obj_name, sizeof(uri)-1);
        uri[sizeof(uri)-1] = '\0';
    } else {
        if (!pp_mq) pp_mq = strdup(LIO_MQ_NAME_DEFAULT);
        if (pp_port == 0) pp_port = 6711;
        if (!pp_section) pp_section = strdup("lio");

        //** Based on the host we may need to adjust the config file
        if (pp_host) {
            if (!pp_cfg) pp_cfg = strdup("lio");
        } else if (!pp_cfg) {
            pp_cfg = strdup("LOCAL");
        }

        snprintf(uri, sizeof(uri), "lstore://%s|%s:%d:%s:%s", pp_mq, pp_host, pp_port, pp_cfg, pp_section);
    }

    log_printf(15, "START: lpath=%s user=%s uri=%s path=%s\n", lpath, userid, uri, fname);

    apr_thread_mutex_lock(_lc_lock);

    if (userid == NULL) {
        snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_account(lio_gc->creds, NULL), uri);
    } else {
        snprintf(buffer, sizeof(buffer), "tuple:%s@%s", userid, uri);
    }

    tuple2 = _lc_object_get(buffer);
    if (tuple2 != NULL) { //** Already exists!
        tuple = *tuple2;
        tuple.path = tuple_fname_helper(tuple.lc, fname);
        goto finished;
    }

    //** Get the LC
    n = 0;
    tuple.lc = _lc_object_get(uri);
    if (tuple.lc == NULL) { //** Doesn't exist so need to load it
        if (pp_host == NULL) {
            tuple.lc = lio_create_nl(lio_gc->ifd, pp_section, userid, uri, _lio_exe_name, 0, &tuple2);  //** Use the non-locking routine
            if (tuple.lc == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname != NULL) free(fname);
                goto finished;
            }
            tuple = *tuple2;  //** Copy the one we just created over since it also has the creds
            tuple.lc->ifd = tbx_inip_dup(tuple.lc->ifd);  //** Dup the ifd
            tuple.lc->anonymous_creation = 1;
            tuple.path = tuple_fname_helper(tuple.lc, fname);
            goto finished;
        } else { //** Look up using the remote config query
            if (rc_client_get_config(NULL, NULL, uri, NULL,  &config, &hints_string, &obj_name, NULL, &ts) != 0) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname != NULL) free(fname);
                goto finished;
            }

            strncpy(uri, obj_name, sizeof(uri));
            uri[sizeof(uri)-1] = '\0';
            free(obj_name);

            ifd = tbx_inip_string_read_with_hints_string(config, hints_string, 1);
            if (config) free(config);
            if (hints_string) free(hints_string);
            if (ifd == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                if (fname) free(fname);
                goto finished;
            }
            tuple.lc = lio_create_nl(ifd, pp_section, strdup(userid), uri, _lio_exe_name, 0, &tuple2);  //** Use the non-locking routine
            if (tuple.lc == NULL) {
                memset(&tuple, 0, sizeof(tuple));
                tbx_inip_destroy(ifd);
                if (fname != NULL) free(fname);
                goto finished;
            }

            tuple = *tuple2;  //** Copy the one we just created over sine it also has the creds
            tuple.lc->anonymous_creation = 1;
            tuple.path = tuple_fname_helper(tuple.lc, fname);
            goto finished;
        }

        tuple.lc->anonymous_creation = 1;
        n = 1; //** Flag as anon for cred check
    }

    //** Now determine the user
    if (userid == NULL) {
        userid = an_cred_get_account(tuple.lc->creds, NULL);  //** IF not specified default to the one in the LC
    }

    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", userid, uri);
    tuple2 = _lc_object_get(buffer);
    if (tuple2 == NULL) { //** Doesn't exist so insert the tuple
        cred_args[0] = userid;
        tuple.creds = authn_cred_init(tuple.lc->authn, AUTHN_INIT_DEFAULT, (void **)cred_args);
        tbx_type_malloc_clear(tuple2, lio_path_tuple_t, 1);
        tuple2->creds = tuple.creds;
        tuple2->lc = tuple.lc;
        tuple2->path = "ANONYMOUS";
        log_printf(15, "adding anon creds tuple=%s ptr=%p\n", buffer, tuple2);
        _lc_object_put(buffer, tuple2);  //** Add it to the table
    } else if (n==1) {//** This is default user tuple just created so mark it as anon as well
        log_printf(15, "marking anon creds tuple=%s\n", buffer);
        tuple2->path = "ANONYMOUS-DEFAULT";
    }

    tuple.creds = tuple2->creds;
    tuple.path = tuple_fname_helper(tuple.lc, fname);

finished:
    apr_thread_mutex_unlock(_lc_lock);

    if (pp_mq != NULL) free(pp_mq);
    if (pp_host != NULL) free(pp_host);
    if (pp_cfg != NULL) free(pp_cfg);
    if (pp_section != NULL) free(pp_section);
    if (userid != NULL) free(userid);
    if (hints_string) free(hints_string);

    tuple.is_lio = is_lio;
    log_printf(15, "END: uri=%s path=%s is_lio=%d\n", tuple.path, uri, tuple.is_lio);
    return(tuple);
}

lio_path_tuple_t lio_path_resolve_base(char *lpath)
{
    return(lio_path_resolve_base_full(lpath, lio_gc->path_is_literal));  //TWEAK-ME
}

//***************************************************************
// lio_path_auto_fuse_convert - Automatically detects and converts
//    local paths sitting on a LFS mount
//***************************************************************

lio_path_tuple_t lio_path_auto_fuse_convert_full(lio_path_tuple_t *ltuple, int path_is_literal)
{
    char path[OS_PATH_MAX];
    lio_path_tuple_t tuple;
    int do_convert, prefix_len, i;

    if (ltuple->path == NULL) {
        fprintf(stderr, "lio_path_auto_fuse_convert: ERROR: Missing path!\n");
        return(*ltuple);
    }

    //** Convert if to an absolute path
    lio_path_local_make_absolute(ltuple);
    tuple = *ltuple;

    //** Now check if the prefixes match
    for (i=0; i < _lfs_mount_count; i++) {
        if (strncmp(ltuple->path, lfs_mount[i].prefix, lfs_mount[i].len) == 0) {
            do_convert = 0;
            prefix_len = lfs_mount[i].len;
            if ((int)strlen(ltuple->path) > prefix_len) {
                if (ltuple->path[prefix_len] == '/') {
                    do_convert = 1;
                    snprintf(path, sizeof(path), "@@%s:%s", lfs_mount[i].shortcut, &(ltuple->path[prefix_len]));
                }
            } else {
                do_convert = 1;
                snprintf(path, sizeof(path), "@@%s:/", lfs_mount[i].shortcut);
            }

            if (do_convert == 1) {
                log_printf(5, "auto convert\n");
                log_printf(5, "auto convert path=%s\n", path);
                tuple = lio_path_resolve_base_full(path, path_is_literal);
                lio_path_release(ltuple);
                break;  //** Found a match so kick out
            }
        }
    }

    return(tuple);
}

//***************************************************************

lio_path_tuple_t lio_path_auto_fuse_convert(lio_path_tuple_t *ltuple)
{
    return(lio_path_auto_fuse_convert_full(ltuple, lio_gc->path_is_literal)); //**TWEAK-ME
}

//***************************************************************
//  lio_path_resolve - Returns a  path tuple object
//      containing the cred, lc, and path
//***************************************************************

lio_path_tuple_t lio_path_resolve_full(int auto_fuse_convert, char *lpath, int path_is_literal)
{
    lio_path_tuple_t tuple;

    tuple = lio_path_resolve_base_full(lpath, path_is_literal);

    log_printf(5, "auto_fuse_convert=%d\n", auto_fuse_convert);
    if ((tuple.is_lio == 0) && (auto_fuse_convert > 0)) {
        return(lio_path_auto_fuse_convert_full(&tuple, path_is_literal));
    }

    return(tuple);
}

//***************************************************************

lio_path_tuple_t lio_path_resolve(int auto_fuse_convert, char *lpath)
{
    return(lio_path_resolve_full(auto_fuse_convert, lpath, lio_gc->path_is_literal)); //**TWEAK-ME
}

//***************************************************************
// lc_object_remove_unused  - Removes unused LC's from the global
//     table.  The default, remove_all_unsed=0, is to only
//     remove anonymously created LC's.
//***************************************************************

void lc_object_remove_unused(int remove_all_unused)
{
    tbx_list_t *user_lc;
    tbx_list_iter_t it;
    lc_object_container_t *lcc, *lcc2;
    lio_path_tuple_t *tuple;
    lio_config_t *lc;
    char *key;
    tbx_stack_t *stack;

    stack = tbx_stack_new();

    apr_thread_mutex_lock(_lc_lock);

    //** Make a list of all the different LC's in use from the tuples
    //** Keep track of the anon creds for deletion
    user_lc = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);
    it = tbx_list_iter_search(_lc_object_list, "tuple:", 0);
    while ((tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&lcc)) == 0) {
        if (strncmp(lcc->key, "tuple:", 6) != 0) break;  //** No more tuples
        tuple = lcc->object;
        if (tuple->path == NULL) {
            tbx_list_insert(user_lc, tuple->lc->obj_name, tuple->lc);
            log_printf(15, "user_lc adding key=%s lc=%s\n", lcc->key, tuple->lc->obj_name);
        } else {
            log_printf(15, "user_lc marking creds key=%s for removal\n", lcc->key);
            if (strcmp(tuple->path, "ANONYMOUS") == 0) tbx_stack_push(stack, lcc);
        }
    }

    //** Go ahead and delete the anonymously created creds (as long as they aren't the LC default
    while ((lcc = tbx_stack_pop(stack)) != NULL) {
        tuple = lcc->object;
        _lc_object_destroy(lcc->key);
        an_cred_destroy(tuple->creds);
    }

    //** Now iterate through all the LC's
    it = tbx_list_iter_search(_lc_object_list, "lstore://", 0);
    while ((tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&lcc)) == 0) {
        if (strncmp(lcc->key, "lstore://", 9) != 0) break;  //** No more LCs
        lc = lcc->object;
        log_printf(15, "checking key=%s lc=%s anon=%d count=%d\n", lcc->key, lc->obj_name, lc->anonymous_creation, lcc->count);
        lcc2 = tbx_list_search(user_lc, lc->obj_name);
        if (lcc2 == NULL) {  //** No user@lc reference so safe to delete from that standpoint
            log_printf(15, "not in user_lc key=%s lc=%s anon=%d count=%d\n", lcc->key, lc->obj_name, lc->anonymous_creation, lcc->count);
            if (((lc->anonymous_creation == 1) && (lcc->count <= 1)) ||
                    ((remove_all_unused == 1) && (lcc->count <= 0))) {
                tbx_stack_push(stack, lcc);
            }
        }
    }

    while ((lcc = tbx_stack_pop(stack)) != NULL) {
        lc = lcc->object;
        _lc_object_destroy(lcc->key);
        lio_destroy_nl(lc);
    }

    apr_thread_mutex_unlock(_lc_lock);

    tbx_stack_free(stack, 0);
    tbx_list_destroy(user_lc);

    return;
}

//***************************************************************
// lio_print_options - Prints the standard supported lio options
//   Use "LIO_COMMON_OPTIONS" in the arg list
//***************************************************************

void lio_print_options(FILE *fd)
{
    fprintf(fd, "    LIO_COMMON_OPTIONS\n");
    fprintf(fd, "       -d level           - Set the debug level (0-20).  Defaults to 0\n");
    fprintf(fd, "       -log log_out       - Set the log output file.  Defaults to using config setting\n");
    fprintf(fd, "       -no-auto-lfs       - Disable auto-conversion of LFS mount paths to lio\n");
    fprintf(fd, "       -c config_uri      - Config file to use.  Either local or remote\n");
    fprintf(fd, "          [lstore://][user@][MQ_NAME|]HOST:[port:][cfg:][section]\n");
    fprintf(fd, "                            Get the config from a remote LServer\n");
    fprintf(fd, "          [ini://]/path/to/ini_file\n");
    fprintf(fd, "                            Local INI config file\n");
    fprintf(fd, "          file:///path/to/file\n");
    fprintf(fd, "                            File with a single line containing either an lstore or init URI\n");
    fprintf(fd, "       -lc user@config    - Use the user and config section specified for making the default LIO\n");
    fprintf(fd, "       -u userid          - User account to use\n");
    fprintf(fd, "       -np N              - Number of simultaneous operations. Default is %d.\n", lio_parallel_task_count);
    fprintf(fd, "       -i N               - Print information messages of level N or greater. No header is printed\n");
    fprintf(fd, "       -it N              - Print information messages of level N or greater. Thread ID header is used\n");
    fprintf(fd, "       -if N              - Print information messages of level N or greater. Full header is used\n");
    fprintf(fd, "       -ilog info_log_out - Where to send informational log output.\n");
    fprintf(fd, "       --path-literals    - All paths are treated as literal fixed strings\n");
    fprintf(fd, "       --fcreds creds_file      - Specific file for user credentials. Must be accesssible only by the user!\n");
    fprintf(fd, "                                  Can also be set via the environment variable %s\n", LIO_ENV_KEY_FILE);
    fprintf(fd, "       --print-config           - Print the parsed config with hints applied but not substitutions.\n");
    fprintf(fd, "       --print-config-with-subs - Print the parsed config with substitutions applied.\n");
    fprintf(fd, "       --print-running-config   - Print the running config.\n");
    fprintf(fd, "       --monitor_fname fname    - Set the monitoring log file\n");
    fprintf(fd, "       --monitor_state 0|1      - Enable/Disable monitoring\n");
    fprintf(fd, "\n");
    tbx_inip_print_hint_options(fd);
}

//***************************************************************
//  lio_destroy_nl - Destroys a LIO config object - NO locking
//***************************************************************

void lio_destroy_nl(lio_config_t *lio)
{
    lc_object_container_t *lcc;
    lio_path_tuple_t *tuple;
    lio_segment_t *seg;
    char lc_obj[1024];

    //** Update the lc count for the creds
    log_printf(1, "Destroying LIO context %s\n", lio->obj_name);

    if (_lc_object_destroy(lio->obj_name) > 0) {  //** Still in use so return.
        return;
    }

    //** Destroy the recovery log if it  exists
    seg = lio_lookup_service(lio->ess, ESS_RUNNING, "recovery_segment");
    remove_service(lio->ess, ESS_RUNNING, "recovery_segment");
    if (seg) {
        tbx_obj_put(&seg->obj);
    }

    //** The creds is a little tricky cause we need to get the tuple first
    lcc = tbx_list_search(_lc_object_list, lio->creds_name);
    tuple = (lcc != NULL) ? lcc->object : NULL;
    if (_lc_object_destroy(lio->creds_name) <= 0) {
        if (lio->creds) an_cred_destroy(lio->creds);
        free(tuple);
    }

    free(lio->creds_name);

    log_printf(15, "removing lio=%s\n", lio->obj_name);

    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", lio->rs_section, lio->obj_name);
    if (_lc_object_destroy(lc_obj) <= 0) {
        rs_destroy_service(lio->rs);
    }
    free(lio->rs_section);

    ds_attr_destroy(lio->ds, lio->da);
    if (_lc_object_destroy(lio->ds_section) <= 0) {
        ds_destroy_service(lio->ds);
    }
    free(lio->ds_section);

    if (_lc_object_destroy(ESS_ONGOING_CLIENT) <= 0) {
        gop_mq_ongoing_t *on = lio_lookup_service(lio->ess, ESS_RUNNING, ESS_ONGOING_CLIENT);
        if (on != NULL) {  //** And also the ongoing client
            gop_mq_ongoing_destroy(on);
        }
    }

    char *host_id = lio_lookup_service(lio->ess, ESS_RUNNING, ESS_ONGOING_HOST_ID);
    if (host_id) free(host_id);

    //** The OS should be destroyed AFTER the ongoing service since it's used by the onging
    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", lio->os_section, lio->obj_name);
    if (_lc_object_destroy(lc_obj) <= 0) {
        os_destroy_service(lio->os);
    }
    free(lio->os_section);


    if (_lc_object_destroy(lio->mq_section) <= 0) {  //** Destroy the MQ context
        //** Also shutdown the server portal and ongoing server if loaded
        gop_mq_portal_t *portal = lio_lookup_service(lio->ess, ESS_RUNNING, ESS_SERVER_PORTAL);
        if (portal) {
            gop_mq_portal_remove(lio->mqc, portal);
            gop_mq_ongoing_t *ons = lio_lookup_service(lio->ess, ESS_RUNNING, ESS_ONGOING_SERVER);
            if (ons) gop_mq_ongoing_destroy(ons);
            gop_mq_portal_destroy(portal);
        }
        gop_mq_destroy_context(lio->mqc);
    }
    free(lio->mq_section);

    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", lio->authn_section, lio->obj_name);
    if (_lc_object_destroy(lc_obj) <= 0) {  //** Destroy the AuthN
        if (lio->authn) {
            authn_destroy(lio->authn);
        }
    }
    free(lio->authn_section);

    if (_lc_object_destroy(lio->notify_section) <= 0) {
        tbx_notify_destroy(lio->notify);
    }
    free(lio->notify_section);

    if (lio->section_name != NULL) free(lio->section_name);

    void *val = lio_lookup_service(lio->ess, ESS_RUNNING, "jerase_paranoid");
    remove_service(lio->ess, ESS_RUNNING, "jerase_paranoid");
    if (val) free(val);
    val = lio_lookup_service(lio->ess, ESS_RUNNING, "jerase_max_parity_on_stack");
    remove_service(lio->ess, ESS_RUNNING, "jerase_max_parity_on_stack");
    if (val) free(val);

    _lio_destroy_plugins(lio);

    lio_exnode_service_set_destroy(lio->ess);
    lio_exnode_service_set_destroy(lio->ess_nocache);

    tbx_inip_destroy(lio->ifd);

    //** Table of open files
    tbx_list_destroy(lio->open_index);

    //** Remove ourselves from the info handler
    tbx_siginfo_handler_remove(SIGUSR1, lio_open_files_info_fn, lio);
    tbx_siginfo_handler_remove(SIGUSR1, lio_dump_running_config_fn, lio);
    tbx_siginfo_handler_remove(SIGUSR1, lio_monitor_fn, lio);

    //** Blacklist if used
    if (lio->blacklist != NULL) {
        blacklist_destroy(lio->blacklist);
    }
    if (lio->blacklist_section) free(lio->blacklist_section);

    if (lio->cache_section) free(lio->cache_section);
    if (lio->creds_user) free(lio->creds_user);

    if (lio->server_address) free(lio->server_address);

    //** Shutdown the thread pools after everything using it has been stopped
    if (_lc_object_destroy(lio->tpc_unlimited_section) <= 0) {
        gop_tp_context_destroy(lio->tpc_unlimited);
    }
    free(lio->tpc_unlimited_section);

    if (_lc_object_destroy(lio->tpc_cache_section) <= 0) {
        gop_tp_context_destroy(lio->tpc_cache);
    }
    free(lio->tpc_cache_section);

    if (_lc_object_destroy(lio->tpc_ongoing_section) <= 0) {
        gop_tp_context_destroy(lio->tpc_ongoing);
    }
    free(lio->tpc_ongoing_section);

    if (lio->special_file_prefix) free(lio->special_file_prefix);
    if (lio->monitor_fname) {
        tbx_monitor_destroy();
        free(lio->monitor_fname);
    }

    if (lio->host_id) free(lio->host_id);
    if (lio->open_close_lock) free(lio->open_close_lock);

    apr_thread_mutex_destroy(lio->lock);
    apr_pool_destroy(lio->mpool);

    if (lio->obj_name) free(lio->obj_name);
    if (lio->root_prefix) free(lio->root_prefix);

    if (lio->exe_name != NULL) free(lio->exe_name);
    free(lio);

    return;
}

//***************************************************************
//  lio_destroy - Destroys a LIO config object
//***************************************************************

void lio_destroy(lio_config_t *lio)
{
    apr_thread_mutex_lock(_lc_lock);
    lio_destroy_nl(lio);
    apr_thread_mutex_unlock(_lc_lock);
}

//***************************************************************
// lio_create_nl - Creates a lio configuration according to the config file
//   NOTE:  No locking is used
//***************************************************************

lio_config_t *lio_create_nl(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name, int make_monitor, lio_path_tuple_t **lc_tuple)
{
    lio_config_t *lio;
    int i, n, cores, max_recursion, err;
    char buffer[1024];
    char lc_obj[1024];
    void *cred_args[2];
    char *ctype, *stype;
    authn_create_t *authn_create;
    ds_create_t *ds_create;
    rs_create_t *rs_create;
    os_create_t *os_create;
    cache_load_t *cache_create;
    gop_mq_portal_t *portal;
    lio_path_tuple_t *tuple;
    gop_mq_ongoing_t *on = NULL;
    ex_id_t sid;
    lio_segment_t *seg;

    //** Add the LC first cause it may already exist
    log_printf(1, "START: Creating LIO context %s\n", obj_name);

    lio = _lc_object_get(obj_name);
    if (lio != NULL) {  //** Already loaded so can skip this part
        return(lio);
    }

    tbx_type_malloc_clear(lio, lio_config_t, 1);
    lio->ess = lio_exnode_service_set_create();
    lio->auto_translate = 1;
    if (section) lio->section_name = strdup(section);
    if (exe_name) lio->exe_name = strdup(exe_name);

    if (lc_tuple) *lc_tuple = NULL;

    //** Add it to the table for ref counting
    _lc_object_put(obj_name, lio);

    lio->obj_name = strdup(obj_name);

    lio->ifd = ifd;
    if (lio->ifd == NULL) {
        // TODO: The error handling here needs to be more measured
        log_printf(-1, "ERROR: Failed to parse INI1\n");
        return NULL;
    }

    //** Go ahead and load the service flags
    lio_load_inip_flag_service(lio->ess, ESS_RUNNING, lio->ifd, section);

    _lio_load_plugins(lio, lio->ifd);  //** Load the plugins

    check_for_section(lio->ifd, section, "No primary LIO config section!\n");
    lio->timeout = tbx_inip_get_integer(lio->ifd, section, "timeout", lio_default_options.timeout);
    lio->max_attr = tbx_inip_get_integer(lio->ifd, section, "max_attr_size", lio_default_options.max_attr);
    lio->calc_adler32 = tbx_inip_get_integer(lio->ifd, section, "calc_adler32", lio_default_options.calc_adler32);
    lio->readahead = tbx_inip_get_integer(lio->ifd, section, "readahead", lio_default_options.readahead);
    lio->readahead_trigger = tbx_inip_get_integer(lio->ifd, section, "readahead_trigger", lio_default_options.readahead_trigger);
    lio->stream_buffer_max_size = tbx_inip_get_integer(lio->ifd, section, "stream_buffer_max_size", lio_default_options.stream_buffer_max_size);
    lio->stream_buffer_min_size = tbx_inip_get_integer(lio->ifd, section, "stream_buffer_min_size", lio_default_options.stream_buffer_min_size);
    lio->stream_buffer_total_size = tbx_inip_get_integer(lio->ifd, section, "stream_buffer_total_size", lio_default_options.stream_buffer_total_size);
    lio->small_files_in_metadata_max_size = tbx_inip_get_integer(lio->ifd, section, "small_files_in_metadata_max_size", lio_default_options.small_files_in_metadata_max_size);
    lio->special_file_prefix = tbx_inip_get_string(lio->ifd, section, "special_file_prefix", lio_default_options.special_file_prefix);
    lio->root_prefix = tbx_inip_get_string(lio->ifd, section, "root_prefix", lio_default_options.root_prefix);

    //** Get the mount's UUID. Most of the time this isn't set specifically in the section but instead stored as a parameter
    stype = tbx_inip_get_string_full(lio->ifd, section, "uuid", "${uuid}", &err);
    if (stype[0] == '$') { //** Didn't get  one
        lio->uuid = -1;
    } else {
        lio->uuid = atoll(stype);
        if (lio->uuid == 0) lio->uuid = -1;
    }
    free(stype);

    //** Set up the monitoring
    if (make_monitor == 1) {
        lio->monitor_fname = (_monitoring_fname) ? strdup(_monitoring_fname) : tbx_inip_get_string(lio->ifd, section, "monitor_fname", lio_default_options.monitor_fname);
        lio->monitor_enable = (_monitoring_state == 1) ? 1 : tbx_inip_get_integer(lio->ifd, section, "monitor_enable", lio_default_options.monitor_enable);
        tbx_monitor_create(lio->monitor_fname);
        if (lio->monitor_enable) tbx_monitor_set_state(1);
    }

    lio->path_is_literal = tbx_inip_get_integer(lio->ifd, section, "path_is_literal", lio_default_options.path_is_literal);
    cores = tbx_inip_get_integer(lio->ifd, section, "tpc_unlimited", lio_default_options.tpc_unlimited_count);
    lio->tpc_unlimited_count = cores;
    max_recursion = tbx_inip_get_integer(lio->ifd, section, "tpc_max_recursion", lio_default_options.tpc_max_recursion);
    lio->tpc_max_recursion = max_recursion;
    sprintf(buffer, "tpc:%d", cores);
    stype = buffer;
    lio->tpc_unlimited_section = strdup(stype);
    lio->tpc_unlimited = _lc_object_get(stype);
    if (lio->tpc_unlimited == NULL) {  //** Need to load it
        n = 0.1 * cores;
        if (n > 10) n = 10;
        if (n <= 0) n = 1;
        lio->tpc_unlimited = gop_tp_context_create("UNLIMITED", n, cores, max_recursion);
        if (lio->tpc_unlimited == NULL) {
            log_printf(0, "Error loading tpc_unlimited threadpool!  n=%d\n", cores);
            fprintf(stderr, "ERROR createing tpc_unlimited threadpool! n=%d\n", cores);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->tpc_unlimited);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_TPC_UNLIMITED, lio->tpc_unlimited);


    cores = tbx_inip_get_integer(lio->ifd, section, "tpc_cache", lio_default_options.tpc_cache_count);
    lio->tpc_cache_count = cores;
    sprintf(buffer, "tpc-cache:%d", cores);
    stype = buffer;
    lio->tpc_cache_section = strdup(stype);
    lio->tpc_cache = _lc_object_get(stype);
    if (lio->tpc_cache == NULL) {  //** Need to load it
        n = 0.1 * cores;
        if (n > 10) n = 10;
        if (n <= 0) n = 1;
        lio->tpc_cache = gop_tp_context_create("CACHE", n, cores, max_recursion);
        if (lio->tpc_cache == NULL) {
            log_printf(0, "Error loading tpc_cache threadpool!  n=%d\n", cores);
            fprintf(stderr, "ERROR createing tpc_cache threadpool! n=%d\n", cores);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->tpc_cache);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_TPC_CACHE, lio->tpc_cache);

    //** Also need to do the ongoing fail pool for any ONGOING_SERVER objects
    cores = tbx_inip_get_integer(lio->ifd, section, "tpc_ongoing", lio_default_options.tpc_ongoing_count);
    if (cores < 1.1*max_recursion) cores += max_recursion;
    lio->tpc_ongoing_count = cores;
    sprintf(buffer, "tpc-ongoing:%d", cores);
    stype = buffer;
    lio->tpc_ongoing_section = strdup(stype);
    lio->tpc_ongoing = _lc_object_get(stype);
    if (lio->tpc_ongoing == NULL) {  //** Need to load it
        lio->tpc_ongoing = gop_tp_context_create("ONGOING", 0, cores, max_recursion);  //** No need for any standby threads
        if (lio->tpc_ongoing == NULL) {
            log_printf(0, "Error loading tpc_ongoing threadpool!  n=%d\n", cores);
            fprintf(stderr, "ERROR createing tpc_ongoing threadpool! n=%d\n", cores);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->tpc_ongoing);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_TPC_ONGOING, lio->tpc_ongoing);


    stype = tbx_inip_get_string(lio->ifd, section, "mq", lio_default_options.mq_section);
    lio->mq_section = stype;
    lio->mqc = _lc_object_get(stype);
    if (lio->mqc == NULL) {  //** Need to load it
        lio->mqc = gop_mq_create_context(lio->ifd, stype);
        if (lio->mqc == NULL) {
            log_printf(1, "Error loading MQ service! stype=%s\n", stype);
            fprintf(stderr, "ERROR loading MQ service! stype=%s\n", stype);
            fflush(stderr);
            abort();
        } else {
            add_service(lio->ess, ESS_RUNNING, ESS_MQ, lio->mqc);  //** It's used by the block loader
        }

        _lc_object_put(stype, lio->mqc);  //** Add it to the table

        //** Add the shared ongoing object
        on = gop_mq_ongoing_create(lio->mqc, NULL, 5, ONGOING_CLIENT, lio->tpc_ongoing);
        _lc_object_put(ESS_ONGOING_CLIENT, on);  //** Add it to the table
    } else {
        add_service(lio->ess, ESS_RUNNING, ESS_MQ, lio->mqc);  //** It's used by other services
        on = _lc_object_get(ESS_ONGOING_CLIENT);
    }
    add_service(lio->ess, ESS_RUNNING, ESS_ONGOING_CLIENT, on);
    char *host_id = gop_generate_host_id(NULL, NULL, 60, -1);
    add_service(lio->ess, ESS_RUNNING, ESS_ONGOING_HOST_ID, host_id);
    lio->host_id = strdup(host_id);
    lio->host_id_len = strlen(host_id);

    //** check if we're running as a server
    portal = NULL;
    lio->server_address = tbx_inip_get_string(lio->ifd, section, "server_address", NULL);
    if (lio->server_address) {
        //** Make the Server Portal
        portal = gop_mq_portal_create(lio->mqc, lio->server_address, MQ_CMODE_SERVER);
        add_service(lio->ess, ESS_RUNNING, ESS_SERVER_PORTAL, portal);

        //** And the ongoing stream object
        gop_mq_ongoing_t *ons = gop_mq_ongoing_create(lio->mqc, portal, 30, ONGOING_SERVER, lio->tpc_ongoing);
        add_service(lio->ess, ESS_RUNNING, ESS_ONGOING_SERVER, ons);

        //** This is to handle client stream responses
        gop_mq_command_table_t *ctable = gop_mq_portal_command_table(portal);
        gop_mq_command_set(ctable, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, ons, gop_mqs_server_more_cb);
    }

    //** Load the notification service
    stype = tbx_inip_get_string(lio->ifd, section, "notify", lio_default_options.notify_section);
    if (strcmp(stype,lio_default_options.notify_section) != 0) check_for_section(lio->ifd, stype, "No Notify Service (notify) found in LIO config!\n");
    lio->notify_section = stype;
    lio->notify = _lc_object_get(stype);
    if (lio->notify == NULL) {  //** Need to load it
        lio->notify = tbx_notify_create(lio->ifd, NULL, lio->notify_section);
        if (lio->notify == NULL) {
            log_printf(1, "Error loading notification service!  section=%s\n", stype);
            fprintf(stderr, "Error loading notification service! section=%s\n", stype);
            fflush(stderr);
            abort();
        }

        _lc_object_put(stype, lio->notify);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_NOTIFY, lio->notify);

    //** Check and see if we need to enable the blacklist
    lio->blacklist_section = tbx_inip_get_string(lio->ifd, section, "blacklist", lio_default_options.blacklist_section);
    if (lio->blacklist_section != NULL) { //** Yup we need to parse and load those params
        check_for_section(lio->ifd, section, "No blacklist section found!\n");
        lio->blacklist = blacklist_load(lio->ifd, lio->blacklist_section, lio->notify);
        add_service(lio->ess, ESS_RUNNING, "blacklist", lio->blacklist);
    }

    //** Load the authentication service
    stype = tbx_inip_get_string(lio->ifd, section, "authn", lio_default_options.authn_section);
    if (strcmp(stype,lio_default_options.authn_section) != 0) check_for_section(lio->ifd, stype, "No AuthN Service (authn) found in LIO config!\n");
    lio->authn_section = stype;
    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", stype, lio->obj_name);
    lio->authn = _lc_object_get(lc_obj);
    if (lio->authn == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", AUTHN_TYPE_FAKE);
        authn_create = lio_lookup_service(lio->ess, AUTHN_AVAILABLE, ctype);
        lio->authn = (*authn_create)(lio->ess, lio->ifd, stype);
        if (lio->authn == NULL) {
            log_printf(1, "Error loading Authentication service!  type=%s section=%s\n", ctype, stype);
            fprintf(stderr, "Error loading Authentication service!  type=%s section=%s\n", ctype, stype);
            fflush(stderr);
            abort();
        }
        free(ctype);

        _lc_object_put(lc_obj, lio->authn);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_AUTHN, lio->authn);

    stype = tbx_inip_get_string(lio->ifd, section, "ds", lio_default_options.ds_section);
    if (strcmp(stype, lio_default_options.ds_section) != 0) check_for_section(lio->ifd, stype, "No primary Data Service (ds) found in LIO config!\n");
    lio->ds_section = stype;
    lio->ds = _lc_object_get(stype);
    if (lio->ds == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", DS_TYPE_IBP);
        ds_create = lio_lookup_service(lio->ess, DS_SM_AVAILABLE, ctype);
        lio->ds = (*ds_create)(lio->ess, lio->ifd, stype);
        if (lio->ds == NULL) {
            log_printf(1, "Error loading data service!  type=%s\n", ctype);
            fprintf(stderr, "ERROR loading data service!  type=%s\n", ctype);
            fflush(stderr);
            abort();
        } else {
            add_service(lio->ess, DS_SM_RUNNING, ctype, lio->ds);  //** It's used by the block loader
        }
        free(ctype);

        _lc_object_put(stype, lio->ds);  //** Add it to the table
    } else {
        add_service(lio->ess, DS_SM_RUNNING, stype, lio->ds);  //** It's used by the block loader
    }
    lio->da = ds_attr_create(lio->ds);

    add_service(lio->ess, ESS_RUNNING, ESS_DS, lio->ds);  //** This is needed by the RS service
    add_service(lio->ess, ESS_RUNNING, ESS_DA, lio->da);  //** This is needed by the RS service

    stype = tbx_inip_get_string(lio->ifd, section, "rs", lio_default_options.rs_section);
    if (strcmp(stype,lio_default_options.rs_section) != 0) check_for_section(lio->ifd, stype, "No Resource Service (rs) found in LIO config!\n");
    lio->rs_section = stype;
    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", stype, lio->obj_name);
    lio->rs = _lc_object_get(lc_obj);
    if (lio->rs == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", RS_TYPE_SIMPLE);
        rs_create = lio_lookup_service(lio->ess, RS_SM_AVAILABLE, ctype);
        lio->rs = (*rs_create)(lio->ess, lio->ifd, stype);
        if (lio->rs == NULL) {
            log_printf(1, "Error loading resource service!  type=%s section=%s\n", ctype, stype);
            fprintf(stderr, "Error loading resource service!  type=%s section=%s\n", ctype, stype);
            fflush(stderr);
            abort();
        }
        free(ctype);

        _lc_object_put(lc_obj, lio->rs);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_RS, lio->rs);

    stype = tbx_inip_get_string(lio->ifd, section, "os", lio_default_options.os_section);
    if (strcmp(stype, lio_default_options.os_section) != 0) check_for_section(lio->ifd, stype, "No Object Service (os) found in LIO config!\n");
    lio->os_section = stype;
    snprintf(lc_obj, sizeof(lc_obj), "%s:%s", stype, lio->obj_name);
    lio->os = _lc_object_get(lc_obj);
    if (lio->os == NULL) {  //** Need to load it
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", OS_TYPE_REMOTE_CLIENT);
        os_create = lio_lookup_service(lio->ess, OS_AVAILABLE, ctype);
        lio->os = (*os_create)(lio->ess, lio->ifd, stype);
        if (lio->os == NULL) {
            log_printf(1, "Error loading object service!  type=%s section=%s\n", ctype, stype);
            fprintf(stderr, "Error loading object service!  type=%s section=%s\n", ctype, stype);
            fflush(stderr);
            abort();
        }
        free(ctype);

        _lc_object_put(lc_obj, lio->os);  //** Add it to the table
    }
    add_service(lio->ess, ESS_RUNNING, ESS_OS, lio->os);

    //** Launch the server portal if enabled
    if (portal) gop_mq_portal_install(lio->mqc, portal);

    ctype = user;
    if (!user) {
        ctype = tbx_inip_get_string(lio->ifd, section, "user", lio_default_options.creds_user);
    }
    n = (ctype) ? strlen(ctype) : 0;

    cred_args[0] = ctype; cred_args[1] = &n;
    snprintf(buffer, sizeof(buffer), "tuple:%s@%s", (char *)cred_args[0], lio->obj_name);
    tuple = _lc_object_get(buffer);
    if (tuple == NULL) {  //** Need to load it
        lio->creds = authn_cred_init(lio->authn, AUTHN_INIT_DEFAULT, (void **)cred_args);
        if (lio->creds == NULL) {
            log_printf(0, "ERROR: Failed getting creds! creds_user=%s\n", lio->creds_user); tbx_log_flush();
            fprintf(stderr, "ERROR: Failed getting creds! creds_user=%s\n", lio->creds_user); fflush(stderr);
        }
        tbx_type_malloc_clear(tuple, lio_path_tuple_t, 1);
        tuple->creds = lio->creds;
        tuple->lc = lio;
        if (user) {
            snprintf(buffer, sizeof(buffer), "tuple:%s@%s", user, lio->obj_name);
        } else if (tuple->creds) {
            snprintf(buffer, sizeof(buffer), "tuple:%s@%s", an_cred_get_account(tuple->creds, NULL), lio->obj_name);
        }
        lio->creds_name = strdup(buffer);
        if (lc_tuple) *lc_tuple = tuple;
        _lc_object_put(lio->creds_name, tuple);  //** Add it to the table
    } else {
        lio->creds = tuple->creds;
        lio->creds_name = strdup(buffer);
    }
    if (cred_args[0] != NULL) free(cred_args[0]);

    lio->creds_user = (lio->creds) ? strdup(an_cred_get_account(lio->creds, NULL)) : strdup("NONE");

    if (_lio_cache == NULL) {
        stype = tbx_inip_get_string(lio->ifd, section, "cache", lio_default_options.cache_section);
        if (strcmp(stype,lio_default_options.cache_section) != 0) check_for_section(lio->ifd, stype, "No Cache section found in LIO config!\n");
        lio->cache_section = strdup(stype);
        ctype = tbx_inip_get_string(lio->ifd, stype, "type", CACHE_TYPE_AMP);
        cache_create = lio_lookup_service(lio->ess, CACHE_LOAD_AVAILABLE, ctype);
        _lio_cache = (*cache_create)(lio->ess, lio->ifd, stype, lio->da, lio->timeout);
        if (_lio_cache == NULL) {
            log_printf(0, "Error loading cache service!  type=%s\n", ctype);
            fprintf(stderr, "Error loading cache service!  type=%s\n", ctype);
            fflush(stderr);
            abort();
        }
        free(stype);
        free(ctype);
    }

    //** This is just used for creating empty exnodes or dup them.
    //** Since it's missing the cache it doesn't get added to the global cache list
    //** and accidentally generate collisions.  Especially useful for mkdir to copy the parent exnode
    lio->ess_nocache = clone_service_manager(lio->ess);

    lio->cache = _lio_cache;
    add_service(lio->ess, ESS_RUNNING, ESS_CACHE, lio->cache);

    //** Table of open files
    lio->open_index = tbx_sl_new_full(10, 0.5, 0, &ex_id_compare, NULL, NULL, NULL);

    assert_result(apr_pool_create(&(lio->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(lio->lock), APR_THREAD_MUTEX_DEFAULT, lio->mpool);

    //** Make the open/close coordination locks
    lio->open_close_lock_size = tbx_inip_get_integer(lio->ifd, section, "open_close_lock_size", 1000);
    if (lio->open_close_lock_size <= 0) {
        log_printf(0, "ERROR:  Invalid open_close_lock_size! got=%d should be greater than zero!  Adjusting to 1000\n", lio->open_close_lock_size);
        fprintf(stderr, "ERROR:  Invalid open_close_lock_size! got=%d should be greater than zero!  Adjusting to 1000\n", lio->open_close_lock_size);
    }
    tbx_type_malloc_clear(lio->open_close_lock, apr_thread_mutex_t *, lio->open_close_lock_size);
    for (i=0; i<lio->open_close_lock_size; i++) {
        apr_thread_mutex_create(&(lio->open_close_lock[i]), APR_THREAD_MUTEX_DEFAULT, lio->mpool);
    }

    //** Add the recovery log segment if available
    sid = tbx_inip_get_integer(lio->ifd, section, "recovery_segment", -1);
    if (sid > 0) {
        lio_exnode_exchange_t ex;
        ex.type = EX_TEXT;
        ex.text.text = NULL;
        ex.text.fd = lio->ifd;
        seg = load_segment(lio->ess, sid, &ex);
        if (seg) {
            add_service(lio->ess, ESS_RUNNING, "recovery_segment", seg);
        }
    }

    //** Add ourselves to the info handler
    tbx_siginfo_handler_add(SIGUSR1, lio_open_files_info_fn, lio);
    tbx_siginfo_handler_add(SIGUSR1, lio_dump_running_config_fn, lio);
    tbx_siginfo_handler_add(SIGUSR1, lio_monitor_fn, lio);

    log_printf(1, "END: uri=%s\n", obj_name);

    return(lio);
}


//***************************************************************
// lio_create - Creates a lio configuration according to the config file
//***************************************************************

lio_config_t *lio_create(tbx_inip_file_t *ifd, char *section, char *user, char *obj_name, char *exe_name, int make_monitor)
{
    lio_config_t *lc;

    apr_thread_mutex_lock(_lc_lock);
    lc = lio_create_nl(ifd, section, user, obj_name, exe_name, make_monitor, NULL);
    apr_thread_mutex_unlock(_lc_lock);

    return(lc);
}

//***************************************************************
// lio_print_path_options - Prints the path options to the device
//***************************************************************

void lio_print_path_options(FILE *fd)
{
    fprintf(fd, "    LIO_PATH_OPTIONS: [-rp regex_path | -gp glob_path]  [-ro regex_objext | -go glob_object] [path_1 ... path_N]\n");
    fprintf(fd, "       -rp regex_path  - Regex of path to scan\n");
    fprintf(fd, "       -gp glob_path   - Glob of path to scan\n");
    fprintf(fd, "       -ro regex_obj   - Regex for final object selection.\n");
    fprintf(fd, "       -go glob_obj    - Glob for final object selection.\n");
    fprintf(fd, "       path1 .. pathN  - Glob of paths to target\n");
    fprintf(fd, "\n");
}

//***************************************************************
// lio_print_object_type_options - Prints the object type options to the device
//***************************************************************

void lio_print_object_type_options(FILE *fd, int obj_type_default)
{
    fprintf(fd, "    -t  object_types   - Types of objects to list or traverse. Bitwise OR of:\n");
    fprintf(fd, "                             %d=Files, %d=Directories, %d=symlink, %d=hardlink,\n", OS_OBJECT_FILE_FLAG, OS_OBJECT_DIR_FLAG, OS_OBJECT_SYMLINK_FLAG, OS_OBJECT_HARDLINK_FLAG);
    fprintf(fd, "                             %d=broken link, %d=executable, %d=virtual attribute, %d=Follow symlinks\n", OS_OBJECT_BROKEN_LINK_FLAG, OS_OBJECT_EXEC_FLAG, OS_OBJECT_VIRTUAL_FLAG, OS_OBJECT_FOLLOW_SYMLINK_FLAG);
    fprintf(fd, "                             %d=socket, %d=fifo %d=No broken links %d=No symlinks\n", OS_OBJECT_SOCKET_FLAG, OS_OBJECT_FIFO_FLAG, OS_OBJECT_NO_BROKEN_LINK_FLAG, OS_OBJECT_NO_SYMLINK_FLAG);
    fprintf(fd, "                             Default is %d.\n", obj_type_default);
}

//***************************************************************
// lio_parse_path_options - Parses the path options
//***************************************************************

int lio_parse_path_options(int *argc, char **argv, int auto_mode, lio_path_tuple_t *tuple, lio_os_regex_table_t **rp, lio_os_regex_table_t **ro)
{
    int nargs, i;

    *rp = NULL;
    *ro = NULL;

    if (*argc == 1) return(0);

    nargs = 1;  //** argv[0] is preserved as the calling name

    i=1;
    do {
        if (strcmp(argv[i], "-rp") == 0) { //** Regex for path
            i++;
            *tuple = lio_path_resolve(auto_mode, argv[i]);  //** Pick off the user/host
            *rp = lio_os_regex2table(tuple->path);
            i++;
        } else if (strcmp(argv[i], "-ro") == 0) {  //** Regex for object
            i++;
            *ro = lio_os_regex2table(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-gp") == 0) {  //** Glob for path
            i++;
            *tuple = lio_path_resolve(auto_mode, argv[i]);  //** Pick off the user/host
            *rp = lio_os_path_glob2regex(tuple->path);
            i++;
        } else if (strcmp(argv[i], "-go") == 0) {  //** Glob for object
            i++;
            *ro = lio_os_path_glob2regex(argv[i]);
            i++;
        } else {
            if (i!=nargs)argv[nargs] = argv[i];
            nargs++;
            i++;
        }
    } while (i<*argc);

    if (*argc == nargs) return(0);  //** Nothing was processed

    //** Adjust argv to reflect the parsed arguments
    *argc = nargs;

    return(1);
}

//***************************************************************
// lio_init - Initializes LIO for use.  argc and argv are
//    modified by removing LIO common options.
//
//   NOTE: This should be called AFTER any fork() calls because
//         it spawns new threads!
//***************************************************************

//char **t2 = NULL;
int lio_init(int *argc, char ***argvp)
{
    int i, j, k, ll, ll_override, neargs, nargs, auto_mode, ifll, if_mode, print_config, path_is_literal;
    time_t ts;
    FILE *fd;
    tbx_inip_file_t *ifd;
    tbx_stack_t *hints;
    char *name, *info_fname;
    char var[4096];
    char cname[8192];
    char *dummy = NULL;
    char *env;
    char **eargv;
    char **argv;
    char *out_override = NULL;
    char *cfg_name = NULL;
    char *config = NULL;
    char *remote_config = NULL;
    char *section_name = "lio";
    char *userid = NULL;
    char *hints_string = NULL;
    char *home;
    char *obj_name;
    char buffer[4096];
    char text[4096];

    if(NULL != lio_gc && lio_gc->ref_cnt > 0) {
        // lio_gc is a global singleton, if it is already initialized don't initialize again. (Note this implementation is not entirely immune to race conditions)
        lio_gc->ref_cnt++;
        return 0;
    }

    argv = *argvp;

    //** This is a dummy routine to trigger loading the TBX constructor in case we are using a static library
    tbx_construct_fn_static();

    //** Setup the info signal handler.  We'll reset the name after we've got a lio_gc
    tbx_siginfo_install(NULL, SIGUSR1);
    gop_init_opque_system();  //** Initialize GOP.  This needs to be done after any fork() calls
    exnode_system_init();

    tbx_set_log_level(-1);     //** Disables log output
    tbx_log_open("stderr", 0); //** But if it's changed via an opt let's have a place to put the output

    //** Create the lio object container
    apr_pool_create(&_lc_mpool, NULL);
    apr_thread_mutex_create(&_lc_lock, APR_THREAD_MUTEX_DEFAULT, _lc_mpool);
    _lc_object_list = tbx_list_create(0, &tbx_list_string_compare, NULL, tbx_list_no_key_free, tbx_list_no_data_free);

    //** Grab the exe name
    //** Determine the preferred environment variable based on the calling name to use for the args
    lio_os_path_split(argv[0], &dummy, &name);
    _lio_exe_name = name;
    if (dummy != NULL) free(dummy);
    j = strncmp(name, "lio_", 4) == 0 ? 4 : 0;
    i = 0;
    memcpy(var, "LIO_OPTIONS_", 12);
    k = 12;
    while ((name[j+i] != 0) && (i<3900)) {
        // FIXME: Bad for UTF-8
        var[k+i] = toupper(name[j+i]);
        i++;
    }
    var[k+i] = 0;

    env = getenv(var); //** Get the exe based options if available
    if (env == NULL) env = getenv("LIO_OPTIONS");  //** If not get the global default
    if (env != NULL) {  //** Got args so prepend them to the front of the list
        env = strdup(env);  //** Don't want to mess up the actual env variable
        eargv = NULL;
        tbx_stk_string2args(env, &neargs, &eargv);

        if (neargs > 0) {
            ll = *argc + neargs;
            tbx_type_malloc_clear(myargv, char *, ll);
            myargv[0] = argv[0];
            memcpy(&(myargv[1]), eargv, sizeof(char *)*neargs);
            if (*argc > 1) memcpy(&(myargv[neargs+1]), &(argv[1]), sizeof(char *)*(*argc - 1));
            argv = myargv;
            *argvp = myargv;
            *argc = ll;
            free(eargv);

        }
    }

    tbx_type_malloc_clear(myargv, char *, *argc);

    //** Parse any arguments
    nargs = 1;  //** argv[0] is preserved as the calling name
    myargv[0] = argv[0];
    i=1;
    print_config = 0;
    ll_override = -100;
    ifll = 0;
    if_mode = INFO_HEADER_NONE;
    info_fname = NULL;
    auto_mode = -1;
    path_is_literal = 0;

    //** Load the hints if we have any
    hints = tbx_stack_new();
    tbx_inip_hint_options_parse(hints, argv, argc);

    if (*argc < 2) goto no_args;  //** Nothing to parse

    do {
        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            i++;
            ll_override = atoi(argv[i]);
            tbx_set_log_level(ll_override);
            i++;
        } else if (strcmp(argv[i], "-u") == 0) { //** Default UserID/Account
            i++;
            userid = strdup(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-log") == 0) { //** Override log file output
            i++;
            out_override = argv[i];
            i++;
        } else if (strcmp(argv[i], "-no-auto-lfs") == 0) { //** Regex for path
            i++;
            auto_mode = 0;
        } else if (strcmp(argv[i], "-np") == 0) { //** Parallel task count
            i++;
            lio_parallel_task_count = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-i") == 0) { //** Info level w/o header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_NONE;
        } else if (strcmp(argv[i], "-it") == 0) { //** Info level w thread header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_THREAD;
        } else if (strcmp(argv[i], "-if") == 0) { //** Info level w full header
            i++;
            ifll = atoi(argv[i]);
            i++;
            if_mode = INFO_HEADER_FULL;
        } else if (strcmp(argv[i], "-ilog") == 0) { //** Override Info log file output
            i++;
            info_fname = argv[i];
            i++;
        } else if (strcmp(argv[i], "-c") == 0) { //** Load a config file from either a remote or local source
            i++;
            cfg_name = argv[i];
            i++;
        } else if (strcmp(argv[i], "-lc") == 0) { //** Default LIO config section
            i++;
            section_name = argv[i];
            i++;
        } else if (strcmp(argv[i], "--path-literals") == 0) { //** All paths are assumed to be fixed literal strings
            i++;
            path_is_literal = 1;
        } else if (strcmp(argv[i], "--print-config") == 0) { //** Print the parsed config file
            i++;
            print_config |= 1;
        } else if (strcmp(argv[i], "--print-config-with-subs") == 0) { //** Print the parsed config file
            i++;
            print_config |= 2;
        } else if (strcmp(argv[i], "--print-running-config") == 0) { //** Print the running config
            i++;
            print_config |= 4;
        } else if (strcmp(argv[i], "--monitor_fname") == 0) { //** Override the monitor file name
            i++;
            _monitoring_fname = argv[i];
            i++;
        } else if (strcmp(argv[i], "--monitor_state") == 0) { //** Override the monitor state
            i++;
            _monitoring_state = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--fcreds") == 0) { //** File to use for getting creds
            i++;
            setenv(LIO_ENV_KEY_FILE, argv[i], 1);
            i++;
        } else {
            myargv[nargs] = argv[i];
            nargs++;
            i++;
        }
    } while (i<*argc);

no_args:

    //** Make the info logging device
    if (info_fname != NULL) { //** User didn't specify anything
        if (strcmp(info_fname, "stdout") == 0) {
            fd = stdout;
        } else if (strcmp(info_fname, "stderr") == 0) {
            fd = stderr;
        } else {
            fd = tbx_io_fopen(info_fname, "w+");
        }
        if (fd != NULL) _lio_ifd = fd;
    }

    if (_lio_ifd == NULL) _lio_ifd = stdout;

    lio_ifd = tbx_info_create(_lio_ifd, if_mode, ifll);

    //** Adjust argv to reflect the parsed arguments
    *argvp = myargv;
    *argc = nargs;

    if (!cfg_name) { //** Nothing explicitly passed in so try and use a default
        if (lio_os_local_filetype("default") & OS_OBJECT_FILE_FLAG) {  //** Local remote config
            cfg_name = "file://default";
        } else if (lio_os_local_filetype("lio.cfg") & OS_OBJECT_FILE_FLAG) {  //** Local INI file
            cfg_name = "ini://lio.cfg";
        } else {
            home = getenv("HOME");
            snprintf(var, sizeof(var), "%s/.lio/default", home);
            snprintf(buffer, sizeof(buffer), "%s/.lio/lio.cfg", home);
            if (lio_os_local_filetype(var) & OS_OBJECT_FILE_FLAG) {  //* $HOME default
                snprintf(cname, sizeof(cname), "file://%s", var);
                cfg_name = cname;
            } else if (lio_os_local_filetype(var) & OS_OBJECT_FILE_FLAG) {  //* $HOME INI file
                snprintf(cname, sizeof(cname), "file://%s", buffer);
                cfg_name = cname;
            } else if (lio_os_local_filetype("/etc/lio/default") & OS_OBJECT_FILE_FLAG) {
                cfg_name = "file:///etc/lio/default";
            } else if (lio_os_local_filetype("/etc/lio/lio.cfg") & OS_OBJECT_FILE_FLAG) {
                cfg_name = "file:///etc/lio/lio.cfg";
            }
        }

        if (!cfg_name) {
            printf("Missing config file!\n");
            exit(1);
        }
    }

    if (strncasecmp(cfg_name, "file://", 7) == 0) { //** Load the default and see what we have
        fd = tbx_io_fopen(cfg_name+7, "r");
        if (!fd) {
            printf("Failed to open config file: %s  errno=%d\n", cfg_name+8, errno);
            exit(1);
        }
        if (!fgets(text, sizeof(text), fd)) {
            printf("No data in config file: %s  errno=%d\n", cfg_name, errno);
            exit(1);
        }
        tbx_io_fclose(fd);

        i = strlen(text);
        if (i == 0) {
            printf("No data in config file: %s\n", cfg_name);
            exit(1);
        }
        if (text[i-1] == '\n') text[i-1] = '\0';
        if (strncasecmp(text, "file://", 7) == 0) { //** Can't recursively load files
            printf("Config file must contain either a ini:// or lstore:// URI!\n");
            printf("Loaded from file %s\n", cfg_name);
            printf("Text: %s\n", text);
            exit(1);
        }
        cfg_name = text;
    }

    //** See what we load for default
    if ((strncasecmp(cfg_name, "ini://", 6) == 0) || (cfg_name[0] == '/')) { //** It's a local file to load
        ifd = (cfg_name[0] == '/') ? tbx_inip_file_read(cfg_name, 0) : tbx_inip_file_read(cfg_name+6, 0);
        i = 9 + 2 + 1 + 6 + 1 + 6 + 6 + 1 + sizeof(section_name) + 20;
        tbx_type_malloc(obj_name, char, i);
        dummy = NULL;
        snprintf(obj_name, i, "lstore://%s|%s:%d:%s:%s", LIO_MQ_NAME_DEFAULT, dummy, 6711, "LOCAL", section_name);
    } else {            //** Try and load a remote config
        ts = 0;
        if (rc_client_get_config(NULL, NULL, cfg_name, NULL, &config, &hints_string, &obj_name, &userid, &ts) == 0) {
            ifd = tbx_inip_string_read_with_hints_string(config, hints_string, 0);
            free(config);
            if (hints_string) free(hints_string);
        } else {
            printf("Failed loading config: %s\n", cfg_name);
            exit(1);
        }
    }

    if (ifd == NULL) {
        printf("Failed to parse INI file! config=%s\n", cfg_name);
        exit(1);
    }

    tbx_inip_hint_list_apply(ifd, hints);  //** Apply the hints
    tbx_inip_hint_list_destroy(hints);     //** and cleanup

    if (print_config & 1) {
        char *cfg_dump = tbx_inip_serialize(ifd);
        fprintf(stdout, "------------------- Dumping input LStore configuration file -------------------\n");
        fprintf(stdout, "Config string: %s   Section: %s\n", cfg_name, section_name);
        fprintf(stdout, "-------------------------------------------------------------------------------\n\n");
        fprintf(stdout, "%s", cfg_dump);
        fprintf(stdout, "-------------------------------------------------------------------------------\n\n");
        if (cfg_dump) free(cfg_dump);
    }

    tbx_inip_apply_params(ifd);

    if (print_config & 2) {
        char *cfg_dump = tbx_inip_serialize(ifd);
        fprintf(stdout, "------------------- Dumping input LStore configuration file after substitutions -------------------\n");
        fprintf(stdout, "Config string: %s   Section: %s\n", cfg_name, section_name);
        fprintf(stdout, "-------------------------------------------------------------------------------\n\n");
        fprintf(stdout, "%s", cfg_dump);
        fprintf(stdout, "-------------------------------------------------------------------------------\n\n");
        if (cfg_dump) free(cfg_dump);
    }

    tbx_mlog_load(ifd, out_override, ll_override);
    lio_gc = lio_create(ifd, section_name, userid, obj_name, name, 1);

    if (!lio_gc) {
        log_printf(-1, "Failed to create lio context.\n");
        return 1;
    }

    tbx_notify_handle = lio_gc->notify;   //** Set the global handle for notifications
    if (os_notify_handle == NULL) os_notify_handle = tbx_notify_handle;

    if (path_is_literal) lio_gc->path_is_literal = path_is_literal;  //** See if the user specified literal paths

    if (print_config & 4) {
        fprintf(stdout, "--------------------- Dumping running LStore configuration --------------------\n");
        lio_print_running_config(stdout, lio_gc);
        fprintf(stdout, "-------------------------------------------------------------------------------\n\n");
    }

    lio_gc->ref_cnt = 1;

    if (obj_name) free(obj_name);

    if (auto_mode != -1) lio_gc->auto_translate = auto_mode;

    lio_find_lfs_mounts();  //** Make the global mount prefix table

    //** Get the Work Que started
    i = tbx_inip_get_integer(lio_gc->ifd, section_name, "wq_n", 5);
    lio_wq_startup(i);

    //** Get the LUN retry size and set it up
    i = tbx_inip_get_integer(lio_gc->ifd, section_name, "lun_retry_max_size", 250);
    lun_global_state_create(i);

    //** See if we run a remote config server
    remote_config = tbx_inip_get_string(lio_gc->ifd, section_name, "remote_config", NULL);
    lio_gc->rc_section = remote_config;
    if (remote_config) {
        if (strcmp(remote_config, "standalone") != 0) {   //** Skip it if running in standalone mode
            rc_server_install(lio_gc, remote_config);
        }
    }

    //** Install the signal handler hook to get info
    dummy = tbx_inip_get_string(lio_gc->ifd, section_name, "info_fname", "/tmp/lio_info.txt");
    tbx_siginfo_install(dummy, SIGUSR1);

    //** Install the handler to release memory
    tbx_siginfo_install(NULL, SIGUSR2);
    tbx_siginfo_handler_add(SIGUSR2, memory_release_fn, NULL);
    _tcfree = (tcfree_t *)dlsym(RTLD_DEFAULT, "MallocExtension_ReleaseFreeMemory");

    log_printf(1, "INIT completed\n");

    //** Let any processes that are waiting for the initial startup to complete know so they can contine
    apr_thread_mutex_lock(lio_gc->lock);
    lio_gc->init_complete = 1;
    apr_thread_mutex_unlock(lio_gc->lock);

    return(0);
}

//***************************************************************
//  lio_shutdown - Shuts down the LIO system
//***************************************************************

int lio_shutdown()
{
    log_printf(1, "SHUTDOWN started\n");

    lio_gc->ref_cnt--;
    if(NULL != lio_gc && lio_gc->ref_cnt > 0) {
        return 0;
    }

    rc_server_destroy();
    if (lio_gc->rc_section) free(lio_gc->rc_section);

    lio_wq_shutdown();
    lun_global_state_destroy();

    lio_destroy(lio_gc);
    lio_gc = NULL;  //** Reset the global to NULL so it's not accidentally reused.

    cache_destroy(_lio_cache);
    _lio_cache = NULL;

    exnode_system_destroy();

    gop_shutdown();

    tbx_info_destroy(lio_ifd);
    lio_ifd = NULL;
    if (_lio_exe_name) free(_lio_exe_name);
    if (myargv != NULL) free(myargv);

    apr_thread_mutex_destroy(_lc_lock);
    apr_pool_destroy(_lc_mpool);
    _lc_mpool = NULL;
    _lc_lock  = NULL;

    tbx_siginfo_shutdown();

    return(0);
}

