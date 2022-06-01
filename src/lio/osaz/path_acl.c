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
// Path ACL AuthN service
//***********************************************************************

#define _log_module_index 186

#include <errno.h>
#include <stdlib.h>
#include <lio/os.h>
#include <tbx/apr_wrapper.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "authn.h"
#include "lio.h"
#include "ex3/system.h"
#include "service_manager.h"
#include "../path_acl.h"  //** This is the generic routines
#include "path_acl.h"  //** This is me

typedef struct {
    path_acl_context_t *pa;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    gop_mq_context_t *mqc;
    char *pa_file;
    char *lfs_tmp_prefix;
    char *section;
    int check_interval;
    int shutdown;
    time_t modify_time;
} osaz_pacl_t;

//*************************************************************************
// os2pacl_mode - Conversts the OS mode to Path ACL versions
//*************************************************************************

int os2pacl_mode(int os_mode)
{
    int pacl_mode;

    pacl_mode = (((OS_MODE_READ_IMMEDIATE|OS_MODE_READ_BLOCKING) & os_mode) > 0) ? PACL_MODE_READ : 0;
    if (((OS_MODE_WRITE_IMMEDIATE|OS_MODE_WRITE_BLOCKING) & os_mode) > 0) pacl_mode |= PACL_MODE_WRITE;

    return(pacl_mode);
}

//*************************************************************************
// map_line - Takes the current string position and find the start and end of the line
//*************************************************************************

void map_line(char *string_start, char *string_end, char *match, char **start, char **end)
{
    char *curr;

    //** Move backward
    curr = match;
    while (string_start != curr) {
        if (*curr == '\n') {
            *start = curr + 1;
            break;
        }
        curr--;
    }
    if (string_start == curr) *start = string_start;

    //** Now find the end
    curr = match;
    while (string_end != curr) {
        if (*curr == '\n') {
            *end = curr;
            break;
        }
        curr++;
    }
    if (string_end == curr) *end = string_end;
}

//*************************************************************************
// exnode_ro_filter - function to filter the Write and Maange caps from
//    an exnode to enforce read only access
//*************************************************************************

void osaz_pacl_exnode_ro_filter(lio_os_authz_t *osa, char *key, int mode, void *v_in, int len_in, void **v_out, int *len_out)
{
    int n, k;
    char *exnode_in = v_in;
    char *exnode_in_end = exnode_in + len_in;
    char *ex, *curr, *next, *start, *end, *match;

    if (len_in == 0) {
        *len_out = 0;
        *v_out = NULL;
        return;
    }

    tbx_type_malloc(ex, char, len_in+1);
    ex[len_in] = '\0';

    n = 0;
    curr = next = exnode_in;
    while ((match = strstr(next, "_cap")) != NULL) {
        k = match-exnode_in+1;
        if (k >= 6) {
            if (strncmp(match-6, "manage", 6) == 0) { //**Got a match
                map_line(exnode_in, exnode_in_end, match, &start, &end);
                k = start - curr;
                memcpy(ex + n, curr, k);
                n += k;
                curr = end + 1;
                next = curr;
                continue;
            }
        }
        if (k >= 5) {
            if (strncmp(match-5, "write", 5) == 0) { //**Got a match
                map_line(exnode_in, exnode_in_end, match, &start, &end);
                k = start - curr;
                memcpy(ex + n, curr, k);
                n += k;
                curr = end + 1;
                next = curr;
                continue;
            }
        }

        next = match + 1;
    }

    if (*curr != '\0') {
        k = exnode_in + len_in - 1 - curr + 1;
        memcpy(ex + n, curr, k);
        n += k;
    }

    ex[n] = '\0';
    *v_out = ex;
    *len_out = n;

    return;
}

//*************************************************************************
// osaz_pacl_ug_hint_init - Initializes a hint
//*************************************************************************

void osaz_pacl_ug_hint_init(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    osaz_pacl_t *osaz = osa->priv;

    apr_thread_mutex_lock(osaz->lock);
    pacl_ug_hint_init(osaz->pa, ug);
    apr_thread_mutex_unlock(osaz->lock);
}

//*************************************************************************
// osaz_pacl_ug_hint_free - Frees any internal structures from the hint
//*************************************************************************

void osaz_pacl_ug_hint_free(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    osaz_pacl_t *osaz = osa->priv;

    apr_thread_mutex_lock(osaz->lock);
    pacl_ug_hint_free(osaz->pa, ug);
    apr_thread_mutex_unlock(osaz->lock);
}

//*************************************************************************
// osaz_pacl_ug_hint_set - Sets a hint
//*************************************************************************

void osaz_pacl_ug_hint_set(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    osaz_pacl_t *osaz = osa->priv;

    apr_thread_mutex_lock(osaz->lock);
    pacl_ug_hint_set(osaz->pa, ug);
    apr_thread_mutex_unlock(osaz->lock);
}

//*************************************************************************
// osaz_pacl_ug_hint_set - Sets a hint
//*************************************************************************

int osaz_pacl_ug_hint_get(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    osaz_pacl_t *osaz = osa->priv;
    int n;

    apr_thread_mutex_lock(osaz->lock);
    n = pacl_ug_hint_get(osaz->pa, ug);
    apr_thread_mutex_unlock(osaz->lock);
    return(n);
}


//*************************************************************************
// osaz_pacl_print_running_config - Dumps the running config
//*************************************************************************

void osaz_pacl_print_running_config(lio_os_authz_t *osa, FILE *fd, int print_section_heading)
{
    osaz_pacl_t *osaz = osa->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", osaz->section);
    fprintf(fd, "type=%s\n", OSAZ_TYPE_PATH_ACL);
    fprintf(fd, "file=%s\n", osaz->pa_file);
    if (osaz->lfs_tmp_prefix) {
        fprintf(fd, "lfs_temp=%s  # Used when generating LFS POSIX ACLs\n", osaz->lfs_tmp_prefix);
    } else {
        fprintf(fd, "# lfs_temp=NULL  # LFS POSIX ACL mode is disabled\n");
    }
    fprintf(fd, "check_interval=%d\n", osaz->check_interval);
    fprintf(fd, "\n");

    apr_thread_mutex_lock(osaz->lock);
    pacl_print_running_config(osaz->pa, fd);
    apr_thread_mutex_unlock(osaz->lock);
}

//*************************************************************************
// osaz_pacl_can_access - checks if the ID is allowed to access the file/dir
//*************************************************************************

int osaz_pacl_can_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, int mode, int *acl)
{
    osaz_pacl_t *osaz = osa->priv;
    int can_access, pacl_mode;

    if (!path) return(0);

    pacl_mode = os2pacl_mode(mode);

log_printf(0, "HINT fname=%s ug=%p\n", path, ug);
    apr_thread_mutex_lock(osaz->lock);
    if (ug) {
//FIXME        if (geteuid() == ug->uid) {
//log_printf(0, "HINT geteuid == ug->uid == %d\n", ug->uid);
//FIXME            can_access = 2;
//FIXME        } else {
//QWERT            can_access = pacl_can_access_gid_list(osaz->pa, (char *)path, ug->n_gid, ug->gid, pacl_mode, acl);
            can_access = pacl_can_access_hint(osaz->pa, (char *)path, pacl_mode, ug, acl);
//FIXME        }
        log_printf(10, "fname=%s n_gid=%d gid[0]=%d uid=%d pacl_mode=%d mode=%d can_access=%d\n", path, ug->n_gid, ug->gid[0], ug->uid, pacl_mode, mode, can_access);
    } else {
        can_access = pacl_can_access(osaz->pa, (char *)path, (char *)an_cred_get_id(c, NULL), pacl_mode, acl);
        log_printf(10, "fname=%s pacl_mode=%d mode=%d can_access=%d\n", path, pacl_mode, mode, can_access);
    }
    apr_thread_mutex_unlock(osaz->lock);

    return(can_access);
}

//***********************************************************************

int osaz_pacl_object_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path)
{
    int acl;

    return((osaz_pacl_can_access(osa, c, ug, path, PACL_MODE_WRITE, &acl) == 2) ? 1 : 0);
}

//***********************************************************************

int osaz_pacl_object_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, int mode)
{
    int acl;
    return(osaz_pacl_can_access(osa, c, ug, path, mode, &acl));
}

//***********************************************************************

int osaz_pacl_attr_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key)
{
    int acl;
    return((osaz_pacl_can_access(osa, c, ug, path, PACL_MODE_WRITE, &acl) == 2) ? 1 : 0);
}

//***********************************************************************

int osaz_pacl_posix_acl(lio_os_authz_t *osa, lio_creds_t *c, const char *path, int lio_ftype, char *value, size_t size, uid_t *uid, gid_t *gid, mode_t *mode)
{
    osaz_pacl_t *osaz = osa->priv;
    int n, err;
    void *data;

    err = pacl_lfs_get_acl(osaz->pa, (char *)path, lio_ftype, &data, &n, uid, gid, mode);
    if (err == 0) {
        if (n <= (int)size) memcpy(value, data, n);
        err = n;
    } else {
        err = -ENODATA;
    }

    return(err);
}

//***********************************************************************

int osaz_pacl_attr_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key, int mode, osaz_attr_filter_t *filter)
{
    int acl, status, ok, pacl_mode;

    pacl_mode = os2pacl_mode(mode);

    *filter = NULL;
    ok = osaz_pacl_can_access(osa, c, ug, path, mode, &acl);
    log_printf(20, "path=%s key=%s can_access=%d mode=%d READ=%d acl=%d pacl_mode=%d\n", path, key, ok, mode, OS_MODE_READ_IMMEDIATE, acl, pacl_mode);
    status = (ok > 0) ? 2 : 0;

    if (ok) {
        if ((acl & PACL_MODE_WRITE) > 0) return(status);  //** HAve write access so nothing special

        //** If we made it here then the user only has read access so we may need to protect the exnode
        if (strcmp(key, "system.exnode") == 0) *filter = osaz_pacl_exnode_ro_filter;
    }

    return(status);
}

//***********************************************************************
// _pacl_load - Loads the Path ACLs
//***********************************************************************

void _pacl_load(lio_os_authz_t *az)
{
    osaz_pacl_t *osaz = az->priv;
    char *obj_name = NULL;
    tbx_inip_file_t *ifd;
    lio_creds_t *creds;
    int n;

    log_printf(20, "Loading config. pa_file=%s lio_gc=%p\n", osaz->pa_file, lio_gc);
    creds = (lio_gc) ? lio_gc->creds : NULL;
    if (creds) {
        log_printf(20, "creds=%s\n", an_cred_get_descriptive_id(creds, &n));
    } else {
        log_printf(20, "creds=NULL\n");
    }
    ifd = lio_fetch_config(osaz->mqc, creds, osaz->pa_file, &obj_name, &(osaz->modify_time));
    log_printf(20, "ifd=%p obj_name=%s\n", ifd, obj_name);
    if (ifd) {
        log_printf(5, "RELOADING data\n");
        tbx_monitor_thread_message(MON_MY_THREAD, "Reloading");
        if (osaz->pa) pacl_destroy(osaz->pa);
        osaz->pa = pacl_create(ifd, osaz->lfs_tmp_prefix);
        tbx_inip_destroy(ifd);
    } else {
        tbx_monitor_thread_message(MON_MY_THREAD, "No changes");
    }

    if (obj_name) free(obj_name);
}

//***********************************************************************
//  pacl_check_thread - checks for changes in the Path ACLs
//***********************************************************************

void *pacl_check_thread(apr_thread_t *th, void *data)
{
    lio_os_authz_t *az = data;
    osaz_pacl_t *osaz = az->priv;
    apr_time_t dt;

    tbx_monitor_thread_create(MON_MY_THREAD, "pacl_check_thread: Monitoring file=%s", osaz->pa_file);
    dt = apr_time_from_sec(osaz->check_interval);

    apr_thread_mutex_lock(osaz->lock);
    do {
        log_printf(5, "LOOP START check_interval=%d\n", osaz->check_interval);
        _pacl_load(az);  //** Do a quick check and see if the file has changed

        if (osaz->shutdown == 0) apr_thread_cond_timedwait(osaz->cond, osaz->lock, dt);
    } while (osaz->shutdown == 0);
    apr_thread_mutex_unlock(osaz->lock);

    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************

void osaz_pacl_destroy(lio_os_authz_t *az)
{
    osaz_pacl_t *osaz = az->priv;
    apr_status_t value;

    log_printf(5, "Shutting down\n");
    osaz->shutdown = 1;
    apr_thread_cond_broadcast(osaz->cond);
    apr_thread_mutex_unlock(osaz->lock);

    //** Wait for it to shutdown
    apr_thread_join(&value, osaz->check_thread);

    pacl_destroy(osaz->pa);

    if (osaz->lfs_tmp_prefix) free(osaz->lfs_tmp_prefix);
    if (osaz->pa_file) free(osaz->pa_file);
    free(osaz->section);
    apr_thread_mutex_destroy(osaz->lock);
    apr_thread_cond_destroy(osaz->cond);
    apr_pool_destroy(osaz->mpool);
    free(osaz);
    free(az);
}

//***********************************************************************
// osaz_path_acl_create - Create a PAth ACL based AuthZ service
//***********************************************************************

lio_os_authz_t *osaz_path_acl_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section, lio_object_service_fn_t *os)
{
    lio_os_authz_t *osaz;
    osaz_pacl_t *opa;

    log_printf(5, "START: section=%s\n", section);

    tbx_type_malloc(osaz, lio_os_authz_t, 1);
    tbx_type_malloc(opa, osaz_pacl_t, 1);
    osaz->priv = opa;
    opa->section = strdup(section);
    assert_result(apr_pool_create(&(opa->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(opa->lock), APR_THREAD_MUTEX_DEFAULT, opa->mpool);
    apr_thread_cond_create(&(opa->cond), opa->mpool);

    osaz->object_create = osaz_pacl_object_create_remove;
    osaz->object_remove = osaz_pacl_object_create_remove;
    osaz->object_access = osaz_pacl_object_access;
    osaz->attr_create = osaz_pacl_attr_create_remove;
    osaz->attr_remove = osaz_pacl_attr_create_remove;
    osaz->attr_access = osaz_pacl_attr_access;
    osaz->posix_acl = osaz_pacl_posix_acl;
    osaz->destroy = osaz_pacl_destroy;
    osaz->print_running_config = osaz_pacl_print_running_config;
    osaz->ug_hint_set = osaz_pacl_ug_hint_set;
    osaz->ug_hint_get = osaz_pacl_ug_hint_get;
    osaz->ug_hint_init = osaz_pacl_ug_hint_init;
    osaz->ug_hint_free = osaz_pacl_ug_hint_free;

    opa->pa_file = tbx_inip_get_string(ifd, section, "file", "path_acl.cfg");
    opa->lfs_tmp_prefix = tbx_inip_get_string(ifd, section, "lfs_temp", NULL);
    opa->check_interval = tbx_inip_get_integer(ifd, section, "check_interval", 60);
    opa->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ); FATAL_UNLESS(opa->mqc != NULL);

    //** Load the initial config
    opa->modify_time = 0;
    _pacl_load(osaz);

    //** Launch the check thread
    tbx_thread_create_assert(&(opa->check_thread), NULL, pacl_check_thread, (void *)osaz, opa->mpool);

    return(osaz);
}
