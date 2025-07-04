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
// Dummy/Fake AuthN service.  Always returns success!
//***********************************************************************

#define _log_module_index 186

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include <lio/os.h>

#include "fake.h"
#include "authn.h"
#include "os.h"
#include "service_manager.h"


//*************************************************************************

void osaz_fake_ug_hint_init(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    memset(ug, 0, sizeof(lio_os_authz_local_t));
    return;
}

//*************************************************************************

void osaz_fake_ug_hint_free(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

//*************************************************************************

void osaz_fake_ug_hint_release(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

//*************************************************************************

void osaz_fake_ug_hint_set(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    return;
}

//*************************************************************************

int osaz_fake_ug_hint_get(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug)
{
    ug->hint_counter = 0;
    ug->hint = NULL;
    return(0);
}

//*************************************************************************

char *osaz_fake_perms_attr(lio_os_authz_t *osa, const char *fname)
{
    return(NULL);
}

//*************************************************************************

int osaz_fake_perms_encode(lio_os_authz_t *osa, const char *fname, int ftype, char *val, int v_size, uid_t uid, gid_t gid, mode_t mode)
{
    return(0);
}

//*************************************************************************

int osaz_fake_perms_decode(lio_os_authz_t *osa, const char *fname, int ftype, char *val, int v_size, uid_t *uid, gid_t *gid, mode_t *mode)
{
    return(-1);
}

//***********************************************************************

void osaz_fake_print_running_config(lio_os_authz_t *osa, FILE *fd, int print_section_heading)
{
    if (print_section_heading) fprintf(fd, "[%s]\n", (char *)osa->priv);
    fprintf(fd, "type=%s\n", OSAZ_TYPE_FAKE);
}

//***********************************************************************

int osaz_fake_object_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path)
{
    return(1);
}

//***********************************************************************

int osaz_fake_object_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, int mode)
{
    return(2);
}

//***********************************************************************

int osaz_fake_attr_create_remove(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key)
{
    return(1);
}

//***********************************************************************

int osaz_fake_get_acl(lio_os_authz_t *osa, lio_creds_t *c, char **attr, int *attr_size, const char *path, int lio_ftype, char *val, size_t size, uid_t *uid, gid_t *gid, mode_t *mode, int get_nfs4, char **use_instead)
{
    if (lio_ftype & OS_OBJECT_SYMLINK_FLAG) {
        *mode = S_IFLNK | 0770;
    } else if (lio_ftype & OS_OBJECT_DIR_FLAG) {
        *mode = S_IFDIR | 0770;
    } else {
        *mode = S_IFREG | 0660;  //** Make it so that everything has RW access
        if (lio_ftype & OS_OBJECT_EXEC_FLAG) *mode |= S_IXUSR | S_IXGRP;
    }

    return(-ENODATA);
}

//***********************************************************************

void osaz_fake_ns_acl_add(lio_os_authz_t *osa, int n_start, int *n_array, char **attr)
{
    return;
}

//***********************************************************************

int osaz_fake_attr_access(lio_os_authz_t *osa, lio_creds_t *c, lio_os_authz_local_t *ug, const char *path, const char *key, int mode, osaz_attr_filter_t *filter)
{
    *filter = NULL;
    return(2);
}

//***********************************************************************

void osaz_fake_destroy(lio_os_authz_t *osa)
{
    if (osa->priv) free(osa->priv);
    free(osa);
}


//***********************************************************************
// osaz_fake_create - Create a Fake AuthN service
//***********************************************************************

lio_os_authz_t *osaz_fake_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section, lio_object_service_fn_t *os)
{
    lio_os_authz_t *osaz;

    tbx_type_malloc(osaz, lio_os_authz_t, 1);

    osaz->priv = (section) ? strdup(section) : NULL;
    osaz->print_running_config = osaz_fake_print_running_config;
    osaz->object_create = osaz_fake_object_create_remove;
    osaz->object_remove = osaz_fake_object_create_remove;
    osaz->object_access = osaz_fake_object_access;
    osaz->attr_create = osaz_fake_attr_create_remove;
    osaz->attr_remove = osaz_fake_attr_create_remove;
    osaz->attr_access = osaz_fake_attr_access;
    osaz->get_acl = osaz_fake_get_acl;
    osaz->destroy = osaz_fake_destroy;
    osaz->perms_attr = osaz_fake_perms_attr;
    osaz->perms_encode = osaz_fake_perms_encode;
    osaz->perms_decode = osaz_fake_perms_decode;
    osaz->ug_hint_set = osaz_fake_ug_hint_set;
    osaz->ug_hint_get = osaz_fake_ug_hint_get;
    osaz->ug_hint_init = osaz_fake_ug_hint_init;
    osaz->ug_hint_free = osaz_fake_ug_hint_free;
    osaz->ug_hint_release = osaz_fake_ug_hint_release;

    return(osaz);
}
