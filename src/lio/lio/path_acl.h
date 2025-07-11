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


// *************************************************************
//  Path ACL structures
// *************************************************************

#ifndef __PATH_ACL_H_
#define __PATH_ACL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <tbx/iniparse.h>
#include <lio/core.h>
#include <lio/visibility.h>

#define PACL_MODE_READ  1
#define PACL_MODE_WRITE 2
#define PACL_MODE_RW    3

//** Bits controlling if the prefix supports overriding the PREFIX externally with OS flags
#define PACL_MODE_PERMS   1    //** Can override UID/GID/mode
#define PACL_MODE_POSIX   2    //** Can override system.posix_acl_access
#define PACL_MODE_NOPOSIX 4    //** Never rerurn the constructed system.posix_acl_acces
#define PACL_MODE_NFS4    8    //** Can override system.nfs4_acl
#define PACL_MODE_NONFS4 16    //** Never return the constructed system.nfs4_acl

typedef struct path_acl_context_s path_acl_context_t;

LIO_API void pacl_print_running_config(path_acl_context_t *pa, FILE *fd);
LIO_API int pacl_lfs_get_acl(path_acl_context_t *pa, char *path, int lio_ftype, void **lfs_acl, int *acl_size, uid_t *uid, gid_t *gid, mode_t *mode, int get_nfs4, int *override_mode);
LIO_API int pacl_can_access_account(path_acl_context_t *pa, char *path, char *account, int mode, int *perms);
LIO_API void pacl_ug_hint_init(path_acl_context_t *pa, lio_os_authz_local_t *ug);
LIO_API void pacl_ug_hint_free(path_acl_context_t *pa, lio_os_authz_local_t *ug);
LIO_API void pacl_ug_hint_release(path_acl_context_t *pa, lio_os_authz_local_t *ug);
LIO_API void pacl_ug_hint_set(path_acl_context_t *pa, lio_os_authz_local_t *ug);
LIO_API int pacl_ug_hint_get(path_acl_context_t *pa, lio_os_authz_local_t *ug);
LIO_API int pacl_can_access_hint(path_acl_context_t *pa, char *path, int mode, lio_os_authz_local_t *ug, int *acl);
LIO_API path_acl_context_t *pacl_create(tbx_inip_file_t *fd, char *fname_lfs_acls, int nfs4_enable);
LIO_API void pacl_destroy(path_acl_context_t *pa);
LIO_API int pacl_path_probe(path_acl_context_t *pa, const char *prefix, int do_acl_tree, FILE *fd, int seed);
LIO_API int pacl_print_tree(path_acl_context_t *pa, const char *prefix, FILE *fd);
LIO_API uint64_t pacl_unused_guid_get();
LIO_API void pacl_unused_guid_set(uint64_t guid);
LIO_API int pacl_override_settings(path_acl_context_t *pa);

#ifdef __cplusplus
}
#endif
#endif