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

#define PACL_MODE_READ  1
#define PACL_MODE_WRITE 2
#define PACL_MODE_RW    3

typedef struct path_acl_context_s path_acl_context_t;

void pacl_print_running_config(path_acl_context_t *pa, FILE *fd);
char *pacl_gid2account(path_acl_context_t *pa, gid_t gid);
int pacl_lfs_get_acl(path_acl_context_t *pa, char *path, int lio_ftype, void **lfs_acl, int *acl_size, uid_t *uid, gid_t *gid, mode_t *mode);
int pacl_can_access(path_acl_context_t *pa, char *path, char *account, int mode, int *perms);
int pacl_can_access_gid(path_acl_context_t *pa, char *path, gid_t gid, int mode, int *perms, char **acct);
int pacl_can_access_gid_list(path_acl_context_t *pa, char *path, int n_gid, gid_t *gid_list, int mode, int *acl);
void pacl_ug_hint_set(path_acl_context_t *pa, lio_os_authz_local_t *ug);
int pacl_ug_hint_get(path_acl_context_t *pa, lio_os_authz_local_t *ug);
path_acl_context_t *pacl_create(tbx_inip_file_t *fd, char *fname_lfs_acls);
void pacl_destroy(path_acl_context_t *pa);

#ifdef __cplusplus
}
#endif
#endif