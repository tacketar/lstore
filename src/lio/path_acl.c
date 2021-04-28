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

//**************************************************************************
// Path ACL checking routines.  This does not do any AuthN. These routines
// should be used by higher level packages providing AuthN/Z.
//**************************************************************************

#include <fcntl.h>

#include <sys/types.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/acl.h>
#include <sys/xattr.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <tbx/append_printf.h>
#include <tbx/fmttypes.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <lio/os.h>
#include <os.h>

#include "path_acl.h"

typedef struct {  //** FUSE compliant POSIX ACL
    gid_t gid_primary;  //** Primary GID to report for LFS
    mode_t mode[3];     //** Normal perms User/Group/Other
    void *acl[3];       //** Full fledged system.posix_acl_access (0=DIR, 1=FILE, 2=EXEC-FILE)
    int  size[3];       //** And It's size
} fuse_acl_t;

typedef struct {  //account->GID mappings
    char *account;
    gid_t lfs_gid;
    int n_gid;
    gid_t *gid;
} account2gid_t;

typedef struct {
    char *account;
    int mode;
} account_acl_t;

typedef struct {    //** Individual path ACL
    char *prefix;           //** Path prefix
    account_acl_t *account; //** Array of account name/mode ACLs
    int n_account;          //** Number of accounts in the list
    int n_prefix;           //** Length of the prefix
    char *lfs_account;      //** Default account to report for FUSE
    int other_mode;         //** Access for other accounts. Defaults to NONE
    fuse_acl_t *lfs_acl;    //** Composite FUSE ACL
} path_acl_t;

struct path_acl_context_s {    //** Context for containing the Path ACL's
    path_acl_t *pacl_default;   //** Default Path ACL
    path_acl_t **path_acl;      //** List of Path ACLs
    int        n_path_acl;      //** Number of entries
    apr_hash_t *gid2acct_hash; //** Mapping from GID->account
    apr_hash_t *a2gid_hash;    //** Mapping from account->GID
    apr_hash_t *hints_hash;    //** Hash for gid->account hints
    apr_pool_t *mpool;
    apr_time_t dt_hint_cache;  //** How long to keep the hint cache before expiring
    apr_time_t timestamp;      //** Used to determine if a hint is old
    account2gid_t **a2gid;
    int n_a2gid;
    char *fname_acl;           //** Used for making the LFS ACLs if enabled
};

#define PA_MAX_ACCOUNT 100
#define PA_HINT_TIMEOUT apr_time_from_sec(60)
#define PA_UID_UNUSED   999999999

typedef struct {
    int search_hint;
    int perms;
} pacl_seed_hint_t;

typedef struct {    //** Structure used for hints
    uid_t uid;
    apr_time_t ts;
    pacl_seed_hint_t prev_search;
    int n_account;
    char *account[PA_MAX_ACCOUNT];
    gid_t gid[PA_MAX_ACCOUNT];
} pa_hint_t;

//**************************************************************************
// pacl_print_running_config - Dumps the PATh ACL running config
//**************************************************************************

void pacl_print_running_config(path_acl_context_t *pa, FILE *fd)
{
    int i, j;
    path_acl_t *acl;
    account2gid_t *a2g;

    fprintf(fd, "#----------------Path ACL start------------------\n");
    fprintf(fd, "# n_path_acl = %d\n", pa->n_path_acl);
    fprintf(fd, "# LFS acl fname template: %s\n", ((pa->fname_acl) ? pa->fname_acl : "NOT_ENABLED"));
    fprintf(fd, "\n");
    if (pa->pacl_default) {
        acl = pa->pacl_default;
        fprintf(fd, "[path_acl_default]\n");
        if (acl->other_mode > 0) {
            fprintf(fd, "other = %s\n", ((acl->other_mode == PACL_MODE_READ) ? "r" : "rw"));
        } else {
            fprintf(fd, "# other groups have NO access\n");
        }
        if (acl->lfs_account) fprintf(fd, "lfs_account=%s\n", acl->lfs_account);
        for (j=0; j<acl->n_account; j++) {
            fprintf(fd, "account(%s) = %s\n", ((acl->account[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->account[j].account);
        }
        fprintf(fd, "\n");
    }

    for (i=0; i<pa->n_path_acl; i++) {
        acl = pa->path_acl[i];
        fprintf(fd, "[path_acl]\n");
        fprintf(fd, "path = %s\n", acl->prefix);
        if (acl->other_mode > 0) {
            fprintf(fd, "other = %s\n", ((acl->other_mode == PACL_MODE_READ) ? "r" : "rw"));
        } else {
            fprintf(fd, "# other groups have NO access\n");
        }
        if (acl->lfs_account) fprintf(fd, "lfs_account = %s\n", acl->lfs_account);
        for (j=0; j<acl->n_account; j++) {
            fprintf(fd, "account(%s) = %s\n", ((acl->account[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->account[j].account);
        }
        fprintf(fd, "\n");
    }
    fprintf(fd, "#----------------Path ACL end------------------\n");
    fprintf(fd, "\n");
    fprintf(fd, "#----------------Account to GID mappings start------------------\n");
    fprintf(fd, "# n_account2gid = %d\n", pa->n_a2gid);
    fprintf(fd, "\n");
    for (i=0; i<pa->n_a2gid; i++) {
        a2g = pa->a2gid[i];
        fprintf(fd, "[path_acl_mapping]\n");
        fprintf(fd, "account = %s\n", a2g->account);
        if (a2g->lfs_gid) fprintf(fd, "lfs_gid = %u\n", a2g->lfs_gid);
        for (j=0; j<a2g->n_gid; j++) {
            fprintf(fd, "gid = %u\n", a2g->gid[j]);
        }
        fprintf(fd, "\n");
    }
    fprintf(fd, "#----------------Account to GID mappings end------------------\n");
    fprintf(fd, "\n");

}

//**************************************************************************
//  pacl_ug_hint_set - Creates a hints structure and stores it
//**************************************************************************

void pacl_ug_hint_set(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    account2gid_t *a2g;
    int n, i;
    pa_hint_t *hint;

    if (ug->uid == PA_UID_UNUSED) {
        hint = ug->hint;
        if (!hint) return;
        if (ug->creds) {
            hint->account[0] = an_cred_get_id(ug->creds, NULL);
            hint->n_account = 1;
        }
        return;
    } else {
        hint = apr_hash_get(pa->hints_hash, &(ug->uid), sizeof(gid_t));
        if (hint == NULL) {
            log_printf(10, "HINT_SET: NEW uid=%d\n", ug->uid);
            tbx_type_malloc_clear(hint, pa_hint_t, 1);
        } else {
            log_printf(10, "HINT_SET: REUSE uid=%d\n", ug->uid);
            memset(hint, 0, sizeof(pa_hint_t));
        }
    }

    ug->hint = hint;
    hint->ts = apr_time_now();
    hint->uid = ug->uid;
    ug->hint_counter = hint->ts;

    if (ug->n_gid >= PA_MAX_ACCOUNT) ug->n_gid = PA_MAX_ACCOUNT;  //** We cap the comparisions to keep from having to malloc an array

    n = 0;
    for (i=0; i<ug->n_gid; i++) {
        a2g = apr_hash_get(pa->gid2acct_hash, &(ug->gid[i]), sizeof(gid_t));
        if (a2g) {
            hint->account[n] = a2g->account;
            hint->gid[n] = ug->gid[i];
            n++;
        }
    }
    hint->n_account = n;

    if (hint->uid != PA_UID_UNUSED) apr_hash_set(pa->hints_hash, &(hint->uid), sizeof(uid_t), hint);
}

//**************************************************************************
//  pacl_ug_hint_get - Gets a hints structure and stores it in ther ug
//    If no hint avail then 1 is returned otherwize 0 is returned on success
//**************************************************************************

int pacl_ug_hint_get(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    pa_hint_t *hint;
    apr_time_t dt;


    hint = (ug->uid != PA_UID_UNUSED) ? apr_hash_get(pa->hints_hash, &(ug->uid), sizeof(gid_t)) : NULL;
    if (hint) {
        dt = apr_time_now() - hint->ts;
        if (dt < pa->dt_hint_cache) {
            ug->hint_counter = hint->ts;
            ug->hint = hint;
            ug->n_gid = hint->n_account; //** copying these allows us to make a best effort attempt to map ACLS if the hint is destroyed
            memcpy(ug->gid, hint->gid, sizeof(gid_t)*hint->n_account);
            log_printf(10, "HINT HIT! uid=%d\n", ug->uid);
            return(0);
        } else { //** Expired so destroy the hint and let the caller know
            log_printf(01, "HINT EXPIRED! uid=%d\n", ug->uid);
            apr_hash_set(pa->hints_hash, &(ug->uid), sizeof(uid_t), NULL);
            free(hint);
        }
    }

    log_printf(10, "HINT MISS! uid=%d\n", ug->uid);
    ug->hint_counter = 0;
    return(1);
}

//**************************************************************************
//  pacl_ug_hint_init - Initializes a hints structure for use
//**************************************************************************

void pacl_ug_hint_init(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    pa_hint_t *hint;

    log_printf(10, "HINT_INIT ug=%p\n", ug);

    memset(ug, 0, sizeof(lio_os_authz_local_t));

    ug->uid = PA_UID_UNUSED;
    ug->gid[0] = PA_UID_UNUSED;

    tbx_type_malloc_clear(hint, pa_hint_t, 1);
    ug->hint = hint;
    hint->ts = apr_time_now();
    hint->uid = ug->uid;
    hint->prev_search.search_hint = -2;
    ug->hint_counter = hint->ts;

}

//**************************************************************************
//  pacl_ug_hint_free - Frees the internal hints structure
//**************************************************************************

void pacl_ug_hint_free(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    log_printf(10, "HINT_FREE ug=%p\n", ug);

    if (ug->hint) {
        if (ug->uid == PA_UID_UNUSED) free(ug->hint);
    }
    ug->hint = NULL;
}

//**************************************************************************
// pacl_search - Does a binary search of the ACL prefixes and returns
//    the ACL or the default ACL if no match.
//
//    NOTE: In order for this to work the prefixes MUST be orthogonal/non-nested!
//          The prefixes are sorted in ascending order using the full prefixes
//          but here we only do a strncmp() based on the prefix and not the
//          full path provided. Working on just the n_prefix characters
//          simplifies the check
//**************************************************************************

path_acl_t *pacl_search(path_acl_context_t *pa, const char *path, int *exact, int *got_default, int *seed_hint)
{
    int low, mid, high, cmp, n;
    path_acl_t **acl = pa->path_acl;

    n = strlen(path);

    *got_default = 0;
    *exact = 0;
    low = 0; high = pa->n_path_acl-1;
    if (seed_hint) { //** Got a hint so give it a shot
log_printf(0, "HINT SEED=%d\n", *seed_hint);
        mid = (*seed_hint>=0) ? *seed_hint : high/2;
        goto fingers_crossed;
    }
    while (low <= high) {
        mid = low + (high-low)/2;
fingers_crossed:
        cmp = strncmp(acl[mid]->prefix, path, acl[mid]->n_prefix);

        //** We have a match but we need to make sure it's a full
        //** match based on a directory boundary
        if (cmp == 0) {
            if (n == acl[mid]->n_prefix) {  //** prefix and path are the same length so it's a real match
                *exact = 1;
                if (seed_hint) *seed_hint = mid;
                return(acl[mid]);
            } else if (n > acl[mid]->n_prefix) {  //** path is longer than the prefix
                if (path[acl[mid]->n_prefix] == '/') {  //** The next charatcher in the path ia a '/' so a match
                    if (seed_hint) *seed_hint = mid;
                    return(acl[mid]);
                } else {    //**No match. Just a partial dir match, ie dirs with similar names
                    cmp = -1;
                }
            } else {    //** Definitely no match since the prefix is longer than the path
                cmp = 1;
            }
        }

        if (cmp < 0) {  //** Check if we drop the lower half
            low = mid + 1;
        } else {               //** Otherwise we drop the high side
            high = mid - 1;
        }

    }

    if (strcmp(path, "/") == 0) *exact = 1;
    *got_default = 1;
    if (seed_hint) *seed_hint = -1;
    return(pa->pacl_default);
}

//**************************************************************************
//  _pacl_can_access_list - Verifies an account i nthe list can access the object
//      Returns 2 for Full access
//      Returns 1 for visible name only. Can't do anything else other than see the object
//      and 0 no access allowed
//**************************************************************************

int _pacl_can_access_list(path_acl_context_t *pa, char *path, int n_account, char **account_list, int mode, int *perms, path_acl_t **acl_mapped, pacl_seed_hint_t *ps)
{
    int i, j, check, exact, got_default, old_seed;
    path_acl_t *acl;
    char *account;

    //** Look for the prefix
    if (ps) {
        old_seed = ps->search_hint;
        acl = pacl_search(pa, path, &exact, &got_default, &(ps->search_hint));
log_printf(0, "HINT exact=%d seed -- start=%d end=%d\n", exact, old_seed, ps->search_hint);

        if (old_seed == ps->search_hint) { //** Same path ACL and accounts so use the prev hint perms
            check = mode & ps->perms;
            *perms = ps->perms;
            return((check > 0) ? 2 : exact);
        }
log_printf(0, "HINT miss so doing full check\n");
    } else {
        acl = pacl_search(pa, path, &exact, &got_default, NULL);
    }
log_printf(0, "HINT ps=NULL\n");

    if (acl_mapped) *acl_mapped = acl;
    log_printf(10, "path=%s acl=%p exact=%d\n", path, acl, exact);
    if (!acl) {  //** Not mapped to any prefix and no default
        *perms = 0;
        log_printf(10, "path=%s acl=%p exact=%d DEFAULT perm=%d = mode=%d\n", path, acl, exact, *perms, mode);
        return(0);
    }


log_printf(10, "path=%s prefix=%s acl->n_account=%d n_account=%d\n", path, acl->prefix, acl->n_account, n_account);
    //** If we made it here there's an overlapping prefix
    for (j=0; j<n_account; j++) {
        account = account_list[j];
        if (account) { //** We have a valid account to check against
            for (i=0; i<acl->n_account; i++) {
                log_printf(10, "path=%s account[%d]=%s valid_acct=%s\n", path, j, account, acl->account[i].account);
                if (strcmp(acl->account[i].account, account) == 0) {
                    check = mode & acl->account[i].mode;
                    log_printf(10, "path=%s account[%d]=%s valid_acct=%s mode=%d perms=%d check=%d\n", path, j, account, acl->account[i].account, mode, acl->account[i].mode, check);
                    if (check) {
                        *perms = acl->account[i].mode;
                        if (ps) ps->perms = *perms;
                        return(2);  //** full access so kick out
                    }
                }
            }
        }
    }

   //** We made it without a match so see if we use the default
   *perms = acl->other_mode;
   if (ps) ps->perms = *perms;
   log_printf(10, "path=%s acl=%p exact=%d DEFAULT2 perm=%d  mode=%d\n", path, acl, exact, *perms, mode);
   return(((mode & *perms) > 0) ? 2 : exact);
}

//**************************************************************************
//  pacl_can_access - Verifies the account can access the object
//      Returns 2 for Full access
//      Returns 1 for visible name only. Can't do anything else other than see the object
//      and 0 no access allowed
//**************************************************************************

int pacl_can_access(path_acl_context_t *pa, char *path, char *account, int mode, int *perms)
{
    char *account_list[1];

    account_list[0] = account;
    return(_pacl_can_access_list(pa, path, 1, account_list, mode, perms, NULL, NULL));
}

//**************************************************************************
//  pacl_can_access_gid - Verifies the gid can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int pacl_can_access_gid(path_acl_context_t *pa, char *path, gid_t gid, int mode, int *acl, char **acct)
{
    account2gid_t *a2g;

    a2g = apr_hash_get(pa->gid2acct_hash, &gid, sizeof(gid));
    if (!a2g) {
        if (acct) *acct = NULL;
        return(pacl_can_access(pa, path, NULL, mode, acl));  //** Basically only "other" ACL's are checked
    } else if (acct) {
        *acct = strdup(a2g->account);
    }
    return(pacl_can_access(pa, path, a2g->account, mode, acl));
}

//**************************************************************************
//  pacl_can_access_gid_list -Checks if one of the gid's provided can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int pacl_can_access_gid_list(path_acl_context_t *pa, char *path, int n_gid, gid_t *gid_list, int mode, int *acl)
{
    account2gid_t *a2g;
    int n, i;
    char *account_list[PA_MAX_ACCOUNT];

    if (n_gid >= PA_MAX_ACCOUNT) n_gid = PA_MAX_ACCOUNT;  //** We cap the comparisions to keep from having to malloc an array

    n = 0;
    for (i=0; i<n_gid; i++) {
        a2g = apr_hash_get(pa->gid2acct_hash, &gid_list[i], sizeof(gid_t));
        if (a2g) {
            account_list[n] = a2g->account;
            n++;
        }
    }

    log_printf(10, "path=%s n_gid=%d n_account=%d\n", path, n_gid, n);
    if (n == 0) {
        return(pacl_can_access(pa, path, NULL, mode, acl));  //** Basically only "other" ACL's are checked
    }

    i = _pacl_can_access_list(pa, path, n, account_list, mode, acl, NULL, NULL);
    return(i);
}


//**************************************************************************
//  pacl_can_access_hint -Checks if the accounts in the hints provided can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int pacl_can_access_hint(path_acl_context_t *pa, char *path, int mode, lio_os_authz_local_t *ug, int *acl)
{
    pa_hint_t *hint;

    if (ug->hint_counter < pa->timestamp) { //** It's an old hint so do the fallback using GIDs stored i nteh hint
        return(pacl_can_access_gid_list(pa, path, ug->n_gid, ug->gid, mode, acl));
    }

    hint = ug->hint;
    return(_pacl_can_access_list(pa, path, hint->n_account, hint->account, mode, acl, NULL, &(hint->prev_search)));
}

//**************************************************************************
// pacl_lfs_get_acl - Returns the LFS ACL
//**************************************************************************

int pacl_lfs_get_acl(path_acl_context_t *pa, char *path, int lio_ftype, void **lfs_acl, int *acl_size, uid_t *uid, gid_t *gid, mode_t *mode)
{
    path_acl_t *acl;
    int exact, slot, got_default;

    acl = pacl_search(pa, path, &exact, &got_default, NULL);
    log_printf(10, "path=%s exact=%d acl=%p\n", path, exact, acl);
    if (acl) {
        log_printf(10, "path=%s exact=%d lfs_acl=%p\n", path, exact, acl->lfs_acl);
        if (acl->lfs_acl) {
            if (exact) {
                slot = 0;
            } else {
                if (lio_ftype & OS_OBJECT_DIR_FLAG) {
                    slot = 0;
                } else {
                    slot = (lio_ftype & OS_OBJECT_EXEC_FLAG) ? 2 : 1;
                }
            }
            *lfs_acl = acl->lfs_acl->acl[slot];
            *acl_size = acl->lfs_acl->size[slot];
            *mode = acl->lfs_acl->mode[slot];
            *gid = acl->lfs_acl->gid_primary;
            //** Still need to map the file type over
            if (lio_ftype & OS_OBJECT_SYMLINK_FLAG) {
                *mode |= S_IFLNK;
            } else if (lio_ftype & OS_OBJECT_DIR_FLAG) {
                *mode |= S_IFDIR;
            } else {
                *mode |= S_IFREG;
                if (lio_ftype & OS_OBJECT_EXEC_FLAG) { //** Executable
                    if (*mode & S_IRUSR) *mode |= S_IXUSR;
                    if (*mode & S_IRGRP) *mode |= S_IXGRP;
                    if (*mode & S_IROTH) *mode |= S_IXOTH;
                }
            }
            return(0);
        }

    }

    return(1);

}

//**************************************************************************
// pacl_sort_fn - Compare function for qsort
//**************************************************************************

int pacl_sort_fn(const void *a1, const void *a2, void *arg)
{
    const path_acl_t *acl1 = *(const path_acl_t **)a1;
    const path_acl_t *acl2 = *(const path_acl_t **)a2;

    return(strcmp(acl1->prefix, acl2->prefix));
}

//**************************************************************************
// pacl2lfs_gid_primary - Get the primary GID to use for the account
//**************************************************************************

gid_t pacl2lfs_gid_primary(path_acl_context_t *pa, char *account)
{
    account2gid_t *a2g;

    a2g = apr_hash_get(pa->a2gid_hash, account, APR_HASH_KEY_STRING);
    if (a2g) {
        return(a2g->lfs_gid);
    }

    return(0);
}

//**************************************************************************
// _make_lfs_acl - Makes an LFS compatible ACL
//**************************************************************************

int _make_lfs_acl(int fd, char *acl_text, void **kacl, int *kacl_size)
{
    acl_t acl;
    char acl_buf[10*1024];

    log_printf(10, "acl_text=%s\n", acl_text);

    //** Convert from a string to an ACL
    acl = acl_from_text(acl_text);
    if (acl == (acl_t)NULL) {
        log_printf(0, "acl_from_text ERROR: acl_text=%s\n", acl_text);
        return(1);
    }

    //** Apply it to the file
    if (acl_set_fd(fd, acl) != 0) {
        log_printf(0, "acl_set_file ERROR: acl_text=%s acl_set_fd=%d\n", acl_text, errno);
        acl_free(acl);
        return(2);
    }

    acl_free(acl);  //** Destroy the ACL

    //** Now read back the raw attribute
    *kacl_size = fgetxattr(fd, "system.posix_acl_access", acl_buf, sizeof(acl_buf));
    if (*kacl_size <= 0) {
        log_printf(0, "acl_getxattr ERROR: acl_text=%s getxattr=%d\n", acl_text, errno);
        return(3);
    }

    if (acl == (acl_t)NULL) {
        log_printf(0, "ERROR: acl_copy_int acl_text=%s\n", acl_text);
        return(1);
    }

    tbx_type_malloc_clear(*kacl, char, *kacl_size+1);
    memcpy(*kacl, acl_buf, *kacl_size);
    return(0);
}

//**************************************************************************
// pacl2lfs_acl - Converts the given path acl to a usable extended ACL for
//   use with FUSE.  The returned object can be returned as system.posic_acl_access
//**************************************************************************

fuse_acl_t *pacl2lfs_acl(path_acl_context_t *pa, path_acl_t *acl, int fdf, int fdd)
{
    fuse_acl_t *facl;
    int nbytes = 1024*1024;
    char dir_acl_text[nbytes];
    char file_acl_text[nbytes];
    char exec_acl_text[nbytes];
    int dir_pos, file_pos, exec_pos, i, j;
    account2gid_t *a2g;
    account_acl_t *aa;
    gid_t gid;

    //** Prep things
    dir_pos = file_pos = exec_pos = 0;
    nbytes--;;
    memset(dir_acl_text, 0, sizeof(dir_acl_text));
    memset(file_acl_text, 0, sizeof(file_acl_text));
    tbx_type_malloc_clear(facl, fuse_acl_t, 1);

    //** First get the LFS primary group
    facl->gid_primary = pacl2lfs_gid_primary(pa, acl->lfs_account);

    log_printf(10, "gid_primary=%u lfs_account=%s n_account=%d\n", facl->gid_primary, acl->lfs_account, acl->n_account);
    if (acl->other_mode == 0) {
        facl->mode[0] = S_IRWXU | S_IROTH;
        facl->mode[1] = S_IRUSR | S_IWUSR;
        facl->mode[2] = S_IRWXU;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "u::rwx,o::r--,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "u::rw-,o::---,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "u::rwx,o::---,m::rwx");
    } else if (acl->other_mode & PACL_MODE_WRITE) {
        facl->mode[0] = S_IRWXU | S_IRWXO;
        facl->mode[1] = S_IRUSR | S_IWUSR | S_IROTH | S_IWOTH;
        facl->mode[2] = S_IRWXU | S_IRWXO;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "u::rwx,o::rwx,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "u::rw-,o::rw-,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "u::rwx,o::rwx,m::rwx");
    } else {
        facl->mode[0] = S_IRWXU | S_IROTH | S_IXOTH;
        facl->mode[1] = S_IRUSR | S_IWUSR | S_IROTH;
        facl->mode[2] = S_IRWXU | S_IROTH | S_IXOTH;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "u::rwx,o::r-x,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "u::rw-,o::r--,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "u::rwx,o::r-x,m::rwx");
    }

    //** Find the primary account and just add it's primary GID
    for (i=0; i<acl->n_account; i++) {
        aa = &(acl->account[i]);
        log_printf(10, "i=%d account=%s mode=%d\n", i, aa->account, aa->mode); tbx_log_flush();
        if (strcmp(aa->account, acl->lfs_account) == 0) {
            a2g = apr_hash_get(pa->a2gid_hash, aa->account, APR_HASH_KEY_STRING);
            if (!a2g) break;  //** Can't find it so kick out and use the default READ only

            //** We have to find the primary GID first and add it
            for (j=0; j<a2g->n_gid; j++) {
                gid = a2g->gid[j];
                if (gid == facl->gid_primary) {  //** It's the primary GID so also add the other default perms
                    if (aa->mode & PACL_MODE_WRITE) {
                        facl->mode[0] |= S_IRWXG;
                        facl->mode[1] |= S_IRGRP | S_IWGRP;
                        facl->mode[2] |= S_IRWXG;
                        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g::rwx");
                        tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g::rw-");
                        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g::rwx");
                    } else {
                        facl->mode[0] |= S_IRGRP | S_IXGRP;
                        facl->mode[1] |= S_IRGRP;
                        facl->mode[2] |= S_IRGRP | S_IXGRP;
                        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g::r-x");
                        tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g::r--");
                        tbx_append_printf(file_acl_text, &exec_pos, nbytes, ",g::r-x");
                    }

                    break;
                }
            }
        }
    }

    if (dir_pos == 0) { //** No default found so assume the worst and just give read access
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "u::r-x,g::r-x,o::---,m::r-x");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "u::r--,g::r--,o::---,m::r--");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "u::r-x,g::r-x,o::---,m::r-x");
    }

    //** Now cycle through all the accounts allowed to access and add them
    for (i=0; i<acl->n_account; i++) {
        aa = &(acl->account[i]);
        log_printf(10, "i=%d account=%s mode=%d\n", i, aa->account, aa->mode); tbx_log_flush();

        a2g = apr_hash_get(pa->a2gid_hash, aa->account, APR_HASH_KEY_STRING);
        if (!a2g) continue;

        for (j=0; j<a2g->n_gid; j++) {
            gid = a2g->gid[j];
            log_printf(10, "i=%d gid=%u mode=%d\n", i, gid, aa->mode);
            if (gid != facl->gid_primary) {  //** Skip over the primary since it's already done
                if (aa->mode & PACL_MODE_WRITE) {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%u:rwx", gid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%u:rw-", gid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%u:rwx", gid);
                } else {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%u:r-x", gid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%u:r--", gid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%u:r-x", gid);
                }

            }
        }
    }

    log_printf(10, "DIRACL=%s\n", dir_acl_text);
    log_printf(10, "DIRMODE=%o\n", facl->mode[0]);

    //** Convert it to an ACL
    _make_lfs_acl(fdd, dir_acl_text, &(facl->acl[0]), &(facl->size[0]));
    _make_lfs_acl(fdf, file_acl_text, &(facl->acl[1]), &(facl->size[1]));
    _make_lfs_acl(fdf, exec_acl_text, &(facl->acl[2]), &(facl->size[2]));

    return(facl);
}

//**************************************************************************
// pacl_lfs_acls_generate - Generates the LFS ACLS for use with FUSE
//**************************************************************************

void pacl_lfs_acls_generate(path_acl_context_t *pa)
{
    int i, fdf, fdd;
    DIR *dir;
    char *fname, *dname;

    //** Make the temp file for generating the ACLs on
    fname = strdup(pa->fname_acl);
    fdf = mkstemp(fname);
    dname = strdup(pa->fname_acl);
    if (mkdtemp(dname) == NULL) {
        log_printf(0, "ERROR: failed maing temp ACL directory: %s\n", dname);
    }
    dir = opendir(dname);
    fdd = dirfd(dir);

    log_printf(10, "Generating default ACL\n"); tbx_log_flush();
    //** 1st set the default FUSE ACL
    pa->pacl_default->lfs_acl = pacl2lfs_acl(pa, pa->pacl_default, fdf, fdd);

    //** And now all the Path's
    for (i=0; i<pa->n_path_acl; i++) {
        log_printf(10, "Generating acl for prefix[%d]=%s\n", i, pa->path_acl[i]->prefix);
        pa->path_acl[i]->lfs_acl = pacl2lfs_acl(pa, pa->path_acl[i], fdf, fdd);
    }

    //** Cleanup
    free(fname);
    close(fdf);
    closedir(dir);
    rmdir(dname);
    free(dname);
}

//**************************************************************************
// prefix_account_parse - Parse the INI file and populates the
//     prefix/account associations.
//     NOTE: The INI file if from the LIO context and persists for
//           dureation of the program so no need to dup strings
//**************************************************************************

void prefix_account_parse(path_acl_context_t *pa, tbx_inip_file_t *fd)
{
    path_acl_t *acl;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *value, *prefix, *other_mode, *lfs;
    tbx_stack_t *stack, *acl_stack;
    int i, def;

    stack = tbx_stack_new();
    acl_stack = tbx_stack_new();

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        if ((strcmp("path_acl", tbx_inip_group_get(ig)) == 0) ||
            (strcmp("path_acl_default", tbx_inip_group_get(ig)) == 0)) { //** Got a prefix
            ele = tbx_inip_ele_first(ig);
            other_mode = NULL;
            prefix = NULL;
            def = 0;
            if (strcmp("path_acl_default", tbx_inip_group_get(ig)) == 0) {
                prefix = "DEFAULT";
                def = 1;
            }
            lfs = NULL;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "prefix") == 0) {  //** Got the path prefix
                    prefix = value;
                } else if (strncmp(key, "account", 7) == 0) { //** Got an account
                    tbx_stack_push(stack, key);
                    tbx_stack_push(stack, value);
                } else if (strcmp(key, "other") == 0) {  //** Only used for defaults
                    other_mode = value;
                } else if (strcmp(key, "lfs_account") == 0) {  //** Only used with LFS
                    lfs = value;
                }

                ele = tbx_inip_ele_next(ele);
            }

            if ((prefix) && (tbx_stack_count(stack)>0)) { //** Got a valid entry
                tbx_type_malloc_clear(acl, path_acl_t, 1);
                acl->n_account = tbx_stack_count(stack)/2;
                tbx_type_malloc_clear(acl->account, account_acl_t, acl->n_account);
                acl->prefix = strdup(prefix);
                acl->n_prefix = strlen(prefix);
                for (i=0; i<acl->n_account; i++) {
                    acl->account[i].account = strdup(tbx_stack_pop(stack));
                    acl->account[i].mode = (strcmp(tbx_stack_pop(stack), "account(r)") == 0) ? PACL_MODE_READ : PACL_MODE_RW;
                }
                if (other_mode) acl->other_mode = (strcmp(other_mode, "rw") == 0) ? PACL_MODE_RW : PACL_MODE_READ;
                if (def == 1) {
                    pa->pacl_default = acl;
                } else {
                    tbx_stack_push(acl_stack, acl);
                }
                if (lfs) {
                    acl->lfs_account = strdup(lfs);
                } else if (acl->n_account > 0) {
                    acl->lfs_account = strdup(acl->account[acl->n_account-1].account);
                }
            } else {
                log_printf(0, "ERROR: Missing fields! prefix=%s stack_count(accounts)=%d\n", prefix, tbx_stack_count(stack));
                tbx_stack_empty(stack, 0);
            }
        }

        ig = tbx_inip_group_next(ig);
    }

    //** Convert the ACL stack to an array and sort it.
    pa->n_path_acl = tbx_stack_count(acl_stack);
    tbx_type_malloc_clear(pa->path_acl, path_acl_t *, pa->n_path_acl);
    for (i=0; i<pa->n_path_acl; i++) {
        pa->path_acl[i] = tbx_stack_pop(acl_stack);
    }
    qsort_r(pa->path_acl, pa->n_path_acl, sizeof(path_acl_t *), pacl_sort_fn, NULL);

    //** Clean up
    tbx_stack_free(stack, 0);
    tbx_stack_free(acl_stack, 0);
}

//**************************************************************************
// gid2account_parse - Make the GID -> accoutn mappings
//     NOTE: The INI file if from the LIO context and persists for
//           dureation of the program so no need to dup strings
//**************************************************************************

void gid2account_parse(path_acl_context_t *pa, tbx_inip_file_t *fd)
{
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *value, *account, *lfs;
    tbx_stack_t *stack, *a2g_stack;
    account2gid_t *a2g;
    int i;

    stack = tbx_stack_new();
    a2g_stack = tbx_stack_new();

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        if (strcmp("path_acl_mapping", tbx_inip_group_get(ig)) == 0) { //** Got a prefix
            ele = tbx_inip_ele_first(ig);
            account = NULL;
            lfs = NULL;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "account") == 0) {  //** Got the account
                    account = value;
                } else if (strcmp(key, "gid") == 0) { //** Got a GID
                    tbx_stack_move_to_bottom(stack);
                    tbx_stack_insert_below(stack, value);
                } else if (strcmp(key, "lfs_gid") == 0) { //** Got the default GID for LFS
                    lfs = value;
                }

                ele = tbx_inip_ele_next(ele);
            }

            if ((account) && (tbx_stack_count(stack)>0)) {
                tbx_type_malloc_clear(a2g, account2gid_t, 1);
                a2g->account = strdup(account);

                if (!lfs) { //** No specific LFS GID is provided so use the first one provided
                    tbx_stack_move_to_top(stack);
                    lfs = tbx_stack_get_current_data(stack);
                }
                if (lfs) {
                    a2g->lfs_gid = tbx_stk_string_get_integer(lfs);
                }

                a2g->n_gid = tbx_stack_count(stack);
                tbx_type_malloc_clear(a2g->gid, gid_t, a2g->n_gid);
                i = 0;
                while ((value = tbx_stack_pop(stack)) != NULL) {
                    a2g->gid[i] = tbx_stk_string_get_integer(value);
                    apr_hash_set(pa->gid2acct_hash, a2g->gid + i, sizeof(gid_t), a2g);
                    i++;
                }
                tbx_stack_push(a2g_stack, a2g);
            } else {
                log_printf(0, "ERROR: Missing fields! account=%s stack_count(gid)=%d\n", account, tbx_stack_count(stack));
                tbx_stack_empty(stack, 0);
            }
        }

        ig = tbx_inip_group_next(ig);
    }

    pa->n_a2gid = tbx_stack_count(a2g_stack);
    tbx_type_malloc_clear(pa->a2gid, account2gid_t *, pa->n_a2gid);
    for (i=0; i<pa->n_a2gid; i++) {
        pa->a2gid[i] = tbx_stack_pop(a2g_stack);
        apr_hash_set(pa->a2gid_hash, pa->a2gid[i]->account, APR_HASH_KEY_STRING, pa->a2gid[i]);
    }
    tbx_stack_free(a2g_stack, 0);
    tbx_stack_free(stack, 0);
}

//**************************************************************************
// pacl_create - Creates a Path ACL structure
//    This parse all [path_acl] and [path_acl_mapping] sections in the INI file
//    to create the structure.  The format for each section type are defined below.
//    NOTE: The prefixes must be non-overlapping and not nested!
//          The GID->account mapping must be unique!
//
//    [path_acl_default]
//    default=r|rw
//    account=<account>
//
//    [path_acl]
//    path=<prefix>
//    lfs_account=<account> #default account reported by FUSE
//    account(r)=<account_1>
//    ...
//    account(rw)=<account_N>
//
//    [path_acl_mapping]
//    account=<account>
//    lfs_gid=<gid_1>  #This is used for the FUSE group ownership
//    ...
//    gid=<gid_N>
//**************************************************************************

path_acl_context_t *pacl_create(tbx_inip_file_t *fd, char *fname_lfs_acls)
{
    path_acl_context_t *pa;
    char fname[4096];

    log_printf(10, "Loading PACL's\n");

    //** Create the structure
    tbx_type_malloc_clear(pa, path_acl_context_t, 1);
    assert_result(apr_pool_create(&(pa->mpool), NULL), APR_SUCCESS);
    pa->gid2acct_hash = apr_hash_make(pa->mpool);
    pa->a2gid_hash = apr_hash_make(pa->mpool);
    pa->hints_hash = apr_hash_make(pa->mpool);
    pa->dt_hint_cache = PA_HINT_TIMEOUT;

    //**Now populate it
    prefix_account_parse(pa, fd);  //** Add the prefix/account associations
    gid2account_parse(pa, fd);     //** And the GID->Account mappings, if any exist

    fname[sizeof(fname)-1] = 0;
    if (fname_lfs_acls) {
        snprintf(fname, sizeof(fname)-1, "%s.XXXXXX", fname_lfs_acls);
        pa->fname_acl = strdup(fname);
        pacl_lfs_acls_generate(pa);
    }

    pa->timestamp = apr_time_now();

    return(pa);
}

//**************************************************************************
// prefix_destroy
//**************************************************************************

void prefix_destroy(path_acl_t *acl)
{
    int i;

    log_printf(10, "prefix=%s lfs_acl=%p\n", acl->prefix, acl->lfs_acl);
    if (acl->lfs_acl) {
        for (i=0; i<3; i++) {
            if (acl->lfs_acl->acl[i]) free(acl->lfs_acl->acl[i]);
        }
        free(acl->lfs_acl);
    }

    if (acl->prefix) free(acl->prefix);
    for (i=0; i<acl->n_account; i++) {
        free(acl->account[i].account);
    }
    if (acl->lfs_account) free(acl->lfs_account);
    free(acl->account);
    free(acl);
}

//**************************************************************************
// pacl_destroy - Destroy the Path ACL structure
//**************************************************************************

void pacl_destroy(path_acl_context_t *pa)
{
    account2gid_t *a2g;
    int i;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    pa_hint_t *hint;
    
    log_printf(10, "n_path_acl=%d\n", pa->n_path_acl);
    if (pa->pacl_default) prefix_destroy(pa->pacl_default);

    //** Tear down the Path ACL list strcture
    for (i=0; i<pa->n_path_acl; i++) {
        prefix_destroy(pa->path_acl[i]);
    }
    free(pa->path_acl);

    //** And the LFS account2gid mappings
    if (pa->fname_acl) free(pa->fname_acl);
    for (i=0; i<pa->n_a2gid; i++) {
        a2g = pa->a2gid[i];
        free(a2g->account);
        free(a2g->gid);
        free(a2g);
    }
    free(pa->a2gid);

    //** Also have to free all the hints
    for (hi=apr_hash_first(NULL, pa->hints_hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&hint);
        free(hint);  //** The interior strings are just pointers to fields in a2g above
    }
    
    //** This also destroys the hash
    apr_pool_destroy(pa->mpool);

    //** and container
    free(pa);
}
