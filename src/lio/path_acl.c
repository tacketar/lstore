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
#include <grp.h>
#include <pwd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/acl.h>
#include <sys/mount.h>
#include <sys/types.h>
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

#include <lio/path_acl.h>

//** ACL slots
#define ACL_POSIX_DIR  0
#define ACL_POSIX_FILE 1
#define ACL_POSIX_EXEC 2
#define ACL_NFS4_DIR   3
#define ACL_NFS4_FILE  4
#define ACL_NFS4_EXEC  5
#define ACL_MAX        6
#define ACL_NFS4_START ACL_NFS4_DIR

#define IS_SET(n, flag) (((n & flag) > 0) ? 1 : 0)

char *_acl_name[] = { "# POSIX_DIR_ACL", "# POSIX_FILE_ACL", "# POSIX_EXEC_ACL", "nfs4_dir", "nfs4_file", "nfs4_exec" };

typedef struct {  //** FUSE compliant POSIX and NFS4 ACLs
    gid_t gid_primary;     //** Primary GID to report for LFS
    uid_t uid_primary;     //** Primary UID to report for LFS
    mode_t mode[ACL_MAX];  //** Normal perms User/Group/Other
    void *acl[ACL_MAX];    //** Full fledged system.posix_acl_access (0=DIR, 1=FILE, 2=EXEC-FILE)
    int  size[ACL_MAX];    //** And It's size
} fuse_acl_t;

typedef struct {    //** FS ACL for use by the FS layer and sits on top of the LStore account type ACLs
    union {
        uid_t uid;
        gid_t gid;
     };
    int mode;
    char *name;
} fs_acl_t;

typedef struct {   //** List of FS ACLs
    int n;
    fs_acl_t id[];
} fs_acl_list_t;

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
    char *lfs_account;      //** Primary account to report for FS/FUSE
    uid_t lfs_uid;          //** Primary user for FUSE
    uid_t lfs_gid;          //** Primary group for FUSE
    fs_acl_list_t *uid_map;
    fs_acl_list_t *gid_map;
    int other_mode;         //** Access for other accounts. Defaults to NONE
    fuse_acl_t *lfs_acl;    //** Composite FUSE ACL
    char *acl_text[ACL_MAX];  //** Text version of the ACLs
    int override_mode;      //** Prefex passthru mode bits
    int nested_end;         //** Tracks nesting of ACL prefixes
    int nested_primary;     //** Initial prefix of nested group
    int nested_parent;      //** PArent of current prefix if nested
    int rlut;               //** Reverse LUT
    int gid_account_start;  //** Index representing the GID was added from the account mappings
} path_acl_t;

struct path_acl_context_s {    //** Context for containing the Path ACL's
    path_acl_t *pacl_default;  //** Default Path ACL
    path_acl_t **path_acl;     //** List of Path ACLs
    int        n_path_acl;     //** Number of entries
    int        inuse_pending;  //** Tracks lingering usage before final destruction
    apr_hash_t *gid2acct_hash; //** Mapping from GID->account
    apr_hash_t *a2gid_hash;    //** Mapping from account->GID
    apr_hash_t *hints_hash;    //** Hash for gid->account hints
    apr_pool_t *mpool;
    apr_time_t dt_hint_cache;  //** How long to keep the hint cache before expiring
    apr_time_t timestamp;      //** Used to determine if a hint is old
    int n_lut;                 //** Number of unique non-overlapping prefixes
    int *lut;                  //** Lookup table for unique prefixes
    account2gid_t **a2gid;
    int n_a2gid;
    char *fname_acl;           //** Used for making the LFS POSIX ACLs if enabled
    int nfs4_enable;           //** Enable creation of NFS4 ACLs if provided
};

#define PA_MAX_ACCOUNT 100
#define PA_HINT_TIMEOUT apr_time_from_sec(60)

uint64_t _pa_guid_unused = 999999999;   //** This is the value used to signify a uid/gid has not been set.

typedef struct {
    int search_hint;
    int perms;
} pacl_seed_hint_t;

typedef struct {    //** Structure used for hints
    uid_t uid;
    apr_time_t ts;
    int inuse;
    int to_release;
    path_acl_context_t *pa;               //** This is the parent PA in case of a lingering handle after destruction
    pacl_seed_hint_t prev_search;
    int gid_last_match;
    int gid_acl_last_match;
    int uid_acl_last_match;
    int last_match_was_uid;
    int n_account;
    int n_gid;
    gid_t gid[PA_MAX_ACCOUNT];
    char *account[PA_MAX_ACCOUNT];
} pa_hint_t;

void facl_destroy(fuse_acl_t *facl, char **acl_text);
int _group2gid(const char *group, gid_t *gid);
char *pacl_gid2account(path_acl_context_t *pa, gid_t gid);

//**************************************************************************
// These are helper routines to set/get the unused UID/GID value
//**************************************************************************

uint64_t pacl_unused_guid_get() { return(_pa_guid_unused); }
void pacl_unused_guid_set(uint64_t guid) { _pa_guid_unused = guid; }

//**************************************************************************
// pacl_print_running_config - Dumps the PATh ACL running config
//**************************************************************************

void pacl_print_running_config(path_acl_context_t *pa, FILE *fd)
{
    int i, j, n;
    path_acl_t *acl;
    account2gid_t *a2g;
    char *from_acct;

    if (pa == NULL) {
        fprintf(fd, "#----------------Path ACL start------------------\n");
        fprintf(fd, "# ERROR pa = NULL!\n");
        fprintf(fd, "#----------------Path ACL end------------------\n\n");
        return;
    }

    n = (pa->nfs4_enable) ? ACL_MAX : 3;

    fprintf(fd, "#----------------Path ACL start------------------\n");
    fprintf(fd, "# n_path_acl = %d   nfs4_enabled = %d\n", pa->n_path_acl, pa->nfs4_enable);
    fprintf(fd, "# LFS POSIX acl fname template: %s\n", ((pa->fname_acl) ? pa->fname_acl : "NOT_ENABLED"));
    fprintf(fd, "\n");
    if (pa->pacl_default) {
        acl = pa->pacl_default;
        fprintf(fd, "[path_acl_default]\n");
        fprintf(fd, "perms_override=%d\n", IS_SET(acl->override_mode, PACL_MODE_PERMS));
        fprintf(fd, "posix_override=%d\n", IS_SET(acl->override_mode, PACL_MODE_POSIX));
        fprintf(fd, "noposix_override=%d\n", IS_SET(acl->override_mode, PACL_MODE_NOPOSIX));
        fprintf(fd, "nfs4_override=%d\n", IS_SET(acl->override_mode, PACL_MODE_NFS4));
        fprintf(fd, "nonfs4_override=%d\n", IS_SET(acl->override_mode, PACL_MODE_NONFS4));
        if (acl->other_mode > 0) {
            fprintf(fd, "other = %s\n", ((acl->other_mode == PACL_MODE_READ) ? "r" : "rw"));
        } else {
            fprintf(fd, "# other groups have NO access\n");
        }
        if (acl->lfs_account) fprintf(fd, "lfs_account = %s\n", acl->lfs_account);
        for (j=0; j<acl->n_account; j++) {
            fprintf(fd, "account(%s) = %s\n", ((acl->account[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->account[j].account);
        }
        for (j=0; j<n; j++) {
            fprintf(fd, "%s = %s\n", _acl_name[j], acl->acl_text[j]);
        }
        fprintf(fd, "\n");
    }

    for (i=0; i<pa->n_path_acl; i++) {
        acl = pa->path_acl[i];
        fprintf(fd, "[path_acl]\n");
        fprintf(fd, "path = %s\n", acl->prefix);
        fprintf(fd, "perms_override = %d\n", IS_SET(acl->override_mode, PACL_MODE_PERMS));
        fprintf(fd, "posix_override = %d\n", IS_SET(acl->override_mode, PACL_MODE_POSIX));
        fprintf(fd, "noposix_override = %d\n", IS_SET(acl->override_mode, PACL_MODE_NOPOSIX));
        fprintf(fd, "nfs4_override = %d\n", IS_SET(acl->override_mode, PACL_MODE_NFS4));
        fprintf(fd, "nonfs4_override = %d\n", IS_SET(acl->override_mode, PACL_MODE_NONFS4));
        if (acl->other_mode > 0) {
            fprintf(fd, "other = %s\n", ((acl->other_mode == PACL_MODE_READ) ? "r" : "rw"));
        } else {
            fprintf(fd, "# other groups have NO access\n");
        }
        if (acl->lfs_account) fprintf(fd, "lfs_account = %s\n", acl->lfs_account);
        for (j=0; j<acl->n_account; j++) {
            fprintf(fd, "account(%s) = %s\n", ((acl->account[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->account[j].account);
        }
        if (acl->lfs_uid != _pa_guid_unused) fprintf(fd, "lfs_uid = %u\n", acl->lfs_uid);
        if (acl->uid_map) {
            for (j=0; j<acl->uid_map->n; j++) {
                if (acl->uid_map->id[j].name) {
                    fprintf(fd, "uid(%s) = %u # %s\n", ((acl->uid_map->id[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->uid_map->id[j].uid, acl->uid_map->id[j].name);
                } else {
                    fprintf(fd, "uid(%s) = %u\n", ((acl->uid_map->id[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->uid_map->id[j].uid);
                }
            }
        }
        if (acl->lfs_gid != _pa_guid_unused) fprintf(fd, "lfs_gid = %u\n", acl->lfs_gid);
        if (acl->gid_map) {
            for (j=0; j<acl->gid_map->n; j++) {
                from_acct = (j>=acl->gid_account_start) ? "# - from account mapping" : "";
                if (acl->gid_map->id[j].name) {
                    fprintf(fd, "gid(%s) = %u # %s %s\n", ((acl->gid_map->id[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->gid_map->id[j].uid, acl->gid_map->id[j].name, from_acct);
                } else {
                    fprintf(fd, "gid(%s) = %u %s\n", ((acl->gid_map->id[j].mode == PACL_MODE_READ) ? "r" : "rw"), acl->gid_map->id[j].gid, from_acct);
                }
            }
        }
        for (j=0; j<n; j++) {
            fprintf(fd, "%s = %s\n", _acl_name[j], acl->acl_text[j]);
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
//  NOTE: Should be protected by a lock
//**************************************************************************

void pacl_ug_hint_set(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    pa_hint_t *hint;

    if (ug->uid == _pa_guid_unused) {
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
            if (hint->inuse) {
                log_printf(10, "HINT_SET: INUSE uid=%d\n", ug->uid);
                hint->to_release = 1;
                apr_hash_set(pa->hints_hash, &(hint->uid), sizeof(uid_t), NULL); //** Clear it from the hash. The current holder will free it on release
                tbx_type_malloc_clear(hint, pa_hint_t, 1);  //** Now make a new hint to use
            } else {
                log_printf(10, "HINT_SET: REUSE uid=%d\n", ug->uid);
                memset(hint, 0, sizeof(pa_hint_t));
            }
        }
    }

    ug->hint = hint;
    hint->ts = apr_time_now();
    hint->inuse = 1;
    hint->uid = ug->uid;
    hint->prev_search.search_hint = -2;
    ug->hint_counter = hint->ts;

    if (ug->n_gid >= PA_MAX_ACCOUNT) ug->n_gid = PA_MAX_ACCOUNT;  //** We cap the comparisions to keep from having to malloc an array

    hint->n_gid = ug->n_gid;
    memcpy(hint->gid, ug->gid, sizeof(gid_t)*ug->n_gid);

    if (hint->uid != _pa_guid_unused) apr_hash_set(pa->hints_hash, &(hint->uid), sizeof(uid_t), hint);
}

//**************************************************************************
//  pacl_ug_hint_get - Gets a hints structure and stores it in ther ug
//    If no hint avail then 1 is returned otherwize 0 is returned on success
//  NOTE: Should be protected by a lock
//**************************************************************************

int pacl_ug_hint_get(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    pa_hint_t *hint;
    apr_time_t dt;

    hint = (ug->uid != _pa_guid_unused) ? apr_hash_get(pa->hints_hash, &(ug->uid), sizeof(gid_t)) : NULL;
    if (hint) {
        dt = apr_time_now() - hint->ts;
        if (dt < pa->dt_hint_cache) {
            ug->hint_counter = hint->ts;
            ug->hint = hint;
            hint->inuse++;
            log_printf(10, "HINT HIT! uid=%d\n", ug->uid);
            return(0);
        } else { //** Expired so destroy the hint and let the caller know
            log_printf(10, "HINT EXPIRED! uid=%d\n", ug->uid);
            apr_hash_set(pa->hints_hash, &(ug->uid), sizeof(uid_t), NULL);
            if (hint->inuse) {
                hint->to_release = 1;
            } else {
                free(hint);
            }
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

    ug->uid = _pa_guid_unused;
    ug->gid[0] = _pa_guid_unused;

    tbx_type_malloc_clear(hint, pa_hint_t, 1);
    ug->hint = hint;
    hint->ts = apr_time_now();
    hint->uid = ug->uid;
    hint->prev_search.search_hint = -2;
    ug->hint_counter = hint->ts;
}

//**************************************************************************
//  pacl_ug_hint_free - Frees the internal hints structure
//  NOTE: Should be protected by a lock
//**************************************************************************

void pacl_ug_hint_free(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    log_printf(10, "HINT_FREE ug=%p\n", ug);

    if (ug->hint) {
        if (ug->uid == _pa_guid_unused) free(ug->hint);
    }
    ug->hint = NULL;
}

//**************************************************************************
//  pacl_ug_hint_release - Releases the internal hints structure
//  NOTE: Should be protected by a lock
//**************************************************************************

void pacl_ug_hint_release(path_acl_context_t *pa, lio_os_authz_local_t *ug)
{
    pa_hint_t *hint;
    log_printf(10, "HINT_RELEASE ug=%p\n", ug);

    if (ug->hint) {
        hint = ug->hint;
        hint->inuse--;
        if ((hint->to_release) && (hint->inuse == 0)) {
            if (hint->pa) {  //** The PA was destroyed so see if we are the last handle
                hint->pa->inuse_pending--;
                if (hint->pa->inuse_pending == 0) {
                    pacl_destroy(hint->pa);
                }
            }
            free(ug->hint);
        }
    }
    ug->hint = NULL;
}

//**************************************************************************
// pacl_search_base - Does a binary search of the ACL prefixes and returns
//    the ACL or the default ACL if no match.
//
//    NOTE: This routine will return a partial match so further checks may be required.
//          The prefixes are sorted in ascending order using the full prefixes
//          but here we only do a strncmp() based on the prefix and not the
//          full path provided. Working on just the n_prefix characters
//          simplifies the check
//**************************************************************************

path_acl_t *pacl_search_base(path_acl_context_t *pa, const char *path, int *exact, int *got_default, int *seed_hint, int n)
{
    int low, mid, high, cmp, mlut;
    path_acl_t **acl = pa->path_acl;

    // n = strlen(path); <-- passed in

    *got_default = 0;
    *exact = 0;
    low = 0; high = pa->n_lut-1;
    if (seed_hint) { //** Got a hint so give it a shot
        mid = (*seed_hint>=0) ? acl[*seed_hint]->nested_primary : high/2;
        mid = acl[mid]->rlut;
        goto fingers_crossed;
    }
    while (low <= high) {
        mid = (high+low)/2;
fingers_crossed:
        mlut = pa->lut[mid];
        cmp = strncmp(acl[mlut]->prefix, path, acl[mlut]->n_prefix);

        //** We have a match but we need to make sure it's a full
        //** match based on a directory boundary
        if (cmp == 0) {
            if (n == acl[mlut]->n_prefix) {  //** prefix and path are the same length so it's a real match
                *exact = 1;
                if (seed_hint) *seed_hint = mlut;
                return(acl[mlut]);
            } else {  //** path is longer than the prefix
                if (path[acl[mlut]->n_prefix] == '/') {  //** The next charatcher in the path ia a '/' so a match
                    if (seed_hint) *seed_hint = mlut;
                    return(acl[mlut]);
                } else {    //**No match. Just a partial dir match, ie dirs with similar names
                    cmp = -1;
                }
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
// pacl_search - Does a binary search of the ACL prefixes and returns
//    the ACL or the default ACL if no match.
//
//**************************************************************************

path_acl_t *pacl_search(path_acl_context_t *pa, const char *path, int *exact, int *got_default, int *seed_hint)
{
    int index, i, cmp, n;
    path_acl_t *myacl, **acl;

    n = strlen(path);
    index = (seed_hint) ? ((*seed_hint > pa->n_path_acl) ? -1 : *seed_hint) : -1;
    myacl = pacl_search_base(pa, path, exact, got_default, &index, n);

    //** Kick out if an exact match or got the default
    if (*got_default == 1) {
        if (seed_hint) *seed_hint = -1;
        return(myacl);
    } else if (*exact == 1) {
        if (seed_hint) *seed_hint = index;
        return(myacl);
    }

    acl = pa->path_acl;

    //** If the original hint maps to the same primary lets check it directly
    if ((seed_hint) && (*seed_hint >= 0) && (*seed_hint < pa->n_path_acl)) {
        if (acl[*seed_hint]->nested_primary == myacl->nested_primary) { //** See if the seed maps to the same acl returned from the base
            cmp = strncmp(acl[*seed_hint]->prefix, path, acl[*seed_hint]->n_prefix);
            if (cmp == 0) {   //** Got a potential match
                if (n == acl[*seed_hint]->n_prefix) {  //** prefix and path are the same length so it's a real match
                    *exact = 1;
                    return(acl[*seed_hint]);
                } else if (path[acl[*seed_hint]->n_prefix] == '/') {  //** The next charatcher in the path ia a '/' so a match and the path is longer than the prefix
                    index = *seed_hint;
                }
            }
        }
    }


    //** Got a partial match so scan for nested matches
    for (i=index+1; i<=acl[index]->nested_end; i++) {
        cmp = strncmp(acl[i]->prefix, path, acl[i]->n_prefix);
        if (cmp == 0) {   //** Got a potential match
            if (n == acl[i]->n_prefix) {  //** prefix and path are the same length so it's a real match
                *exact = 1;
                index = i;
                break;
            } else if (path[acl[i]->n_prefix] == '/') {  //** The next charatcher in the path ia a '/' so a match and the path is longer than the prefix
                index = i;
            }
        } else if (cmp>0) { //** Moved past it so kick out
            break;
        }
    }

    if (seed_hint) *seed_hint = index;
    return(acl[index]);
}

//**************************************************************************
//  _pacl_can_access_account_list - Verifies an account in the list can access the object
//      Returns 2 for Full access
//      Returns 1 for visible name only. Can't do anything else other than see the object
//      and 0 no access allowed
//**************************************************************************

int _pacl_can_access_account_list(path_acl_context_t *pa, char *path, int n_account, char **account_list, int mode, int *perms, path_acl_t **acl_mapped, pacl_seed_hint_t *ps)
{
    int i, j, check, exact, got_default, old_seed;
    path_acl_t *acl;
    char *account;

    //** Look for the prefix
    if (ps) {
        old_seed = ps->search_hint;
        acl = pacl_search(pa, path, &exact, &got_default, &(ps->search_hint));
        log_printf(10, "HINT exact=%d seed -- start=%d end=%d mode=%d perms=%d\n", exact, old_seed, ps->search_hint, mode, ps->perms);
        if (old_seed == ps->search_hint) { //** Same path ACL and accounts so use the prev hint perms
            check = mode & ps->perms;
            *perms = ps->perms;
            return((check == mode) ? 2 : exact);
        }
        log_printf(10, "HINT miss so doing full check\n");
    } else {
        log_printf(10, "HINT ps=NULL\n");
        acl = pacl_search(pa, path, &exact, &got_default, NULL);
    }

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
                    if (check == mode) {
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
   return(((mode & *perms) == mode) ? 2 : exact);
}

//**************************************************************************
//  pacl_can_access_account - Verifies the account can access the object
//      Returns 2 for Full access
//      Returns 1 for visible name only. Can't do anything else other than see the object
//      and 0 no access allowed
//**************************************************************************

int pacl_can_access_account(path_acl_context_t *pa, char *path, char *account, int mode, int *perms)
{
    char *account_list[1];

    account_list[0] = account;
    return(_pacl_can_access_account_list(pa, path, 1, account_list, mode, perms, NULL, NULL));
}

//**************************************************************************
//  pacl_can_access_acl_uid - Checks if the UID provided can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int _pacl_can_access_acl_uid(path_acl_context_t *ctx, path_acl_t *pa, int *acl_last_index, uid_t uid, int mode, int *acl)
{
    int m, j;
    int check;
    fs_acl_t *id;

    if (!pa->uid_map) return(0);

    id = pa->uid_map->id;

    for (j=0; j<pa->uid_map->n; j++) {
        m = (j + (*acl_last_index)) % pa->uid_map->n;
        if (id[m].uid == uid) {
            check = mode & id[m].mode;
            if (check == mode) {
                *acl = id[m].mode;
                *acl_last_index = m;
                return(2);   //Full access so kick out
            }
        }
    }

    //** No match so check the defaults
    *acl = pa->other_mode;
    check = mode & (*acl);
    return((check == mode) ? 2 : 0);
}

//**************************************************************************
//  pacl_can_access_acl_gid_list - Checks if one of the gid's provided can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int _pacl_can_access_acl_gid_list(path_acl_context_t *ctx, path_acl_t *pa, int *gid_last_index, int *acl_last_index, int n_gid, gid_t *gid_list, int mode, int *acl)
{
    int n, m, i, j;
    int check;
    fs_acl_t *id;
    gid_t cgid;

    if (!pa->gid_map) return(0);

    if (n_gid >= PA_MAX_ACCOUNT) n_gid = PA_MAX_ACCOUNT;  //** We cap the comparisions to keep from having to malloc an array

    id = pa->gid_map->id;

    for (i=0; i<n_gid; i++) {
        n = (i + (*gid_last_index)) % n_gid;
        cgid = gid_list[n];
        for (j=0; j<pa->gid_map->n; j++) {
            m = (j + (*acl_last_index)) % pa->gid_map->n;
            if (id[m].gid == cgid) {
                check = mode & id[m].mode;
                if (check == mode) {
                    *acl = id[m].mode;
                    *gid_last_index = n;
                    *acl_last_index = m;
                    return(2);   //Full access so kick out
                }
            }
        }
    }

    //** No match so check the defaults
    *acl = pa->other_mode;
    check = mode & (*acl);
    return((check == mode) ? 2 : 0);
}

//**************************************************************************
//  pacl_can_access_hint -Checks if the accounts in the hints provided can access the object
//      Returns 2,1 for success and 0 otherwise
//**************************************************************************

int pacl_can_access_hint(path_acl_context_t *ctx, char *path, int mode, lio_os_authz_local_t *ug, int *acl)
{
    path_acl_t *pa;
    pa_hint_t *hint;
    int exact, got_default, old_seed;
    int check;

    hint = ug->hint;
    if (ug->valid_guids == 0) {
        hint->prev_search.search_hint = -2;
        return(_pacl_can_access_account_list(ctx, path, hint->n_account, hint->account, mode, acl, NULL, &(hint->prev_search)));
    }

    if (ug->hint_counter > ctx->timestamp) {   //** hint is good so check if we can use it
        old_seed = hint->prev_search.search_hint;
        pa = pacl_search(ctx, path, &exact, &got_default, &(hint->prev_search.search_hint));
        if (old_seed == hint->prev_search.search_hint) { //** Same Path acl as before
            check = mode & hint->prev_search.perms;
            *acl = hint->prev_search.perms;
            return((check == mode) ? 2 : exact);
        }
    } else { //** Old hint so reset the seed;
        hint->prev_search.search_hint = -2;
        pa = pacl_search(ctx, path, &exact, &got_default, &(hint->prev_search.search_hint));
    }

    //** Check the UID 1st
    check = _pacl_can_access_acl_uid(ctx, pa, &(hint->uid_acl_last_match), ug->uid, mode, acl);
    if (check) {
        hint->prev_search.perms = *acl;
        return(check);
    }

    //** No match so check the GIDs. It also returns the default perms if they work
    check = _pacl_can_access_acl_gid_list(ctx, pa, &(hint->gid_last_match),  &(hint->gid_acl_last_match), hint->n_gid, hint->gid, mode, acl);
    hint->prev_search.perms = *acl;
    return(check);
}

//**************************************************************************
// pacl_lfs_get_acl - Returns the LFS ACL
//**************************************************************************

int pacl_lfs_get_acl(path_acl_context_t *pa, char *path, int lio_ftype, void **lfs_acl, int *acl_size, uid_t *uid, gid_t *gid, mode_t *mode, int get_nfs4, int *override_mode)
{
    path_acl_t *acl;
    int exact, slot, got_default;
    mode_t filebits;

    filebits = (mode) ? *mode & S_IFMT : 0;

    acl = pacl_search(pa, path, &exact, &got_default, NULL);
    if (override_mode) *override_mode = acl->override_mode;
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

            //** These always come from the POSIX
            if (mode) *mode = acl->lfs_acl->mode[slot];
            if (gid) *gid = acl->lfs_acl->gid_primary;
            if (acl->lfs_acl->uid_primary != _pa_guid_unused) {
                if (uid) *uid = acl->lfs_acl->uid_primary;
            }

            //** Tweak things for NFS4
            if (get_nfs4) slot = slot + ACL_NFS4_START;
            if (lfs_acl) *lfs_acl = acl->lfs_acl->acl[slot];
            if (acl_size) *acl_size = acl->lfs_acl->size[slot];

            //** Still need to map the file type over
            if (mode) {
                *mode |= filebits;
                if ((lio_ftype & OS_OBJECT_FILE_FLAG) && (lio_ftype & OS_OBJECT_EXEC_FLAG)) { //** Executable
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
//  _find_binary - Finds the given binary
//**************************************************************************

char *_find_binary(char *exec, int force_root_uid)
{
    int n_paths = 5;
    char *search_path[] = { NULL, "/bin", "/sbin", "/usr/bin", "/usr/sbin" };
    char *prefix;
    char fname[OS_PATH_MAX];
    struct stat sbuf;
    int i;

    //** 1st check for LIO_NFS4_PATH setting
    search_path[0] = getenv("LIO_NFS4_PATH");

    for (i=0; i<n_paths; i++) {
        prefix = search_path[i];
        if (prefix) {
            snprintf(fname, sizeof(fname)-1, "%s/%s", prefix, exec);
            fname[OS_PATH_MAX-1] = '\0';
            if (stat(fname, &sbuf) == 0) {
                if (force_root_uid) {
                    if (sbuf.st_uid == 0) {   //** Make sure it's owned by root
                        return(strdup(fname));
                    } else {
                        log_printf(0, "ERROR: %s should be owned by root! Ignoring...\n", fname);
                        fprintf(stderr, "ERROR: %s should be owned by root! Ignoring...\n", fname);
                    }
                } else {
                    return(strdup(fname));
                }

            }
        }
    }

    return(NULL);
}

//** These are set on he initial call and assumed static for the dureation of the program
char *_nfs4_setfacl = NULL;
char *_nfs4_getfacl = NULL;

//**************************************************************************
// _find_nfs4_binaries - Finds the NFS4 nfs4_setfacl and nfs4_getfacl binaries
//**************************************************************************

int _find_nfs4_binaries()
{
    if (_nfs4_setfacl == NULL) _nfs4_setfacl = _find_binary("nfs4_setfacl", 1);
    if (_nfs4_getfacl == NULL) _nfs4_getfacl = _find_binary("nfs4_getfacl", 1);

    if ((_nfs4_setfacl == NULL) || (_nfs4_getfacl == NULL)) {
        log_printf(0, "ERROR: _nfs4_setfacl=%s _nfs4_getfacl=%s\n", _nfs4_setfacl, _nfs4_getfacl);
        log_printf(0, "ERROR: Try setting the LIO_NFS4_PATH envirnment variable to locate the executabales!\n");
        fprintf(stderr, "ERROR: _nfs4_setfacl=%s _nfs4_getfacl=%s\n", _nfs4_setfacl, _nfs4_getfacl);
        fprintf(stderr, "ERROR: Try setting the LIO_NFS4_PATH envirnment variable to locate the executabales!\n");

        return(1);
    }

    return(0);
}

//**************************************************************************
// _set_nfs4_acl - Shells out and sets the NFS4 ACL on the object
//**************************************************************************

int _set_nfs4_acl(const char *dfname, char *acl_text)
{
    int err;
    char cmd[OS_PATH_MAX];

    snprintf(cmd, sizeof(cmd)-1, "%s -s %s %s", _nfs4_setfacl, acl_text, dfname);

    err = system(cmd);
    if (err != 0) {
        err = errno;
        fprintf(stderr, "ERROR: _set_nfs4_acl errno=%d -- CMD=%s\n", err, cmd);
        log_printf(-1, "ERROR: _set_nfs4_acl errno=%d -- CMD=%s\n", err, cmd);
    }

    return(err);
}

//**************************************************************************
// _get_nfs4_acl - Shells out and gets the NFS4 ACL on the object
//**************************************************************************

int _get_nfs4_acl(const char *dfname, void **kacl, int *kacl_size)
{
    size_t nbytes;
    char *buf = NULL;

    //** Get the size
    nbytes = 0;
    *kacl_size = getxattr(dfname, "system.nfs4_acl", buf, nbytes);
    if (*kacl_size < 0) {
        *kacl_size = 0;
        *kacl = NULL;
        return(1);
    } else if (*kacl_size == 0) {
        *kacl = NULL;
        return(0);
    }

    //** Now store it
    tbx_type_malloc_clear(*kacl, void, *kacl_size);
    nbytes = *kacl_size;
    getxattr(dfname, "system.nfs4_acl", *kacl, nbytes);

    return(0);
}

//**************************************************************************
// _make_nfs4_acl - Makes an LFS compatible NFSv4 ACL
//**************************************************************************

int _make_nfs4_acl(const char *dfname, char *acl_text, void **kacl, int *kacl_size, char *prefix, char *atype)
{

    log_printf(10, "acl_text=%s\n", acl_text);

    //** Kick out if no suitable binaries
    if (_find_nfs4_binaries() != 0) return(1);

    //** Set the ACL
    if (_set_nfs4_acl(dfname, acl_text) != 0) return(2);

    //** Read it back
    if (_get_nfs4_acl(dfname, kacl, kacl_size) != 0) return(3);

    return(0);
}

//**************************************************************************
// _make_posix_acl - Makes an LFS compatible POSIX ACL
//**************************************************************************

int _make_posix_acl(int fd, char *acl_text, void **kacl, int *kacl_size, char *prefix, char *atype)
{
    acl_t acl;
    char acl_buf[10*1024];

    log_printf(10, "acl_text=%s\n", acl_text);

    //** Convert from a string to an ACL
    acl = acl_from_text(acl_text);
    if (acl == (acl_t)NULL) {
        log_printf(0, "acl_from_text ERROR: prefix=%s type=%s acl_text=%s errno=%d\n", prefix, atype, acl_text, errno);
        return(1);
    }

    //** Apply it to the file
    if (acl_set_fd(fd, acl) != 0) {
        log_printf(0, "acl_set_file ERROR: prefix=%s type=%s acl_text=%s acl_set_fd=%d\n", prefix, atype, acl_text, errno);
        acl_free(acl);
        return(2);
    }

    acl_free(acl);  //** Destroy the ACL

    //** Now read back the raw attribute
    *kacl_size = fgetxattr(fd, "system.posix_acl_access", acl_buf, sizeof(acl_buf));
    if (*kacl_size <= 0) {
        log_printf(0, "acl_getxattr ERROR: prefix=%s type=%s acl_text=%s getxattr=%d\n", prefix, atype, acl_text, errno);
        return(3);
    }

    if (acl == (acl_t)NULL) {
        log_printf(0, "ERROR: acl_copy_int prefix=%s type=%s acl_text=%s\n", prefix, atype, acl_text);
        return(1);
    }

    tbx_type_malloc_clear(*kacl, char, *kacl_size+1);
    memcpy(*kacl, acl_buf, *kacl_size);

    return(0);
}

//**************************************************************************
// _mount_nfs4 - Mounts min_fise at the provided path to set/get the NFS4 ACLs
//**************************************************************************

int _mount_nfs4(const char *prefix)
{
    char *min_fuse;
    char cmd[OS_PATH_MAX];
    int err;

    min_fuse = _find_binary("min_fuse", 0);
    if (min_fuse == NULL) {
        log_printf(-1, "ERROR: Unable to find min_fuse binary!\n");
        fprintf(stderr, "ERROR: Unable to find min_fuse binary!\n");
        return(1);
    }

    snprintf(cmd, sizeof(cmd)-1, "MIN_IGNORE_XATTR_REPLACE='1' %s -o fsname=min %s", min_fuse, prefix);
    err = system(cmd);
    free(min_fuse);

    if (err != 0) {
        err = errno;
        fprintf(stderr, "ERROR: _mount_nfs4 errno=%d -- CMD=%s\n", err, cmd);
        log_printf(-1, "ERROR: _mount_nfs4 errno=%d -- CMD=%s\n", err, cmd);
    }

    return(err);
}


//**************************************************************************
// _umount_nfs4 - Unmounts the min_fuse
//**************************************************************************

void _umount_nfs4(const char *prefix)
{
    int err;

    err = umount(prefix);
    if (err != 0) {
        err = errno;
        log_printf(-1, "ERROR: Unable to unmount min_fuse from %s errno=%d!\n", prefix, err);
        fprintf(stderr, "ERROR: Unable to unmount min_fuse from %s errno=%d!\n", prefix, err);
    }

    return;
}

//**************************************************************************
// pacl2lfs_acl - Converts the given path acl to a usable extended ACL for
//   use with FUSE.  The returned object can be returned as system.posic_acl_access
//**************************************************************************

fuse_acl_t *pacl2lfs_acl(path_acl_context_t *pa, path_acl_t *acl, int fdf, int fdd, const char *fdf_name, const char *fdd_name, char *nfs4_fname)
{
    fuse_acl_t *facl;
    int nbytes = 1024*1024;
    char dir_acl_text[nbytes];
    char file_acl_text[nbytes];
    char exec_acl_text[nbytes];
    char fname[OS_PATH_MAX];
    char *name;
    int dir_pos, file_pos, exec_pos, i, primary_added;
    gid_t gid;
    uid_t uid;
    int mode, err;

    //** Prep things
    dir_pos = file_pos = exec_pos = 0;
    nbytes--;;
    memset(dir_acl_text, 0, sizeof(dir_acl_text));
    memset(file_acl_text, 0, sizeof(file_acl_text));
    tbx_type_malloc_clear(facl, fuse_acl_t, 1);

    //** First get the LFS primary group
    facl->gid_primary = acl->lfs_gid;
    facl->uid_primary = acl->lfs_uid;

    if (acl->other_mode == 0) {
        facl->mode[ACL_POSIX_DIR] = 0;
        facl->mode[ACL_POSIX_FILE] = 0;
        facl->mode[ACL_POSIX_EXEC] = 0;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "o::---,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "o::---,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "o::---,m::rwx");
    } else if (acl->other_mode & PACL_MODE_WRITE) {   //** If you have write you also have read
        facl->mode[ACL_POSIX_DIR] = S_IRWXO;
        facl->mode[ACL_POSIX_FILE] = S_IROTH | S_IWOTH;
        facl->mode[ACL_POSIX_EXEC] = S_IRWXO;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "o::rwx,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "o::rw-,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "o::rwx,m::rwx");
    } else {  //** Read only access
        facl->mode[ACL_POSIX_DIR] = S_IROTH | S_IXOTH;
        facl->mode[ACL_POSIX_DIR] = S_IROTH | S_IXOTH;
        facl->mode[ACL_POSIX_FILE] = S_IROTH;
        facl->mode[ACL_POSIX_EXEC] = S_IROTH | S_IXOTH;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "o::r-x,m::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "o::r--,m::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "o::r-x,m::rwx");
    }

    primary_added = 0;
    if (acl->gid_map) {
        for (i=0; i<acl->gid_map->n; i++) {
            name = acl->gid_map->id[i].name;
            gid = acl->gid_map->id[i].gid;
            mode = acl->gid_map->id[i].mode;

            if (gid == facl->gid_primary) {  //** Primary GID
                primary_added = 1;
                if (mode & PACL_MODE_WRITE) {
                    facl->mode[ACL_POSIX_DIR] |= S_IRWXG;
                    facl->mode[ACL_POSIX_FILE] |= S_IRGRP | S_IWGRP;
                    facl->mode[ACL_POSIX_EXEC] |= S_IRWXG;
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g::rwx");
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g::rw-");
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g::rwx");
                } else {
                    facl->mode[ACL_POSIX_DIR] |= S_IRGRP | S_IXGRP;
                    facl->mode[ACL_POSIX_FILE] |= S_IRGRP;
                    facl->mode[ACL_POSIX_EXEC] |= S_IRGRP | S_IXGRP;
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g::r-x");
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g::r--");
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g::r-x");
                }
            } else if (mode & PACL_MODE_WRITE) {
                if (name == NULL) {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%u:rwx", gid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%u:rw-", gid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%u:rwx", gid);
                } else {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%s:rwx", name);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%s:rw-", name);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%s:rwx", name);
                }
            } else {
                if (name == NULL) {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%u:r-x", gid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%u:r--", gid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%u:r-x", gid);
                } else {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g:%s:r-x", name);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g:%s:r--", name);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g:%s:r-x", name);
                }
            }
        }
    }

    if (primary_added == 0) { //** No primary GID so manually add one. Assumes full perms.
        facl->mode[ACL_POSIX_DIR] |= S_IRWXG;
        facl->mode[ACL_POSIX_FILE] |= S_IRGRP | S_IWGRP;
        facl->mode[ACL_POSIX_EXEC] |= S_IRWXG;
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",g::rwx");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, ",g::rw-");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",g::rwx");
    }

    primary_added = 0;
    if (acl->uid_map) {
        for (i=0; i<acl->uid_map->n; i++) {
            name = acl->uid_map->id[i].name;
            uid = acl->uid_map->id[i].uid;
            mode = acl->uid_map->id[i].mode;

            if (uid == facl->uid_primary) {  //** Primary GID
                primary_added = 1;
                if (mode & PACL_MODE_WRITE) {
                    facl->mode[ACL_POSIX_DIR] |= S_IRWXU;
                    facl->mode[ACL_POSIX_FILE] |= S_IRUSR | S_IWUSR;
                    facl->mode[ACL_POSIX_EXEC] |= S_IRWXU;
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u::rwx");
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u::rw-");
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u::rwx");
                } else {
                    facl->mode[ACL_POSIX_DIR] |= S_IRUSR | S_IXUSR;
                    facl->mode[ACL_POSIX_FILE] |= S_IRUSR;
                    facl->mode[ACL_POSIX_EXEC] |= S_IRUSR | S_IXUSR;
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u::r-x");
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u::r--");
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u::r-x");
                }
            } else if (mode & PACL_MODE_WRITE) {
                if (name == NULL) {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u:%u:rwx", uid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u:%u:rw-", uid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u:%u:rwx", uid);
                } else {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u:%s:rwx", name);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u:%s:rw-", name);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u:%s:rwx", name);
                }
            } else {
                if (name == NULL) {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u:%u:r-x", uid);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u:%u:r--", uid);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u:%u:r-x", uid);
                } else {
                    tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u:%s:r-x", name);
                    tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u:%s:r--", name);
                    tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u:%s:r-x", name);
                }
            }
        }
    }

    if (primary_added == 0) { //** No primary UID so manually add one. Assumes full perms.
       facl->mode[ACL_POSIX_DIR] |= S_IRWXU;
       facl->mode[ACL_POSIX_FILE] |= S_IRUSR | S_IWUSR;
       facl->mode[ACL_POSIX_EXEC] |= S_IRWXU;
       tbx_append_printf(dir_acl_text, &dir_pos, nbytes, ",u::rwx");
       tbx_append_printf(file_acl_text, &file_pos, nbytes, ",u::rw-");
       tbx_append_printf(exec_acl_text, &exec_pos, nbytes, ",u::rwx");
    }

    if (dir_pos == 0) { //** No default found so assume the worst and just give read access
        tbx_append_printf(dir_acl_text, &dir_pos, nbytes, "u::r-x,g::r-x,o::---,m::r-x");
        tbx_append_printf(file_acl_text, &file_pos, nbytes, "u::r--,g::r--,o::---,m::r--");
        tbx_append_printf(exec_acl_text, &exec_pos, nbytes, "u::r-x,g::r-x,o::---,m::r-x");
    }

    log_printf(10, "DIRACL=%s\n", dir_acl_text);
    log_printf(10, "DIRMODE=%o\n", facl->mode[ACL_POSIX_DIR]);

    log_printf(1, "Prefix: %s uid=%u gid=%u\n", acl->prefix, facl->uid_primary, facl->gid_primary);

    //** Convert it to an ACL
    log_printf(1, "    posix_dir_acl=%s mode=%o\n", dir_acl_text, facl->mode[ACL_POSIX_DIR]);
    err = 0;
    err += _make_posix_acl(fdd, dir_acl_text, &(facl->acl[ACL_POSIX_DIR]), &(facl->size[ACL_POSIX_DIR]), acl->prefix, "posix_dir_acl");
    log_printf(1, "    posix_file_acl=%s mode=%o\n", file_acl_text, facl->mode[ACL_POSIX_FILE]);
    err += _make_posix_acl(fdf, file_acl_text, &(facl->acl[ACL_POSIX_FILE]), &(facl->size[ACL_POSIX_FILE]), acl->prefix, "posix_file_acl");
    log_printf(1, "    posix_exec_acl=%s mode=%o\n", exec_acl_text, facl->mode[ACL_POSIX_EXEC]);
    err += _make_posix_acl(fdf, exec_acl_text, &(facl->acl[ACL_POSIX_EXEC]), &(facl->size[ACL_POSIX_EXEC]), acl->prefix, "posix_exec_acl");

    acl->acl_text[ACL_POSIX_DIR] = strdup(dir_acl_text);
    acl->acl_text[ACL_POSIX_FILE] = strdup(file_acl_text);
    acl->acl_text[ACL_POSIX_EXEC] = strdup(exec_acl_text);

    if (pa->nfs4_enable) {
        snprintf(fname, sizeof(fname), "%s/dir", nfs4_fname);
        fname[sizeof(fname)-1] = '\0';
        log_printf(1, "    nfs4_dir_acl=%s\n", acl->acl_text[ACL_NFS4_DIR]);
        err += _make_nfs4_acl(fname, acl->acl_text[ACL_NFS4_DIR], &(facl->acl[ACL_NFS4_DIR]), &(facl->size[ACL_NFS4_DIR]), acl->prefix, "nfs4_dir_acl");

        snprintf(fname, sizeof(fname), "%s/file", nfs4_fname);
        fname[sizeof(fname)-1] = '\0';
        log_printf(1, "    nfs4_file_acl=%s\n", acl->acl_text[ACL_NFS4_FILE]);
        err += _make_nfs4_acl(fname, acl->acl_text[ACL_NFS4_FILE], &(facl->acl[ACL_NFS4_FILE]), &(facl->size[ACL_NFS4_FILE]), acl->prefix, "nfs4_file_acl");
        log_printf(1, "    nfs4_exec_acl=%s\n", acl->acl_text[ACL_NFS4_EXEC]);
        err += _make_nfs4_acl(fname, acl->acl_text[ACL_NFS4_EXEC], &(facl->acl[ACL_NFS4_EXEC]), &(facl->size[ACL_NFS4_EXEC]), acl->prefix, "nfs4_exec_acl");
    }

    if (err) {
        facl_destroy(facl, acl->acl_text);
        facl = NULL;
    }

    return(facl);
}

//**************************************************************************
// pacl_lfs_acls_generate - Generates the LFS ACLS for use with FUSE
//**************************************************************************

int pacl_lfs_acls_generate(path_acl_context_t *pa)
{
    int i, fdf, fdd;
    DIR *dir;
    char *fname, *dname, *nfs4_dname;
    int err, mount_err;

    err = 0;
    fname = dname = nfs4_dname = NULL;
    fdf = fdd = mount_err = -1;
    dir = NULL;

    //** Make the temp file for generating the ACLs on
    fname = strdup(pa->fname_acl);
    fdf = mkstemp(fname);
    dname = strdup(pa->fname_acl);
    if (mkdtemp(dname) == NULL) {
        err = errno;
        log_printf(-1, "ERROR: failed maxing temp ACL directory: %s errno=%d\n", dname, err);
        fprintf(stderr, "ERROR: pacl_lfs_acls_generate() failed maxing temp ACL directory: %s errno=%d\n", dname, err);
        err = 1;
        goto oops;
    }
    dir = opendir(dname);
    fdd = dirfd(dir);

    nfs4_dname = NULL;
    if (pa->nfs4_enable) {
        nfs4_dname = strdup(pa->fname_acl);
        if (mkdtemp(nfs4_dname) == NULL) {
            err = errno;
            log_printf(0, "ERROR: pacl_lfs_acls_generate -- failed making NFS4 ACL directory: %s errno=%d\n", dname, err);
            fprintf(stderr, "ERROR: acl_lfs_acls_generate -- failed making NFS4 ACL directory: %s errno=%d\n", dname, err);
            err = 2;
            goto oops;
        }
        if ((mount_err = _mount_nfs4(nfs4_dname)) != 0) {
            err = errno;
            log_printf(0, "ERROR: acl_lfs_acls_generate -- failed mounting min_fuse NFS4 ACL directory: %s errno=%d\n", nfs4_dname, err);
            fprintf(stderr, "ERROR: acl_lfs_acls_generate -- failed mounting min_fuse NFS4 ACL directory: %s errno=%d\n", nfs4_dname, err);
            err = 3;
            goto oops;
        }
    }

    log_printf(10, "Generating default ACL\n"); tbx_log_flush();
    //** 1st set the default FUSE ACL
    pa->pacl_default->lfs_acl = pacl2lfs_acl(pa, pa->pacl_default, fdf, fdd, fname, dname, nfs4_dname);
    if (!pa->pacl_default->lfs_acl) err++;

    //** And now all the Path's
    for (i=0; i<pa->n_path_acl; i++) {
        log_printf(10, "Generating acl for prefix[%d]=%s\n", i, pa->path_acl[i]->prefix);
        pa->path_acl[i]->lfs_acl = pacl2lfs_acl(pa, pa->path_acl[i], fdf, fdd, fname, dname, nfs4_dname);
        if (!pa->path_acl[i]->lfs_acl) {
            fprintf(stderr, "ERROR: pacl_lfs_acls_generate() failed generating acl for prefix[%d]:%s\n", i, pa->path_acl[i]->prefix);
            log_printf(-1, "ERROR: pacl_lfs_acls_generate() failed generating acl for prefix[%d]:%s\n", i, pa->path_acl[i]->prefix);
            err++;
        }
    }

    //** Cleanup
oops:
    if (fdf != -1) close(fdf);
    if (dir != NULL) closedir(dir);
    if (fname) remove(fname);
    if (dname) rmdir(dname);
    if (fname) free(fname);
    if (dname) free(dname);
    if (nfs4_dname) {
        if (mount_err == 0) _umount_nfs4(nfs4_dname);
        rmdir(nfs4_dname);
        free(nfs4_dname);
    }
    return(err);
}

//**************************************************************************
// pacl_compare_nested_acls - Compares the 2 acls making sure the 2ndary is
//    a subset of the primary
//**************************************************************************

int pacl_compare_nested_acls(path_acl_t *a, path_acl_t *b)
{
    int i, j, checked;

    //** Check the "other_mode"
    if (a->other_mode < b->other_mode) {
        log_printf(-1, "ERROR: OTHER mismatch! parent->prefix=%s parent->other_mode=%d child->prefix=%s child->other=%d\n",
            a->prefix, a->other_mode, b->prefix, b->other_mode);
        return(1);
    }

    //** Check the accounts. All the "b" accounts should exist in "a"
    for (i=0; i<b->n_account; i++) {
        checked = 0;
        for (j=0; j<a->n_account; j++) {
            if (strcmp(b->account[i].account, a->account[j].account) == 0) {
                if (a->account[j].mode < b->account[i].mode) {
                    log_printf(-1, "ERROR: ACCOUNT mismatch! account=%s parent->prefix=%s parent->account_mode=%d child->prefix=%s child->account_mode=%d\n",
                        a->account[j].account, a->prefix, a->account[j].mode, b->prefix, b->account[i].mode);
                    return(2);
                }

                checked = 1;
                break;  //** Got a match so continue to the next one
            }
        }
        if (checked == 0) {
            log_printf(0, "ERROR: ACCOUNT missing in parent! account=%s parent->prefix=%s child->prefix=%s\n",
               b->account[i].account, a->prefix, b->prefix);
            return(3);  //** The account is missing in the parent
        }
    }

    return(0);
}

//**************************************************************************
// _uid_found - Scans the UID list to see if it already exists and if so returns 1 otherwise 0
//**************************************************************************

int _uid_found(int n, fs_acl_t *uid_list, uid_t uid)
{
    int i;

    for (i=0; i<n; i++) {
        if (uid == uid_list[i].uid) return(i);
    }

    return(-1);
}

//**************************************************************************
// _user2uid - converts the user name to a UID. Returns 0 on success -1 otherwise
//**************************************************************************

int _user2uid(const char *user, uid_t *uid)
{
    struct passwd *pw;

    pw = getpwnam(user);
    if (!pw) return(-1);
    *uid = pw->pw_uid;
    return(0);
}

//**************************************************************************
// uid_list_add - Adds a UID to the list if it doesn't already exist
//**************************************************************************

int uid_list_add(uid_t uid, int mode, char *user, fs_acl_t *uid_list, int *n)
{
//    if (_uid_found(*n, uid_list, uid) != -1) return(-1);  //** Already exists so kick out

    uid_list[*n].uid = uid;
    uid_list[*n].mode = mode;
    uid_list[*n].name = user;
    (*n)++;
    return(*n-1);
}

//**************************************************************************
// uid_parse
//**************************************************************************

int uid_parse(char *uid_str, int mode, fs_acl_t *uid_list, int *n)
{
    uid_t uid;
    struct passwd *pw;

    uid = tbx_stk_string_get_integer(uid_str);
    pw = getpwuid(uid);
    if (_uid_found(*n, uid_list, uid) != -1) return(-1);

    if (pw) return(uid_list_add(uid, mode, strdup(pw->pw_name), uid_list, n));

    return(uid_list_add(uid, mode, NULL, uid_list, n));
}

//**************************************************************************
// user_parse
//**************************************************************************

int user_parse(char *user, int mode, fs_acl_t *uid_list, int *n)
{
    uid_t uid;
    int k;

    if (_user2uid(user, &uid) == -1)  return(-1);

    k =_uid_found(*n, uid_list, uid);
    if (k != -1) return(k);   //** Already exists so kick out

    return(uid_list_add(uid, mode, strdup(user), uid_list, n));
}

//**************************************************************************
// _fsgid_found - Scans the GID list to see if it already exists and if so returns 1 otherwise 0
//**************************************************************************

int _fsgid_found(int n, fs_acl_t *gid_list, gid_t gid)
{
    int i;

    for (i=0; i<n; i++) {
        if (gid == gid_list[i].gid) return(i);
    }

    return(-1);
}

//**************************************************************************
// gid_list_add - Adds a GID to the list if it doesn't already exist
//**************************************************************************

int gid_list_add(gid_t gid, int mode, char *group, fs_acl_t *gid_list, int *n)
{
//    if (_fsgid_found(*n, gid_list, gid) != -1) return(-1);  //** Already exists so kick out

    gid_list[*n].gid = gid;
    gid_list[*n].mode = mode;
    gid_list[*n].name = group;

    (*n)++;
    return(*n-1);
}

//**************************************************************************
// gid_parse
//**************************************************************************

int gid_parse(char *gid_str, int mode, fs_acl_t *gid_list, int *n)
{
    gid_t gid;
    struct group *g;
    int k;

    gid = tbx_stk_string_get_integer(gid_str);
    g = getgrgid(gid);
    k = _fsgid_found(*n, gid_list, gid);
    if (k != -1) return(k);
    if (g) return(gid_list_add(gid, mode, strdup(g->gr_name), gid_list, n));
    return(gid_list_add(gid, mode, NULL, gid_list, n));
}

//**************************************************************************
// group_parse
//**************************************************************************

int group_parse(char *group, int mode, fs_acl_t *gid_list, int *n)
{
    gid_t gid;

    if (_group2gid(group, &gid) == -1)  return(-1);

    if (_fsgid_found(*n, gid_list, gid) != -1) return(-1);   //** Already exists so kick out

    return(gid_list_add(gid, mode, strdup(group), gid_list, n));
}

//**************************************************************************
// guid_set_primary - Sets the primary UIG/GID for the prefix
//**************************************************************************

void guid_set_primary(path_acl_context_t *ctx, path_acl_t *acl)
{
    account2gid_t *a2g;

    if (acl->lfs_gid != _pa_guid_unused) goto set_uid;

    //** Check if there is a default from the account->GID mappings
    if (acl->lfs_account) {
        a2g = apr_hash_get(ctx->a2gid_hash, acl->lfs_account, APR_HASH_KEY_STRING);
        if (a2g) {
            if (a2g->lfs_gid != _pa_guid_unused) {
                acl->lfs_gid = a2g->lfs_gid;
                goto set_uid;
            }
        }
    }

    //** So just use the process GID
    acl->lfs_gid = getgid();

set_uid:
    if (acl->lfs_uid == _pa_guid_unused) {
        acl->lfs_uid = getuid();
    }
}

//**************************************************************************
// lfs_add_account_mappings - Adds the account mappings to the GID list's for use by LFS and the FS
//**************************************************************************

void lfs_add_account_mappings(path_acl_context_t *ctx, path_acl_t *acl)
{
    int j, k, n_gid, n_old;
    account2gid_t *a2g;
    fs_acl_t gid_list[200];
    struct group *g;

    //** Copy the existing list
    n_gid = n_old = 0;
    acl->gid_account_start = 0;
    if (acl->gid_map) {
        acl->gid_account_start = n_gid+1;
        n_gid = acl->gid_map->n;
        n_old = n_gid;
        if (n_gid > 0) memcpy(gid_list, acl->gid_map->id, sizeof(fs_acl_t)*n_gid);
    }
    for (j=0; j<acl->n_account; j++) {
        a2g = apr_hash_get(ctx->a2gid_hash, acl->account[j].account, APR_HASH_KEY_STRING);
        if (!a2g) continue;  //** Skip if no entry
        for (k=0; k<a2g->n_gid; k++) {
            if (_fsgid_found(n_gid, gid_list, a2g->gid[k]) == -1) {
                g = getgrgid(a2g->gid[k]);
                gid_list_add(a2g->gid[k], acl->account[j].mode, ((g) ? strdup(g->gr_name) : NULL), gid_list, &n_gid);
            }
        }
    }

    if (n_gid > n_old) { //** Added some entries so add them
        if (acl->gid_map) free(acl->gid_map);
        acl->gid_map = malloc(sizeof(fs_acl_list_t) + n_gid*sizeof(fs_acl_t));
        memcpy(acl->gid_map->id, gid_list, sizeof(fs_acl_t)*n_gid);
        acl->gid_map->n = n_gid;
    }
}

//**************************************************************************
// prefix_account_parse - Parse the INI file and populates the
//     prefix/account associations.
//     NOTE: The INI file if from the LIO context and persists for
//           duration of the program so no need to dup strings
//**************************************************************************

int prefix_account_parse(path_acl_context_t *pa, tbx_inip_file_t *fd)
{
    path_acl_t *acl;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *value, *prefix, *other_mode, *lfs;
    tbx_stack_t *stack, *acl_stack;
    int i, j, k, def, match, n_uid, n_gid, err, override_mode;
    uid_t lfs_uid, uid;
    gid_t lfs_gid, gid;
    fs_acl_t uid_list[100];
    fs_acl_t gid_list[100];
    char *nfs4_dir, *nfs4_file, *nfs4_exec;

    err = 0;

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
            lfs_uid = _pa_guid_unused; n_uid = 0;
            lfs_gid = _pa_guid_unused; n_gid = 0;
            nfs4_dir = nfs4_file = nfs4_exec = NULL;
            override_mode = 0;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "prefix") == 0) {  //** Got the path prefix
                    prefix = value;
                } else if (strncmp(key, "account", 7) == 0) { //** Got an account
                    if ((strcmp(key, "account") == 0) || (strcmp(key, "account(rw)") == 0) || (strcmp(key, "account(r)") == 0)) {
                        tbx_stack_push(stack, key);
                        tbx_stack_push(stack, value);
                    } else {
                        log_printf(-1, "ERROR: Unknown account option: %s = %s\n", key, value);
                    }
                } else if (strcmp(key, "other") == 0) {  //** Only used for defaults
                    other_mode = value;
                } else if (strcmp(key, "lfs_account") == 0) {  //** Only used with LFS
                    lfs = value;
                } else if (strcmp(key, "lfs_uid") == 0) { //** Got the default UID for LFS
                    lfs_uid = tbx_stk_string_get_integer(value);
                } else if (strcmp(key, "lfs_user") == 0) { //** Got the default User for LFS
                    if (_user2uid(value, &uid) != -1) {
                        lfs_uid = uid;
                    }
                } else if (strcmp(key, "lfs_gid") == 0) { //** Got the default GID for LFS
                    lfs_gid = tbx_stk_string_get_integer(value);
                } else if (strcmp(key, "lfs_group") == 0) { //** Got the default Group for LFS
                    if (_group2gid(value, &gid) != -1) {
                        lfs_gid = gid;
                    }
                } else if ((strcmp(key, "user") == 0) || (strcmp(key, "user(rw)") == 0)) {
                    user_parse(value, PACL_MODE_RW, uid_list, &n_uid);
                } else if (strcmp(key, "user(r)") == 0) {
                    user_parse(value, PACL_MODE_READ, uid_list, &n_uid);
                } else if ((strcmp(key, "uid") == 0) || (strcmp(key, "uid(rw)") == 0)) {
                    uid_parse(value, PACL_MODE_RW, uid_list, &n_uid);
                } else if (strcmp(key, "uid(r)") == 0) {
                    uid_parse(value, PACL_MODE_READ, uid_list, &n_uid);
                } else if ((strcmp(key, "group") == 0) || (strcmp(key, "group(rw)") == 0)) {
                    group_parse(value, PACL_MODE_RW, gid_list, &n_gid);
                } else if (strcmp(key, "group(r)") == 0) {
                    group_parse(value, PACL_MODE_READ, gid_list, &n_gid);
                } else if ((strcmp(key, "gid") == 0) || (strcmp(key, "gid(rw)") == 0)) {
                    gid_parse(value, PACL_MODE_RW, gid_list, &n_gid);
                } else if (strcmp(key, "gid(r)") == 0) {
                    gid_parse(value, PACL_MODE_READ, gid_list, &n_gid);
                } else if (strcmp(key, "nfs4_dir") == 0) {
                    if (pa->nfs4_enable) {
                        nfs4_dir = strdup(value);
                        tbx_stk_string_remove_space(nfs4_dir);
                    }
                } else if (strcmp(key, "nfs4_file") == 0) {
                    if (pa->nfs4_enable) {
                        nfs4_file = strdup(value);
                        tbx_stk_string_remove_space(nfs4_file);
                    }
                } else if (strcmp(key, "nfs4_exec") == 0) {
                    if (pa->nfs4_enable) {
                        nfs4_exec = strdup(value);
                        tbx_stk_string_remove_space(nfs4_exec);
                    }
                } else if (strcmp(key, "perms_override") == 0) {
                    if (tbx_stk_string_get_integer(value) > 0) override_mode |= PACL_MODE_PERMS;
                } else if (strcmp(key, "posix_override") == 0) {
                    if (tbx_stk_string_get_integer(value) > 0) override_mode |= PACL_MODE_POSIX;
                } else if (strcmp(key, "noposix_override") == 0) {
                    if (tbx_stk_string_get_integer(value) > 0) override_mode |= PACL_MODE_NOPOSIX;
                } else if (strcmp(key, "nfs4_override") == 0) {
                    if (tbx_stk_string_get_integer(value) > 0) override_mode |= PACL_MODE_NFS4;
                } else if (strcmp(key, "nonfs4_override") == 0) {
                    if (tbx_stk_string_get_integer(value) > 0) override_mode |= PACL_MODE_NONFS4;
                } else {  //** Unknown option so just report it
                    log_printf(-1, "ERROR: Unknown option: %s = %s\n", key, value);
                }

                ele = tbx_inip_ele_next(ele);
            }

            //** Make sure we have a default mapping if needed
            if ((prefix) && (tbx_stack_count(stack)>0)) { //** Got a valid entry
                tbx_type_malloc_clear(acl, path_acl_t, 1);
                acl->n_account = tbx_stack_count(stack)/2;
                tbx_type_malloc_clear(acl->account, account_acl_t, acl->n_account);
                acl->override_mode = override_mode;
                acl->acl_text[ACL_NFS4_DIR] = nfs4_dir;
                acl->acl_text[ACL_NFS4_FILE] = nfs4_file;
                acl->acl_text[ACL_NFS4_EXEC] = nfs4_exec;
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
                }

                acl->lfs_uid = lfs_uid;
                if (n_uid > 0) {
                    acl->uid_map = malloc(sizeof(fs_acl_list_t) + n_uid * sizeof(fs_acl_t));
                    acl->uid_map->n = n_uid;
                    memcpy(acl->uid_map->id, uid_list, sizeof(fs_acl_t)*n_uid);
                }
                acl->lfs_gid = lfs_gid;
                if (n_gid > 0) {
                    acl->gid_map = malloc(sizeof(fs_acl_list_t) + n_gid * sizeof(fs_acl_t));
                    acl->gid_map->n = n_gid;
                    memcpy(acl->gid_map->id, gid_list, sizeof(fs_acl_t)*n_gid);
                }

            } else {
                log_printf(-1, "ERROR: Missing fields! prefix=%s stack_count(accounts)=%d\n", prefix, tbx_stack_count(stack));
                err = 1;
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
        pa->path_acl[i]->nested_primary = -1;
        pa->path_acl[i]->nested_parent = -1;

        if (pa->nfs4_enable) {
            if (pa->path_acl[i]->acl_text[ACL_NFS4_DIR] == NULL) {  //** IF empty copy over the values from default
                if (pa->pacl_default->acl_text[ACL_NFS4_DIR]) pa->path_acl[i]->acl_text[ACL_NFS4_DIR] = strdup(pa->pacl_default->acl_text[ACL_NFS4_DIR]);
                if (pa->pacl_default->acl_text[ACL_NFS4_FILE]) pa->path_acl[i]->acl_text[ACL_NFS4_FILE] = strdup(pa->pacl_default->acl_text[ACL_NFS4_FILE]);
                if (pa->pacl_default->acl_text[ACL_NFS4_EXEC]) pa->path_acl[i]->acl_text[ACL_NFS4_EXEC] = strdup(pa->pacl_default->acl_text[ACL_NFS4_EXEC]);
            }
        }
    }
    qsort_r(pa->path_acl, pa->n_path_acl, sizeof(path_acl_t *), pacl_sort_fn, NULL);

    //** check for nested ACLs and annotate as needed
    pa->n_lut = 0;
    tbx_type_malloc_clear(pa->lut, int, pa->n_path_acl);
    for (i=0; i<pa->n_path_acl; i++) {
        match = -1;
        if (pa->path_acl[i]->nested_primary == -1) pa->path_acl[i]->nested_primary = i;
        pa->path_acl[i]->rlut = pa->n_lut;
        for (j=i+1; j<pa->n_path_acl; j++) {
            //** check if the base prefixes match based on parent string
            if (strncmp(pa->path_acl[i]->prefix, pa->path_acl[j]->prefix, pa->path_acl[i]->n_prefix) != 0) break;

            //** Ok they match so it could be a similar prefix
            //** Check to make sure the next char is a '/' otherwise it's a similar name
            k = pa->path_acl[i]->n_prefix;
            if ((pa->path_acl[j]->n_prefix > k) && (pa->path_acl[j]->prefix[k] != '/')) {
                break;
            }

            if (pa->path_acl[j]->nested_primary == -1) pa->path_acl[j]->nested_primary = i;
            pa->path_acl[j]->nested_parent = i;
            if (pacl_compare_nested_acls(pa->path_acl[i], pa->path_acl[j]) != 0) {
                log_printf(-1, "ERROR: bad nested ACLs! prefix=%s prefix_nested=%s\n", pa->path_acl[i]->prefix, pa->path_acl[j]->prefix);
                tbx_log_flush();
                return(1);
            }
            match = j;
        }

        pa->path_acl[i]->nested_end = match;
        if (match == -1) {
            pa->lut[pa->n_lut] = pa->path_acl[i]->nested_primary;
            pa->n_lut++;
        }
    }

    //** Clean up
    tbx_stack_free(stack, 0);
    tbx_stack_free(acl_stack, 0);

    return(err);
}

//**************************************************************************
// _gid_found - Scans the GID list to see if it already exists and if so returns 1 otherwise 0
//**************************************************************************

int _gid_found(int n, gid_t *gid_list, gid_t gid)
{
    int i;

    for (i=0; i<n; i++) {
        if (gid == gid_list[i]) return(1);
    }

    return(0);
}

//**************************************************************************
// _group2gid - converts the group name to a GID. Returns 0 on success -1 otherwise
//**************************************************************************

int _group2gid(const char *group, gid_t *gid)
{
    struct group *grp;

    grp = getgrnam(group);
    if (!grp) return(-1);

    *gid = grp->gr_gid;
    return(0);
}

//**************************************************************************
// gid2account_parse - Make the GID -> account mappings
//     NOTE: The INI file if from the LIO context and persists for
//           duration of the program so no need to dup strings
//**************************************************************************

void gid2account_parse(path_acl_context_t *pa, tbx_inip_file_t *fd)
{
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *value, *account;
    tbx_stack_t *a2g_stack;
    gid_t gid_list[100], lfs_gid, got_lfs_gid, g;
    int n;
    account2gid_t *a2g;
    int i;

    a2g_stack = tbx_stack_new();

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        if (strcmp("path_acl_mapping", tbx_inip_group_get(ig)) == 0) { //** Got a prefix
            ele = tbx_inip_ele_first(ig);
            account = NULL;
            got_lfs_gid = 0;
            lfs_gid = 0;
            n = 0;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "account") == 0) {  //** Got the account
                    account = value;
                } else if (strcmp(key, "gid") == 0) { //** Got a GID
                    g = tbx_stk_string_get_integer(value);
                    if (_gid_found(n, gid_list, g) == 0) {
                        gid_list[n] = g;
                        n++;
                    }
                } else if (strcmp(key, "lfs_gid") == 0) { //** Got the default GID for LFS
                    g = tbx_stk_string_get_integer(value);
                    if (_gid_found(n, gid_list, g) == 0) {
                        gid_list[n] = g;
                        n++;
                    }
                    lfs_gid = g;
                    got_lfs_gid = 1;
                } else if (strcmp(key, "group") == 0) { //** Got a group name
                    if (_group2gid(value, &g) == 0) {
                        if (_gid_found(n, gid_list, g) == 0) {
                            gid_list[n] = g;
                            n++;
                        }
                    }
                } else if (strcmp(key, "lfs_group") == 0) { //** Got an LFS group name
                    if (_group2gid(value, &g) == 0) {
                        if (_gid_found(n, gid_list, g) == 0) {
                            gid_list[n] = g;
                            n++;
                        }
                        lfs_gid = g;
                        got_lfs_gid = 1;
                    }
                }

                ele = tbx_inip_ele_next(ele);
            }

            if ((account) && (n>0)) {
                tbx_type_malloc_clear(a2g, account2gid_t, 1);
                a2g->account = strdup(account);

                a2g->lfs_gid = (got_lfs_gid == 0) ? gid_list[0] : lfs_gid;

                a2g->n_gid = n;
                tbx_type_malloc_clear(a2g->gid, gid_t, a2g->n_gid);
                memcpy(a2g->gid, gid_list, n*sizeof(gid_t));
                for (i=0; i<a2g->n_gid; i++) {
                    apr_hash_set(pa->gid2acct_hash, a2g->gid + i, sizeof(gid_t), a2g);
                }
                tbx_stack_push(a2g_stack, a2g);
            } else {
                log_printf(0, "ERROR: Missing fields! account=%s n_gid=%d\n", account, n);
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
}

//**************************************************************************
// pacl_override_settings - Returns the ACL override settings over all the prefixes
//**************************************************************************

int pacl_override_settings(path_acl_context_t *pa)
{
    int i, override;
    path_acl_t *acl;

    override = 0;
    for (i=0; i<pa->n_path_acl; i++) {
        acl = pa->path_acl[i];
        if (acl->override_mode & PACL_MODE_PERMS) override |= PACL_MODE_PERMS;
        if (acl->override_mode & PACL_MODE_POSIX) override |= PACL_MODE_POSIX;
        if (acl->override_mode & PACL_MODE_NFS4) override |= PACL_MODE_NFS4;
    }

    return(override);
}

//**************************************************************************
// pacl_create - Creates a Path ACL structure
//    This parse all [path_acl] and [path_acl_mapping] sections in the INI file
//    to create the structure.  The format for each section type are defined below.
//    Multiple account, user, group, uid, and gid entries are allowed in [path_acl] sections.
//    If no default user or group is specified then the calling process user/group is used.
//
//    NOTE: The prefixes can be nested as long as the resulting ACLs are restrictive
//          The GID->account mapping must be unique!
//
//    [path_acl_default]
//    default=r|rw
//    account=<account>
//
//    [path_acl]
//    path=<prefix>
//    perms_override = 1 #Optional flag to tell the calling program overriding UID/GID/PERMS is wanted
//    posix_override = 1 #Optional flag to tell the calling program overriding POSIX ACLs is wanted
//    noposix_override = 1 #Optional flag to tell the calling program to NOT return the constructed POSIXACLs
//    nfs4_override = 1  #Optional flag to tell the calling program overriding NFSv4 ACLs is wanted
//    nonfs4_override = 1  #Optional flag to tell the calling program to NOT return the constructed NFSv4 ACLs
//    lfs_account=<account> #Optional default account reported by FUSE. Must still have an "account" entry
//    account =<account_1>   # Same as "(rw)"
//    account(r)=<account_2>
//    ...
//    account(rw)=<account_N>
//    lfs_user = <user>   #Optionial default owner reported by FUSE.  Must still have a user/uid entry
//    lfs_uid = <uid>     #Optionial default owner UID reported by FUSE.  Must still have a user/uid entry.
//    lfs_group = <group> #Optionial default group owner reported by FUSE.  Must still have a group/gid entry. Overrides lfs_account.
//    lfs_gid = <gid>     #Optionial default group owner GID reported by FUSE.  Must still have a group/gid entry. Overrides lfs_account.
//    user = <user>       #Local user access.  No modifier means full R/W access. Valid modifiers user(r), user(rw).
//    uid = <uid>         #Local user UID access.  No modifier means full R/W access. Valid modifiers uid(r), uid(rw).
//    group = <group>     #Local group access.  No modifier means full R/W access. Valid modifiers group(r), group(rw).
//    gid = <gid>         #Local group UID access.  No modifier means full R/W access. Valid modifiers gid(r), gid(rw).
//    nfs4_dir = <NFSv4 ACE>  #NFSv4 ACL for directories
//    nfs4_file = <NFSv4 ACE>  #NFSv4 ACL for files
//    nfs4_exec = <NFSv4 ACE>  #NFSv4 ACL for executable files
//
//    [path_acl_mapping]
//    account=<account>
//    lfs_gid=<gid_1>  #This is used for the FUSE group ownership
//    ...
//    gid=<gid_N>
//
//**************************************************************************

path_acl_context_t *pacl_create(tbx_inip_file_t *fd, char *fname_lfs_acls, int nfs4_enable)
{
    path_acl_context_t *pa;
    char fname[4096];
    int i;

    log_printf(10, "Loading PACL's\n");

    //** Create the structure
    tbx_type_malloc_clear(pa, path_acl_context_t, 1);
    assert_result(apr_pool_create(&(pa->mpool), NULL), APR_SUCCESS);
    pa->gid2acct_hash = apr_hash_make(pa->mpool);
    pa->a2gid_hash = apr_hash_make(pa->mpool);
    pa->hints_hash = apr_hash_make(pa->mpool);
    pa->dt_hint_cache = PA_HINT_TIMEOUT;
    pa->nfs4_enable = nfs4_enable;    //** If this is set then we add NFS4 ACLs and also throw errors if hit

    //**Now populate it
    if (prefix_account_parse(pa, fd) != 0) { //** Add the prefix/account associations
        fprintf(stderr, "ERROR: prefix_account_parse() failed!\n"); fflush(stderr);
        log_printf(0, "ERROR: prefix_account_parse() failed!\n"); tbx_log_flush();
        pacl_destroy(pa);  //** Got an error so cleanup and kick out
        return(NULL);
    }
    gid2account_parse(pa, fd);     //** And the GID->Account mappings, if any exist

    for (i=0; i<pa->n_path_acl; i++) {
        lfs_add_account_mappings(pa, pa->path_acl[i]);  //** Add the account's GIDs to the GID lists for use by LFS and the FS layer
        guid_set_primary(pa, pa->path_acl[i]);         //** Set the default UID/GID if not specified
    }
    lfs_add_account_mappings(pa, pa->pacl_default);
    guid_set_primary(pa, pa->pacl_default);

    fname[sizeof(fname)-1] = 0;
    if (fname_lfs_acls) {
        snprintf(fname, sizeof(fname)-1, "%s.XXXXXX", fname_lfs_acls);
        pa->fname_acl = strdup(fname);
        if (pacl_lfs_acls_generate(pa) != 0) {
            fprintf(stderr, "ERROR: pacl_lfs_acls_genereate() failed! fname=%s\n", fname);  fflush(stderr);
            log_printf(0, "ERROR: pacl_lfs_acls_genereate() failed! fname=%s\n", fname);  tbx_log_flush();
            pacl_destroy(pa);
            return(NULL);
        }
    }

    pa->timestamp = apr_time_now();

    return(pa);
}

//**************************************************************************
// facl_destroy - Frees the mem associated with the FUSE ACL
//**************************************************************************

void facl_destroy(fuse_acl_t *facl, char **acl_text)
{
    int i;

    if (!facl) return;

    for (i=0; i<ACL_MAX; i++) {
        if (facl->acl[i]) free(facl->acl[i]);
        if (acl_text[i]) { free(acl_text[i]); acl_text[i] = NULL; }
     }
     free(facl);
}

//**************************************************************************
// prefix_destroy
//**************************************************************************

void prefix_destroy(path_acl_t *acl)
{
    int i;

    log_printf(10, "prefix=%s lfs_acl=%p\n", acl->prefix, acl->lfs_acl);
    facl_destroy(acl->lfs_acl, acl->acl_text);
    if (acl->uid_map) {
        for (i=0; i<acl->uid_map->n; i++) {
            if (acl->uid_map->id[i].name) free(acl->uid_map->id[i].name);
        }
        free(acl->uid_map);
    }

    if (acl->gid_map) {
        for (i=0; i<acl->gid_map->n; i++) {
            if (acl->gid_map->id[i].name) free(acl->gid_map->id[i].name);
        }
        free(acl->gid_map);
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

    //** Free the hints and check if there are lingering hint handles.  If so defer the destruction.
    for (hi=apr_hash_first(NULL, pa->hints_hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&hint);
        if (hint->inuse == 0) {
            free(hint);  //** The interior strings are just pointers to fields in a2g above
        } else {  //** Still in use so we have to defer destruction
            pa->inuse_pending++;
            hint->to_release = 1;
            hint->pa = pa;
        }
    }

    if (pa->inuse_pending) return;  //** Kick out of we still have hints in use.

    log_printf(10, "n_path_acl=%d\n", pa->n_path_acl);
    if (pa->pacl_default) prefix_destroy(pa->pacl_default);
    if (pa->lut) free(pa->lut);

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

    //** This also destroys the hash
    apr_pool_destroy(pa->mpool);

    //** and container
    free(pa);
}

//**************************************************************************
// pacl_facl_print - Prints the FUSE ACL to the FD
//**************************************************************************

void pacl_acl_print(path_acl_t *acl, fuse_acl_t *facl, const char *text_prefix, FILE *fd)
{
    struct group *grp;
    struct passwd *user;
    int i;

    //** Print the LStore ACL info
    for (i=0; i<acl->n_account; i++) {
        fprintf(fd, "%saccount(%s) = %s\n", text_prefix, ((acl->account[i].mode == PACL_MODE_READ) ? "r" : "rw"), acl->account[i].account);
    }

    //** Now the FUSE ACL's
    grp = getgrgid(facl->gid_primary);
    user = getpwuid(facl->uid_primary);
    fprintf(fd, "%sperms_override=%d posix_override=%d nfs4_override=%d\n", text_prefix, IS_SET(acl->override_mode, PACL_MODE_PERMS),
        IS_SET(acl->override_mode, PACL_MODE_POSIX), IS_SET(acl->override_mode, PACL_MODE_NFS4));
    fprintf(fd, "%suser=%s group=%s\n", text_prefix, user->pw_name, grp->gr_name);
    fprintf(fd, "%sPOSIX_DIR:  %s\n", text_prefix, acl->acl_text[ACL_POSIX_DIR]);
    fprintf(fd, "%sPOSIX_FILE:  %s\n", text_prefix, acl->acl_text[ACL_POSIX_FILE]);
    fprintf(fd, "%sPOSIX_EXEC:  %s\n", text_prefix, acl->acl_text[ACL_POSIX_EXEC]);

    if (acl->acl_text[ACL_NFS4_DIR]) {
        fprintf(fd, "%sNFS4_DIR:  %s\n", text_prefix, acl->acl_text[ACL_NFS4_DIR]);
        fprintf(fd, "%sNFS4_FILE:  %s\n", text_prefix, acl->acl_text[ACL_NFS4_FILE]);
        fprintf(fd, "%sNFS4_EXEC:  %s\n", text_prefix, acl->acl_text[ACL_NFS4_EXEC]);
    }
    fprintf(fd, "\n");
}

//**************************************************************************
// pacl_path_probe_recurse - Recurses print the PACL tree
//**************************************************************************

void pacl_path_probe_recurse(path_acl_context_t *pa, path_acl_t *acl, int index, FILE *fd)
{
    if (acl->nested_parent != -1 ) {
        pacl_path_probe_recurse(pa, pa->path_acl[acl->nested_parent], acl->nested_parent, fd);
    }

    fprintf(fd, "%4d: %s  nested_primary=%d nested_end=%d\n", index, acl->prefix, acl->nested_parent, acl->nested_end);
}


//**************************************************************************
// pacl_path_probe - Dumps information about the prefix the path maps to
//   Prints details to the given fd if provided and returns an index representing
//   which internal prefix it maps to or -1 if the default is hit
//**************************************************************************

int pacl_path_probe(path_acl_context_t *pa, const char *prefix, int do_acl_tree, FILE *fd, int seed)
{
    int exact, got_default, index;
    path_acl_t *acl;

    index = seed;
    acl = pacl_search(pa, prefix, &exact, &got_default, &index);

    if (do_acl_tree == 0) {
        fprintf(fd, "  Prefix: %s\n", acl->prefix);
        fprintf(fd, "  exact=%d got_default=%d index=%d\n", exact, got_default, index);
        return(index);
    }

    //** If we made it here they want the annotated tree
    if (got_default == 0) {
        pacl_path_probe_recurse(pa, acl, index, fd);
    } else {
        fprintf(fd, "DEFAULT\n");
    }
    fprintf(fd, "USER: %s\n", prefix);

    return(index);
}

//**************************************************************************
// pacl_print_tree - Prints the ACL tree
//**************************************************************************

int pacl_print_tree(path_acl_context_t *pa, const char *prefix, FILE *fd)
{
    path_acl_t *acl;
    int i, fdf, fdd, err, mount_err;
    DIR *dir;
    char *fname, *dname, *nfs4_dname;


    err = 0;
    fdf = fdd = mount_err = -1;
    fname = dname = nfs4_dname = NULL;
    dir = NULL;

    //** Make the temp file for generating the ACLs on
    fname = strdup(pa->fname_acl);
    fdf = mkstemp(fname);
    dname = strdup(pa->fname_acl);
    if (mkdtemp(dname) == NULL) {
        log_printf(0, "ERROR: failed making temp POSIX ACL directory: %s\n", dname);
        fprintf(stderr, "ERROR: failed making temp POSIX ACL directory: %s\n", dname);
        err = 1;
        goto oops;
    }
    dir = opendir(dname);
    fdd = dirfd(dir);

    nfs4_dname = NULL;
    if (pa->nfs4_enable) {
        nfs4_dname = strdup(pa->fname_acl);
        if (mkdtemp(nfs4_dname) == NULL) {
            err = errno;
            log_printf(0, "ERROR: failed making NFS4 ACL directory: %s, errno=%d\n", dname, err);
            fprintf(stderr, "ERROR: failed making NFS4 ACL directory: %s, errno=%d\n", dname, err);
            err = 2;
            goto oops;
        }
        if ((mount_err = _mount_nfs4(nfs4_dname)) != 0) {
            err = errno;
            log_printf(0, "ERROR: failed mounting min_fuse NFS4 ACL directory: %s  errno=%d\n", nfs4_dname, err);
            fprintf(stderr, "ERROR: failed mounting min_fuse NFS4 ACL directory: %s  errno=%d\n", nfs4_dname, err);
            err = 3;
            goto oops;
        }
    }

    fprintf(fd, "n_path_acl=%d\n", pa->n_path_acl);
    fprintf(fd, "\n");
    fprintf(fd, "Default ACL\n");
    pacl_acl_print(pa->pacl_default, pa->pacl_default->lfs_acl, "      ", fd);

    for (i=0; i<pa->n_path_acl; i++) {
        acl = pa->path_acl[i];
        fprintf(fd, "\n");
        fprintf(fd, "%4d: %s  nested_primary=%d nested_end=%d nested_parent=%d rlut=%d\n", i, acl->prefix,
            acl->nested_primary, acl->nested_end, acl->nested_parent, acl->rlut);
        pacl_acl_print(acl, acl->lfs_acl, "      ", fd);
    }

    //** Cleanup
oops:
    if (fdf != -1) close(fdf);
    if (dir != NULL) closedir(dir);
    if (fname) remove(fname);
    if (dname) rmdir(dname);
    if (fname) free(fname);
    if (dname) free(dname);
    if (nfs4_dname) {
        if (mount_err == 0) _umount_nfs4(nfs4_dname);
        rmdir(nfs4_dname);
        free(nfs4_dname);
    }
    return(err);
}
