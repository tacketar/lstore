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

//***************************************************************************
//***************************************************************************

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mntent.h>
#include <assert.h>
#include <apr_time.h>
#include "resource.h"
#include <tbx/log.h>
#include "debug.h"
#include <tbx/fmttypes.h>
#include "rid.h"
#include <tbx/append_printf.h>
#include <tbx/monitor.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/assert_result.h>
#include "ibp_time.h"


#define _RESOURCE_BUF_SIZE 1048576
char _blanks[_RESOURCE_BUF_SIZE];

const int alloc_lru_size = sizeof(Allocation_t);
const char *_res_types[] = { DEVICE_UNKNOWN, DEVICE_DIR };


//** Structures used for rebuilding the resource
#define REBUILD_FOUND_OSD 1
#define REBUILD_FOUND_DB  2
#define REBUILD_FOUND_HISTORY 4
#define REBUILD_FOUND_ALL 3     //** This is the 2 locations it's critical

typedef struct {
    osd_id_t id;
    int found;
    Allocation_t a;
} rebuild_lut_t;

typedef struct {                //** Internal resource iterator
    int mode;
    DB_iterator_t *dbi;
    osd_iter_t *fsi;
    Resource_t *r;
    Allocation_t a;
} res_iterator_t;


void *resource_cleanup_thread(apr_thread_t *th, void *data);
int _remove_allocation_for_make_free(Resource_t *r, int rmode, Allocation_t *alloc,
                                     DB_iterator_t *it);

//*****************************************************************************
// snap_prefix - Makes a snap prefix
//*****************************************************************************

char *snap_prefix(char *prefix, int add_time)
{
    char *p;
    char my_prefix[4096];
    char time_str[128];
    apr_time_exp_t now;
    apr_size_t n;

    p = (prefix) ? prefix : "snap";
    if (add_time) {
        apr_time_exp_lt(&now, apr_time_now());
        apr_strftime(time_str, &n, sizeof(time_str), "%F_%T", &now);
        snprintf(my_prefix, sizeof(my_prefix)-1, "%s-%s", p, time_str); my_prefix[sizeof(my_prefix)-1] = 0;
    } else {
        snprintf(my_prefix, sizeof(my_prefix)-1, "%s", p); my_prefix[sizeof(my_prefix)-1] = 0;
    }

    return(strdup(my_prefix));
}

//***************************************************************************
//** fetch_historykey - returns the history key for the LRU
//***************************************************************************

void fetch_historykey(void *global, void *object, void **key, int *len)
{
    lru_history_t *h = (lru_history_t *)object;

    *key = &(h->id);
    *len = sizeof(osd_id_t);
}

//***************************************************************************
// fetch_idkey - Routine to fetch the ID key for the allocation LRU
//***************************************************************************

void fetch_idkey(void *global, void *object, void **key, int *len)
{
    Allocation_t *a = (Allocation_t *)object;

    *key = &(a->id);
    *len = sizeof(osd_id_t);
}

//***************************************************************************
//  alru_put - Updates/adds the allocation to the LRU
//***************************************************************************

void alru_put(Resource_t *r, Allocation_t *a)
{
    tbx_lru_put(r->id_lru, a);
}

//***************************************************************************
//  alru_remove - Removes the allocation from the LRU
//***************************************************************************

void alru_remove(Resource_t *r, osd_id_t id)
{
    tbx_lru_delete(r->id_lru, &id, sizeof(id));
}

//***************************************************************************
//  fname2dev - Maps the file or directory to the physical device
//***************************************************************************

char *fname2dev(char *fname)
{
    FILE *fd;
    struct mntent minfo;
    char buffer[4096];
    char *apath, *dev;
    int len;

    apath = realpath(fname, NULL);
    dev = NULL;

    fd = setmntent("/etc/mtab", "r");
    assert_result_not_null(fd);

    while (getmntent_r(fd, &minfo, buffer, sizeof(buffer)) != NULL) {
        len = strlen(minfo.mnt_dir);
        if (strncmp(apath, minfo.mnt_dir, len) == 0) {
            if ((int) strlen(apath) > len) {
                if ((apath[len] == '/') || (minfo.mnt_dir[len - 1] == '/')) {
                    if (dev)
                        free(dev);
                    dev = strdup(minfo.mnt_fsname);
                }
            } else {
                if (dev)
                    free(dev);
                dev = strdup(minfo.mnt_fsname);
            }
        }
    }

    endmntent(fd);

    free(apath);

    return (dev);
}


//***************************************************************************
// trash_adjust - Adjusts the trash space
//     NOTE: No Locking is performed.
//***************************************************************************

void _trash_adjust(Resource_t *r, int rmode, osd_id_t id)
{
    int64_t fsize;
    int ind = 0;

    if (rmode != OSD_ID) {
        fsize = osd_size(r->dev, id);
        if (fsize > 0) {
            ind = (rmode == OSD_EXPIRE_ID) ? RES_EXPIRE_INDEX : RES_DELETE_INDEX;
            r->trash_size[ind] += fsize;
            r->n_trash[ind]++;
        }
        if (fsize < 0)
            fsize = 0;
        ibp_off_t dummy = fsize;
        log_printf(15, "_trash_adjust: id=" LU " * size=" LU " trash_size[%d]=" LU "\n", id, dummy,
                   ind, r->trash_size[ind]);
    }
}

//***************************************************************************
// mkfs_resource - Creates a new resource
//***************************************************************************

int mkfs_resource(rid_t rid, char *dev_type, char *device_name, char *db_location,
                  ibp_off_t max_bytes, int n_partitions)
{
    int err;
    char rname[256];
    char dname[2048];
    char fname[2048];
    char buffer[10 * 1024];
    int used;
    int n_cache = 100000;
    apr_time_t expire_time = apr_time_from_sec(30);
    DIR *dir = NULL;
    Resource_t res;
    char kgroup[1000], name[1000];
    struct statfs stat;


    memset(&res, 0, sizeof(Resource_t));

    if (strlen(db_location) > 1900) {
        printf("mkfs_resource: Can't make fname.  location and device too long\n");
        printf("mkfs_resource: DB location: %s\n", db_location);
        printf("mkfs_resource: device: %s\n", device_name);
        abort();
    }
    //*** Fill in defaults for everything ***
    snprintf(kgroup, sizeof(kgroup), "resource %s", ibp_rid2str(rid, rname));
    res.keygroup = kgroup;
//    res.keygroup = strdup(kgroup);
//WORKS
    res.name = ibp_rid2str(rid, name);
//return(0);
//BROKEN
    memcpy(&(res.rid), &rid, sizeof(rid_t));
    res.max_duration = 2592000; //default to 30 days
    assert_result(strcmp(dev_type, DEVICE_DIR), 0);
    res.device_type = dev_type;
    res.res_type = RES_TYPE_DIR;

    res.device = device_name;
    res.rwm_mode = RES_MODE_WRITE | RES_MODE_READ | RES_MODE_MANAGE;
    res.preallocate = 0;
    res.minfree = (ibp_off_t) 10 *1024 * 1024 * 1024;   //Default to 10GB free
    res.update_alloc = 0;
    res.remove_mangled = 0;
    res.enable_read_history = 1;
    res.enable_write_history = 1;
    res.enable_manage_history = 1;
    res.enable_alias_history = 1;
    res.enable_history_update_on_delete = 1;
    res.cleanup_interval = 600;
    res.trash_grace_period[RES_DELETE_INDEX] = 2 * 3600;
    res.trash_grace_period[RES_EXPIRE_INDEX] = 14 * 24 * 3600;
    res.preexpire_grace_period = 24 * 3600;
    res.restart_grace_period = 2 * 24 * 3600;
    res.n_partitions = n_partitions;
    res.n_history = 32;
    res.n_lru = 100000;
    res.lru_history_bytes = sizeof(lru_history_t) + 3*res.n_history*sizeof(apr_time_t);
    res.rescan_interval = 24 * 3600;
    res.chksum_blocksize = 64 * 1024;
    res.enable_chksum = 1;
    tbx_chksum_set(&(res.chksum), CHKSUM_MD5);
    res.n_cache = 100000;
    res.cache_expire = apr_time_from_sec(30);

    //**Make the directory for the DB if needed
    snprintf(dname, sizeof(dname), "%s", db_location);
    mkdir(dname, S_IRWXU);
    assert_result_not_null(dir = opendir(dname));       //Make sure I can open it
    closedir(dir);

    //**Create the DB
    snprintf(fname, sizeof(fname), "db %s", ibp_rid2str(rid, rname));
    assert_result(mkfs_db(&(res.db), dname, fname, NULL, res.n_partitions), 0);

    //**Create the device
    if (strcmp("dir", dev_type) == 0) {
        res.res_type = RES_TYPE_DIR;
        assert_result_not_null(res.dev = osd_mount_fs(res.device, n_cache, res.n_partitions, expire_time));

        if (max_bytes == 0) {
            statfs(device_name, &stat);
            max_bytes = stat.f_bavail;
            max_bytes = max_bytes * (ibp_off_t) stat.f_bsize;
        }
    }

    res.max_size[ALLOC_HARD] = max_bytes;
    res.max_size[ALLOC_SOFT] = max_bytes;
    res.max_size[ALLOC_TOTAL] = max_bytes;
    res.used_space[ALLOC_HARD] = 0;
    res.used_space[ALLOC_SOFT] = 0;
    res.n_allocs = 0;
    res.n_alias = 0;

    err = create_history_table(&res);
    if (err != 0) {
        printf("mkfs_resource: Can't create the history table.  err=%d\n", err);
        abort();
    }
    //*** Print everything out to the screen for the user to use ***
    used = 0;
    print_resource(buffer, &used, sizeof(buffer), &res);
    printf("%s", buffer);

    umount_db(&(res.db));
    umount_history_table(&res);
    osd_umount(res.dev);

    return (0);
}

//***************************************************************************
//  check_snap_merge - Add any informtaion available from the merge DB if used
//***************************************************************************

void check_snap_merge(Resource_t *r, rebuild_lut_t *d)
{
    char buf[r->lru_history_bytes];
    lru_history_t *lh = (lru_history_t *)buf;
    Allocation_t a;

    //** Just blindly copy over the merge history info
    //** Dups should be the same so no harm done just some wasted ops
    //** Remember this is just called when we don't have all the fields in the startup rebuild
    if (r->db_merge_valid) lru_history_populate_merge(r, d->id);

    //** This will properly delete the old records
    lru_history_populate(r, d->id, lh);

    //** If we're missing the actual allocation in the primary DB add it if possible
    if (r->db_merge_valid) {
        if (!(d->found & REBUILD_FOUND_DB)) {
            if (_get_alloc_with_id_db(&(r->db_merge), d->id, &a) == 0) {
                d->a = a;
            }
        }
    }
}

//***************************************************************************
//  allocation_sanity_check - Sanity checks the allocation
//***************************************************************************

int allocation_sanity_check(Resource_t *r, osd_id_t id, Allocation_t *a)
{
    if (id != a->id) return(1);

    switch(a->type) {
        case (IBP_BYTEARRAY): break;
        case (IBP_BUFFER): break;
        case (IBP_FIFO): break;
        case (IBP_CIRQ): break;
        default: return(2);
    }

    switch(a->reliability) {
        case (ALLOC_SOFT) : break;
        case (ALLOC_HARD) : break;
        default: return(3);
    }

    if (a->r_pos > a->size) return(4);
    if (a->w_pos > a->size) return(5);
    if (a->read_refcount < 0) return(6);
    if (a->write_refcount < 0) return(7);

    if (a->is_alias > 1) return(8);

    return(0);
}

//***************************************************************************
// rebuild_populate_partition_lut_with_osd - Populates the LUT for the partition
//     using OSD data
//***************************************************************************

void rebuild_populate_partition_lut_with_osd(Resource_t *r, int partition, apr_hash_t *lut, apr_pool_t *mpool)
{
    osd_iter_t *it;
    rebuild_lut_t *d;
    int err;
    osd_id_t id;

    it = osd_new_iterator(r->dev, partition);

    while ((err = osd_iterator_next(it, &id)) == 0) {
        if (id == RES_CHECK_ID) continue;   //** Skip the check allocation
        d = apr_hash_get(lut, &id, sizeof(id));
        if (!d) {
            d = apr_palloc(mpool, sizeof(rebuild_lut_t));
            d->id = id;
            d->found = 0;
        }

        d->found |= REBUILD_FOUND_OSD;
        apr_hash_set(lut, &(d->id), sizeof(id), d);
    }
    osd_destroy_iterator(it);
}

//***************************************************************************
// rebuild_populate_partition_lut_with_db - Populates the LUT for the partition
//     using DB data
//***************************************************************************

void rebuild_populate_partition_lut_with_db(Resource_t *r, int partition, apr_hash_t *lut, apr_pool_t *mpool)
{
    DB_iterator_t *it;
    rebuild_lut_t *d;
    Allocation_t a;

    it = id_iterator(&(r->db));
    if (set_id_iterator(it, partition) != 0) goto finished;

    while (db_iterator_next(it, DBR_NEXT, &a) == 0) {
        //** Kick out if changed partition.
        if ((a.id % r->n_partitions) != (unsigned int)partition) goto finished;

        d = apr_hash_get(lut, &(a.id), sizeof(osd_id_t));
        if (!d) {
            d = apr_palloc(mpool, sizeof(rebuild_lut_t));
            d->id = a.id;
            d->found = 0;
        }

        d->a = a;  //** We get the allocation header from the DB and only get the ID from the OSD

        d->found |= REBUILD_FOUND_DB;
        apr_hash_set(lut, &(d->id), sizeof(osd_id_t), d);
    }

finished:
    db_iterator_end(it);
}

//***************************************************************************
// rebuild_populate_partition_lut_with_history - Populates the LUT for the partition
//     using the history data
//***************************************************************************

void rebuild_populate_partition_lut_with_history(Resource_t *r, int partition, apr_hash_t *lut, apr_pool_t *mpool)
{
    rocksdb_iterator_t *it;
    char buf[r->lru_history_bytes];
    lru_history_t *lh = (lru_history_t *)buf;
    db_history_key_t *key;
    db_history_key_t base_key;
    rebuild_lut_t *d;
    size_t nbytes;
    osd_id_t id;
    tbx_stack_t *stack = NULL;  //** This is used for anything we need to delete

    //** Create the iterator
    it = rocksdb_create_iterator(r->db.history, r->db.ropts);
    rocksdb_iter_seek(it, db_fill_history_key(&base_key, partition, 0, 0), sizeof(base_key));

    while (rocksdb_iter_valid(it) > 0) {
        nbytes = 0;
        key = (db_history_key_t *)rocksdb_iter_key(it, &nbytes);
        if (nbytes == 0) break;
        id = key->id; //** Snag the ID for later use. key gits overwritten below
        if ((id % r->n_partitions) != (unsigned int)partition) {   //** Kick out if a different partition
           break;
        }

        if (id == RES_CHECK_ID) continue;   //** Skip the check allocation

        lru_history_populate_core(r, id, lh, &stack, it);  //** This uses the iterator and hence overwrites the iterator key

        d = apr_hash_get(lut, &id, sizeof(key->id));
        if (!d) {
            d = apr_palloc(mpool, sizeof(rebuild_lut_t));
            d->id = id;
            d->found = 0;
        }

        //** Flag it as found.  If this isn't a viable ID then it will get removed in the merge process
        d->found |= REBUILD_FOUND_HISTORY;
        apr_hash_set(lut, &(d->id), sizeof(d->id), d);
    }

    //** Cleanup
    rocksdb_iter_destroy(it);

    //** See if we have to delete something
    if (stack) lru_history_populate_remove(r, stack);
}

//***************************************************************************
// rebuild_remove - Removes an allocation during the rebuild process
//***************************************************************************

void rebuild_remove(Resource_t *r, rebuild_lut_t *d)
{

    log_printf(1, "rid=%s Removing id=" LU " d->a.id=" LU " found=%d\n", r->name, d->id, d->a.id, d->found);

    d->a.id = d->id;  //** Make sure we use the correct ID since this is used to filter out mangled allocations

    if (d->found & REBUILD_FOUND_OSD) {
        if (r->remove_mangled == 1) {
            _trash_adjust(r, OSD_EXPIRE_ID, d->id);
            osd_remove(r->dev, OSD_EXPIRE_ID, d->id);
        } else {
            log_printf(0, "Skipping OSD removal id=" LU "\n", d->id);
        }
    }

    if (d->found & REBUILD_FOUND_DB) {
       remove_alloc_db(&(r->db), &(d->a));
    }

    if (d->found & REBUILD_FOUND_HISTORY) db_delete_history(r, d->id);  //** And the history
}

//***************************************************************************
// rebuild_add - Adds an allocation during the rebuild process
//***************************************************************************

void rebuild_add(Resource_t *r, rebuild_lut_t *d, ibp_time_t old_expiration, int update_alloc)
{
    if ((!(d->found & REBUILD_FOUND_DB)) || update_alloc || (d->a.expiration != old_expiration)) {
        _put_alloc_db(&(r->db), &(d->a), old_expiration);
    } else {
        rebuild_add_expiration_db(&(r->db), &(d->a));
    }

    if ((r->update_alloc == 1) || (update_alloc == 1)) {
        write_allocation_header(r, &(d->a), 0);
    }

    alru_put(r, &(d->a)); //** and the LRU copy

    //** Adjust the space
    r->n_allocs++;
    if (d->a.is_alias == 0) {
        r->used_space[d->a.reliability] += d->a.max_size;
    } else {
        r->n_alias++;
    }
}

//***************************************************************************
// rebuild_populate_partition_lut_process - Processes the LUT
//***************************************************************************

void rebuild_populate_partition_lut_process(Resource_t *r, apr_hash_t *lut, int remove_expired, int truncate_expiration)
{
    apr_hash_index_t *hi;
    apr_ssize_t hlen;
    rebuild_lut_t *d;
    ibp_time_t max_expiration, old_expiration, now, t1, t2;
    int update_alloc;

    now = ibp_time_now();
    max_expiration = now + r->max_duration;

    //** Cycle over all the id's
    for (hi=apr_hash_first(NULL, lut); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&d);

        //** Fetch the record or cleanup
        if ((d->found & REBUILD_FOUND_ALL) == REBUILD_FOUND_ALL) goto got_it;

        //** If we made it here then in some combination of OSD|DB|History
        //** If it's not in the OSD then we always delete it
        if (d->found & REBUILD_FOUND_OSD)  {  //** It's in the OSD but not the DB
            int bad = read_allocation_header(r, d->id, &(d->a));
            if (bad == 0) {
                bad = allocation_sanity_check(r, d->id, &(d->a));
            } else {
                bad = -1;
            }
            if (bad != 0) { // Failed reading the allocation or it's mangled
                log_printf(1, "(rid=%s) Removing mangled allocation with id: " LU " err=%d\n", r->name, d->id, bad);
                rebuild_remove(r, d);
                continue;
            }

            check_snap_merge(r, d);
            if (d->a.expiration < now) d->a.expiration = now + r->restart_grace_period;
        } else { //** Missing from the OSD so delete it from the DB
            rebuild_remove(r, d);
            continue;  //** Move on to the next one
        }

got_it:  //** Got a valid allocation so see if we add it
        //** Check if we need to correct the size
        update_alloc = 0;
        if (d->a.size > d->a.max_size) {
            log_printf(1, "(rid=%s) Correcting allocation size with id: " LU " curr_size=" LU " max_size=" LU "\n", r->name, d->id, d->a.size, d->a.max_size);
            update_alloc = 1;
            d->a.size = d->a.max_size;
            if (d->a.w_pos >= d->a.size) d->a.w_pos = d->a.size-1;
            if (d->a.r_pos >= d->a.size) d->a.r_pos = d->a.size-1;
            osd_truncate(r->dev, d->id, d->a.max_size + ALLOC_HEADER);
        }

        if ((d->a.expiration < now) && (remove_expired == 1)) {
            log_printf(1,
                       "(rid=%s) Removing expired record with id: " LU "\n", r->name, d->a.id);
            rebuild_remove(r, d);
        } else {                //*** Adding the record
            old_expiration = d->a.expiration;
            if ((d->a.expiration > max_expiration) && (truncate_expiration == 1)) {
                t1 = d->a.expiration;
                t2 = max_expiration;
                log_printf(1,
                           "(rid=%s) Truncating expiration for id id: " LU " is_alias=%d"
                           " Old expiration:" TT " * new:" TT "\n",
                           r->name, d->a.id, d->a.is_alias, ibp2apr_time(t1), ibp2apr_time(t2));
                d->a.expiration = max_expiration;
            }

            log_printf(1, "(rid=%s) Adding record " LU " with id: " LU " is_alias=%d location=%d\n", r->name, r->n_allocs, d->a.id, d->a.is_alias, d->found);
            rebuild_add(r, d, old_expiration, update_alloc);
        }
    }

}

//***************************************************************************
// rebuild_resource - Rebuilds the resource
//***************************************************************************

int rebuild_resource(Resource_t *r, tbx_inip_file_t *kfd, int remove_expired,
                     int wipe_clean, int truncate_expiration)
{
    char ppbuf[128];
    apr_pool_t *mpool;
    apr_hash_t *lut;
    int i;

    apr_ctime(ppbuf, apr_time_now());
    log_printf(0,
               "(rid=%s) Rebuilding Resource rid=%s.  Starting at %s  remove_expired=%d wipe_clean=%d truncate_expiration=%d\n",
               r->name, r->name, ppbuf, remove_expired, wipe_clean, truncate_expiration);

    //*** Now we have to fill it ***
    r->used_space[0] = 0;
    r->used_space[1] = 0;
    r->n_allocs = 0;
    r->n_alias = 0;

    for (i=0; i<r->n_partitions; i++) {  //** Iterate over the partitions
        //** Create the LUT
        apr_pool_create(&mpool, NULL);
        lut = apr_hash_make(mpool);

        //** Populate the LUT
        rebuild_populate_partition_lut_with_osd(r, i, lut, mpool);
        rebuild_populate_partition_lut_with_db(r, i, lut, mpool);
        rebuild_populate_partition_lut_with_history(r, i, lut, mpool);

        //** Process it
        rebuild_populate_partition_lut_process(r, lut, remove_expired, truncate_expiration);

        //** Destroy the LUT
        apr_pool_destroy(mpool);
    }


    //** Print the summary
    log_printf(0, "\n(rid=%s) " LU " allocations added\n", r->name, r->n_allocs);
    log_printf(0, "(rid=%s) " LU " alias allocations added\n", r->name, r->n_alias);
    log_printf(0, "#(rid=%s) soft_used = %s\n", r->name, tbx_stk_pretty_print_double_with_scale(1000, r->used_space[ALLOC_SOFT], ppbuf));
    log_printf(0, "#(rid=%s) hard_used = %s\n", r->name, tbx_stk_pretty_print_double_with_scale(1000, r->used_space[ALLOC_HARD], ppbuf));
    apr_ctime(ppbuf, apr_time_now());
    log_printf(0, "\n(rid=%s) Finished Rebuilding RID %s at %s\n", r->name,
               r->name, ppbuf);
    tbx_log_flush();

    return(0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// parse_resource - Parses the resource Keyfile
//***************************************************************************

int parse_resource(Resource_t *res, tbx_inip_file_t *keyfile, char *group)
{
    char *str, *str2, *bstate;
    int i, fin;

    res->keygroup = strdup(group);
    res->name = tbx_inip_get_string(keyfile, group, "rid", NULL);
    if (res->name == NULL) {
        printf("parse_resource: (%s) Missing resource ID\n", group);
        abort();
    }
    ibp_str2rid(res->name, &(res->rid));

    res->preallocate = tbx_inip_get_integer(keyfile, group, "preallocate", 0);
    res->update_alloc = tbx_inip_get_integer(keyfile, group, "update_alloc", 1);
    res->remove_mangled = tbx_inip_get_integer(keyfile, group, "remove_mangled", 0);
    res->enable_write_history = tbx_inip_get_integer(keyfile, group, "enable_write_history", 1);
    res->enable_read_history = tbx_inip_get_integer(keyfile, group, "enable_read_history", 1);
    res->enable_manage_history = tbx_inip_get_integer(keyfile, group, "enable_manage_history", 1);
    res->enable_alias_history = tbx_inip_get_integer(keyfile, group, "enable_alias_history", 1);
    res->enable_history_update_on_delete = tbx_inip_get_integer(keyfile, group, "enable_history_update_on_delete", 1);

    res->rescan_interval = tbx_inip_get_integer(keyfile, group, "rescan_interval", 86400);
    res->cleanup_interval = tbx_inip_get_integer(keyfile, group, "cleanup_interval", 500);

    res->trash_grace_period[RES_DELETE_INDEX] =
        tbx_inip_get_integer(keyfile, group, "delete_grace_period", 3600);
    res->trash_grace_period[RES_EXPIRE_INDEX] =
        tbx_inip_get_integer(keyfile, group, "expire_grace_period", 7 * 24 * 3600);
    res->preexpire_grace_period =
        tbx_inip_get_integer(keyfile, group, "preexpire_grace_period", 24 * 3600);

    res->restart_grace_period =
        tbx_inip_get_integer(keyfile, group, "restart_grace_period", 2 * 24 * 3600);

    res->n_partitions = tbx_inip_get_integer(keyfile, group, "n_partitions", 256);
    res->n_history = tbx_inip_get_integer(keyfile, group, "n_history", 16);
    res->lru_history_bytes = sizeof(lru_history_t) + 3*res->n_history*sizeof(apr_time_t);

    res->max_duration = tbx_inip_get_integer(keyfile, group, "max_duration", 0);
    if (res->max_duration == 0) {
        printf("parse_resource: (%s) Missing max duration: %d\n", group, res->max_duration);
        abort();
    }

    res->device_type = tbx_inip_get_string(keyfile, group, "resource_type", NULL);
    if (strcmp(res->device_type, DEVICE_DIR) != 0) {
        res->res_type = RES_TYPE_DIR;
        printf("parse_resource: (%s) Invalid device type: %s\n", group, res->device_type);
        abort();
    }

    res->device = tbx_inip_get_string(keyfile, group, "device", NULL);
    if (res->device == NULL) {
        printf("parse_resource: (%s) Missing resource device\n", group);
        abort();
    }

    str = tbx_inip_get_string(keyfile, group, "max_size", NULL);
    if (str == NULL) {
        printf("parse_resource: (%s) Missing max_size for resource\n", group);
        abort();
    }
    sscanf(str, "" I64T "", &(res->max_size[ALLOC_TOTAL]));
    res->max_size[ALLOC_TOTAL] *= 1024 * 1024;
    free(str);

    str = tbx_inip_get_string(keyfile, group, "soft_size", NULL);
    if (str == NULL) {
        printf("parse_resource: (%s) Missing soft_size for resource\n", group);
        abort();
    }
    sscanf(str, "" I64T "", &(res->max_size[ALLOC_SOFT]));
    res->max_size[ALLOC_SOFT] *= 1024 * 1024;
    free(str);

    str = tbx_inip_get_string(keyfile, group, "hard_size", NULL);
    if (str == NULL) {
        printf("parse_resource: (%s) Missing hard_size for resource\n", group);
        abort();
    }
    sscanf(str, "" I64T "", &(res->max_size[ALLOC_HARD]));
    res->max_size[ALLOC_HARD] *= 1024 * 1024;
    free(str);

    str = tbx_inip_get_string(keyfile, group, "minfree_size", NULL);
    if (str == NULL) {
        printf("parse_resource: (%s) Missing minfreesize for resource\n", group);
        abort();
    }
    sscanf(str, "" I64T "", &(res->minfree));
    res->minfree *= 1024 * 1024;
    free(str);

    res->enable_chksum = tbx_inip_get_integer(keyfile, group, "enable_chksum", 0);
    res->chksum_blocksize = tbx_inip_get_integer(keyfile, group, "chksum_blocksize_kb", 64);
    if (res->chksum_blocksize > 32768) {
        log_printf(0, "parse_resource(%s): chksumblocksize_kb > 32768.  Got " I64T "\n", group,
                   res->chksum_blocksize);
        abort();
    }
    res->chksum_blocksize *= 1024;      //** Convert it to bytes

    str = tbx_inip_get_string(keyfile, group, "chksum_type", "SHA256");
    i = tbx_chksum_type_name(str);
    if (i != -1) {
        tbx_chksum_set(&(res->chksum), i);
        if (i == CHKSUM_NONE)
            res->enable_chksum = 0;     //** If none disable disk chskum check
    } else {
        log_printf(0,
                   "parse_resource(%s): Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n",
                   group, str);
        abort();
    }
    free(str);

    //** Get the cache information
    res->n_cache = tbx_inip_get_integer(keyfile, group, "n_cache", 100000);
    res->cache_expire = tbx_inip_get_integer(keyfile, group, "cache_expire", 30);
    res->cache_expire = apr_time_from_sec(res->cache_expire);

    //** Get the rwm_mode
    str = tbx_inip_get_string(keyfile, group, "mode", "read,write,manage");
    str2 = tbx_stk_string_token(str, " ,:|", &bstate, &fin);
    res->rwm_mode = 0;
    fin = 0;
    while (fin == 0) {
        if (strcasecmp(str2, "read") == 0) {
            res->rwm_mode |= RES_MODE_READ;
        } else if (strcasecmp(str2, "write") == 0) {
            res->rwm_mode |= RES_MODE_WRITE;

        } else if (strcasecmp(str2, "manage") == 0) {
            res->rwm_mode |= RES_MODE_MANAGE;
        }

        str2 = tbx_stk_string_token(NULL, " ,:|", &bstate, &fin);
    }
    free(str);

    //** Setup the LRU table
    res->n_lru = tbx_inip_get_integer(keyfile, group, "n_lru", 100000);
    res->id_lru = tbx_lru_create(res->n_lru, fetch_idkey, tbx_lru_clone_default, tbx_lru_copy_default, tbx_lru_free_default, (void *)(&alloc_lru_size));
    res->history_lru = tbx_lru_create(res->n_lru, fetch_historykey, tbx_lru_clone_default, tbx_lru_copy_default, tbx_lru_free_default, (void *)&(res->lru_history_bytes));

    return (0);
}

//***************************************************************************
// mount_resource - Mounts a resource for use
//***************************************************************************

int mount_resource(Resource_t *res, tbx_inip_file_t *keyfile, char *group,
                   int force_rebuild, int lazy_allocate, int truncate_expiration, char *start_snap_prefix, char *merge_snap)
{
    int  err, wipe_expired;
    char db_group[1024];
    char *db_snap_merge;

    memset(_blanks, 0, _RESOURCE_BUF_SIZE * sizeof(char));      //** This is done multiple times and it doesn't have to be but is trivial
    memset(res, 0, sizeof(Resource_t));

    res->start_time = ibp_time_now();   //** Track when we were added.

    //*** Load the resource data ***
    assert_result(parse_resource(res, keyfile, group), 0);

    wipe_expired = (res->preexpire_grace_period == 0) ? 1 : 0;  //** Only wipe the expired allocs on rebuild if no grace period

    res->data_pdev = fname2dev(res->device);
    res->pending = 0;

    log_printf(15, "mount_resource: rid=%s force_rebuild=%d device=%s\n", res->name, force_rebuild,
               res->device);

    res->lazy_allocate = lazy_allocate;

    //*** Now mount the device ***
    if (strcmp(DEVICE_DIR, res->device_type) == 0) {
        DIR *dir = NULL;
        assert_result_not_null(dir = opendir(res->device));
        closedir(dir);

        res->res_type = RES_TYPE_DIR;
        assert_result_not_null(res->dev =
                                   osd_mount_fs(res->device, res->n_cache, res->n_partitions, res->cache_expire));
    }
    //** Init the lock **
    apr_pool_create(&(res->pool), NULL);
    apr_thread_mutex_create(&(res->mutex), APR_THREAD_MUTEX_DEFAULT, res->pool);
    apr_thread_mutex_create(&(res->cleanup_lock), APR_THREAD_MUTEX_DEFAULT, res->pool);
    apr_thread_cond_create(&(res->cleanup_cond), res->pool);

    res->cleanup_shutdown = -1;

    //** Rebuild the DB or mount it here **
    snprintf(db_group, sizeof(db_group), "db %s", res->name);

    err = mount_db_generic(keyfile, db_group, &(res->db), force_rebuild, res->n_partitions, NULL);
    if (err != 0) {
        log_printf(0, "mount_resource:  Error mounting DB force_rebuild=%d! res=%s err=%d\n", force_rebuild, res->name, err);
        return (err);
    }

    //** Make the preemptive snap before making any changes
    snap_resource(res, start_snap_prefix, stderr);

    //** Now figure out which snap to use for the merge
    db_snap_merge = (merge_snap) ? strdup(merge_snap) : snap_merge_pick(&res->db);

    //** Mount the snap if needed
    err = (db_snap_merge) ? mount_db_generic(keyfile, db_group, &(res->db_merge), 0, res->n_partitions, db_snap_merge) : 1;
    res->db_merge_valid = (err==0) ? 1 : 0;

    if (res->db_merge_valid) {
        log_printf(1, "(rid=%s) snap_merge=%s\n", res->name, db_snap_merge);
    }
    //** Construct the interal tables and fix any issues
    err = rebuild_resource(res, keyfile, wipe_expired, force_rebuild, truncate_expiration);

    //** We can tear down the merge DBs
    if (res->db_merge_valid) umount_db(&(res->db_merge));
    if (db_snap_merge) free(db_snap_merge);

    log_printf(15, "mount_resource: err=%d  res=%s cleanup_shutdown=%d\n", err,
               res->name, res->cleanup_shutdown);
    tbx_log_flush();

    if (err != 0) return (err);

    err = mount_history_table(res);
    if (err != 0) {
        log_printf(0, "mount_resource:  Error mounting history table! res=%s err=%d\n", res->name,
                   err);
        return (err);
    }

    if (err != 0)
        return (err);

    log_printf(15, "mount_resource: END res=%s cleanup_shutdown=%d\n", res->name,
               res->cleanup_shutdown);
    tbx_log_flush();

    return (err);
}

//***************************************************************************
// umount_resource - Unmounts the given resource
//***************************************************************************

int umount_resource(Resource_t *res)
{
    apr_status_t dummy;
    log_printf(15, "umount_resource:  Unmounting resource %s cleanup_shutdown=%d\n", res->name,
               res->cleanup_shutdown);
    tbx_log_flush();

    //** Kill the cleanup thread
    if (res->cleanup_shutdown == 0) {
        apr_thread_mutex_lock(res->cleanup_lock);
        res->cleanup_shutdown = 1;
        apr_thread_cond_signal(res->cleanup_cond);
        apr_thread_mutex_unlock(res->cleanup_lock);
        apr_thread_join(&dummy, res->cleanup_thread);
    }

    umount_db(&(res->db));
    umount_history_table(res);

    osd_umount(res->dev);

    tbx_lru_destroy(res->id_lru);
    tbx_lru_destroy(res->history_lru);

    apr_thread_mutex_destroy(res->cleanup_lock);
    apr_thread_mutex_destroy(res->mutex);
    apr_thread_cond_destroy(res->cleanup_cond);
    //** The threadattr is destroyed via the pool.  APR has no attr destroy call:(
    apr_pool_destroy(res->pool);

    free(res->name);
    free(res->keygroup);
    free(res->device);
    if (res->data_pdev) free(res->data_pdev);
    if (res->device_type != NULL) free(res->device_type);

    return (0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// print_resource - Prints the resource information out to fd.
//***************************************************************************

int print_resource(char *buffer, int *used, int nbytes, Resource_t *res)
{
    int i;
    ibp_off_t n;
    char string[1024];

    tbx_append_printf(buffer, used, nbytes, "[%s]\n", res->keygroup);
    tbx_append_printf(buffer, used, nbytes, "rid = %s\n", res->name);
    tbx_append_printf(buffer, used, nbytes, "max_duration = %d\n", res->max_duration);
    tbx_append_printf(buffer, used, nbytes, "resource_type = %s\n", res->device_type);
    tbx_append_printf(buffer, used, nbytes, "device = %s\n", res->device);
    tbx_append_printf(buffer, used, nbytes, "update_alloc = %d\n", res->update_alloc);
    tbx_append_printf(buffer, used, nbytes, "remove_mangled = %d\n", res->remove_mangled);
    tbx_append_printf(buffer, used, nbytes, "n_lru = %d\n", res->n_lru);
    tbx_append_printf(buffer, used, nbytes, "n_partitions = %d\n", res->n_partitions);

    string[0] = '\0';
    strcat(string, "mode = ");
    i = 0;
    if ((res->rwm_mode & RES_MODE_READ) > 0) {
        if (i == 1)
            strcat(string, ",");
        strcat(string, "read");
        i = 1;
    }
    if ((res->rwm_mode & RES_MODE_WRITE) > 0) {
        if (i == 1)
            strcat(string, ",");
        strcat(string, "write");
        i = 1;
    }
    if ((res->rwm_mode & RES_MODE_MANAGE) > 0) {
        if (i == 1)
            strcat(string, ",");
        strcat(string, "manage");
        i = 1;
    }
    tbx_append_printf(buffer, used, nbytes, "%s\n", string);

    tbx_append_printf(buffer, used, nbytes, "n_history = %d\n", res->n_history);
    tbx_append_printf(buffer, used, nbytes, "enable_history_update_on_delete = %d\n", res->enable_history_update_on_delete);
    tbx_append_printf(buffer, used, nbytes, "enable_read_history = %d\n", res->enable_read_history);
    tbx_append_printf(buffer, used, nbytes, "enable_write_history = %d\n",
                      res->enable_write_history);
    tbx_append_printf(buffer, used, nbytes, "enable_manage_history = %d\n",
                      res->enable_manage_history);
    tbx_append_printf(buffer, used, nbytes, "enable_alias_history = %d\n",
                      res->enable_alias_history);
    tbx_append_printf(buffer, used, nbytes, "cleanup_interval = %d\n", res->cleanup_interval);
    tbx_append_printf(buffer, used, nbytes, "rescan_interval = %d\n", res->rescan_interval);
    tbx_append_printf(buffer, used, nbytes, "delete_grace_period = %d\n",
                      res->trash_grace_period[RES_DELETE_INDEX]);
    tbx_append_printf(buffer, used, nbytes, "expire_grace_period = %d\n",
                      res->trash_grace_period[RES_EXPIRE_INDEX]);
    tbx_append_printf(buffer, used, nbytes, "preexpire_grace_period = %d\n",
                      res->preexpire_grace_period);
    tbx_append_printf(buffer, used, nbytes, "restart_grace_period = %d\n",
                      res->restart_grace_period);

    n = res->max_size[ALLOC_TOTAL] / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "max_size = " I64T "\n", n);
    n = res->max_size[ALLOC_SOFT] / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "soft_size = " I64T "\n", n);
    n = res->max_size[ALLOC_HARD] / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "hard_size = " I64T "\n", n);
    n = res->minfree / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "minfree_size = " I64T "\n", n);
    tbx_append_printf(buffer, used, nbytes, "preallocate = %d\n", res->preallocate);

    tbx_append_printf(buffer, used, nbytes, "enable_chksum = %d\n", res->enable_chksum);
    tbx_append_printf(buffer, used, nbytes, "chksum_type = %s\n", tbx_chksum_name(&(res->chksum)));
    n = res->chksum_blocksize / 1024;
    tbx_append_printf(buffer, used, nbytes, "chksum_blocksize_kb = " I64T "\n", n);

    n = apr_time_sec(res->cache_expire);
    tbx_append_printf(buffer, used, nbytes, "n_cache = %d\n", res->n_cache);
    tbx_append_printf(buffer, used, nbytes, "cache_expire = %d\n", n);

    tbx_append_printf(buffer, used, nbytes, "\n");

    print_db_resource(buffer, used, nbytes, &(res->db));
    tbx_append_printf(buffer, used, nbytes, "\n");

    n = res->used_space[ALLOC_SOFT] / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "#soft_used = " I64T " mb\n", n);
    n = res->used_space[ALLOC_HARD] / 1024 / 1024;
    tbx_append_printf(buffer, used, nbytes, "#hard_used = " I64T " mb\n", n);
    n = res->used_space[ALLOC_SOFT];
    tbx_append_printf(buffer, used, nbytes, "#soft_used = " I64T " b\n", n);
    n = res->used_space[ALLOC_HARD];
    tbx_append_printf(buffer, used, nbytes, "#hard_used = " LU " b\n", n);

    tbx_append_printf(buffer, used, nbytes, "#n_allocations = " LU "\n", res->n_allocs);
    tbx_append_printf(buffer, used, nbytes, "#n_alias = " LU "\n", res->n_alias);
    i = tbx_append_printf(buffer, used, nbytes, "\n");
    return (i);
}

//---------------------------------------------------------------------------

//***************************************************************************
// snap_resource - Does a DB snap on the resource
//***************************************************************************

int snap_resource(Resource_t *r, char *prefix, FILE *fd)
{
    int err;

    apr_thread_mutex_lock(r->mutex);
    err = snap_db(&(r->db), prefix, fd);
    apr_thread_mutex_unlock(r->mutex);

    return(err);
}

//***************************************************************************
//  resource_get_mode - Returns the RWM mode
//***************************************************************************

int resource_get_mode(Resource_t *r)
{
    int mode;

    apr_thread_mutex_lock(r->mutex);
    mode = r->rwm_mode;
    apr_thread_mutex_unlock(r->mutex);

    return (mode);
}

//***************************************************************************
//  resource_set_mode - Sets the RWM mode
//***************************************************************************

int resource_set_mode(Resource_t *r, int mode)
{
    apr_thread_mutex_lock(r->mutex);
    r->rwm_mode = mode;
    apr_thread_mutex_unlock(r->mutex);

    return (0);
}


//***************************************************************************
//  resource_get_corrupt_count - Retreives the number of corrupt allocations
//***************************************************************************

int resource_get_corrupt_count(Resource_t *r)
{
    return (osd_get_corrupt_count(r->dev));
}

//***************************************************************************
//  get_allocation_state - Retreives the allocations current state
//***************************************************************************

int get_allocation_state(Resource_t *r, osd_fd_t *fd)
{
    return (osd_get_state(r->dev, fd));
}

//***************************************************************************
// open_allocation - Opens an allocation for use
//***************************************************************************

osd_fd_t *open_allocation(Resource_t *r, osd_id_t id, int mode)
{
    osd_fd_t *fd = osd_open(r->dev, id, mode);
    if (fd == NULL) {
        log_printf(0, "open_allocation: Error with open_allocation(-res-, " LU ", %d) = %d\n", id,
                   mode, errno);
        return (NULL);
    }

    tbx_atomic_inc(r->counter);
    return (fd);
}

//***************************************************************************
// close_allocation - Opens an allocation for use
//***************************************************************************

int close_allocation(Resource_t *r, osd_fd_t *fd)
{
    tbx_atomic_inc(r->counter);

    return (osd_close(r->dev, fd));
}

//***************************************************************************
// _remove_allocation - Removes the given allocation without locking!
//***************************************************************************

int _remove_allocation(Resource_t *r, int rmode, Allocation_t *alloc, int dolock)
{
    int err;

    log_printf(10, "_remove_allocation:  Removing " LU "\n", alloc->id);

    //** EVen if this fails we want to try and remove the physical allocation
    if ((err = remove_alloc_db(&(r->db), alloc)) != 0) {
        debug_printf(1, "_remove_allocation:  Error with remove_alloc_db!  Error=%d\n", err);
//      return(err);
    }
    log_printf(10, "_remove_allocation:  Removed db entry\n");

    db_delete_history(r, alloc->id); //** Remove the history

    alru_remove(r, alloc->id); //** Update the LRU copy

    if (dolock)
        apr_thread_mutex_lock(r->mutex);

    _trash_adjust(r, rmode, alloc->id);

    if (r->enable_alias_history == 1) {
        if ((err = osd_remove(r->dev, rmode, alloc->id)) != 0) {
            debug_printf(1, "_remove_allocation:  Error with fs->remove!  Error=%d\n", err);
        }
    } else if (alloc->is_alias == 0) {
        if ((err = osd_remove(r->dev, rmode, alloc->id)) != 0) {
            debug_printf(1, "_remove_allocation:  Error with fs->remove!  Error=%d\n", err);
        }
    } else {
        log_printf(15, "_remove_allocation:  a->is_alias=1.  Skipping fs->remove().\n");
    }

    debug_printf(10, "_remove_allocation: After remove\n");

    log_printf(15, "_remove_allocation: start rel=%d used=" LU " a.max_size=" LU "\n",
               alloc->reliability, r->used_space[alloc->reliability], alloc->max_size);

    r->n_allocs--;
    if (alloc->is_alias == 0) {
        r->used_space[alloc->reliability] -= alloc->max_size;   //** Upodate the amount of space used
    } else {
        r->n_alias--;
    }

    log_printf(15, "_remove_allocation: end rel=%d used=" LU " a.max_size=" LU "\n",
               alloc->reliability, r->used_space[alloc->reliability], alloc->max_size);

    if (dolock)
        apr_thread_mutex_unlock(r->mutex);

    debug_printf(10, "_remove_allocation: end of routine\n");

    return (0);
}

//***************************************************************************
// remove_allocation_resource - Removes the given allocation using locking
//       This should be called by end users.
//***************************************************************************

int remove_allocation_resource(Resource_t *r, int rmode, Allocation_t *alloc)
{
    tbx_atomic_inc(r->counter);

    return (_remove_allocation(r, rmode, alloc, 1));
}

//***************************************************************************
// merge_allocation_resource - Merges the space for the child allocation, a,
//    into the master(ma).  THe child allocations data is NOT merged and is lost.
//    The child allocation is also deleted.
//***************************************************************************

int merge_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a)
{
    int err;

    tbx_atomic_inc(r->counter);

    apr_thread_mutex_lock(r->mutex);
    err = _remove_allocation(r, OSD_DELETE_ID, a, 0);
    if (err == 0) {
        r->used_space[ma->reliability] += a->max_size;  //** Update the amount of space used
        ma->max_size += a->max_size;
        if (r->update_alloc == 1)
            write_allocation_header(r, ma, 0);
        err = modify_alloc_db(&(r->db), ma, ma->expiration);
        alru_put(r, ma); //** Update the LRU copy
    }
    apr_thread_mutex_unlock(r->mutex);

    return (err);
}

//***************************************************************************
// _remove_allocation_for_make_free - Removes the given allocation without locking and
//     assumes you are using an iterator;
//***************************************************************************

int _remove_allocation_for_make_free(Resource_t *r, int rmode, Allocation_t *alloc,
                                     DB_iterator_t *it)
{
    int err;

    log_printf(10, "_remove_allocation_for_make_free:  Removing " LU " with space " LU "\n",
               alloc->id, alloc->max_size);

    //** EVen if this fails we want to try and remove the physical allocation
    if ((err = remove_alloc_iter_db(it, alloc)) != 0) {
        debug_printf(1,
                     "_remove_allocation_for_make_free:  Error with remove_alloc_db!  Error=%d\n",
                     err);
//      return(err);
    }

    log_printf(10, "_remove_allocation_for_make_free:  Removed db entry\n");

    _trash_adjust(r, rmode, alloc->id);

    if (r->enable_alias_history) {
        if ((err = osd_remove(r->dev, rmode, alloc->id)) != 0) {
            debug_printf(1,
                         "_remove_allocation_for_make_free:  Error with fs->remove!  Error=%d\n",
                         err);
        }
    } else if (alloc->is_alias == 0) {
        if ((err = osd_remove(r->dev, rmode, alloc->id)) != 0) {
            debug_printf(1,
                         "_remove_allocation_for_make_free:  Error with fs->remove!  Error=%d\n",
                         err);
        }
    } else {
        log_printf(15,
                   "_remove_allocation_for_make_free:  a->is_alias=1.  Skipping fs->remove().\n");
    }

    debug_printf(10, "_remove_allocation_for_make_free: After remove\n");

    if (alloc->is_alias == 0) {
        r->used_space[alloc->reliability] -= alloc->max_size;   //** Upodate the amount of space used
    }
    r->n_allocs--;
    if (alloc->is_alias == 1)
        r->n_alias--;

    debug_printf(10, "_remove_allocation_for_make_free: end of routine\n");

    return (0);
}


//***************************************************************************
//  blank_space - Fills an allocation with 0's
//***************************************************************************

int blank_space(Resource_t *r, osd_id_t id, ibp_off_t off, ibp_off_t size)
{
    int j;
    osd_fd_t *fd;
    ibp_off_t offset;
    ibp_off_t bcount = size / _RESOURCE_BUF_SIZE;
    ibp_off_t remainder = size - bcount * _RESOURCE_BUF_SIZE;

    log_printf(10,
               "blank_space: id=" LU " off=" I64T " size=" I64T " bcount = " I64T " rem = " I64T
               "\n", id, off, size, bcount, remainder);
    offset = off;               // Now store the data in chunks
    fd = osd_open(r->dev, id, OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "blank_space: error opening id!\n");
        return (-1);
    }
    for (j = 0; j < bcount; j++) {
        osd_write(r->dev, fd, offset, _RESOURCE_BUF_SIZE, _blanks);
        offset = offset + _RESOURCE_BUF_SIZE;
    }
    if (remainder > 0)
        osd_write(r->dev, fd, offset, remainder, _blanks);

    osd_close(r->dev, fd);

//  debug_printf(10, "blank_space: err=%d\n", err);
    return (0);
}

//***************************************************************************
// make_free_space_iterator - Frees space up using the given iterator and
//    time stamp
//***************************************************************************

int make_free_space_iterator(Resource_t *r, DB_iterator_t *dbi, ibp_off_t *nbytesleft,
                             ibp_time_t timestamp)
{
    int err;
    Allocation_t a;
    int finished;
    ibp_off_t nleft;

    log_printf(10, "make_free_space_iterator: Attempting to free " LU " bytes\n", *nbytesleft);
    nleft = *nbytesleft;
    finished = 0;
    do {
        if ((err = db_iterator_next(dbi, DBR_NEXT, &a)) == 0) {
            if (a.expiration < timestamp) {
                if (nleft < (ibp_off_t) a.max_size) {   //** for alias allocations max_size == 0
                    nleft = 0;  //
                } else {
                    nleft -= a.max_size;        //** Free to delete it
                }

                err = _remove_allocation_for_make_free(r, OSD_PHYSICAL_ID, &a, dbi);
            } else {
                finished = 1;   //** Nothing else has expired:(
            }
        } else {
            finished = 1;
        }

        if (err == 0) {
            log_printf(10, "make_free_space_iterator: checked id " LU "\n", a.id);
        }
    } while ((nleft > 0) && (err == 0) && (!finished));

    *nbytesleft = nleft;

    log_printf(10,
               "make_free_space_iterator: Completed with err=%d and " LU " bytes left to free\n",
               err, *nbytesleft);

    if (nleft <= 0)
        err = 0;

    return (err);
}

//***************************************************************************
// _trash_free_space - Attempts to free up trash space
//    NOTE: No locking is performed
//***************************************************************************

int _trash_free_space(Resource_t *r, int tmode, ibp_off_t *nleft)
{
    osd_iter_t *iter;
    osd_id_t id;
    ibp_time_t move_time;
    int64_t bleft;
    char trash_id[1024];

    int rmode = (tmode == RES_DELETE_INDEX) ? OSD_DELETE_ID : OSD_EXPIRE_ID;

    bleft = *nleft;
    iter = osd_new_trash_iterator(r->dev, rmode);
    while ((osd_trash_iterator_next(iter, &id, &move_time, trash_id) == 0) && (bleft > 0)) {
        bleft = bleft - osd_trash_size(r->dev, rmode, trash_id);
        osd_trash_physical_remove(r->dev, rmode, trash_id);
    }
    osd_destroy_iterator(iter);

    *nleft = (bleft > 0) ? bleft : 0;

    return (0);
}

//***************************************************************************
// make_space - Creates enough free space on the device for a subsequent
//      allocation
//***************************************************************************

int make_space(Resource_t *r, ibp_off_t size, int atype)
{
    struct statfs stat;
    ibp_off_t free_bytes, trash_bytes;
    ibp_off_t nleft, type_over, aggregate_over, minfree_over, over, num;
    DB_iterator_t *dbi;
    int err;

    //** Get baseline values
    trash_bytes = r->trash_size[RES_DELETE_INDEX] + r->trash_size[RES_EXPIRE_INDEX];
    osd_statfs(r->dev, &stat);
    free_bytes = (ibp_off_t) stat.f_bavail * (ibp_off_t) stat.f_bsize;
//  nbytes = free_bytes + trash_bytes;
    nleft = 0;
    type_over = 0;
    aggregate_over = 0;
    minfree_over = 0;

    num = r->minfree + r->pending;
    log_printf(10,
               "make_space: stat=" LU " * trash=" I64T " * minfree=" I64T " size=" I64T " used="
               I64T " max=" I64T " pending=" I64T " type=%d\n", free_bytes, trash_bytes, num, size,
               r->used_space[atype], r->max_size[atype], r->pending, atype);

    //*** Are we over quota for the "type" ***
    if (r->used_space[atype] + size > r->max_size[atype]) {     //**Over quota so trim expired no matter what
        type_over = type_over + size + r->used_space[atype] - r->max_size[atype];
        log_printf(10, "make_space: Over quota for type %d needed space " LU "\n", atype,
                   type_over);
    }
    //*** Are we over the aggregate total ***
    num = r->used_space[ALLOC_HARD] + r->used_space[ALLOC_SOFT] + size;
    if (num > r->max_size[ALLOC_TOTAL]) {
        aggregate_over = num - r->max_size[ALLOC_TOTAL];
        log_printf(10, "make_space: Over quota for aggregate " LU "\n", aggregate_over);
    }
    //*** Check if minfree is Ok ***
    num = r->minfree + r->pending + size;
    if (num > free_bytes) {
        minfree_over = num - free_bytes;
        log_printf(10, "make_space: Adjusting needed space to satisfy minfree.   =" LU "\n",
                   minfree_over);
    }
    //** Figure out which "over" is larger
    over = (type_over > aggregate_over) ? type_over : aggregate_over;
    if (over < minfree_over)
        over = minfree_over;

    nleft = over;

    //** Check if we need to free trash space **
//  if (needed > free_bytes) {
//     nleft = needed - free_bytes;
//     log_printf(10, "make_space: Need to remove from trash. nleft =" LU "\n", nleft);
//  }

    if (nleft == 0)
        return (0);             //** Plenty of space so return

    //*** Start by freeing all the expired allocations ***
    ibp_time_t now = ibp_time_now();    //Get the current time so I know when to stop

    //** 1st free space from the trash bins
    err = _trash_free_space(r, RES_DELETE_INDEX, &nleft);
    if (nleft > 0)
        err = _trash_free_space(r, RES_EXPIRE_INDEX, &nleft);

    dbr_lock(&(r->db));
    dbi = expire_iterator(&(r->db));

    err = make_free_space_iterator(r, dbi, &nleft, now);

    db_iterator_end(dbi);
    dbr_unlock(&(r->db));

    //*** Now free up any soft allocations if needed ***
    if ((nleft > 0) && (err == 0)) {
        now = 0;                //** We can delete everything here if needed
        dbr_lock(&(r->db));
        dbi = soft_iterator(&(r->db));
        err = make_free_space_iterator(r, dbi, &nleft, now);
        db_iterator_end(dbi);
        dbr_unlock(&(r->db));

    }

    if ((nleft > 0) || (err != 0)) {
        return (1);             //*** Didn't have enough space **
    } else {
        return (0);
    }
}

//***************************************************************************
//  free_expired_allocations - Frees all expired allocations on the resource
//***************************************************************************

void free_expired_allocations(Resource_t *r)
{
    ibp_off_t size;

    tbx_atomic_inc(r->counter);

    apr_thread_mutex_lock(r->mutex);
    size = r->max_size[ALLOC_HARD];
    make_space(r, size, ALLOC_HARD);
    apr_thread_mutex_unlock(r->mutex);

    tbx_atomic_inc(r->counter);
}

//***************************************************************************
// resource_allocable - Returns the max amount of space that can be allocated
//    for the resource.
//***************************************************************************

uint64_t resource_allocable(Resource_t *r, int free_space)
{
    int64_t diff, fsdiff;
    uint64_t allocable;
    struct statfs stat;

    if (free_space == 1)
        free_expired_allocations(r);

    apr_thread_mutex_lock(r->mutex);

    diff = r->max_size[ALLOC_TOTAL] - r->used_space[ALLOC_HARD] - r->used_space[ALLOC_SOFT];
    if (diff < 0)
        diff = 0;

    osd_statfs(r->dev, &stat);
    fsdiff =
        (int64_t) stat.f_bavail * (int64_t) stat.f_bsize - r->minfree - r->pending +
        r->trash_size[RES_DELETE_INDEX] + r->trash_size[RES_EXPIRE_INDEX];
    if (fsdiff < 0)
        fsdiff = 0;

    apr_thread_mutex_unlock(r->mutex);

    log_printf(10, "resource_allocatble: diff=" I64T " fsdiff=" I64T "\n", diff, fsdiff);
    allocable = (diff < fsdiff) ? diff : fsdiff;

    return (allocable);
}


//***************************************************************************
// _new_allocation_resource - Creates and returns a uniqe allocation
//        for the resource.
//
//  **NOTE: NO LOCKING IS DONE.  THE ALLOCATION IS NOT BLANKED!  *****
//***************************************************************************

int _new_allocation_resource(Resource_t *r, Allocation_t *a, ibp_off_t size, int type,
                             int reliability, ibp_time_t length, int is_alias, int cs_type,
                             ibp_off_t blocksize)
{
    int err = 0;
    ibp_off_t total_size = ALLOC_HEADER + size;

    a->max_size = size;
    a->size = 0;
    a->type = type;
    a->reliability = reliability;
    a->expiration = length;
    a->read_refcount = 1;
    a->write_refcount = 0;
    a->r_pos = 0;
    a->w_pos = 0;
    a->is_alias = is_alias;

    //**Make sure we have enough space if this is a real allocation and record it
    if (a->is_alias == 0) {
        err = make_space(r, total_size, reliability);
        if (r->preallocate)
            r->pending += size;
    }

    if (err != 0)
        return (err);           //** Exit if not enough space

    //** Munge the disk chksum type and blocksize
    if (cs_type == -1) {
        if (r->enable_chksum == 0) {
            cs_type = CHKSUM_NONE;
            blocksize = 0;
        } else {
            cs_type = tbx_chksum_type(&(r->chksum));
            blocksize = r->chksum_blocksize;
        }
    }

    if (blocksize > (int64_t) 2147483648) {
        log_printf(0, "blocksize too large!  bs=" I64T "\n", blocksize);
        return (1);
    }

    a->id = osd_create_id(r->dev, cs_type, ALLOC_HEADER, blocksize, 0);
    if (a->id == 0) {           //** Got an error creating the allocation
        log_printf(1, "ERROR creating allocation!\n");
        return (1);;
    }

    alru_put(r, a); //** Add it to the LRU copy
    create_alloc_db(&(r->db), a);

    //** Always store the initial alloc in the file header
    if (a->is_alias == 0) {
        write_allocation_header(r, a, 1);       //** Store the header
        osd_blank_history(r, a->id);        //** Also store the history
    } else if (r->enable_alias_history) {
        write_allocation_header(r, a, 1);       //** Store the header
        osd_blank_history(r, a->id);        //** Also store the history
    }

    r->n_allocs++;
    if (is_alias == 0) {
        r->used_space[a->reliability] += a->max_size;
    } else {
        r->n_alias++;
    }

    debug_printf(5, "_new_allocation_resource: rid=%s rel=%d, used=" LU "\n", r->name,
                 a->reliability, r->used_space[a->reliability]);
    debug_printf(5, "_new_allocation_resource: rcap=%s\n", a->caps[READ_CAP].v);

    return (err);
}

//***************************************************************************
// create_allocation_resource - Creates and returns a uniqe allocation
//        for the resource
//***************************************************************************

int create_allocation_resource(Resource_t *r, Allocation_t *a, ibp_off_t size, int type,
                               int reliability, ibp_time_t length, int is_alias,
                               int preallocate_space, int cs_type, ibp_off_t blocksize)
{
    int err;
    ibp_off_t total_size;

    memset(a, 0, sizeof(Allocation_t));

    tbx_atomic_inc(r->counter);

    apr_thread_mutex_lock(r->mutex);
    err =
        _new_allocation_resource(r, a, size, type, reliability, length, is_alias, cs_type,
                                 blocksize);
    apr_thread_mutex_unlock(r->mutex);

    if (err == 0) {
        if (a->is_alias == 0) {
            total_size = ALLOC_HEADER + size;
            if ((preallocate_space & RES_RESERVE_FALLOCATE) > 0)
                osd_reserve(r->dev, a->id, total_size);
            if ((preallocate_space & RES_RESERVE_BLANK) > 0)
                blank_space(r, a->id, 0, total_size);

            apr_thread_mutex_lock(r->mutex);
            r->pending -= size;
            apr_thread_mutex_unlock(r->mutex);
        }

    }

    return (err);
}

//***************************************************************************
//  resource_undelete - Undeletes a resource from the specificed trash bin
//    The recovered allocation is returned via recovered_a if the field is non-NULL
//***************************************************************************

int resource_undelete(Resource_t *r, int trash_type, const char *trash_id, ibp_time_t expiration,
                      Allocation_t *recovered_a)
{
    Allocation_t a;
    ibp_off_t nbytes;
    osd_id_t id;
    int rmode, err;

    rmode = (trash_type == RES_DELETE_INDEX) ? OSD_DELETE_ID : OSD_EXPIRE_ID;

    tbx_atomic_inc(r->counter);

    //** Undelete it
    id = osd_trash_undelete(r->dev, rmode, trash_id);
    if (id == 0) {
        log_printf(10,
                   "resource_undelete: Can't find allocation.  RID=%s trash_type=%d trash_id=%s\n",
                   r->name, trash_type, trash_id);
        return (-1);
    }
    //** Adjust the trash size/count
    apr_thread_mutex_lock(r->mutex);
    r->n_trash[trash_type]--;
    r->trash_size[trash_type] = r->trash_size[trash_type] - osd_size(r->dev, id);
    apr_thread_mutex_unlock(r->mutex);

    //** Read the undeleted allocation
    err = read_allocation_header(r, id, &a);
    if (err != 0) {
        log_printf(10,
                   "resource_undelete: Error reading recovered allocation.  RID=%s trash_type=%d trash_id=%s id="
                   LU " err=%d\n", r->name, trash_type, trash_id, id, err);
        osd_remove(r->dev, rmode, id);  //** re-delete it putting it back where it came from
        return (-2);
    }
    //** Verify I have the space
    apr_thread_mutex_lock(r->mutex);
    if (a.is_alias == 0) {
        nbytes = a.max_size - a.size;
        err = make_space(r, nbytes, a.reliability);
        if (err != 0) {
            apr_thread_mutex_unlock(r->mutex);
            log_printf(10,
                       "resource_undelete: Not enough free space! RID=%s trash_type=%d trash_id=%s id="
                       LU " needed nbytes=" I64T " err=%d\n", r->name, trash_type, trash_id, id,
                       nbytes, err);
            osd_remove(r->dev, rmode, id);      //** re-delete it putting it back where it came from
            return (-2);
        }
    }
    //** Adjust expiration and refcount and update the allocation
    a.expiration = expiration;
    a.read_refcount = 1;
    err = write_allocation_header(r, &a, 0);
    if (err != 0) {
        apr_thread_mutex_unlock(r->mutex);
        log_printf(10,
                   "resource_undelete: Error writing recovered allocation.  RID=%s trash_type=%d trash_id=%s id="
                   LU " err=%d\n", r->name, trash_type, trash_id, id, err);
        osd_remove(r->dev, rmode, id);  //** re-delete it putting it back where it came from
        return (-2);
    }
    //** Add it to the DB
    err = _put_alloc_db(&(r->db), &a, 0);
    alru_put(r, &a); //** and the LRU copy

    //** Adjust the space
    r->n_allocs++;
    if (a.is_alias == 0) {
        r->used_space[a.reliability] += a.max_size;
    } else {
        r->n_alias++;
    }

    apr_thread_mutex_unlock(r->mutex);

    if (recovered_a != NULL)
        *recovered_a = a;

    return (err);
}



//***************************************************************************
// split_allocation_resource - Splits an existing allocation and returns a unique
//      allocation with the correct space and trims the size of the master allocation
//***************************************************************************

int split_allocation_resource(Resource_t *r, Allocation_t *ma, Allocation_t *a, ibp_off_t size,
                              int type, int reliability, ibp_time_t length, int is_alias,
                              int preallocate_space, int cs_type, ibp_off_t cs_blocksize)
{
    int err;
    ibp_off_t total_size;

    if ((ibp_off_t) ma->max_size < size) {
        log_printf(15,
                   "split_allocation_resource: Not enough space left on master id! mid=" LU
                   " msize=" I64T " size=" I64T "\n", ma->id, ma->size, size);
        return (1);
    }

    memset(a, 0, sizeof(Allocation_t));
    a->split_parent_id = ma->id;

    tbx_atomic_inc(r->counter);

    apr_thread_mutex_lock(r->mutex);
    r->used_space[ma->reliability] = r->used_space[ma->reliability] - size;
    ma->max_size = ma->max_size - size;
    err =
        _new_allocation_resource(r, a, size, type, reliability, length, is_alias, cs_type,
                                 cs_blocksize);
    if (err == 0) {
        if (osd_size(r->dev, ma->id) > (ibp_off_t) ma->max_size)
            osd_truncate(r->dev, ma->id, ma->max_size + ALLOC_HEADER);
        if (ma->size > ma->max_size)
            ma->size = ma->max_size;
        if (r->update_alloc == 1)
            write_allocation_header(r, ma, 0);
        alru_put(r, ma); //** Update the LRU copy
        err = modify_alloc_db(&(r->db), ma, ma->expiration);    //** Store the master back with updated size
    } else {                    //** Problem so undo size tweaks
        log_printf(15, "Error with _new_allocation!\n");
        r->used_space[ma->reliability] = r->used_space[ma->reliability] + size;
        ma->max_size = ma->max_size + size;
    }
    apr_thread_mutex_unlock(r->mutex);

    if (err == 0) {
        if (a->is_alias == 0) {
            total_size = ALLOC_HEADER + size;
            if ((preallocate_space & RES_RESERVE_FALLOCATE) > 0)
                osd_reserve(r->dev, a->id, total_size);
            if ((preallocate_space & RES_RESERVE_BLANK) > 0)
                blank_space(r, a->id, 0, total_size);
        }
    }

    return (err);
}

//***************************************************************************
// rename_allocation_resource - Renames an allocation.  Actually it just
//    replaces the caps associated with the allocation so the ID
//    stays the same
//***************************************************************************

int rename_allocation_resource(Resource_t *r, Allocation_t *a)
{
    int err;

    apr_thread_mutex_lock(r->mutex);
    err = remove_alloc_db(&(r->db), a);
    if (err == 0) {
        //** Replace the caps
        create_alloc_db(&(r->db), a);

        //** Update the LRU entry
        alru_put(r, a);
    }
    if (a->is_alias == 0)
        write_allocation_header(r, a, 0);
    apr_thread_mutex_unlock(r->mutex);

    tbx_atomic_inc(r->counter);

    return (err);
}

//***************************************************************************
// get_allocation_resource - Returns the allocations data structure
//***************************************************************************

int get_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a)
{
    int err;

    //** See if we can find it in the LRU
    if (tbx_lru_get(r->id_lru, &id, sizeof(osd_id_t), a) == 0) { //** Got it
        return(0);
    }

    //** Not in the LRU so look it up
    err = get_alloc_with_id_db(&(r->db), id, a);

    //** and add it to the LRU
    if (err == 0) alru_put(r, a);

    return (err);
}

//***************************************************************************
// get_allocation_by_cap_id_resource - Returns the allocations data structure
//***************************************************************************

int get_allocation_by_cap_id_resource(Resource_t * r, int cap_type, cap_id_t * cap_id, Allocation_t * a)
{
    int err;

    err = get_allocation_resource(r, cap_id->id, a);
    if (err == 0) { //** Now validate the CAP
        err = memcmp(a->caps[cap_type].v, cap_id->cap.v, CAP_SIZE);
    }
    return (err);
}

//***************************************************************************
// modify_allocation_resource - Stores the allocation data structure
//***************************************************************************

int modify_allocation_resource(Resource_t *r, osd_id_t id, Allocation_t *a, int mandatory_change)
{
    Allocation_t old_a;
    int err;
    ibp_off_t size;

    err = 0;

    tbx_atomic_inc(r->counter);

    if ((err = get_allocation_resource(r, a->id, &old_a)) != 0) {
        log_printf(0, "put_allocation_resource: Can't find id " LU "  db err = %d\n", a->id, err);
        return (err);
    }

    if ((old_a.reliability != a->reliability) || (old_a.max_size != a->max_size)) {
        if (a->is_alias == 0) {
            apr_thread_mutex_lock(r->mutex);
            r->used_space[old_a.reliability] -= old_a.max_size; //** Update the amount of space used from the old a

            size = 0;
            err = 0;
            if (old_a.max_size < a->max_size) { //** Growing so need to add space
                size = a->max_size - old_a.max_size;

                if ((err = make_space(r, a->max_size, a->reliability)) == 0) {  //**Make sure we have enough space and record it
                    if (r->preallocate)
                        r->pending += size;
                } else {
                    log_printf(0, "modify_allocation_resource:  Error with make_space err=%d\n",
                               err);
                }
            } else {            //** Shrinking the space
                if (a->size > a->max_size) {
                    a->size = a->max_size;
                    if (a->w_pos >= a->size) a->w_pos = a->size - 1;
                    if (a->r_pos >= a->size) a->r_pos = a->size - 1;
                    osd_truncate(r->dev, a->id, a->max_size + ALLOC_HEADER);
                }

            }

            if (err == 0) {
                r->used_space[a->reliability] += a->max_size;   //** Add in the new size if no errors
            } else {
                r->used_space[old_a.reliability] += old_a.max_size;     //** If not enough space revert back
            }

            apr_thread_mutex_unlock(r->mutex);

            if (err != 0) {
                return (err);   // ** FAiled on make_space
            } else if ((r->preallocate > 0) && (size > 0)) {    //** Actually fill the extra space they requested
                if ((r->preallocate & RES_RESERVE_FALLOCATE) > 0)
                    osd_reserve(r->dev, a->id, size);
                if ((r->preallocate & RES_RESERVE_BLANK) > 0)
                    blank_space(r, a->id, old_a.max_size, size);

                apr_thread_mutex_lock(r->mutex);
                r->pending -= size;
                apr_thread_mutex_unlock(r->mutex);
            }
        }
    }

    if ((r->update_alloc == 1) || (mandatory_change == 1)) {
        if (a->is_alias == 0) {
            write_allocation_header(r, a, 0);
        } else if (r->enable_alias_history) {
            write_allocation_header(r, a, 0);
        }
    }

    //** Update the LRU entry
    alru_put(r, a);

    return (modify_alloc_db(&(r->db), a, old_a.expiration));
}

//---------------------------------------------------------------------------

//***************************************************************************
// validate_allocation - Validates (and optionally corrects) an allocation
//***************************************************************************

int validate_allocation(Resource_t *r, osd_id_t id, int correct_errors)
{
    tbx_atomic_inc(r->counter);

    return (osd_validate_chksum(r->dev, id, correct_errors));
}


//***************************************************************************
// get_allocation_chksums - Retreives the chksums for the allocation
//***************************************************************************

ibp_off_t get_allocation_chksum(Resource_t *r, osd_id_t id, char *disk_buffer, char *calc_buffer,
                                ibp_off_t buffer_size, osd_off_t *block_len, char *good_block,
                                ibp_off_t start_block, ibp_off_t end_block)
{
    tbx_atomic_inc(r->counter);
    return (osd_get_chksum
            (r->dev, id, disk_buffer, calc_buffer, buffer_size, block_len, good_block, start_block,
             end_block));
}

//***************************************************************************
// get_allocation_chksum_info - Retreives the allocation chksum information
//***************************************************************************

int get_allocation_chksum_info(Resource_t *r, osd_id_t id, int *cs_type,
                               ibp_off_t *header_blocksize, ibp_off_t *blocksize)
{
    tbx_atomic_inc(r->counter);
    return (osd_chksum_info(r->dev, id, cs_type, header_blocksize, blocksize));
}


//***************************************************************************
// write_allocation - Writes to a resource using the provided fd
//***************************************************************************

ibp_off_t write_allocation(Resource_t *r, osd_fd_t *fd, ibp_off_t offset, ibp_off_t len,
                           void *buffer)
{
    ibp_off_t nbytes, n;
    int i;

    tbx_atomic_inc(r->counter);

    n = osd_write(r->dev, fd, offset + ALLOC_HEADER, len, buffer);
    log_printf(10, "write_allocation(%s, %p, " I64T ", " I64T ")=" I64T "\n", r->name, fd, offset,
               len, n);
    if (n == len) {
        n = 0;
    } else {                    //** Need to free space if possible
        n = 1;
        i = 0;
        nbytes = offset + len + ALLOC_HEADER - osd_fd_size(r->dev, fd) + 1;
        while ((resource_allocable(r, 0) > 0) && (n != 0) && (i < 3)) {
            i++;
            apr_thread_mutex_lock(r->mutex);
            make_space(r, nbytes, ALLOC_HARD);
            apr_thread_mutex_unlock(r->mutex);

            n = osd_write(r->dev, fd, offset + ALLOC_HEADER, len, buffer);
            if (n == len)
                n = 0;
        }
    }

    return (n);
}

//***************************************************************************
// write_allocation_header - Stores the allocation header
//***************************************************************************

int write_allocation_header(Resource_t *r, Allocation_t *a, int do_blank)
{
    int n;
    osd_fd_t *fd;
    char header[ALLOC_HEADER];

    fd = osd_open(r->dev, a->id, OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "write_allocation_header: Error with open_allocation(-res-, " LU ") = %d\n",
                   a->id, errno);
        return (IBP_E_FILE_WRITE);
    }

    if (do_blank == 0) {
        n = osd_write(r->dev, fd, 0, sizeof(Allocation_t), a);  //**Store the header
        if (n == sizeof(Allocation_t))
            n = 0;
    } else {
        memset(header, 0, ALLOC_HEADER);
        memcpy(header, a, sizeof(Allocation_t));
        n = osd_write(r->dev, fd, 0, ALLOC_HEADER, header);
        if (n == ALLOC_HEADER)
            n = 0;
    }

    osd_close(r->dev, fd);
    return (n);
}

//***************************************************************************
// read_allocation_header - Reads the allocation header
//***************************************************************************

int read_allocation_header(Resource_t *r, osd_id_t id, Allocation_t *a)
{
    int n;
    osd_fd_t *fd;

    fd = osd_open(r->dev, id, OSD_READ_MODE);
    if (fd == NULL) {
        log_printf(0, "read_allocation_header: Error with open_allocation(-res-, " LU ") = %d\n",
                   a->id, errno);
        return (IBP_E_FILE_READ);
    }

    n = osd_read(r->dev, fd, 0, sizeof(Allocation_t), a);       //**read the header

    osd_close(r->dev, fd);

    if (n == sizeof(Allocation_t))
        n = 0;
    return (n);
}

//***************************************************************************
// read_allocation - Reads to a resource using the provided fd
//***************************************************************************

ibp_off_t read_allocation(Resource_t *r, osd_fd_t *fd, ibp_off_t offset, ibp_off_t len,
                          void *buffer)
{
    ibp_off_t n;

    if (fd == NULL) {
        log_printf(0, "read_allocation: invalid fd!\n");
        return (IBP_E_FILE_READ);
    }

    tbx_atomic_inc(r->counter);

    n = osd_read(r->dev, fd, offset + ALLOC_HEADER, len, buffer);
    if (n == len)
        n = 0;

    return (n);
}

//***************************************************************************
// print_allocation_resource - Prints the allocation info to the fd
//***************************************************************************

int print_allocation_resource(Resource_t *r, FILE *fd, Allocation_t *a)
{
    apr_time_t now;
    int64_t diff;

    fprintf(fd, "id = " LU "\n", a->id);
    fprintf(fd, "is_alias = %d\n", a->is_alias);
    fprintf(fd, "read_cap = %s\n", a->caps[READ_CAP].v);
    fprintf(fd, "write_cap = %s\n", a->caps[WRITE_CAP].v);
    fprintf(fd, "manage_cap = %s\n", a->caps[MANAGE_CAP].v);
    fprintf(fd, "reliability = %d\n", a->reliability);
    fprintf(fd, "type = %d\n", a->type);
    now = apr_time_now();
    diff = a->expiration - apr_time_sec(now);
    now = a->expiration;
    fprintf(fd, "expiration = " TT " (expires in " LU " sec) \n", now, diff);
    fprintf(fd, "read_refcount = %d\n", a->read_refcount);
    fprintf(fd, "write_refcount = %d\n", a->write_refcount);
    fprintf(fd, "max_size = " LU "\n", a->max_size);

    return (0);
}

//*****************************************************************
// walk_expire_iterator_begin - Creates an interator to walk through both
//     the hard and soft expire iterators
//*****************************************************************

walk_expire_iterator_t *walk_expire_iterator_begin(Resource_t *r)
{
    walk_expire_iterator_t *wei;

    tbx_type_malloc_clear(wei, walk_expire_iterator_t, 1);
    tbx_atomic_inc(r->counter);

    wei->reset = 1;
    wei->r = r;

    dbr_lock(&(r->db));

    wei->hard = expire_iterator(&(r->db));
    if (wei->hard == NULL) {
        log_printf(10, "walk_expire_hard_iterator: wei->hard = NULL! r=%s\n", r->name);
        return (NULL);
    }

    wei->soft = soft_iterator(&(r->db));
    if (wei->hard == NULL) {
        log_printf(10, "walk_expire_hard_iterator: wei->soft = NULL! r=%s\n", r->name);
        return (NULL);
    }

    return (wei);
}


//*****************************************************************
// walk_expire_iterator_end - Destroys the walk through iterator
//*****************************************************************

void walk_expire_iterator_end(walk_expire_iterator_t *wei)
{
    tbx_atomic_inc(wei->r->counter);

    db_iterator_end(wei->hard);
    db_iterator_end(wei->soft);

    dbr_unlock(&(wei->r->db));

    free(wei);
}

//*****************************************************************
// set_walk_expire_iterator - Sets the time for the  walk through iterator
//*****************************************************************

int set_walk_expire_iterator(walk_expire_iterator_t *wei, ibp_time_t t)
{
    int i;

    wei->reset = 0;             //** rest the times to trigger a reload on get next

    tbx_atomic_inc(wei->r->counter);

    i = set_expire_iterator(wei->hard, t, &(wei->hard_a));
    if (i != 0) {
        log_printf(10, "set_walk_expire_iterator: Error with set_soft: %d, time=" TT "\n", i,
                   ibp2apr_time(t));
        wei->hard_a.expiration = 0;
    }

    i = set_expire_iterator(wei->soft, t, &(wei->soft_a));
    if (i != 0) {
        log_printf(10, "set_walk_expire_iterator: Error with set_hard: %d time=" TT "\n", i,
                   ibp2apr_time(t));
        wei->soft_a.expiration = 0;
    }

    return (0);
}

//*****************************************************************
// get_next_walk_expire_iterator - Gets the next record for the walk through iterator
//*****************************************************************

int get_next_walk_expire_iterator(walk_expire_iterator_t *wei, int direction, Allocation_t *a)
{
    int err, dir;
    int64_t dt;

    tbx_atomic_inc(wei->r->counter);

    if (wei->reset == 1) {      //** Reload starting records
        wei->reset = 0;
        err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
        if (err != 0) {
            log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n",
                       err);
            wei->hard_a.expiration = 0;
        }

        err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
        if (err != 0) {
            log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n",
                       err);
            wei->soft_a.expiration = 0;
        }
    }

    log_printf(10, "get_next_walk_expire_iterator: hard= %u soft= %u \n", wei->hard_a.expiration,
               wei->soft_a.expiration);

    //** Do a boundary check ***
    if (wei->hard_a.expiration == 0) {
        if (wei->soft_a.expiration == 0) {
            return (1);
        } else {
            *a = wei->soft_a;
            err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
            if (err != 0) {
                log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n",
                           err);
                wei->soft_a.expiration = 0;
            }

            log_printf(15, "get_next_walk_expire_iterator: 1 expire= %u\n", a->expiration);
            return (0);
        }
    } else if (wei->soft_a.expiration == 0) {
        *a = wei->hard_a;
        err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
        if (err != 0) {
            log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n",
                       err);
            wei->hard_a.expiration = 0;
        }

        log_printf(15, "get_next_walk_expire_iterator: 2 expire= %u\n", a->expiration);
        return (0);
    }

    //** If I make it here that means both the hard and soft allocations are valid

    //** Fancy way to unify DBR_PREV/DBR_NEXT into a single set **
    dir = 1;
    if (direction == DBR_PREV)
        dir = -1;

    dt = dir * (wei->hard_a.expiration - wei->soft_a.expiration);
    if (dt > 0) {               //** hard > soft so return the soft one
        *a = wei->soft_a;
        err = db_iterator_next(wei->soft, direction, &(wei->soft_a));
        if (err != 0) {
            log_printf(10, "get_next_walk_expire_iterator: Error or end with next_soft: %d \n",
                       err);
            wei->soft_a.expiration = 0;
        }
    } else {                    //** hard < soft so return the hard a
        *a = wei->hard_a;
        err = db_iterator_next(wei->hard, direction, &(wei->hard_a));
        if (err != 0) {
            log_printf(10, "get_next_walk_expire_iterator: Error or end with next_hard: %d \n",
                       err);
            wei->hard_a.expiration = 0;
        }
    }

    log_printf(15, "get_next_walk_expire_iterator: 3 expire= %u\n", a->expiration);
    return (0);
}

//*****************************************************************
// resource_rescan - Forces a resource rescan.
//    NOTE:  This is done in the background thread
//*****************************************************************

void resource_rescan(Resource_t *r)
{
    //** Set the time for the next rescan
    apr_thread_mutex_lock(r->mutex);
    r->next_rescan = 0;
    apr_thread_mutex_unlock(r->mutex);

    //** Wake up the cleanup thread
    apr_thread_mutex_lock(r->cleanup_lock);
    apr_thread_cond_signal(r->cleanup_cond);
    apr_thread_mutex_unlock(r->cleanup_lock);
}

//*****************************************************************
// trash_rescan  - Scans the trash directory and accums sizes
//    NOTE: This is not completely thread safe.  You can drop some allocations that occur
//          during the scan.  But this should be OK since the min_free setting keeps you
//          from running out of space.
//*****************************************************************

ibp_time_t trash_rescan(Resource_t *r, int tmode)
{
    osd_iter_t *iter;
    osd_id_t id;
    ibp_time_t oldest_time;
    ibp_off_t nbytes;
    int nwipe;
    ibp_time_t move_time;
    char trash_id[1024];

    int rmode = (tmode == RES_DELETE_INDEX) ? OSD_DELETE_ID : OSD_EXPIRE_ID;

    //** Set the starting counts
    apr_thread_mutex_lock(r->mutex);
    r->trash_size[tmode] = 0;
    r->n_trash[tmode] = 0;
    apr_thread_mutex_unlock(r->mutex);

    oldest_time = 0;
    move_time = 0;
    nbytes = 0;
    nwipe = 0;

    tbx_atomic_inc(r->counter);

    iter = osd_new_trash_iterator(r->dev, rmode);
    if (iter == NULL)
        goto fail;
    while (osd_trash_iterator_next(iter, &id, &move_time, trash_id) == 0) {
        if (oldest_time == 0)
            oldest_time = move_time;
        if (move_time < oldest_time)
            oldest_time = move_time;

        nwipe++;
        nbytes = nbytes + osd_trash_size(r->dev, rmode, trash_id);

        if ((nwipe % 100) == 0)
            tbx_atomic_inc(r->counter);
    }
    osd_destroy_iterator(iter);

fail:
    log_printf(15, "trash_rescan: RID=%s tmode=%d nbytes=" LU " nalloca=%d\n", r->name, tmode,
               nbytes, nwipe);

    //** Update the counts **
    apr_thread_mutex_lock(r->mutex);
    r->trash_size[tmode] = nbytes;
    r->n_trash[tmode] = nwipe;
    apr_thread_mutex_unlock(r->mutex);

    return (oldest_time);
}

//*****************************************************************
// trash_cleanup  - Performs the actual resource cleanup for the trash bins
//*****************************************************************

time_t trash_cleanup(Resource_t *r, int tmode, ibp_time_t wipe_time, int enforce_minfree)
{
    osd_iter_t *iter;
    osd_id_t id;
    ibp_time_t oldest_time, move_time;
    ibp_off_t nbytes, free_bytes;
    int nwipe, loop;
    char trash_id[1024];
    struct statfs stat;

    int rmode = (tmode == RES_DELETE_INDEX) ? OSD_DELETE_ID : OSD_EXPIRE_ID;

    //** Get the starting counts
    oldest_time = 0;
    nbytes = 0;
    nwipe = 0;
    loop = 0;

    osd_statfs(r->dev, &stat);
    free_bytes = (ibp_off_t) stat.f_bavail * (ibp_off_t) stat.f_bsize;

    iter = osd_new_trash_iterator(r->dev, rmode);
    if (iter == NULL)
        goto fail;
    while (osd_trash_iterator_next(iter, &id, &move_time, trash_id) == 0) {
        if (oldest_time == 0)
            oldest_time = move_time;

        //** Free the data if too old (<wipe_time) or not enough free space for min reserve
        if ((move_time <= wipe_time) || ((free_bytes < r->minfree) && (enforce_minfree == 1))) {
            nwipe++;
            nbytes = nbytes + osd_trash_size(r->dev, rmode, trash_id);
            free_bytes = free_bytes + osd_trash_size(r->dev, rmode, trash_id);
            osd_trash_physical_remove(r->dev, rmode, trash_id);
        } else if (move_time < oldest_time) {
            oldest_time = move_time;
        }

        loop++;
        if ((loop % 100) == 0)
            tbx_atomic_inc(r->counter);
    }
    osd_destroy_iterator(iter);

fail:
    //** Update the counts **
    apr_thread_mutex_lock(r->mutex);
    r->trash_size[tmode] = (r->trash_size[tmode] > nbytes) ? r->trash_size[tmode] - nbytes : 0;
    r->n_trash[tmode] = (r->n_trash[tmode] > nwipe) ? r->n_trash[tmode] - nwipe : 0;
    apr_thread_mutex_unlock(r->mutex);

    log_printf(10,
               "trash_cleanup: rid=%s tmode=%d oldest_time=" TT " freed nbytes=" LU " nalloc=%d\n",
               r->name, tmode, ibp2apr_time(oldest_time), nbytes, nwipe);

    return (oldest_time);
}

//*****************************************************************
// resource_cleanup  - Performs the actual resource cleanup
//*****************************************************************

void resource_cleanup(Resource_t *r, ibp_time_t start_grace_time)
{
    int max_alloc = 100;
    int i, n, err, start_index;
    Allocation_t a[max_alloc], b;
    ibp_time_t grace_over = start_grace_time + r->preexpire_grace_period;
    walk_expire_iterator_t *wei;

    log_printf(1, "resource_background_cleanup: Start of routine.  rid=%s time= " TT "\n", r->name,
               apr_time_now());

    if ((ibp_time_now() - r->start_time) < r->preexpire_grace_period) {
        log_printf(1, "END.  Skipping.  In preexpire_grace_period. rid=%s time= " TT "\n", r->name,
                   apr_time_now());
        return;
    }

    n = max_alloc;
    while (n == max_alloc) {
        //** Perform the walk
        wei = walk_expire_iterator_begin(r);
        if (wei == NULL) {
            n = 0;
            goto fail;
        }
        //** First skip all the stuff that has expired while the depot was shut down
        //** using the preexpire_grace_period
        start_index = 0;
        if (grace_over > ibp_time_now()) {
            do {
                err = get_next_walk_expire_iterator(wei, DBR_NEXT, &(a[0]));
                if (err == 0) {
                    if (a[0].expiration > start_grace_time) {
                        err = 1;        //** Kick out
                        start_index = 1;
                    }
                }
            } while (err == 0);
        }
        //** Now get the rest of the stuff that has expired
        n = max_alloc;
        for (i = start_index; i < max_alloc; i++) {
            err = get_next_walk_expire_iterator(wei, DBR_NEXT, &(a[i]));
            if (err != 0) {
                n = i;
                break;
            }
            if ((a[i].expiration + r->preexpire_grace_period) > ibp_time_now()) {
                n = i;
                break;
            }
        }

fail:
        walk_expire_iterator_end(wei);

        log_printf(1, "resource_background_cleanup: rid=%s n=%d\n", r->name, n);

        //** Do the actual removal
        for (i = 0; i < n; i++) {
            log_printf(1, "resource_background_cleanup:i=%d.  rid=%s checking/removing:" LU "\n",
                       i, r->name, a[i].id);
            err = get_alloc_with_id_db(&(r->db), a[i].id, &b);
            if (err == 0) {
                if (b.expiration < ibp_time_now())
                    _remove_allocation(r, OSD_EXPIRE_ID, &b, 1);
            }
        }
    }

    log_printf(1, "resource_background_cleanup: End of routine.  rid=%s time= " TT "\n", r->name,
               apr_time_now());

    return;
}

//*****************************************************************
// resource_cleanup_thread - Thread for doing background cleanups
//*****************************************************************

void *resource_cleanup_thread(apr_thread_t *th, void *data)
{
    Resource_t *r = (Resource_t *) data;
    ibp_time_t delete_oldest, expire_oldest, wipe_start, start_time;
    apr_interval_time_t t;
    int count;

    log_printf(5, "resource_cleanup_thread: Start.  rid=%s time= " TT "\n", r->name,
               apr_time_now());

    tbx_monitor_thread_create(MON_MY_THREAD, "resource_cleanup_thread: rid=%s", r->name);
    delete_oldest = 0;
    expire_oldest = 0;

    start_time = ibp_time_now();        //** Get the starting time

    apr_thread_mutex_lock(r->cleanup_lock);
    r->next_rescan = ibp_time_now() - 1;        //** Force a rescan;
    apr_thread_mutex_unlock(r->cleanup_lock);

    apr_thread_mutex_lock(r->cleanup_lock);
    while (r->cleanup_shutdown == 0) {
        tbx_monitor_thread_message(MON_MY_THREAD, "Starting run");
        count = tbx_atomic_get(r->counter);
        if (count > 1073741824)
            tbx_atomic_set(r->counter, 0);

        //** Check if we ned to do a rescan
        if (ibp_time_now() > r->next_rescan) {
            apr_thread_mutex_unlock(r->cleanup_lock);

            tbx_monitor_thread_message(MON_MY_THREAD, "Performing a rescans");
            delete_oldest = trash_rescan(r, RES_DELETE_INDEX);
            expire_oldest = trash_rescan(r, RES_EXPIRE_INDEX);

            apr_thread_mutex_lock(r->cleanup_lock);
            r->next_rescan = ibp_time_now() + r->rescan_interval;
        }
        apr_thread_mutex_unlock(r->cleanup_lock);

        wipe_start = ibp_time_now() - r->trash_grace_period[RES_DELETE_INDEX];
        log_printf(10,
                   "resource_cleanup_thread: rid=%s wipe_start_delete=" TT " oldest=" TT " now=" TT
                   " grace=" TT "\n", r->name, ibp2apr_time(wipe_start),
                   ibp2apr_time(delete_oldest), ibp2apr_time(ibp_time_now()),
                   ibp2apr_time(r->trash_grace_period[RES_DELETE_INDEX]));
        if (wipe_start >= delete_oldest)
            delete_oldest = trash_cleanup(r, RES_DELETE_INDEX, wipe_start, 1);

        wipe_start = ibp_time_now() - r->trash_grace_period[RES_EXPIRE_INDEX];
        log_printf(10,
                   "resource_cleanup_thread: rid=%s wipe_start_expire=" TT " oldest=" TT " now=" TT
                   " grace=" TT "\n", r->name, ibp2apr_time(wipe_start),
                   ibp2apr_time(expire_oldest), ibp2apr_time(ibp_time_now()),
                   ibp2apr_time(r->trash_grace_period[RES_EXPIRE_INDEX]));
        if (wipe_start >= expire_oldest)
            expire_oldest = trash_cleanup(r, RES_EXPIRE_INDEX, wipe_start, 1);

        log_printf(10,
                   "resource_cleanup_thread: rid=%s expire_oldest=" TT " delete_oldest=" TT "\n",
                   r->name, ibp2apr_time(expire_oldest), ibp2apr_time(delete_oldest));
        resource_cleanup(r, start_time);

        tbx_monitor_thread_message(MON_MY_THREAD, "run finished");

        t = 1000000 * r->cleanup_interval;      //Cleanup interval in us
        apr_thread_mutex_lock(r->cleanup_lock);
        if (r->cleanup_shutdown == 0) {
            log_printf(5, "resource_cleanup_thread: Sleeping rid=%s time= " TT " shutdown=%d\n",
                       r->name, apr_time_now(), r->cleanup_shutdown);
            apr_thread_cond_timedwait(r->cleanup_cond, r->cleanup_lock, t);
        }
        log_printf(5, "resource_cleanup_thread: waking up rid=%s time= " TT " shutdown=%d\n",
                   r->name, apr_time_now(), r->cleanup_shutdown);
        tbx_log_flush();
    }

    apr_thread_mutex_unlock(r->cleanup_lock);

    tbx_monitor_thread_destroy(MON_MY_THREAD);

    log_printf(5, "resource_cleanup_thread: Exit.  rid=%s time= " TT "\n", r->name, apr_time_now());
    tbx_log_flush();
    apr_thread_exit(th, 0);

    return (0);                 //** Never makes it here but this suppresses the warning
}

//*****************************************************************
// launch_resource_cleanup_thread
//*****************************************************************

void launch_resource_cleanup_thread(Resource_t *r)
{
    r->cleanup_shutdown = 0;

    //** if set the default stack size **
    apr_size_t stacksize = 2 * 1024 * 1024;
    apr_threadattr_create(&(r->cleanup_attr), r->pool);
    apr_threadattr_stacksize_set(r->cleanup_attr, stacksize);

    apr_thread_create(&(r->cleanup_thread), r->cleanup_attr, resource_cleanup_thread, (void *) r,
                      r->pool);
}
