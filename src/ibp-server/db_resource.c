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

#include "db_resource.h"
#include "allocation.h"
#include "random.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include "debug.h"
#include <tbx/append_printf.h>
#include <tbx/type_malloc.h>
#include "ibp_time.h"
#include <apr_time.h>
#include <apr_base64.h>
#include <apr_lib.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <signal.h>

#define DB_INDEX_ID     0
#define DB_INDEX_EXPIRE 1
#define DB_INDEX_SOFT   2

apr_thread_mutex_t *dbr_mutex = NULL;   //** Only used if testing a common lock

//***************************************************************************
//  dbr_lock - Locks the DB
//***************************************************************************

void dbr_lock(DB_resource_t *dbr)
{
    apr_thread_mutex_lock(dbr->mutex);
}

//***************************************************************************
//  dbr_unlock - Locks the DB
//***************************************************************************

void dbr_unlock(DB_resource_t *dbr)
{
    apr_thread_mutex_unlock(dbr->mutex);
}

//***************************************************************************
// fill_timekey - Fills a DB_timekey_t strcuture
//***************************************************************************

const char *fill_timekey(DB_timekey_t *tk, ibp_time_t t, osd_id_t id)
{
    memset(tk, 0, sizeof(DB_timekey_t));

    tk->time = t;
    tk->id = id;

    return ((const char *)tk);
}

//****************************************************************************
// db_expire_comparator - these are the comparator routines
//****************************************************************************

int db_expire_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    DB_timekey_t *akey = (DB_timekey_t *)a;
    DB_timekey_t *bkey = (DB_timekey_t *)b;


    //** Types are the same so compare based on the time
    if (akey->time > bkey->time) {
        return(1);
    } else if (akey->time < bkey->time) {
        return(-1);
    }

    //** If made it here then the times are the same so sort by the ID
    if (akey->id > bkey->id) {
        return(1);
    } else if (akey->id < bkey->id) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

void db_expire_compare_destroy(void *arg) { return; }
const char *db_expire_compare_name(void *arg) { return("expire"); }


//*************************************************************************
// update_expiresoft_db - Updates the expiration entry for the DB
//*************************************************************************

char *update_expiresoft_db(rocksdb_t *db, rocksdb_writeoptions_t *opts, osd_id_t id, uint32_t new_time, uint32_t old_time)
{
    DB_timekey_t key;
    char *errstr = NULL;

    //** Delete the old key if needed
    if ((old_time > 0) && (old_time != new_time)) {
        rocksdb_delete(db, opts, fill_timekey(&key, old_time, id), sizeof(key), &errstr);
        if (errstr != NULL) { free(errstr); errstr = NULL; }
    }

    //** Add the entry
    rocksdb_put(db, opts, fill_timekey(&key, new_time, id), sizeof(key), (const char *)NULL, 0, &errstr);

    return(errstr);
}

//*************************************************************************
// open_db - Opens the RocksDB database using the given path
//*************************************************************************

rocksdb_t *open_db(char *db_path, rocksdb_comparator_t *cmp, int wipe_clean)
{
    rocksdb_t *db;
    rocksdb_options_t *opts, *opts2;
    char *errstr = NULL;

    if (wipe_clean != 0) { //** Wipe the DB if requested
        opts2 = rocksdb_options_create();
        rocksdb_options_set_error_if_exists(opts2, 1);

        db = rocksdb_open(opts2, db_path, &errstr);
        if (errstr != NULL) {  //** It already exists so need to remove it first
            free(errstr);
            errstr = NULL;

            //** Remove it
            rocksdb_destroy_db(opts2, db_path, &errstr);
            if (errstr != NULL) {  //** Got an error so just kick out
                fprintf(stderr, "ERROR: Failed removing %s for fresh DB. DB error:%s\n", db_path, errstr);
                exit(1);
            }
        }
        rocksdb_options_destroy(opts2);
    }

    //** Try opening it for real
    opts = rocksdb_options_create();
    rocksdb_options_set_comparator(opts, cmp);
    rocksdb_options_set_create_if_missing(opts, 1);

    db = rocksdb_open(opts, db_path, &errstr);
    if (errstr != NULL) {  //** An Error occured
        fprintf(stderr, "ERROR: Failed Opening/Creating %s. DB error:%s\n", db_path, errstr);
        exit(1);
    }

    rocksdb_options_destroy(opts);

    return(db);
}

//****************************************************************************
// db_id_compare_op - Comparator routine for the ID DB
//****************************************************************************

int db_id_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    DB_resource_t *dbres = (DB_resource_t *)arg;
    osd_id_t aid = *(osd_id_t *)a;
    osd_id_t bid = *(osd_id_t *)b;

    int a_part, b_part;

    //** check based on the ID partition
    a_part = aid % dbres->n_partitions;
    b_part = bid % dbres->n_partitions;
    if (a_part > b_part) {
        return(1);
    } else if (a_part < b_part) {
        return(-1);
    }

    //** If made it here then the ID partitions are the same so check using the ID
    if (aid > bid) {
        return(1);
    } else if (aid < bid) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

void db_id_compare_destroy(void *arg) { return; }
const char *db_id_compare_name(void *arg) { return("ID"); }

//***************************************************************************
// print_db_resource - Prints the DB resource
//***************************************************************************

int print_db_resource(char *buffer, int *used, int nbytes, DB_resource_t *dbr)
{
    int i;
    tbx_append_printf(buffer, used, nbytes, "[%s]\n", dbr->kgroup);
    i = tbx_append_printf(buffer, used, nbytes, "loc = %s\n", dbr->loc);

    return (i);
}


//***************************************************************************
// mkfs_db - Creates a new DB resource
//      loc  - Directory to store the DB files
//      type - Right now this is ignored and dhould be "db"
//      fd   - Key file to store any DB related keys in.
//***************************************************************************

int mkfs_db(DB_resource_t *dbres, char *loc, const char *kgroup, FILE *fd, int n_partitions)
{
    char fname[2048];
    char buffer[10 * 1024];
    int used;

    if (strlen(loc) > 2000) {
        printf("mkfs_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(loc));
        abort();
    }

    dbres->n_partitions = n_partitions;

    //*** Create/Open the primary DB containing the ID's ***
    dbres->id_compare = rocksdb_comparator_create(dbres, db_id_compare_destroy,
        db_id_compare_op, db_id_compare_name);
    snprintf(fname, sizeof(fname), "%s/id", loc);
    dbres->pdb = open_db(fname, dbres->id_compare, 1);
    if (dbres->pdb != NULL) {
        printf("mkfs_db: Error creating history DB: %s\n", fname);
        abort();
    }

    //*** Make the expire and soft DBs.  These are always a full wipe
    dbres->expire_compare = rocksdb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/history", loc);
    dbres->expire = open_db(fname, dbres->expire_compare, 1);
    if (dbres->expire != NULL) {
        printf("mkfs_db: Error creating expire DB: %s\n", fname);
        abort();
    }

    dbres->soft_compare = rocksdb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/soft", loc);
    dbres->soft = open_db(fname, dbres->soft_compare, 1);
    if (dbres->expire != NULL) {
        printf("mkfs_db: Error creating soft DB: %s\n", fname);
        abort();
    }

    //** And the history DB
    dbres->history_compare = rocksdb_comparator_create(dbres, db_history_compare_destroy,
        db_history_compare_op, db_history_compare_name);
    snprintf(fname, sizeof(fname), "%s/history", loc);
    dbres->history= open_db(fname, dbres->history_compare, 0);
    if (dbres->history == NULL) {
        printf("mkfs_db: Error creating history DB: %s\n", fname);
        abort();
    }

    //** And the generic RW opt
    dbres->wopts = rocksdb_writeoptions_create();
    dbres->ropts = rocksdb_readoptions_create();

    //*** Lastly add the group to the Key file ***
    dbres->loc = strdup(loc);
    dbres->kgroup = strdup(kgroup);

    //** and make the mutex
    apr_pool_create(&(dbres->pool), NULL);
    apr_thread_mutex_create(&(dbres->mutex), APR_THREAD_MUTEX_DEFAULT, dbres->pool);

    used = 0;
    print_db_resource(buffer, &used, sizeof(buffer), dbres);
    if (fd != NULL)
        fprintf(fd, "%s", buffer);

    return (0);
}

//***************************************************************************
// mount_db_generic - Mounts a DB for use using the keyfile for the location
//    and optionally wipes the data files
//***************************************************************************

int mount_db_generic(tbx_inip_file_t *kf, const char *kgroup,
                     DB_resource_t *dbres, int wipe_clean, int n_partitions)
{
    char fname[2048];

    //** Get the directory containing everything **
    dbres->kgroup = strdup(kgroup);
    log_printf(10, "mount_db_generic: kgroup=%s\n", kgroup);
    tbx_log_flush();
    assert_result_not_null(dbres->loc = tbx_inip_get_string(kf, kgroup, "loc", NULL));

    dbres->n_partitions = (n_partitions <= 0) ? n_partitions : tbx_inip_get_integer(kf, kgroup, "n_partitions", 256);

    log_printf(15, "mound_db_generic: wipe_clean=%d\n", wipe_clean);

    //** Now open everything up and associate it **
    if (strlen(dbres->loc) > 2000) {
        printf("mount_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(dbres->loc));
        abort();
    }

    //*** Create/Open the primary DB containing the ID's ***
    dbres->id_compare = rocksdb_comparator_create(dbres, db_id_compare_destroy,
        db_id_compare_op, db_id_compare_name);
    snprintf(fname, sizeof(fname), "%s/id", dbres->loc);
    dbres->pdb = open_db(fname, dbres->id_compare, wipe_clean);
    if (dbres->pdb == NULL) {
        printf("mount_db: Error creating ID DB: %s\n", fname);
        abort();
    }

    //*** Make the expire and soft DBs.  These are always a full wipe
    dbres->expire_compare = rocksdb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/expire", dbres->loc);
    dbres->expire = open_db(fname, dbres->expire_compare, 1);
    if (dbres->expire == NULL) {
        printf("mount_db: Error creating expire DB: %s\n", fname);
        abort();
    }

    dbres->soft_compare = rocksdb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/soft", dbres->loc);
    dbres->soft = open_db(fname, dbres->soft_compare, 1);
    if (dbres->soft == NULL) {
        printf("mount_db: Error creating soft DB: %s\n", fname);
        abort();
    }

    //** And the history DB
    dbres->history_compare = rocksdb_comparator_create(dbres, db_history_compare_destroy,
        db_history_compare_op, db_history_compare_name);
    snprintf(fname, sizeof(fname), "%s/history", dbres->loc);
    dbres->history = open_db(fname, dbres->history_compare, 0);
    if (dbres->history == NULL) {
        printf("mount_db: Error opening history DB: %s\n", fname);
        abort();
    }

    //** And the generic RW opt
    dbres->wopts = rocksdb_writeoptions_create();
    dbres->ropts = rocksdb_readoptions_create();

    //** and make the mutex
    apr_pool_create(&(dbres->pool), NULL);
    apr_thread_mutex_create(&(dbres->mutex), APR_THREAD_MUTEX_DEFAULT, dbres->pool);

    return (0);
}

//***************************************************************************
// umount_db - Unmounts the given DB
//***************************************************************************

int umount_db(DB_resource_t *dbres)
{
    rocksdb_cancel_all_background_work(dbres->pdb, 1);     rocksdb_close(dbres->pdb);
    rocksdb_cancel_all_background_work(dbres->expire, 1);  rocksdb_close(dbres->expire);
    rocksdb_cancel_all_background_work(dbres->soft, 1);    rocksdb_close(dbres->soft);
    rocksdb_cancel_all_background_work(dbres->history, 1); rocksdb_close(dbres->history);

    rocksdb_comparator_destroy(dbres->id_compare);
    rocksdb_comparator_destroy(dbres->expire_compare);
    rocksdb_comparator_destroy(dbres->soft_compare);
    rocksdb_comparator_destroy(dbres->history_compare);
    rocksdb_writeoptions_destroy(dbres->wopts);
    rocksdb_readoptions_destroy(dbres->ropts);

    apr_thread_mutex_destroy(dbres->mutex);
    apr_pool_destroy(dbres->pool);

    free(dbres->loc);
    free(dbres->kgroup);

    return (0);
}

//---------------------------------------------------------------------------

//***************************************************************************
// _get_alloc_with_id_db - Returns the alloc with the given ID from the DB
//      internal version that does no locking
//***************************************************************************

int _get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc)
{
    void *ptr;
    size_t nbytes;
    char *errstr;

    nbytes = 0;
    ptr = NULL;
    errstr = NULL;
    ptr = rocksdb_get(dbr->pdb, dbr->ropts, (void *)&id, sizeof(id), &nbytes, &errstr);
    if (errstr) {
        log_printf(10, "Unknown ID: " LU " errstr=%s\n", id, errstr);
        if (ptr) free(ptr);
        free(errstr);
        return(1);
    }

    //** Make sure the record is the right size
    if (nbytes != sizeof(Allocation_t)) {
        log_printf(0, "ERROR: id=" LU " Record size incorrect size! got=" ST " should be " ST "\n", id, nbytes, sizeof(Allocation_t));
        if (ptr) free(ptr);
        return(2);
    }

    //** If we made it here all is good so just copy the data over.
    memcpy(alloc, ptr, sizeof(Allocation_t));
    free(ptr);

    return (0);
}

//***************************************************************************
// get_alloc_with_id_db - Returns the alloc with the given ID from the DB
//***************************************************************************

int get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc)
{
    dbr_lock(dbr);
    int err = _get_alloc_with_id_db(dbr, id, alloc);
    dbr_unlock(dbr);
    return (err);
}

//***************************************************************************
// rebuild_add_expiration_db - Adds the expiration index
//***************************************************************************

int rebuild_add_expiration_db(DB_resource_t *dbr, Allocation_t *a)
{
    char *errstr;

    errstr = update_expiresoft_db(dbr->expire, dbr->wopts, a->id, a->expiration, 0);
    if (errstr) {
        log_printf(1, "ERROR: Failed updating expiration: id=" LU " error: %s\n", a->id, errstr);
        free(errstr);
    }

    if (a->reliability == ALLOC_SOFT) {
        errstr = update_expiresoft_db(dbr->soft, dbr->wopts, a->id, a->expiration, 0);
        if (errstr) {
            log_printf(1, "ERROR: Failed updating soft expiration: id=" LU " error: %s\n", a->id, errstr);
            free(errstr);
        }
    }

    return(0);
}

//***************************************************************************
// _put_alloc_db - Stores the allocation in the DB
//    Internal routine that performs no locking
//***************************************************************************

int _put_alloc_db(DB_resource_t *dbr, Allocation_t *a, uint32_t old_expiration)
{
    char *errstr;

    fill_timekey(&(a->expirekey), a->expiration, a->id);
    if (a->reliability == ALLOC_SOFT)
        fill_timekey(&(a->softkey), a->expiration, a->id);

    errstr = NULL;

    //** Update the DB
    rocksdb_put(dbr->pdb, dbr->wopts, (void *)&(a->id), sizeof(osd_id_t), (const char *)a, sizeof(Allocation_t), &errstr);
    if (errstr) {
        log_printf(10, "ERROR: Failed storing primary key: id=" LU " error=%s\n", a->id, errstr);
        free(errstr);
        return(1);
    }

    //** And the expiration
    if (a->expiration != old_expiration) {
        errstr = update_expiresoft_db(dbr->expire, dbr->wopts, a->id, a->expiration, old_expiration);
        if (errstr) {
            log_printf(1, "ERROR: Failed updating expiration: id=" LU " error: %s\n", a->id, errstr);
            free(errstr);
            errstr = NULL;
        }

        if (a->reliability == ALLOC_SOFT) {
            errstr = update_expiresoft_db(dbr->soft, dbr->wopts, a->id, a->expiration, old_expiration);
            if (errstr) {
                log_printf(1, "ERROR: Failed updating soft expiration: id=" LU " error: %s\n", a->id, errstr);
                free(errstr);
            }

        }
    }

    apr_time_t t = ibp2apr_time(a->expiration);
    log_printf(10,
               "id=" LU ", r=%s w=%s m=%s a.size=" LU " a.max_size=" LU
               " expire=" TT "\n", a->id, a->caps[READ_CAP].v, a->caps[WRITE_CAP].v,
               a->caps[MANAGE_CAP].v, a->size, a->max_size, t);

    return (0);
}

//***************************************************************************
// put_alloc_db - Stores the allocation in the DB
//***************************************************************************

int put_alloc_db(DB_resource_t *dbr, Allocation_t *a, uint32_t old_expiration)
{
    int err;

    dbr_lock(dbr);
    err = _put_alloc_db(dbr, a, old_expiration);
    dbr_unlock(dbr);

    return (err);
}

//***************************************************************************
// _remove_alloc_db - Removes the given key from the DB
//***************************************************************************

int _remove_alloc_db(DB_resource_t *dbr, Allocation_t *alloc)
{
    int err;
    char *errstr;
    DB_timekey_t tkey;


    err = 0;
    errstr = NULL;
    rocksdb_delete(dbr->pdb, dbr->wopts, (void *)&(alloc->id), sizeof(osd_id_t), &errstr);
    if (errstr) {
        log_printf(0, "ERROR removing key! id=" LU " error=%s\n", alloc->id, errstr);
        free(errstr);
        err = 1;
    }

    errstr = NULL;
    rocksdb_delete(dbr->expire, dbr->wopts, fill_timekey(&tkey, alloc->expiration, alloc->id), sizeof(tkey), &errstr);
    if (errstr != NULL) { free(errstr); errstr = NULL; }
    if (alloc->reliability == ALLOC_SOFT) {
        errstr = NULL;
        rocksdb_delete(dbr->soft, dbr->wopts, (const char *)&tkey, sizeof(tkey), &errstr);
        if (errstr != NULL) { free(errstr); errstr = NULL; }
    }
    return (err);
}

//***************************************************************************
// remove_alloc_db - Removes the given key from the DB
//***************************************************************************

int remove_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
    int err;

    dbr_lock(dbr);
    err = _remove_alloc_db(dbr, a);
    dbr_unlock(dbr);

    return (err);
}

//***************************************************************************
// remove_alloc_iter_db - Removes the given key from the DB with an iter
//***************************************************************************

int remove_alloc_iter_db(DB_iterator_t *it, Allocation_t *a)
{
    return(_remove_alloc_db(it->dbr, a));
}


//***************************************************************************
// modify_alloc_record_db - Stores the modified allocation in the DB
//***************************************************************************

int modify_alloc_db(DB_resource_t *dbr, Allocation_t *a, uint32_t old_expiration)
{
    return (put_alloc_db(dbr, a, old_expiration));
}

//***************************************************************************
// create_alloc_db - Creates the different unique caps and uses the existing
//      info already stored in the prefilled allocation to add an entry into
//      the DB for the resource
//***************************************************************************

int create_alloc_db(DB_resource_t *dbr, Allocation_t *a)
{
    int i, j, err;
    char key[CAP_SIZE], b64[CAP_SIZE + 1];

    dbr_lock(dbr);

    for (i = 0; i < 3; i++) {   //** Get the differnt caps
        tbx_random_get_bytes((void *) key, CAP_BITS / 8);
        err = apr_base64_encode(b64, key, CAP_BITS / 8);
        for (j = 0; j < CAP_SIZE; j++) {
            if (b64[j] == '/') {
                a->caps[i].v[j] = '-';  //**IBP splits using a "/" so need to change it
            } else {
                a->caps[i].v[j] = b64[j];
            }
        }
        a->caps[i].v[CAP_SIZE] = '\0';
    }

    if ((err = _put_alloc_db(dbr, a, 0)) != 0) {   //** Now store it in the DB
        log_printf(0, "create_alloc_db:  Error in DB: %d\n", err);
    }

    dbr_unlock(dbr);

    return (err);
}

//***************************************************************************
// db_iterator_end - Closes an iterator
//***************************************************************************

int db_iterator_end(DB_iterator_t *it)
{
    if (it->it) rocksdb_iter_destroy(it->it);
    free(it);

    return (0);
}

//***************************************************************************
// db_iterator_next - Returns the next record from the DB in the given direction
//
//  NOTE: This actually buffers a response to get around a deadlock with the
//        c_* commands and the normal commands which occurs if the cursor
//        is on the same record as another command is.
//
//  **** If the direction changes in mid-stride this command will not work***
//
//***************************************************************************

int db_iterator_next(DB_iterator_t *it, int direction, Allocation_t *a)
{
    osd_id_t id;
    DB_timekey_t *tkey;
    size_t nbytes;
    Allocation_t *aptr;

    //** Kick out  if reached the end
    if (rocksdb_iter_valid(it->it) == 0) return(1);

    nbytes = 0;

    switch (it->db_index) {
    case (DB_INDEX_ID):
        aptr = (Allocation_t *)rocksdb_iter_value(it->it, &nbytes);
        if (nbytes != sizeof(Allocation_t)) return(1);

        memcpy(a, aptr, sizeof(Allocation_t));
        rocksdb_iter_next(it->it);
        return (0);
        break;
    case DB_INDEX_EXPIRE:
    case DB_INDEX_SOFT:
        tkey = (DB_timekey_t *)rocksdb_iter_key(it->it, &nbytes);
        if (nbytes == 0) return(1);

        id = tkey->id;  //** Preserve the ID because the next call changes the contents
        rocksdb_iter_next(it->it);
        return(_get_alloc_with_id_db(it->dbr, id, a));  //** The iterator already holds the lock.
        break;
    default:
        log_printf(0, "Invalid key_index!  key_index=%d\n", it->db_index);
        return (-1);
    }

    return (0);
}


//***************************************************************************
// expire_iterator - Returns a handle to iterate through the expire DB from
//     oldest to newest times
//***************************************************************************

DB_iterator_t *expire_iterator(DB_resource_t *dbr)
{
    DB_iterator_t *it;
    DB_timekey_t key;

    tbx_type_malloc_clear(it, DB_iterator_t, 1);
    it->id = rand();
    it->db_index = DB_INDEX_EXPIRE;
    it->dbr = dbr;
    it->it = rocksdb_create_iterator(dbr->expire, dbr->ropts);
    rocksdb_iter_seek(it->it, fill_timekey(&key, 0, 0), sizeof(key));

    return(it);
}

//***************************************************************************
// soft_iterator - Returns a handle to iterate through the soft DB from
//     oldest to newest times
//***************************************************************************

DB_iterator_t *soft_iterator(DB_resource_t *dbr)
{
    DB_iterator_t *it;
    DB_timekey_t key;

    tbx_type_malloc_clear(it, DB_iterator_t, 1);
    it->id = rand();
    it->db_index = DB_INDEX_SOFT;
    it->dbr = dbr;
    it->it = rocksdb_create_iterator(dbr->soft, dbr->ropts);
    rocksdb_iter_seek(it->it, fill_timekey(&key, 0, 0), sizeof(key));

    return(it);
}

//***************************************************************************
// id_iterator - Returns a handle to iterate through all the id's
//***************************************************************************

DB_iterator_t *id_iterator(DB_resource_t *dbr)
{
    DB_iterator_t *it;
    osd_id_t id = 0;

    tbx_type_malloc_clear(it, DB_iterator_t, 1);
    it->id = rand();
    it->db_index = DB_INDEX_ID;
    it->dbr = dbr;
    it->it = rocksdb_create_iterator(dbr->pdb, dbr->ropts);
    rocksdb_iter_seek(it->it, (void *)&id, sizeof(id));

    return(it);
}

//***************************************************************************
// set_id_iterator - Sets the position for the ID iterator
//***************************************************************************

int set_id_iterator(DB_iterator_t *dbi, osd_id_t id)
{

    rocksdb_iter_seek(dbi->it, (void *)&id, sizeof(osd_id_t));

    log_printf(15, "id=" LU "\n", id);

    return (0);
}

//***************************************************************************
// set_expire_iterator - Sets the position for the hard iterator
//***************************************************************************

int set_expire_iterator(DB_iterator_t *dbi, ibp_time_t t, Allocation_t *a)
{
    DB_timekey_t key;

    rocksdb_iter_seek(dbi->it, fill_timekey(&key, t, a->id), sizeof(key));

    log_printf(15, "set_expire_iterator: t=" TT " id=" LU "\n", ibp2apr_time(t), a->id);

    return (0);
}
