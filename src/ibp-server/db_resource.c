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

#define db_txn(str, err)       \
  DB_TXN  *txn = NULL;                \
  err = dbr->dbenv->txn_begin(dbr->dbenv, NULL, &txn, 0); \
  if (err != 0) {                     \
     log_printf(0, "%s: Transaction begin failed with err %d\n", str, err); \
     return(err);                     \
  }

#define db_commit(str, err)      \
  if (err != 0) {                \
     err = txn->commit(txn, 0);  \
     if (err != 0) {             \
        log_printf(0, "%s: Transaction commit failed with err %d\n", str, err); \
        return(1);               \
     }                           \
  } else {                       \
    txn->abort(txn);             \
  }


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

char *update_expiresoft_db(leveldb_t *db, leveldb_writeoptions_t *opts, osd_id_t id, uint32_t new_time, uint32_t old_time)
{
    DB_timekey_t key;
    char *errstr = NULL;

    //** Delete the old key if needed
    if ((old_time > 0) && (old_time != new_time)) {
        leveldb_delete(db, opts, fill_timekey(&key, old_time, id), sizeof(key), &errstr);
        if (errstr != NULL) { free(errstr); errstr = NULL; }
    }

    //** Add the entry
    leveldb_put(db, opts, fill_timekey(&key, new_time, id), sizeof(key), (const char *)NULL, 0, &errstr);

    return(errstr);
}

//*************************************************************************
// create_expiresoft_db - Creates  a LevelDB database using the given path
//*************************************************************************

leveldb_t *create_expiresoft_db(char *db_path, leveldb_comparator_t *cmp)
{
    leveldb_t *db;
    leveldb_options_t *opt_exists, *opt_create, *opt_none;
    char *errstr = NULL;

    opt_exists = leveldb_options_create(); leveldb_options_set_error_if_exists(opt_exists, 1);
    opt_create = leveldb_options_create(); leveldb_options_set_create_if_missing(opt_create, 1);
    leveldb_options_set_comparator(opt_create, cmp);
    opt_none = leveldb_options_create();

    db = leveldb_open(opt_exists, db_path, &errstr);
    if (errstr != NULL) {  //** It already exists so need to remove it first
        free(errstr);
        errstr = NULL;

        //** Remove it
        leveldb_destroy_db(opt_none, db_path, &errstr);
        if (errstr != NULL) {  //** Got an error so just kick out
            fprintf(stderr, "ERROR: Failed removing %s for fresh DB. ERROR:%s\n", db_path, errstr);
            exit(1);
        }

        //** Try opening it again
        db = leveldb_open(opt_create, db_path, &errstr);
        if (errstr != NULL) {  //** An ERror occured
            fprintf(stderr, "ERROR: Failed creating %s. ERROR:%s\n", db_path, errstr);
            exit(1);
        }
    }

    leveldb_options_destroy(opt_none);
    leveldb_options_destroy(opt_exists);
    leveldb_options_destroy(opt_create);

    return(db);
}

//***************************************************************************
// compare_idmod - Compares the allocation IDs using the modulo for fast rebuilds
//***************************************************************************

#if (DB_VERSION_MAJOR > 5)
int compare_idmod(DB *db, const DBT *k1, const DBT *k2, size_t *locp)
#else
int compare_idmod(DB *db, const DBT *k1, const DBT *k2)
#endif
{
    osd_id_t id1, id2;
    int mod1, mod2;

    memcpy(&id1, k1->data, sizeof(osd_id_t));
    memcpy(&id2, k2->data, sizeof(osd_id_t));

    mod1 = id1 % 256;  //** DIR_MAX=256 and this is a hack that will get properly
    mod2 = id2 % 256;  //** in subsequent commits

    if (mod1 < mod2) return(-1);
    if (mod1 > mod2) return(1);

    if (id1 < id2) return(-1);
    if (id1 > id2) return(1);

    return(0);
}

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
    u_int32_t flags;
    int err;
    char fname[2048];
    char buffer[10 * 1024];
    int used;
    char *errstr = NULL;

    if (strlen(loc) > 2000) {
        printf("mkfs_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(loc));
        abort();
    }

    flags = DB_CREATE | DB_THREAD;

    dbres->n_partitions = n_partitions;

    //*** Create/Open the primary DB containing the ID's ***
    assert_result(db_create(&(dbres->pdb), NULL, 0), 0);
    assert_result(dbres->pdb->set_bt_compare(dbres->pdb, compare_idmod), 0);
    assert_result(dbres->pdb->set_pagesize(dbres->pdb, 32 * 1024), 0);
    snprintf(fname, sizeof(fname), "%s/id.db", loc); remove(fname);
    if ((err = dbres->pdb->open(dbres->pdb, NULL, fname, NULL, DB_BTREE, flags, 0)) != 0) {
        printf("mkfs_db: Can't create primary DB: %s\n", fname);
        printf("mkfs_db: %s\n", db_strerror(err));
        abort();
    }

    //*** Make the expire and soft DBs.  These are always a full wipe
    dbres->expire_compare = leveldb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/history", loc);
    dbres->expire = create_expiresoft_db(fname, dbres->expire_compare);
    if (dbres->expire != NULL) {
        printf("mkfs_db: Error creating expire DB: %s\n", fname);
        abort();
    }

    dbres->soft_compare = leveldb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/soft", loc);
    dbres->soft = create_expiresoft_db(fname, dbres->expire_compare);
    if (dbres->expire != NULL) {
        printf("mkfs_db: Error creating soft DB: %s\n", fname);
        abort();
    }

    //** And the history DB
    dbres->opts = leveldb_options_create();
    leveldb_options_set_create_if_missing(dbres->opts, 1);
    dbres->history_compare = leveldb_comparator_create(dbres, db_history_compare_destroy,
        db_history_compare_op, db_history_compare_name);
    leveldb_options_set_comparator(dbres->opts, dbres->history_compare);
    snprintf(fname, sizeof(fname), "%s/history", loc);
    dbres->history = leveldb_open(dbres->opts, fname, &errstr);
    if (errstr != NULL) {
        printf("mkfs_db: Error creating history DB: %s\n", fname);
        printf("mkfs_db: Error: %s\n", errstr);
        free(errstr);
        abort();
    }

    //** And the generic RW opt
    dbres->wopts = leveldb_writeoptions_create();
    dbres->ropts = leveldb_readoptions_create();

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

int mount_db_generic(tbx_inip_file_t *kf, const char *kgroup, DB_env_t *env,
                     DB_resource_t *dbres, int wipe_clean, int n_partitions)
{
    char fname[2048];
    u_int32_t flags;
    DB_env_t *lenv;

    //** Get the directory containing everything **
    dbres->kgroup = strdup(kgroup);
    log_printf(10, "mount_db_generic: kgroup=%s\n", kgroup);
    tbx_log_flush();
    assert_result_not_null(dbres->loc = tbx_inip_get_string(kf, kgroup, "loc", NULL));

    dbres->n_partitions = (n_partitions <= 0) ? n_partitions : tbx_inip_get_integer(kf, kgroup, "n_partitions", 256);

    log_printf(15, "mound_db_generic: wipe_clean=%d\n", wipe_clean);

    dbres->env = env;
    if (env->dbenv == NULL) {   //** Got to make a local environment
        log_printf(10,
                   "mount_db_generic:  Creating local DB environment. loc=%s max_size=" ST
                   " wipe_clean=%d\n", dbres->loc, env->max_size, wipe_clean);
        lenv = create_db_env(dbres->loc, env->max_size, wipe_clean);
        lenv->local = 1;
        dbres->env = lenv;
    }

    dbres->dbenv = dbres->env->dbenv;

    //** Now open everything up and associate it **
    if (strlen(dbres->loc) > 2000) {
        printf("mount_db:  Need to increase fname size.  strlen(loc)=" ST "\n", strlen(dbres->loc));
        abort();
    }

    flags = DB_AUTO_COMMIT | DB_THREAD;
    if (wipe_clean != 0) flags |= DB_CREATE;  //** Only wipe if needed

    //*** Create/Open the primary DB containing the ID's ***
    assert_result(db_create(&(dbres->pdb), dbres->dbenv, 0), 0);
    assert_result(dbres->pdb->set_pagesize(dbres->pdb, 32 * 1024), 0);
    assert_result(dbres->pdb->set_bt_compare(dbres->pdb, compare_idmod), 0);
    snprintf(fname, sizeof(fname), "%s/id.db", dbres->loc);
    if (wipe_clean) remove(fname);
    if (dbres->pdb->open(dbres->pdb, NULL, fname, NULL, DB_BTREE, flags, 0) != 0) {
        printf("mount_db: Can't open primary DB: %s\n", fname);
        abort();
    }

    //*** Make the expire and soft DBs.  These are always a full wipe
    dbres->expire_compare = leveldb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/expire", dbres->loc);
    dbres->expire = create_expiresoft_db(fname, dbres->expire_compare);
    if (dbres->expire == NULL) {
        printf("mount_db: Error creating expire DB: %s\n", fname);
        abort();
    }

    dbres->soft_compare = leveldb_comparator_create(NULL, db_expire_compare_destroy,
        db_expire_compare_op, db_expire_compare_name);
    snprintf(fname, sizeof(fname), "%s/soft", dbres->loc);
    dbres->soft = create_expiresoft_db(fname, dbres->expire_compare);
    if (dbres->expire == NULL) {
        printf("mount_db: Error creating soft DB: %s\n", fname);
        abort();
    }

    //** And the history DB
    char *errstr = NULL;
    dbres->opts = leveldb_options_create();
    leveldb_options_set_create_if_missing(dbres->opts, 1);
    dbres->history_compare = leveldb_comparator_create(dbres, db_history_compare_destroy,
        db_history_compare_op, db_history_compare_name);
    leveldb_options_set_comparator(dbres->opts, dbres->history_compare);
    snprintf(fname, sizeof(fname), "%s/history", dbres->loc);
    dbres->history = leveldb_open(dbres->opts, fname, &errstr);
    if (errstr != NULL) {
        printf("mount_db: Error opening history DB: %s\n", fname);
        printf("mount_db: Error: %s\n", errstr);
        free(errstr);
        abort();
    }

    //** And the generic RW opt
    dbres->wopts = leveldb_writeoptions_create();
    dbres->ropts = leveldb_readoptions_create();

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
    int i, err, val;

    err = 0;

    val = dbres->pdb->close(dbres->pdb, 0);
    if (val != 0) {
        err++;
        log_printf(0, "ERROR closing main DB err=%d\n", val);
    }

    if (dbres->env != NULL) {
        if (dbres->env->local == 1) {   //(** Local DB environment so shut it down as well
            log_printf(15, "Closing local environment\n");
            if ((i = close_db_env(dbres->env)) != 0) {
                log_printf(0, "ERROR closing DB envirnment!  Err=%d\n", i);
                err++;
            }
        }
    }

    leveldb_close(dbres->expire);
    leveldb_close(dbres->soft);
    leveldb_close(dbres->history);

    leveldb_comparator_destroy(dbres->expire_compare);
    leveldb_comparator_destroy(dbres->soft_compare);
    leveldb_comparator_destroy(dbres->history_compare);
    leveldb_writeoptions_destroy(dbres->wopts);
    leveldb_readoptions_destroy(dbres->ropts);
    leveldb_options_destroy(dbres->opts);

    apr_thread_mutex_destroy(dbres->mutex);
    apr_pool_destroy(dbres->pool);

    free(dbres->loc);
    free(dbres->kgroup);

    return (err);
}

//---------------------------------------------------------------------------

//***************************************************************************
// wipe_db_env - Removes all env files in the provided dir
//***************************************************************************

void wipe_db_env(const char *loc)
{
    DIR *dir;
    struct dirent entry;
    struct dirent *result;
    int i, ok;
    char fname[4096];

    if (strcmp(loc, "local") == 0)
        return;

    dir = opendir(loc);
    if (dir == NULL) {
        log_printf(0, "wipe_db_env:  opendir(%s) failed!\n", loc);
        return;
    }

    readdir_r(dir, &entry, &result);
    while (result != NULL) {
        ok = 1;
        if ((strncmp("log.", result->d_name, 4) == 0) && (strlen(result->d_name) == 14)) {
            for (i = 4; i < 14; i++) {
                if (!apr_isdigit(result->d_name[i]))
                    ok = 0;
            }
        } else if ((strncmp("__db.", result->d_name, 5) == 0) && (strlen(result->d_name) == 8)) {
            for (i = 5; i < 8; i++) {
                if (!apr_isdigit(result->d_name[i]))
                    ok = 0;
            }
        } else {
            ok = 0;
        }

        if (ok == 1) {
            snprintf(fname, sizeof(fname) - 1, "%s/%s", loc, result->d_name);
            remove(fname);
        }

        readdir_r(dir, &entry, &result);
    }

    closedir(dir);
}


//***************************************************************************
// create_db_env - Creates the DB environment
//***************************************************************************

DB_env_t *create_db_env(const char *loc, int db_mem, int wipe_clean)
{
    u_int32_t flags;
    DB_ENV *dbenv = NULL;
    int err;
    DB_env_t *env;

    tbx_type_malloc_clear(env, DB_env_t, 1);

    env->dbenv = NULL;
    env->max_size = db_mem;
    env->local = 0;

    if (strcmp(loc, "local") == 0)
        return (env);

    if (wipe_clean) wipe_db_env(loc);       //** Wipe if requested

    flags = DB_CREATE | DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL |
            DB_INIT_TXN | DB_THREAD | DB_AUTO_COMMIT | DB_RECOVER;

    assert_result(db_env_create(&dbenv, 0), 0);
    env->dbenv = dbenv;

    u_int32_t gbytes = db_mem / 1024;
    u_int32_t bytes = (db_mem % 1024) * 1024 * 1024;
    log_printf(10, "create_db_env: gbytes=%u bytes=%u\n", gbytes, bytes);
    assert_result(dbenv->set_cachesize(dbenv, gbytes, bytes, 1), 0);
    assert_result(dbenv->log_set_config(dbenv, DB_LOG_AUTO_REMOVE, 1), 0);
    if ((err = dbenv->open(dbenv, loc, flags, 0)) != 0) {
        printf("create_db_env: Warning!  No environment located in %s\n", loc);
        printf("create_db_env: Attempting to create a new environment.\n");
        printf("create_Db_env: DB error: %s\n", db_strerror(err));

        DIR *dir = NULL;
        mkdir(loc, S_IRWXU);
        assert_result_not_null(dir = opendir(loc));     //Make sure I can open it
        closedir(dir);

        if ((err = dbenv->open(dbenv, loc, flags, 0)) != 0) {
            printf("create_db_env: Error opening DB environment!  loc=%s\n", loc);
            printf("create_db_env: %s\n", db_strerror(err));
        }
    }

    return (env);
}

//***************************************************************************
// close_db_env - Closes the DB environment
//***************************************************************************

int close_db_env(DB_env_t *env)
{
    int err = 0;

log_printf(0, "CLOSING\n");
    if (env->dbenv != NULL) {
        err = env->dbenv->close(env->dbenv, 0);
    }

    free(env);

    return (err);
}

//***************************************************************************
// print_db - Prints the DB information out to fd.
//***************************************************************************

int print_db(DB_resource_t *db, FILE *fd)
{
    fprintf(fd, "DB location: %s\n", db->loc);

    db->pdb->stat_print(db->pdb, 0);
    db->dbenv->stat_print(db->dbenv, DB_STAT_ALL);

    return (0);
}

//***************************************************************************
// get_num_allocations_db - Returns the number of allocations according to
//    primary DB
//***************************************************************************

int get_num_allocations_db(DB_resource_t *db)
{
    int n, err;
    DB_BTREE_STAT *dstat;
    u_int32_t flags = 0;


    dbr_lock(db);
    err = db->pdb->stat(db->pdb, NULL, (void *) &dstat, flags);
    if (err != 0) {
        log_printf(0, "get_allocations_db:  error=%d  (%s)\n", err, db_strerror(err));
    }
    dbr_unlock(db);

    n = -1;
    if (err == 0) {
        n = dstat->bt_nkeys;
        free(dstat);
        log_printf(10, "get_allocations_db: nkeys=%d\n", n);
    }

    return (n);
}


//---------------------------------------------------------------------------

//***************************************************************************
// _get_alloc_with_id_db - Returns the alloc with the given ID from the DB
//      internal version that does no locking
//***************************************************************************

int _get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc)
{
    DBT key, data;
    int err;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = &id;
    key.ulen = sizeof(osd_id_t);
    key.flags = DB_DBT_USERMEM;

    data.data = alloc;
    data.ulen = sizeof(Allocation_t);
    data.flags = DB_DBT_USERMEM;

    err = dbr->pdb->get(dbr->pdb, NULL, &key, &data, 0);
    if (err != 0) {
        log_printf(10, "_get_alloc_with_id_db:  Unknown ID=" LU " error=%d  (%s)\n", id, err,
                   db_strerror(err));
    }

    return (err);
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
    int err;
    char *errstr;
    DBT key, data;

    fill_timekey(&(a->expirekey), a->expiration, a->id);
    if (a->reliability == ALLOC_SOFT)
        fill_timekey(&(a->softkey), a->expiration, a->id);

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = &(a->id);
    key.size = sizeof(osd_id_t);

    data.data = a;
    data.size = sizeof(Allocation_t);

    //** Update the DB
    if ((err = dbr->pdb->put(dbr->pdb, NULL, &key, &data, 0)) != 0) {
        log_printf(10, "put_alloc_db: Error storing primary key: %d id=" LU "\n", err, a->id);
        return (err);
    }

    //** And the expiration
    if (a->expiration != old_expiration) {
        errstr = update_expiresoft_db(dbr->expire, dbr->wopts, a->id, a->expiration, old_expiration);
        if (errstr) {
            log_printf(1, "ERROR: Failed updating expiration: id=" LU " error: %s\n", a->id, errstr);
            free(errstr);
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
               "put_alloc_db: err=%d  id=" LU ", r=%s w=%s m=%s a.size=" LU " a.max_size=" LU
               " expire=" TT "\n", err, a->id, a->caps[READ_CAP].v, a->caps[WRITE_CAP].v,
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
// modify_alloc_iter_db - Replaces the current allocation pointed top by the iterator
//***************************************************************************

int modify_alloc_iter_db(DB_iterator_t *it, Allocation_t *a)
{
    int err;
    DBT data;

    debug_printf(10, "modify_alloc_iter_db: Start\n");

    fill_timekey(&(a->expirekey), a->expiration, a->id);
    if (a->reliability == ALLOC_SOFT)
        fill_timekey(&(a->softkey), a->expiration, a->id);

    memset(&data, 0, sizeof(DBT));
    data.data = a;
    data.size = sizeof(Allocation_t);

    err = it->cursor->put(it->cursor, NULL, &data, DB_CURRENT);
    if (err != 0) {
        log_printf(0, "modify_alloc_iter_db: %s\n", db_strerror(err));
    }

    return (err);
}

//***************************************************************************
// remove_alloc_iter_db - Removes the given key from the DB with an iter
//***************************************************************************

int remove_alloc_iter_db(DB_iterator_t *it)
{
    int err;

    debug_printf(10, "_remove_alloc_iter_db: Start\n");

    err = it->cursor->c_del(it->cursor, 0);
    if (err != 0) {
        log_printf(0, "remove_alloc_iter_db: %s\n", db_strerror(err));
    }

    return (err);
}

//***************************************************************************
// _remove_alloc_db - Removes the given key from the DB
//***************************************************************************

int _remove_alloc_db(DB_resource_t *dbr, Allocation_t *alloc)
{
    DBT key;
    int err;
    char *errstr;
    DB_timekey_t tkey;

    memset(&key, 0, sizeof(DBT));
    key.data = &(alloc->id);
    key.size = sizeof(osd_id_t);

    err = dbr->pdb->del(dbr->pdb, NULL, &key, 0);
    if (err != 0) {
        log_printf(0, "remove_alloc_db: %s\n", db_strerror(err));
    }

    errstr = NULL;
    leveldb_delete(dbr->expire, dbr->wopts, fill_timekey(&tkey, alloc->expiration, alloc->id), sizeof(tkey), &errstr);
    if (errstr != NULL) { free(errstr); errstr = NULL; }
    if (alloc->reliability == ALLOC_SOFT) {
        errstr = NULL;
        leveldb_delete(dbr->soft, dbr->wopts, (const char *)&tkey, sizeof(tkey), &errstr);
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
        log_printf(0, "create_alloc_db:  Error in DB put - %s\n", db_strerror(err));
    }

    dbr_unlock(dbr);

    return (err);
}

//***************************************************************************
// db_iterator_begin - Returns an iterator to cycle through the DB
//***************************************************************************

DB_iterator_t *db_iterator_begin(DB_resource_t *dbr, DB *db, DB_ENV *dbenv, int index)
{
    DB_iterator_t *it;
    int err;

    tbx_type_malloc_clear(it, DB_iterator_t, 1);
    err = dbenv->txn_begin(dbenv, NULL, &(it->transaction), 0);
    if (err != 0) {
        log_printf(0, "db_iterator_begin: index=%d Transaction begin failed with err %s (%d)\n",
                   index, db_strerror(err), err);
        return (NULL);
    }

    err = db->cursor(db, it->transaction, &(it->cursor), 0);
    if (err != 0) {
        log_printf(0, "db_iterator_begin: index=%d cursor failed with err %s (%d)\n", index,
                   db_strerror(err), err);
        return (NULL);
    }


    it->id = rand();
    it->db_index = index;
    it->dbr = dbr;

    debug_printf(10, "db_iterator_begin:  id=%d\n", it->id);

    return (it);
}

//***************************************************************************
// db_iterator_end - Closes an iterator
//***************************************************************************

int db_iterator_end(DB_iterator_t *it)
{
    int err;

    debug_printf(10, "db_iterator_end:  id=%d\n", it->id);

    if (it->it) {
        leveldb_iter_destroy(it->it);
        free(it);
        return(0);
    }

    it->cursor->c_close(it->cursor);

    err = it->transaction->commit(it->transaction, DB_TXN_SYNC);
    if (err != 0) {
        log_printf(0, "db_iterator_end: Transaction commit failed with err %s (%d)\n",
                   db_strerror(err), err);
    }

    free(it);


    return (err);
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
    DBT key, data;
    int err;
    osd_id_t id;
    DB_timekey_t *tkey;
    size_t nbytes;

    err = -1234;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.flags = DB_DBT_USERMEM;
    data.flags = DB_DBT_USERMEM;

    data.data = a;
    data.ulen = sizeof(Allocation_t);

    switch (it->db_index) {
    case (DB_INDEX_ID):
        key.data = &id;
        key.ulen = sizeof(id);

        err = it->cursor->get(it->cursor, &key, &data, direction);      //** Read the 1st
        if (err != 0) {
            log_printf(10, "key_index=%d err = %s\n", it->db_index,
                       db_strerror(err));
        }
        return (err);
        break;
    case DB_INDEX_EXPIRE:
    case DB_INDEX_SOFT:
        //** Kick out  if reached the end
        if (leveldb_iter_valid(it->it) == 0) return(1);

        nbytes = 0;
        tkey = (DB_timekey_t *)leveldb_iter_key(it->it, &nbytes);
        if (nbytes == 0) return(1);

        id = tkey->id;  //** Preserve the ID because the next call changes the contents
        leveldb_iter_next(it->it);
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
    it->it = leveldb_create_iterator(dbr->expire, dbr->ropts);
    leveldb_iter_seek(it->it, fill_timekey(&key, 0, 0), sizeof(key));

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
    it->it = leveldb_create_iterator(dbr->soft, dbr->ropts);
    leveldb_iter_seek(it->it, fill_timekey(&key, 0, 0), sizeof(key));

    return(it);
}

//***************************************************************************
// id_iterator - Returns a handle to iterate through all the id's
//***************************************************************************

DB_iterator_t *id_iterator(DB_resource_t *dbr)
{
    return (db_iterator_begin(dbr, dbr->pdb, dbr->dbenv, DB_INDEX_ID));
}

//***************************************************************************
// set_id_iterator - Sets the position for the ID iterator
//***************************************************************************

int set_id_iterator(DB_iterator_t *dbi, osd_id_t id, Allocation_t *a)
{
    int err;
    DBT key, data;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.flags = DB_DBT_USERMEM;
    data.flags = DB_DBT_USERMEM;

    key.ulen = sizeof(osd_id_t);
    key.data = &id;
    data.ulen = sizeof(Allocation_t);
    data.data = a;

    if ((err = dbi->cursor->get(dbi->cursor, &key, &data, DB_SET_RANGE)) != 0) {
        log_printf(5, "Error with get!  error=%d\n", err);
        return (err);
    }

    return (0);
}

//***************************************************************************
// set_expire_iterator - Sets the position for the hard iterator
//***************************************************************************

int set_expire_iterator(DB_iterator_t *dbi, ibp_time_t t, Allocation_t *a)
{
    DB_timekey_t key;

    leveldb_iter_seek(dbi->it, fill_timekey(&key, t, a->id), sizeof(key));

    log_printf(15, "set_expire_iterator: t=" TT " id=" LU "\n", ibp2apr_time(t), a->id);

    return (0);
}
