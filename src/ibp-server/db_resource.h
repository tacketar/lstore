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

//*************************************************************
//*************************************************************

#ifndef _DB_RESOURCE_H_
#define _DB_RESOURCE_H_

#include "visibility.h"
#include <rocksdb/c.h>
#include <apr_thread_mutex.h>
#include <apr_pools.h>
#include "allocation.h"
#include <tbx/iniparse.h>
#include "ibp_time.h"

#define DBR_TYPE_DB "db"
#define DBR_NEXT 1
#define DBR_PREV -1

#define DBR_ITER_INIT   0
#define DBR_ITER_BUFFER 1
#define DBR_ITER_EMPTY  2

typedef struct { //** Key for the history DB
    osd_id_t id;
    char type;
    apr_time_t date;
} db_history_key_t;

typedef struct {                //Resource DB interface
    char *kgroup;               //Ini file group
    char *loc;                  //Directory with all the DB's in it
    rocksdb_t *pdb;             //Primary DB (key=Object ID)
    rocksdb_t *expire;          //DB with expiration as the key
    rocksdb_t *soft;            //Expiration is used as the key but only soft allocs are stored in it
    rocksdb_t *history;         //History DB
    rocksdb_writeoptions_t *wopts; //Generic option for Write
    rocksdb_readoptions_t *ropts; //Generic option for Read
    rocksdb_comparator_t *id_compare;  //History comparator
    rocksdb_comparator_t *history_compare;  //History comparator
    rocksdb_comparator_t *expire_compare;   //Expiriration comparator
    rocksdb_comparator_t *soft_compare;     //Soft expireation comparator
    apr_thread_mutex_t *mutex;  // Lock used for creates
    apr_pool_t *pool;           //** Memory pool
    int n_partitions;           //** Number of load balancing splits for keys
} DB_resource_t;

typedef struct {                //Container for cursor
    DB_resource_t *dbr;
    rocksdb_iterator_t *it;
    int db_index;
    int id;
} DB_iterator_t;

void dbr_lock(DB_resource_t *dbr);
void dbr_unlock(DB_resource_t *dbr);
int print_db_resource(char *buffer, int *used, int nbytes, DB_resource_t *dbr);
int mkfs_db(DB_resource_t *dbr, char *loc, const char *kgroup, FILE *fd, int n_partitions);
int snap_db(DB_resource_t *dbres, char *prefix, FILE *fd);
char *snap_merge_pick(DB_resource_t *dbr);
int mount_db(tbx_inip_file_t *kf, const char *kgroup, DB_resource_t *dbres);
int mount_db_generic(tbx_inip_file_t *kf, const char *kgroup,
                     DB_resource_t *dbres, int wipe_clean, int n_partitions, char *loc_override);
int umount_db(DB_resource_t *dbres);
int print_db(DB_resource_t *db, FILE *fd);
int get_num_allocations_db(DB_resource_t *db);
int get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc);
int _get_alloc_with_id_db(DB_resource_t *dbr, osd_id_t id, Allocation_t *alloc);
int _put_alloc_db(DB_resource_t *dbr, Allocation_t *a, uint32_t old_expiration);
int put_alloc_db(DB_resource_t *dbr, Allocation_t *alloc, uint32_t old_expiration);
int remove_id_only_db(DB_resource_t *dbr, osd_id_t id);
int remove_alloc_db(DB_resource_t *dbr, Allocation_t *alloc);
int remove_alloc_iter_db(DB_iterator_t *it, Allocation_t *a);
int modify_alloc_db(DB_resource_t *dbr, Allocation_t *a, uint32_t old_expiration);
int create_alloc_db(DB_resource_t *dbr, Allocation_t *alloc);
int rebuild_add_expiration_db(DB_resource_t *dbr, Allocation_t *a);

int _id_iter_put_alloc_db(DB_iterator_t *it, Allocation_t *a);
int db_iterator_end(DB_iterator_t *it);
int db_iterator_next(DB_iterator_t *it, int direction, Allocation_t *a);
DB_iterator_t *expire_iterator(DB_resource_t *dbr);
DB_iterator_t *soft_iterator(DB_resource_t *dbr);
DB_iterator_t *id_iterator(DB_resource_t *dbr);
int set_expire_iterator(DB_iterator_t *dbi, ibp_time_t t, Allocation_t *a);
int set_id_iterator(DB_iterator_t *dbi, osd_id_t id);

//** History Comparator routines
int db_history_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen);
void db_history_compare_destroy(void *arg);
const char *db_history_compare_name(void *arg);
#endif
