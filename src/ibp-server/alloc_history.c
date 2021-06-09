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

#include "resource.h"
#include "allocation.h"
#include "cap_timestamp.h"
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

//** These are just dummies and are really only needed for a DB implementation
int create_history_table(Resource_t *r)
{
    return (0);
}

int mount_history_table(Resource_t *r)
{
    return (0);
}

void umount_history_table(Resource_t *r)
{
    return;
}


//****************************************************************************
// get_history_table - Retreives the history table fro the provided allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int fd_get_history_table(Resource_t *r, osd_fd_t *fd, Allocation_history_t *h)
{
    int n;

    n = osd_read(r->dev, fd, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
    if (n == sizeof(Allocation_history_t))
        n = 0;

    log_printf(15, "fd_get_history: r=%s fd=%p h.id=" LU " ws=%d rs=%d ms=%d\n", r->name, fd,
               h->id, h->write_slot, h->read_slot, h->manage_slot);
    tbx_log_flush();

    return (n);
}

//****************************************************************************
// get_history_table - Retreives the history table for the allocation from the DB
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int  get_history_table(Resource_t *r, osd_id_t id, Allocation_history_db_t *h)
{
    int err;
    rocksdb_iterator_t *it;
    db_history_key_t base_key;
    db_history_key_t *key;
    char *buf;
    size_t nbytes;
    int count[3];
    int mode;

    err = 0;

    //** Make the space
    tbx_type_malloc_clear(h->read_ts, Allocation_rw_ts_t, r->n_history);
    tbx_type_malloc_clear(h->write_ts, Allocation_rw_ts_t, r->n_history);
    tbx_type_malloc_clear(h->manage_ts, Allocation_manage_ts_t, r->n_history);

    //** Create the iterator
    it = rocksdb_create_iterator(r->db.history, r->db.ropts);
    rocksdb_iter_seek(it, db_fill_history_key(&base_key, id, 0, 0), sizeof(base_key));

    count[0] = 0; count[1] = 0; count[2] = 0;
    while (rocksdb_iter_valid(it) > 0) {
        key = (db_history_key_t *)rocksdb_iter_key(it, &nbytes);
        if (nbytes == 0) break;
        if (key->id != id) break;

        //** Skip it if an invalid key
        if (!((key->type>=0) && (key->type<=2))) goto next;

        mode = (unsigned char)key->type;

        //** Store it
        buf = (char *)rocksdb_iter_value(it, &nbytes);
        switch (mode) {
            case 0:
                if (nbytes == sizeof(Allocation_rw_ts_t)) {
                    memcpy(&(h->read_ts[count[mode] % r->n_history]), buf, nbytes);
                    count[mode]++;
                }
                break;
            case 1:
                if (nbytes == sizeof(Allocation_rw_ts_t)) {
                    memcpy(&(h->write_ts[count[mode] % r->n_history]), buf, nbytes);
                    count[mode]++;
                }
                break;
            case 2:
                if (nbytes == sizeof(Allocation_manage_ts_t)) {
                    memcpy(&(h->manage_ts[count[mode] % r->n_history]), buf, nbytes);
                    count[mode]++;
                }
                break;
        }

next:
        rocksdb_iter_next(it);
    }

    //** Update the size
    h->n_read   = (count[0] > r->n_history) ? r->n_history : count[0];
    h->n_write  = (count[1] > r->n_history) ? r->n_history : count[1];
    h->n_manage = (count[2] > r->n_history) ? r->n_history : count[2];

    //** Cleanup
    rocksdb_iter_destroy(it);

    return (err);
}

//****************************************************************************
// fd_put_history_table - Stores the history table for the given allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int fd_put_history_table(Resource_t *r, osd_fd_t *fd, Allocation_history_t *h)
{
    int n;

    n = osd_write(r->dev, fd, sizeof(Allocation_t), sizeof(Allocation_history_t), h);
    if (n == sizeof(Allocation_history_t))
        n = 0;

    log_printf(15, "fd_put_history: r=%s fd=%p h.id=" LU " write_slot=%d\n", r->name, fd, h->id,
               h->write_slot);
    tbx_log_flush();

    return (n);
}

//****************************************************************************
// put_history_table - Stores the history table for the given allocation
//    *** NOTE:  Calling application is responsible for locking!!!! ****
//****************************************************************************

int osd_put_history_table(Resource_t *r, osd_id_t id, Allocation_history_t *h)
{
    int err;
    osd_fd_t *fd = osd_open(r->dev, id, OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "put_history_table: Error with open_allocation for res=%s id=" LU "\n",
                   r->name, id);
        return (-1);
    }

    err = fd_put_history_table(r, fd, h);

    osd_close(r->dev, fd);

    return (err);
}

//****************************************************************************
// blank_history - Writes a blank history record
//****************************************************************************

int osd_blank_history(Resource_t *r, osd_id_t id)
{
    Allocation_history_t h;
    int err;

    if ((r->enable_read_history == 0) && (r->enable_write_history == 0)
        && (r->enable_manage_history == 0))
        return (0);

    memset(&h, 0, sizeof(h));
    h.id = id;

    err = osd_put_history_table(r, id, &h);
    if (err != 0) {
        log_printf(0, "blank_history: Error putting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        return (err);
    }

    return (0);
}


//============================================================================
// Routines that directly store the history in the allocation
//============================================================================

//****************************************************************************
// osd_update_read_history - Updates the read history table for the allocation
//****************************************************************************

void osd_update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                         uint64_t offset, uint64_t size, osd_id_t pid)
{
    Allocation_history_t h;
    int err;
    osd_fd_t *fd;

    fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "update_read_history: Error with open_allocation for res=%s id=" LU "\n",
                   r->name, id);
        return;
    }
    err = fd_get_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_read_history: Error getting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }

    set_read_timestamp(&h, add, offset, size, pid);

    err = fd_put_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_read_history: Error putting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }

    osd_close(r->dev, fd);
}

//****************************************************************************
// osd_update_write_history - Updates the write history table for the allocation
//****************************************************************************

void osd_update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                          uint64_t offset, uint64_t size, osd_id_t pid)
{
    Allocation_history_t h;
    int err;
    osd_fd_t *fd;

    fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "update_write_history: Error with open_allocation for res=%s id=" LU "\n",
                   r->name, id);
        return;
    }
    err = fd_get_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_write_history: Error getting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }
    set_write_timestamp(&h, add, offset, size, pid);

    err = fd_put_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_write_history: Error putting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }

    osd_close(r->dev, fd);
}

//****************************************************************************
// osd_update_manage_history - Updates the manage history table for the allocation
//****************************************************************************

void osd_update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                           int cmd, int subcmd, int reliability, uint32_t expiration,
                           uint64_t size, osd_id_t pid)
{
    Allocation_history_t h;
    int err;
    osd_fd_t *fd;

    fd = osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "update_manage_history: Error with open_allocation for res=%s id=" LU "\n",
                   r->name, id);
        return;
    }

    err = fd_get_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_manage_history: Error getting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }

    set_manage_timestamp(&h, add, cmd, subcmd, reliability, expiration, size, pid);

    err = fd_put_history_table(r, fd, &h);
    if (err != 0) {
        log_printf(0, "update_manage_history: Error putting history for res=%s id=" LU " err=%d\n",
                   r->name, id, err);
        osd_close(r->dev, fd);
        return;
    }

    osd_close(r->dev, fd);
}

//============================================================================
//  Routines for adding history to the DB
//============================================================================

//****************************************************************************
// db_history_comparator - these are the comparator routines
//****************************************************************************

int db_history_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    DB_resource_t *dbres = (DB_resource_t *)arg;
    db_history_key_t *akey = (db_history_key_t *)a;
    db_history_key_t *bkey = (db_history_key_t *)b;
    int a_part, b_part;

    //** check based on the ID partition
    a_part = akey->id % dbres->n_partitions;
    b_part = bkey->id % dbres->n_partitions;
    if (a_part > b_part) {
        return(1);
    } else if (a_part < b_part) {
        return(-1);
    }

    //** If made it here then the ID partitions are the same so check usign the ID
    if (akey->id > bkey->id) {
        return(1);
    } else if (akey->id < bkey->id) {
        return(-1);
    }

    //** The ID's are the same so sort by type next
    if (akey->type > bkey->type) {
        return(1);
    } else if (akey->type < bkey->type) {
        return(-1);
    }

    //** Types are the same so compare based on the time
    if (akey->date > bkey->date) {
        return(1);
    } else if (akey->date < bkey->date) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

void db_history_compare_destroy(void *arg) { return; }
const char *db_history_compare_name(void *arg) { return("history"); }

//****************************************************************************
// db_fill_history_key - Fills the history look up key
//****************************************************************************

const char *db_fill_history_key(db_history_key_t *key, osd_id_t id, int type, apr_time_t date)
{
    key->id = id;
    key->type = type;
    key->date = date;

    return((const char *)key);
}

//****************************************************************************
// lru_history_populate_core_merge - Fills in an lru_history_t struct with information
//     from the History DB from the merge DB
//****************************************************************************

void lru_history_populate_core_merge(Resource_t *r, osd_id_t id, rocksdb_iterator_t *it)
{
    db_history_key_t *key;
    const char *buf;
    size_t nbytes;
    char *errstr;

    while (rocksdb_iter_valid(it) > 0) {
        key = (db_history_key_t *)rocksdb_iter_key(it, &nbytes);
        if (nbytes != sizeof(db_history_key_t)) break;
        if (key->id != id) break;

        //** Skip it if an invalid key
        if (!((key->type>=0) && (key->type<=2))) goto next;

        buf = rocksdb_iter_value(it, &nbytes);

        //** and the actual RID's history DB
        errstr = NULL;
        rocksdb_put(r->db.history, r->db.wopts, (const char *)key, sizeof(db_history_key_t), (const char *)buf, nbytes, &errstr);
        if (errstr != NULL) {
            log_printf(1, "ERROR Adding history record! loc=%s id=" LU " DT=" TT " error=%s\n", r->db.loc, id, key->date, errstr);
            free(errstr);
        }
next:
        rocksdb_iter_next(it);
    }

    return;
}

//****************************************************************************
// lru_history_populate_merge - Copies over the history information from the
//     history DB used for the snap merge.
//****************************************************************************

void lru_history_populate_merge(Resource_t *r, osd_id_t id)
{
    rocksdb_iterator_t *it;
    db_history_key_t base_key;
    tbx_stack_t *stack = NULL;  //** This is used for anything we need to delete

    //** Create the iterator on the old DB
    it = rocksdb_create_iterator(r->db_merge.history, r->db.ropts);
    rocksdb_iter_seek(it, db_fill_history_key(&base_key, id, 0, 0), sizeof(base_key));

    //** Copy over the entries
    lru_history_populate_core_merge(r, id, it);

    //** Cleanup
    rocksdb_iter_destroy(it);

    //** See if we have to delete something
    if (stack) lru_history_populate_remove(r, stack);

    return;
}

//****************************************************************************
// lru_history_populate_core - Fills in an lru_history_t struct with information
//     from the History DB
//****************************************************************************

void lru_history_populate_core(Resource_t *r, osd_id_t id, lru_history_t *lh, tbx_stack_t **rm_stack, rocksdb_iterator_t *it)
{
    db_history_key_t *key, *kd;
    size_t nbytes;
    int mode;
    tbx_stack_t *stack = *rm_stack;  //** This is used for anything we need to delete

    memset(lh, 0, r->lru_history_bytes); //** Clear the structure
    lh->id = id;

    while (rocksdb_iter_valid(it) > 0) {
        key = (db_history_key_t *)rocksdb_iter_key(it, &nbytes);
        if (nbytes == 0) break;
        if (key->id != id) break;

        //** Skip it if an invalid key
        if (!((key->type>=0) && (key->type<=2))) {
            if (!stack) stack = tbx_stack_new();
            tbx_type_malloc(kd, db_history_key_t, 1);
            memcpy(kd, key, sizeof(db_history_key_t));
            tbx_stack_push(stack, kd);
            goto next;
        }

        //** See if we need to delete an older entry before overwriting it
        mode = (unsigned char)key->type;
        if (lh->slot[mode] > r->n_history) {
            if (!stack) stack = tbx_stack_new();
            tbx_type_malloc(kd, db_history_key_t, 1);
            memcpy(kd, key, sizeof(db_history_key_t));
            tbx_stack_push(stack, kd);
        }

        //** Store it
        lh->ts[(lh->slot[mode] % r->n_history) + mode * r->n_history] = key->date;
        lh->slot[mode]++;  //** and increment the counter
next:
        rocksdb_iter_next(it);
    }

    //** Wrap the slots now
    lh->slot[0] = lh->slot[0] % r->n_history;
    lh->slot[1] = lh->slot[1] % r->n_history;
    lh->slot[2] = lh->slot[2] % r->n_history;

    if (*rm_stack == NULL) *rm_stack = stack;

    return;
}

//****************************************************************************
// lru_history_populate_remove - Removes the records found during the scan
//****************************************************************************

void lru_history_populate_remove(Resource_t *r, tbx_stack_t *stack)
{
    db_history_key_t *key;
    char *errstr = NULL;

    if (!stack) return;

    while ((key = tbx_stack_pop(stack)) != NULL) {
        rocksdb_delete(r->db.history, r->db.wopts, (const char *)key, sizeof(db_history_key_t), &errstr);
        if (errstr) {
            log_printf(1, "ERROR deleting history key id=" LU " type=%d time=" TT " error=%s\n", key->id, key->type, key->date, errstr);
            free(errstr);
        }

        free(key);
    }

    tbx_stack_free(stack, 0);
}

//****************************************************************************
// lru_history_populate - Fills in an lru_history_t struct with information
//     from the History DB
//****************************************************************************

void lru_history_populate(Resource_t *r, osd_id_t id, lru_history_t *lh)
{
    rocksdb_iterator_t *it;
    db_history_key_t base_key;
    tbx_stack_t *stack = NULL;  //** This is used for anything we need to delete

    //** Create the iterator
    it = rocksdb_create_iterator(r->db.history, r->db.ropts);
    rocksdb_iter_seek(it, db_fill_history_key(&base_key, id, 0, 0), sizeof(base_key));

    lru_history_populate_core(r, id, lh, &stack, it);

    //** Cleanup
    rocksdb_iter_destroy(it);

    //** See if we have to delete something
    if (stack) lru_history_populate_remove(r, stack);

    return;
}

//****************************************************************************
// lru_get_history - Retreives a history LRU record and optionally
//   creates it if requested
//****************************************************************************

int lru_get_history(Resource_t *r, osd_id_t id, int do_create_if_missing, lru_history_t *lh)
{
    if (tbx_lru_get(r->history_lru, &id, sizeof(osd_id_t), lh) == 0) return(0); //** In the LRU

    //** Kick out if not allowed to create it
    if (do_create_if_missing != 1) return(1);

    //** If we made it here then it's not in the LRU so have to construct one.
    lru_history_populate(r, id, lh);

    return(0);
}

//****************************************************************************
// db_history_index - Determines the flexible array index to use and also
//      removes the entry being overwritten if needed.
//****************************************************************************

int db_history_index(Resource_t *r, int mode, lru_history_t *lh)
{
    db_history_key_t key;
    int k;
    char *errstr = NULL;

    //** Get the history record and slot
    k = lh->slot[mode] + mode * r->n_history; //** Actual offset in 1d array

    lh->slot[mode] = (lh->slot[mode]+1) % r->n_history;  //** New slot for next round

    //** Delete the old record
    if (lh->ts[k] != 0) {
        rocksdb_delete(r->db.history, r->db.wopts, db_fill_history_key(&key, lh->id, mode, lh->ts[k]), sizeof(key), &errstr);
        if (errstr != NULL) {
            log_printf(1, "ERROR deleting read history record! loc=%s id=" LU " DT=" TT " error=%s\n", r->db.loc, lh->id, lh->ts[k], errstr);
            free(errstr);
        }
    }

    return(k);
}

//****************************************************************************
// db_delete_history - Deletes the history records associated with the ID
//****************************************************************************

void db_delete_history(Resource_t *r, osd_id_t id)
{
    char buf[r->lru_history_bytes];
    char *errstr = NULL;
    void *ptr;
    lru_history_t *lh = (lru_history_t *)buf;
    db_history_key_t key;
    int i, j, k, base_slot;
    Allocation_history_t h;
    int err;
    size_t nbytes;
    osd_fd_t *fd;

    //** Fetch the record
    if (lru_get_history(r, id, 1, lh) != 0) return; //** Nothing to delete

    //** And fetch the history from the OSD if needed
    fd = (r->enable_history_update_on_delete == 0) ? NULL : osd_open(r->dev, id, OSD_READ_MODE | OSD_WRITE_MODE);
    if (fd) {
        err = fd_get_history_table(r, fd, &h);
        if (err != 0) {
            log_printf(0, "Error getting history for res=%s id=" LU " err=%d\n", r->name, id, err);
            osd_close(r->dev, fd);
            fd = NULL;
        }
    }

    //** Now delete everything but at the same time preserve it in the allocation header
    for (i=0; i<3; i++) {
        base_slot = lh->slot[i];
        for (j=0; j<r->n_history; j++) {
            k = ((j+base_slot) % r->n_history) + i*r->n_history;
            if (lh->ts[k] > 0) { //** Only delete if used
                //** Fetch the history and copy it to the allocation
                errstr = NULL;
                db_fill_history_key(&key, id, i, lh->ts[k]);
                if (fd) {
                    nbytes = 0;
                    ptr = rocksdb_get(r->db.history, r->db.ropts, (void *)&key, sizeof(db_history_key_t), &nbytes, &errstr);
                    if (errstr) {
                        log_printf(1, "ERROR getting history key id=" LU " type=%d time=" TT " error=%s\n", key.id, key.type, key.date, errstr);
                        free(errstr);
                    }
                    switch (i) {
                        case 0:  //** Read history
                            if (nbytes == sizeof(Allocation_rw_ts_t)) {
                                memcpy(&(h.read_ts[h.read_slot]), ptr, sizeof(Allocation_rw_ts_t));
                                h.read_slot = (h.read_slot+1) % ALLOC_HISTORY;
                            }
                            break;
                        case 1:  //** Write history
                            if (nbytes == sizeof(Allocation_rw_ts_t)) {
                                memcpy(&(h.write_ts[h.write_slot]), ptr, sizeof(Allocation_rw_ts_t));
                                h.write_slot = (h.write_slot+1) % ALLOC_HISTORY;
                            }
                            break;
                        case 2:  //** Manage history
                            if (nbytes == sizeof(Allocation_manage_ts_t)) {
                                memcpy(&(h.manage_ts[h.manage_slot]), ptr, sizeof(Allocation_manage_ts_t));
                                h.manage_slot = (h.manage_slot+1) % ALLOC_HISTORY;
                            }
                            break;
                    }
                    free(ptr);
                }

                //** Delete the record
                rocksdb_delete(r->db.history, r->db.wopts, (void *)&key, sizeof(db_history_key_t), &errstr);
                if (errstr) {
                    log_printf(1, "ERROR deleting history key id=" LU " type=%d time=" TT " error=%s\n", key.id, key.type, key.date, errstr);
                    free(errstr);
                }
            }
        }
    }

    //** Update the history in the OSD header
    if (fd) {
        fd_put_history_table(r, fd, &h);
        osd_close(r->dev, fd);
    }

    //** Remove it from the LRU
    tbx_lru_delete(r->history_lru, &id, sizeof(osd_id_t));
}

//****************************************************************************
// db_update_read_history - Updates the history in the DB
//****************************************************************************

void db_update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                         uint64_t offset, uint64_t size, osd_id_t pid)
{
    db_history_key_t key;
    Allocation_rw_ts_t ts;
    char buf[r->lru_history_bytes];
    lru_history_t *lh = (lru_history_t *)buf;
    int k, n;
    char *errstr = NULL;

    //** Get the history record and slot
    lru_get_history(r, id, 1, lh);
    k = db_history_index(r, 0, lh);

    //** Add the new one
    n = 0;
    set_rw_ts(&ts, &n, add, offset, size, pid);
    lh->ts[k] = apr_time_now();

    rocksdb_put(r->db.history, r->db.wopts, db_fill_history_key(&key, id, 0, lh->ts[k]), sizeof(key), (const char *)&ts, sizeof(ts), &errstr);
    if (errstr != NULL) {
        log_printf(1, "ERROR Adding read history record! loc=%s id=" LU " DT=" TT " error=%s\n", r->db.loc, id, lh->ts[k], errstr);
        free(errstr);
    }

    //** Update the LRU version
    tbx_lru_put(r->history_lru, lh);
}

//****************************************************************************
// db_update_write_history - Updates the history in the DB
//****************************************************************************

void db_update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                         uint64_t offset, uint64_t size, osd_id_t pid)
{
    db_history_key_t key;
    Allocation_rw_ts_t ts;
    char buf[r->lru_history_bytes];
    lru_history_t *lh = (lru_history_t *)buf;
    int n, k;
    char *errstr = NULL;

    //** Get the history record and slot
    lru_get_history(r, id, 1, lh);
    k = db_history_index(r, 1, lh);

    //** Add the new one
    n = 0;
    set_rw_ts(&ts, &n, add, offset, size, pid);
    lh->ts[k] = apr_time_now();

    rocksdb_put(r->db.history, r->db.wopts, db_fill_history_key(&key, id, 1, lh->ts[k]), sizeof(key), (const char *)&ts, sizeof(ts), &errstr);
    if (errstr != NULL) {
        log_printf(1, "ERROR Adding write history record! loc=%s id=" LU " DT=" TT " error=%s\n", r->db.loc, id, lh->ts[k], errstr);
        free(errstr);
    }

    //** Update the LRU version
    tbx_lru_put(r->history_lru, lh);
}

//****************************************************************************
// db_update_manage_history - Updates the history in the DB
//****************************************************************************

void db_update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                           int cmd, int subcmd, int reliability, uint32_t expiration,
                           uint64_t size, osd_id_t pid)
{
    db_history_key_t key;
    Allocation_manage_ts_t ts;
    char buf[r->lru_history_bytes];
    lru_history_t *lh = (lru_history_t *)buf;
    int k, n;
    char *errstr = NULL;

    //** Get the history record and slot
    lru_get_history(r, id, 1, lh);
    k = db_history_index(r, 2, lh);

    //** Add the new one
    n = 0;
    set_manage_ts(&ts, &n, add, cmd, subcmd, reliability, expiration, size, pid);
    lh->ts[k] = apr_time_now();

    rocksdb_put(r->db.history, r->db.wopts, db_fill_history_key(&key, id, 2, lh->ts[k]), sizeof(key), (const char *)&ts, sizeof(ts), &errstr);
    if (errstr != NULL) {
        log_printf(1, "ERROR Adding manage history record! loc=%s id=" LU " DT=" TT " error=%s\n", r->db.loc, id, lh->ts[k], errstr);
        free(errstr);
    }

    //** Update the LRU version
    tbx_lru_put(r->history_lru, lh);
}

//============================================================================
// High-level callable routines for updating the history
//============================================================================

//****************************************************************************
// update_read_history - Updates the read history information
//****************************************************************************

void update_read_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                         uint64_t offset, uint64_t size, osd_id_t pid)
{
    if (r->enable_read_history == 0) return;
    if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

    if (r->enable_read_history > 1) osd_update_read_history(r, id, is_alias, add, offset, size, pid);

    db_update_read_history(r, id, is_alias, add, offset, size, pid);
}

//****************************************************************************
// update_write_history - Updates the write history information
//****************************************************************************

void update_write_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                          uint64_t offset, uint64_t size, osd_id_t pid)
{
    if (r->enable_write_history == 0) return;
    if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

    if (r->enable_write_history > 1) osd_update_write_history(r, id, is_alias, add, offset, size, pid);

    db_update_write_history(r, id, is_alias, add, offset, size, pid);
}

//****************************************************************************
// update_manage_history - Updates the manage history table for the allocation
//****************************************************************************

void update_manage_history(Resource_t *r, osd_id_t id, int is_alias, Allocation_address_t *add,
                           int cmd, int subcmd, int reliability, uint32_t expiration,
                           uint64_t size, osd_id_t pid)
{
    if (r->enable_manage_history == 0) return;
    if ((r->enable_alias_history == 0) && (is_alias == 1)) return;

    if (r->enable_manage_history > 1) osd_update_manage_history(r, id, is_alias, add, cmd, subcmd, reliability, expiration, size, pid);

    db_update_manage_history(r, id, is_alias, add, cmd, subcmd, reliability, expiration, size, pid);
}
