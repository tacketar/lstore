/*
   Copyright 2025 Vanderbilt University

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

//******************************************************************************
// Provides an in memory LUT for the Inode DBs
//******************************************************************************

#define _log_module_index 251

#include <math.h>

#include <lio/ex3_fwd.h>
#include <ex3.h>
#include <lio/os.h>
#include <tbx/type_malloc.h>
#include <tbx/append_printf.h>
#include <tbx/string_token.h>

#include "inode_lut.h"

#include <sys/stat.h>
#include <sys/types.h>

#define EPOCH_ROLLOVER (INT_MAX-10)
//#define EPOCH_ROLLOVER (1000)

#define TBX_LIST_REMOVE_CHECK(tag, err, rec, de)
#define TBX_LIST_INSERT(tag, rec, de)

/********************
#define TBX_LIST_REMOVE_CHECK(tag, err, rec, de) if (err != 0) { \
    log_printf(0, "REMOVE: ERROR: err=%d tag=%s r=%p Missing dentry! inode=" XIDT " parent=" XIDT " de=!%s!\n", err, tag, (rec), (rec)->r.inode, (de)->parent_inode, (de)->dentry); tbx_log_flush(); \
    fprintf(stderr, "REMOVE: ERROR: err=%d tag=%s r=%p Missing dentry! inode=" XIDT " parent=" XIDT " de=!%s!\n", err, tag, (rec), (rec)->r.inode, (de)->parent_inode, (de)->dentry); tbx_log_flush(); \
} else { \
    log_printf(0, "REMOVE: GOOD: err=%d tag=%s r=%p inode=" XIDT " parent=" XIDT " de=!%s!\n", err, tag, (rec), (rec)->r.inode, (de)->parent_inode, (de)->dentry); tbx_log_flush(); \
}

#define TBX_LIST_INSERT(tag, rec, de) { \
    log_printf(0, "INSERT: tag=%s r=%p inode=" XIDT " parent=" XIDT " de=!%s! tbx_list_search=%p\n", tag, (rec), (rec)->r.inode, (de)->parent_inode, (de)->dentry, tbx_list_search(ilut->dentry_table, de)); tbx_log_flush(); \
}
*******************/

//******************************************************************************
// dentry_next - Returns the next dentry in the chain
//******************************************************************************

os_inode_dentry_t *dentry_next(os_inode_dentry_t *de)
{
    os_inode_dentry_t *de2;

    memcpy(&de2, &(de->dentry[de->len + 1]), sizeof(os_inode_dentry_t *));
    return(de2);
}
//******************************************************************************
// dentry_cmp - Dentry {parent_inode, dentry} comparision function
//******************************************************************************

int dentry_cmp_fn(void *arg, tbx_list_key_t *k1, tbx_list_key_t *k2)
{
    os_inode_dentry_t *a = (os_inode_dentry_t *)k1;
    os_inode_dentry_t *b = (os_inode_dentry_t *)k2;

//log_printf(0, "a->parent=" XIDT " de=%s\n", a->parent_inode, a->dentry); tbx_log_flush();
//log_printf(0, "b->parent=" XIDT " de=%s\n", b->parent_inode, b->dentry); tbx_log_flush();
    if (a->parent_inode == b->parent_inode) {
        return(strcmp(a->dentry, b->dentry));
    } else if (a->parent_inode < b->parent_inode) {
        return(-1);
    }

    return(1);
}

tbx_list_compare_t dentry_cmp = {dentry_cmp_fn, NULL};

//******************************************************************************
// _dentry_dump
//******************************************************************************

void dentry_dump(os_inode_lut_rec_t *r)
{
    os_inode_dentry_t *de;
    int i;

    log_printf(0, "START inode=" XIDT " main dentry=%s parent=" XIDT " hardlink=%p\n", r->r.inode, r->r.dentry.dentry, r->r.dentry.parent_inode, r->hardlink_list);
    de = r->hardlink_list;
    i=0;
    while (de) {
        log_printf(0, "    i=%d dentry=%s parent=" XIDT "\n", i, de->dentry, de->parent_inode);
        de = dentry_next(de);
        i++;
    }
    log_printf(0, "END inode=" XIDT " main dentry=%s parent=" XIDT " hardlink=%p\n", r->r.inode, r->r.dentry.dentry, r->r.dentry.parent_inode, r->hardlink_list);
}

//******************************************************************************
// dentry_list_dump
//******************************************************************************

void dentry_list_dump(tbx_list_t *list, const char *header)
{
    tbx_list_iter_t it;
    os_inode_dentry_t *de;
    os_inode_lut_rec_t *r;
    int i;
return;

    log_printf(0, "%s: START Dumping all dentries-----------\n", header);

    it = tbx_list_iter_search(list, NULL, 0);
    tbx_list_next(&it, (tbx_sl_key_t **)&de, (tbx_sl_data_t **)&r);
    i = 0;
    while (de != NULL) {
        log_printf(0, "    i=%d parent=" XIDT " dentry=%s inode=" XIDT "\n", i, de->parent_inode, de->dentry, r->r.inode);
        tbx_list_next(&it, (tbx_sl_key_t **)&de, (tbx_sl_data_t **)&r);
        i++;
    }

    log_printf(0, "%s: END--------------------------\n", header);
}

//******************************************************************************
// _remove_all_dentries - Removes all the dentries for the record
//******************************************************************************

void _remove_all_dentries(os_inode_lut_t *ilut, os_inode_lut_rec_t *r)
{
    os_inode_dentry_t *de, *de2;
//int err;

    tbx_list_remove(ilut->dentry_table, &(r->r.dentry), r);
//err = tbx_list_remove(ilut->dentry_table, &(r->r.dentry), r);
//TBX_LIST_REMOVE_CHECK("A", err, r,  &(r->r.dentry));
//if (err != 0) {
//    log_printf(0, "ERROR: A: Missing dentry! inode=" XIDT " parent=" XIDT " de=%s\n", r->r.inode, r->r.dentry.parent_inode, r->r.dentry.dentry); tbx_log_flush();
//    fprintf(0, "ERROR: A: Missing dentry! inode=" XIDT " parent=" XIDT " de=%s\n", r->r.inode, r->r.dentry.parent_inode, r->r.dentry.dentry); tbx_log_flush();
//}
    de = r->hardlink_list;  //** Loop over any additional references
    while (de) {
        tbx_list_remove(ilut->dentry_table, de, r);
//err = tbx_list_remove(ilut->dentry_table, de, r);
//TBX_LIST_REMOVE_CHECK("B", err, r, de);
        de2 = de;
        de = dentry_next(de);
        free(de2);
    }

    r->hardlink_list = NULL;
}

//******************************************************************************
// _lut_epoch_update_min - Updates the min epoch
//******************************************************************************

void _lut_epoch_update_min(os_inode_lut_t *ilut)
{
    int min_epoch, i, es, slot;

    min_epoch = ilut->min_epoch;
    for (i=0; i<ilut->n_epoch; i++) {
        es = i + min_epoch;
        slot = es % ilut->n_epoch;
        if (ilut->epoch_tally[slot] > 0) {
            ilut->min_epoch = es;
            return;
        }
    }

    ilut->min_epoch = (ilut->n_epoch - 1)  + min_epoch;
    return;
}

//******************************************************************************
// _lut_epoch_rollover - Adjusts internal values to reset the epoch
//******************************************************************************

void _lut_epoch_rollover(os_inode_lut_t *ilut)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    os_inode_lut_rec_t *r;
    int curr_epoch_offset, new_epoch, depoch;

    //** Determine our base offset in the epoch_tally since we want to keep that
    curr_epoch_offset = ilut->epoch % ilut->n_epoch;

    //** Now determine the new "epoch" and "min_epoch"
    new_epoch = curr_epoch_offset + ilut->n_epoch;
    depoch = ilut->epoch - new_epoch;

fprintf(stderr, "_lut_epoch_rollover: old_epoch=%d new_epoch=%d depoch=%d\n", ilut->epoch, new_epoch, depoch);

    //** Cycle through adjusting the record epochs
    for (hi=apr_hash_first(NULL, ilut->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&r);
fprintf(stderr, "_lut_epoch_rollover: checking r->inode=" XIDT " old epoch=%d new=%d\n", r->r.inode, r->epoch, (r->epoch - depoch));
        r->epoch = r->epoch - depoch;
    }


fprintf(stderr, "_lut_epoch_rollover: old_min_epoch=%d new_min_epoch=%d\n", ilut->min_epoch, (ilut->min_epoch-depoch));
    ilut->epoch = new_epoch;
    ilut->min_epoch = ilut->min_epoch - depoch;
//    _lut_epoch_update_min(ilut);

    return;
}

//******************************************************************************
// _lut_epoch_shrink - Culls old records from the LUT
//******************************************************************************

void _lut_epoch_shrink(os_inode_lut_t *ilut, os_inode_lut_rec_t *rkeep)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    os_inode_lut_rec_t *r;
    int i, slot, slot2, min_epoch, es, es2, n_cull, count, new_min_epoch;

    //** See how many entries we need to cull
    n_cull = apr_hash_count(ilut->table) - ilut->n_lut_shrink;

int n_start = apr_hash_count(ilut->table);
fprintf(stderr, "lut_epoch_shrink: START epoch=%d n_cull=%d min_epoch=%d arp_hash_count=%d\n", ilut->epoch, n_cull, ilut->min_epoch, n_start);
log_printf(0, "lut_epoch_shrink: START epoch=%d n_cull=%d min_epoch=%d arp_hash_count=%d\n", ilut->epoch, n_cull, ilut->min_epoch, n_start);

    //** See which epochs we will be dropping
    min_epoch = ilut->min_epoch;
    new_min_epoch = min_epoch;
    count = 0;
    if (n_cull < 0) { //** Not enough entries to shrink so just renumber the epochs
        es2 = ilut->epoch-1;
        slot2 = es2 % ilut->n_epoch;
fprintf(stderr, "_lut_epoch_shrink: switching everything epoch=%d slot=%d\n", es2, slot2);
        for (hi=apr_hash_first(NULL, ilut->table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, &hlen, (void **)&r);
//            if (r == rkeep) continue;  //** Skip the current record
            if (r->epoch > es2) continue;
count++;
//fprintf(stderr, "_lut_epoch_shrink: checking r->inode=" XIDT " epoch=%d\n", r->r.inode, r->epoch);
            es = r->epoch;
            slot = es % ilut->n_epoch;
            ilut->epoch_tally[slot]--;
            r->epoch = es2;
            ilut->epoch_tally[slot2]++;
        }

fprintf(stderr, "_lut_epoch_shrink: SMALL END moved everything to the latest epoch ilut->epoch=%d count=%d\n", ilut->epoch, count);
        _lut_epoch_update_min(ilut);
fprintf(stderr, "_lut_epoch_shrink: SMALL END OOPS cull and updated new min_epoch=%d\n", ilut->min_epoch);
        return;
    }

    for (i=0; i<ilut->n_epoch; i++) {
        es = i + min_epoch;
        slot = es % ilut->n_epoch;
        count = count + ilut->epoch_tally[slot];
//fprintf(stderr, "lut_epoch_shrink: epoch=%d count=%d total=%d\n", es, ilut->epoch_tally[slot], count);
        if (count > n_cull) {
//fprintf(stderr, "lut_epoch_shrink: culling epoch=%d count=%d total=%d\n", es, ilut->epoch_tally[slot], count);
            new_min_epoch = es;
            break;
        }
    }

fprintf(stderr, "lut_epoch_shrink: AFTER cull loop  epoch=%d n_cull=%d min_epoch=%d new_min_epoch=%d count=%d\n", ilut->epoch, n_cull, ilut->min_epoch, new_min_epoch, count);

    //** See if we can have a few empty epochs we can also drop at no cost
    for (i=(new_min_epoch-min_epoch); i<ilut->n_epoch; i++) {
        es = i + min_epoch;
        slot = es % ilut->n_epoch;
        if (ilut->epoch_tally[slot] != 0) {
            new_min_epoch = es;
            break;
        }
    }

fprintf(stderr, "lut_epoch_shrink: AFTER empty check epoch=%d n_cull=%d min_epoch=%d new_min_epoch=%d\n", ilut->epoch, n_cull, ilut->min_epoch, new_min_epoch);

    es = ilut->epoch - new_min_epoch;
    if (es >= ilut->n_epoch) {
        new_min_epoch = ilut->epoch - ilut->n_epoch;
fprintf(stderr, "lut_epoch_shrink: ADJUSTING epoch=%d n_cull=%d min_epoch=%d new_min_epoch=%d\n", ilut->epoch, n_cull, ilut->min_epoch, new_min_epoch);
    }

    //** Cycle through culling records until we hit our target
    count = 0;
    for (hi=apr_hash_first(NULL, ilut->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&r);
//fprintf(stderr, "_lut_epoch_shrink: checking r->inode=" XIDT " epoch=%d\n", r->r.inode, r->epoch);
        es = r->epoch;
        slot = es % ilut->n_epoch;
        if (r->epoch <= new_min_epoch) {
            if (r != rkeep) {
                count++;
                ilut->epoch_tally[slot]--;
//fprintf(stderr, "_lut_epoch_shrink: start_epoch=%d culling r->inode=" XIDT " epoch=%d tally=%d\n", ilut->epoch, r->r.inode, r->epoch, ilut->epoch_tally[slot]);

                apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), NULL);
                if (ilut->dentry_table) {
                    _remove_all_dentries(ilut, r);
                }
                free(r);
            }
        }
    }

    _lut_epoch_update_min(ilut);

int n_end = apr_hash_count(ilut->table);
log_printf(0, "_lut_epoch_shrink: END OOPS cull and updated new min_epoch=%d calculated=%d culled=%d n_start=%d n_end=%d\n", ilut->min_epoch, new_min_epoch, count, n_start, n_end);
fprintf(stderr, "_lut_epoch_shrink: END OOPS cull and updated new min_epoch=%d calculated=%d culled=%d n_start=%d n_end=%d\n", ilut->min_epoch, new_min_epoch, count, n_start, n_end);
if (ilut->min_epoch < new_min_epoch) {
 fprintf(stderr, "_lut_epoch_shrink: OOPS ERROR min_epoch=%d calculated=%d\n", ilut->min_epoch, new_min_epoch);
}
    return;
}

//******************************************************************************
// _lut_epoch_get - Gets the current epoch and adjusts counters as needed
//******************************************************************************

void _lut_epoch_get(os_inode_lut_t *ilut, os_inode_lut_rec_t *r)
{
    int epoch, slot;

    epoch = ilut->epoch;
    r->epoch = epoch;
    slot = epoch % ilut->n_epoch;
    ilut->epoch_tally[slot]++;
//log_printf(0, "inode=" XIDT " epoch=%d slot=%d tally=%d\n", r->r.inode, r->epoch, slot, ilut->epoch_tally[slot]);
    ilut->count++;
    if (ilut->count > ilut->n_new_epoch) {
        ilut->epoch++;
        ilut->count = 0;
        if ((ilut->epoch - ilut->min_epoch) >= ilut->n_epoch) {
fprintf(stderr, "_lut_epoch_get: FORCING SHRINK epoch=%d min_epoch=%d \n", ilut->epoch, ilut->min_epoch);
            _lut_epoch_shrink(ilut, r);
        }

        if (ilut->epoch >= EPOCH_ROLLOVER) {
            _lut_epoch_rollover(ilut);
        }
    }

//fprintf(stderr, "_lut_epoch_get: n_lut_max=%d hash_count=%d\n", ilut->n_lut_max, apr_hash_count(ilut->table));
    //** See if we need to shrink things
    if ((unsigned int)ilut->n_lut_max > apr_hash_count(ilut->table)) return;

fprintf(stderr, "_lut_epoch_get: normal shrink du to size \n_lut_max=%d hash_count=%d\n", ilut->n_lut_max, apr_hash_count(ilut->table));

    //** We do so shrink the table
    _lut_epoch_shrink(ilut, r);
}

//******************************************************************************
// _lut_epoch_dec - Decrements the given epoch's tally and adjusts fields as needed
//******************************************************************************

void _lut_epoch_dec(os_inode_lut_t *ilut, os_inode_lut_rec_t *r)
{
    int slot;

    slot = r->epoch % ilut->n_epoch;
    ilut->epoch_tally[slot]--;
//log_printf(0, "inode=" XIDT " epoch=%d slot=%d tally=%d\n", r->r.inode, r->epoch, slot, ilut->epoch_tally[slot]);
if (ilut->epoch_tally[slot] < 0) {
  fprintf(stderr, "_lut_epoch_dec: ERROR: inode=" XIDT " epoch=%d tally=%d slot=%d\n", r->r.inode, r->epoch, ilut->epoch_tally[slot], slot); fflush(stderr);
  log_printf(0, "_lut_epoch_dec: ERROR: inode=" XIDT " epoch=%d tally=%d slot=%d\n", r->r.inode, r->epoch, ilut->epoch_tally[slot], slot); tbx_log_flush();
}
    if (ilut->epoch_tally[slot] > 0) return;  //** Nothing to do
    if (r->epoch != ilut->epoch) return;

    _lut_epoch_update_min(ilut);

    return;
}

//******************************************************************************
// _lut_epoch_update - Updates the epoch for the entry
//******************************************************************************

void _lut_epoch_update(os_inode_lut_t *ilut, os_inode_lut_rec_t *r)
{
    _lut_epoch_dec(ilut, r);
    _lut_epoch_get(ilut, r);
    return;
}

//******************************************************************************
// _inode_lut_rec_new - Creates and fills in a new Inode LUT record
//******************************************************************************

os_inode_lut_rec_t *_inode_lut_rec_new(ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry)
{
    os_inode_lut_rec_t *r;
    int n;

    n = sizeof(os_inode_lut_rec_t) + len + 1;
    r = (os_inode_lut_rec_t *)malloc(n);
    r->epoch = 0;  ///** this needs to be filled in by the calling routine
    r->hardlink_list = NULL;
    r->r.inode = inode;
    r->r.ftype = ftype;
    r->r.dentry.parent_inode = parent_inode;
    r->r.dentry.len = len;
    strncpy(r->r.dentry.dentry, dentry, len+1);

    return(r);
}


//******************************************************************************
// _dentry_new - Allocates a new dentry structure, including space for the pointer
//******************************************************************************

os_inode_dentry_t *_dentry_new(ex_id_t parent_inode, int len, const char *name)
{
    os_inode_dentry_t *de;
    int n;

    n = sizeof(os_inode_dentry_t) + len + 1 + sizeof(os_inode_dentry_t *);
    de = (os_inode_dentry_t *)malloc(n);
    de->parent_inode = parent_inode;
    de->len = len;
    strncpy(de->dentry, name, len+1);
    memset(de->dentry + len + 1, 0, sizeof(os_inode_dentry_t *));
    return(de);
}

//******************************************************************************
// os_inode_lut_put - Adds an entry to the LUT.
//    NOTE: This should not be used for renames!
//******************************************************************************

void os_inode_lut_put(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, ex_id_t parent_inode, int ftype, int len, const char *dentry)
{
    os_inode_lut_rec_t *r;
    os_inode_dentry_t *de;

    int pchange, ichange;

    if (do_lock) apr_thread_mutex_lock(ilut->lock);

dentry_list_dump(ilut->dentry_table, "A");

    //** Delete any  existing dentry in that place
    if (ilut->dentry_table) {
log_printf(0, "deleting an old dentry new inode=" XIDT " parent=" XIDT " de=!%s!\n", inode, parent_inode, dentry);
        os_inode_lut_dentry_del(ilut, 0, parent_inode, dentry);
    }

    //** See if it already exists
    r = apr_hash_get(ilut->table, &inode, sizeof(ex_id_t));
    if (r) {  //** An old entry exists.  See if a field used in a key changed
log_printf(0, "Existing record inode=" XIDT " parent=" XIDT " de=!%s! hardlink=%p\n", r->r.inode, r->r.dentry.parent_inode, r->r.dentry.dentry, r->hardlink_list);
//HERE        _lut_epoch_dec(ilut, r);  //** Decr the old epoch
        if ((r->r.dentry.len != len) || (strcmp(r->r.dentry.dentry, dentry) != 0)) {  //** Different dentry so add it
            if (ilut->dentry_table) {
                if (ftype & OS_OBJECT_HARDLINK_FLAG) { //** Got a hardlink so insert an additional dentry
                    _lut_epoch_dec(ilut, r);  //** Decr the old epoch
                    de = _dentry_new(parent_inode, len, dentry);  //** Make the new entry
                    if (tbx_list_search(ilut->dentry_table, de)) { //** Already exists
                        free(de);
                    } else {
                        //** Insert it in the record hardlink list
log_printf(0, "HARDLINK-INSERT: A. inode=" XIDT " hardlink=%p r.dentry=%s new dentry=%s\n", inode, r->hardlink_list, r->r.dentry.dentry, de->dentry);
                        memcpy(&(de->dentry[len + 1]), &(r->hardlink_list), sizeof(os_inode_dentry_t *));
                        r->hardlink_list = de;

                        //** Add it to the dentry_table
                        tbx_list_insert(ilut->dentry_table, de, r);
TBX_LIST_INSERT("A", r, de);
                    }
                } else {  //** Hmmm We probably have an external rename that's occured so treat this as a new record
log_printf(0, "POSSIBLE-RENAME: A. inode=" XIDT " r.dentry=%s new dentry=%s\n", inode, r->r.dentry.dentry, dentry);
                    os_inode_lut_del(ilut, 0, inode);
                    r = NULL;
                    goto insert_new;
                }
            } else {  //** We don't support hardlinks
                //** Remove the old entry
                _lut_epoch_dec(ilut, r);  //** Decr the old epoch
                apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), NULL);
                free(r);

                //**And add the new one back
                r = _inode_lut_rec_new(inode, parent_inode, ftype, len, dentry);
                apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), r);
            }
        } else if ((r->r.inode != inode) || (r->r.dentry.parent_inode != parent_inode)) { //** An inode changed so keep the record
            //** See what changed
            _lut_epoch_dec(ilut, r);  //** Decr the old epoch
            pchange = (r->r.dentry.parent_inode == parent_inode) ? 0 : (ilut->dentry_table ? 1 : 0);
            ichange = (r->r.inode == inode) ? 0 : 1;

            if (pchange) {
                de = _dentry_new(parent_inode, len, dentry);  //** Make the new entry

log_printf(0, "HARDLINK-INSERT: B. inode=" XIDT " hardlink=%p r.dentry=%s new dentry=%s\n", inode, r->hardlink_list, r->r.dentry.dentry, de->dentry);

                //** Insert it in the record hardlink list
                memcpy(&(de->dentry[len + 1]), &(r->hardlink_list), sizeof(os_inode_dentry_t *));
                r->hardlink_list = de;

                //** Add it to the dentry_table
                tbx_list_insert(ilut->dentry_table, de, r);
TBX_LIST_INSERT("B", r, de);
            } else {  //** Hardlinks aren't supported so just change the parent
                r->r.dentry.parent_inode = parent_inode;
            }
            //** Remove the old entry

            if (ichange) apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), NULL);

            //** Change the fields. NOTE: the dentry name is the same if we are here
            r->r.inode = inode;
            r->r.ftype = ftype;

            //**And add the new one back
            if (ichange) apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), r);
        } else {  //** Just the ftype may have changed
            _lut_epoch_dec(ilut, r);  //** Decr the old epoch
            r->r.ftype = ftype;
        }

        //** Lastly store our new epoch and see if it's time to generate a new one
        _lut_epoch_get(ilut, r);
dentry_list_dump(ilut->dentry_table, "B");

        if (do_lock) apr_thread_mutex_unlock(ilut->lock);

        return;  //** Kick out since we've done the update
    }

insert_new:
    //** If we made it here we have a new entry
    r = _inode_lut_rec_new(inode, parent_inode, ftype, len, dentry);

    //** Store the LUT entry BEFORE adjusting the epoch to keep things in sync
    apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), r);
    if (ilut->dentry_table) tbx_list_insert(ilut->dentry_table, &(r->r.dentry), r);
if (ilut->dentry_table) { TBX_LIST_INSERT("C", r, &(r->r.dentry)); }

dentry_list_dump(ilut->dentry_table, "C");

    //** Store our epoch and see if it's time to generate a new one
    _lut_epoch_get(ilut, r);

    if (do_lock) apr_thread_mutex_unlock(ilut->lock);
}

//******************************************************************************
// os_inode_lut_get - Fetches an entry from the LUT
//******************************************************************************

int os_inode_lut_get(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    os_inode_lut_rec_t *r;

    *parent_inode = OS_INODE_MISSING;
    if (do_lock) apr_thread_mutex_lock(ilut->lock);
    r = apr_hash_get(ilut->table, &inode, sizeof(ex_id_t));
    if (r) {
        _lut_epoch_update(ilut, r);  //** Update out access
        *parent_inode = r->r.dentry.parent_inode;
        *ftype = r->r.ftype;
        *len = r->r.dentry.len;
        if (dentry) *dentry = strdup(r->r.dentry.dentry);
    }
    if (do_lock) apr_thread_mutex_unlock(ilut->lock);

    return((r ? 0 : 1));
}

//******************************************************************************
// os_inode_lut_dentry_get - Fetches an entry from the LUT dentry table
//******************************************************************************

int os_inode_lut_dentry_get(os_inode_lut_t *ilut, int do_lock, ex_id_t parent_target, const char *dentry_target, ex_id_t *inode, ex_id_t *parent_inode, int *ftype, int *len, char **dentry)
{
    os_inode_lut_rec_t *r;
    os_inode_dentry_t *de;
    char buf[sizeof(os_inode_lut_rec_t) + OS_PATH_MAX + 1];

    //** Make the tmp record which holds the "key"
    de = (os_inode_dentry_t *)buf;
    de->parent_inode = parent_target;
    de->len = strlen(dentry_target);
    strcpy(de->dentry, dentry_target);

    *parent_inode = OS_INODE_MISSING;
    if (do_lock) apr_thread_mutex_lock(ilut->lock);
    r = tbx_list_search(ilut->dentry_table, de);
    if (r) {
        _lut_epoch_update(ilut, r);  //** Update out access
        *inode = r->r.inode;
        *parent_inode = r->r.dentry.parent_inode;
        *ftype = r->r.ftype;
        *len = r->r.dentry.len;
        if (dentry) *dentry = strdup(r->r.dentry.dentry);
    }
    if (do_lock) apr_thread_mutex_unlock(ilut->lock);

    return((r ? 0 : 1));
}

//******************************************************************************
// os_inode_lut_del - Removes an entry from the LUT
//******************************************************************************

void os_inode_lut_del(os_inode_lut_t *ilut, int do_lock, ex_id_t inode)
{
    os_inode_lut_rec_t *r;

    if (do_lock) apr_thread_mutex_lock(ilut->lock);
    r = apr_hash_get(ilut->table, &inode, sizeof(ex_id_t));
    if (r) {
        apr_hash_set(ilut->table, &inode, sizeof(ex_id_t), NULL);
        if (ilut->dentry_table) {  //** Iterate over all the dentrys
            _remove_all_dentries(ilut, r);
        }
        _lut_epoch_dec(ilut, r);
        free(r);
    }
    if (do_lock) apr_thread_mutex_unlock(ilut->lock);
}

//******************************************************************************
// _dentry_redo - Does a lut record swap when the builtin dentry is changed
//******************************************************************************

void _dentry_redo(os_inode_lut_t *ilut, os_inode_lut_rec_t *rold, ex_id_t parent, const char *name)
{
    os_inode_lut_rec_t *r;
    os_inode_dentry_t *de, *de2, *deprev;

    //** Use the 1st entry as the new primary key
    de = rold->hardlink_list;
    if (de == NULL) return;

    //** Make the new record
    r = _inode_lut_rec_new(rold->r.inode, parent, rold->r.ftype, de->len, de->dentry);
    r->hardlink_list = rold->hardlink_list;
    r->epoch = rold->epoch;  //** Copy over the epoch

    //** Replace the hash entry
    apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), NULL); //** Clear the old one since it's key is with the old record
    apr_hash_set(ilut->table, &(r->r.inode), sizeof(ex_id_t), r);    //** Store the new one with the same key
    if (ilut->dentry_table == NULL) return;

    //** Remove the old dentry but hold off adding the new primary in
    tbx_list_remove(ilut->dentry_table, &(rold->r.dentry), rold);

    de = rold->hardlink_list;  //** Loop over any additional references
    deprev = NULL;
    de2 = NULL;
    while (de) {
        tbx_list_remove(ilut->dentry_table, de, rold);
        if (dentry_cmp_fn(NULL, &(r->r.dentry), de) == 0) { //** This is the "new" primary dentry
            if (deprev == NULL) {
                memcpy(&(r->hardlink_list), &(de->dentry[de->len + 1]), sizeof(os_inode_dentry_t *));
             } else {
                memcpy(deprev->dentry + deprev->len + 1, de->dentry + de->len + 1, sizeof(os_inode_dentry_t *));
             }
             de2 = de;
             de = dentry_next(de);
             free(de2);
        } else { //** Only add it back if still on the list
            tbx_list_insert(ilut->dentry_table, de, r);
            deprev = de;
            de = dentry_next(de);
        }
    }

    //** It should be purged now and safe to add back in
    tbx_list_insert(ilut->dentry_table, &(r->r.dentry), r);

    free(rold);
}


//******************************************************************************
// os_inode_lut_dentry_del - Deletes a dentry fromm the LUT dentry table
//******************************************************************************

void os_inode_lut_dentry_del(os_inode_lut_t *ilut, int do_lock, ex_id_t parent, const char *name)
{
    os_inode_lut_rec_t *r;
    os_inode_dentry_t *de, *de2, *deprev;
    char buf[sizeof(os_inode_lut_rec_t) + OS_PATH_MAX + 1];

    //** Make the tmp record which holds the "key"
    de = (os_inode_dentry_t *)buf;
    de->parent_inode = parent;
    de->len = strlen(name);
    strcpy(de->dentry, name);

    if (do_lock) apr_thread_mutex_lock(ilut->lock);
    r = tbx_list_search(ilut->dentry_table, de);
    if (r == NULL) return;

    if (r->hardlink_list == NULL) { //** Simple removal
        os_inode_lut_del(ilut, 0, r->r.inode);
        if (do_lock) apr_thread_mutex_unlock(ilut->lock);
        return;
    }

    //** If we made it here we have a file with multiple references
    if (dentry_cmp_fn(NULL, de, &(r->r.dentry)) != 0) { //** We're just deleting a hardlink reference
        de2 = r->hardlink_list;  //** Loop over any additional references
        deprev = NULL;
        while (de2) {
            if (dentry_cmp_fn(NULL, de, de2) == 0) { //** Found it
                tbx_list_remove(ilut->dentry_table, de2, r);
                if (deprev == NULL) {
                    memcpy(&(r->hardlink_list), &(de2->dentry[de2->len + 1]), sizeof(os_inode_dentry_t *));
                } else {
                    memcpy(deprev->dentry + deprev->len + 1, de2->dentry + de2->len + 1, sizeof(os_inode_dentry_t *));
                }
                free(de2);
                if (do_lock) apr_thread_mutex_unlock(ilut->lock);
                return;
            }
            deprev = de2;
            de2 = dentry_next(de2);
        }
    }

    //** If we made it here we have to redo the whole record:(
    _dentry_redo(ilut, r, parent, name);

    if (do_lock) apr_thread_mutex_unlock(ilut->lock);

    return;
}

//******************************************************************************
// os_inode_lut_dentry_rename - Adds an entry to the LUT
//******************************************************************************

void os_inode_lut_dentry_rename(os_inode_lut_t *ilut, int do_lock, ex_id_t inode, int ftype, ex_id_t oldparent, const char *oldname, ex_id_t newparent, const char *newname)
{
    //** IF we don't support hardlinks then do a simple put
    if (ilut->dentry_table == NULL) {
        os_inode_lut_put(ilut, do_lock, inode, newparent, ftype, strlen(newname), newname);
        return;
    }

    //** Got to handle hardlinks
    if (do_lock) apr_thread_mutex_lock(ilut->lock);

    //** Delete the old dentry
    os_inode_lut_dentry_del(ilut, 0, oldparent, oldname);

    //** Add in the new version which could be a hardlink
    os_inode_lut_put(ilut, 0, inode, newparent, ftype, strlen(newname), newname);

    if (do_lock) apr_thread_mutex_unlock(ilut->lock);
}

//******************************************************************************
// os_inode_lut_create - Creates a Inode in-memory LUT
//******************************************************************************

os_inode_lut_t *os_inode_lut_create(int n_lut_max, double shrink_fraction, int n_epoch, int n_new_epoch, int enable_dentry_lut)
{
    os_inode_lut_t *ilut;
    int nlevels;

    tbx_type_malloc_clear(ilut, os_inode_lut_t, 1);
    tbx_type_malloc_clear(ilut->epoch_tally, int, n_epoch);

    if (enable_dentry_lut) {
        nlevels = log2(n_lut_max);
        ilut->dentry_table = tbx_sl_new_full(nlevels, 0.5, 0, &dentry_cmp, NULL, NULL, NULL);
    }

    ilut->epoch = 0;
    ilut->n_lut_max = n_lut_max;
    ilut->n_lut_shrink = shrink_fraction * n_lut_max;
    ilut->n_epoch = n_epoch;
    ilut->n_new_epoch = n_new_epoch;

    assert_result(apr_pool_create(&(ilut->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(ilut->lock), APR_THREAD_MUTEX_DEFAULT, ilut->mpool);
    ilut->table = apr_hash_make(ilut->mpool);

    return(ilut);
}

//******************************************************************************
// os_inode_lut_destroy - Destroys an Inode LUT table
//******************************************************************************

void os_inode_lut_destroy(os_inode_lut_t *ilut)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    os_inode_lut_rec_t *r;

    //** Destroy all the LUT records
    for (hi=apr_hash_first(NULL, ilut->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&r);
        if (ilut->dentry_table) {
            _remove_all_dentries(ilut, r);
        }
        free(r);
    }

    //** Delete the dentry table All the references should be removed above
    if (ilut->dentry_table) {
        tbx_list_destroy(ilut->dentry_table);
    }

    apr_thread_mutex_destroy(ilut->lock);
    apr_pool_destroy(ilut->mpool);
    free(ilut->epoch_tally);
    free(ilut);
}

//******************************************************************************

os_inode_lut_t *os_inode_lut_load(lio_service_manager_t *ess, tbx_inip_file_t *fd, char *section)
{
    int n_lut_max, n_epoch, n_new_epoch, enable_dentry;
    double shrink_fraction;

    n_lut_max = tbx_inip_get_integer(fd, section, "n_lut_max", 1000*1000);
    enable_dentry = tbx_inip_get_integer(fd, section, "enable_dentry", 0);
    n_epoch = tbx_inip_get_integer(fd, section, "n_epoch", 10000);
    n_new_epoch = tbx_inip_get_integer(fd, section, "n_new_epoch", 10000);
    shrink_fraction = tbx_inip_get_double(fd, section, "shrink_fraction", 0.5);

    return(os_inode_lut_create(n_lut_max, shrink_fraction, n_epoch, n_new_epoch, enable_dentry));
}

//******************************************************************************

void os_inode_lut_print_running_config(os_inode_lut_t *ilut, FILE *fd)
{
    int i, slot, epoch;
    double d;
    char pp[256];

    d = ilut->n_lut_max;
    d = (1.0*ilut->n_lut_shrink)/d;

    fprintf(fd, "n_lut_max = %s\n", tbx_stk_pretty_print_int_with_scale(ilut->n_lut_max, pp));
    fprintf(fd, "n_epoch = %s\n", tbx_stk_pretty_print_int_with_scale(ilut->n_epoch, pp));
    fprintf(fd, "n_new_epoch = %s\n", tbx_stk_pretty_print_int_with_scale(ilut->n_new_epoch, pp));
    fprintf(fd, "enable_dentry = %d\n", ((ilut->dentry_table) ? 1 : 0));
    fprintf(fd, "shrink_fraction = %s\n", tbx_stk_pretty_print_double_with_scale(1000, d, pp));
    fprintf(fd, "\n");
    fprintf(fd, "# Current epoch = %s\n", tbx_stk_pretty_print_int_with_scale(ilut->epoch, pp));
    fprintf(fd, "# Min epoch = %s\n", tbx_stk_pretty_print_int_with_scale(ilut->min_epoch, pp));
    fprintf(fd, "# count = %s\n", tbx_stk_pretty_print_int_with_scale(apr_hash_count(ilut->table), pp));
    fprintf(fd, "# epoch  count\n");
    for (i=0; i<ilut->n_epoch; i++) {
        epoch = i + ilut->min_epoch;
        slot = epoch % ilut->n_epoch;
        fprintf(fd, " %d  %s\n", epoch, tbx_stk_pretty_print_int_with_scale(ilut->epoch_tally[slot], pp));
    }
    fprintf(fd, "\n");

    return;
}
