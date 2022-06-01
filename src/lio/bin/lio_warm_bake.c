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

#define _log_module_index 207

#include <rocksdb/c.h>
#include <apr.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <ibp/types.h>
#include <lio/authn.h>
#include <lio/ds.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/que.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "warmer_helpers.h"

typedef struct {
    char *rid_key;
    ex_off_t n_good;
    ex_off_t n_bad;
    ex_off_t total_size;
    ex_off_t dtime;
} rid_tally_t;

typedef struct {
    rid_tally_t *tally;
    int n_caps;
    int curr;
    ibp_depot_t depot;
    char *rid_key;
    char **cap;
    char *cap_buffer;
    ex_id_t *inode;
    ex_off_t *nbytes;
    ex_off_t total_cap_size;
} rid_cap_list_t;

typedef struct {
    int slot;
    int n;
    int n_failed;
    int *failed;
    rid_cap_list_t *rcl;
    gop_op_generic_t *gop;
    ibp_depot_t depot;
} warm_task_t;

typedef struct {  //** inode info needed for baking
    ex_id_t inode;
    int n_allocs;
    int n_good;
    int n_bad;
    int write_error;
    char *fname;
} warm_prep_info_t;

typedef struct {  //** Warmer parition handle
    apr_pool_t *mpool;
    apr_hash_t *inode;
    apr_hash_t *rid_caps;
    apr_hash_t *rid_tally;
    warm_db_t *db_rid;
    warm_db_t *db_inode;
    warm_prep_db_t *wdb;
    tbx_que_t *que_setattr;
    ex_off_t n_good;
    ex_off_t n_bad;
    ex_off_t n_missing_err;
    ex_off_t n_write_err;
    int n_bulk;
    int n_warm;
} warm_partition_t;

int verbose = 0;

ibp_context_t *ic = NULL;
int bulk_mode = 1;
static int dt = 30*86400;

//*************************************************************************
// setattr_thread - Sets the warming attribute
//*************************************************************************

void *setattr_thread(apr_thread_t *th, void *data)
{
    tbx_que_t *que = (tbx_que_t *)data;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    int i, n, running;
    int running_max = 1000;
    int n_max = 1000;
    char *fname[n_max], *fn;
    char *etext;

    tbx_monitor_thread_create(MON_MY_THREAD, "setattr_thread");
    q = gop_opque_new();
    tbx_monitor_obj_label(gop_mo(opque_get_gop(q)), "setattr_thread_que");
    tbx_monitor_thread_group(gop_mo(opque_get_gop(q)), MON_MY_THREAD);

    etext = NULL;
    opque_start_execution(q);

    running = 0;
    while (!tbx_que_is_finished(que)) {
        n = tbx_que_bulk_get(que, n_max, fname, TBX_QUE_BLOCK);

        //** Clean up any oustanding setattr calls
        while ((gop = opque_get_next_finished(q)) != NULL) {
            running--;
            fn = gop_get_private(gop);
            free(fn);
            gop_free(gop, OP_DESTROY);
        }

        if (n <= 0) continue;  //** Loop back if we got nothing

        //** process the next round
        for (i=0; i<n; i++) {
            //** Make sure we don't flood the system
            while (running > running_max) {
                gop = opque_waitany(q);
                running--;
                fn = gop_get_private(gop);
                free(fn);
                gop_free(gop, OP_DESTROY);
            }

            running++;
            gop = lio_setattr_gop(lio_gc, lio_gc->creds, fname[i], NULL, "os.timestamp.system.warm", (void *)etext, 0);
            gop_set_private(gop, fname[i]);
            gop_opque_add(q, gop);
        }
    }

    //** Wait for the rest to complete
    opque_finished_submission(q);
    while ((gop = opque_waitany(q)) != NULL) {
        fn = gop_get_private(gop);
        free(fn);
        gop_free(gop, OP_DESTROY);
    }

    tbx_monitor_thread_ungroup(gop_mo(opque_get_gop(q)), MON_MY_THREAD);
    gop_opque_free(q, OP_DESTROY);
    tbx_monitor_thread_destroy(MON_MY_THREAD);

    return(NULL);
}

//*************************************************************************
// object_warm_finish - Does the final steps in warming a file
//*************************************************************************

void object_warm_finish(warm_partition_t *wp, warm_prep_info_t *info)
{
    int state;

    state = (info->write_error == 0) ? 0 : WFE_WRITE_ERR;
    if (info->n_bad == 0) {
        wp->n_good++;
        state |= WFE_SUCCESS;
        if (verbose == 1) info_printf(lio_ifd, 0, "Succeeded with file %s with %d allocations\n", info->fname, info->n_good);
    } else {
        wp->n_bad++;
        state |= WFE_FAIL;
        info_printf(lio_ifd, 0, "Failed with file %s on %d out of %d allocations\n", info->fname, info->n_bad, info->n_good+info->n_bad);
    }
    warm_put_inode(wp->db_inode, info->inode, state, info->n_bad, info->fname);

    tbx_que_put(wp->que_setattr, &(info->fname), TBX_QUE_BLOCK);

    info->fname = NULL;
}

//*************************************************************************
// process_warm_result - Processes the results of the warming operation
//*************************************************************************

void process_warm_result(warm_partition_t *wp, warm_task_t *wt)
{
    int i, j;
    rid_tally_t *tally = wt->rcl->tally;
    int *failed;
    ex_off_t *nbytes;
    ex_id_t *inode;
    warm_prep_info_t *info;
    char **cap;
    gop_op_status_t status;

    //** Set up all the pointers
    failed = wt->failed;
    cap = wt->rcl->cap + wt->slot;
    inode = wt->rcl->inode + wt->slot;
    nbytes = wt->rcl->nbytes + wt->slot;

    //** Update the time
    tally->dtime += gop_time_exec(wt->gop);

    //** First handle all the failed allocations
    //** Since the next step after this assumes all the allocations are good we
    //** do some odd inc/dec to account for that
    status = gop_get_status(wt->gop);
    if (status.op_status == OP_STATE_SUCCESS) {
        for (i=0; i<wt->n_failed; i++) {
            j = failed[i];
            info = apr_hash_get(wp->inode, (const void *)&(inode[j]), sizeof(ex_id_t));
            info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", info->fname, cap[j]);
            cap[j][0] = 0;
            info->n_bad++;
            tally->n_bad++;
        }
    } else { //** Everything failed
        for (i=0; i<wt->n; i++) {
            info = apr_hash_get(wp->inode, (const void *)&(inode[i]), sizeof(ex_id_t));
            info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", info->fname, cap[i]);
            cap[i][0] = 0;
            info->n_bad++;
            tally->n_bad++;
        }
    }

    //** Now process all the allocations and clean up as we go
    for (i=0; i<wt->n; i++) {
        info = apr_hash_get(wp->inode, (const void *)&(inode[i]), sizeof(ex_id_t));
        if (info == NULL) continue;

        if (cap[i][0] != 0) {
            tally->n_good++;
            info->n_good++;
            warm_put_rid(wp->db_rid, wt->rcl->rid_key, inode[i], nbytes[i], WFE_SUCCESS);
        } else {
            warm_put_rid(wp->db_rid, wt->rcl->rid_key, inode[i], nbytes[i], WFE_FAIL);
        }

        if (info) {
            if ((info->n_good+info->n_bad) == info->n_allocs) object_warm_finish(wp, info);
        }
    }

    wt->gop = NULL;  //** Clear it just in case
}

//*************************************************************************
// part_warm_caps - Does the actual warming of the caps for the partition
//*************************************************************************

void part_warm_caps(warm_partition_t *wp, int n_parallel, int n_bulk)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    int n_processed, i;
    rid_cap_list_t *rcl;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    warm_task_t wtask[n_parallel];
    warm_task_t *wt;
    int *failed;

    //** Push the warming task details on the unused stack
    tbx_type_malloc_clear(failed, int, n_parallel* n_bulk);
    for (i=0; i<n_parallel; i++) {
        wtask[i].failed = failed + i*n_bulk;
    }

    q = gop_opque_new();
    i = 0;
    do {
        n_processed = 0;
        for (hi=apr_hash_first(NULL, wp->rid_caps); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, &hlen, (void **)&rcl);

            if (rcl->curr < rcl->n_caps) {
                //** Process a result if needed
                if (i >= n_parallel) {
                    gop = opque_waitany(q);
                    wt = gop_get_private(gop);
                    process_warm_result(wp, wt);
                    gop_free(gop, OP_DESTROY);
                } else {
                    wt = wtask + i;
                }

                //** Generate the next operation
                n_processed++;
                i++;
                wt->rcl = rcl;
                wt->slot = rcl->curr;
                wt->n = rcl->n_caps - rcl->curr;
                if (wt->n > n_bulk) wt->n = n_bulk;
                ibp_cap2depot(rcl->cap[wt->slot], &(wt->depot));
                rcl->curr =+ wt->n;
                gop = ibp_rid_bulk_warm_gop(ic, &(wt->depot), dt, wt->n, &(rcl->cap[wt->slot]), &(wt->n_failed), wt->failed, lio_gc->timeout);
                wt->gop = gop;
                gop_set_private(gop, wt);
                gop_opque_add(q, gop);
            }
        }
    } while (n_processed);

    //** Process the remaining tasks
    while ((gop = opque_waitany(q)) != NULL) {
        wt = gop_get_private(gop);
        process_warm_result(wp, wt);
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);
    free(failed);
}

//*************************************************************************
// part_load_inodes - Loads the inode/fnames for the partition
//*************************************************************************

void part_load_inodes(warm_partition_t *wp, int n_part)
{
    rocksdb_iterator_t *it;
    ex_id_t inode_dummy;
    ex_id_t *inode_ptr;
    size_t nbytes;
    inode_value_t *ival;
    warm_prep_info_t *info;

    //** Make the iterator
    it = rocksdb_create_iterator(wp->wdb->inode->db, wp->wdb->inode->ropt);
    inode_dummy = n_part;
    rocksdb_iter_seek(it, (const char *)&inode_dummy, sizeof(ex_id_t));

    //** Iterate over the partition
    while (rocksdb_iter_valid(it)) {
        inode_ptr = (ex_id_t *)rocksdb_iter_key(it, &nbytes);
        if ((*inode_ptr % wp->wdb->n_partitions) != (unsigned int)n_part) break;  //** Kick out on partition change

        //** Get the record
        ival = (inode_value_t *)rocksdb_iter_value(it, &nbytes);

        //** Now store it in the new structure
        tbx_type_malloc_clear(info, warm_prep_info_t, 1);
        info->inode = *inode_ptr;
        info->n_allocs = ival->n_allocs;
        info->fname = strdup(ival->strings);

        //** Finally add it to the hash
        apr_hash_set(wp->inode, &(info->inode), sizeof(ex_id_t), info);
        rocksdb_iter_next(it);
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// part_load_caps - Loads the caps for the partition
//*************************************************************************

void part_load_caps(warm_partition_t *wp, int n_part)
{
    rocksdb_iterator_t *it;
    rid_prep_key_t rcap_dummy;
    rid_prep_key_t *rcap_ptr;
    size_t nbytes;
    int n_caps, n_caps_size, kick_out;
    ex_off_t *cap_size;
    ex_off_t total_cap_size;
    char *rid_key;
    rid_cap_list_t *rcl;


    //** Make the iterator
    it = rocksdb_create_iterator(wp->wdb->rid->db, wp->wdb->rid->ropt);
    rcap_dummy.id = n_part;
    rocksdb_iter_seek(it, (const char *)&rcap_dummy, sizeof(rcap_dummy));

    //** Iterate over the partition. We do this twice once to get sizes and again to actually store the info
    n_caps = 0;
    n_caps_size = 0;
    total_cap_size = 0;
    rid_key = NULL;
    kick_out = 0;
    while (rocksdb_iter_valid(it)) {
        rcap_ptr = (rid_prep_key_t *)rocksdb_iter_key(it, &nbytes);
        if ((rcap_ptr->id % wp->wdb->n_partitions) != (unsigned int)n_part) { //** Kick out on partition change
            kick_out = 1;
            goto kick_out;
        }

        if ((rid_key == NULL) || (strcmp(rid_key, rcap_ptr->strings) != 0)) { //** RID change
kick_out:
            if (rid_key) {
                tbx_type_malloc_clear(rcl, rid_cap_list_t, 1);
                rcl->n_caps = n_caps;
                rcl->rid_key = rid_key;
                rcl->total_cap_size = total_cap_size;
                tbx_type_malloc_clear(rcl->cap, char *, n_caps);
                tbx_type_malloc_clear(rcl->inode, ex_id_t, n_caps);
                tbx_type_malloc_clear(rcl->nbytes, ex_off_t, n_caps);
                tbx_type_malloc_clear(rcl->cap_buffer, char, n_caps_size + n_caps);
                apr_hash_set(wp->rid_caps, rcl->rid_key, APR_HASH_KEY_STRING, rcl);

                rcl->tally = apr_hash_get(wp->rid_tally, rid_key, APR_HASH_KEY_STRING);
                if (!rcl->tally) {
                    tbx_type_malloc_clear(rcl->tally, rid_tally_t, 1);
                    rcl->tally->rid_key = strdup(rid_key);
                    apr_hash_set(wp->rid_tally, rcl->tally->rid_key, strlen(rid_key), rcl->tally);
                }
                rcl->tally->total_size += total_cap_size;
            }

            if (kick_out) break;

            rid_key = strdup(rcap_ptr->strings);
            n_caps = 0;
            n_caps_size = 0;
            total_cap_size = 0;
        }

        n_caps++;
        //** Get the cap length
        n_caps_size += rcap_ptr->mcap_len;

        //** Get the allocation  size
        cap_size = (ex_off_t *)rocksdb_iter_value(it, &nbytes);
        total_cap_size += *cap_size;

        rocksdb_iter_next(it);
    }

    if (kick_out == 0) {  //** This is definitely a hack to keep from copying the code
        kick_out = 1;
        goto kick_out;
    }
    //** Now iterate over everything again but this time store the caps
    rocksdb_iter_seek(it, (const char *)&rcap_dummy, sizeof(rcap_dummy));
    n_caps = 0;
    n_caps_size = 0;
    rid_key = NULL;
    while (rocksdb_iter_valid(it)) {
        rcap_ptr = (rid_prep_key_t *)rocksdb_iter_key(it, &nbytes);
        if ((rcap_ptr->id % wp->wdb->n_partitions) != (unsigned int)n_part) break; //** Kick out on partition change

        if ((rid_key == NULL) || (strcmp(rid_key, rcap_ptr->strings) != 0)) { //** RID change
            rcl = apr_hash_get(wp->rid_caps, rcap_ptr->strings, APR_HASH_KEY_STRING);  //** The start of the strings is the new RID key
            if (!rcl) {
                fprintf(stderr, "ERROR: rid_key=%s Failed retreiving structure!\n", rcap_ptr->strings); fflush(stderr);
                info_printf(lio_ifd, 0, "ERROR: rid_key=%s Failed retreiving structure!\n", rcap_ptr->strings); tbx_log_flush();
                exit(1);
            }

            rid_key = rcl->rid_key;
            n_caps = 0;
            n_caps_size = 0;
        }

        rcl->cap[n_caps] = rcl->cap_buffer + n_caps_size;
        strcpy(rcl->cap[n_caps], rcap_ptr->strings + rcap_ptr->rid_len + 1);
        rcl->nbytes[n_caps] =+ *(ex_off_t *)rocksdb_iter_value(it, &nbytes);
        rcl->inode[n_caps] = rcap_ptr->id;
        n_caps++;
        n_caps_size += rcap_ptr->mcap_len + 1;

        rocksdb_iter_next(it);
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// part_cleanup - Clean ups the partition structures for the next round
//*************************************************************************

void part_cleanup_and_tally_results(warm_partition_t *wp)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    warm_prep_info_t *info;
    rid_cap_list_t *rcl;

    //** Drop all the info structs
    for (hi=apr_hash_first(NULL, wp->inode); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&info);
        apr_hash_set(wp->inode, &(info->inode), sizeof(ex_id_t), NULL);
        if (info->n_allocs == 0) {  //** Empty file so just flag it as being warmed
            object_warm_finish(wp, info);
        } else if ((info->n_good+info->n_bad) != info->n_allocs) {
            info_printf(lio_ifd, 0, "ERROR: Missing warming op! fname=%s n_good=%d n_bad=%d n_allocs=%d\n", info->fname, info->n_good, info->n_bad, info->n_allocs);
            log_printf(0, "ERROR: Missing warming op! fname=%s n_good=%d n_bad=%d n_allocs=%d\n", info->fname, info->n_good, info->n_bad, info->n_allocs);
            fprintf(stderr, "ERROR: Missing warming op! fname=%s n_good=%d n_bad=%d n_allocs=%d\n", info->fname, info->n_good, info->n_bad, info->n_allocs);
            free(info->fname);
        }

        if (info->write_error > 0) wp->n_write_err++;  //** Update the write error count
        if (info->fname != NULL) free(info->fname);  //** This should only occur with an empty file

        free(info);  //** The fname is destroyed after the warmer attribute is updated in a different thread
    }

    //** and all the caps
    for (hi=apr_hash_first(NULL, wp->rid_caps); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&rcl);
        apr_hash_set(wp->rid_caps, rcl->rid_key, APR_HASH_KEY_STRING, NULL);
        free(rcl->cap);
        free(rcl->inode);
        free(rcl->nbytes);
        free(rcl->cap_buffer);
        free(rcl->rid_key);
        free(rcl);
    }
}

//*************************************************************************
// part_annotate_write_errors - Adds the write error flag
//*************************************************************************

void part_annotate_write_errors(warm_partition_t *wp, int n_partition)
{
    rocksdb_iterator_t *it;
    size_t nbytes;
    ex_id_t inode;
    ex_id_t *inode_ptr;
    warm_prep_info_t *info;

    //** Make the iterator
    it = rocksdb_create_iterator(wp->wdb->write_errors->db, wp->wdb->write_errors->ropt);
    inode = n_partition;
    rocksdb_iter_seek(it, (const char *)&inode, sizeof(ex_id_t));

    while (rocksdb_iter_valid(it)) {
        inode_ptr = (ex_id_t *)rocksdb_iter_key(it, &nbytes);
        if ((*inode_ptr % wp->wdb->n_partitions) != (unsigned int)n_partition) goto next;  //** Skip if not in this partition

        info = apr_hash_get(wp->inode, inode_ptr, sizeof(ex_id_t));
        if (!info) goto next;
        info->write_error = 1;
        info_printf(lio_ifd, 0, "WRITE_ERROR for file %s\n", info->fname);
next:
        rocksdb_iter_next(it);
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// part_annotate_missing_exnode_errors - Flags the files with missing exnodes
//*************************************************************************

void part_annotate_missing_exnode_errors(warm_partition_t *wp, int n_partition)
{
    rocksdb_iterator_t *it;
    size_t nbytes;
    ex_id_t inode;
    ex_id_t *inode_ptr;
    warm_prep_info_t *info;

    //** Make the iterator
    it = rocksdb_create_iterator(wp->wdb->missing_exnode_errors->db, wp->wdb->missing_exnode_errors->ropt);
    inode = n_partition;
    rocksdb_iter_seek(it, (const char *)&inode, sizeof(ex_id_t));

    while (rocksdb_iter_valid(it)) {
        inode_ptr = (ex_id_t *)rocksdb_iter_key(it, &nbytes);
        if ((*inode_ptr % wp->wdb->n_partitions) != (unsigned int)n_partition) goto next;  //** Skip if not in this partition

        info = apr_hash_get(wp->inode, inode_ptr, sizeof(ex_id_t));
        if (!info) goto next;
        wp->n_missing_err++;
        info_printf(lio_ifd, 0, "MISSING_EXNODE for file %s\n", info->fname);
next:
        rocksdb_iter_next(it);
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// warm_dump_summary - Dumps the summary
//*************************************************************************

int warm_dump_summary(warm_partition_t *wp, int summary_mode)
{
    ex_off_t submitted = wp->n_good + wp->n_bad;
    rid_tally_t *rtally;
    tbx_list_t *master;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    ex_off_t nbytes, good, bad, total;
    int i, j;
    double dtime, dtime_total;
    char ppbuf[128], ppbuf2[128], ppbuf3[128];
    char *rkey, *dskey, *config, *value;
    tbx_inip_file_t *ifd;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    tbx_list_iter_t lit;
    char *line_end;
    int return_code = 0;

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Submitted: " XOT "   Success: " XOT "   Fail: " XOT "    Write Errors: " XOT "   Missing Exnodes: " XOT "\n", submitted, wp->n_good, wp->n_bad, wp->n_write_err, wp->n_missing_err);
    return_code = 0;
    if (wp->n_bad > 0) {
        fprintf(stderr, "ERROR Some files failed to warm!\n");
        return_code = EIO;
    }

    if (submitted == 0) return(return_code);


    //** Get the RID config which is used in the summary
    config = rs_get_rid_config(lio_gc->rs);
    ifd = tbx_inip_string_read(config, 1); FATAL_UNLESS(ifd);

    //** Convert it for easier lookup
    ig = tbx_inip_group_first(ifd);
    while (ig != NULL) {
        rkey = tbx_inip_group_get(ig);
        if (strcmp("rid", rkey) == 0) {  //** Found a resource
            //** Now cycle through the attributes
            ele = tbx_inip_ele_first(ig);
            while (ele != NULL) {
                rkey = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(rkey, "rid_key") == 0) {
                    tbx_inip_group_free(ig);
                    tbx_inip_group_set(ig, strdup(value));
                }

                ele = tbx_inip_ele_next(ele);
            }
        }

        ig = tbx_inip_group_next(ig);
    }

    //** Convert the table from a hash to a sorted list
    master = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_simple_free);
    for (hi=apr_hash_first(NULL, wp->rid_tally); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&rtally);
        tbx_list_insert(master, rtally->rid_key, rtally);
    }

    //** Print the summary
    info_printf(lio_ifd, 0, "\n");
    info_printf(lio_ifd, 0, "                                                              Allocations\n");
    info_printf(lio_ifd, 0, "                 RID Key                    Size    Avg Time(us)   Total       Good         Bad\n");
    info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");
    nbytes = good = bad = j = i = 0;
    dtime_total = 0;
    lit = tbx_list_iter_search(master, NULL, 0);
    while (tbx_list_next(&lit, (tbx_list_key_t **)&rkey, (tbx_list_data_t **)&rtally) == 0) {
        j++;
        nbytes += rtally->total_size;
        good += rtally->n_good;
        bad += rtally->n_bad;
        total = rtally->n_good + rtally->n_bad;
        if (rtally->n_bad > 0) i++;

        if ((summary_mode == 0) || ((summary_mode == 1) && (rtally->n_bad == 0))) continue;
        dtime_total += rtally->dtime;
        dtime = rtally->dtime / (double)total;
        dskey = tbx_inip_get_string(ifd, rtally->rid_key, "ds_key", rtally->rid_key);
        line_end = (rtally->n_bad == 0) ? "\n" : "  RID_ERR\n";
        info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "%s", dskey,
                    tbx_stk_pretty_print_double_with_scale_full(1024, (double)rtally->total_size, ppbuf, 1),  tbx_stk_pretty_print_double_with_scale_full(1024, dtime, ppbuf2, 1),
                    total, rtally->n_good, rtally->n_bad, line_end);
        free(dskey);

         //** Go ahead and free the rtally structure and clean up ti wp->rid_tally hash
         free(rtally->rid_key);  //** The rest is free'ed when the list is destroyed.
    }
    if (summary_mode != 0) info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");

    snprintf(ppbuf2, sizeof(ppbuf2), "SUM (%d RIDs, %d bad)", j, i);
    total = good + bad;
    dtime_total = dtime_total / (double)total;
    info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "\n", ppbuf2,
                tbx_stk_pretty_print_double_with_scale_full(1024, (double)nbytes, ppbuf, 1), tbx_stk_pretty_print_double_with_scale_full(1024, dtime_total, ppbuf3, 1), total, good, bad);

    tbx_list_destroy(master);

    //** Cleanup the RIDs
    tbx_inip_destroy(ifd);
    free(config);

    return(return_code);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, start_option, return_code, n_warm, n_bulk;
    warm_partition_t *wp;
    tbx_que_t  *que_setattr;
    apr_thread_t *sa_thread;
    apr_status_t val;
    char *db_prep_base = "/lio/log/warm_prep";
    char *db_bake_base = "/lio/log/warm";
    int summary_mode;

    n_bulk = 1000;
    n_warm = 10;
    return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_warm LIO_COMMON_OPTIONS [-db_prep DB_prep_dir] [-db_bake DB_output_dir] [-serial] [-n_bulk n] [-n_warm n] [-dt time] [-sb] [-sf] [-v]\n");
        lio_print_options(stdout);
        printf("    -db_prep DB_prep_dir   - Input prep directory for the DBes. Default is %s\n", db_prep_base);
        printf("    -db_bake DB_bake_dir   - Outputp directory for the DBes. Default is %s\n", db_bake_base);
        printf("    -serial            - Use the IBP individual warming operation.\n");
        printf("    -n_bulk            - Number of allocations to warn at a time for bulk operations.\n");
        printf("                         NOTE: This is evenly divided among the warming threads. Default is %d\n", n_bulk);
        printf("    -n_warm            - Number of bulk warming operations to perform at a time. Default is %d\n", n_warm);
        printf("    -dt time           - Duration time in sec.  Default is %d sec\n", dt);
        printf("    -sb                - Print the summary but only list the bad RIDs\n");
        printf("    -sf                - Print the the full summary\n");
        printf("    -v                 - Print all Success/Fail messages instead of just errors\n");
        return(1);
    }

    lio_init(&argc, &argv);

    i=1;
    summary_mode = 0;
    verbose = 0;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db_prep") == 0) { //** DB prep input  base directory
            i++;
            db_prep_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-db_bake") == 0) { //** DB output base directory
            i++;
            db_bake_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-serial") == 0) { //** Serial warming mode
            i++;
            bulk_mode = 0;
        } else if (strcmp(argv[i], "-dt") == 0) { //** Time
            i++;
            dt = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-n_bulk") == 0) { //** How many allocatiosn to warm at a time
            i++;
            n_bulk = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-sb") == 0) { //** Print only bad RIDs
            i++;
            summary_mode = 1;
        } else if (strcmp(argv[i], "-sf") == 0) { //** Print the full summary
            i++;
            summary_mode = 2;
        } else if (strcmp(argv[i], "-v") == 0) { //** Verbose printing
            i++;
            verbose = 1;
        }
    } while ((start_option < i) && (i<argc));

    //** Set things up
    tbx_type_malloc_clear(wp, warm_partition_t, 1);
    create_warm_db(db_bake_base, &(wp->db_inode), &(wp->db_rid));  //** Create the output DB
    wp->wdb = open_prep_db(db_prep_base, DB_OPEN_EXISTS, -1);          //** Open the input DB
    apr_pool_create(&(wp->mpool), NULL);
    wp->inode = apr_hash_make(wp->mpool);
    wp->rid_caps = apr_hash_make(wp->mpool);
    wp->rid_tally = apr_hash_make(wp->mpool);
    ic = hack_ds_ibp_context_get(lio_gc->ds);                      //** Get the IBP context to use
    wp->n_bulk = n_bulk;
    que_setattr = tbx_que_create(10000, sizeof(char *));
    wp->que_setattr = que_setattr;

    //** And the setattr thread
    tbx_thread_create_assert(&sa_thread, NULL, setattr_thread,
                                 (void *)que_setattr, lio_gc->mpool);

    //** Process all the files
    //** Loop through all the partitions
    for (j=0;  j<wp->wdb->n_partitions; j++) {
        log_printf(1, "Processing partition %d of n_partitions=%d\n", j, wp->wdb->n_partitions);
        part_load_inodes(wp, j);
        part_annotate_write_errors(wp, j);
        part_annotate_missing_exnode_errors(wp, j);
        part_load_caps(wp, j);
        part_warm_caps(wp, lio_parallel_task_count, n_bulk);
        part_cleanup_and_tally_results(wp);
    }

    tbx_que_set_finished(que_setattr);  //** Let the setattr thread know we are done
    apr_thread_join(&val, sa_thread);

    return_code = warm_dump_summary(wp, summary_mode);

    close_warm_db(wp->db_inode, wp->db_rid);  //** Close the DBs
    close_prep_db(wp->wdb);
    apr_pool_destroy(wp->mpool);
    tbx_que_destroy(que_setattr);
    free(wp);
    lio_shutdown();

    return(return_code);
}


