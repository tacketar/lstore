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

//***********************************************************************
// Simple resource managment implementation
//***********************************************************************

#define _log_module_index 159

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <apr_thread_proc.h>
#include <apr_time.h>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/append_printf.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/fmttypes.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>

#include "blacklist.h"
#include "ds.h"
#include "ex3/system.h"
#include "ex3/types.h"
#include "rs.h"
#include "rs/query_base.h"
#include "rs/simple.h"
#include "service_manager.h"

static lio_rs_simple_priv_t rss_default_options = {
    .section = "rs_simple",
    .fname = "/etc/lio/client.rid",
    .dynamic_mapping = 1,
    .check_interval = 60,
    .check_timeout = 0,
    .min_free = 1*1024*1024*1024,
    .over_avg_fraction = 0.05
};

typedef struct {
    char *key;
    char *value;
}  kvq_ele_t;

typedef struct {
    int n_max;
    int n_used;
    int n_snap;
    kvq_ele_t *list;
} kvq_tbx_list_t;

typedef struct {
    int n_unique;
    int n_pickone;
    kvq_ele_t **unique;
    kvq_ele_t *pickone;
} kvq_table_t;

int _rs_simple_refresh(lio_resource_service_fn_t *rs);

//***********************************************************************
// rss_test - Tests the current RID entry for query compatibility
//***********************************************************************

int rss_test(lio_rsq_base_ele_t *q, lio_rss_rid_entry_t *rse, int n_match, kvq_ele_t *uniq, kvq_ele_t *pickone)
{
    int found;
    int k_unique, k_pickone, k_op;
    int v_unique, v_pickone, v_op;
    int err, i, nlen;
    char *key, *val, *str_tomatch;
    tbx_list_iter_t it;
    tbx_list_compare_t cmp_fn;

    //** Factor the ops
    k_unique = q->key_op & RSQ_BASE_KV_UNIQUE;
    k_pickone = q->key_op & RSQ_BASE_KV_PICKONE;
    k_op = q->key_op & RSQ_BASE_KV_OP_BITMASK;
    v_unique = q->val_op & RSQ_BASE_KV_UNIQUE;
    v_pickone = q->val_op & RSQ_BASE_KV_PICKONE;
    v_op = q->val_op & RSQ_BASE_KV_OP_BITMASK;

    log_printf(15, "key=%s val=%s n_attr=%d k_uniq=%d k_pick=%d v_uniq=%d v_pick=%d\n", q->key, q->val, tbx_list_key_count(rse->attr), k_unique, k_pickone, v_unique, v_pickone);

    str_tomatch = (q->key != NULL) ? q->key : "";
    nlen = strlen(str_tomatch);
    tbx_list_strncmp_set(&cmp_fn, nlen);
    it = tbx_list_iter_search_compare(rse->attr, str_tomatch, &cmp_fn, 0);
    found = 0;
    while ((found==0) && ((err=tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&val)) == 0)) {
        //** First check the key for a comparison
        str_tomatch = (q->key != NULL) ? q->key : "";
        switch (k_op) {
        case (RSQ_BASE_KV_EXACT):
            if (strcmp(key, str_tomatch) == 0) found = 1;
            break;
        case (RSQ_BASE_KV_PREFIX):
            if (strncmp(key, str_tomatch, nlen) == 0) found = 1;
            break;
        case (RSQ_BASE_KV_ANY):
            found = 1;
            break;
        }

        log_printf(15, "ckey=%s found=%d\n", key, found);
        //** If got a match then compare the unique or pickone
        if (found == 1) {
            if (n_match > 0) {
                if (k_unique > 0) {
                    for (i=0; i<n_match; i++) {
                        if (strcmp(key, uniq[i].key) == 0) {
                            found = 0;
                            break;
                        }
                    }
                } else if (k_pickone > 0) {
                    if (strcmp(key, pickone->key) != 0) {
                        found = 0;
                    }
                }
            }

            //** If still got a match do the same for the value
            //** Compare the value based on the op
            str_tomatch = (q->val != NULL) ? q->val : "";
            switch (v_op) {
            case (RSQ_BASE_KV_EXACT):
                if (strcmp(val, str_tomatch) != 0) found = 0;
                break;
            case (RSQ_BASE_KV_PREFIX):
                if (strncmp(val, str_tomatch, nlen) != 0) found = 0;
                break;
            case (RSQ_BASE_KV_ANY):
                break;
            }

            //** If still a match then do the uniq/pickone check if needed on the value
            if (found == 1) {
                if (n_match > 0) {
                    if (v_unique > 0) {
                        for (i=0; i<n_match; i++) {
                            if (uniq[i].value != NULL) {  //** This could be NULL if a previous RID wasn't in the config table
                                if (strcmp(val, uniq[i].value) == 0) {
                                    found = 0;
                                    break;
                                }
                            }
                        }
                    } else if (v_pickone > 0) {
                        if (pickone->value != NULL) {  //** Same as above for uniq check
                            if (strcmp(val, pickone->value) != 0) {
                                found = 0;
                            }
                        }
                    }
                }
            }
        }
    }

    log_printf(15, "last err=%d found=%d\n", err, found);

    //** Got a match so store it if needed
    if (found == 1) {
        if (k_unique > 0) uniq[n_match].key = key;
        if (v_unique > 0) uniq[n_match].value = val;
        if (k_pickone > 0) pickone->key = key;
        if (v_pickone > 0) pickone->value = val;
    }

    return(found);
}

//***********************************************************************
// rs_simple_request - Processes a simple RS request
//***********************************************************************

gop_op_generic_t *rs_simple_request(lio_resource_service_fn_t *arg, data_attr_t *da, rs_query_t *rsq, data_cap_set_t **caps, lio_rs_request_t *req, int req_size, lio_rs_hints_t *hints_list, int fixed_size, int n_rid, int ignore_fixed_err, int timeout)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)arg->priv;
    lio_rsq_base_t *query_global = (lio_rsq_base_t *)rsq;
    lio_rsq_base_t *query_local;
    kvq_table_t kvq_global, kvq_local, *kvq;
    apr_hash_t *pick_from;
    lio_rid_change_entry_t *rid_change;
    ex_off_t change;
    gop_op_status_t status;
    gop_opque_t *que;
    lio_rss_rid_entry_t *rse;
    lio_rsq_base_ele_t *q;
    int slot, rnd_off, i, j, k, i_unique, i_pickone, found, err_cnt, loop, loop_end, avg_full_skip, full_skip_retry;
    int rnd_shuffle, rnd_shuffle_off, sslot;
    int *shuffle;
    int state, *a, *b, *op_state, unique_size;
    tbx_stack_t *stack;

    log_printf(15, "rs_simple_request: START rss->n_rids=%d n_rid=%d req_size=%d fixed_size=%d ignore=%d\n", rss->n_rids, n_rid, req_size, fixed_size, ignore_fixed_err);

    for (i=0; i<req_size; i++) {req[i].rid_key = NULL; req[i].gop = NULL; } //** Clear the result in case of an error

    apr_thread_mutex_lock(rss->lock);
    i = _rs_simple_refresh(arg);  //** Check if we need to refresh the data
    if (i != 0) {
        apr_thread_mutex_unlock(rss->lock);
        return(gop_dummy(gop_failure_status));
    }

    //** Determine the query sizes and make the processing arrays
    memset(&kvq, 0, sizeof(kvq));
    rs_query_count(arg, rsq, &i, &(kvq_global.n_unique), &(kvq_global.n_pickone));

    log_printf(15, "rs_simple_request: n_unique=%d n_pickone=%d\n", kvq_global.n_unique, kvq_global.n_pickone);
    tbx_log_flush();

    //** Make space the for the uniq and pickone fields.
    //** Make sure we have space for at least 1 more than we need of each to pass to the routines even though they aren't used
    j = (kvq_global.n_pickone == 0) ? 1 : kvq_global.n_pickone + 1;
    tbx_type_malloc_clear(kvq_global.pickone, kvq_ele_t, j);

    unique_size = kvq_global.n_unique + 1;
    tbx_type_malloc_clear(kvq_global.unique, kvq_ele_t *, unique_size);
    log_printf(15, "MALLOC j=%d\n", unique_size);
    for (i=0; i<unique_size; i++) {
        tbx_type_malloc_clear(kvq_global.unique[i], kvq_ele_t, n_rid);
    }

    //** We don't allow these on the local but make a temp space anyway
    kvq_local.n_pickone = 0;
    tbx_type_malloc_clear(kvq_local.pickone, kvq_ele_t, 1);
    kvq_global.n_unique = 0;
    tbx_type_malloc_clear(kvq_local.unique, kvq_ele_t *, 1);
    tbx_type_malloc_clear(kvq_local.unique[0], kvq_ele_t, n_rid);

    status = gop_success_status;

    que = gop_opque_new();
    stack = tbx_stack_new();

    err_cnt = 0;
    found = 0;

    for (i=0; i < n_rid; i++) {
        full_skip_retry = 0;
disable_too_full:
        found = 0;
        avg_full_skip = 0;
        loop_end = 1;
        query_local = NULL;
        rnd_off = tbx_random_get_int64(0, rss->n_rids-1);
        rnd_shuffle = tbx_random_get_int64(0, rss->n_shuffles-1);
        rnd_shuffle_off = tbx_random_get_int64(0, rss->n_rids-1);
        shuffle = rss->shuffle + rnd_shuffle*rss->n_rids;

        if (hints_list != NULL) {
            query_local = (lio_rsq_base_t *)hints_list[i].local_rsq;
            if (query_local != NULL) {
                loop_end = 2;
                rs_query_count(arg, query_local, &j, &(kvq_local.n_unique), &(kvq_local.n_pickone));
                if ((kvq_local.n_unique != 0) && (kvq_local.n_pickone != 0)) {
                    log_printf(0, "Unsupported use of pickone/unique in local RSQ hints_list[%d]=%s!\n", i, hints_list[i].fixed_rid_key);
                    status.op_status = OP_STATE_FAILURE;
                    status.error_code = RS_ERROR_EMPTY_STACK;
                    hints_list[i].status = RS_ERROR_HINTS_INVALID_LOCAL;
                    err_cnt++;
                    continue;
                }
            }

            if (i<fixed_size) {  //** Use the fixed list for assignment
                rse = tbx_list_search(rss->rid_table, hints_list[i].fixed_rid_key);
                if (rse == NULL) {
                    log_printf(0, "Missing element in hints list[%d]=%s! Ignoring check.\n", i, hints_list[i].fixed_rid_key);
                    hints_list[i].status = RS_ERROR_FIXED_NOT_FOUND;
                    continue;   //** Skip the check
                }
                rnd_off = rse->slot;
                rnd_shuffle_off = 0;
                shuffle = rss->shuffle;
            }
        }

        //** See if we use a restrictive list.  Ususally used when rebalancing space
        pick_from = (hints_list != NULL) ? hints_list[i].pick_from : NULL;
        rid_change = NULL;
        change = 0;
        for (k=0; k<req_size; k++) {
            if (req[k].rid_index == i) {
                change += req[k].size;
            }
        }

        for (j=0; j<rss->n_rids; j++) {
            sslot = (rnd_shuffle_off + j) % rss->n_rids;
            slot = (shuffle[sslot] + rnd_off) % rss->n_rids;
            rse = rss->random_array[slot];
            if (pick_from != NULL) {
                rid_change = apr_hash_get(pick_from, rse->rid_key, APR_HASH_KEY_STRING);
                log_printf(15, "PICK_FROM != NULL i=%d j=%d slot=%d rse->rid_key=%s rse->status=%d rid_change=%p\n", i, j, slot, rse->rid_key, rse->status, rid_change);

                if (rid_change == NULL) continue;  //** Not in our list so skip to the next
                ex_off_t delta = rid_change->delta - change;
                log_printf(15, "PICK_FROM != NULL i=%d j=%d slot=%d rse->rid_key=%s rse->status=%d rc->state=%d (" XOT ") > " XOT "????\n", i, j, slot, rse->rid_key, rse->status, rid_change->state, delta, rid_change->tolerance);

                //** Make sure we don't overshoot the target
                if (rid_change->state == 1) continue;   //** Already converged RID
                if (rid_change->delta <= 0) continue;   //** Need to move data OFF this RID
                if ((change - rid_change->delta) > rid_change->tolerance) continue;  //**delta>0 if we made it here
            }

            log_printf(15, "i=%d j=%d slot=%d rse->rid_key=%s rse->status=%d rse->too_full=%d\n", i, j, slot, rse->rid_key, rse->status, rse->too_full);
            if ((rse->status != RS_STATUS_UP) && (i>=fixed_size)) continue;  //** Skip this if disabled and not in the fixed list
            if ((i>=fixed_size) && (full_skip_retry == 0) && (rse->too_full == 1)) {  //** Skip this if disabled and not in the fixed list
                log_printf(2, "i=%d full skip full_skip_retry=%d too_full=%d\n", i, full_skip_retry, rse->too_full);
                avg_full_skip = 1;
                continue;
            }

            tbx_stack_empty(stack, 1);
            q = query_global->head;
            kvq = &kvq_global;
            for (loop=0; loop<loop_end; loop++) {
                i_unique = 0;
                i_pickone = 0;
                while (q != NULL) {
                    state = -1;
                    switch (q->op) {
                    case RSQ_BASE_OP_KV:
                        state = rss_test(q, rse, i, kvq->unique[i_unique], &(kvq->pickone[i_pickone]));
                        log_printf(15, "KV: key=%s val=%s i_unique=%d i_pickone=%d loop=%d rss_test=%d rse->rid_key=%s\n", q->key, q->val, i_unique, i_pickone, loop, state, rse->rid_key);
                        tbx_log_flush();
                        if ((q->key_op & RSQ_BASE_KV_UNIQUE) || (q->val_op & RSQ_BASE_KV_UNIQUE)) i_unique++;
                        if ((q->key_op & RSQ_BASE_KV_PICKONE) || (q->val_op & RSQ_BASE_KV_PICKONE)) i_pickone++;
                        break;
                    case RSQ_BASE_OP_NOT:
                        a = (int *)tbx_stack_pop(stack);
                        state = (*a == 0) ? 1 : 0;
                        //log_printf(0, "NOT(%d)=%d\n", *a, state);
                        free(a);
                        break;
                    case RSQ_BASE_OP_AND:
                        a = (int *)tbx_stack_pop(stack);
                        b = (int *)tbx_stack_pop(stack);
                        state = (*a) && (*b);
                        //log_printf(0, "%d AND %d = %d\n", *a, *b, state);
                        free(a);
                        free(b);
                        break;
                    case RSQ_BASE_OP_OR:
                        a = (int *)tbx_stack_pop(stack);
                        b = (int *)tbx_stack_pop(stack);
                        state = a || b;
                        //log_printf(0, "%d OR %d = %d\n", *a, *b, state);
                        free(a);
                        free(b);
                        break;
                    }

                    tbx_type_malloc(op_state, int, 1);
                    *op_state = state;
                    tbx_stack_push(stack, (void *)op_state);
                    log_printf(15, " stack_size=%d loop=%d push state=%d\n",tbx_stack_count(stack), loop, state);
                    tbx_log_flush();
                    q = q->next;
                }

                if (query_local != NULL) {
                    q = query_local->head;
                    kvq = &kvq_local;
                }
            }

            op_state = (int *)tbx_stack_pop(stack);
            state = -1;
            if (op_state != NULL) {
                state = *op_state;
                free(op_state);
            }

            log_printf(2, "i=%d fixed_size=%d state=%d avg_full_skip=%d\n", i, fixed_size, state, avg_full_skip);
            if (op_state == NULL) {
                log_printf(1, "rs_simple_request: ERROR processing i=%d EMPTY STACK\n", i);
                found = 0;
                status.op_status = OP_STATE_FAILURE;
                status.error_code = RS_ERROR_EMPTY_STACK;
            } else if  (state == 1) { //** Got one
                log_printf(15, "rs_simple_request: processing i=%d ds_key=%s\n", i, rse->ds_key);
                found = 1;
                if ((i<fixed_size) && hints_list) hints_list[i].status = RS_ERROR_OK;

                for (k=0; k<req_size; k++) {
                    if (req[k].rid_index == i) {
                        log_printf(15, "rs_simple_request: ADDING i=%d ds_key=%s, rid_key=%s size=" XOT "\n", i, rse->ds_key, rse->rid_key, req[k].size);
                        req[k].rid_key = strdup(rse->rid_key);
                        req[k].gop = ds_allocate(rss->ds, rse->ds_key, da, req[k].size, caps[k], timeout);
                        gop_opque_add(que, req[k].gop);
                    }
                }

                if (rid_change != NULL) { //** Flag that I'm tweaking things.  The caller does the source pending/delta half
                    rid_change->delta -= change;
                    rid_change->state = ((llabs(rid_change->delta) <= rid_change->tolerance) || (rid_change->tolerance == 0)) ? 1 : 0;
                }
                break;  //** Got one so exit the RID scan and start the next one
            } else if (i<fixed_size) {  //** This should have worked so flag an error
                if (hints_list) {
                   log_printf(1, "Match fail in fixed list[%d]=%s!\n", i, hints_list[i].fixed_rid_key);
                   hints_list[i].status = RS_ERROR_FIXED_MATCH_FAIL;
                } else {
                   log_printf(1, "Match fail in fixed list and no hints are provided!\n");
                }

                if ((ignore_fixed_err & 1) == 0) err_cnt++;
                break;  //** Skip to the next in the list
            } else {
                found = 0;
            }
        }

        if ((found == 0) && (i>=fixed_size) && (avg_full_skip == 1)) {  //** Try again but disable the avg_full check
            log_printf(1, "rs_simple_request: ERROR processing i=%d.  Attempting retry disabling average full check\n", i);
            avg_full_skip = 0;
            full_skip_retry = 1;
            goto disable_too_full;
        }

        if ((ignore_fixed_err & 2) == 0) {   //** See if it's Ok to return a partial list
            if ((found == 0) && (i>=fixed_size)) break;
        }
    }

    //** Clean up
    log_printf(15, "FREE j=%d\n", unique_size);
    for (i=0; i<unique_size; i++) {
        free(kvq_global.unique[i]);
    }
    free(kvq_global.unique);
    free(kvq_global.pickone);

    free(kvq_local.unique[0]);
    free(kvq_local.unique);
    free(kvq_local.pickone);

    tbx_stack_free(stack, 1);

    log_printf(15, "rs_simple_request: END n_rid=%d found=%d err_cnt=%d q_tasks=%d\n", n_rid, found, err_cnt, gop_opque_task_count(que));

    apr_thread_mutex_unlock(rss->lock);

    if ((ignore_fixed_err & 2) == 2) found = 1;   //** It's Ok to return a partial list

    if ((found == 0) || (err_cnt>0)) {
        gop_opque_free(que, OP_DESTROY);

        for (i=0; i<req_size; i++) {  //** Clear the result in case of an error
            req[i].rid_key = NULL;
            req[i].gop = NULL;
        }

        if (status.error_code == 0) {
            log_printf(1, "rs_simple_request: Can't find enough RIDs! requested=%d found=%d err_cnt=%d\n", n_rid, found, err_cnt);
            status.op_status = OP_STATE_FAILURE;
            status.error_code = RS_ERROR_NOT_ENOUGH_RIDS;
        }
        return(gop_dummy(status));
    }

    return(opque_get_gop(que));
}

//***********************************************************************
// rs_simple_get_rid_value - Returns the value associated with ther RID key
//    provided
//***********************************************************************

char *rs_simple_get_rid_value(lio_resource_service_fn_t *arg, char *rid_key, char *key)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)arg->priv;
    lio_rss_rid_entry_t *rse;
    char *value = NULL;


    apr_thread_mutex_lock(rss->lock);
    rse = tbx_list_search(rss->rid_table, rid_key);
    if (rse != NULL) {
        value = tbx_list_search(rse->attr, key);
        if (value != NULL)  value = strdup(value);
    }
    apr_thread_mutex_unlock(rss->lock);

    return(value);
}


//***********************************************************************
// rse_free - Frees the memory associated with the RID entry
//***********************************************************************

void rs_simple_rid_free(tbx_list_data_t *arg)
{
    lio_rss_rid_entry_t *rse = (lio_rss_rid_entry_t *)arg;

    log_printf(15, "START\n");
    tbx_log_flush();

    if (rse == NULL) return;

    log_printf(15, "removing rid_key=%s ds_key=%s attr=%p\n", rse->rid_key, rse->ds_key, rse->attr);

    tbx_list_destroy(rse->attr);

    if (rse->ds_key != NULL) free(rse->ds_key);

    free(rse);
}

//***********************************************************************
//  rs_load_entry - Loads an RID entry fro mthe file
//     NOTE:  Assumes the RS snd hte BL structure is locked!
//***********************************************************************

lio_rss_rid_entry_t *rss_load_entry(tbx_inip_group_t *grp, lio_blacklist_t *bl)
{
    lio_rss_rid_entry_t *rse;
    tbx_inip_element_t *ele;
    char *key, *value;

    //** Create the new RS list
    tbx_type_malloc_clear(rse, lio_rss_rid_entry_t, 1);
    rse->status = RS_STATUS_UP;
    rse->attr = tbx_list_create(1, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_simple_free);

    //** Now cycle through the attributes
    ele = tbx_inip_ele_first(grp);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        value = tbx_inip_ele_get_value(ele);
        if (strcmp(key, "rid_key") == 0) {  //** This is the RID so store it separate
            rse->rid_key = strdup(value);
            tbx_list_insert(rse->attr, key, rse->rid_key);
        } else if (strcmp(key, "ds_key") == 0) {  //** This is what gets passed to the data service
            rse->ds_key = strdup(value);
        } else if (strcmp(key, "status") == 0) {  //** Current status
            rse->status = atoi(value);
            if (bl) {
                if ((rse->status == RS_STATUS_IGNORE) || (rse->status == RS_STATUS_DOWN)) {
                    blacklist_add(bl, rse->rid_key, 1, 1);
                }
            }
        } else if (strcmp(key, "space_free") == 0) {  //** Free space
            rse->space_free = tbx_stk_string_get_integer(value);
        } else if (strcmp(key, "space_used") == 0) {  //** Used bytes
            rse->space_used = tbx_stk_string_get_integer(value);
        } else if (strcmp(key, "space_total") == 0) {  //** Total bytes
            rse->space_total = tbx_stk_string_get_integer(value);
        } else if ((key != NULL) && (value != NULL)) {  //** Anything else is an attribute
            tbx_list_insert(rse->attr, key, strdup(value));
        }

        log_printf(15, "rss_load_entry: key=%s value=%s\n", key, value);

        ele = tbx_inip_ele_next(ele);
    }

    //** Make sure we have an RID and DS link
    if ((rse->rid_key == NULL) || (rse->ds_key == NULL)) {
        log_printf(1, "rss_load_entry: missing RID or ds_key! rid=%s ds_key=%s\n", rse->rid_key, rse->ds_key);
        rs_simple_rid_free(rse);
        rse = NULL;
    }

    return(rse);
}

//***********************************************************************
// rss_get_rid_config - Gets the rid configuration
//***********************************************************************

char *rss_get_rid_config(lio_resource_service_fn_t *rs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    char *buffer, *key, *val;
    int bufsize;
    apr_hash_index_t *hi;
    lio_rss_check_entry_t *ce;
    int used;
    apr_ssize_t klen;
    tbx_list_iter_t ait;

    buffer = NULL;

    apr_thread_mutex_lock(rss->lock);

    bufsize = (rss->last_config_size > 0) ? rss->last_config_size/2 + 4096 : 100*1024;

    do {
        if (buffer != NULL) {
            free(buffer);
            buffer = NULL;
        }
        bufsize = 2 * bufsize;
        tbx_type_malloc_clear(buffer, char, bufsize);

        used = 0;
        for (hi = apr_hash_first(NULL, rss->rid_mapping); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&key, &klen, (void **)&ce);

            //** Print the Standard fields
            tbx_append_printf(buffer, &used, bufsize, "[rid]\n");
            tbx_append_printf(buffer, &used, bufsize, "rid_key=%s\n", ce->rid_key);
            tbx_append_printf(buffer, &used, bufsize, "ds_key=%s\n", ce->ds_key);
            tbx_append_printf(buffer, &used, bufsize, "status=%d\n", ce->re->status);
            tbx_append_printf(buffer, &used, bufsize, "space_used=" XOT "\n", ce->re->space_used);
            tbx_append_printf(buffer, &used, bufsize, "space_free=" XOT "\n", ce->re->space_free);
            tbx_append_printf(buffer, &used, bufsize, "space_total=" XOT "\n", ce->re->space_total);

            //** Now cycle through printing the attributes
            ait = tbx_list_iter_search(ce->re->attr, (tbx_list_key_t *)NULL, 0);
            while (tbx_list_next(&ait, (tbx_list_key_t **)&key, (tbx_list_data_t **)&val) == 0) {
                //if ((strcmp("rid_key", key) == 0) || (strcmp("ds_key", key) == 0)) tbx_append_printf(buffer, &used, bufsize, "%s=%s-BAD\n", key, val);
                if ((strcmp("rid_key", key) != 0) && (strcmp("ds_key", key) != 0)) tbx_append_printf(buffer, &used, bufsize, "%s=%s\n", key, val);
            }

            if (tbx_append_printf(buffer, &used, bufsize, "\n") < 0) break;  //** Kick out have to grow the buffer
        }
    } while (used >= bufsize);

    rss->last_config_size = used;
    log_printf(5, "last_config_size=%d config=%s\n", rss->last_config_size, buffer);

    apr_thread_mutex_unlock(rss->lock);


    return(buffer);
}

//***********************************************************************
// _rss_clear_check_table - Clears the check table
//   NOTE:  Assumes rs is already loacked!
//***********************************************************************

void _rss_clear_check_table(lio_data_service_fn_t *ds, apr_hash_t *table, apr_pool_t *mpool)
{
    apr_hash_index_t *hi;
    lio_rss_check_entry_t *entry;
    const void *rid;
    apr_ssize_t klen;

    for (hi = apr_hash_first(NULL, table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, &rid, &klen, (void **)&entry);
        apr_hash_set(table, rid, klen, NULL);

        ds_inquire_destroy(ds, entry->space);
        free(entry->ds_key);
        free(entry->rid_key);
        free(entry);
    }

    apr_hash_clear(table);
}

//***********************************************************************
// rss_mapping_register - Registration for mapping updates
//***********************************************************************

void rss_mapping_register(lio_resource_service_fn_t *rs, lio_rs_mapping_notify_t *map_version)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;

    apr_thread_mutex_lock(rss->update_lock);
    apr_hash_set(rss->mapping_updates, map_version, sizeof(lio_rs_mapping_notify_t *), map_version);
    apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_mapping_unregister - DE-Register for mapping updates
//***********************************************************************

void rss_mapping_unregister(lio_resource_service_fn_t *rs, lio_rs_mapping_notify_t *map_version)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;

    apr_thread_mutex_lock(rss->update_lock);
    apr_hash_set(rss->mapping_updates, map_version, sizeof(lio_rs_mapping_notify_t *), NULL);
    apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_mapping_noitfy - Notifies all registered entities
//***********************************************************************

void rss_mapping_notify(lio_resource_service_fn_t *rs, int new_version, int status_change)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    apr_hash_index_t *hi;
    lio_rs_mapping_notify_t *rsn;
    apr_ssize_t klen;
    void *rid;

    if (status_change > 0) status_change = tbx_random_get_int64(0, 100000);

    apr_thread_mutex_lock(rss->update_lock);
    for (hi = apr_hash_first(NULL, rss->mapping_updates); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&rid, &klen, (void **)&rsn);
        apr_thread_mutex_lock(rsn->lock);
        rsn->map_version = new_version;
        if (status_change > 0) rsn->status_version = status_change;
        apr_thread_cond_broadcast(rsn->cond);
        apr_thread_mutex_unlock(rsn->lock);
    }
    apr_thread_mutex_unlock(rss->update_lock);
}

//***********************************************************************
// rss_translate_cap_set - Translates the cap set based o nthe latest RID mappings
//***********************************************************************

void rss_translate_cap_set(lio_resource_service_fn_t *rs, char *rid_key, data_cap_set_t *cs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    lio_rss_check_entry_t *rce;

    apr_thread_mutex_lock(rss->lock);
    rce = apr_hash_get(rss->rid_mapping, rid_key, APR_HASH_KEY_STRING);
    if (rce != NULL) ds_translate_cap_set(rss->ds, rid_key, rce->ds_key, cs);
    apr_thread_mutex_unlock(rss->lock);
}

//***********************************************************************
// rss_perform_check - Checks the RIDs and updates their status
//***********************************************************************

int rss_perform_check(lio_resource_service_fn_t *rs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    apr_hash_index_t *hi;
    lio_rss_check_entry_t *ce;
    int prev_status, status_change;
    char *rid;
    apr_ssize_t klen;
    gop_op_status_t status;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    lio_blacklist_t *bl;
    double total_space, used_space, avg, global_avg;

    log_printf(5, "START\n");

    global_avg = rss->over_avg_fraction + rss->avg_fraction; //** We start off using the old avg_fraction and update it in the end

    //** Generate the task list
    q = gop_opque_new();

    status_change = 0;
    apr_thread_mutex_lock(rss->lock);
    for (hi = apr_hash_first(NULL, rss->rid_mapping); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&rid, &klen, (void **)&ce);
        gop = ds_res_inquire(rss->ds, ce->ds_key, rss->da, ce->space, rss->check_timeout);
        gop_set_private(gop, ce);
        gop_opque_add(q, gop);
    }
    apr_thread_mutex_unlock(rss->lock);

    //** Wait for them all to complete
    opque_waitall(q);

    //** Process the results
    apr_thread_mutex_lock(rss->lock);
    log_printf(5, "modify_time=" TT " current_check=" TT "\n", rss->modify_time, rss->current_check);

    //** Load the blacklist if available
    bl = lio_lookup_service(rss->ess, ESS_RUNNING, "blacklist");

    //** Clean out any old blacklisted RIDs from a previous RS run
    if (bl) blacklist_remove_rs_added(bl);

    total_space = 1;
    used_space = 0;
    for (gop = opque_get_next_finished(q); gop != NULL; gop = opque_get_next_finished(q)) {
        status = gop_get_status(gop);
        ce = gop_get_private(gop);
        prev_status = ce->re->status;
        if (status.op_status == OP_STATE_SUCCESS) {  //** Got a valid response
            ce->re->space_free = ds_res_inquire_get(rss->ds, DS_INQUIRE_FREE, ce->space);
            ce->re->space_used = ds_res_inquire_get(rss->ds, DS_INQUIRE_USED, ce->space);
            ce->re->space_total = ds_res_inquire_get(rss->ds, DS_INQUIRE_TOTAL, ce->space);
            if (ce->re->status != RS_STATUS_IGNORE) {
                if (ce->re->space_free <= (int)rss->min_free) {
                    ce->re->status = RS_STATUS_OUT_OF_SPACE;
                } else {
                    ce->re->status = RS_STATUS_UP;
                }

                //** check if too full
                avg = (double)ce->re->space_used / (double)ce->re->space_total;
                ce->re->too_full = (avg <= global_avg) ? 0 : 1;

                total_space += ce->re->space_total;
                used_space += ce->re->space_used;
            }
        } else {  //** No response so mark it as down
            if (ce->re->status != RS_STATUS_IGNORE) ce->re->status = RS_STATUS_DOWN;
        }
        if (prev_status != ce->re->status) status_change = 1;

        //** Blacklist it if needed
        if ((bl) && ((ce->re->status == RS_STATUS_IGNORE) || (ce->re->status == RS_STATUS_DOWN))) {
            blacklist_add(bl, ce->re->rid_key, 1, 1);
        }
        log_printf(15, "ds_key=%s prev_status=%d new_status=%d\n", ce->ds_key, prev_status, ce->re->status);
        gop_free(gop, OP_DESTROY);
    }

    rss->avg_fraction = used_space / total_space; //** Update the avg used
    gop_opque_free(q, OP_DESTROY);
    apr_thread_mutex_unlock(rss->lock);

    log_printf(5, "END status_change=%d\n", status_change);

    return(status_change);
}

//***********************************************************************
// _rss_make_check_table - Makes the RID->Resource mapping table
//   NOTE:  Assumes rs is already loacked!
//***********************************************************************

void _rss_make_check_table(lio_resource_service_fn_t *rs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    lio_rss_check_entry_t *ce, *ce2;
    lio_rss_rid_entry_t *re;
    int i;

    //** Clear out the old one
    _rss_clear_check_table(rss->ds, rss->rid_mapping, rss->mpool);

    //** Now make the new one
    rss->unique_rids = 1;
    for (i=0; i<rss->n_rids; i++) {
        re = rss->random_array[i];
        tbx_type_malloc(ce, lio_rss_check_entry_t, 1);
        ce->ds_key = strdup(re->ds_key);
        ce->rid_key = strdup(re->rid_key);
        ce->space = ds_inquire_create(rss->ds);
        ce->re = re;

        //** Check for dups.  If so we only keep the 1st entry and spew a log message
        ce2 = apr_hash_get(rss->rid_mapping, ce->rid_key, APR_HASH_KEY_STRING);
        if (ce2 == NULL) {  //** Unique so add it
            apr_hash_set(rss->rid_mapping, ce->rid_key, APR_HASH_KEY_STRING, ce);
        } else {  //** Dup so disable dynamic mapping by unsetting unique_rids
            log_printf(0, "WARNING duplicate RID found.  Dropping dynamic mapping.  res=%s ---  new res=%s\n", ce2->ds_key, ce->ds_key);
            rss->unique_rids = 0;
            ds_inquire_destroy(rss->ds, ce->space);
            free(ce->rid_key);
            free(ce->ds_key);
            free(ce);
        }
    }

    return;
}

//***********************************************************************
//  rss_check_thread - checks for availabilty on all the RIDS
//***********************************************************************

void *rss_check_thread(apr_thread_t *th, void *data)
{
    lio_resource_service_fn_t *rs = (lio_resource_service_fn_t *)data;
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    int do_notify, map_version, status_change;
    apr_interval_time_t dt;

    tbx_monitor_thread_create(MON_MY_THREAD, "rss_check_thread: Monitoring file=%s", rss->fname);
    dt = apr_time_from_sec(rss->check_interval);

    apr_thread_mutex_lock(rss->lock);
    rss->current_check = 0;  //** Triggers a reload
    do {
        log_printf(5, "LOOP START check_timeout=%d\n", rss->check_timeout);
        _rs_simple_refresh(rs);  //** Do a quick check and see if the file has changed

        do_notify = 0;
        if (rss->current_check != rss->modify_time) { //** Need to reload
            rss->current_check = rss->modify_time;
            do_notify = 1;
        }
        map_version = rss->modify_time;
        apr_thread_mutex_unlock(rss->lock);

        status_change = (rss->check_timeout <= 0) ? 0 : rss_perform_check(rs);

        if (((do_notify == 1) && (rss->dynamic_mapping == 1)) || (status_change != 0)) {
            tbx_monitor_thread_message(MON_MY_THREAD, "Update occurred. Notifying clients");
            rss_mapping_notify(rs, map_version, status_change);
        }
        log_printf(5, "LOOP END status_change=%d\n", status_change);

        apr_thread_mutex_lock(rss->lock);
        if (rss->shutdown == 0) apr_thread_cond_timedwait(rss->cond, rss->lock, dt);
    } while (rss->shutdown == 0);

    //** Clean up
    _rss_clear_check_table(rss->ds, rss->rid_mapping, rss->mpool);
    apr_thread_mutex_unlock(rss->lock);

    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
// _rs_generate_shuffles - Generates the shuffles and stores them
//***********************************************************************

void _rs_generate_shuffle(int n_shuffles, int n_rids, int *shuffle)
{
    int i, j, k, rnd;
    int *s;
    uint64_t *rnd_vals;
    double dn;

    tbx_type_malloc(rnd_vals, uint64_t, n_rids);

    for (i=0; i<n_shuffles; i++) {
        s = shuffle + i*n_rids;

        tbx_random_get_bytes(rnd_vals, sizeof(uint64_t)*n_rids);

        //** Init the slots
        for (j=0; j<n_rids; j++) s[j] = j;
        if (i==0) continue;  //* the initial shuffle is not done

        //** Now do the shuffle
        for (j=1; j<n_rids; j++) {
            dn = (1.0 * rnd_vals[j]) / (UINT64_MAX + 1.0);
            rnd = j + (n_rids-1 - j) * dn;
            k = s[rnd];
            s[rnd] = s[j-1];
            s[j-1] = k;
        }
    }

    free(rnd_vals);
}

//***********************************************************************
// _rs_simple_load - Loads the config file
//   NOTE:  No locking is performed!
//***********************************************************************

int _rs_simple_load(lio_resource_service_fn_t *res, char *fname)
{
    tbx_inip_group_t *ig;
    char *key;
    lio_rss_rid_entry_t *rse;
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)res->priv;
    tbx_list_iter_t it;
    int i, n, err;
    tbx_inip_file_t *kf;
    lio_blacklist_t *bl;
    double total_space, space_used, avg, global_avg;

    log_printf(5, "START fname=%s n_rids=%d\n", fname, rss->n_rids);

    err = 0;

    //** Open the file
    kf = tbx_inip_file_read(fname, 1);
    if (!kf) return(-1);

    //** Load the blacklist if available
    bl = lio_lookup_service(rss->ess, ESS_RUNNING, "blacklist");

    //** Clean out any old blacklisted RIDs from a previous RS run
    if (bl) blacklist_remove_rs_added(bl);

    //** Create the new RS list
    rss->rid_table = tbx_list_create(0, &tbx_list_string_compare, NULL, NULL, rs_simple_rid_free);
    log_printf(15, "rs_simple_load: sl=%p\n", rss->rid_table);

    //** And load it
    total_space = 1;
    space_used = 0;
    ig = tbx_inip_group_first(kf);
    while (ig != NULL) {
        key = tbx_inip_group_get(ig);
        if (strcmp("rid", key) == 0) {  //** Found a resource
            rse = rss_load_entry(ig, bl);
            if (rse != NULL) {
                tbx_list_insert(rss->rid_table, rse->rid_key, rse);
                if (rse->status != RS_STATUS_IGNORE) {
                    total_space += rse->space_total;
                    space_used += rse->space_used;
                }
            }
        }
        ig = tbx_inip_group_next(ig);
    }

    //** Make the randomly permuted table
    rss->n_rids = tbx_list_key_count(rss->rid_table);
    if (rss->n_rids == 0) {
        log_printf(0, "ERROR: n_rids=%d\n", rss->n_rids);
        fprintf(stderr, "ERROR: n_rids=%d\n", rss->n_rids);
        if (rss->rid_table) tbx_list_destroy(rss->rid_table);
        rss->rid_table = NULL;
        err = 1;
    } else {
        rss->avg_fraction = space_used / total_space; //** Update the avg used
        global_avg = rss->over_avg_fraction + rss->avg_fraction;

        //** Generate the random shuffles
        tbx_type_malloc_clear(rss->shuffle, int, rss->n_rids*rss->n_shuffles);
        _rs_generate_shuffle(rss->n_shuffles, rss->n_rids, rss->shuffle);

        log_printf(2, "over_avg_fraction=%lf avg_fraction=%lf global_avg=%lf used=%lf total=%lf\n", rss->over_avg_fraction, rss->avg_fraction, rss->avg_fraction, space_used, total_space);
        tbx_type_malloc_clear(rss->random_array, lio_rss_rid_entry_t *, rss->n_rids);
        it = tbx_list_iter_search(rss->rid_table, (tbx_list_key_t *)NULL, 0);
        for (i=0; i < rss->n_rids; i++) {
            tbx_list_next(&it, (tbx_list_key_t **)&key, (tbx_list_data_t **)&rse);

            n = tbx_random_get_int64(0, rss->n_rids-1);
            while (rss->random_array[n] != NULL) {
                n = (n+1) % rss->n_rids;
            }
            rse->slot = n;
            rss->random_array[n] = rse;

            //** check if too full
            avg = (rse->space_total > 0) ? (double)rse->space_used / (double)rse->space_total : 0;
            rse->too_full = (avg <= global_avg) ? 0 : 1;
            log_printf(2, "rid_key=%s status=%d too_full=%d used=" XOT " total=" XOT " avg=%lf\n", rse->rid_key, rse->status, rse->too_full, rse->space_used, rse->space_total, avg);
        }
    }

    tbx_inip_destroy(kf);

    log_printf(5, "END n_rids=%d\n", rss->n_rids);

    return(err);
}


//***********************************************************************
// _rs_simple_refresh - Refreshes the RID table if needed
//   NOTE: No Locking is performed
//***********************************************************************

int _rs_simple_refresh(lio_resource_service_fn_t *rs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    struct stat sbuf;
    tbx_list_t *old_table;
    lio_rss_rid_entry_t **old_random;
    int *old_shuffle;
    int err, old_n_rids;

    if (stat(rss->fname, &sbuf) != 0) {
        log_printf(1, "RS file missing!!! Using old definition. fname=%s\n", rss->fname);
        if (rss->rid_table == NULL) rss->rid_table = tbx_list_create(0, &tbx_list_string_compare, NULL, NULL, rs_simple_rid_free);  //** Make an empty one to keep things from crashing
        return(0);
    }

    if (rss->modify_time != sbuf.st_mtime) {  //** File changed so reload it
        log_printf(5, "RELOADING data\n");
        old_n_rids = rss->n_rids;    //** Preserve the old info in case of an error
        old_table = rss->rid_table;
        old_random = rss->random_array;
        old_shuffle = rss->shuffle;

        err = _rs_simple_load(rs, rss->fname);  //** Load the new file
        if (err == 0) {
            rss->modify_time = sbuf.st_mtime;
            if (old_table != NULL) tbx_list_destroy(old_table);
            if (old_random != NULL) free(old_random);
            if (old_shuffle != NULL) free(old_shuffle);
            _rss_make_check_table(rs);  //** and make the new inquiry table
            apr_thread_cond_signal(rss->cond);  //** Notify the check thread that we made a change
        } else {
            rss->n_rids = old_n_rids;
            rss->rid_table = old_table;
            rss->random_array = old_random;
            rss->shuffle = old_shuffle;
        }
        return(err);
    }

    return(0);
}

//***********************************************************************
// rss_print_running_config - Prints the running config
//***********************************************************************

void rss_print_running_config(lio_resource_service_fn_t *rs, FILE *fd, int print_section_heading)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    char text[1024];
    char *rids;

    if (print_section_heading) fprintf(fd, "[%s]\n", rss->section);
    fprintf(fd, "type = %s\n", RS_TYPE_SIMPLE);
    fprintf(fd, "fname = %s\n", rss->fname);
    fprintf(fd, "dynamic_mapping = %d\n", rss->dynamic_mapping);
    fprintf(fd, "check_interval = %d #seconds\n", rss->check_interval);
    fprintf(fd, "check_timeout = %d #seconds (if 0 then no resource checks are made)\n", rss->check_timeout);
    fprintf(fd, "min_free = %s\n", tbx_stk_pretty_print_int_with_scale(rss->min_free, text));
    fprintf(fd, "\n");

    //** Now print all the rids
    rids = rs_get_rid_config(rs);
    fprintf(fd, "#------------------RID information start----------------------\n");
    if (rids) {
        fprintf(fd, "%s", rids);
        free(rids);
    }
    fprintf(fd, "#------------------RID information end----------------------\n");
    fprintf(fd, "\n");
}

//***********************************************************************
// rs_simple_destroy - Destroys the simple RS service
//***********************************************************************

void rs_simple_destroy(lio_resource_service_fn_t *rs)
{
    lio_rs_simple_priv_t *rss = (lio_rs_simple_priv_t *)rs->priv;
    apr_status_t value;

    //** Notify the depot check thread
    apr_thread_mutex_lock(rss->lock);
    log_printf(15, "rs_simple_destroy: sl=%p\n", rss->rid_table);
    tbx_log_flush();

    rss->shutdown = 1;
    apr_thread_cond_broadcast(rss->cond);
    apr_thread_mutex_unlock(rss->lock);

    //** Wait for it to shutdown
    apr_thread_join(&value, rss->check_thread);

    //** Now we can free up all the space
    apr_thread_mutex_destroy(rss->lock);
    apr_thread_cond_destroy(rss->cond);
    apr_pool_destroy(rss->mpool);  //** This also frees the hash tables

    if (rss->rid_table != NULL) tbx_list_destroy(rss->rid_table);

    free(rss->random_array);
    free(rss->shuffle);
    free(rss->fname);
    free(rss->section);
    free(rss);
    free(rs);
}

//***********************************************************************
// rs_simple_create - Creates a simple resource management service from
//    the given file.
//***********************************************************************

lio_resource_service_fn_t *rs_simple_create(void *arg, tbx_inip_file_t *kf, char *section)
{
    lio_service_manager_t *ess = (lio_service_manager_t *)arg;
    lio_rs_simple_priv_t *rss;
    lio_resource_service_fn_t *rs;

    if (section == NULL) section = rss_default_options.section;

    //** Create the new RS list
    tbx_type_malloc_clear(rss, lio_rs_simple_priv_t, 1);

    rss->section = strdup(section);

    assert_result(apr_pool_create(&(rss->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(rss->lock), APR_THREAD_MUTEX_DEFAULT, rss->mpool);
    apr_thread_mutex_create(&(rss->update_lock), APR_THREAD_MUTEX_DEFAULT, rss->mpool);
    apr_thread_cond_create(&(rss->cond), rss->mpool);
    rss->rid_mapping = apr_hash_make(rss->mpool);
    rss->mapping_updates = apr_hash_make(rss->mpool);

    rss->n_shuffles = 20;  //** This is hardcoded.. but just in case it's here if we want to vary it

    rss->ess = ess;
    rss->ds = lio_lookup_service(ess, ESS_RUNNING, ESS_DS);
    rss->da = lio_lookup_service(ess, ESS_RUNNING, ESS_DA);

    //** Set the resource service fn ptrs
    tbx_type_malloc_clear(rs, lio_resource_service_fn_t, 1);
    rs->priv = rss;
    rs->get_rid_config = rss_get_rid_config;
    rs->register_mapping_updates = rss_mapping_register;
    rs->unregister_mapping_updates = rss_mapping_unregister;
    rs->translate_cap_set = rss_translate_cap_set;
    rs->query_new = rs_query_base_new;
    rs->query_dup = rs_query_base_dup;
    rs->query_add = rs_query_base_add;
    rs->query_append = rs_query_base_append;
    rs->query_destroy = rs_query_base_destroy;
    rs->query_print = rs_query_base_print;
    rs->query_parse = rs_query_base_parse;
    rs->get_rid_value = rs_simple_get_rid_value;
    rs->data_request = rs_simple_request;
    rs->destroy_service = rs_simple_destroy;
    rs->print_running_config = rss_print_running_config;
    rs->type = RS_TYPE_SIMPLE;

    //** This is the file to use for loading the RID table
    rss->fname = tbx_inip_get_string(kf, section, "fname", rss_default_options.fname);
    rss->dynamic_mapping = tbx_inip_get_integer(kf, section, "dynamic_mapping", rss_default_options.dynamic_mapping);
    rss->check_interval = tbx_inip_get_integer(kf, section, "check_interval", rss_default_options.check_interval);
    rss->check_timeout = tbx_inip_get_integer(kf, section, "check_timeout", rss_default_options.check_timeout);
    rss->min_free = tbx_inip_get_integer(kf, section, "min_free", rss_default_options.min_free);
    rss->over_avg_fraction = tbx_inip_get_double(kf, section, "over_avg_fraction", rss_default_options.over_avg_fraction);

    //** Set the modify time to force a change
    rss->modify_time = 0;

    //** Load the RID table
    assert_result(_rs_simple_refresh(rs), 0);
    if (rss->check_timeout > 0) rss_perform_check(rs);  //** And do an initial update if needed

    //** Launch the check thread
    //** NOTE: This will also do an initial check like before which isn't ideal.
    tbx_thread_create_assert(&(rss->check_thread), NULL, rss_check_thread, (void *)rs, rss->mpool);

    return(rs);
}

