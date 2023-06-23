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
    char *fname;
    char *exnode;
    ex_id_t inode;
    int write_err;
    int n_left;
    int n_good;
    int n_bad;
} inode_entry_t;

typedef struct {
    int n_used;
    int curr_slot;
    int n_running;
    int running_slot;
    int n_failed;
    int serial_max;
    int *failed;
    char **cap;
    inode_entry_t **inode;
    ex_off_t *nbytes;
    char *key;
    ibp_depot_t depot;
    gop_op_generic_t *gop;
} rid_warm_t;

typedef struct {
    char *rid_key;
    ex_off_t good;
    ex_off_t bad;
    ex_off_t nbytes;
    ex_off_t dtime;
    rid_warm_t *warm;
} warm_hash_entry_t;

typedef struct {
   char *cap;
   ex_off_t nbytes;
} warm_cap_info_t;

typedef struct {
    apr_thread_t *thread;
    tbx_que_t *que;
    tbx_que_t *que_setattr;
    apr_hash_t *hash;
    apr_pool_t *mpool;
    inode_entry_t **inode;
    gop_opque_t *q;
    int write_err;
    ex_off_t good;
    ex_off_t bad;
    ex_off_t werr;
    int n_bulk;
} warm_thread_t;

typedef struct {
    lio_path_tuple_t tuple;
    warm_cap_info_t *cap;
    char *fname;
    char *exnode;
    ibp_context_t *ic;
    apr_hash_t *hash;
    apr_pool_t *mpool;
    int n;
    ex_id_t inode;
    int write_err;
} warm_t;

apr_hash_t *tagged_rids = NULL;
apr_pool_t *tagged_pool = NULL;
tbx_stack_t *tagged_keys = NULL;
warm_results_db_t *results = NULL;
int n_partitions = 100;
int verbose = 0;
int do_setattr = 1;

ibp_context_t *ic = NULL;
int bulk_mode = 1;
static int dt = 86400;

//*************************************************************************
// parse_tag_file - Parse the file contianing the RID's for tagging
//*************************************************************************

void parse_tag_file(char *fname)
{
    tbx_inip_file_t *fd;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *key, *value, *v;

    fd = tbx_inip_file_read(fname, 1);
    if (fd == NULL) return;

    apr_pool_create(&tagged_pool, NULL);
    tagged_rids = apr_hash_make(tagged_pool);
    tagged_keys = tbx_stack_new();

    //** Cycle through the blocks storing both the segment block information and also the cap blocks
    g = tbx_inip_group_find(fd, "tag");
    ele = tbx_inip_ele_first(g);
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "rid_key") == 0) {
            v = tbx_inip_ele_get_value(ele);
            value = strdup(v);
            info_printf(lio_ifd, 0, "Tagging RID %s\n", value);
            apr_hash_set(tagged_rids, value, APR_HASH_KEY_STRING, value);
            tbx_stack_push(tagged_keys, value);
        }

        ele = tbx_inip_ele_next(ele);
    }

    tbx_inip_destroy(fd);

    if (apr_hash_count(tagged_rids) == 0) {
        tbx_stack_free(tagged_keys, 0);
        apr_pool_destroy(tagged_pool);
        tagged_pool = NULL;
        tagged_rids = NULL;
    } else {
        info_printf(lio_ifd, 0, "\n");
    }
}

//*************************************************************************
// object_warm_finish - Does the final steps in warming a file
//*************************************************************************

void object_warm_finish(warm_thread_t *w, inode_entry_t *inode)
{
    int state;
    int mod;

    state = (inode->write_err == 0) ? 0 : WFE_WRITE_ERR;
    if (inode->n_bad == 0) {
        w->good++;
        state |= WFE_SUCCESS;
        if (verbose == 1) info_printf(lio_ifd, 0, "Succeeded with file %s with %d allocations\n", inode->fname, inode->n_good);
    } else {
        w->bad++;
        state |= WFE_FAIL;
        info_printf(lio_ifd, 0, "Failed with file %s on %d out of %d allocations\n", inode->fname, inode->n_bad, inode->n_good+inode->n_bad);
    }

    mod = inode->inode % n_partitions;
    warm_put_inode(results->p[mod]->inode, inode->inode, state, inode->n_bad, inode->fname);

    if (do_setattr) {
        tbx_que_put(w->que_setattr, &(inode->fname), TBX_QUE_BLOCK);
    } else {
        if (inode->fname) free(inode->fname);
    }
    free(inode);
}

//*************************************************************************
// process_warm_op - Processes the results of the warming op
//*************************************************************************

void process_warm_op(warm_hash_entry_t *wr, warm_thread_t *w)
{
    rid_warm_t *r = wr->warm;
    int i, j, mod;
    int *failed;
    ex_off_t *nbytes;
    inode_entry_t **inode;
    char **cap;
    gop_op_status_t status;

    //** Set up all the pointers
    failed = r->failed + r->running_slot;
    nbytes = r->nbytes + r->running_slot;
    cap = r->cap + r->running_slot;
    inode = r->inode + r->running_slot;

    //** Update the time
    wr->dtime += gop_time_exec(r->gop);

    //** First handle all the failed allocations
    //** Since the next step after this assumes all the allocations are good we
    //** do some odd inc/dec to account for that
    status = gop_get_status(r->gop);
    if (status.op_status == OP_STATE_SUCCESS) {
        for (i=0; i<r->n_failed; i++) {
            j = failed[i];
            info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", inode[j]->fname, cap[j]);
            inode[j]->n_bad++;
            free(cap[j]);
            cap[j] = NULL; //** Flag that it's bad
            wr->bad++;
        }
    } else { //** Everything failed
        for (i=0; i<r->n_running; i++) {
            info_printf(lio_ifd, 1, "ERROR: %s  cap=%s\n", inode[i]->fname, cap[i]);
            inode[i]->n_bad++;
            free(cap[i]);
            cap[i] = NULL; //** Flag that it's bad
            wr->bad++;
        }
    }

    //** Now process all the allocations and clean up as we go
    for (i=0; i<r->n_running; i++) {
        mod = inode[i]->inode % n_partitions;
        if (cap[i] != NULL) {
            free(cap[i]);
            wr->good++;
            inode[i]->n_good++;
            warm_put_rid(results->p[mod]->rid, wr->rid_key, inode[i]->inode, nbytes[i], WFE_SUCCESS);
        } else {
            warm_put_rid(results->p[mod]->rid, wr->rid_key, inode[i]->inode, nbytes[i], WFE_FAIL);
        }

        inode[i]->n_left--;
        if (inode[i]->n_left == 0) object_warm_finish(w, inode[i]);
    }

    gop_free(r->gop, OP_DESTROY);
    r->gop = NULL;
}

//*************************************************************************
// warm_serial_fn - Performs the serial warming
//*************************************************************************

gop_op_status_t warm_serial_fn(void *arg, int id)
{
    warm_hash_entry_t *wr = (warm_hash_entry_t *)arg;
    rid_warm_t *r = wr->warm;
    int i, slot, nbad;
    gop_op_status_t status = gop_success_status;
    gop_op_status_t gs;
    gop_op_generic_t *gop;
    gop_opque_t *q = gop_opque_new();

    //** Submit tasks and process them as needed
    nbad = 0;
    opque_start_execution(q);
    for (i=0; i<r->n_running; i++) {
        gop = ibp_modify_alloc_gop(ic, r->cap[r->running_slot + i], -1, dt, -1, lio_gc->timeout);
        gop_set_myid(gop, i);
        gop_opque_add(q, gop);

        if (i>r->serial_max) { //** Reap a task
            gop = opque_waitany(q);
            slot = gop_get_myid(gop);
            gs = gop_get_status(gop);
            if (gs.op_status != OP_STATE_SUCCESS) {
                r->failed[r->running_slot + nbad] = slot;
                nbad++;
            }
            gop_free(gop, OP_DESTROY);
        }
    }

    //** Finish the processing
    while ((gop = opque_waitany(q)) != NULL) {
        slot = gop_get_myid(gop);
        gs = gop_get_status(gop);
        if (gs.op_status != OP_STATE_SUCCESS) {
            r->failed[r->running_slot + nbad] = slot;
            nbad++;
        }
        gop_free(gop, OP_DESTROY);
    }
    r->n_failed = nbad;

    if (nbad == r->n_running) status = gop_failure_status;

    gop_opque_free(q, OP_DESTROY);

    return(status);
}

//*************************************************************************
// submit_warm_op - Warming op
//*************************************************************************

void submit_warm_op(warm_hash_entry_t *wr, warm_thread_t *w)
{
    rid_warm_t *r = wr->warm;

    r->n_running = r->n_used;
    r->running_slot = r->curr_slot;
    if (bulk_mode == 1) {
        r->gop = ibp_rid_bulk_warm_gop(ic, &(r->depot), dt, r->n_running, &(r->cap[r->curr_slot]), &(r->n_failed), &(r->failed[r->curr_slot]), lio_gc->timeout);
    } else {
        r->gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, warm_serial_fn, (void *)wr, NULL, 1);
    }
    r->curr_slot = (r->curr_slot == 0) ? w->n_bulk : 0;
    r->n_used = 0;  //** Reset the todo count
    gop_set_private(r->gop, wr);
    gop_opque_add(w->q, r->gop);
}

//*************************************************************************
// warm_rid_wait - Waits for the current warming gop fo rthe rid to complete and cleans up
//*************************************************************************

void warm_rid_wait(warm_thread_t *w, rid_warm_t *r)
{
    warm_hash_entry_t *wr2;
    gop_op_generic_t *gop;

    while (r->gop) { //** Already have a task running so we have to wait
        gop = opque_waitany(w->q);
        wr2 = gop_get_private(gop);
        process_warm_op(wr2, w);
    }
}

//*************************************************************************
// rid_todo_slot - Gets the next free slot to use
//        If needed the routine will force a warming call if needed.
//*************************************************************************

int rid_todo_slot(warm_hash_entry_t *wr, warm_thread_t *w)
{
    rid_warm_t *r = wr->warm;
    int slot;

    if (r->n_used == w->n_bulk) { //** Generate a warm task
        warm_rid_wait(w, r);  //** Already have a task running so we have to wait

        //** Generate the new operation and submit it
        submit_warm_op(wr, w);
    }

    slot = r->curr_slot + r->n_used;
    r->n_used++;

    return(slot);
}

//*************************************************************************
// rid_todo_create - Creates a todo structure for the RID
//*************************************************************************

rid_warm_t *rid_todo_create(char *rid_key, int n, char *cap)
{
    rid_warm_t *rid;

    tbx_type_malloc_clear(rid, rid_warm_t, 1);
    tbx_type_malloc_clear(rid->cap, char *, 2*n);
    tbx_type_malloc_clear(rid->inode, inode_entry_t *, 2*n);
    tbx_type_malloc_clear(rid->nbytes, ex_off_t, 2*n);
    tbx_type_malloc_clear(rid->failed, int, 2*n);
    rid->curr_slot = 0;
    rid->n_running = 0;
    rid->n_used = 0;
    rid->serial_max = 20;
    rid->key = rid_key;
    ibp_cap2depot(cap, &(rid->depot));

    return(rid);
}

//*************************************************************************
// rid_todo_destroy - Destroys the RID todo structure
//*************************************************************************

void rid_todo_destroy(rid_warm_t *rid)
{
    free(rid->cap);
    free(rid->inode);
    free(rid->nbytes);
    free(rid->failed);
    free(rid);
}

//*************************************************************************
//  gen_warm_task
//*************************************************************************

int total_gen_caps=0;

void gen_warm_tasks(warm_thread_t *w, inode_entry_t *inode)
{
    tbx_inip_file_t *fd;
    warm_hash_entry_t *wrid = NULL;
    rid_warm_t *r;
    char *etext, *mcap;
    int slot, cnt;
    char *exnode = inode->exnode;  //** We save this just in case the blocks are all warmed and the inode is destroyed during the call

    log_printf(15, "warming fname=%s, dt=%d\n", inode->fname, dt);
    fd = tbx_inip_string_read(exnode, 1);
    tbx_inip_group_t *g;

    cnt = 0;
    g = tbx_inip_group_first(fd);
    inode->n_left = 1;  //** Offset it to keep it from getting reaped during the processing
    while (g) {
        if (strncmp(tbx_inip_group_get(g), "block-", 6) == 0) { //** Got a data block
            //** Get the manage cap first
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "manage_cap", NULL);
            if (!etext) {
                info_printf(lio_ifd, 1, "MISSING_MCAP_ERROR: fname=%s  block=%s\n", inode->fname, tbx_inip_group_get(g));
                goto next;
            }
            mcap = tbx_stk_unescape_text('\\', etext);
            free(etext);

            //** Get the RID key
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "rid_key", NULL);
            if (etext != NULL) {
                wrid = apr_hash_get(w->hash, etext, APR_HASH_KEY_STRING);
                if (wrid == NULL) { //** 1st time so need to make an entry
                    tbx_type_malloc_clear(wrid, warm_hash_entry_t, 1);
                    wrid->rid_key = etext;
                    apr_hash_set(w->hash, wrid->rid_key, APR_HASH_KEY_STRING, wrid);

                    wrid->warm = rid_todo_create(etext, w->n_bulk, mcap);
                } else {
                    free(etext);
                }
            }
            r = wrid->warm;

            //** Get the data size and update the counts
            wrid->nbytes += tbx_inip_get_integer(fd, tbx_inip_group_get(g), "max_size", 0);

            inode->n_left++;  //** Incr this before the todo_slot call in case we flush all the existing caps so it won't be reaped
            cnt++;

            //** Get the slot
            slot = rid_todo_slot(wrid, w);

            //** Fill in the rest of the fields
            r->inode[slot] = inode;
            r->cap[slot] = mcap;

            //** Check if it was tagged
            if (tagged_rids != NULL) {
                if (apr_hash_get(tagged_rids, wrid->rid_key, APR_HASH_KEY_STRING) != NULL) {
                    info_printf(lio_ifd, 0, "RID_TAG: %s  rid_key=%s\n", inode->fname, wrid->rid_key);
                }
            }
        }
next:
        g = tbx_inip_group_next(g);
    }

    inode->n_left--; //** Undo our offset so we can reap the inode
    tbx_inip_destroy(fd);

    free(exnode);

    //** Check if there was nothing to do. If so go ahead and mark the inode as complete
    if (cnt == 0) object_warm_finish(w, inode);
}

//*************************************************************************
// setattr_thread - Sets the warming attribute
//*************************************************************************

void *setattr_thread(apr_thread_t *th, void *data)
{
    tbx_que_t *que = (tbx_que_t *)data;
    gop_opque_t *q = gop_opque_new();
    gop_op_generic_t *gop;
    int i, n, running;
    int running_max = 1000;
    int n_max = 1000;
    char *fname[n_max];
    char *fn;

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
            gop = lio_setattr_gop(lio_gc, lio_gc->creds, fname[i], NULL, "os.timestamp.system.warm", (void *)lio_gc->host_id, lio_gc->host_id_len);
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

    gop_opque_free(q, OP_DESTROY);

    return(NULL);
}

//*************************************************************************
// warming_thread - Main work thread for submitting warming operations
//*************************************************************************

void *warming_thread(apr_thread_t *th, void *data)
{
    warm_thread_t *w = (warm_thread_t *)data;
    int n, i;
    gop_op_generic_t *gop;
    warm_hash_entry_t *wr;

    tbx_monitor_thread_create(MON_MY_THREAD, "warming_thread");
    w->q = gop_opque_new();
    tbx_monitor_thread_group(gop_mo(opque_get_gop(w->q)), MON_MY_THREAD);
    tbx_monitor_obj_label(gop_mo(opque_get_gop(w->q)), "warming_thread_que");

    while (!tbx_que_is_finished(w->que)) {
        n = tbx_que_bulk_get(w->que, w->n_bulk, w->inode, TBX_QUE_BLOCK);

        //** Process any submitted tasks that occurred during the warming
        while ((gop = opque_get_next_finished(w->q)) != NULL) {
            wr = gop_get_private(gop);
            process_warm_op(wr, w);
        }

        if (n <= 0) continue;  //** Loop back if we got nothing

        //** Submit all the tasks
        for (i=0; i<n; i++) {
            gen_warm_tasks(w, w->inode[i]);
        }
    }

    //** Flush all remaining RIDs to be warmed
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    i = 0;
    for (hi=apr_hash_first(NULL, w->hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&wr);
        if (wr->warm->n_used > 0) {
            if (wr->warm->gop) warm_rid_wait(w, wr->warm);  //** Already have a task running so we have to wait

            submit_warm_op(wr, w);
            i++;
            if (i > 20) {
                gop = opque_waitany(w->q);
                wr = gop_get_private(gop);
                process_warm_op(wr, w);
            }
        }
    }

    //** Process the results
    opque_finished_submission(w->q);
    while ((gop = opque_waitany(w->q)) != NULL) {
        wr = gop_get_private(gop);
        process_warm_op(wr, w);
    }

    //** And clean up
    for (hi=apr_hash_first(NULL, w->hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&wr);
        rid_todo_destroy(wr->warm);
    }

    tbx_monitor_thread_ungroup(gop_mo(opque_get_gop(w->q)), MON_MY_THREAD);
    gop_opque_free(w->q, OP_DESTROY);
    tbx_monitor_thread_destroy(MON_MY_THREAD);

    free(w->inode);
    return(NULL);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, j, start_option, rg_mode, ftype, prefix_len, return_code, n_warm, n_bulk, n_put, n, nleft;
    char *fname, *path;
    inode_entry_t *inode;
    inode_entry_t **inode_list;
    tbx_que_t *que, *que_setattr;
    apr_thread_t *sa_thread;
    char *keys[] = { "system.exnode", "system.inode", "system.write_errors" };
    char *vals[3];
    char *db_base = "/lio/log/warm";
    int slot, v_size[3];
    os_object_iter_t *it;
    lio_os_regex_table_t *rp_single, *ro_single;
    tbx_list_t *master;
    apr_hash_index_t *hi;
    apr_ssize_t klen;
    char *rkey, *config, *value;
    char *line_end;
    warm_hash_entry_t *mrid, *wrid;
    tbx_inip_file_t *ifd;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    void *piter;
    char ppbuf[128], ppbuf2[128], ppbuf3[128];
    lio_path_tuple_t tuple;
    ex_off_t total, good, bad, nbytes, submitted, werr, missing_err, count, last;
    tbx_list_iter_t lit;
    tbx_stack_t *stack;
    int recurse_depth = 10000;
    int summary_mode;
    warm_thread_t *w;
    double dtime, dtime_total;

    n_bulk = 1000;
    n_warm = 2;
    n_put = 1000;
    count = 0;
    last = 0;
    que_setattr = NULL;

    if (argc < 2) {
        printf("\n");
        printf("lio_warm LIO_COMMON_OPTIONS [-db DB_output_dir] [-t tag.cfg] [-rd recurse_depth] [-n_partitions n] [-serial] [-dt time] [-sb] [-sf] [-v] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -db DB_output_dir  - Output Directory for the DBes. Default is %s\n", db_base);
        printf("    -t tag.cfg         - INI file with RID to tag by printing any files usign the RIDs\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -n_partitions n    - Number of partitions for managing workload. Defaults to %d\n", n_partitions);
        printf("    -serial            - Use the IBP individual warming operation.\n");
        printf("    -n_bulk            - Number of allocations to warn at a time for bulk operations.\n");
        printf("                         NOTE: This is evenly divided among the warming threads. Default is %d\n", n_bulk);
        printf("    -n_warm            - Number of warming threads. Default is %d\n", n_warm);
        printf("    -dt time           - Duration time in sec.  Default is %d sec\n", dt);
        printf("    -setwarm n         - Sets the system.warm attribute if 1. Default is %d\n", do_setattr);
        printf("    -count n           - Post an update every n files processed\n");
        printf("    -sb                - Print the summary but only list the bad RIDs\n");
        printf("    -sf                - Print the the full summary\n");
        printf("    -v                 - Print all Success/Fail messages instead of just errors\n");
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the path args
    rp_single = ro_single = NULL;
    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    summary_mode = 0;
    verbose = 0;
    do {
        start_option = i;
        if (strcmp(argv[i], "-db") == 0) { //** DB output base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-serial") == 0) { //** Serial warming mode
            i++;
            bulk_mode = 0;
        } else if (strcmp(argv[i], "-dt") == 0) { //** Time
            i++;
            dt = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-n_bulk") == 0) { //** Time
            i++;
            n_bulk = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-n_warm") == 0) { //** Time
            i++;
           n_warm = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-n_partitions") == 0) { //** Number of partitions to manage workload
            i++;
            n_partitions = atoi(argv[i]);
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
        } else if (strcmp(argv[i], "-t") == 0) { //** Got a list of RIDs to tag
            i++;
            parse_tag_file(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-count") == 0) { //** They want ongoing updates
            i++;
            count = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-setwarm") == 0) { //** See if they want to set the warming attr
            i++;
            do_setattr = atoi(argv[i]);
            i++;
        }
    } while ((start_option < i) && (i<argc));
    start_option = i;

    if (rg_mode == 0) {
        if (i>=argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_option--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    piter = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));

    results = create_results_db(db_base, n_partitions);  //** Create the DB
    ic = hack_ds_ibp_context_get(lio_gc->ds);

    n_bulk = n_bulk / n_warm;  //** Rescale the # of bulk caps to warn at a time

    tbx_type_malloc(inode_list, inode_entry_t *, n_put);

    //** Launch the warming threads
    que = tbx_que_create(4*n_bulk*n_warm, sizeof(inode_entry_t *));
    if (do_setattr) que_setattr = tbx_que_create(10000, sizeof(char *));
    tbx_type_malloc_clear(w, warm_thread_t, n_warm);
    for (j=0; j<n_warm; j++) {
        w[j].n_bulk = n_bulk;
        apr_pool_create(&(w[j].mpool), NULL);
        w[j].hash = apr_hash_make(w[j].mpool);
        w[j].que = que;
        w[j].que_setattr = que_setattr;
        tbx_type_malloc(w[j].inode, inode_entry_t *, w[j].n_bulk);

        // ** Launch the backend thread
        tbx_thread_create_assert(&(w[j].thread), NULL, warming_thread,
                                 (void *)(&w[j]), lio_gc->mpool);
    }

    //** And the setattr thread
    if (do_setattr) {
        tbx_thread_create_assert(&sa_thread, NULL, setattr_thread, (void *)que_setattr, lio_gc->mpool);
    }

    //** Process all the files
    submitted = good = bad = werr = missing_err = 0;
    slot = 0;
    return_code = 0;
    while ((path = tbx_stdinarray_iter_next(piter)) != NULL) {
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                free(path);
                return_code = EINVAL;
                continue;
            }
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }
        free(path);  //** No longer needed.  lio_path_resolve will strdup

        v_size[0] = v_size[1] = -tuple.lc->max_attr; v_size[2] = -tuple.lc->max_attr;
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE_FLAG, recurse_depth, keys, (void **)vals, v_size, 3);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            goto finished;
        }

        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            if ((ftype & OS_OBJECT_SYMLINK) || (v_size[0] == -1)) { //** We skip symlinked files and files missing exnodes
                info_printf(lio_ifd, 0, "MISSING_EXNODE_ERROR for file %s\n", fname);
                missing_err++;
                free(fname);
                for (i=-0; i<3; i++) {
                    if (v_size[i] > 0) free(vals[i]);
                }
                continue;
            }

            tbx_type_malloc(inode, inode_entry_t, 1);
            inode_list[slot] = inode;
            slot++;
            inode->fname = fname;
            inode->exnode = vals[0];
            inode->write_err = 0;

            if (v_size[2] != -1) {
                werr++;
                inode->write_err = 1;
                info_printf(lio_ifd, 0, "WRITE_ERROR for file %s\n", fname);
                if (vals[2] != NULL) {
                    free(vals[2]);
                    vals[2] = NULL;
                }
            }

            inode->inode = 0;
            if (v_size[1] > 0) {
               sscanf(vals[1], XIDT, &(inode->inode));
               free(vals[1]);
               vals[1] = NULL;
            }

            vals[0] = NULL;
            fname = NULL;
            submitted++;

            //** See if we update the count
            if ((count > 0) && ((submitted/count) != last)) {
                last = submitted/count;
                fprintf(stderr, "Submitted " XOT " objects\n", submitted);
            }

            if (slot == n_put) {
                nleft = slot;
                do {
                    n = tbx_que_bulk_put(que, nleft, inode_list + slot - nleft, TBX_QUE_BLOCK);
                    if (n > 0) nleft = nleft - n;
                } while (nleft > 0);
                slot = 0;
            }
        }

        lio_destroy_object_iter(lio_gc, it);
        if (ftype < 0) {
            fprintf(stderr, "ERROR getting the next object!\n");
            return_code = EIO;
        }

        lio_path_release(&tuple);
        if (rp_single != NULL) {
            lio_os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            lio_os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
    }

    //** dump any remaining files to be processed
    if (slot > 0) {
        nleft = slot;
        do {
            n = tbx_que_bulk_put(que, nleft, inode_list + slot - nleft, TBX_QUE_BLOCK);
            if (n > 0) nleft = nleft - n;
        } while (nleft > 0);
    }

    tbx_que_set_finished(que);  //** Let the warming threads know we are done

    //** and wait for them to complete
    apr_status_t val;
    good = bad = 0;
    for (i=0; i<n_warm; i++) {
        apr_thread_join(&val, w[i].thread);
        good += w[i].good;
        bad += w[i].bad;
    }

    if (do_setattr) {
        tbx_que_set_finished(que_setattr);  //** Let the setattr thread know we are done
        apr_thread_join(&val, sa_thread);
        tbx_que_destroy(que_setattr);
    }

    //** Cleanup the que's
    tbx_que_destroy(que);

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Submitted: " XOT "   Success: " XOT "   Fail: " XOT "    Write Errors: " XOT "   Missing Exnodes: " XOT "\n", submitted, good, bad, werr, missing_err);
    if (submitted != (good+bad)) {
        fprintf(stderr, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
        return_code = EFAULT;
    }
    if (bad > 0) {
        fprintf(stderr, "ERROR Some files failed to warm!\n");
        return_code = EIO;
    }


    if (submitted == 0) goto cleanup;

    //** Merge the data from all the tables
    master = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, tbx_list_no_data_free);
    for (i=0; i<n_warm; i++) {
        hi = apr_hash_first(NULL, w[i].hash);
        while (hi != NULL) {
            apr_hash_this(hi, (const void **)&rkey, &klen, (void **)&wrid);
            mrid = tbx_list_search(master, wrid->rid_key);
            if (mrid == NULL) {
                tbx_list_insert(master, wrid->rid_key, wrid);
            } else {
                mrid->good += wrid->good;
                mrid->bad += wrid->bad;
                mrid->nbytes += wrid->nbytes;
                mrid->dtime += wrid->dtime;

                apr_hash_set(w[i].hash, wrid->rid_key, APR_HASH_KEY_STRING, NULL);
                free(wrid->rid_key);
                free(wrid);
            }

            hi = apr_hash_next(hi);
        }
    }

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

    //** Print the summary
    info_printf(lio_ifd, 0, "\n");
    info_printf(lio_ifd, 0, "                                                              Allocations\n");
    info_printf(lio_ifd, 0, "                 RID Key                    Size    Avg Time(us)   Total       Good         Bad\n");
    info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");
    nbytes = good = bad = j = i = 0;
    stack = tbx_stack_new();
    dtime_total = 0;
    lit = tbx_list_iter_search(master, NULL, 0);
    while (tbx_list_next(&lit, (tbx_list_key_t **)&rkey, (tbx_list_data_t **)&mrid) == 0) {
        j++;
        nbytes += mrid->nbytes;
        good += mrid->good;
        bad += mrid->bad;
        total = mrid->good + mrid->bad;
        if (mrid->bad > 0) i++;

        tbx_stack_push(stack, mrid);

        if ((summary_mode == 0) || ((summary_mode == 1) && (mrid->bad == 0))) continue;
        dtime_total += mrid->dtime;
        dtime = mrid->dtime / (double)total;
        line_end = (mrid->bad == 0) ? "\n" : "  RID_ERR\n";
        rkey = tbx_inip_get_string(ifd, mrid->rid_key, "ds_key", mrid->rid_key);
        info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "%s", rkey,
                    tbx_stk_pretty_print_double_with_scale_full(1024, (double)mrid->nbytes, ppbuf, 1),  tbx_stk_pretty_print_double_with_scale_full(1024, dtime, ppbuf2, 1),
                    total, mrid->good, mrid->bad, line_end);
        free(rkey);
    }
    if (summary_mode != 0) info_printf(lio_ifd, 0, "----------------------------------------  ---------  ---------   ----------  ----------  ----------\n");

    snprintf(ppbuf2, sizeof(ppbuf2), "SUM (%d RIDs, %d bad)", j, i);
    total = good + bad;
    dtime_total = dtime_total / (double)total;
    info_printf(lio_ifd, 0, "%-40s  %s  %s   %10" PXOT "  %10" PXOT "  %10" PXOT "\n", ppbuf2,
                tbx_stk_pretty_print_double_with_scale_full(1024, (double)nbytes, ppbuf, 1), tbx_stk_pretty_print_double_with_scale_full(1024, dtime_total, ppbuf3, 1), total, good, bad);

    tbx_list_destroy(master);

    tbx_inip_destroy(ifd);
    free(config);

    while ((mrid = tbx_stack_pop(stack)) != NULL) {
        free(mrid->rid_key);
        free(mrid);
    }
    tbx_stack_free(stack, 0);
cleanup:
    for (j=0; j<n_warm; j++) {
        apr_pool_destroy(w[j].mpool);
    }

    free(w);

finished:
    if (tagged_rids != NULL) {
        tbx_stack_free(tagged_keys, 1);
        apr_pool_destroy(tagged_pool);
    }

    close_results_db(results);  //** Close the DBs

    free(inode_list);
    tbx_stdinarray_iter_destroy(piter);
    lio_shutdown();

    return(return_code);
}
