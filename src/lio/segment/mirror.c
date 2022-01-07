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
// Routines for managing a mirroring segment
//***********************************************************************

#define _log_module_index 162

#include <gop/gop.h>
#include <gop/portal.h>
#include <gop/tp.h>
#include <libgen.h>
#include <lio/segment.h>
#include <rocksdb/c.h>
#include <sys/file.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <lio/lio.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/object.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "ex3.h"
#include "ex3/header.h"
#include "ex3/system.h"
#include "segment/mirror.h"
#include "service_manager.h"

// Forward declaration
const lio_segment_vtable_t lio_mirror_vtable;

#define WRITE_COUNT_MAX ((ex_off_t)1<<60)
#define E_SOFT 0
#define E_HARD 1
#define E_WRITE 2

typedef struct {
    char *group;                 //** Ini file group
    gop_thread_pool_context_t *tpc;
    int errors[3];
    ex_off_t errors_wc[3];
    int last_mirror_used;
    ex_off_t used_size;
    ex_off_t block_size;
    ex_off_t write_count;
    int n_mirrors;               //** Number of mirrors, child segments
    int n_good;                  //** Number of good mirrors
    lio_segment_t **child_seg;   //** This is an array of child segment mirrors with the first n_good up-to-date mirrors.  The remaining are out of sync
    ex_off_t *child_status;
} seg_mirror_priv_t;

typedef struct {
    lio_segment_t *seg;
    tbx_tbuf_t *buffer;
    ex_tbx_iovec_t *iov;
    lio_segment_rw_hints_t *rw_hints;
    data_attr_t *da;
    ex_off_t  boff;
    ex_off_t len;
    int n_iov;
    int timeout;
} seg_mirror_rw_op_t;

typedef struct {
    lio_segment_t *seg;
    int new_size;
    data_attr_t *da;
    int timeout;
    int mode;
    ex_off_t bufsize;
    lio_inspect_args_t *args;
    tbx_log_fd_t *ifd;
} seg_mirror_multi_op_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t **dseg;
    int mode;
    data_attr_t *da;
    void *attr;
    int timeout;
} seg_mirror_clone_t;

typedef struct {
    lio_segment_t *seg;
    data_attr_t *da;
    ex_id_t sid;
    const char *stype;
    const char *match_section;
    const char *args_section;
    tbx_inip_file_t *fd;
    int dryrun;
    int timeout;
} seg_mirror_tool_t;

typedef struct {
    lio_segment_t *child_seg;
    data_attr_t *da;
    int timeout;
} seg_mirror_cleanup_t;

//***********************************************************************
// smi_summary_start - Prints the Mirror Segment summary
//***********************************************************************

gop_op_status_t smi_summary_start(seg_mirror_multi_op_t *op)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    int i;
    char *status_text;

    info_printf(op->ifd, 1, XIDT ": mirror: n_mirrors=%d n_good=%d\n", segment_id(op->seg), s->n_mirrors, s->n_good);
    info_printf(op->ifd, 1, XIDT ": mirror: size=" XOT " block_size=" XOT " last_write_count=" XOT "\n", segment_id(op->seg), s->used_size, s->block_size, s->write_count);

    for (i=0; i<s->n_mirrors; i++) {
        status_text = (s->child_status[i] == s->write_count) ? "GOOD" : "BAD";
        info_printf(op->ifd, 1, XIDT ":MAP: mirror %d with write_count=" XOT " (%s) maps to child " XIDT "\n", segment_id(op->seg), i, s->child_status[i], status_text, segment_id(s->child_seg[i]));
    }

    return((s->n_mirrors == s->n_good) ? gop_success_status : gop_failure_status);
}

//***********************************************************************
// smi_error_count - Returns the hard/soft errors from the underlying mirrors
//***********************************************************************

gop_op_status_t smi_error_count(seg_mirror_multi_op_t *op)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    gop_op_status_t status, ssum;
    int i, err;
    lio_ex3_inspect_command_t option;

    option = op->mode & INSPECT_COMMAND_BITS;

    segment_lock(op->seg);

    err = -1234;
    switch (option) {
    case (INSPECT_SOFT_ERRORS):
        if (s->errors_wc[E_SOFT] == s->write_count) err = s->errors[E_SOFT];
        break;
    case (INSPECT_HARD_ERRORS):
        if (s->errors_wc[E_HARD] == s->write_count) err = s->errors[E_HARD];
        break;
    case (INSPECT_WRITE_ERRORS):
        if (s->errors_wc[E_WRITE] == s->write_count) err = s->errors[E_WRITE];
        if ((err == 0) && (s->n_good != s->n_mirrors)) err= 1;
        break;
    default:
        break;
    }

    if (err != -1234) {
        if (err == 0) {
            status = gop_success_status;
        } else {
            status.op_status = OP_STATE_FAILURE; status.error_code = 1;
        }
        segment_unlock(op->seg);
        return(status);
    }

    //** Iterate over the good mirrors
    ssum = gop_success_status;
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_status[i] == s->write_count) {
            status = gop_sync_exec_status(segment_inspect(s->child_seg[i], op->da, op->ifd, op->mode, op->bufsize, op->args, op->timeout));
            ssum.error_code += status.error_code;
        }
    }

    switch (option) {
    case (INSPECT_SOFT_ERRORS):
        s->errors[E_SOFT] = ssum.error_code;
        s->errors_wc[E_SOFT] = s->write_count;
        break;
    case (INSPECT_HARD_ERRORS):
        s->errors[E_HARD] = ssum.error_code;
        s->errors_wc[E_HARD] = s->write_count;
        break;
    case (INSPECT_WRITE_ERRORS):
        s->errors[E_WRITE] = ssum.error_code;
        s->errors_wc[E_WRITE] = s->write_count;
        break;
    default:
        break;
    }

    segment_unlock(op->seg);

    if (ssum.error_code != 0) ssum.op_status = OP_STATE_FAILURE;
    return(ssum);
}

//***********************************************************************
// smi_passthru - Does the Inspect passthru on the mirrors
//***********************************************************************

gop_op_status_t smi_passthru(seg_mirror_multi_op_t *op)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    int i, err;
    gop_opque_t *q;

    q = gop_opque_new();
    opque_start_execution(q);

    segment_lock(op->seg);
    //** Iterate over the good mirrors
    err = 0;
    for (i=0; i<s->n_mirrors; i++) {
        gop_opque_add(q, segment_inspect(s->child_seg[i], op->da, op->ifd, op->mode, op->bufsize, op->args, op->timeout));
    }
    segment_unlock(op->seg);

    err = opque_waitall(q);
    gop_opque_free(q, OP_DESTROY);

    return((err == OP_STATE_SUCCESS) ? gop_success_status : gop_failure_status);
}

//***********************************************************************
// seg_mirror_cleanup_fn - Does the child segment cleanup
//***********************************************************************

gop_op_status_t seg_mirror_cleanup_fn(void *arg, int id)
{
    seg_mirror_cleanup_t *cop = (seg_mirror_cleanup_t *)arg;

    //** Truncate it to 0 bytes
    gop_sync_exec(lio_segment_truncate(cop->child_seg, cop->da, 0, cop->timeout));

    //**And tear it down
    tbx_obj_put(&(cop->child_seg->obj));

    return(gop_success_status);
}

//***********************************************************************
//  mirror_child_cleanup_gop - Removes the data and destroys the child segment
//***********************************************************************

gop_op_generic_t *mirror_child_cleanup_gop(seg_mirror_multi_op_t *op, lio_segment_t *child_seg)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    seg_mirror_cleanup_t *cop;

    tbx_type_malloc_clear(cop, seg_mirror_cleanup_t, 1);

    cop->child_seg = child_seg;
    cop->da = op->da;
    cop->timeout = op->timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_cleanup_fn, (void *)cop, free, 1));
}

//***********************************************************************
// mirror_success_cleanup - Puts the child on the success cleanup que
//***********************************************************************

void mirror_success_cleanup(seg_mirror_multi_op_t *op, lio_segment_t *child_seg)
{
    if (op->args->qs) {
        gop_opque_add(op->args->qs, mirror_child_cleanup_gop(op, child_seg));
    }
}

//***********************************************************************
// map_mirror_sigs - Maps the mirrors to similar segments
//***********************************************************************

void map_mirror_sigs(lio_segment_t *seg, int *map)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    int i, j, k, m;
    int lut[s->n_mirrors];
    int mybufsize = 10*1024;
    char mbuf[s->n_mirrors][mybufsize];

    //** First organize the good mirrors to the front with bad in the back and also get the sigs
    j = 0; k = s->n_mirrors-1;
    for (i=0; i<s->n_mirrors; i++) {
        m = 0;
        segment_signature(s->child_seg[i], mbuf[i], &m, mybufsize);
        map[i] = s->n_mirrors;
        if (s->child_status[i] == s->write_count) {
            lut[j] = i;
            j++;
        } else {
            lut[k] = i;
            k--;
        }
    }

    //** Leaving it just in case it's needed to track a bug
    //for (i=0; i<s->n_mirrors; i++) {
    //    log_printf(0, "lut[%d]=%d\n", i, lut[i]); tbx_log_flush();
    //}

    //** Then do the mapping
    for (i=0; i<s->n_mirrors; i++) {
        m = lut[i];
        if (map[m] != s->n_mirrors) continue;
        map[m] = m;
        for (j=i+1; j<s->n_mirrors; j++) {
            if (map[lut[j]] == s->n_mirrors) {
                if (strcmp(mbuf[m], mbuf[lut[j]]) == 0) {
                    map[lut[j]] = m;
                }
            }
        }
    }


    //** Leaving it just in case it's needed to track a bug
    //for (i=0; i<s->n_mirrors; i++) {
    //    log_printf(0, "map[%d]=%d\n", i, map[i]); tbx_log_flush();
    //    log_printf(0, "map[%d]=%d sid=" XIDT "\n", i, map[i], segment_id(s->child_seg[map[i]])); tbx_log_flush();
    //}

}

//***********************************************************************
// smi_repair_bad - Repairs the bad mirrors
//***********************************************************************

gop_op_status_t smi_repair_bad(seg_mirror_multi_op_t *op)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    lio_segment_t *child_seg_org[s->n_mirrors];
    ex_off_t child_status_org[s->n_mirrors];
    gop_op_status_t status;
    gop_opque_t *q, *q2;
    gop_op_generic_t *gop, *g2;
    int map[s->n_mirrors];
    char *buffer[s->n_mirrors];
    ex_off_t bufsize = 10*1024*1024;
    ex_off_t n;
    int i, todo, loop, redo, good;

    //** Preserve the original mirrors in case we need to restore them
    memcpy(child_seg_org, s->child_seg, sizeof(lio_segment_t *)*s->n_mirrors);
    memcpy(child_status_org, s->child_status, sizeof(ex_off_t)*s->n_mirrors);

    //** Figure out the bufsize
    memset(buffer, 0, sizeof(char *)*s->n_mirrors);
    i = (s->n_mirrors - s->n_good);
    if (i == 0) i = 1;
    bufsize = op->bufsize / i;
    if (bufsize == 0) bufsize = 10*1024*1024;
    n = bufsize / s->block_size;
    if (n == 0) n = 1;
    bufsize = n*s->block_size;

    //** Find a "good" mirror
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_status[i] == s->write_count) {
            good = i;
            break;
        }
    }

    q = gop_opque_new();
    q2 = NULL;
    opque_start_execution(q);

    map_mirror_sigs(op->seg, map);

    loop = 0;

    segment_lock(op->seg);
again:
    //** Generate the clone list (and hopefully do that data as well)
    todo = 0;
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_status[i] != s->write_count) {
            s->child_seg[i] = NULL;
            if (child_status_org[map[i]] == s->write_count) {
                gop = segment_clone(child_seg_org[map[i]], op->da, &(s->child_seg[i]), CLONE_STRUCT_AND_DATA, NULL, op->timeout);
            } else {
                gop = segment_clone(child_seg_org[map[i]], op->da, &(s->child_seg[i]), CLONE_STRUCTURE, NULL, op->timeout);
            }
            gop_set_id(gop, i);
            gop_opque_add(q, gop);
        } else {
            s->child_seg[i] = child_seg_org[i];
        }
    }

    //** Process the clones
    redo = 0;
    todo = 0;
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        i = gop_get_id(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            if (child_status_org[map[i]] != s->write_count) {  //** Got to manually copy it over
                if (!q2) {
                    q2 = gop_opque_new();
                    opque_start_execution(q2);
                }
                if (buffer[i] == NULL) tbx_type_malloc(buffer[i], char, bufsize);
                g2 = lio_segment_copy_gop(s->tpc, op->da, NULL, child_seg_org[good], s->child_seg[i], 0, 0, s->used_size, bufsize, buffer[i], 1, op->timeout);
                gop_set_id(g2, i);
                gop_opque_add(q2, g2);
                todo++;
            } else { //** Data was copied during the cloning
                info_printf(op->ifd, 1, XIDT ":MAP: mirror %d replaced (full clone) with segment " XIDT "\n", segment_id(child_seg_org[i]), i, segment_id(s->child_seg[i]));
                s->child_status[i] = s->write_count;  //** Flag it as good now
            }
        } else {  //** Got an error with the clone
            info_printf(op->ifd, 1, XIDT ": mirror %d replacement failed\n", segment_id(child_seg_org[i]), i);
            redo = 1;
            if (s->child_seg[i] != child_seg_org[i]) {
                tbx_obj_put(&(s->child_seg[i]->obj));
                s->child_seg[i] = NULL;
            }
        }

        gop_free(gop, OP_DESTROY);
    }

    //** Wait for any manual copies to complete
    if (todo) {
        while ((g2 = opque_waitany(q2)) != NULL) {
            status = gop_get_status(g2);
            i = gop_get_id(g2);
            if (status.op_status == OP_STATE_SUCCESS) {
                info_printf(op->ifd, 1, XIDT ": mirror %d replaced (manual data copy) with segment " XIDT "\n", segment_id(child_seg_org[i]), i, segment_id(s->child_seg[i]));
                s->child_status[i] = s->write_count;
            } else { //** Manual copy failed so try it again
                redo = 1;
                tbx_obj_put(&(s->child_seg[i]->obj));
                s->child_seg[i] = NULL;
            }
            gop_free(g2, OP_DESTROY);
        }
    }

    //** See if we need to try again
    if (redo) {
        loop++;
        if (loop < 5) {
            goto again;
        } else { //** Failed so see what we can salvage;
            for (i=0; i<s->n_mirrors; i++) {
                if (s->child_status[i] != s->write_count) {
                    s->child_seg[i] = child_seg_org[i];
                }
                if (s->child_seg[i] != child_seg_org[i]) {
                    tbx_obj_put(&(child_seg_org[i]->obj));
                }
            }
        }
    }

    //** Do some clean up
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_seg[i] != child_seg_org[i]) {
            mirror_success_cleanup(op, child_seg_org[i]);
        }
    }

    segment_unlock(op->seg);

    gop_opque_free(q, OP_DESTROY);
    if (q2) gop_opque_free(q2, OP_DESTROY);

    for (i=0; i<s->n_mirrors; i++) {
        if (buffer[i]) free(buffer[i]);
    }
    return((redo ? gop_failure_status : gop_success_status));
}

//***********************************************************************
// bad_byte_count - Counts the bad bytes in the buffers
//***********************************************************************

ex_off_t bad_byte_count(const char *buf1, const char *buf2, ex_off_t len, ex_off_t *rng)
{
    ex_off_t i, bad;

    rng[0] = rng[1] = -1;
    bad = 0;
    for (i=0; i<len; i++) {
        if (buf1[i] != buf2[i]) {
            if (rng[0] == -1) rng[0] = i;
            rng[1] = i;
            bad++;
        }
    }

    rng[2] = bad;

    return(bad);
}

//***********************************************************************
// smi_full_check - Does a full check of all the mirrors
//***********************************************************************

gop_op_status_t smi_full_check(seg_mirror_multi_op_t *op)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    gop_op_status_t status, mstatus;
    gop_opque_t *q, *q2;
    gop_op_generic_t *gop, *gmaster, *g2;
    char *buffer[s->n_mirrors];
    char *state;
    tbx_tbuf_t tbuf[s->n_mirrors];
    int bufsize;
    ex_off_t merrs[s->n_mirrors], rerrs[s->n_mirrors], repaired[s->n_mirrors], nerrs;
    ex_off_t bad[s->n_mirrors][3];
    apr_time_t mread_dt[s->n_mirrors], mwrite_dt[s->n_mirrors], tstart, tend;
    char pp1[128], pp2[128];
    int i, master, err, do_repair;
    ex_off_t off, off_end, len, nbytes, lo, hi, nbad;
    double dt1, dt2, rate1, rate2;
    ex_tbx_iovec_t iov;
    lio_ex3_inspect_command_t option;

    //** See if we try and fix things
    option = op->mode & INSPECT_COMMAND_BITS;
    do_repair =  ((option == INSPECT_QUICK_REPAIR) || (option == INSPECT_SCAN_REPAIR) || (option == INSPECT_FULL_REPAIR)) ? 1 : 0;

do_repair = 0;  //QWERT

    memset(merrs, 0, sizeof(ex_off_t)*s->n_mirrors);
    memset(rerrs, 0, sizeof(ex_off_t)*s->n_mirrors);
    memset(repaired, 0, sizeof(ex_off_t)*s->n_mirrors);

    //** Get the bufsize and make the buffers
    bufsize = op->bufsize / s->n_mirrors;
    if (bufsize == 0) bufsize = 10*1024*1024;
    len = bufsize / s->block_size;
    if (len == 0) len = 1;
    bufsize = len*s->block_size;
    for (i=0; i<s->n_mirrors; i++) {
        tbx_type_malloc(buffer[i], char, bufsize);
        tbx_tbuf_single(&(tbuf[i]), bufsize, buffer[i]);
    }

    //** Find a "good" mirror
    master = -1;
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_status[i] == s->write_count) {
            master = i;
            break;
        }
    }
    if (master == -1) {
        abort();
    }
    //** Now loop over getting the blocks and doing the comparison
    status = gop_success_status;
    off = 0;
    err = 0;
    q = gop_opque_new();
    q2 = gop_opque_new();
    opque_start_execution(q);
    nerrs = 0;
    do {
        //** Figure out how much to get
        if ((off+bufsize) <= s->used_size) {
            len = bufsize;
        } else {
            len = s->used_size - off;
        }

        //** Clear the old values
        memset(mread_dt, 0, sizeof(apr_time_t)*s->n_mirrors);
        memset(mwrite_dt, 0, sizeof(apr_time_t)*s->n_mirrors);
        memset(bad, 0, sizeof(bad));

        //** Go ahead and generate all the tasks
        iov.offset = off; iov.len = len;
        tstart = apr_time_now();
        gmaster = segment_read(s->child_seg[master], op->da, NULL, 1, &iov, &(tbuf[master]), 0, op->timeout);
        gop_start_execution(gmaster);
        for (i=0; i<s->n_mirrors; i++) {
            if (i == master) continue;
            gop = segment_read(s->child_seg[i], op->da, NULL, 1, &iov, &(tbuf[i]), 0, op->timeout);
            gop_set_id(gop, i);
            gop_opque_add(q, gop);
        }

        err = gop_waitall(gmaster);  //** Wait for the master
        mread_dt[master] = gop_time_exec(gmaster);
        gop_free(gmaster, OP_DESTROY);
        nbytes = len;
        if (err != OP_STATE_SUCCESS) {  //** Can't proceed if the master fails
            status = gop_failure_status;
            opque_waitall(q);
            goto finished;
        }

        //** Do the comparisons
        err = 0;
        while ((gop = opque_waitany(q)) != NULL) {
            i = gop_get_id(gop);
            mread_dt[i] = gop_time_exec(gop);
            mstatus = gop_get_status(gop);
            nbytes += len;
            if ((mstatus.op_status == OP_STATE_SUCCESS) && (memcmp(buffer[master], buffer[i], len)) != 0) {
                if (do_repair) {
                    g2 = segment_write(s->child_seg[i], op->da, NULL, 1, &iov, &(tbuf[master]), 0, op->timeout);
                    gop_set_id(g2, i);
                    gop_opque_add(q2, g2);
                }
                err++;
                merrs[i] += bad_byte_count(buffer[master], buffer[i], len, bad[i]);
            }
            gop_free(gop, OP_DESTROY);
        }

        //** Handle any repairs
        if (do_repair) {
            while ((gop = opque_waitany(q2)) != NULL) {
                i = gop_get_id(gop);
                mwrite_dt[i] = gop_time_exec(gop);
                nbytes += len;
                status = gop_get_status(gop);
                if (status.op_status != OP_STATE_SUCCESS) {
                    err++;
                    rerrs[i] += len;
                } else {
                    repaired[i] += len;
                }
                gop_free(gop, OP_DESTROY);
            }
        }
        tend = apr_time_now();

        //** Update the stats and print them
        if (err) {
            lo = len+1; hi = -1;
            for (i=0; i<s->n_mirrors; i++) {
                if (bad[i][2] > 0) {
                    if (bad[i][0] < lo) lo = bad[i][0];
                    if (bad[i][1] > hi) hi = bad[i][1];
                }
            }
            nbad = hi-lo+1;
            nerrs += nbad;
        } else {
            lo = hi = -off-1;
            nbad = 0;
        }

        off_end = off + len - 1;
        dt1 = (double)(tend - tstart) / APR_USEC_PER_SEC;
        rate1 = (dt1 > 0) ? (double)nbytes / dt1 : 0;
        info_printf(op->ifd, 1, XIDT ": Master: %d  Block: (" XOT ", " XOT ") T[%lf sec  %s/s] bad byte count: " XOT " bad region: " XOT " (" XOT ", " XOT ")\n",
            segment_id(op->seg), master, off, off_end, dt1, tbx_stk_pretty_print_double_with_scale(1024, rate1, pp1), nerrs, nbad, lo+off, hi+off);
        for (i=0; i<s->n_mirrors; i++) {
            dt1 = (double)mread_dt[i] / APR_USEC_PER_SEC;
            rate1 = (dt1 > 0) ? (double)len / dt1 : 0;
            dt2 = (double)mwrite_dt[i] / APR_USEC_PER_SEC;
            rate2 = (dt2 > 0) ? (double)len / dt2 : 0;
            if (bad[i][2] == 0) {
                bad[i][0] = bad[i][1] = -1;
            } else {
                bad[i][0] += off;
                bad[i][1] += off;
            }
            info_printf(op->ifd, 1, XIDT ": Mirror: %d  Block: (" XOT ", " XOT ") R[%lf sec %s/s] W[%lf sec %s/s]  bad byte count: " XOT " bad region: " XOT " (" XOT ", " XOT ") -- Repair errors: " XOT "\n",
                segment_id(s->child_seg[i]), i, off, off_end, dt1, tbx_stk_pretty_print_double_with_scale(1024, rate1, pp1), dt2, tbx_stk_pretty_print_double_with_scale(1024, rate2, pp2),
                merrs[i], bad[i][2], bad[i][0], bad[i][1], rerrs[i]);
        }

        //** Set up for the next round
        off += bufsize;
    } while (off < s->used_size);

    status = gop_success_status;
    for (i=0; i<s->n_mirrors; i++) {
        if (buffer[i]) free(buffer[i]);
        if (((repaired[i] > 0) && (rerrs[i] > 0)) || (merrs[i] > 0)) {
            state = "BAD ";
            status = gop_failure_status;
        } else {
            state = "GOOD";
        }
        info_printf(op->ifd, 1, XIDT ": SUMMARY Mirror: %d status: %s\n", segment_id(s->child_seg[i]), i, state);
    }

finished:
    gop_opque_free(q, OP_DESTROY);
    gop_opque_free(q2, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seg_mirror_inspect_fn - Does the actual inspect operation
//***********************************************************************

gop_op_status_t seg_mirror_inspect_fn(void *arg, int id)
{
    seg_mirror_multi_op_t *op = (seg_mirror_multi_op_t *)arg;
    gop_op_status_t status, status2;
    lio_ex3_inspect_command_t option;

    option = op->mode & INSPECT_COMMAND_BITS;

    switch (option) {
    case (INSPECT_WRITE_ERRORS):
    case (INSPECT_SOFT_ERRORS):
    case (INSPECT_HARD_ERRORS):
        status = smi_error_count(op);
        return(status);     //** For the errors we just kick out
        break;
    case (INSPECT_QUICK_CHECK):
    case (INSPECT_SCAN_CHECK):
    case (INSPECT_MIGRATE):
        status = smi_summary_start(op);
        if (status.op_status == OP_STATE_SUCCESS) status = smi_passthru(op);
        break;
    case (INSPECT_FULL_CHECK):
        status = smi_summary_start(op);
        status2 = smi_passthru(op);
        if (status2.op_status != OP_STATE_SUCCESS) status = status2;
        status2 = smi_full_check(op);
        if (status2.op_status != OP_STATE_SUCCESS) status = status2;
        break;
    case (INSPECT_QUICK_REPAIR):
    case (INSPECT_SCAN_REPAIR):
        status = smi_summary_start(op);
        status2 = smi_passthru(op);
        if (status2.op_status == OP_STATE_SUCCESS) {
            status = smi_repair_bad(op);
        }
        break;
    case (INSPECT_FULL_REPAIR):
        status = smi_summary_start(op);
        status2 = smi_passthru(op);
        status2 = smi_repair_bad(op);
        if (status2.op_status == OP_STATE_SUCCESS) {
            status = smi_full_check(op);
        } else {
            status = status2;
        }
        break;
    case (INSPECT_NO_CHECK):
        break;
    }

    info_printf(op->ifd, 1, XIDT ": status: %s\n", segment_id(op->seg), ((status.op_status == OP_STATE_SUCCESS) ? "SUCCESS" : "FAILURE"));
    return(status);
}

//***********************************************************************
// seg_mirror_inspect - Inspect function. Simple pass-through
//***********************************************************************

gop_op_generic_t *seg_mirror_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *ifd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_multi_op_t *op;

    tbx_type_malloc_clear(op, seg_mirror_multi_op_t, 1);

    op->seg = seg;
    op->da = da;
    op->ifd = ifd;
    op->mode = mode;
    op->bufsize = bufsize;
    op->args = args;
    op->timeout = timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_inspect_fn, (void *)op, free, 1));
}


//***********************************************************************
// seg_mirror_read_fn - Reads from a Mirror segment
//***********************************************************************

gop_op_status_t seg_mirror_read_fn(void *arg, int id)
{
    seg_mirror_rw_op_t *op = (seg_mirror_rw_op_t *)arg;
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    gop_op_status_t status;
    int i, loop, slot;

    for (loop=0; loop<s->n_mirrors; loop++) {
        segment_lock(op->seg);

        //** Make sure a good mirror exists
        if (s->n_good == 0) {
            segment_unlock(op->seg);
            return(gop_failure_status);
        }

        //** Find a valid good mirror
        slot = -1;
        for (i=1; i<=s->n_mirrors; i++) {
            slot = (i+s->last_mirror_used) % s->n_mirrors;
            if (s->child_status[slot] == s->write_count) break;
        }
        s->last_mirror_used = slot;

        segment_unlock(op->seg);

        //** Do the read
        status = gop_sync_exec_status(segment_read(s->child_seg[slot], op->da, op->rw_hints, op->n_iov, op->iov, op->buffer, op->boff, op->timeout));
        if (status.op_status == OP_STATE_SUCCESS) break;
    }

    return(status);
}

//***********************************************************************
// seg_mirror_read - Read from a file segment
//***********************************************************************

gop_op_generic_t *seg_mirror_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_rw_op_t *srw;

    tbx_type_malloc_clear(srw, seg_mirror_rw_op_t, 1);

    srw->seg = seg;
    srw->da = da;
    srw->rw_hints = rw_hints;
    srw->n_iov = n_iov;
    srw->iov = iov;
    srw->boff = boff;
    srw->timeout = timeout;
    srw->buffer = buffer;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_read_fn, (void *)srw, free, 1));
}

//***********************************************************************
// seg_mirror_write_func - Write from a Mirror segment
//***********************************************************************

gop_op_status_t seg_mirror_write_fn(void *arg, int id)
{
    seg_mirror_rw_op_t *op = (seg_mirror_rw_op_t *)arg;
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    int i;
    ex_off_t size, old_size, good_wc;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    segment_lock(op->seg);
    old_size = s->used_size;

    //** Make sure a good mirror exists
    if (s->n_good == 0) {
        segment_unlock(op->seg);
        return(gop_failure_status);
    }

    q = gop_opque_new();

    //** Generate the list
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_status[i] == s->write_count) {
            gop = segment_write(s->child_seg[i], op->da, op->rw_hints, op->n_iov, op->iov, op->buffer, op->boff, op->timeout);
            gop_set_id(gop, i);
            gop_opque_add(q, gop);
        }
    }
    segment_unlock(op->seg);

    opque_waitall(q);  //** Wait for everything to complete

    segment_lock(op->seg);
    //** Now process the individual writes
    size = -1;
    good_wc = s->write_count;
    s->write_count++;
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        i = gop_get_id(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            if (s->child_status[i] == good_wc) {
                if (size == -1) size = segment_size(s->child_seg[gop_get_id(gop)]);
                s->child_status[gop_get_id(gop)] = s->write_count;
            }
        } else {
            s->n_good--;
            s->errors[E_WRITE]++;
            s->errors_wc[E_WRITE] = s->write_count;
        }
        gop_free(gop, OP_DESTROY);
    }
    segment_unlock(op->seg);

    //** See if we grew the segment
    if (size > old_size) {
        segment_lock(op->seg);
        if (s->used_size < size) s->used_size = size;
        segment_unlock(op->seg);
    }

    //** Cleanup
    gop_opque_free(q, OP_DESTROY);

    return(gop_failure_status);
}

//***********************************************************************
// seg_mirror_write - Writes to a linear segment
//***********************************************************************

gop_op_generic_t *seg_mirror_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_rw_op_t *srw;

    tbx_type_malloc_clear(srw, seg_mirror_rw_op_t, 1);

    srw->seg = seg;
    srw->da = da;
    srw->rw_hints = rw_hints;
    srw->n_iov = n_iov;
    srw->iov = iov;
    srw->boff = boff;
    srw->timeout = timeout;
    srw->buffer = buffer;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_write_fn, (void *)srw, free, 1));
}

//***********************************************************************
//  seg_mirror_remove_fn - Handles the segment remove operations
//***********************************************************************

gop_op_status_t seg_mirror_remove_fn(void *arg, int id)
{
    seg_mirror_multi_op_t *op = (seg_mirror_multi_op_t *)arg;
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    int i;
    gop_opque_t *q;

    //** Generate the tasks
    q = gop_opque_new();
    for (i=0; i<s->n_mirrors; i++) {
        gop_opque_add(q, lio_segment_truncate(s->child_seg[i], op->da, op->new_size, op->timeout));
    }

    //** Now execute the individual truncates
    opque_waitall(q);
    gop_opque_free(q, OP_DESTROY);

    return(gop_success_status);
}

//***********************************************************************
// seg_mirror_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

gop_op_generic_t *seg_mirror_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_multi_op_t *op;

    tbx_type_malloc_clear(op, seg_mirror_multi_op_t, 1);

    op->seg = seg;
    op->da = da;
    op->timeout = timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_remove_fn, (void *)op, free, 1));
}

//***********************************************************************
//  seg_mirror_truncate_fn - Handles the segment truncate operation
//***********************************************************************

gop_op_status_t seg_mirror_truncate_fn(void *arg, int id)
{
    seg_mirror_multi_op_t *op = (seg_mirror_multi_op_t *)arg;
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    int i;
    ex_off_t max_size, n;

    segment_lock(op->seg);

    n = op->new_size / s->block_size;
    if (op->new_size % s->block_size) n++;
    max_size = n*s->block_size;

    //** Generate the tasks
    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<s->n_mirrors; i++) {
        if ((s->child_status[i] == s->write_count) || (max_size == 0)) {
            gop = lio_segment_truncate(s->child_seg[i], op->da, max_size, op->timeout);
            gop_set_id(gop, i);
            gop_opque_add(q, gop);
        }
    }

    opque_waitall(q);  //**Wait for everything to complete

    //** See if we can clear some flags
    //** Truncate to 0 so assume all mirrors are good
    if (max_size == 0) {
        s->n_good = s->n_mirrors;
        for (i=0; i<3; i++) {
            s->errors[i] = 0;
            s->errors_wc[i] = s->write_count;
        }
    }

    s->write_count++;
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        i = gop_get_id(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            s->child_status[i] = s->write_count;
        } else {   //** Error with truncate
            s->n_good--;
            s->errors[E_WRITE]++;
            s->errors_wc[E_WRITE] = s->write_count;
        }
        gop_free(gop, OP_DESTROY);
    }

    if (s->n_good > 0) {
        if (op->new_size >= 0) {
            s->used_size = max_size;
        }
        status = gop_success_status;
    } else {
        status = gop_failure_status;
    }

    segment_unlock(op->seg);

    gop_opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seg_mirror_truncate - Expands or contracts a segment
//***********************************************************************

gop_op_generic_t *seg_mirror_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_multi_op_t *op;

    if (new_size < 0) return(gop_dummy(gop_success_status));  //** Reserve call which we ignore

    tbx_type_malloc_clear(op, seg_mirror_multi_op_t, 1);

    op->seg = seg;
    op->new_size = new_size;
    op->da = da;
    op->timeout = timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_truncate_fn, (void *)op, free, 1));
}

//***********************************************************************
// seg_mirror_flush - Flushes a segment. Simple pass-through
//***********************************************************************

gop_op_generic_t *seg_mirror_flush(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    gop_opque_t *q;
    int i;

    q = gop_opque_new();
    for (i=0; i<s->n_mirrors; i++) {
        gop_opque_add(q, segment_flush(s->child_seg[i], da, lo, hi, timeout));
    }

    return(opque_get_gop(q));
}

//***********************************************************************

int qsort_strcmp( const void *av, const void *bv)
{
    const char **a = (const char **)av;
    const char **b = (const char **)bv;

    return(strcmp(*a, *b));
}

//***********************************************************************
// seg_mirror_signature - Generates the segment signature. Just a pass-through
//***********************************************************************

int seg_mirror_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    const int mybufsize = 10*1024;
    char mbuf[s->n_mirrors][mybufsize];
    char *mbuf_sorted[s->n_mirrors];
    int mbused[s->n_mirrors];
    int i;

    //** In order for the signatures to match we need to put the mirrors in sorted order always
    for (i=0; i<s->n_mirrors; i++) {
        mbused[i] = 0;
        mbuf_sorted[i] = mbuf[i];
        segment_signature(s->child_seg[i], mbuf[i], &(mbused[i]), mybufsize);
    }

    //** Now sort them
    qsort(mbuf_sorted, s->n_mirrors, sizeof(char *), qsort_strcmp);

    //** And finally merge them all together
    tbx_append_printf(buffer, used, bufsize, "mirror(%d)\n", s->n_mirrors);
    for (i=0; i<s->n_mirrors; i++) {
        tbx_append_printf(buffer, used, bufsize, "%s", mbuf_sorted[i]);
    }
    return(0);
}

//***********************************************************************
// seg_mirror_clone_fn - Clones a segment
//***********************************************************************

gop_op_status_t seg_mirror_clone_fn(void *arg, int id)
{
    seg_mirror_clone_t *op = (seg_mirror_clone_t *)arg;
    seg_mirror_priv_t *ss = (seg_mirror_priv_t *)op->sseg->priv;
    seg_mirror_priv_t *sd;
    lio_segment_t *clone;
    lio_segment_t **child_mirrors;
    ex_off_t *child_status;
    gop_opque_t *q;
    gop_op_status_t status;
    int i;
    ex_off_t old_write_count;
    int use_existing = (*op->dseg != NULL) ? 1 : 0;

    //** Make the base segment
    if (use_existing == 0) {
        *op->dseg = segment_mirror_create(op->sseg->ess);
        if (op->sseg->header.name != NULL) (*op->dseg)->header.name = strdup(op->sseg->header.name);
    }

    clone = *op->dseg;
    sd = (seg_mirror_priv_t *)clone->priv;

    //** Do a base copy
    old_write_count = sd->write_count;
    child_mirrors = sd->child_seg;  //** We need to store this for cleanup later
    child_status = sd->child_status;
    *sd = *ss;

    tbx_type_malloc_clear(sd->child_seg, lio_segment_t *, ss->n_mirrors);
    tbx_type_malloc_clear(sd->child_status, ex_off_t, ss->n_mirrors);
    memcpy(sd->child_status, ss->child_status, sizeof(ex_off_t)*ss->n_mirrors);

    q = gop_opque_new();
    opque_start_execution(q);
    for (i=0; i<ss->n_mirrors; i++) {
        gop_opque_add(q, segment_clone(ss->child_seg[i], op->da, &(sd->child_seg[i]), op->mode, op->attr, op->timeout));
    }

    status = gop_success_status;
    i = opque_waitall(q);
    if (i != OP_STATE_SUCCESS) {
        for (i=0; i<sd->n_mirrors; i++) {
            if (sd->child_seg[i]) tbx_obj_put(&(sd->child_seg[i]->obj));
        }
        free(sd->child_seg);
        sd->child_seg = child_mirrors;
        free(sd->child_status);
        sd->child_status = child_status;
        status = gop_failure_status;
        child_mirrors = NULL;
        child_status = NULL;
        sd->used_size = 0;
        sd->write_count = old_write_count;
    } else if (op->mode == CLONE_STRUCTURE) {
        sd->used_size = 0;
        memset(sd->errors, 0, sizeof(sd->errors));
        memset(sd->errors_wc, 0, sizeof(sd->errors_wc));
        sd->n_good = sd->n_mirrors;
    }

    if ((child_mirrors) && (op->mode == CLONE_STRUCT_AND_DATA)) { //** Need to clean up the old children
        for (i=0; i<sd->n_mirrors; i++) {
            if (child_mirrors[i]) gop_opque_add(q, lio_segment_truncate(child_mirrors[i], op->da, 0, op->timeout));
        }
        opque_waitall(q);
        for (i=0; i<sd->n_mirrors; i++) {
            if (child_mirrors[i]) tbx_obj_put(&(child_mirrors[i]->obj));
        }
        free(child_mirrors);
        free(child_status);
    }

    gop_opque_free(q, OP_DESTROY);
    return(status);
}


//***********************************************************************
// seg_mirror_clone - Generates the clone segment operation
//***********************************************************************

gop_op_generic_t *seg_mirror_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *attr, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_clone_t *op;

    tbx_type_malloc_clear(op, seg_mirror_clone_t, 1);

    op->sseg = seg;
    op->da = da;
    op->dseg = clone_seg;
    op->mode = mode;
    op->attr = attr;
    op->timeout = timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_clone_fn, (void *)op, free, 1));
}

//***********************************************************************
// seg_mirror_size - Returns the segment size.
//***********************************************************************

ex_off_t seg_mirror_size(lio_segment_t *seg)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    ex_off_t size;

    segment_lock(seg);
    size = s->used_size;
    segment_unlock(seg);

    return(size);
}

//***********************************************************************
// seg_mirror_block_size - Returns the segment block size. Simple pass-through
//***********************************************************************

ex_off_t seg_mirror_block_size(lio_segment_t *seg, int btype)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;

    return(s->block_size);
}

//***********************************************************************
// seg_mirror_tool_fn - Performs the tool fn on the mirrors
//***********************************************************************

gop_op_status_t seg_mirror_tool_fn(void *arg, int id)
{
    seg_mirror_tool_t *op = (seg_mirror_tool_t *)arg;
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)op->seg->priv;
    int i, err;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status, ms;

    q = gop_opque_new();
    for (i=0; i<s->n_mirrors; i++) {
        gop = segment_tool(s->child_seg[i], op->da, op->sid, op->stype, op->match_section, op->args_section, op->fd, op->dryrun, op->timeout);
        gop_opque_add(q, gop);
    }

    err = opque_waitall(q);
    if (err == OP_STATE_SUCCESS) {
        status = gop_success_status;
        while ((gop = opque_waitany(q)) != NULL) {
            ms = gop_get_status(gop);
            status.error_code += ms.error_code;
            gop_free(gop, OP_DESTROY);
        }
    } else {
        status = gop_failure_status;
    }

    gop_opque_free(q, OP_DESTROY);

    return(status);
}

//***********************************************************************
// seg_mirror_tool - Returns the tool GOP
//***********************************************************************

gop_op_generic_t *seg_mirror_tool(lio_segment_t *seg, data_attr_t *da, ex_id_t sid, const char *stype, const char *match_section, const char *args_section, tbx_inip_file_t *fd, int dryrun, int timeout)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    seg_mirror_tool_t *op;

    tbx_type_malloc_clear(op, seg_mirror_tool_t, 1);

    op->seg = seg;
    op->da = da;
    op->sid = sid;
    op->stype = stype;
    op->match_section = match_section;
    op->args_section = args_section;
    op->fd = fd;
    op->dryrun = dryrun;
    op->timeout = timeout;

    return(gop_tp_op_new(s->tpc, NULL, seg_mirror_tool_fn, (void *)op, free, 1));
}

//***********************************************************************
// seg_mirror_serialize - Convert the segment to a more portable format. Simple pass-through
//***********************************************************************

int seg_mirror_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    int i;
    int bufsize=10*1024;
    char segbuf[bufsize];
    char *etext;
    int sused;

    segbuf[0] = 0;
    sused = 0;

    //** We can only handle the text/INI format
    if (exp->type != EX_TEXT) {
        return(-1);
    }

    //** Serialize the children first and add then
    for (i=0; i<s->n_mirrors; i++) {
        if (s->child_seg[i]) segment_serialize(s->child_seg[i], exp);
    }

    //** And now add ourself
    //** Store the segment header
    tbx_append_printf(segbuf, &sused, bufsize, "[segment-" XIDT "]\n", seg->header.id);
    if ((seg->header.name != NULL) && (strcmp(seg->header.name, "") != 0)) {
        etext = tbx_stk_escape_text("=", '\\', seg->header.name);
        tbx_append_printf(segbuf, &sused, bufsize, "name=%s\n", etext);
        free(etext);
    }
    tbx_append_printf(segbuf, &sused, bufsize, "type=%s\n", SEGMENT_TYPE_MIRROR);

    //** And the params
    tbx_append_printf(segbuf, &sused, bufsize, "used_size=" XOT "\n", s->used_size);
    tbx_append_printf(segbuf, &sused, bufsize, "block_size=" XOT "\n", s->block_size);
    tbx_append_printf(segbuf, &sused, bufsize, "n_mirrors=%d\n", s->n_mirrors);
    tbx_append_printf(segbuf, &sused, bufsize, "write_count=" XOT "\n", s->write_count);
    tbx_append_printf(segbuf, &sused, bufsize, "soft_errors=" XOT ":%d\n", s->errors_wc[E_SOFT], s->errors[E_SOFT]);
    tbx_append_printf(segbuf, &sused, bufsize, "hard_errors=" XOT ":%d\n", s->errors_wc[E_HARD], s->errors[E_HARD]);
    tbx_append_printf(segbuf, &sused, bufsize, "write_errors=" XOT ":%d\n", s->errors_wc[E_WRITE], s->errors[E_WRITE]);

    for (i=0; i<s->n_mirrors; i++) {
        tbx_append_printf(segbuf, &sused, bufsize, "mirror=" XOT ":" XIDT "\n", s->child_status[i], segment_id(s->child_seg[i]));
    }

    //** Merge the exnodes together
    exnode_exchange_append_text(exp, segbuf);
    return(0);
}

//***********************************************************************
// seg_mirror_deserialize - This should never be called so flag it if it does
//***********************************************************************

int seg_mirror_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    lio_segment_t *cseg;
    int bufsize=1024;
    char seggrp[bufsize];
    ex_id_t cid;
    ex_off_t bs;
    int i, fin, fail;
    tbx_inip_file_t *fd;
    tbx_inip_group_t *g;
    tbx_inip_element_t *ele;
    char *token, *bstate, *key, *value;

    //** We can only handle the text/INI format
    if (exp->type != EX_TEXT) {
        return(-1);
    }

    fail = 0;

    //** Parse the ini text
    fd = exp->text.fd;

    //** Make the segment section name
    snprintf(seggrp, bufsize, "segment-" XIDT, id);

    //** Get the segment header info
    seg->header.id = id;
    seg->header.type = SEGMENT_TYPE_MIRROR;
    seg->header.name = tbx_inip_get_string(fd, seggrp, "name", "");

    s->n_mirrors = tbx_inip_get_integer(fd, seggrp, "n_mirrors", -1);
    s->used_size = tbx_inip_get_integer(fd, seggrp, "used_size", 0);
    s->block_size = tbx_inip_get_integer(fd, seggrp, "block_size", -1);
    s->write_count = tbx_inip_get_integer(fd, seggrp, "write_count", 0);

    token = tbx_inip_get_string(fd, seggrp, "soft_errors", NULL);
    if (token) {
        sscanf(tbx_stk_escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(s->errors_wc[E_SOFT]));
        sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), "%d", &(s->errors[E_SOFT]));
        free(token);
    }
    token = tbx_inip_get_string(fd, seggrp, "hard_errors", NULL);
    if (token) {
        sscanf(tbx_stk_escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(s->errors_wc[E_HARD]));
        sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), "%d", &(s->errors[E_HARD]));
        free(token);
    }
    token = tbx_inip_get_string(fd, seggrp, "write_errors", NULL);
    if (token) {
        sscanf(tbx_stk_escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(s->errors_wc[E_WRITE]));
        sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), "%d", &(s->errors[E_WRITE]));
        free(token);
    }

    //** Now load the mirrors
    tbx_type_malloc_clear(s->child_seg, lio_segment_t *, s->n_mirrors);
    tbx_type_malloc_clear(s->child_status, ex_off_t, s->n_mirrors);
    g = tbx_inip_group_find(fd, seggrp);
    ele = tbx_inip_ele_first(g);
    i = 0;
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "mirror") == 0) {
            value = tbx_inip_ele_get_value(ele);
            token = strdup(value);
            sscanf(tbx_stk_escape_string_token(token, ":", '\\', 0, &bstate, &fin), XOT, &(s->child_status[i]));
            sscanf(tbx_stk_escape_string_token(NULL, ":", '\\', 0, &bstate, &fin), XIDT, &cid);
            free(token);
            cseg = load_segment(seg->ess, cid, exp);
            if (!cseg) fail = 1;
            s->child_seg[i] = cseg;
            if (s->child_status[i] == s->write_count) s->n_good++;
            i++;
        }
        ele = tbx_inip_ele_next(ele);
    }

    //** Sanity check we have all the mirrors and at least 1 is good.
    if ((i != s->n_mirrors) && (s->n_good <= 0)) fail = 1;

    if (fail) {
        for (i=1; i<s->n_mirrors; i++) {
            if (s->child_seg[i]) tbx_obj_put(&(s->child_seg[i]->obj));
        }
        free(s->child_seg);
        s->child_seg = NULL;
        return(-1);
    }

    if (s->block_size <= 0) { //** Need to calculate the blocksize
        bs = segment_block_size(s->child_seg[0], LIO_SEGMENT_BLOCK_NATURAL);
        for (i=1; i<s->n_mirrors; i++) {
            s->block_size = math_lcm(bs, segment_block_size(s->child_seg[i], LIO_SEGMENT_BLOCK_NATURAL));
            bs = s->block_size;
        }
    }

    //** See if we need to reset the write counter
    if (s->write_count > WRITE_COUNT_MAX) {
        //** Find the smallest entry
        bs = s->write_count;
        for (i=0; i<s->n_mirrors; i++) {
            if (s->child_status[i] < bs) bs = s->child_status[i];
        }

        //** Now make that the baseline
        for (i=0; i<s->n_mirrors; i++) {
            s->child_status[i] -= bs;
        }
        s->write_count -= bs;
    }

    return(fail);
}


//***********************************************************************
// seg_mirror_destroy - Destroys a linear segment struct (not the data)
//***********************************************************************

void seg_mirror_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);
    seg_mirror_priv_t *s = (seg_mirror_priv_t *)seg->priv;
    int i;

    //** Destroy the mirrors
    if (s->child_seg) {
        for (i=0; i<s->n_mirrors; i++) {
            tbx_obj_put(&(s->child_seg[i]->obj));
        }
        free(s->child_seg);
    }
    if (s->child_status) free(s->child_status);
    free(s);

    ex_header_release(&(seg->header));

    apr_thread_mutex_destroy(seg->lock);
    apr_thread_cond_destroy(seg->cond);
    apr_pool_destroy(seg->mpool);

    free(seg);
}


//***********************************************************************
// segment_mirror_create - Creates a mirror segment
//***********************************************************************

lio_segment_t *segment_mirror_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    seg_mirror_priv_t *s;
    lio_segment_t *seg;

    //** Make the space
    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, seg_mirror_priv_t, 1);
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_mirror_vtable);
    assert_result(apr_pool_create(&(seg->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(seg->lock), APR_THREAD_MUTEX_DEFAULT, seg->mpool);
    apr_thread_cond_create(&(seg->cond), seg->mpool);

    generate_ex_id(&(seg->header.id));
    seg->header.type = SEGMENT_TYPE_MIRROR;

    s->tpc = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);

    seg->priv = s;
    seg->ess = es;

    return(seg);
}

//***********************************************************************
// segment_mirror_load - Loads a disk cache segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_mirror_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_mirror_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        tbx_obj_put(&seg->obj);
        seg = NULL;
    }
    return(seg);
}

//***********************************************************************
// Static function table
//***********************************************************************

const lio_segment_vtable_t lio_mirror_vtable = {
        .base.name = SEGMENT_TYPE_MIRROR,
        .base.free_fn = seg_mirror_destroy,
        .read = seg_mirror_read,
        .write = seg_mirror_write,
        .inspect = seg_mirror_inspect,
        .truncate = seg_mirror_truncate,
        .remove = seg_mirror_remove,
        .flush = seg_mirror_flush,
        .clone = seg_mirror_clone,
        .signature = seg_mirror_signature,
        .size = seg_mirror_size,
        .block_size = seg_mirror_block_size,
        .tool = seg_mirror_tool,
        .serialize = seg_mirror_serialize,
        .deserialize = seg_mirror_deserialize,
};
