/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

//*****************************************************
// ibp_rid_bulk_warm- IBP utility to bulk warm allocations for a single RID
//*****************************************************

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <signal.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <ibp/types.h>
#include <ibp/op.h>
#include <tbx/network.h>
#include <tbx/random.h>
#include <tbx/siginfo.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>

int timeout = 60;

typedef struct {
    ibp_cap_t **caps;
    int *failed;
    int n_failed;
    int n;
} bulk_t;

//*************************************************************************
// process_serial
//*************************************************************************

void process_serial(gop_op_generic_t *gop, int *good, int *bad)
{
    gop_op_status_t status;
    ibp_cap_t *cap;

    status = gop_get_status(gop);
    cap = gop_get_private(gop);

    if (status.op_status == OP_STATE_SUCCESS) {
        (*good)++;
        log_printf(2, "GOOD: cap=%s\n", cap);
    } else {
        (*bad)++;
        log_printf(1, "BAD: cap=%s\n", cap);
        printf("ERROR: cap=%s\n", cap);
    }

    free(cap);
    gop_free(gop, OP_DESTROY);

    return;
}

//*************************************************************************
// warm_serial - Warms the allocations one at a time in parallel
//*************************************************************************

int warm_serial(ibp_context_t *ic, void *it, ibp_depot_t *depot, int n_warm, int duration, int *good)
{
    gop_opque_t *q;
    gop_op_generic_t *gop;
    char *cap;
    int count, bad;
    q = gop_opque_new();
    opque_start_execution(q);

    count = 0;
    bad = 0;
    while ((cap = tbx_stdinarray_iter_next(it)) != NULL) {
        //** Wait for a slot to open up
        if (count >= n_warm) {
            process_serial(opque_waitany(q), good, &bad);
        }

        //** Add the next task
        gop = ibp_modify_alloc_gop(ic, cap, -1, duration, -1, timeout);
        gop_set_private(gop, cap);
        gop_opque_add(q, gop);
    }

    //** Wait for the rest of the tasks ot complete
    while ((gop = opque_waitany(q)) != NULL) {
        process_serial(gop, good, &bad);
    }

    gop_opque_free(q, OP_DESTROY);

    return(bad);
}

//*************************************************************************

bulk_t *bulk_new(int n)
{
    bulk_t *bulk;

    tbx_type_malloc_clear(bulk, bulk_t, 1);
    tbx_type_malloc_clear(bulk->caps, ibp_cap_t *, n);
    tbx_type_malloc_clear(bulk->failed, int, n);

    bulk->n = n;

    return(bulk);
}

void bulk_free(bulk_t *bulk)
{
    int i;

    for (i=0; i<bulk->n; i++) {
        free(bulk->caps[i]);
    }

    free(bulk->caps);
    free(bulk->failed);
    free(bulk);
}

//*************************************************************************
// process_bulk
//*************************************************************************

void process_bulk(gop_op_generic_t *gop, int *good, int *bad)
{
    gop_op_status_t status;
    bulk_t *bulk;
    int i;

    status = gop_get_status(gop);
    bulk = gop_get_private(gop);

    if (status.op_status == OP_STATE_SUCCESS) {
        *good = *good + bulk->n - bulk->n_failed;
        *bad = *bad + bulk->n_failed;
        for (i=0; i<bulk->n_failed; i++) {
            log_printf(1, "ERROR: cap=%s\n", bulk->caps[bulk->failed[i]]);
            printf("ERROR: cap=%s\n", bulk->caps[bulk->failed[i]]);
        }
    } else {
        (*bad) = *bad + bulk->n;
        for (i=0; i<bulk->n; i++) {
            log_printf(1, "ERROR: cap=%s\n", bulk->caps[bulk->failed[i]]);
            printf("ERROR: cap=%s\n", bulk->caps[i]);
        }
    }

    bulk_free(bulk);
    gop_free(gop, OP_DESTROY);

    return;
}

//*************************************************************************
// warm_bulk - Warms the allocations using the bulk warm operation
//*************************************************************************

int warm_bulk(ibp_context_t *ic, void *it, ibp_depot_t *depot, int n_warm, int duration, int *good)
{
    gop_opque_t *q;
    gop_op_generic_t *gop;
    bulk_t *bulk;
    char *mcap;
    int count, bad, first;

    q = gop_opque_new();
    opque_start_execution(q);

    first = 1;
    count = 0;
    bad = 0;
    bulk = bulk_new(n_warm);
    while ((mcap = tbx_stdinarray_iter_next(it)) != NULL) {
        //** Wait for a slot to open up
        if (count >= n_warm) {
            if (!first) process_bulk(opque_waitany(q), good, &bad);
            first = 0;
            bulk->n = count;
            gop = ibp_rid_bulk_warm_gop(ic, depot, duration, count, bulk->caps, &(bulk->n_failed), bulk->failed, timeout);
            gop_set_private(gop, bulk);
            gop_opque_add(q, gop);
            count = 0;
            bulk = bulk_new(n_warm);
        }

        //** Add the next task
        bulk->caps[count] = mcap;
        count++;
    }

    //** Submit the last batch
    if (count > 0) {
        bulk->n = count;
        gop = ibp_rid_bulk_warm_gop(ic, depot, duration, count, bulk->caps, &(bulk->n_failed), bulk->failed, timeout);
        gop_set_private(gop, bulk);
        gop_opque_add(q, gop);
    }

    //** Wait for the rest of the tasks ot complete
    while ((gop = opque_waitany(q)) != NULL) {
        process_bulk(gop, good, &bad);
    }

    gop_opque_free(q, OP_DESTROY);
    return(bad);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bulk_mode = 1;
    int n_warm = 1000;
    int i, port, start_option, n_fail, good, duration;
    ibp_depot_t *depot = NULL;
    void *it;
    ibp_context_t *ic;

    duration = 86400;

    //** Print help
    if (argc < 5) {
        printf("\n");
        printf("ibp_rid_bulk_warm [-d log_level] [--log lof_file] [--one] [-n n_warm] [--duration dt] --depot host port rid    mcap_1 ... mcap_N\n");
        printf("--one                  Use individual warm ops instead of the ibp_rid_bulk_warm operation\n");
        printf("-n n_warm              Number of allocations to warn at a time. Defaults to %d\n", n_warm);
        printf("--duration dt          Duration in seconds. Default is %d\n", duration);
        printf("--depot host port rid  The RID to used by all the capabilities\n");
        printf("mcap_*                 Manage capabilities.  If '-' is used then they are taken from stdin\n");
        return(1);
    }


    gop_init_opque_system();  //** Initialize GOP.  This needs to be done after any fork() call
    tbx_random_startup();
    tbx_set_log_level(-1);
    tbx_siginfo_install(strdup("/tmp/lio_info.txt"), SIGUSR1);

    //** Parse the args
    i = 1;
    do {
        start_option = i;

        if (strcmp(argv[i], "--one") == 0) { //** Serial mode
            i++;
            bulk_mode = 0;
        } else if (strcmp(argv[i], "-n") == 0) { //** Number to do at a time
            i++;
            n_warm = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--duration") == 0) { //** Number to do at a time
            i++;
            duration = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--depot") == 0) { //** The RID we're using
            i++;
            port = atoi(argv[i+1]);
            depot = ibp_depot_new();
            ibp_depot_set(depot, argv[i], port, ibp_str2rid(argv[i+2]));
            i = i+3;
        } else if (strcmp(argv[i], "-d") == 0) { //** Log_level
            i++;
            tbx_set_log_level(atoi(argv[i]));
            i++;
        } else if (strcmp(argv[i], "--log") == 0) { //** Log_level
            i++;
            tbx_log_open(argv[i], 1);
            i++;
        }
    } while ((start_option < i) && (i<argc));
    start_option = i;

    //** Verify we got a depot/RID
    if (!depot) {
        fprintf(stderr, "ERROR: Missing depot/RID!\n");
        return(1);
    }

    //** Create the IBP context
    ic = ibp_context_create();

    //** Warm the caps
    good = 0;
    it = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));
    if (bulk_mode == 0) {
        n_fail = warm_serial(ic, it, depot, n_warm, duration, &good);
    } else {
        n_fail = warm_bulk(ic, it, depot, n_warm, duration, &good);
    }

    printf("FINISHED: good=%d failed=%d\n", good, n_fail);

    //** Clean up
    tbx_stdinarray_iter_destroy(it);
    ibp_depot_destroy(depot);
    ibp_context_destroy(ic);
    tbx_siginfo_shutdown();
    tbx_log_flush();
    gop_shutdown();

    return((n_fail == 0) ? 0 : -1);
}