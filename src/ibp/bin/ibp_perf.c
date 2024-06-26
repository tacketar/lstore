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

//*****************************************************
// ibp_perf - Benchmarks IBP depot creates, removes,
//      reads, and writes.  The read and write tests
//      use sync an async iovec style operations.
//*****************************************************

#define _log_module_index 139

#include <apr_time.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/types.h>
#include <ibp/protocol.h>
#include <limits.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/chksum.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/lio_monitor.h>
#include <tbx/network.h>
#include <tbx/random.h>
#include <tbx/siginfo.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <time.h>

#include "op.h"
#include "types.h"
#include "io_wrapper.h"

int a_duration=900;   //** Default duration

ibp_depotinfo_t depotinfo;
ibp_depot_t *depot_list;
int n_depots;
int ibp_timeout;
int sync_transfer;
int nthreads;
int use_proxy;
int report_interval = 0;
int do_validate = 0;
int identical_buffers = 1;
int print_progress;
ibp_connect_context_t *cc = NULL;
tbx_ns_chksum_t *ncs;
int disk_cs_type = CHKSUM_DEFAULT;
ibp_off_t disk_blocksize = 0;

ibp_context_t *ic = NULL;

//*************************************************************************
//  init_buffer - Initializes a buffer.  This routine was added to
//     get around throwing network chksum errors by making all buffers
//     identical.  The char "c" may or may not be used.
//*************************************************************************

void init_buffer(char *buffer, char c, int size)
{
    if (identical_buffers == 1) {
        memset(buffer, 'A', size);
        return;
    }

    memset(buffer, c, size);
}

//*************************************************************************
//  create_proxy_allocs - Creates a group of proxy allocations in parallel
//   The proxy allocations are based on the input allocations and round-robined
//   among them
//*************************************************************************

ibp_capset_t *create_proxy_allocs(int n, ibp_capset_t *base_caps, int n_base)
{
    int i, err, nallocs;
    gop_opque_t *q;
    gop_op_generic_t *op;
    ibp_capset_t *bcap;

    nallocs = abs(n);
    ibp_capset_t *caps = (ibp_capset_t *)malloc(sizeof(ibp_capset_t)*nallocs);

    q = gop_opque_new();

    for (i=0; i<nallocs; i++) {
        bcap = &(base_caps[i % n_base]);
        op = ibp_proxy_alloc_gop(ic, &(caps[i]), ibp_cap_get(bcap, IBP_MANAGECAP), 0, 0, 0, ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("create_proxy_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    if (n < 0) { //** Dump the allocactions
        for (i=0; i<nallocs; i++) {
            printf("PA=%d rcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_READCAP));
            printf("PA=%d wcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_WRITECAP));
            printf("PA=%d mcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_MANAGECAP));
        }
    }

    return(caps);
}

//*************************************************************************
// proxy_remove_allocs - Remove a list of *PROXY* allocations
//*************************************************************************

void proxy_remove_allocs(ibp_capset_t *caps_list, ibp_capset_t *mcaps_list, int nallocs, int mallocs)
{
    int i, j, err;
    gop_opque_t *q;
    gop_op_generic_t *op;

    q = gop_opque_new();

    for (i=0; i<nallocs; i++) {
        j = i % mallocs;
        op = ibp_proxy_remove_gop(ic, ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP),
                                     ibp_cap_get(&(mcaps_list[j]), IBP_MANAGECAP), ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("proxy_remove_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    //** Lastly free all the caps and the array
    for (i=0; i<nallocs; i++) {
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_READCAP));
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_WRITECAP));
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP));
    }

    free(caps_list);

    return;
}

//*************************************************************************
//  create_allocs - Creates a group of allocations in parallel
//*************************************************************************

ibp_capset_t *create_allocs(int n, int asize)
{
    int i, err, nallocs;
    ibp_attributes_t attr;
    ibp_depot_t *depot;
    gop_opque_t *q;
    gop_op_generic_t *op;

    nallocs = abs(n);
    ibp_capset_t *caps = (ibp_capset_t *)malloc(sizeof(ibp_capset_t)*nallocs);

    ibp_attributes_set(&attr, time(NULL) + a_duration, IBP_HARD, IBP_BYTEARRAY);
    q = gop_opque_new();

    for (i=0; i<nallocs; i++) {
        depot = &(depot_list[i % n_depots]);
        op = ibp_alloc_gop(ic, &(caps[i]), asize, depot, &attr, disk_cs_type, disk_blocksize, ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("create_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
        abort();
    }
    gop_opque_free(q, OP_DESTROY);

    if (n < 0) { //** Dump the allocactions
        for (i=0; i<nallocs; i++) {
            printf("CA=%d rcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_READCAP));
            printf("CA=%d wcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_WRITECAP));
            printf("CA=%d mcap=%s\n", i, ibp_cap_get(&(caps[i]), IBP_MANAGECAP));
        }
    }
    return(caps);
}

//*************************************************************************
// remove_allocs - Remove a list of allocations
//*************************************************************************

void remove_allocs(ibp_capset_t *caps_list, int nallocs)
{
    int i, err;
    gop_opque_t *q;
    gop_op_generic_t *op;

    q = gop_opque_new();

    for (i=0; i<nallocs; i++) {
        op = ibp_remove_gop(ic, ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP), ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("remove_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    //** Lastly free all the caps and the array
    for (i=0; i<nallocs; i++) {
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_READCAP));
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_WRITECAP));
        ibp_cap_destroy(ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP));
    }

    free(caps_list);

    return;
}

//*************************************************************************
// save_allocs - Stores the allocations to the provided fd
//*************************************************************************

void save_allocs(FILE *fd, ibp_capset_t *caps_list, int nallocs)
{
    int i;

    //** Print the ds_read compatible portion of the file
    fprintf(fd, "%d\n", nallocs);
    for (i=0; i<nallocs; i++) {
        fprintf(fd,"%s\n", ibp_cap_get(&(caps_list[i]), IBP_READCAP));
    }

    //** Now print the full caps
    fprintf(fd, "=========FULL CAPS FOLLOW===========\n");
    for (i=0; i<nallocs; i++) {
        fprintf(fd,"%s\n", ibp_cap_get(&(caps_list[i]), IBP_READCAP));
        fprintf(fd,"%s\n", ibp_cap_get(&(caps_list[i]), IBP_WRITECAP));
        fprintf(fd,"%s\n", ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP));
    }

    return;
}

//*************************************************************************
// validate_allocs - Validates a list of allocations
//*************************************************************************

void validate_allocs(ibp_capset_t *caps_list, int nallocs)
{
    int i, err;
    int nalloc_bad, nblocks_bad;
    gop_opque_t *q;
    gop_op_generic_t *op;
    int *bad_blocks = (int *) malloc(sizeof(int)*nallocs);
    int correct_errors = 0;

    q = gop_opque_new();

    for (i=0; i<nallocs; i++) {
        bad_blocks[i] = 0;
        op = ibp_validate_chksum_gop(ic, ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP), correct_errors, &(bad_blocks[i]),
                                        ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("validate_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
        nalloc_bad = 0;
        nblocks_bad = 0;
        for (i=0; i<nallocs; i++) {
            if (bad_blocks[i] != 0) {
                printf("  %d   cap=%s  blocks_bad=%d\n", i, ibp_cap_get(&(caps_list[i]), IBP_MANAGECAP), bad_blocks[i]);
                nalloc_bad++;
                nblocks_bad = nblocks_bad + bad_blocks[i];
            }
        }

        printf("  Total Bad allocations: %d   Total Bad blocks: %d\n", nalloc_bad, nblocks_bad);
    }
    gop_opque_free(q, OP_DESTROY);

    free(bad_blocks);

    return;
}

//*************************************************************************
// write_allocs - Upload data to allocations
//*************************************************************************

void write_allocs(ibp_capset_t *caps, int n, int asize, int block_size)
{
    int i, j, nblocks, rem, len, err, slot;
    gop_opque_t *q;
    gop_op_generic_t *op;
    tbx_tbuf_t *buf;

    char *buffer = (char *)malloc(block_size);
    init_buffer(buffer, 'W', block_size);

    q = gop_opque_new();

    nblocks = asize / block_size;
    rem = asize % block_size;
    if (rem > 0) nblocks++;

    tbx_type_malloc_clear(buf, tbx_tbuf_t, n*nblocks);

    for (j=nblocks-1; j>= 0; j--) {
        for (i=0; i<n; i++) {
            if ((j==(nblocks-1)) && (rem > 0)) {
                len = rem;
            } else {
                len = block_size;
            }
            slot = j*n + i;
            tbx_tbuf_single(&(buf[slot]), len, buffer);
            op = ibp_write_gop(ic, ibp_cap_get(&(caps[i]), IBP_WRITECAP), j*block_size, &(buf[slot]), 0, len, ibp_timeout);
            gop_opque_add(q, op);
        }
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }

    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(buffer);
}

//*************************************************************************
// read_allocs - Downlaod data from allocations
//*************************************************************************

void read_allocs(ibp_capset_t *caps, int n, int asize, int block_size)
{
    int i, j, nblocks, rem, len, err, slot;
    gop_opque_t *q;
    gop_op_generic_t *op;
    tbx_tbuf_t *buf;

    char *buffer = (char *)malloc(block_size);

    q = gop_opque_new();

    nblocks = asize / block_size;
    rem = asize % block_size;
    if (rem > 0) nblocks++;

    tbx_type_malloc_clear(buf, tbx_tbuf_t, n*nblocks);

//  for (j=0; j<nblocks; j++) {
    for (j=nblocks-1; j>= 0; j--) {
        for (i=0; i<n; i++) {
            if ((j==(nblocks-1)) && (rem > 0)) {
                len = rem;
            } else {
                len = block_size;
            }
            slot = j*n + i;
            tbx_tbuf_single(&(buf[slot]), len, buffer);
            op = ibp_read_gop(ic, ibp_cap_get(&(caps[i]), IBP_READCAP), j*block_size, &(buf[slot]), 0, len, ibp_timeout);
            gop_opque_add(q, op);
        }
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("read_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(buffer);
}

//*************************************************************************
// random_allocs - Perform random R/W on allocations
//*************************************************************************

void random_allocs(ibp_capset_t *caps, int n, int asize, int block_size, double rfrac)
{
    int i, err, bslot;
    int j, nblocks, rem, len;
    gop_opque_t *q;
    gop_op_generic_t *op;
    double rnd;
    tbx_tbuf_t *buf;

    char *rbuffer = (char *)malloc(block_size);
    char *wbuffer = (char *)malloc(block_size);
    init_buffer(rbuffer, 'r', block_size);
    init_buffer(wbuffer, 'w', block_size);

    q = gop_opque_new();

    nblocks = asize / block_size;
    rem = asize % block_size;
    if (rem > 0) nblocks++;

    tbx_type_malloc_clear(buf, tbx_tbuf_t, n*nblocks);

    for (j=0; j<nblocks; j++) {
        for (i=0; i<n; i++) {
            rnd = rand()/(RAND_MAX + 1.0);

            if ((j==(nblocks-1)) && (rem > 0)) {
                len = rem;
            } else {
                len = block_size;
            }

            bslot = j*n + i;

            if (rnd < rfrac) {
                tbx_tbuf_single(&(buf[bslot]), len, rbuffer);
                op = ibp_read_gop(ic, ibp_cap_get(&(caps[i]), IBP_READCAP), j*block_size, &(buf[bslot]), 0, len, ibp_timeout);
            } else {
                tbx_tbuf_single(&(buf[bslot]), len, wbuffer);
                op = ibp_write_gop(ic, ibp_cap_get(&(caps[i]), IBP_WRITECAP), j*block_size, &(buf[bslot]), 0, len, ibp_timeout);
            }
            gop_opque_add(q, op);
        }
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("random_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(rbuffer);
    free(wbuffer);
}

//*************************************************************************
// small_write_allocs - Performs small write I/O on the bulk allocations
//*************************************************************************

double small_write_allocs(ibp_capset_t *caps, int n, int asize, int small_count, int min_size, int max_size)
{
    int i, io_size, offset, slot, err;
    gop_opque_t *q;
    gop_op_generic_t *op;
    double rnd, lmin, lmax;
    double nbytes;
    tbx_tbuf_t *buf;

    q = gop_opque_new();

    if (asize < max_size) {
        max_size = asize;
        log_printf(0, "small_write_allocs:  Adjusting max_size=%d\n", max_size);
    }

    lmin = log(min_size);
    lmax = log(max_size);

    char *buffer = (char *)malloc(max_size);
    init_buffer(buffer, 'a', max_size);

    tbx_type_malloc_clear(buf, tbx_tbuf_t, small_count);

    nbytes = 0;
    for (i=0; i<small_count; i++) {
        rnd = rand()/(RAND_MAX+1.0);
        slot = n * rnd;

        rnd = rand()/(RAND_MAX+1.0);
        rnd = lmin + (lmax - lmin) * rnd;
        io_size = exp(rnd);
        if (io_size == 0) io_size = 1;
        nbytes = nbytes + io_size;

        rnd = rand()/(RAND_MAX+1.0);
        offset = (asize - io_size) * rnd;

        tbx_tbuf_single(&(buf[i]), io_size, buffer);
        op = ibp_write_gop(ic, ibp_cap_get(&(caps[slot]), IBP_WRITECAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("small_write_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(buffer);

    return(nbytes);
}

//*************************************************************************
// small_read_allocs - Performs small read I/O on the bulk allocations
//*************************************************************************

double small_read_allocs(ibp_capset_t *caps, int n, int asize, int small_count, int min_size, int max_size)
{
    int i, io_size, offset, slot, err;
    gop_opque_t *q;
    gop_op_generic_t *op;
    double rnd, lmin, lmax;
    double nbytes;
    tbx_tbuf_t *buf;

    q = gop_opque_new();

    lmin = log(min_size);
    lmax = log(max_size);

    if (asize < max_size) {
        max_size = asize;
        log_printf(0, "small_read_allocs:  Adjusting max_size=%d\n", max_size);
    }

    char *buffer = (char *)malloc(max_size);
    init_buffer(buffer, 'r', max_size);

    tbx_type_malloc_clear(buf, tbx_tbuf_t, small_count);

    nbytes = 0;
    for (i=0; i<small_count; i++) {
        rnd = rand()/(RAND_MAX+1.0);
        slot = n * rnd;

        rnd = rand()/(RAND_MAX+1.0);
        rnd = lmin + (lmax - lmin) * rnd;
        io_size = exp(rnd);
        if (io_size == 0) io_size = 1;
        nbytes = nbytes + io_size;

        rnd = rand()/(RAND_MAX+1.0);
        offset = (asize - io_size) * rnd;

        tbx_tbuf_single(&(buf[i]), io_size, buffer);

        op = ibp_read_gop(ic, ibp_cap_get(&(caps[slot]), IBP_READCAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("small_read_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(buffer);

    return(nbytes);
}

//*************************************************************************
// small_random_allocs - Performs small random I/O on the bulk allocations
//*************************************************************************

double small_random_allocs(ibp_capset_t *caps, int n, int asize, double readfrac, int small_count, int min_size, int max_size)
{
    int i, io_size, offset, slot, err;
    gop_opque_t *q;
    gop_op_generic_t *op;
    double rnd, lmin, lmax;
    double nbytes;
    tbx_tbuf_t *buf;

    q = gop_opque_new();

    lmin = log(min_size);
    lmax = log(max_size);

    if (asize < max_size) {
        max_size = asize;
        log_printf(0, "small_random_allocs:  Adjusting max_size=%d\n", max_size);
    }

    char *rbuffer = (char *)malloc(max_size);
    char *wbuffer = (char *)malloc(max_size);
    init_buffer(rbuffer, '1', max_size);
    init_buffer(wbuffer, '2', max_size);

    tbx_type_malloc_clear(buf, tbx_tbuf_t, small_count);

    nbytes = 0;
    for (i=0; i<small_count; i++) {
        rnd = rand()/(RAND_MAX+1.0);
        slot = n * rnd;

        rnd = rand()/(RAND_MAX+1.0);
        rnd = lmin + (lmax - lmin) * rnd;
        io_size = exp(rnd);
        if (io_size == 0) io_size = 1;
        nbytes = nbytes + io_size;

        rnd = rand()/(RAND_MAX+1.0);
        offset = (asize - io_size) * rnd;

//     log_printf(15, "small_random_allocs: slot=%d offset=%d size=%d\n", slot, offset, io_size);

        rnd = rand()/(RAND_MAX+1.0);
        if (rnd < readfrac) {
            tbx_tbuf_single(&(buf[i]), io_size, rbuffer);
            op = ibp_read_gop(ic, ibp_cap_get(&(caps[slot]), IBP_READCAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
        } else {
            tbx_tbuf_single(&(buf[i]), io_size, wbuffer);
            op = ibp_write_gop(ic, ibp_cap_get(&(caps[slot]), IBP_WRITECAP), offset, &(buf[i]), 0, io_size, ibp_timeout);
        }

        gop_opque_add(q, op);
    }

    ibp_io_start(q);
    err = ibp_io_waitall(q);
    if (err != 0) {
        printf("small_random_allocs: At least 1 error occured! * ibp_errno=%d * nfailed=%d\n", err, gop_opque_tasks_failed(q));
    }
    gop_opque_free(q, OP_DESTROY);

    free(buf);
    free(rbuffer);
    free(wbuffer);

    return(nbytes);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    double r1, r2, r3;
    int i, start_option, tcpsize, cs_type, n;
    ibp_capset_t *caps_list, *base_caps;
    ibp_rid_t rid;
    int port, fd_special;
    char buffer[1024];
    apr_time_t stime, dtime;
    double dt;
    char *net_cs_name, *disk_cs_name, *out_fname;
    FILE *fd_out;
    //tbx_phoebus_t pcc;
    tbx_chksum_t cs;
    tbx_ns_chksum_t ns_cs;
    int blocksize = INT_MIN;
    int monitor_on = 0;

    base_caps = NULL;

    if (argc < 12) {
        printf("\n");
        printf("ibp_perf [-d|-dd] [-network_chksum type blocksize] [-disk_chksum type blocksize]\n");
        printf("         [-validate] [-config ibp.cfg] [-phoebus gateway_list] [-tcpsize tcpbufsize]\n");
        printf("         [-duration duration] [-sync] [-proxy] [-progress] [-random]\n");
        printf("         n_depots depot1 port1 resource_id1 ... depotN portN ridN\n");
        printf("         nthreads ibp_timeout\n");
        printf("         proxy_createremove_count createremove_count\n");
        printf("         readwrite_count readwrite_alloc_size rw_block_size read_mix_fraction\n");
        printf("         smallio_count small_min_size small_max_size small_read_fraction\n");
        printf("\n");
        printf("-d                  - Enable *minimal* debug output\n");
        printf("-dd                 - Enable *FULL* debug output\n");
        printf("-network_chksum type blocksize - Enable network checksumming for transfers.\n");
        printf("                      type should be SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("-disk_chksum type blocksize - Enable Disk checksumming.\n");
        printf("                      type should be NONE, SHA256, SHA512, SHA1, or MD5.\n");
        printf("                      blocksize determines how many bytes to send between checksums in kbytes.\n");
        printf("-validate           - Validate disk chksum data.  Option is ignored unless disk chksumming is enabled.\n");
        printf("-config ibp.cfg     - Use the IBP configuration defined in file ibp.cfg.\n");
        printf("                      nthreads overrides value in cfg file unless -1.\n");
        printf("-phoebus            - Use Phoebus protocol for data transfers.\n");
        printf("   gateway_list     - Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321\n");
        printf("-tcpsize tcpbufsize - Use this value, in KB, for the TCP send/recv buffer sizes\n");
        printf("-duration duration  - Allocation duration in sec.  Needs to be big enough to last the entire\n");
        printf("                      run.  The default duration is %d sec.\n", a_duration);
        printf("-save fname         - Don't delete the R/W allocations on completion.  Instead save them to the given filename.\n");
        printf("                      Can use 'stdout' and 'stderr' as the filename to redirect output\n");
        printf("-sync               - Use synchronous protocol.  Default uses async.\n");
        printf("-proxy              - Use proxy allocations for all I/O operations\n");
        printf("-progress           - Print completion progress.\n");
        printf("-random             - Initializes the transfer buffers with quasi-random data.\n");
        printf("                      Disabled if network chksums are enabled.\n");
        printf("--monitor fname     - Set the monitoring log file and enable monitoring\n");
        printf("n_depots            - Number of depot tuplets\n");
        printf("depot               - Depot hostname\n");
        printf("port                - IBP port on depot\n");
        printf("resource_id         - Resource ID to use on depot\n");
        printf("nthreads            - Max Number of simultaneous threads to use.  Use -1 for defaults or value in ibp.cfg\n");
        printf("ibp_timeout         - Timeout(sec) for each IBP copmmand\n");
        printf("proxy_createremove_count*^ - Number of 0 byte files to create and remove using proxy allocations\n");
        printf("createremove_count*^ - Number of 0 byte files to create and remove to test metadata performance\n");
        printf("readwrite_count*    - Number of files to write sequentially then read sequentially\n");
        printf("                      Use the -save option to keep the allocations and not delete them\n");
        printf("readwrite_alloc_size  - Size of each allocation in KB for sequential and random tests\n");
        printf("rw_block_size       - Size of each R/W operation in KB for sequential and random tests\n");
        printf("read_mix_fraction   - Fraction of Random I/O operations that are READS\n");
        printf("smallio_count*      - Number of random small I/O operations\n");
        printf("small_min_size      - Minimum size of each small I/O operation(kb)\n");
        printf("small_max_size      - Max size of each small I/O operation(kb)\n");
        printf("small_read_fraction - Fraction of small random I/O operations that are READS\n");
        printf("\n");
        printf("*If the variable is set to 0 then the test is skipped\n");
        printf("^If the variable is negative then the allocations are NOT removed\n");
        printf(" It also causes the IBP allocations to be printed to stdout\n");
        printf("\n");

        return(-1);
    }

    gop_init_opque_system();  //** Initialize GOP.  This needs to be done after any fork() call
    tbx_random_startup();
    tbx_set_log_level(-1);
    tbx_siginfo_install(strdup("/tmp/lio_info.txt"), SIGUSR1);

    ic = ibp_context_create();  //** Initialize IBP

    i = 1;
    net_cs_name = NULL;
    disk_cs_name = NULL;
    sync_transfer = 0;
    use_proxy = 0;
    print_progress = 0;
    fd_special = 0;
    fd_out = NULL;
    out_fname = NULL;
    do {
        start_option = i;

        if (strcmp(argv[i], "-d") == 0) { //** Enable debugging
            tbx_set_log_level(5);
            i++;
        } else if (strcmp(argv[i], "-dd") == 0) { //** Enable debugging
            tbx_set_log_level(20);
            i++;
        } else if (strcmp(argv[i], "-random") == 0) { //** Random buffers
            i++;
            identical_buffers = 0;
        } else if (strcmp(argv[i], "--monitor") == 0) { //** Enable monitoring
            i++;
            tbx_monitor_create(argv[i]);
            tbx_monitor_set_state(1);
            monitor_on = 1;
            i++;
        } else if (strcmp(argv[i], "-network_chksum") == 0) { //** Add checksum capability
            i++;
            net_cs_name = argv[i];
            cs_type = tbx_chksum_type_name(argv[i]);
            if (cs_type == -1) {
                printf("Invalid chksum type.  Got %s should be SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            tbx_chksum_set(&cs, cs_type);
            i++;

            blocksize = atoi(argv[i])*1024;
            i++;
            tbx_ns_chksum_set(&ns_cs, &cs, blocksize);
            ncs = &ns_cs;
            ibp_context_chksum_set(ic, ncs);
        } else if (strcmp(argv[i], "-disk_chksum") == 0) { //** Add checksum capability
            i++;
            disk_cs_name = argv[i];
            disk_cs_type = tbx_chksum_type_name(argv[i]);
            if (disk_cs_type < CHKSUM_DEFAULT) {
                printf("Invalid chksum type.  Got %s should be NONE, SHA1, SHA256, SHA512, or MD5\n", argv[i]);
                abort();
            }
            i++;

            disk_blocksize = atoi(argv[i])*1024;
            i++;
        } else if (strcmp(argv[i], "-validate") == 0) { //** Enable validation
            i++;
            do_validate=1;
        } else if (strcmp(argv[i], "-config") == 0) { //** Read the config file
            i++;
            ibp_config_load_file(ic, argv[i], NULL);
            i++;
        } else if (strcmp(argv[i], "-save") == 0) { //** Save the allocations and don't delete them
            i++;
            out_fname = argv[i];
            i++;
            if (strcmp(out_fname, "stderr") == 0) {
                fd_out = stderr;
                fd_special = 1;
            } else if (strcmp(out_fname, "stdout") == 0) {
                fd_out = stdout;
                fd_special = 1;
            } else {
                fd_out = fopen(out_fname, "w");FATAL_UNLESS(fd_out != NULL);
            }
// FIXME trim
#if 0
        } else if (strcmp(argv[i], "-phoebus") == 0) { //** Check if we want Phoebus transfers
            cc = (ibp_connect_context_t *)malloc(sizeof(ibp_connect_context_t));
            cc->type = NS_TYPE_PHOEBUS;
            i++;

            ppath = argv[i];
            ibp_phoebus_path_set(&pcc, ppath);
//   printf("ppath=%s\n", ppath);
            cc->data = &pcc;

            ibp_read_cc_set(ic, cc);
            ibp_write_cc_set(ic, cc);

            i++;
#endif
        } else if (strcmp(argv[i], "-tcpsize") == 0) { //** Check if we want sync tests
            i++;
            tcpsize = atoi(argv[i]) * 1024;
            ibp_tcpsize_set(ic, tcpsize);
            i++;
        } else if (strcmp(argv[i], "-duration") == 0) { //** Check if we want sync tests
            i++;
            a_duration = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-sync") == 0) { //** Check if we want sync tests
            sync_transfer = 1;
            i++;
        } else if (strcmp(argv[i], "-proxy") == 0) { //** Check if we want to use proxy allocation
            use_proxy = 1;
            i++;
        } else if (strcmp(argv[i], "-progress") == 0) { //** Check if we want to print the progress
            print_progress = 1;
            i++;
        }
    } while (start_option < i);

    if (net_cs_name != NULL) identical_buffers = 1;

    n_depots = atoi(argv[i]);
    i++;

    depot_list = (ibp_depot_t *)malloc(sizeof(ibp_depot_t)*n_depots);
    int j;
    for (j=0; j<n_depots; j++) {
        port = atoi(argv[i+1]);
        rid = ibp_str2rid(argv[i+2]);
        ibp_depot_set(&(depot_list[j]), argv[i], port, rid);
        i = i + 3;
    }

    //*** Get thread count ***
    nthreads = atoi(argv[i]);
    if (nthreads <= 0) {
        nthreads = ibp_context_max_host_conn_get(ic);
    } else {
        ibp_context_max_host_conn_set(ic, nthreads);
    }
    i++;

    ibp_timeout = atoi(argv[i]);
    i++;

    //****** Get the different Stream counts *****
    int proxycreateremove_count = atol(argv[i]);
    i++;
    int createremove_count = atol(argv[i]);
    i++;
    int readwrite_count = atol(argv[i]);
    i++;
    int readwrite_size = atol(argv[i])*1024;
    i++;
    int rw_block_size = atol(argv[i])*1024;
    i++;
    double read_mix_fraction = atof(argv[i]);
    i++;

    //****** Get the different small I/O counts *****
    int smallio_count = atol(argv[i]);
    i++;
    int small_min_size = atol(argv[i])*1024;
    i++;
    int small_max_size = atol(argv[i])*1024;
    i++;
    double small_read_fraction = atof(argv[i]);
    i++;

    //*** Print the ibp client version ***
    printf("\n");
    printf("================== IBP Client Version =================\n");
    printf("%s\n", ibp_version());

    //*** Print summary of options ***
    printf("\n");
    printf("======= Base options =======\n");
    printf("n_depots: %d\n", n_depots);
    for (i=0; i<n_depots; i++) {
        printf("depot %d: %s:%d rid:%s\n", i, depot_list[i].host, depot_list[i].port, ibp_rid2str(depot_list[i].rid, buffer));
    }
    printf("\n");
    printf("IBP timeout: %d\n", ibp_timeout);
    printf("IBP duration: %d\n", a_duration);
    printf("Max Threads: %d\n", nthreads);
    if (sync_transfer == 1) {
        printf("Transfer_mode: SYNC\n");
    } else {
        printf("Transfer_mode: ASYNC\n");
    }
    printf("Use proxy: %d\n", use_proxy);

    if (cc != NULL) {
        switch (cc->type) {
        case NS_TYPE_SOCK:
            printf("Connection Type: SOCKET\n");
            break;
// FIXME trim
#if 0
        case NS_TYPE_PHOEBUS:
            phoebus_path_to_string(pstr, sizeof(pstr), &pcc);
            printf("Connection Type: PHOEBUS (%s)\n", pstr);
            break;
#endif
        case NS_TYPE_1_SSL:
            printf("Connection Type: Single SSL\n");
            break;
        case NS_TYPE_2_SSL:
            printf("Connection Type: Dual SSL\n");
            break;
        }
    } else {
        printf("Connection Type: SOCKET\n");
    }

    if (identical_buffers == 1) {
        printf("Identical buffers being used.\n");
    } else {
        printf("Quasi-random buffers being used.\n");
    }

    if (net_cs_name == NULL) {
        printf("Network Checksum Type: NONE\n");
    } else {
        printf("Network Checksum Type: %s   Block size: %dkb\n", net_cs_name, (blocksize/1024));
    }
    if (disk_cs_name == NULL) {
        printf("Disk Checksum Type: NONE\n");
    } else {
        printf("Disk Checksum Type: %s   Block size: " I64T "kb\n", disk_cs_name, (disk_blocksize/1024));
        if (do_validate == 1) {
            printf("Disk Validation: Enabled\n");
        } else {
            printf("Disk Validation: Disabled\n");
        }
    }

    if (fd_out != NULL) {
        printf("Saving allocations to %s\n", out_fname);
    }

    printf("TCP buffer size: %dkb (0 defaults to OS)\n", ibp_context_tcpsize_get(ic)/1024);
    printf("\n");

    printf("======= Bulk transfer options =======\n");
    printf("proxycreateremove_count: %d\n", proxycreateremove_count);
    printf("createremove_count: %d\n", createremove_count);
    printf("readwrite_count: %d\n", readwrite_count);
    printf("readwrite_alloc_size: %dkb\n", readwrite_size/1024);
    printf("rw_block_size: %dkb\n", rw_block_size/1024);
    printf("read_mix_fraction: %lf\n", read_mix_fraction);
    printf("\n");
    printf("======= Small Random I/O transfer options =======\n");
    printf("smallio_count: %d\n", smallio_count);
    printf("small_min_size: %dkb\n", small_min_size/1024);
    printf("small_max_size: %dkb\n", small_max_size/1024);
    printf("small_read_fraction: %lf\n", small_read_fraction);
    printf("\n");

    r1 =  1.0 * readwrite_size/1024.0/1024.0;
    r1 = readwrite_count * r1;
    printf("Approximate I/O for sequential tests: %lfMB\n", r1);
    printf("\n");

    ibp_io_mode_set(sync_transfer, print_progress, nthreads);

    //**************** Create/Remove tests ***************************
    if (proxycreateremove_count != 0) {
        n = abs(proxycreateremove_count);
        i = n/nthreads;
        printf("Starting Proxy create test (total files: %d, approx per thread: %d)\n", n, i);
        base_caps = create_allocs(1, 1);
        stime = apr_time_now();
        caps_list = create_proxy_allocs(proxycreateremove_count, base_caps, 1);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*n/dt;
        printf("Proxy create : %lf creates/sec (%.2lf sec total) \n", r1, dt);

        if (proxycreateremove_count > 0) {
            stime = apr_time_now();
            proxy_remove_allocs(caps_list, base_caps, proxycreateremove_count, 1);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*n/dt;
            printf("Proxy remove : %lf removes/sec (%.2lf sec total) \n", r1, dt);
        } else {
            printf("Proxy Remove: Skipping and leaving allocations\n");
        }
        printf("\n");

        printf("-----------------------------\n");
        fflush(stdout);

        remove_allocs(base_caps, 1);

        printf("\n");
    }

    if (createremove_count != 0) {
        n = abs(createremove_count);
        i = n/nthreads;
        printf("Starting Create test (total files: %d, approx per thread: %d)\n",n, i);

        stime = apr_time_now();
        caps_list = create_allocs(createremove_count, 1);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*n/dt;
        printf("Create : %lf creates/sec (%.2lf sec total)\n", r1, dt);

        if (createremove_count != 0) {
            stime = apr_time_now();
            remove_allocs(caps_list, abs(createremove_count));
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*n/dt;
            printf("Remove : %lf removes/sec (%.2lf sec total)\n", r1, dt);
        } else {
            printf("Remove: Skipping and leaving allocations\n");
        }
        printf("\n");
    }

    //**************** Read/Write tests ***************************
    if (readwrite_count != 0) {
        i = readwrite_count/nthreads;
        printf("Starting Bulk tests (total files: %d, approx per thread: %d", readwrite_count, i);
        r1 = 1.0*readwrite_count*readwrite_size/1024.0/1024.0;
        r2 = r1 / nthreads;
        printf(" -- total size: %lfMB, approx per thread: %lfMB\n", r1, r2);

        printf("Creating allocations....");
        fflush(stdout);
        stime = apr_time_now();
        caps_list = create_allocs(readwrite_count, readwrite_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count/dt;
        printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);

        if (use_proxy) {
            base_caps = caps_list;
            printf("Creating proxy allocations....");
            fflush(stdout);
            stime = apr_time_now();
            caps_list = create_proxy_allocs(readwrite_count, base_caps, readwrite_count);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*readwrite_count/dt;
            printf(" %lf creates/sec (%.2lf sec total) \n", r1, dt);
        }

        stime = apr_time_now();
        write_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
        printf("Write: %lf MB/sec (%.2lf sec total) \n", r1, dt);

        stime = apr_time_now();
        read_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
        printf("Read: %lf MB/sec (%.2lf sec total) \n", r1, dt);

        stime = apr_time_now();
        random_allocs(caps_list, readwrite_count, readwrite_size, rw_block_size, read_mix_fraction);
        dtime = apr_time_now() - stime;
        dt = dtime / (1.0 * APR_USEC_PER_SEC);
        r1 = 1.0*readwrite_count*readwrite_size/(dt*1024*1024);
        printf("Random: %lf MB/sec (%.2lf sec total) \n", r1, dt);

        //**************** Small I/O tests ***************************
        if (smallio_count > 0) {
            if (small_min_size == 0) small_min_size = 1;
            if (small_max_size == 0) small_max_size = 1;

            printf("\n");
            printf("Starting Small Random I/O tests...\n");

            stime = apr_time_now();
            r1 = small_write_allocs(caps_list, readwrite_count, readwrite_size, smallio_count, small_min_size, small_max_size);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = r1/(1024.0*1024.0);
            r2 = r1/dt;
            r3 = smallio_count;
            r3 = r3 / dt;
            printf("Small Random Write: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);

            stime = apr_time_now();
            r1 = small_read_allocs(caps_list, readwrite_count, readwrite_size, smallio_count, small_min_size, small_max_size);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = r1/(1024.0*1024.0);
            r2 = r1/dt;
            r3 = smallio_count;
            r3 = r3 / dt;
            printf("Small Random Read: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);

            stime = apr_time_now();
            r1 = small_random_allocs(caps_list, readwrite_count, readwrite_size, small_read_fraction, smallio_count, small_min_size, small_max_size);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = r1/(1024.0*1024.0);
            r2 = r1/dt;
            r3 = smallio_count;
            r3 = r3 / dt;
            printf("Small Random R/W: %lf MB/sec (%.2lf sec total using %lfMB or %.2lf ops/sec) \n", r2, dt, r1, r3);
        }

        if (use_proxy) {
            printf("Removing proxy allocations....");
            fflush(stdout);
            stime = apr_time_now();
            proxy_remove_allocs(caps_list, base_caps, readwrite_count, readwrite_count);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*readwrite_count/dt;
            printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);

            caps_list = base_caps;
        }


        //** If disk chksumming is enabled then validate it as well
        if ((tbx_chksum_type_valid(disk_cs_type) == 1) && (do_validate == 1)) {
            printf("Validating allocations....");
            fflush(stdout);
            stime = apr_time_now();
            validate_allocs(caps_list, readwrite_count);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*readwrite_count/dt;
            printf(" %lf validates/sec (%.2lf sec total) \n", r1, dt);
            printf("\n");
        }

        if ((fd_out == NULL) && (readwrite_count > 0)) {
            printf("Removing allocations....");
            fflush(stdout);
            stime = apr_time_now();
            remove_allocs(caps_list, readwrite_count);
            dtime = apr_time_now() - stime;
            dt = dtime / (1.0 * APR_USEC_PER_SEC);
            r1 = 1.0*readwrite_count/dt;
            printf(" %lf removes/sec (%.2lf sec total) \n", r1, dt);
            printf("\n");
        } else {
            printf("Saving allocations to %s instead of deleting them\n", out_fname);
            save_allocs(fd_out, caps_list, readwrite_count);
            if (fd_special == 0) fclose(fd_out);
        }
    }

    printf("Final network connection counter: %d\n", tbx_network_counter(NULL));

    ibp_context_destroy(ic);  //** Shutdown IBP

    if (monitor_on) tbx_monitor_destroy();

    tbx_siginfo_shutdown();
    gop_shutdown();

    return(0);
}


