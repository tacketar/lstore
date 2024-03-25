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
// Routines for managing the segment loading framework
//***********************************************************************

#define _log_module_index 160

#include <apr_time.h>
#include <errno.h>
#include <fcntl.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <sodium.h>
#include <lio/segment.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <tbx/append_printf.h>
#include <tbx/assert_result.h>
#include <tbx/direct_io.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"
#include "service_manager.h"

typedef struct {
    lio_segment_t *src;
    lio_segment_t *dest;
    data_attr_t *da;
    char *buffer;
    FILE *fd;
    lio_segment_rw_hints_t *rw_hints;
    ex_off_t src_offset;
    ex_off_t dest_offset;
    ex_off_t len;
    ex_off_t bufsize;
    int timeout;
    int truncate;
    segment_copy_read_fn_t cp_read;
    segment_copy_write_fn_t cp_write;
} lio_segment_copy_gop_t;

//***********************************************************************
// lio_segment_hint_init
//***********************************************************************

void lio_segment_hint_init(lio_segment_rw_hints_t *rwh)
{
    memset(&(rwh->shints), 0, sizeof(segment_hints_t));
}

//***********************************************************************
// lio_segment_hint_add - Adds a segment to the table.
//     Returns the slot on success and -1 if no slots are available
//***********************************************************************

int lio_segment_hint_add(lio_segment_rw_hints_t *rwh, ex_id_t sid, void *data)
{
    int i, slot;
    segment_hints_t *sh = &(rwh->shints);

    for (i=0; i<MAX_SEGMENT_HINT; i++) {
        slot = (i+sh->last_used) % MAX_SEGMENT_HINT;
        if (sh->hint[slot].sid == 0) {
            sh->hint[slot].sid = sid;
            sh->hint[slot].ptr = data;
            sh->last_used = (slot < sh->last_used) ? MAX_SEGMENT_HINT-1 : slot;;
            return(slot);
        }
    }

    return(-1);
}

//***********************************************************************
// lio_segment_hint_clear - Clears the segment slot
//***********************************************************************

void lio_segment_hint_clear(lio_segment_rw_hints_t *rwh, int slot)
{
    int i;
    segment_hints_t *sh = &(rwh->shints);

    //** Clear the hint
    sh->hint[slot].sid = 0; sh->hint[slot].ptr = NULL;

    //** See if we can contract where to look
    for (i=sh->last_used; i>=0; i--) {
        if (sh->hint[slot].sid != 0) {
            sh->last_used = slot;
            return;
        }
    }

    return;
}

//***********************************************************************
// lio_segment_hint_search - Searches for a hint mathcing the given sid.
//   the hint pointer is returned.  The state is optionally returned
//   allowing the ability to iterate over the table looking for additional matching hints
//***********************************************************************

void *lio_segment_hint_search(lio_segment_rw_hints_t *rwh, ex_id_t sid, int *state)
{
    int i, start;
    segment_hints_t *sh = &(rwh->shints);

    start = (state) ? *state : 0;
    for (i=start; i<=sh->last_used; i++) {
        if (sh->hint[i].sid == sid) {
            if (state) *state = i;
            return(sh->hint[i].ptr);
        }
    }

    return(NULL);
}

//***********************************************************************
// load_segment - Loads the given segment from the file/struct
//***********************************************************************

lio_segment_t *load_segment(lio_service_manager_t *ess, ex_id_t id, lio_exnode_exchange_t *ex)
{
    char *type = "NULL";
    char name[1024];
    segment_load_t *sload;

    if (ex->type == EX_TEXT) {
        snprintf(name, sizeof(name), "segment-" XIDT, id);
        tbx_inip_file_t *fd = ex->text.fd;
        type = tbx_inip_get_string(fd, name, "type", "");
    } else if (ex->type == EX_PROTOCOL_BUFFERS) {
        log_printf(0, "load_segment:  segment exnode parsing goes here\n");
    } else {
        log_printf(0, "load_segment:  Invalid exnode type type=%d for id=" XIDT "\n", ex->type, id);
        return(NULL);
    }

    sload = lio_lookup_service(ess, SEG_SM_LOAD, type);
    if (sload == NULL) {
        log_printf(0, "load_segment:  No matching driver for type=%s  id=" XIDT "\n", type, id);
        if (type) free(type);
        return(NULL);
    }

    if (type) free(type);
    return((*sload)(ess, id, ex));
}

//***********************************************************************
// math_gcd - Greatest common Divisor
//***********************************************************************

ex_off_t math_gcd(ex_off_t a, ex_off_t b)
{
    if (a == 0) return(b);

    return(math_gcd(b%a, a));
}

//***********************************************************************
// math_lcm - Least Common Multiple
//***********************************************************************

ex_off_t math_lcm(ex_off_t a, ex_off_t b)
{
    return((a*b) / math_gcd(a,b));
}

//***********************************************************************
// lio_segment_copy_gop_func - Does the actual segment copy operation
//***********************************************************************

gop_op_status_t lio_segment_copy_gop_func(void *arg, int id)
{
    lio_segment_copy_gop_t *sc = (lio_segment_copy_gop_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    int err;
    ex_off_t bufsize;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, dend, block_size;
    ex_tbx_iovec_t rex, wex;
    gop_opque_t *q;
    gop_op_generic_t *rgop, *wgop;
    gop_op_status_t status;

    //** Set up the buffers
    block_size = math_lcm(segment_block_size(sc->src, LIO_SEGMENT_BLOCK_NATURAL), segment_block_size(sc->dest, LIO_SEGMENT_BLOCK_NATURAL));
    tlen = sc->bufsize / 2;
    if ((block_size > tlen) || (sc->src_offset != 0) || (sc->dest_offset != 0)) {  //** LCM is to big or we have offsets so don't try and hit page boundaries
        bufsize = sc->bufsize / 2;  //** The buffer is split for R/W
    } else {  //** LCM is good and no offsets
        tlen = ( tlen / block_size);
        bufsize = tlen * block_size;
    }

    tbx_tbuf_single(&tbuf1, bufsize, sc->buffer);
    tbx_tbuf_single(&tbuf2, bufsize, &(sc->buffer[bufsize]));
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    //** Check the length
    nbytes = segment_size(sc->src) - sc->src_offset;
    if (nbytes < 0) {
        rlen = bufsize;
    } else {
        rlen = (nbytes > bufsize) ? bufsize : nbytes;
    }
    if ((sc->len != -1) && (sc->len < nbytes)) nbytes = sc->len;

    //** Go ahead and reserve the space in the destintaion
    dend = sc->dest_offset + nbytes;
    log_printf(1, "reserving space=" XOT "\n", dend);
    gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, -dend, sc->timeout));

    //** Read the initial block
    rpos = sc->src_offset;
    wpos = sc->dest_offset;
    wlen = 0;
    ex_iovec_single(&rex, rpos, rlen);
    rpos += rlen;
    nbytes -= rlen;
    rgop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
    err = gop_waitall(rgop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Intial read failed! src=%" PRIu64 " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
        gop_free(rgop, OP_DESTROY);
        return(gop_failure_status);
    }
    gop_free(rgop, OP_DESTROY);

    q = gop_opque_new();
    do {
        //** Swap the buffers
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "sseg=" XIDT " dseg=" XIDT " wpos=%" PRId64 " rlen=%" PRId64 " wlen=%" PRId64 "\n", segment_id(sc->src), segment_id(sc->dest), wpos, rlen, wlen);

        //** Start the write
        ex_iovec_single(&wex, wpos, wlen);
        wpos += wlen;
        wgop = segment_write(sc->dest, sc->da, sc->rw_hints, 1, &wex, wbuf, 0, sc->timeout);
        gop_opque_add(q, wgop);

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            ex_iovec_single(&rex, rpos, rlen);
            rpos += rlen;
            nbytes -= rlen;
            rgop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
            gop_opque_add(q, rgop);
        }

        err = opque_waitall(q);
        if (err != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR read/write failed! src=" XIDT " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
            gop_opque_free(q, OP_DESTROY);
            return(gop_failure_status);
        }
    } while (rlen > 0);

    gop_opque_free(q, OP_DESTROY);

    if (sc->truncate == 1) {  //** Truncate if wanted
        gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
    }

    status = gop_success_status;
    status.error_code = rpos;

    return(status);
}

//***********************************************************************
// lio_segment_copy_gop - Copies data between segments.  This copy is performed
//      by reading from the source and writing to the destination.
//      This is not a depot-depot copy.  The data goes through the client.
//
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *lio_segment_copy_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, lio_segment_t *dest_seg, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
    lio_segment_copy_gop_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_gop_t, 1);

    sc->da = da;
    sc->timeout = timeout;
    sc->src = src_seg;
    sc->dest = dest_seg;
    sc->rw_hints = rw_hints;
    sc->src_offset = src_offset;
    sc->dest_offset = dest_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;
    sc->truncate = do_truncate;

    return(gop_tp_op_new(tpc, NULL, lio_segment_copy_gop_func, (void *)sc, free, 1));
}


//***********************************************************************
// segment_get_gop_func - Does the actual segment get operation
//***********************************************************************

gop_op_status_t segment_get_gop_func(void *arg, int id)
{
    lio_segment_copy_gop_t *sc = (lio_segment_copy_gop_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    char *rb, *wb, *tb;
    ex_off_t bufsize;
    int err;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, total;
    ex_off_t block_size, pplen, initial_len, base_len;
    ex_tbx_iovec_t rex;
    apr_time_t loop_start, file_start;
    double dt_loop, dt_file;
    gop_op_generic_t *gop;
    gop_op_status_t status;

    //** Set up the buffers
    block_size = segment_block_size(sc->src, LIO_SEGMENT_BLOCK_NATURAL);
    tlen = (sc->bufsize / 2 / block_size) - 1;
    pplen = block_size - (sc->src_offset % block_size);
    if (tlen <= 0) {
        bufsize = sc->bufsize / 2;
        initial_len = base_len = bufsize;
    } else {
        base_len = tlen * block_size;
        bufsize = base_len + block_size;
        initial_len = base_len + pplen;
    }

    rb = sc->buffer;
    wb = &(sc->buffer[bufsize]);
    tbx_tbuf_single(&tbuf1, bufsize, rb);
    tbx_tbuf_single(&tbuf2, bufsize, wb);
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    status = gop_success_status;

    //** Read the initial block
    rpos = sc->src_offset;
    wpos = 0;
    nbytes = segment_size(sc->src) - sc->src_offset;
    if (sc->len > 0) {
        if (nbytes > sc->len) nbytes = sc->len;
    }
    if (nbytes < 0) {
        rlen = initial_len;
    } else {
        rlen = (nbytes > initial_len) ? initial_len : nbytes;
    }

    log_printf(5, "FILE fd=%p\n", sc->fd);

    ex_iovec_single(&rex, rpos, rlen);
    wlen = 0;
    rpos += rlen;
    nbytes -= rlen;
    loop_start = apr_time_now();
    gop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
    err = gop_waitall(gop);
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "Intial read failed! src=" XIDT " rpos=" XOT " len=" XOT "\n", segment_id(sc->src), rpos, rlen);
        gop_free(gop, OP_DESTROY);
        return(gop_failure_status);
    }
    gop_free(gop, OP_DESTROY);

    total = 0;
    bufsize = base_len;  //** Everything else uses the base_len

    do {
        //** Swap the buffers
        tb = rb;
        rb = wb;
        wb = tb;
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "sseg=" XIDT " rpos=" XOT " wpos=" XOT " rlen=" XOT " wlen=" XOT " nbytes=" XOT "\n", segment_id(sc->src), rpos, wpos, rlen, wlen, nbytes);

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            ex_iovec_single(&rex, rpos, rlen);
            loop_start = apr_time_now();
            gop = segment_read(sc->src, sc->da, sc->rw_hints, 1, &rex, rbuf, 0, sc->timeout);
            gop_start_execution(gop);  //** Start doing the transfer
            rpos += rlen;
            nbytes -= rlen;
        } else {
            gop = NULL;
        }

        //** Start the write
        file_start = apr_time_now();
        got = sc->cp_write(sc->fd, wb, wlen, -1);
        dt_file = apr_time_now() - file_start;
        dt_file /= (double)APR_USEC_PER_SEC;
        total += got;
        log_printf(5, "sid=" XIDT " fwrite(wb,1," XOT ", sc->fd)=" XOT " total=" XOT "\n", segment_id(sc->src), wlen, got, total);
        if (wlen != got) {
            log_printf(1, "ERROR from fwrite=%d  dest got=" XOT "\n", errno, got);
            status = gop_failure_status;
            if (gop) {
                gop_waitall(gop);
                gop_free(gop, OP_DESTROY);
            }
            goto fail;
        }

        wpos += wlen;

        //** Wait for the read to complete
        if (rlen > 0) {
            err = gop_waitall(gop);
            gop_free(gop, OP_DESTROY);
            if (err != OP_STATE_SUCCESS) {
                log_printf(1, "ERROR read(seg=" XIDT ") failed! wpos=" XOT " len=" XOT "\n", segment_id(sc->src), wpos, wlen);
                status = gop_failure_status;
                goto fail;
            }
        }

        dt_loop = apr_time_now() - loop_start;
        dt_loop /= (double)APR_USEC_PER_SEC;
        log_printf(1, "dt_loop=%lf  dt_file=%lf\n", dt_loop, dt_file);

    } while (rlen > 0);

fail:

    return(status);
}



//***********************************************************************
// segment_get_gop - Reads data from the given segment and copies it to the given FD
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *segment_get_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, lio_segment_t *src_seg, segment_copy_write_fn_t cp_write, FILE *fd, ex_off_t src_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int timeout)
{
    lio_segment_copy_gop_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_gop_t, 1);

    sc->da = da;
    sc->rw_hints = rw_hints;
    sc->timeout = timeout;
    sc->fd = fd;
    sc->src = src_seg;
    sc->src_offset = src_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;
    sc->cp_write = cp_write;

    return(gop_tp_op_new(tpc, NULL, segment_get_gop_func, (void *)sc, free, 1));
}

//***********************************************************************
// segment_put_gop_func - Does the actual segment put operation
//***********************************************************************

gop_op_status_t segment_put_gop_func(void *arg, int id)
{
    lio_segment_copy_gop_t *sc = (lio_segment_copy_gop_t *)arg;
    tbx_tbuf_t *wbuf, *rbuf, *tmpbuf;
    tbx_tbuf_t tbuf1, tbuf2;
    char *rb, *wb, *tb;
    ex_off_t bufsize;
    int err;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, dend, block_size;
    ex_off_t initial_len, base_len, pplen;
    ex_tbx_iovec_t wex;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    apr_time_t loop_start, file_start;
    double dt_loop, dt_file;

    //** Set up the buffers
    block_size = segment_block_size(sc->dest, LIO_SEGMENT_BLOCK_NATURAL);
    tlen = (sc->bufsize / 2 / block_size) - 1;
    pplen = block_size - (sc->dest_offset % block_size);
    if (tlen <= 0) {
        bufsize = sc->bufsize / 2;
        initial_len = base_len = bufsize;
    } else {
        base_len = tlen * block_size;
        bufsize = base_len + block_size;
        initial_len = base_len + pplen;
    }

    //** The buffer is split for R/W
    rb = sc->buffer;
    wb = &(sc->buffer[bufsize]);
    tbx_tbuf_single(&tbuf1, bufsize, rb);
    tbx_tbuf_single(&tbuf2, bufsize, wb);
    rbuf = &tbuf1;
    wbuf = &tbuf2;

    nbytes = sc->len;
    status = gop_success_status;
    dend = 0;

    //** Go ahead and reserve the space in the destintaion
    if (nbytes > 0) {
        if (sc->truncate == 1) {
            dend = sc->dest_offset + nbytes;
            gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, -dend, sc->timeout));
        }
    }

    //** Read the initial block
    rpos = 0;
    wpos = sc->dest_offset;
    if (nbytes < 0) {
        rlen = initial_len;
    } else {
        rlen = (nbytes > initial_len) ? initial_len : nbytes;
    }

    wlen = 0;
    rpos += rlen;
    if (nbytes > 0) nbytes -= rlen;
    log_printf(0, "FILE fd=%p bufsize=" XOT " rlen=" XOT " nbytes=" XOT "\n", sc->fd, bufsize, rlen, nbytes);

    loop_start = apr_time_now();
    got = sc->cp_read(sc->fd, rb, rlen, -1);
    dt_file = apr_time_now() - loop_start;
    dt_file /= (double)APR_USEC_PER_SEC;

    if (got == -1) {
        log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " rlen=" XOT " got=" XOT "\n", errno, segment_id(sc->dest), rlen, got);
        status = gop_failure_status;
        goto finished;
    }
    rlen = got;

    bufsize = base_len;  //** Everything else uses the base_len

    do {
        //** Swap the buffers
        tb = rb;
        rb = wb;
        wb = tb;
        tmpbuf = rbuf;
        rbuf = wbuf;
        wbuf = tmpbuf;
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "dseg=" XIDT " wpos=" XOT " rlen=" XOT " wlen=" XOT "\n", segment_id(sc->dest), wpos, rlen, wlen);

        //** Start the write
        ex_iovec_single(&wex, wpos, wlen);
        wpos += wlen;
        loop_start = apr_time_now();
        gop = segment_write(sc->dest, sc->da, sc->rw_hints, 1, &wex, wbuf, 0, sc->timeout);
        gop_start_execution(gop);  //** Start doing the transfer

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            file_start = apr_time_now();
            got = sc->cp_read(sc->fd, rb, rlen, -1);
            dt_file = apr_time_now() - file_start;
            dt_file /= (double)APR_USEC_PER_SEC;
            if (got == -1) {
                log_printf(1, "ERROR from fread=%d  dest sid=" XIDT " got=" XOT " rlen=" XOT "\n", errno, segment_id(sc->dest), got, rlen);
                status = gop_failure_status;
                gop_waitall(gop);
                gop_free(gop, OP_DESTROY);
                goto finished;
            }
            rlen = got;
            rpos += rlen;
            if (nbytes > 0) nbytes -= rlen;
        }

        //** Wait for it to complete
        err = gop_waitall(gop);
        dt_loop = apr_time_now() - loop_start;
        dt_loop /= (double)APR_USEC_PER_SEC;

        log_printf(1, "dt_loop=%lf  dt_file=%lf nleft=" XOT " rlen=" XOT " err=%d\n", dt_loop, dt_file, nbytes, rlen, err);

        if (err != OP_STATE_SUCCESS) {
            log_printf(1, "ERROR write(dseg=" XIDT ") failed! wpos=" XOT " len=" XOT "\n", segment_id(sc->dest), wpos, wlen);
            status = gop_failure_status;
            gop_free(gop, OP_DESTROY);
            goto finished;
        }
        gop_free(gop, OP_DESTROY);
    } while (rlen > 0);

    if (sc->truncate == 1) {  //** Truncate if wanted
        gop_sync_exec(lio_segment_truncate(sc->dest, sc->da, wpos, sc->timeout));
    }

finished:

    return(status);
}



//***********************************************************************
// segment_put_gop - Stores data from the given FD into the segment.
//      If len == -1 then all available data from src is copied
//***********************************************************************

gop_op_generic_t *segment_put_gop(gop_thread_pool_context_t *tpc, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, segment_copy_read_fn_t cp_read, FILE *fd, lio_segment_t *dest_seg, ex_off_t dest_offset, ex_off_t len, ex_off_t bufsize, char *buffer, int do_truncate, int timeout)
{
    lio_segment_copy_gop_t *sc;

    tbx_type_malloc(sc, lio_segment_copy_gop_t, 1);

    sc->da = da;
    sc->rw_hints = rw_hints;
    sc->timeout = timeout;
    sc->fd = fd;
    sc->dest = dest_seg;
    sc->dest_offset = dest_offset;
    sc->len = len;
    sc->bufsize = bufsize;
    sc->buffer = buffer;
    sc->truncate = do_truncate;
    sc->cp_read = cp_read;

    return(gop_tp_op_new(tpc, NULL, segment_put_gop_func, (void *)sc, free, 1));
}


//======================================================================================================
//  Below are all the encryption at rest helpers
//======================================================================================================

//***********************************************************************
// crypt_newkeys - Generates a new key and nonce for use in encryption
//***********************************************************************

void crypt_newkeys(char **key, char **nonce)
{
    if (key) {
        tbx_type_malloc(*key, char, crypto_stream_xchacha20_KEYBYTES);
        tbx_random_get_bytes(*key, crypto_stream_xchacha20_KEYBYTES);
    }
    if (nonce) {
        tbx_type_malloc_clear(*nonce, char, crypto_stream_xchacha20_NONCEBYTES);
        tbx_random_get_bytes(*nonce, crypto_stream_xchacha20_NONCEBYTES);
    }
}


//***********************************************************************
//  crypt_bin2etext - Converts a crypto key from binary to escaped text
//***********************************************************************

char *crypt_bin2etext(char *bin, int len)
{
    char z85[2*len];

    zmq_z85_encode(z85, (unsigned char *)bin, len);
    return(tbx_stk_escape_text(TBX_INIP_ESCAPE_CHARS, '\\', z85));
}

//***********************************************************************
//  crypt_etext2bin - Converts a crypto key from escaped text to binary
//***********************************************************************

char *crypt_etext2bin(char *etext, int len)
{
    char *text, *bin;

    text = tbx_stk_unescape_text('\\', etext);  //** Unescape it
    tbx_type_malloc_clear(bin, char, len);
    zmq_z85_decode((unsigned char *)bin, text);
    free(text);

    return(bin);
}

//*******************************************************************************
// crypt_loadkeys - Load the keys and initializes the cinfo structure
//*******************************************************************************

int crypt_loadkeys(crypt_info_t *cinfo, tbx_inip_file_t *fd, const char *grp, int ok_to_generate_keys, ex_off_t chunk_size, ex_off_t stripe_size)
{
    char *etext;

    cinfo->chunk_size = chunk_size;
    cinfo->stripe_size = stripe_size;
    cinfo->crypt_chunk_scale = cinfo->chunk_size / 64;
    if (cinfo->stripe_size % 64) cinfo->crypt_chunk_scale++;

    cinfo->crypt_key = tbx_inip_get_string(fd, grp, "crypt_key", NULL);
    cinfo->crypt_nonce = tbx_inip_get_string(fd, grp, "crypt_nonce", NULL);
    if ((cinfo->crypt_key == NULL) || (cinfo->crypt_nonce == NULL)) {
        if (ok_to_generate_keys != 1) {
            log_printf(0, "ERROR: Missing key or nonce!! INI group=%s key=%s nonce=%s\n", grp, cinfo->crypt_key, cinfo->crypt_nonce);
            return(1);
        }
    }

    //** If we made it here we need to either convert the text -> binary for the key/nonce or generate a new one
    if (cinfo->crypt_key) {   //** Got a key so convert it
        etext = cinfo->crypt_key;
        cinfo->crypt_key = crypt_etext2bin(etext, strlen(etext));
        free(etext);
    } else {  //** Got to generate a new one
        crypt_newkeys(&(cinfo->crypt_key), NULL);
    }
    if (cinfo->crypt_nonce) {   //** Got a nonce so convert it
        etext = cinfo->crypt_nonce;
        cinfo->crypt_nonce = crypt_etext2bin(etext, strlen(etext));
        free(etext);
    } else {  //** Got to generate a new one
        crypt_newkeys(NULL, &(cinfo->crypt_nonce));
    }

    return(0);
}

//*******************************************************************************
// crypt_destroykeys - Does cleanup of the keys
//*******************************************************************************

void crypt_destroykeys(crypt_info_t *cinfo)
{
    if (cinfo->crypt_key) { free(cinfo->crypt_key); cinfo->crypt_key = NULL; }
    if (cinfo->crypt_nonce) { free(cinfo->crypt_nonce); cinfo->crypt_nonce = NULL; }
}

//*******************************************************************************
// crypt_regenkeys - Regenerates the keys
//*******************************************************************************

void crypt_regenkeys(crypt_info_t *cinfo)
{
    if (cinfo->crypt_key) free(cinfo->crypt_key);
    if (cinfo->crypt_nonce) free(cinfo->crypt_nonce);
    crypt_newkeys(&(cinfo->crypt_key), &(cinfo->crypt_nonce));
}

//*******************************************************************************
// crypt_read_op_next_block - Transfer buffer function to handle reading an
//        encryption at rest data stream
//*******************************************************************************

int crypt_read_op_next_block(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv)
{
    crypt_rw_t  *rwb = (crypt_rw_t *)tb->arg;
    tbx_tbuf_t tbc;
    int i, slot;
    size_t sum, ds;
    uint64_t loff, mod, page_off, poff2, lun_offset;

    //** See if we have a previous block to process
    page_off = pos / rwb->info->chunk_size;
    poff2 = page_off * rwb->info->stripe_size;
    page_off *= rwb->info->chunk_size;
    if (rwb->crypt_flush == 1) {
        if (rwb->prev_bufoff < 0) return (TBUFFER_OK);
    }
    if (rwb->prev_bufoff < 0) {
        lun_offset = rwb->lun_offset[0] + poff2;
        sum = 0;
        slot = 0;
        goto next;
    } else if ((rwb->prev_bufoff == (int64_t)page_off) && (rwb->crypt_flush == 0)) {
        lun_offset = rwb->prev_lunoff;
        sum = rwb->slot_total_pos;
        slot = rwb->curr_slot;
        goto next;
    }

    //** Need to figure out the crypt counter
    //** Update our position in the total table;
    sum = rwb->slot_total_pos;
    if (pos < sum) {
        sum = 0;
        rwb->curr_slot = 0;
    }

    slot = rwb->curr_slot;
    for (i=rwb->curr_slot; i<rwb->n_ex; i++) {
        ds = sum + rwb->ex_iov[i].len;
        if (ds > pos) {
            slot = i;
            break;
        }
        sum = ds;
    }

    lun_offset = rwb->lun_offset[slot] + poff2;

    //** check if we have an empty block. If so just copy it over
    for (i=0; i<rwb->info->chunk_size; i++) {
        if (rwb->crypt_buffer[i] != 0) goto non_zero;
    }
    goto blanks;  //** If we made it here then we have all blanks

non_zero:
    //** and LUN offset
    loff = rwb->prev_lunoff / rwb->info->chunk_size;
    loff *= rwb->info->crypt_chunk_scale;

    //** then decrypt it
    crypto_stream_xchacha20_xor_ic((unsigned char *)rwb->crypt_buffer, (unsigned char *)rwb->crypt_buffer, rwb->info->chunk_size, (unsigned char *)rwb->info->crypt_nonce, loff, (unsigned char *)rwb->info->crypt_key);

blanks:
    //** And finally copy it back to the use buffer
    tbx_tbuf_single(&tbc, rwb->info->chunk_size, rwb->crypt_buffer);
    tbx_tbuf_copy(&tbc, 0, &(rwb->tbuf_crypt), rwb->prev_bufoff, rwb->info->chunk_size, 1);

    if (rwb->crypt_flush) return(TBUFFER_OK);   //** Kick out since this is just the final call to flush the buffer

    //** Reset the buffer
    memset(rwb->crypt_buffer, 0, rwb->info->chunk_size);

next:
    //** Set things up for the next round
    rwb->prev_bufoff = page_off;
    rwb->prev_lunoff = lun_offset;

    //** And configure the tbv for the next op
    rwb->slot_total_pos = sum;
    rwb->curr_slot = slot;
    tbv->n_iov = 1;
    tbv->buffer = &(tbv->priv.single);
    mod = pos % rwb->info->chunk_size;
    if (rwb->info->chunk_size > (int64_t)(mod + tbv->nbytes)) {
        tbv->priv.single.iov_len = tbv->nbytes;
        tbv->priv.single.iov_base = rwb->crypt_buffer + mod;
        tbv->nbytes = tbv->priv.single.iov_len;
    } else {
        tbv->priv.single.iov_len = rwb->info->chunk_size - mod;
        tbv->priv.single.iov_base = rwb->crypt_buffer + mod;
        tbv->nbytes = tbv->priv.single.iov_len;
    }

    return(TBUFFER_OK);
}


//*******************************************************************************
// crypt_write_op_next_block - Transfer buffer function to handle writing an
//         encryption at rest data stream
//*******************************************************************************

int crypt_write_op_next_block(tbx_tbuf_t *tb, size_t pos, tbx_tbuf_var_t *tbv)
{
    crypt_rw_t  *rwb = (crypt_rw_t *)tb->arg;
    tbx_tbuf_t tbc;
    int i, slot;
    size_t sum, ds, nbytes;
    uint64_t loff, mod, page_off, poff2, lun_offset;

    page_off = pos / rwb->info->chunk_size;
    poff2 = page_off * rwb->info->stripe_size;
    page_off *= rwb->info->chunk_size;
    mod = pos % rwb->info->chunk_size;
    if (rwb->prev_bufoff == (int64_t)page_off) { //** Same page were already got encrypted so just return the partial buf
        tbv->n_iov = 1;
        nbytes = rwb->info->chunk_size - mod;
        tbv->buffer = &(tbv->priv.single);
        tbv->priv.single.iov_len = (nbytes > tbv->nbytes) ? tbv->nbytes : nbytes;  //** return just enough bytes to hit the buffer
        tbv->priv.single.iov_base = rwb->crypt_buffer + mod;
        tbv->nbytes = tbv->priv.single.iov_len;
        return(TBUFFER_OK);
    }

    //** Need to figure out the crypt counter
    //** Update our position in the total table;
    sum = tbv->priv.slot_total_pos;
    if (pos < sum) {
        sum = 0;
        tbv->priv.curr_slot = 0;
    }

    slot = tbv->priv.curr_slot;
    for (i=tbv->priv.curr_slot; i<rwb->n_ex; i++) {
        ds = sum + rwb->ex_iov[i].len;
        if (ds > pos) {
            slot = i;
            break;
        }
        sum = ds;
    }

    //** Found my slot
    tbv->priv.slot_total_pos = sum;
    tbv->priv.curr_slot = slot;

    //** and LUN offset
    lun_offset = rwb->lun_offset[slot] + poff2;
    loff = lun_offset / rwb->info->chunk_size;
    loff *= rwb->info->crypt_chunk_scale;

    //** Copy it from the user buffer into the buffer used for IBP writes
    tbx_tbuf_single(&tbc, rwb->info->chunk_size, rwb->crypt_buffer);
    tbx_tbuf_copy(&(rwb->tbuf_crypt), page_off, &tbc, 0, rwb->info->chunk_size, 1);

    //** then encrypt it
    crypto_stream_xchacha20_xor_ic((unsigned char *)rwb->crypt_buffer, (unsigned char *)rwb->crypt_buffer, rwb->info->chunk_size, (unsigned char *)rwb->info->crypt_nonce, loff, (unsigned char *)rwb->info->crypt_key);

    //** And configure the tbv for the next op
    rwb->prev_bufoff = page_off;
    tbv->n_iov = 1;
    nbytes = rwb->info->chunk_size - mod;
    tbv->buffer = &(tbv->priv.single);
    tbv->priv.single.iov_len = (nbytes > tbv->nbytes) ? tbv->nbytes : nbytes;  //** return just enough bytes to hit the buffer
    tbv->priv.single.iov_base = rwb->crypt_buffer + mod;
    tbv->nbytes = tbv->priv.single.iov_len;

    return(TBUFFER_OK);
}

//=======================================================================================================
// Below are helper routines for data placement
//=======================================================================================================

//***********************************************************************
// segment_placement_check - Checks the placement of each allocation
//***********************************************************************

int segment_placement_check(lio_resource_service_fn_t *rs, data_attr_t *da, segment_block_inspect_t *block, int *block_status, int n_blocks, int soft_error_fail, rs_query_t *query, lio_inspect_args_t *args, int timeout)
{
    int i, nbad;
    lio_rs_hints_t hints_list[n_blocks];
    char *migrate;
    gop_op_generic_t *gop;
    apr_hash_t *rid_changes;
    lio_rid_inspect_tweak_t *rid;
    apr_thread_mutex_t *rid_lock;

    rid_changes = args->rid_changes;
    rid_lock = args->rid_lock;

    //** Make the fixed list table
    for (i=0; i<n_blocks; i++) {
        hints_list[i].fixed_rid_key = block[i].data->rid_key;
        hints_list[i].status = RS_ERROR_OK;
        hints_list[i].local_rsq = NULL;
        hints_list[i].pick_from = NULL;
        migrate = data_block_get_attr(block[i].data, "migrate");
        if (migrate != NULL) {
            hints_list[i].local_rsq = rs_query_parse(rs, migrate);
        }
    }

    //** Now call the query check
    gop = rs_data_request(rs, NULL, query, NULL, NULL, 0, hints_list, n_blocks, n_blocks, 0, timeout);
    gop_waitall(gop);
    gop_free(gop, OP_DESTROY);

    //** Now call the query check
    gop = rs_data_request(rs, NULL, query, NULL, NULL, 0, hints_list, n_blocks, n_blocks, 0, timeout);
    gop_waitall(gop);
    gop_free(gop, OP_DESTROY);

    nbad = 0;
    for (i=0; i<n_blocks; i++) {
        if (hints_list[i].status != RS_ERROR_OK) {
            if (hints_list[i].status == RS_ERROR_FIXED_NOT_FOUND) {
                if (soft_error_fail > 0) nbad++;
            } else {
                nbad++;
            }
            block_status[i] = hints_list[i].status;
        } else if (rid_changes) { //** See if the allocation can be shuffled
            if (rid_lock != NULL) apr_thread_mutex_lock(rid_lock);
            rid = apr_hash_get(rid_changes, block[i].data->rid_key, APR_HASH_KEY_STRING);
            if (rid != NULL) {
                if ((rid->rid->state != 0) || (rid->rid->delta >= 0)) {
                    rid = NULL;
                }
                if (rid != NULL) {  //** See about shuffling the data
                    nbad++;
                    block_status[i] = -103;
                }
            }
            if (rid_lock != NULL) apr_thread_mutex_unlock(rid_lock);

        }

        if (hints_list[i].local_rsq != NULL) {
            rs_query_destroy(rs, hints_list[i].local_rsq);
        }
    }

    return(nbad);
}


//***********************************************************************
// segment_placement_fix - Moves the allocations to satisfy the placement
//     constraints
//***********************************************************************

int segment_placement_fix(lio_resource_service_fn_t *rs, data_attr_t *da, segment_block_inspect_t *block, int *block_status, int n_blocks, lio_inspect_args_t *args, int timeout, tbx_stack_t **db_cleanup)
{
    int i, j, k, nbad, ngood, loop, cleanup_index;
    int missing[n_blocks], m, todo;
    char *cleanup_key[5*n_blocks];
    lio_rs_request_t req[n_blocks];
    lio_rid_inspect_tweak_t *rid_pending[n_blocks];
    rs_query_t *rsq;
    apr_hash_t *rid_changes;
    lio_rid_inspect_tweak_t *rid;
    apr_thread_mutex_t *rid_lock;

    lio_rs_hints_t hints_list[n_blocks];
    lio_data_block_t *db[n_blocks], *dbs, *dbd, *dbold[n_blocks];
    data_cap_set_t *cap[n_blocks];

    char *migrate;
    gop_op_generic_t *gop;
    gop_opque_t *q;

    rid_changes = args->rid_changes;
    rid_lock = args->rid_lock;
    rsq = rs_query_dup(rs, args->query);

    cleanup_index = 0;
    loop = 0;
    do {
        q = gop_opque_new();

        //** Make the fixed list mapping table
        memset(db, 0, sizeof(db));
        nbad = n_blocks-1;
        ngood = 0;
        m = 0;
        if (rid_lock != NULL) apr_thread_mutex_lock(rid_lock);
        for (i=0; i<n_blocks; i++) {
            rid = NULL;
            if (rid_changes != NULL) {
                rid = apr_hash_get(rid_changes, block[i].data->rid_key, APR_HASH_KEY_STRING);
                if (rid != NULL) {
                    if ((rid->rid->state != 0) || (rid->rid->delta >= 0)) {
                        rid = NULL;
                    }
                }
            }
            rid_pending[i] = rid;

            if ((block_status[i] == 0) && (rid == NULL)) {
                j = ngood;
                hints_list[ngood].fixed_rid_key = block[i].data->rid_key;
                hints_list[ngood].status = RS_ERROR_OK;
                hints_list[ngood].local_rsq = NULL;
                hints_list[ngood].pick_from = NULL;
                ngood++;
            } else {
                j = nbad;
                hints_list[nbad].local_rsq = NULL;
                hints_list[nbad].fixed_rid_key = NULL;
                hints_list[nbad].status = RS_ERROR_OK;
                hints_list[nbad].pick_from = NULL;
                if (rid != NULL) {
                    hints_list[nbad].pick_from = rid->pick_pool;
                    rid->rid->delta += block[i].block_len;
                    rid->rid->state = ((llabs(rid->rid->delta) <= rid->rid->tolerance) || (rid->rid->tolerance == 0)) ? 1 : 0;
                    log_printf(5, "i=%d rid_key=%s, pick_pool_count=%d\n", i, block[i].data->rid_key, apr_hash_count(rid->pick_pool));
                }
                req[m].rid_index = nbad;
                req[m].size = block[i].block_len;
                db[m] = data_block_create(block[i].data->ds);
                cap[m] = db[m]->cap;
                missing[m] = i;
                nbad--;
                m++;
            }

            if (hints_list[j].local_rsq != NULL) {
                rs_query_destroy(rs, hints_list[j].local_rsq);
            }

            migrate = data_block_get_attr(block[i].data, "migrate");
            if (migrate != NULL) {
                hints_list[j].local_rsq = rs_query_parse(rs, migrate);
            }
        }

        // 3=ignore fixed and it's ok to return a partial list
        gop = rs_data_request(rs, da, rsq, cap, req, m, hints_list, ngood, n_blocks, 3, timeout);

        if (rid_lock != NULL) apr_thread_mutex_unlock(rid_lock);  //** The data request will use the rid_changes table in constructing the ops

        gop_waitall(gop);
        gop_free(gop, OP_DESTROY);

        //** Process the results
        opque_start_execution(q);
        for (j=0; j<m; j++) {
            i = missing[j];
            if (ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_READ) != NULL) {
                db[j]->rid_key = req[j].rid_key;
                req[j].rid_key = NULL;  //** Cleanup

                //** Make the copy operation
                gop = ds_copy(block[i].data->ds, da, DS_PUSH, NS_TYPE_SOCK, "",
                              ds_get_cap(block[i].data->ds, block[i].data->cap, DS_CAP_READ), block[i].cap_offset,
                              ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_WRITE), 0,
                              block[i].block_len, timeout);
                gop_set_myid(gop, j);
                gop_opque_add(q, gop);
            } else {  //** Make sure we exclude the RID key on the next round due to the failure
                data_block_destroy(db[j]);

                if (req[j].rid_key != NULL) {
                    log_printf(15, "Excluding rid_key=%s on next round\n", req[j].rid_key);
                    cleanup_key[cleanup_index] = req[j].rid_key;
                    req[j].rid_key = NULL;
                    rs_query_add(rs, &rsq, RSQ_BASE_OP_KV, "rid_key", RSQ_BASE_KV_EXACT, cleanup_key[cleanup_index], RSQ_BASE_KV_EXACT);
                    cleanup_index++;
                    rs_query_add(rs, &rsq, RSQ_BASE_OP_NOT, NULL, 0, NULL, 0);
                    rs_query_add(rs, &rsq, RSQ_BASE_OP_AND, NULL, 0, NULL, 0);
                } else if (block_status[i] == -103) {  //** Can't move the allocation so unflag it
                    if (rid_pending[i] != NULL) {
                        apr_thread_mutex_lock(rid_lock);
                        rid_pending[i]->rid->delta -= block[i].block_len;  //** This is the original allocation
                        rid_pending[i]->rid->state = ((llabs(rid_pending[i]->rid->delta) <= rid_pending[i]->rid->tolerance) || (rid_pending[i]->rid->tolerance == 0)) ? 1 : 0;
                        apr_thread_mutex_unlock(rid_lock);
                    }
                }
            }
            log_printf(15, "after rs query block_status[%d]=%d block_len=" XOT "\n", i, block_status[i], block[i].block_len);
        }

        log_printf(15, "q size=%d\n",gop_opque_task_count(q));

        //** Wait for the copies to complete
        opque_waitall(q);
        k = 0;
        while ((gop = gop_get_next_finished(opque_get_gop(q))) != NULL) {
            j = gop_get_myid(gop);
            log_printf(15, "index=%d\n", j);
            if (j >= 0) {  //** Skip any remove ops
                i = missing[j];
                log_printf(15, "missing[%d]=%d status=%d\n", j,i, gop_completed_successfully(gop));
                if (gop_completed_successfully(gop)) {  //** Update the block
                    dbs = block[i].data;
                    dbd = db[j];

                    dbd->size = dbs->size;
                    dbd->max_size = dbs->max_size;
                    tbx_atomic_inc(dbd->ref_count);
                    dbd->attr_stack = dbs->attr_stack;
                    dbs->attr_stack = NULL;

                    data_block_auto_warm(dbd);  //** Add it to be auto-warmed

                    block[i].data = dbd;
                    block[i].cap_offset = 0;
                    block_status[i] = 0;

                    gop_free(gop, OP_DESTROY);

                    if (args->qs) { //** Remove the old data on complete success
                        gop = ds_remove(dbs->ds, da, ds_get_cap(dbs->ds, dbs->cap, DS_CAP_MANAGE), timeout);
                        gop_opque_add(args->qs, gop);  //** This gets placed on the success queue so we can roll it back if needed
                    } else {       //** Remove the just created allocation on failure
                        gop = ds_remove(dbd->ds, da, ds_get_cap(dbd->ds, dbd->cap, DS_CAP_MANAGE), timeout);
                        gop_opque_add(args->qf, gop);  //** This gets placed on the failed queue so we can roll it back if needed
                    }
                    if (*db_cleanup == NULL) *db_cleanup = tbx_stack_new();
                    tbx_stack_push(*db_cleanup, dbs);  //** Dump the data block here cause the cap is needed for the gop.  We'll cleanup up on destroy()
                } else {  //** Copy failed so remove the destintation
                    gop_free(gop, OP_DESTROY);
                    gop = ds_remove(db[j]->ds, da, ds_get_cap(db[j]->ds, db[j]->cap, DS_CAP_MANAGE), timeout);
                    gop_set_myid(gop, -1);
                    dbold[k] = db[j];
                    k++;
                    gop_opque_add(q, gop);

                    if (rid_pending[i] != NULL) { //** Cleanup RID changes
                        apr_thread_mutex_lock(rid_lock);
                        rid_pending[i]->rid->delta -= block[i].block_len;  //** This is the original allocation
                        rid_pending[i]->rid->state = ((llabs(rid_pending[i]->rid->delta) <= rid_pending[i]->rid->tolerance) || (rid_pending[i]->rid->tolerance == 0)) ? 1 : 0;

                        //** and this is the destination
                        rid = apr_hash_get(rid_changes, db[j]->rid_key, APR_HASH_KEY_STRING);
                        if (rid != NULL) {
                            rid->rid->delta += block[i].block_len;
                            rid->rid->state = ((llabs(rid->rid->delta) <= rid->rid->tolerance) || (rid->rid->tolerance == 0)) ? 1 : 0;
                        }
                        apr_thread_mutex_unlock(rid_lock);
                    }
                }
            } else {
                gop_free(gop, OP_DESTROY);
            }
        }

        opque_waitall(q);  //** Wait for the removal to complete.  Don't care if there are errors we can still continue
        gop_opque_free(q, OP_DESTROY);

        //** Clean up
        for (i=0; i<k; i++) {
            data_block_destroy(dbold[i]);
        }

        todo= 0;
        for (i=0; i<n_blocks; i++) if (block_status[i] != 0) todo++;
        loop++;
    } while ((loop < 5) && (todo > 0));

    for (i=0; i<cleanup_index; i++) free(cleanup_key[i]);

    for (i=0; i<n_blocks; i++) {
        if (hints_list[i].local_rsq != NULL) {
            rs_query_destroy(rs, hints_list[i].local_rsq);
        }
    }
    rs_query_destroy(rs, rsq);

    return(todo);
}
