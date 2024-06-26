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

#include <apr_time.h>
#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/portal.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <poll.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/io.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/que.h>
#include <tbx/stack.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/types.h"
#include "lio.h"
#include "os.h"

gop_op_generic_t *lio_read_ex_gop_aio(lio_rw_op_t *op);
gop_op_generic_t *lio_write_ex_gop_aio(lio_rw_op_t *op);
int lio_tier_check_and_handle(lio_rw_op_t *op, int rw_mode, int *in_flight);
void _lio_dec_in_flight_and_unlock(lio_fd_t *fd, int in_flight);

//==========================================================================
//  These are the Asynchronous I/O routines
//==========================================================================

//*************************************************************************

void _stream_flush(lio_fd_t *fd)
{
    int err;
    ex_tbx_iovec_t iov;
    ex_off_t n_stream;

    iov.offset = fd->fh->stream->offset;
    iov.len = fd->fh->stream->used;
    if (iov.len == 0) return;  //** Nothing to do so kick out

    tbx_monitor_obj_message(&fd->fh->mo, "_stream_flush");
    n_stream = iov.offset + iov.len;  //** Get our size and post it while we flush
    tbx_atomic_set(fd->fh->is_stream_flushing_size, n_stream);
    err = gop_sync_exec(segment_write(fd->fh->seg, fd->fh->lc->da, NULL, 1, &iov, &(fd->fh->stream->tbuf), 0, fd->fh->lc->timeout));
    tbx_atomic_set(fd->fh->is_stream_flushing_size, 0);  //** Clear it when we're done
    fd->fh->stream->is_dirty = 0;
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR: WRITE_ERROR while flushing the stream at fname=%s sid=" XIDT " offset=" XOT " len=" XOT "\n", fd->fh->fname, segment_id(fd->fh->seg), fd->fh->stream->offset, fd->fh->stream->used);
    }
}

//*************************************************************************

int stream_read(lio_rw_op_t *op)
{
    lio_fd_t *fd = op->fd;
    lio_config_t *lc = fd->lc;
    ex_tbx_iovec_t *iov = op->iov;
    ex_tbx_iovec_t siov;
    tbx_tbuf_t *buffer = op->buffer;
    stream_buf_t *stream = fd->fh->stream;
    ex_off_t boff, pend, dn, dm, len;
    int i;

    tbx_monitor_thread_group(&fd->fh->mo, MON_MY_THREAD);

    boff = op->boff;
    apr_thread_mutex_lock(fd->fh->lock);
    for (i=0; i<op->n_iov; i++) {
        tbx_monitor_obj_message(&fd->fh->mo, "stream_read: i=%d offset=" XOT " len=" XOT, i, iov[i].offset, iov[i].len);

        pend = iov[i].offset + iov[i].len - 1;
        if (iov[i].len > stream->max_size) { //** Kick out if request is bigger than the stream buf
            //** See if the streambuf overlaps with the request
            if ((stream->is_dirty) && (iov[i].offset <= stream->offset_end) && (pend >= stream->offset)) {
                _stream_flush(fd);  //** Overlaps with a dirty stream buffer so flush before kicking out
            }
            goto failure;
        }

        if ((iov[i].offset >= stream->offset) && (pend <= stream->offset_end)) { //** Full overlap
            dn = iov[i].offset - stream->offset;
            tbx_tbuf_copy(&(stream->tbuf), dn, buffer, boff, iov[i].len, 1);
        } else {     //** Need to populate the stream buffer first
            //** See if we can copy anything from the existing buffer
            len = iov[i].len;
            dm = 0;
            if ((iov[i].offset >= stream->offset) && (iov[i].offset <= stream->offset_end)) { //** partial
                dn = iov[i].offset - stream->offset;
                dm = stream->offset_end - iov[i].offset + 1;
                if (dm) tbx_tbuf_copy(&(stream->tbuf), dn, buffer, boff, dm, 1);
                len -= dm;
            }

            //** Now flush and get the rest
            if (stream->is_dirty) _stream_flush(fd);
            stream->offset = iov[i].offset + dm;
            stream->offset_end = stream->offset + stream->max_size - 1;
            dn = segment_size(fd->fh->seg);
            if (stream->offset_end > dn) stream->offset_end = dn - 1;
            stream->used = stream->offset_end - stream->offset + 1;
            siov.offset = iov[i].offset + dm;
            siov.len = stream->used;
            tbx_monitor_obj_message(&fd->fh->mo, "stream_read: i=%d offset=" XOT " len=" XOT " GOP_SYNC_EXEC", i, iov[i].offset, iov[i].len);
            if (gop_sync_exec(segment_read(fd->fh->seg, lc->da, op->rw_hints, 1, &siov, &(stream->tbuf), 0, lc->timeout)) != OP_STATE_SUCCESS) goto failure;
            tbx_tbuf_copy(&(stream->tbuf), 0, buffer, boff+dm, len, 1);
        }

        boff += iov[i].len;
    }
    apr_thread_mutex_unlock(fd->fh->lock);
    tbx_monitor_thread_ungroup(&fd->fh->mo, MON_MY_THREAD);

    return(OP_STATE_SUCCESS);

failure:
    apr_thread_mutex_unlock(fd->fh->lock);
    tbx_monitor_thread_ungroup(&fd->fh->mo, MON_MY_THREAD);

    return(OP_STATE_FAILURE);
}

//*************************************************************************

int stream_write(lio_rw_op_t *op)
{
    lio_fd_t *fd = op->fd;
    ex_tbx_iovec_t *iov = op->iov;
    tbx_tbuf_t *buffer = op->buffer;
    stream_buf_t *stream = fd->fh->stream;
    ex_off_t boff, pend, dn;
    int i;

    boff = op->boff;
    apr_thread_mutex_lock(fd->fh->lock);
    for (i=0; i<op->n_iov; i++) {
        tbx_monitor_obj_message(&fd->fh->mo, "stream_write: i=%d offset=" XOT " len=" XOT, i, iov[i].offset, iov[i].len);
        pend = iov[i].offset + iov[i].len - 1;
        if (iov[i].len > stream->max_size) { //** Kick out if request is bigger than the stream buf
            //** See if we need to flush due to a partial overlap
            if ((stream->is_dirty) && (iov[i].offset <= stream->offset_end) && (pend >= stream->offset)) {
                _stream_flush(fd);  //** Overlaps with a dirty stream buffer so flush before kicking out
            }
            goto failure;   //** Now we can kick out since we flushed the data if needed.
        }

        //** If we made it here then we know the request is small enough to fit in the stream buffer
        //** Check for a full overlap
        dn = stream->offset + stream->max_size - 1;
        if ((iov[i].offset >= stream->offset) && (pend <= dn)) { //** **Could** be a full overlap
            if (iov[i].offset <= (stream->offset_end+1))  { //** Starts within the existing range so fully in or can be grown
                stream->is_dirty = 1;
                if (pend > stream->offset_end) stream->offset_end = pend;
                stream->used = stream->offset_end - stream->offset + 1;
                tbx_tbuf_copy(buffer, boff, &(stream->tbuf), iov[i].offset - stream->offset, iov[i].len, 1);
            } else {  //**It's a hole. So flush and reposition
                if (stream->is_dirty) _stream_flush(fd);

                //** Reposition the stream and dump the data
                stream->is_dirty = 1;
                stream->offset = iov[i].offset;
                stream->offset_end = pend;
                stream->used = iov[i].len;
                tbx_tbuf_copy(buffer, boff, &(stream->tbuf), 0, iov[i].len, 1);
            }
        } else {  //** Need to reposition the buffer
            if (stream->is_dirty) _stream_flush(fd);  //** Flush the existing data

            //** Reposition the stream and dump the data
            stream->is_dirty = 1;
            stream->offset = iov[i].offset;
            stream->offset_end = pend;
            stream->used = iov[i].len;
            tbx_tbuf_copy(buffer, boff, &(stream->tbuf), 0, iov[i].len, 1);
        }

        boff += iov[i].len;
    }
    apr_thread_mutex_unlock(fd->fh->lock);

    return(OP_STATE_SUCCESS);

failure:
    apr_thread_mutex_unlock(fd->fh->lock);
    return(OP_STATE_FAILURE);
}

//*************************************************************************
//  special_read - Just used for FIFO SOCKET files and only via native or LD_PRELOAD.
//     FUSE directly handles it bypassing the framework
//*************************************************************************

ex_off_t special_read(lio_rw_op_t *op)
{
    ssize_t n;
    tbx_tbuf_var_t tbv;

    if (op->n_iov > 1) {
        log_printf(0, "ERROR: n_iov>1!!! n_iov=%d\n", op->n_iov);
        return(-1);
    }

    tbx_tbuf_var_init(&tbv);

    tbv.nbytes = op->iov[0].len;
    tbx_tbuf_next(op->buffer, op->boff, &tbv);
    if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
    n = tbx_io_readv(op->fd->sfd, tbv.buffer, tbv.n_iov);

    return(n);
}

//*************************************************************************

gop_op_status_t lio_read_ex_fn_aio(void *arg, int id)
{
    lio_rw_op_t *op = (lio_rw_op_t *)arg;
    lio_fd_t *fd = op->fd;
    lio_config_t *lc = fd->lc;
    ex_tbx_iovec_t *iov = op->iov;
    tbx_tbuf_t *buffer = op->buffer;
    gop_op_status_t status;
    int i, err, size, in_flight, ret_size;
    apr_time_t now;
    double dt;
    ex_off_t t1, t2;

    in_flight = 0;

    status = gop_success_status;
    if (op->n_iov <=0) return(status);

    //** Calculate the size read
    size = iov[0].len;
    for (i=1; i < op->n_iov; i++) {
        tbx_monitor_obj_message(&fd->fh->mo, "lio_read_ex_fn_aio: i=%d offset=" XOT " len=" XOT, i, iov[i].offset, iov[i].len);
        size += iov[i].len;
    }

    t1 = iov[0].len;
    t2 = iov[0].offset;
    log_printf(2, "fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fd->path, op->n_iov, t1, t2);
    tbx_log_flush();

    if (fd == NULL) {
        log_printf(0, "ERROR: Got a null file desriptor\n");
        _op_set_status(status, OP_STATE_FAILURE, -EBADF);
        goto error;
    }

    if (tbx_log_level() > 0) {
        for (i=0; i < op->n_iov; i++) {
            t2 = iov[i].offset+iov[i].len-1;
            log_printf(2, "LFS_READ:START " XOT " " XOT "\n", iov[i].offset, t2);
            log_printf(2, "LFS_READ:END " XOT "\n", t2);
        }
    }

    now = apr_time_now();

    //** Do the read op
    if (fd->fh->is_special) {
        ret_size = special_read(op);
        t1 = 0;
        if (ret_size < 0) {
            ret_size = 0;
            status.op_status = OP_STATE_FAILURE;
        } else {
            status.op_status = OP_STATE_SUCCESS;
        }
        size = ret_size;
        goto done;
    } else if ((err = lio_tier_check_and_handle(op, 0, &in_flight)) != OP_STATE_SUCCESS) {
        if (fd->fh->stream) {
            if ((err = stream_read(op)) != OP_STATE_SUCCESS) {
                err = gop_sync_exec(segment_read(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, buffer, op->boff, lc->timeout));
            }
        } else {
            err = gop_sync_exec(segment_read(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, buffer, op->boff, lc->timeout));
        }
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(2, "END fname=%s seg=" XIDT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), dt);
    tbx_log_flush();

    if (err != OP_STATE_SUCCESS) {
        log_printf(2, "ERROR with read! fname=%s\n", fd->path);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        goto error;
    }

    //** Update the file position to the last read and the stats
    t1 = iov[op->n_iov-1].offset + iov[op->n_iov-1].len;
    t2 = tbx_tbuf_size(buffer);
    ret_size = size;
    if ((op->n_iov == 1) && (t2 < iov[0].len)) {
        t1 = iov[0].offset + t2;  //** This is if we are doing a readahead
        ret_size = t2;
    }
done:
    segment_lock(fd->fh->seg);
    fd->curr_offset = t1;
    fd->tally_ops[0]++;
    fd->tally_bytes[0] += size;
    _lio_dec_in_flight_and_unlock(fd, in_flight);
    //segment_unlock(fd->fh->seg);

    status.error_code = ret_size;
    return(status);

error:  //** Only make it here on an error
    segment_lock(fd->fh->seg);
    fd->tally_error_ops[0]++;
    fd->tally_error_bytes[0] += size;
    _lio_dec_in_flight_and_unlock(fd, in_flight);
    //segment_unlock(fd->fh->seg);
    return(status);
}

//*************************************************************************
//  special_write - Just used for FIFO SOCKET files and only via native or LD_PRELOAD.
//     FUSE directly handles it bypassing the framework
//*************************************************************************

ex_off_t special_write(lio_rw_op_t *op)
{
    ssize_t n;
    tbx_tbuf_var_t tbv;

    if (op->n_iov > 1) {
        log_printf(0, "ERROR: n_iov>1!!! n_iov=%d\n", op->n_iov);
        return(-1);
    }

    tbx_tbuf_var_init(&tbv);

    tbv.nbytes = op->iov[0].len;
    tbx_tbuf_next(op->buffer, op->boff, &tbv);
    if (tbv.n_iov > IOV_MAX) tbv.n_iov = IOV_MAX;  //** Make sure we don't have to many entries
    n = tbx_io_writev(op->fd->sfd, tbv.buffer, tbv.n_iov);

    return(n);
}

//*************************************************************************

gop_op_status_t lio_write_ex_fn_aio(void *arg, int id)
{
    lio_rw_op_t *op = (lio_rw_op_t *)arg;
    lio_fd_t *fd = op->fd;
    lio_config_t *lc = op->fd->fh->lc;
    ex_tbx_iovec_t *iov = op->iov;
    tbx_tbuf_t *buffer = op->buffer;
    gop_op_status_t status;
    int i, err, size, in_flight;
    apr_time_t now;
    double dt;
    ex_off_t t1, t2;

    in_flight = 0;

    if ((fd->mode & LIO_WRITE_MODE) == 0) { _op_set_status(status, OP_STATE_FAILURE, -EINVAL); return(gop_failure_status); }
    if (op->n_iov <=0) return(gop_success_status);

    t1 = iov[0].len;
    t2 = iov[0].offset;
    log_printf(2, "START fname=%s n_iov=%d iov[0].len=" XOT " iov[0].offset=" XOT "\n", fd->path, op->n_iov, t1, t2);
    tbx_log_flush();
    if (tbx_log_level() > 0) {
        for (i=0; i < op->n_iov; i++) {
            t2 = iov[i].offset+iov[i].len-1;
            log_printf(2, "LFS_WRITE:START " XOT " " XOT "\n", iov[i].offset, t2);
            log_printf(2, "LFS_WRITE:END " XOT "\n", t2);
        }
    }

    if (tbx_monitor_enabled()) {
        for (i=1; i < op->n_iov; i++) {
            tbx_monitor_obj_message(&fd->fh->mo, "lio_write_ex_fn_aio: i=%d offset=" XOT " len=" XOT, i, iov[i].offset, iov[i].len);
        }
    }

    now = apr_time_now();

    if (fd->fh->is_special == 0) tbx_atomic_set(fd->fh->modified, 1);  //** Flag it as modified

    //** Do the write op
    if (fd->fh->is_special) {
        size = special_write(op);
        if (size < 0) {
            size = 0;
            err = OP_STATE_FAILURE;
        } else {
            err = OP_STATE_SUCCESS;
        }
        goto done;
    } else if ((err = lio_tier_check_and_handle(op, 1, &in_flight)) != OP_STATE_SUCCESS) {
        if (fd->fh->stream) {
            if ((err = stream_write(op)) != OP_STATE_SUCCESS) {
                err = gop_sync_exec(segment_write(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, op->buffer, op->boff, lc->timeout));
            }
        } else {
            err = gop_sync_exec(segment_write(fd->fh->seg, lc->da, op->rw_hints, op->n_iov, iov, op->buffer, op->boff, lc->timeout));
        }
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(2, "END fname=%s seg=" XIDT " dt=%lf\n", fd->path, segment_id(fd->fh->seg), dt);
    tbx_log_flush();

    if (fd->fh->write_table != NULL) {
        tbx_tbuf_t tb;
        lfs_adler32_t *a32;
        unsigned char *buf = NULL;
        ex_off_t blen = 0;
        ex_off_t bpos = op->boff;
        for (i=0; i < op->n_iov; i++) {
            tbx_type_malloc(a32, lfs_adler32_t, 1);
            a32->offset = iov[i].offset;
            a32->len = iov[i].len;
            a32->adler32 = adler32(0L, Z_NULL, 0);

            //** This is sloppy should use tbuffer_next to do this but this is all going ot be thrown away once we track
            //** down the gridftp plugin issue
            if (blen < a32->len) {
                if (buf != NULL) free(buf);
                blen = a32->len;
                tbx_type_malloc(buf, unsigned char, blen);
            }
            tbx_tbuf_single(&tb, a32->len, (char *)buf);
            tbx_tbuf_copy(buffer, bpos, &tb, 0, a32->len, 1);
            a32->adler32 = adler32(a32->adler32, buf, a32->len);
            segment_lock(fd->fh->seg);
            tbx_list_insert(fd->fh->write_table, &(a32->offset), a32);
            segment_unlock(fd->fh->seg);

            bpos += a32->len;
        }

        if (buf != NULL) free(buf);
    }

    //** Calculate the total bytes written
    size = iov[0].len;
    for (i=1; i< op->n_iov; i++) size += iov[i].len;

done:
    if (err != OP_STATE_SUCCESS) {
        log_printf(1, "ERROR with write! fname=%s\n", fd->path);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        segment_lock(fd->fh->seg);
        fd->tally_error_ops[1]++;
        fd->tally_error_bytes[1] += size;
        _lio_dec_in_flight_and_unlock(fd, in_flight);
        //segment_unlock(fd->fh->seg);

        return(status);
    }

    //** Update the file position to the last write
    segment_lock(fd->fh->seg);
    if (fd->fh->is_special == 0) fd->curr_offset = iov[op->n_iov-1].offset+iov[op->n_iov-1].len;
    fd->tally_ops[1]++;
    fd->tally_bytes[1] += size;
     _lio_dec_in_flight_and_unlock(fd, in_flight);
    //segment_unlock(fd->fh->seg);

    _op_set_status(status, OP_STATE_SUCCESS, size);
    return(status);
}


//==========================================================================
//==========================================================================

//==========================================================================
//  All the work queue related code is below
//==========================================================================

#define IN_FLIGHT_MAX 256
#define OP_READ  0
#define OP_WRITE 1

typedef struct {  //** Backend task
    struct iovec *iov;
    ex_off_t     offset;
    ex_off_t     len;
    int          task_start_index;
    int          task_end_index;
    int          n_iov;
    int          rw_type;
} wq_merged_t;

typedef struct {
    gop_op_generic_t gop;
    gop_op_data_t    dop;
    lio_rw_op_t      *rw;
    wq_context_t     *ctx;
    tbx_iovec_t      *iov;
    int              rw_mode;
    int              n_iov;
} wq_op_t;

typedef struct {    //** R/W working struct
    wq_op_t         **tasks;
    struct iovec      *iov;
    wq_merged_t       *merged;
    int               n_tasks;
    int               n_iov;
    int               n_merged;
} wq_work_t;

struct wq_context_s {    //** Device context
    gop_portal_context_t *pc;
    lio_fd_t          *fd;
    struct iovec      *iov;
    apr_pool_t        *mpool;
    tbx_stack_t       *wq;
    wq_work_t         work[2];
    int max_tasks;
    int n_iov;
    int max_iov;
    apr_thread_t *backend_thread;
    lio_path_tuple_t tuple;
    int shutdown;
    tbx_atomic_int_t op_count;
};

typedef struct {
    apr_thread_t *thread;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    tbx_que_t *pipe;
    int n_parallel;
} wq_global_t;

static wq_global_t *wq_global = NULL;

void wq_ctx_shutdown(wq_context_t *ctx);
void wq_ctx_init(wq_context_t *ctx);


//--------------------------------------------------------------------------
//  Routines for plugging into the GOP framework
//--------------------------------------------------------------------------

// *** Simple submit for the HPortal *****
void _wqp_submit_op(void *arg, gop_op_generic_t *gop)
{
    wq_op_t *op = gop->op->priv;

    log_printf(15, "_wqp_submit_op: gid=%d\n", gop_id(gop));

    tbx_que_put(wq_global->pipe, &op, TBX_QUE_BLOCK);
}

//*************************************************************

static gop_portal_fn_t _wqp_base_portal = {
    .dup_connect_context = NULL,
    .destroy_connect_context = NULL,
    .connect = NULL,
    .close_connection = NULL,
    .sort_tasks = NULL,
    .submit = _wqp_submit_op,
    .sync_exec = NULL
};

//*************************************************************

wq_context_t *wq_context_create(lio_fd_t *fd, int max_tasks)
{
    wq_context_t *ctx;

    tbx_type_malloc_clear(ctx, wq_context_t, 1);

    //** We need an FD that will persist for as long as the CTX is in use.
    //** All the FD fields will peresist as long as an FD is using the CTX.
    //** The only field that's transient is the path so it's duped.
    tbx_type_malloc(ctx->fd, lio_fd_t, 1);
    *ctx->fd = *fd;
    ctx->fd->id = 1;  //** This flags that the work is being done by a WQ vs normail I/O for reporting
    ctx->fd->path = strdup(fd->path);

    ctx->max_tasks = (max_tasks <= 0) ? IN_FLIGHT_MAX : max_tasks;
    wq_ctx_init(ctx);
    ctx->pc = gop_hp_context_create(&_wqp_base_portal, "WQ");  //** Really just used for the submit

    return(ctx);
}

//*************************************************************

void wq_context_destroy(wq_context_t *ctx)
{
    wq_ctx_shutdown(ctx);
    gop_hp_context_destroy(ctx->pc);
    free(ctx->fd->path);
    free(ctx->fd);
    free(ctx);
}

//*************************************************************

void _wqp_op_free(gop_op_generic_t *gop, int mode)
{
    wq_op_t *op = gop->op->priv;

    gop_generic_free(gop, OP_FINALIZE);  //** I free the actual op
    if (mode == OP_DESTROY) {
        free(op->rw);
        free(gop->free_ptr);
    }
}

//*************************************************************

gop_op_generic_t *wq_op_new(wq_context_t *ctx, lio_rw_op_t *rw_op, int rw_mode)
{
    wq_op_t *op;
    gop_op_generic_t *gop;
    tbx_tbuf_var_t tv;
    gop_op_status_t default_status = {OP_STATE_ERROR, -3000};

    tbx_type_malloc_clear(op, wq_op_t, 1);
    op->ctx = ctx;
    op->rw = rw_op;
    op->rw_mode = rw_mode;

    //** These are sanity checked and gracefully handled earlier
    tbx_tbuf_var_init(&tv);
    tv.nbytes = tbx_tbuf_size(rw_op->buffer) - rw_op->boff;
    FATAL_UNLESS(tbx_tbuf_next(rw_op->buffer, rw_op->boff, &tv) == TBUFFER_OK);
    op->n_iov = tv.n_iov;
    op->iov = tv.buffer;

    //** Now munge the pointers
    gop = &(op->gop);
    gop_init(gop);
    gop->op = &(op->dop);
    gop->op->priv = op;
    gop->type = Q_TYPE_OPERATION;
    op->dop.priv = op;
    op->dop.pc = ctx->pc;
    gop->base.free = _wqp_op_free;
    gop->free_ptr = op;
    gop->base.pc = ctx->pc;
    gop->base.status = default_status;
    return(gop);
}

//--------------------------------------------------------------------------
// Actual Work Queue code
//--------------------------------------------------------------------------

// *************************************************************************
//  wq_compare - Sort comparison function
// *************************************************************************

int wq_compare(const void *p1, const void *p2)
{
    wq_op_t *t1, *t2;

    t1 = *(wq_op_t **)p1;
    t2 = *(wq_op_t **)p2;

    if (t1->rw->iov->offset < t2->rw->iov->offset) {
        return(-1);
    } else if (t1->rw->iov->offset == t2->rw->iov->offset) {
        return(0);
    }

    return(1);
}

//***********************************************************************
// wq_add_task - Adds a task for execution
//***********************************************************************

void wq_add_task(wq_op_t *op)
{
    tbx_que_put(wq_global->pipe, &op, TBX_QUE_BLOCK);
}

//***********************************************************************
// wq_fetch_tasks - Fetches the incoming tasks for execution
//***********************************************************************

int wq_fetch_tasks(apr_hash_t *table, tbx_que_t *que)
{
    wq_op_t *t;
    int n;


    n = 0;
    while (tbx_que_get(que, &t, (n == 0) ? TBX_QUE_BLOCK : 0) == 0) {
        if (t == NULL) break;
        if (t == (void *)1) return(-(n+1));  //** Kick out

        //** Check if we need to add it to the processing list
        if (apr_hash_get(table, &(t->ctx), sizeof(t->ctx)) == NULL) {
            apr_hash_set(table, &(t->ctx), sizeof(t->ctx), t->ctx);
        }
        tbx_stack_push(t->ctx->wq, t);
        n++;
    }

    return(n);
}

//***********************************************************************
// wq_ctx_fetch_tasks - Fetches the incoming tasks for an individual FD
//***********************************************************************

int wq_ctx_fetch_tasks(wq_context_t *ctx)
{
    wq_op_t *t;
    int n, i, n_iov;

    n = 0;
    n_iov = 0;
    ctx->work[0].n_tasks = 0;
    ctx->work[1].n_tasks = 0;
    for (n=0; n < ctx->max_tasks; n++) {
        t = tbx_stack_pop_bottom(ctx->wq);

        if (t == NULL) break;

        i = t->rw_mode;
        ctx->work[i].tasks[ctx->work[i].n_tasks] = t;
        ctx->work[i].n_tasks++;
        n_iov += t->n_iov;
    }

    //** Make sure we have enough IOV space to process everything
    if (n_iov > ctx->max_iov) {
        free(ctx->iov);
        ctx->max_iov = 1.5* n_iov;
        tbx_type_malloc_clear(ctx->iov, struct iovec, ctx->max_iov);
    }

    return(tbx_stack_count(ctx->wq));
}

//***********************************************************************
// wq_new_merged - Starts a new merged task
//***********************************************************************

wq_merged_t wq_new_merged(wq_context_t *ctx, wq_op_t *t, int index, int rw_type)
{
    wq_merged_t m;

    memset(&m, 0, sizeof(m));

    m.rw_type = rw_type;
    m.task_start_index = index;
    m.task_end_index = index;
    m.iov = &ctx->iov[ctx->n_iov];
    memcpy(m.iov, t->iov, t->n_iov*sizeof(struct iovec));
    ctx->n_iov += t->n_iov;
    m.n_iov = t->n_iov;
    m.offset = t->rw->iov->offset;
    m.len = t->rw->iov->len;

    return(m);
}

//***********************************************************************
// wq_sort_and_merge_tasks
//***********************************************************************

void wq_sort_and_merge_tasks(wq_context_t *ctx)
{
    int rw, i, k, j;
    ex_off_t end;
    wq_work_t *w;
    wq_merged_t *m;
    wq_op_t *t;

    ctx->n_iov = 0;
    for (rw=0; rw<2; rw++) {
        w = &ctx->work[rw];

        //** Sort the index table
        qsort(w->tasks, w->n_tasks, sizeof(wq_op_t *), wq_compare);

        //** Now cycle through them and do the merging
        w->n_merged = 0;
        if (w->n_tasks > 0) {
            t = w->tasks[0];
            m = &w->merged[w->n_merged];
            *m = wq_new_merged(ctx, t, 0, rw);
            end = t->rw->iov->offset + t->rw->iov->len;
            log_printf(10, "rw=%d i=%d n_merged=%d off=" XOT " end=" XOT " len=" XOT " gid=%d\n", rw, 0, w->n_merged, t->rw->iov->offset, end, t->rw->iov->len, gop_get_id(&t->gop));
            w->n_merged++;

            for (i=1; i < w->n_tasks; i++) {
                t = w->tasks[i];
                if (end != t->rw->iov->offset) { //** offset doesn't match up with previous
                    k = w->n_merged;
                    j = 0;
                    m = &w->merged[w->n_merged];
                    *m = wq_new_merged(ctx, t, i, rw);
                    w->n_merged++;
                } else {
                    j=m->n_iov;
                    memcpy(&m->iov[m->n_iov], t->iov, t->n_iov * sizeof(struct iovec));
                    ctx->n_iov += t->n_iov;
                    m->n_iov += t->n_iov;
                    m->task_end_index = i;
                    m->len += t->rw->iov->len;
                    k = w->n_merged-1;
                }
                end = t->rw->iov->offset + t->rw->iov->len;
                log_printf(10, "rw=%d i=%d n_merged=%d off=" XOT " end=" XOT " len=" XOT " [ogid=%d m=%d n_iov_slot=%d]\n", rw, i, w->n_merged, t->rw->iov->offset, end, t->rw->iov->len, gop_get_id(&t->gop), k, j);
            }
        }
    }
}

//***********************************************************************
// wq_execute_tasks -Execute the tasks and process the results
//***********************************************************************

void wq_execute_tasks(wq_context_t *ctx)
{
    int rw, i;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    wq_work_t *w;
    wq_merged_t *m;
    wq_op_t *t;
    lio_rw_op_t *op;

    q = gop_opque_new();
    for (rw=0; rw<2; rw++) {
        w = &ctx->work[rw];

        for (i=0; i<w->n_merged; i++) {
            m = &w->merged[i];
            tbx_type_malloc_clear(op, lio_rw_op_t, 1);
            op->fd = ctx->fd;
            op->n_iov = 1;
            op->iov = &(op->iov_dummy);
            op->buffer = &(op->buffer_dummy);
            op->boff = 0;
            op->rw_hints = NULL;
            tbx_tbuf_vec(&(op->buffer_dummy), m->len, m->n_iov, m->iov);
            ex_iovec_single(op->iov, m->offset, m->len);

            if (rw == OP_READ) {
                gop = lio_read_ex_gop_aio(op);
            } else {
                gop = lio_write_ex_gop_aio(op);
            }
            log_printf(10, "rw=%d m=%d off=" XOT " len=" XOT " gid=%d\n", rw, i, m->offset, m->len, gop_get_id(gop));
            gop_set_private(gop, m);
            gop_set_myid(gop, rw);
            gop_opque_add(q, gop);
        }
    }

    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        m = gop_get_private(gop);
        rw = gop_get_myid(gop);
        log_printf(10, "rw=%d off=" XOT " len=" XOT " status=%d SUCCESS=%d\n", rw, m->offset, m->len, status.op_status, OP_STATE_SUCCESS);

        for (i=m->task_start_index; i<=m->task_end_index; i++) {
            t = ctx->work[rw].tasks[i];
            if (status.op_status == OP_STATE_SUCCESS) {
                status.error_code = ctx->work[rw].tasks[i]->rw->iov->len;  //** Store the bytes read corresponding to the task.
                segment_lock(t->rw->fd->fh->seg);
                t->rw->fd->curr_offset = ctx->work[rw].tasks[i]->rw->iov->offset + ctx->work[rw].tasks[i]->rw->iov->len;
                t->rw->fd->tally_ops[rw]++;
                t->rw->fd->tally_bytes[rw] += ctx->work[rw].tasks[i]->rw->iov->len;
                segment_unlock(t->rw->fd->fh->seg);
            } else {
                status.error_code = 0;
                segment_lock(t->rw->fd->fh->seg);
                t->rw->fd->tally_error_ops[rw]++;
                t->rw->fd->tally_error_bytes[rw] += ctx->work[rw].tasks[i]->rw->iov->len;
                segment_unlock(t->rw->fd->fh->seg);
            }
            gop_mark_completed(&t->gop, status);
        }
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);
}

//***********************************************************************
//  wq_ctx_process_fn - Processes all the pending taks for the FD
//***********************************************************************

gop_op_status_t wq_ctx_process_fn(void *arg, int id)
{
    wq_context_t *ctx = (wq_context_t *)arg;
    int more;

    log_printf(10, "START fname=%s CTX=%p\n", ctx->fd->path, ctx);
    do {
        //** We get back how many tasks are pending.  This needed because
        //** after the tasks execute the file could be closed making the
        //** ctx invalid for checking
        more = wq_ctx_fetch_tasks(ctx);
        log_printf(10, "CTX=%p more=%d\n", ctx, more);
        wq_sort_and_merge_tasks(ctx);
        wq_execute_tasks(ctx);
    } while (more > 0);
    log_printf(10, "END CTX=%p\n", ctx);

    return(gop_success_status);
}

//***********************************************************************
// wq_process - Processes all the pending WQ tasks
//***********************************************************************

void wq_process(wq_global_t *wq, apr_hash_t *table, apr_pool_t *mpool)
{
    apr_hash_index_t *hi;
    wq_context_t *ctx;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    int n;

    q = gop_opque_new();
    n = 0;
    for (hi=apr_hash_first(mpool, table); hi; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, NULL, (void **)&ctx);
        apr_hash_set(table, &ctx, sizeof(ctx), NULL);

        if (n > wq->n_parallel) {
            gop = opque_waitany(q);
            gop_free(gop, OP_DESTROY);
        }

        gop_opque_add(q, gop_tp_op_new(lio_gc->tpc_unlimited, NULL, wq_ctx_process_fn, (void *)ctx, NULL, 0));
    }

    opque_waitall(q);
    gop_opque_free(q, OP_DESTROY);

    return;
}

//***********************************************************************
//  wq_backend_thread - Backend executor for all WQ FDs
//***********************************************************************

void *wq_backend_thread(apr_thread_t *th, void *data)
{
    wq_global_t *wq = (wq_global_t *)data;
    apr_pool_t *mpool;
    apr_hash_t *table;;
    int finished;

    tbx_monitor_thread_create(MON_MY_THREAD, "wq_backend_thread");

    apr_pool_create(&mpool, NULL);
    table = apr_hash_make(mpool);

    finished = 0;
    while (finished != -1) {
        finished = wq_fetch_tasks(table, wq->pipe);
        if (finished != 0) wq_process(wq, table, mpool);
    }

    apr_pool_destroy(mpool);

    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
//  wq_ctx_init - Configures the context
//***********************************************************************

void wq_ctx_init(wq_context_t *ctx)
{
    int i;

    ctx->wq = tbx_stack_new();
    ctx->max_iov = 2*ctx->max_tasks;
    tbx_type_malloc_clear(ctx->iov, struct iovec, ctx->max_iov);

    for (i=0; i<2; i++) {
        tbx_type_malloc_clear(ctx->work[i].tasks, wq_op_t *, ctx->max_tasks);
        tbx_type_malloc_clear(ctx->work[i].merged, wq_merged_t, ctx->max_tasks);
    }

    apr_pool_create(&(ctx->mpool), NULL);
}

//***********************************************************************
//  wq_ctx_shutdown - Shuts down the backend
//***********************************************************************

void wq_ctx_shutdown(wq_context_t *ctx)
{
    tbx_stack_free(ctx->wq, 0);

    free(ctx->iov);
    free(ctx->work[OP_READ].tasks); free(ctx->work[OP_WRITE].tasks);
    free(ctx->work[OP_READ].merged); free(ctx->work[OP_WRITE].merged);

    apr_pool_destroy(ctx->mpool);
}

//***********************************************************************
// lio_wq_startup - Starts up the WQ background process
//***********************************************************************

void lio_wq_startup(int n_parallel)
{
    tbx_type_malloc_clear(wq_global, wq_global_t, 1);
    wq_global->n_parallel = n_parallel;

    wq_global->pipe = tbx_que_create(10000, sizeof(wq_op_t *));

    apr_pool_create(&(wq_global->mpool), NULL);
    apr_thread_mutex_create(&(wq_global->lock), APR_THREAD_MUTEX_DEFAULT, wq_global->mpool);

    // ** Launch the backend thread
    tbx_thread_create_assert(&(wq_global->thread), NULL, wq_backend_thread,
                                 (void *)wq_global, lio_gc->mpool);
}

//***********************************************************************
// lio_wq_shutdown - Shuts down the WQ backend processor
//***********************************************************************

void lio_wq_shutdown()
{
    wq_op_t *op;
    apr_status_t value;

    //** Notify the backend thread
    op = (void *)1;
    tbx_que_put(wq_global->pipe, &op, TBX_QUE_BLOCK);

    //** Wait for it to exit
    apr_thread_join(&value, wq_global->thread);

    //** Close the pipe
    if (wq_global->pipe) tbx_que_destroy(wq_global->pipe);

    //** Cleanup the lock
    apr_thread_mutex_destroy(wq_global->lock);
    apr_pool_destroy(wq_global->mpool);

    //** And free the structure
    free(wq_global);
}
