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
// Routines for managing a disk_cache segment
//***********************************************************************

#define _log_module_index 162

#include <gop/gop.h>
#include <gop/portal.h>
#include <gop/tp.h>
#include <libgen.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
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
#include "segment/disk_cache.h"
#include "service_manager.h"


// Forward declaration
const lio_segment_vtable_t lio_disk_cache_vtable;

typedef struct {
    char *fname;
    char *qname;
    gop_thread_pool_context_t *tpc;
    tbx_atomic_int_t hard_errors;
    tbx_atomic_int_t soft_errors;
    tbx_atomic_int_t write_errors;

    lio_segment_t *child_seg;
} segdc_priv_t;

typedef struct {
    lio_segment_t *seg;
    tbx_tbuf_t *buffer;
    ex_tbx_iovec_t *iov;
    ex_off_t  boff;
    ex_off_t len;
    int n_iov;
    int timeout;
    int mode;
} segdc_rw_op_t;

typedef struct {
    lio_segment_t *seg;
    int new_size;
} segdc_multi_op_t;

typedef struct {
    lio_segment_t *sseg;
    lio_segment_t *dseg;
    int copy_data;
} segdc_clone_t;

//***********************************************************************
// segdc_rw_func - Read/Write from a Disk Cache segment
//***********************************************************************

gop_op_status_t segdc_rw_func(void *arg, int id)
{
    segdc_rw_op_t *srw = (segdc_rw_op_t *)arg;
    segdc_priv_t *s = (segdc_priv_t *)srw->seg->priv;
    ex_off_t bleft, boff;
    size_t nbytes, blen;
    tbx_tbuf_var_t tbv;
    int i, err_cnt;
    gop_op_status_t err;

    FILE *fd = fopen(s->fname, "r+");
    if (fd == NULL) fd = fopen(s->fname, "w+");

    log_printf(15, "segdc_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " mode=%d\n", tbx_atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, srw->mode);
    tbx_log_flush();
    tbx_tbuf_var_init(&tbv);

    blen = srw->len;
    boff = srw->boff;
    bleft = blen;
    err_cnt = 0;
    for (i=0; i<srw->n_iov; i++) {
        fseeko(fd, srw->iov[i].offset, SEEK_SET);
        bleft = srw->iov[i].len;
        err = gop_success_status;
        while ((bleft > 0) && (err.op_status == OP_STATE_SUCCESS)) {
            tbv.nbytes = bleft;
            tbx_tbuf_next(srw->buffer, boff, &tbv);
            blen = tbv.nbytes;
            if (srw->mode == 0) {
                nbytes = readv(fileno(fd), tbv.buffer, tbv.n_iov);
            } else {
                nbytes = writev(fileno(fd), tbv.buffer, tbv.n_iov);
            }

            int ib = blen;
            int inb = nbytes;
            log_printf(15, "segdc_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " blen=%d nbytes=%d err_cnt=%d\n", tbx_atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, ib, inb, err_cnt);
            tbx_log_flush();

            if (nbytes > 0) {
                boff = boff + nbytes;
                bleft = bleft - nbytes;
            } else {
                err = gop_failure_status;
                err_cnt++;
            }
        }
    }

    err =  (err_cnt > 0) ? gop_failure_status : gop_success_status;

    if (err_cnt > 0) {  //** Update the error count if needed
        log_printf(15, "segdc_rw_func: ERROR tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", tbx_atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
        tbx_atomic_inc(s->hard_errors);
        if (srw->mode != 0) tbx_atomic_inc(s->write_errors);
    }

    log_printf(15, "segdc_rw_func: tid=%d fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", tbx_atomic_thread_id, s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
    tbx_log_flush();
    fclose(fd);
    return(err);
}

//***********************************************************************
// segdc_read - Read from a file segment
//***********************************************************************

gop_op_generic_t *segdc_read(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;
    segdc_rw_op_t *srw;

    tbx_type_malloc_clear(srw, segdc_rw_op_t, 1);

    srw->seg = seg;
    srw->n_iov = n_iov;
    srw->iov = iov;
    srw->boff = boff;
    srw->timeout = timeout;
    srw->buffer = buffer;
    srw->mode = 0;

    return(gop_tp_op_new(s->tpc, s->qname, segdc_rw_func, (void *)srw, free, 1));
}

//***********************************************************************
// segdc_write - Writes to a linear segment
//***********************************************************************

gop_op_generic_t *segdc_write(lio_segment_t *seg, data_attr_t *da, lio_segment_rw_hints_t *rw_hints, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;
    segdc_rw_op_t *srw;

    tbx_type_malloc_clear(srw, segdc_rw_op_t, 1);

    srw->seg = seg;
    srw->n_iov = n_iov;
    srw->iov = iov;
    srw->boff = boff;
    srw->timeout = timeout;
    srw->buffer = buffer;
    srw->mode = 1;

    return(gop_tp_op_new(s->tpc, s->qname, segdc_rw_func, (void *)srw, free, 1));
}

//***********************************************************************
// segdc_multi_func - PErforms the truncate and remove ops for a file segment
//***********************************************************************

gop_op_status_t segdc_multi_func(void *arg, int id)
{
    segdc_multi_op_t *cmd = (segdc_multi_op_t *)arg;
    segdc_priv_t *s = (segdc_priv_t *)cmd->seg->priv;
    int err;
    gop_op_status_t status = gop_success_status;

    if (cmd->new_size >= 0) {  //** Truncate operation
        err = truncate(s->fname, cmd->new_size);
        if (err != 0) status = gop_failure_status;
    } else {  //** REmove op
        if (s->fname != NULL) {
            remove(s->fname);
        }
    }

    return(status);
}

//***********************************************************************
// segdc_remove - DECrements the ref counts for the segment which could
//     result in the data being removed.
//***********************************************************************

gop_op_generic_t *segdc_remove(lio_segment_t *seg, data_attr_t *da, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;
    segdc_multi_op_t *cmd;

    tbx_type_malloc_clear(cmd, segdc_multi_op_t, 1);

    cmd->seg = seg;
    cmd->new_size = -1;

    return(gop_tp_op_new(s->tpc, s->qname, segdc_multi_func, (void *)cmd, free, 1));
}

//***********************************************************************
// segdc_truncate - Expands or contracts a segment
//***********************************************************************

gop_op_generic_t *segdc_truncate(lio_segment_t *seg, data_attr_t *da, ex_off_t new_size, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;
    segdc_multi_op_t *cmd;

    if (new_size < 0) return(gop_dummy(gop_success_status));  //** Reserve call which we ignore

    tbx_type_malloc_clear(cmd, segdc_multi_op_t, 1);

    cmd->seg = seg;
    cmd->new_size = new_size;

    return(gop_tp_op_new(s->tpc, s->qname, segdc_multi_func, (void *)cmd, free, 1));
}


//***********************************************************************
// segdc_inspect - Inspect function. Simple pass-through
//***********************************************************************

gop_op_generic_t *segdc_inspect(lio_segment_t *seg, data_attr_t *da, tbx_log_fd_t *ifd, int mode, ex_off_t bufsize, lio_inspect_args_t *args, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_inspect(s->child_seg, da, ifd, mode, bufsize, args, timeout));
}

//***********************************************************************
// segdc_flush - Flushes a segment. Simple pass-through
//***********************************************************************

gop_op_generic_t *segdc_flush(lio_segment_t *seg, data_attr_t *da, ex_off_t lo, ex_off_t hi, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;
    return(segment_flush(s->child_seg, da, lo, hi, timeout));
}

//***********************************************************************
// segdc_signature - Generates the segment signature. Just a pass-through
//***********************************************************************

int segdc_signature(lio_segment_t *seg, char *buffer, int *used, int bufsize)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_signature(s->child_seg, buffer, used, bufsize));
    return(0);
}

//***********************************************************************
// segdc_clone - Clones a segment. Just a pass-through
//***********************************************************************

gop_op_generic_t *segdc_clone(lio_segment_t *seg, data_attr_t *da, lio_segment_t **clone_seg, int mode, void *attr, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_clone(s->child_seg, da, clone_seg, mode, attr, timeout));
}


//***********************************************************************
// segdc_size - Returns the segment size.
//***********************************************************************

ex_off_t segdc_size(lio_segment_t *seg)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_size(s->child_seg));
}

//***********************************************************************
// segdc_block_size - Returns the segment block size. Simple pass-through
//***********************************************************************

ex_off_t segdc_block_size(lio_segment_t *seg, int btype)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_block_size(s->child_seg, btype));
}

//***********************************************************************
// segdc_serialize -Convert the segment to a more portable format. Simple pass-through
//***********************************************************************

int segdc_serialize(lio_segment_t *seg, lio_exnode_exchange_t *exp)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_serialize(s->child_seg, exp));
}

//***********************************************************************
// segment_disk_cache_set_child - Set the child segment
//***********************************************************************

void segment_disk_cache_set_child(lio_segment_t *seg, lio_segment_t *child_seg)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    s->child_seg = child_seg;
}

//***********************************************************************
// segdc_deserialize - This should never be called so flag it if it does
//***********************************************************************

int segdc_deserialize(lio_segment_t *seg, ex_id_t id, lio_exnode_exchange_t *exp)
{
    log_printf(0, "ERROR: This shold never be called.  Call segment_disk_cache_set_child() instead\n"); tbx_log_flush();
    return(-1);
}


//***********************************************************************
// segdc_destroy - Destroys a linear segment struct (not the data)
//***********************************************************************

void segdc_destroy(tbx_ref_t *ref)
{
    tbx_obj_t *obj = container_of(ref, tbx_obj_t, refcount);
    lio_segment_t *seg = container_of(obj, lio_segment_t, obj);

    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    if (s->fname != NULL) free(s->fname);
    if (s->qname != NULL) free(s->qname);

    free(s);

    ex_header_release(&(seg->header));

    free(seg);
}


//***********************************************************************
// segment_disk_cache_create - Creates a file segment
//***********************************************************************

lio_segment_t *segment_disk_cache_create(void *arg)
{
    lio_service_manager_t *es = (lio_service_manager_t *)arg;
    segdc_priv_t *s;
    lio_segment_t *seg;
    char qname[512];

    //** Make the space
    tbx_type_malloc_clear(seg, lio_segment_t, 1);
    tbx_type_malloc_clear(s, segdc_priv_t, 1);
    tbx_obj_init(&seg->obj, (tbx_vtable_t *) &lio_disk_cache_vtable);
    s->fname = NULL;

    generate_ex_id(&(seg->header.id));
    seg->header.type = SEGMENT_TYPE_DISK_CACHE;

    s->tpc = lio_lookup_service(es, ESS_RUNNING, ESS_TPC_UNLIMITED);
    snprintf(qname, sizeof(qname), XIDT HP_HOSTPORT_SEPARATOR "1" HP_HOSTPORT_SEPARATOR "0" HP_HOSTPORT_SEPARATOR "0", seg->header.id);
    s->qname = strdup(qname);

    seg->priv = s;
    seg->ess = es;
    return(seg);
}

//***********************************************************************
// segment_disk_cache_load - Loads a disk cache segment from ini/ex3
//***********************************************************************

lio_segment_t *segment_disk_cache_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex)
{
    lio_segment_t *seg = segment_disk_cache_create(arg);
    if (segment_deserialize(seg, id, ex) != 0) {
        tbx_obj_put(&seg->obj);
        seg = NULL;
    }
    return(seg);
}

const lio_segment_vtable_t lio_disk_cache_vtable = {
        .base.name = SEGMENT_TYPE_DISK_CACHE,
        .base.free_fn = segdc_destroy,
        .read = segdc_read,
        .write = segdc_write,
        .inspect = segdc_inspect,
        .truncate = segdc_truncate,
        .remove = segdc_remove,
        .flush = segdc_flush,
        .clone = segdc_clone,
        .signature = segdc_signature,
        .size = segdc_size,
        .block_size = segdc_block_size,
        .serialize = segdc_serialize,
        .deserialize = segdc_deserialize,
};
