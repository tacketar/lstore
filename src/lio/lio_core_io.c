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

#define _log_module_index 189

#include <fcntl.h>

#include <apr_time.h>
#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/direct_io.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/transfer_buffer.h>
#include <tbx/type_malloc.h>
#include <zlib.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/compare.h"
#include "ex3/types.h"
#include "lio.h"
#include "os.h"

//***********************************************************************
// Core LIO I/O functionality
//***********************************************************************

#define LFH_KEY_INODE  0
#define LFH_KEY_EXNODE 1
#define LFH_KEY_DATA   2
#define LFH_NKEYS      3

gop_op_status_t lio_read_ex_fn_aio(void *op, int id);
gop_op_status_t lio_write_ex_fn_aio(void *op, int id);
gop_op_generic_t *lio_read_ex_gop_aio(lio_rw_op_t *op);
gop_op_generic_t *lio_write_ex_gop_aio(lio_rw_op_t *op);

gop_op_status_t lio_read_ex_fn_wq(void *op, int id);
gop_op_status_t lio_write_ex_fn_wq(void *op, int id);
gop_op_generic_t *lio_read_ex_gop_wq(lio_rw_op_t *op);
gop_op_generic_t *lio_write_ex_gop_wq(lio_rw_op_t *op);

static char *_lio_fh_keys[] = { "system.inode", "system.exnode", "system.exnode.data" };

//***********************************************************************
// Core LIO R/W functionality
//***********************************************************************

//***********************************************************************
// lio_open_files_info_fn - Prints info on open files
//***********************************************************************

void lio_open_files_info_fn(void *arg, FILE *fd)
{
    lio_config_t *lc = (lio_config_t *)arg;
    lio_file_handle_t *fh;
    tbx_list_iter_t it;
    ex_id_t *fid;
    char ppbuf[100];
    double d;

    fprintf(fd, "LIO Open File list ----------------------\n");

    lio_lock(lc);
    it = tbx_list_iter_search(lc->open_index, NULL, 0);
    tbx_list_next(&it, (tbx_list_key_t *)&fid, (tbx_list_data_t **)&fh);
    while (fh != NULL) {
        d = segment_size(fh->seg);
        fprintf(fd, " seg=" XIDT " fname=%s  size=%s  cnt=%d\n", fh->vid, fh->fname, tbx_stk_pretty_print_double_with_scale(1000, d, ppbuf), fh->ref_count);
        tbx_list_next(&it, (tbx_list_key_t *)&fid, (tbx_list_data_t **)&fh);
    }
    lio_unlock(lc);

    fprintf(fd, "\n");
}

//***********************************************************************
// lio_fopen_flags - Handles fopen type string flags and converts them
//   to an integer which can be passed to lio_open calls.
//   On error -1 is returned
//***********************************************************************

int lio_fopen_flags(char *sflags)
{
    int mode = -1;

    if (strcmp(sflags, "r") == 0) {
        mode = LIO_READ_MODE;
    } else if (strcmp(sflags, "r+") == 0) {
        mode = LIO_RW_MODE;
    } else if (strcmp(sflags, "w") == 0) {
        mode = LIO_WRITE_MODE | LIO_TRUNCATE_MODE | LIO_CREATE_MODE;
    } else if (strcmp(sflags, "w+") == 0 ) {
        mode = LIO_RW_MODE | LIO_TRUNCATE_MODE | LIO_CREATE_MODE;
    } else if (strcmp(sflags, "a") == 0) {
        mode = LIO_WRITE_MODE | LIO_CREATE_MODE | LIO_APPEND_MODE;
    } else if (strcmp(sflags, "w+") == 0) {
        mode = LIO_RW_MODE | LIO_CREATE_MODE | LIO_APPEND_MODE;
    }

    return(mode);
}

//***********************************************************************
// lio_encode_error_counts - Encodes the error counts for a setattr call
//
//  The keys, val, and v_size arrays should have 3 elements. Buf is used
//  to store the error numbers.  It's assumed to have at least 3*32 bytes.
//  mode is used to determine how to handle 0 error values
//  (-1=remove attr, 0=no update, 1=store 0 value).
//  On return the number of attributes stored is returned.
//***********************************************************************

int lio_encode_error_counts(lio_segment_errors_t *serr, char **key, char **val, char *buf, int *v_size, int mode)
{
    char *ekeys[] = { "system.hard_errors", "system.soft_errors",  "system.write_errors" };
    int err[3];
    int i, n, k;

    k = n = 0;

    //** So I can do this in a loop
    err[0] = serr->hard;
    err[1] = serr->soft;
    err[2] = serr->write;

    for (i=0; i<3; i++) {
        if ((err[i] != 0) || (mode == 1)) {  //** Always store
            val[n] = &(buf[k]);
            k += snprintf(val[n], 32, "%d", err[i]) + 1;
            v_size[n] = strlen(val[n]);
            key[n] = ekeys[i];
            n++;
        } else if (mode == -1) { //** Remove the attribute
            val[n] = NULL;
            v_size[n] = -1;
            key[n] = ekeys[i];
            n++;
        }
    }

    return(n);
}

//***********************************************************************
// lio_get_error_counts - Gets the error counts
//***********************************************************************

void lio_get_error_counts(lio_config_t *lc, lio_segment_t *seg, lio_segment_errors_t *serr)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(segment_inspect(seg, lc->da, lio_ifd, INSPECT_HARD_ERRORS, 0, NULL, 1));
    serr->hard = status.error_code;

    status = gop_sync_exec_status(segment_inspect(seg, lc->da, lio_ifd, INSPECT_SOFT_ERRORS, 0, NULL, 1));
    serr->soft = status.error_code;

    status =gop_sync_exec_status(segment_inspect(seg, lc->da, lio_ifd, INSPECT_WRITE_ERRORS, 0, NULL, 1));
    serr->write = status.error_code;

    return;
}

//***********************************************************************
// lio_update_error_count - Updates the error count attributes if needed
//***********************************************************************

int lio_update_error_counts(lio_config_t *lc, lio_creds_t *creds, char *path, lio_segment_t *seg, int mode)
{
    char *keys[3];
    char *val[3];
    char buf[128];
    int v_size[3];
    int n;
    lio_segment_errors_t serr;

    lio_get_error_counts(lc, seg, &serr);
    n = lio_encode_error_counts(&serr, keys, val, buf, v_size, mode);
    if (n > 0) {
        lio_multiple_setattr_op(lc, creds, path, NULL, keys, (void **)val, v_size, n);
    }

    return(serr.hard);
}

//***********************************************************************
// lio_update_exnode_attrs - Updates the exnode and system.error_* attributes
//***********************************************************************

int lio_update_exnode_attrs(lio_fd_t *fd, lio_segment_errors_t *serr)
{
    ex_off_t ssize;
    char buffer[32];
    char *key[7] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data", "system.exnode.data", NULL, NULL, NULL };
    char *val[7];
    lio_exnode_exchange_t *exp;
    int n, err, ret, v_size[7];
    lio_segment_errors_t my_serr;
    char ebuf[128];

    ret = 0;

    //** Get any errors that may have occured if needed
    if (serr == NULL) {
        serr = &my_serr;
        lio_get_error_counts(fd->lc, fd->fh->seg, serr);
    }

    //* Get the size and optionally set the data attribute
    if (fd->fh->data_size < 0) {  //** Data stored in segment
        ssize = segment_size(fd->fh->seg);
        val[3] = NULL;   //** Wipe the data attribute
        v_size[3] = -1;
    } else {   //** Data is stored as an attribute
        ssize = fd->fh->data_size;
        val[3] = fd->fh->data;
        v_size[3] = ssize;
    }

    //** Encode the size
    sprintf(buffer, XOT, ssize);
    val[1] = buffer;
    v_size[1] = strlen(val[1]);

    //** And update the modify timestamp
    val[2] = NULL;
    v_size[2] = 0;

    n = 4;
    n += lio_encode_error_counts(serr, &(key[n]), &(val[n]), ebuf, &(v_size[n]), 0);
    if ((serr->hard>0) || (serr->soft>0) || (serr->write>0)) {
        log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fd->path, serr->hard, serr->soft, serr->write);
        ret += 1;
    }

    //** Serialize the exnode. This is done after the error counts in case the segment does some caching
    exp = lio_exnode_exchange_create(EX_TEXT);
    lio_exnode_serialize(fd->fh->ex, exp);

    //** Update the exnode
    val[0] = exp->text.text;
    v_size[0] = strlen(val[0]);


    err = lio_multiple_setattr_op(fd->lc, fd->creds, fd->path, NULL, key, (void **)val, v_size, n);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR updating exnode+attrs! fname=%s\n", fd->path);
        ret += 2;
    }

    lio_exnode_exchange_destroy(exp);

    return(ret);
}

//*****************************************************************
// lio_store_and_release_adler32 - Takes all the adler32 structures
//    and coalesces them into a single adler32 and stores it in the
//    user.lfs_adler32 file attribute.
//    It also detroys the write_table
//*****************************************************************

void lio_store_and_release_adler32(lio_config_t *lc, lio_creds_t *creds, tbx_list_t *write_table, char *fname)
{
    tbx_list_iter_t it;
    ex_off_t next, missing, overlap, dn, nbytes, pend;
    uLong cksum;
    unsigned int aval;
    lfs_adler32_t *a32;
    tbx_stack_t *stack;
    ex_off_t *aoff;
    char value[256];
    stack = tbx_stack_new();
    it = tbx_list_iter_search(write_table, 0, 0);
    cksum = adler32(0L, Z_NULL, 0);
    missing = next = overlap = nbytes = 0;
    while (tbx_list_next(&it, (tbx_list_key_t **)&aoff, (tbx_list_data_t **)&a32) == 0) {
        aval = a32->adler32;
        pend = a32->offset + a32->len - 1;
        tbx_stack_push(stack, a32);

        if (a32->offset != next) {
            dn = a32->offset - next;
            log_printf(1, "fname=%s a32=%08x off=" XOT " end=" XOT " nbytes=" XOT " OOPS dn=" XOT "\n", fname, aval, a32->offset, pend, a32->len, dn);
            if (dn < 0) {
                overlap -= dn;
            } else {
                missing += dn;
            }
        } else {
            log_printf(1, "fname=%s a32=%08x off=" XOT " end=" XOT " nbytes=" XOT "\n", fname, aval, a32->offset, pend, a32->len);
        }

        nbytes += a32->len;
        cksum = adler32_combine(cksum, a32->adler32, a32->len);

        next = a32->offset + a32->len;
    }

    tbx_list_destroy(write_table);
    tbx_stack_free(stack, 1);

    //** Store the attribute
    aval = cksum;
    dn = snprintf(value, sizeof(value), "%08x:" XOT ":" XOT ":" XOT, aval, missing, overlap, nbytes);
    lio_setattr(lc, creds, fname, NULL, "user.lfs_write", value, dn);
}

//***********************************************************************
//  lio_load_file_handle_attrs - Loads the attributes for a file handle
//***********************************************************************

int lio_load_file_handle_attrs(lio_config_t *lc, lio_creds_t *creds, char *fname, ex_id_t *inode, char **exnode, char **data, ex_off_t *data_size)
{
    char *myfname;
    char vino[256];
    int err, v_size[3];
    char *val[3];

    //** Get the attributes
    v_size[LFH_KEY_INODE] = sizeof(vino);
    val[LFH_KEY_INODE] = vino;
    v_size[LFH_KEY_EXNODE] = -lc->max_attr;
    val[LFH_KEY_EXNODE] = NULL;
    v_size[LFH_KEY_DATA] = -lc->max_attr;
    val[LFH_KEY_DATA] = NULL;

    myfname = (strcmp(fname, "") == 0) ? "/" : (char *)fname;
    err = lio_get_multiple_attrs(lc, creds, myfname, NULL, _lio_fh_keys, (void **)val, v_size, LFH_NKEYS);
    if (val[LFH_KEY_EXNODE] == NULL) err = OP_STATE_FAILURE;
    if (err != OP_STATE_SUCCESS) {
        log_printf(15, "Failed retrieving inode info!  path=%s\n", fname);
        if (val[1] != NULL) free(val[1]);
        *exnode = *data = NULL;
        *data_size = 0;
        return(-1);
    }

    *exnode = val[LFH_KEY_EXNODE];
    *data = val[LFH_KEY_DATA];
    *data_size = v_size[LFH_KEY_DATA];

    if (v_size[LFH_KEY_INODE] > 0) {
        *inode = 0;
        sscanf(vino, XIDT, inode);
    } else {
        generate_ex_id(inode);
        log_printf(0, "Missing inode generating a temp fake one! ino=" XIDT "\n", *inode);
    }

    return(0);
}


//***********************************************************************
//  _lio_get_file_handle - Returns the file handle associated with the view ID
//     number if the file is already open.  Otherwise NULL is returned
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

lio_file_handle_t *_lio_get_file_handle(lio_config_t *lc, ex_id_t vid)
{
    return(tbx_list_search(lc->open_index, (tbx_list_key_t *)&vid));

}

//***********************************************************************
// _lio_add_file_handle - Adds the file handle to the table
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

void _lio_add_file_handle(lio_config_t *lc, lio_file_handle_t *fh)
{
    tbx_list_insert(lc->open_index, (tbx_list_key_t *)&(fh->vid), (tbx_list_data_t *)fh);
}


//***********************************************************************
// _lio_remove_file_handle - Removes the file handle from the open table
//  ****NOTE: assumes that lio_lock(lfs) has been called ****
//***********************************************************************

void _lio_remove_file_handle(lio_config_t *lc, lio_file_handle_t *fh)
{
    tbx_list_remove(lc->open_index, (tbx_list_key_t *)&(fh->vid), (tbx_list_data_t *)fh);
}

//*************************************************************************
// lio_open_gop - Attempt to open the object for R/W
//*************************************************************************

typedef struct {
    char *id;
    lio_config_t *lc;
    lio_creds_t *creds;
    char *path;
    lio_fd_t **fd;
    int max_wait;
    int mode;
} lio_fd_op_t;

//*************************************************************************

int lio_wq_enable(lio_fd_t *fd, int max_in_flight)
{
    wq_context_t *ctx;
    int throwaway = 0;

    //** Make it outside the lock in case we're dumping to not trigger a deadlock
    ctx = wq_context_create(fd, max_in_flight);
    segment_lock(fd->fh->seg);
    if (!fd->fh->wq_ctx) {
        fd->fh->wq_ctx = ctx;
        log_printf(15, "wq_ctx=%p\n", fd->fh->wq_ctx);
    } else {
         throwaway = 1;
    }
    segment_unlock(fd->fh->seg);

    if (throwaway) wq_context_destroy(ctx);

    if (fd->fh->wq_ctx == NULL) return(1);

    fd->read_gop = lio_read_ex_gop_wq;
    fd->write_gop = lio_write_ex_gop_wq;
    log_printf(15, "wq enabeled\n");

    return(0);
}


//*************************************************************************
// _lio_metadata_io_fn - Function that does the actual R/W for data in metadata
//    NOTE: Assumes the file lock is already heldd and that the metadata buffer
//          can handle any write passed in
//*************************************************************************

int _lio_metadata_io_fn(lio_rw_op_t *op, int rw_mode)
{
    int i;
    ex_off_t bpos;
    tbx_tbuf_t tbmd;
    lio_file_handle_t *fh = op->fd->fh;
    ex_tbx_iovec_t *iov = op->iov;

    bpos = op->boff;
    tbx_tbuf_single(&tbmd, fh->data_size, fh->data);
    if (rw_mode == 0) { //** Read mode
        for (i=0; i<op->n_iov; i++) {
            tbx_tbuf_copy(&tbmd, iov[i].offset, op->buffer, bpos, iov[i].len, 1);
            bpos += iov[i].len;
        }
    } else { //** Write mode
        for (i=0; i<op->n_iov; i++) {
            tbx_tbuf_copy(op->buffer, bpos, &tbmd, iov[i].offset, iov[i].len, 1);
            bpos += iov[i].len;
        }
    }
    return(0);
}

//***********************************************************************
//  _metadata_free - Releases the data
//***********************************************************************

void _metadata_free(lio_file_handle_t  *fh)
{
    if (fh->data) {
        free(fh->data);
        fh->data = NULL;
        fh->max_data_allocated = fh->data_size = -1;
    }
}

//***********************************************************************
// _metadata_grow - Grows the space for storing metadata
//***********************************************************************

void _metadata_grow(lio_file_handle_t  *fh, ex_off_t new_size)
{
    if (new_size == 0) {
        if (fh->data) free(fh->data);
        fh->data = NULL;
        fh->max_data_allocated = fh->data_size = 0;
    } else {
        fh->max_data_allocated = new_size;
        if (fh->data) {
            tbx_type_realloc(fh->data, char, new_size);
        } else {
            tbx_type_malloc(fh->data, char, new_size);
        }

        if (fh->data_size == -1) {
            memset(fh->data, 0, new_size);
        } else {
            memset(fh->data + fh->data_size, 0, new_size - fh->data_size);
        }
    }
}

//*************************************************************************
// _lio_wait_for_tier_change_ok - Flags we want to make a tier change
//      and waits for the Ok
//
//    NOTE: The fh lock is held on entry and it's release/reacquired
//*************************************************************************

int _lio_wait_for_tier_change_ok(lio_file_handle_t *fh)
{
    int skip = 0;

    segment_lock(fh->seg);
    if (fh->adjust_tier_pending > 0) {  //** Someone else is adjusting the tier so wait for them to complete
        do {
            segment_unlock(fh->seg);
            apr_thread_cond_wait(fh->cond, fh->lock);
            segment_lock(fh->seg);
        } while (fh->adjust_tier_pending != 0);
        skip = 1;
    } else {  //** We are going to be doing the adjusting
        fh->adjust_tier_pending++;
        if (fh->in_flight != 0) { //** Got to wait
            do {
                segment_unlock(fh->seg);
                apr_thread_cond_wait(fh->cond, fh->lock);
                segment_lock(fh->seg);
            } while (fh->in_flight != 0);
        }
    }
    segment_unlock(fh->seg);

    return(skip);
}

//*************************************************************************
// _lio_release_tier_change_flag - Release the Tier change flag
//*************************************************************************

void _lio_release_tier_change_flag(lio_file_handle_t *fh)
{
    segment_lock(fh->seg);
    fh->adjust_tier_pending--;
    apr_thread_cond_signal(fh->cond);  //** Wake up anybody listening
    segment_unlock(fh->seg);
}


//*************************************************************************
//  lio_adjust_data_tier - Shuffles the *existing* data to the appropriate
//     Tier based on the new size.  The files size itself is not adjusted.
//     The new_size is justused to determine which tier the data should
//     be located.
//*************************************************************************

void lio_adjust_data_tier(lio_file_handle_t *fh, ex_off_t new_size, int do_lock)
{
    int err;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t iov;
    ex_off_t n;

    if (do_lock) apr_thread_mutex_lock(fh->lock);
    if (new_size > fh->lc->small_files_in_metadata_max_size) {  //** Needs to be in the segment tier
        if (fh->data_size == 0) {  //** Currently empty
            fh->data_size = -1;  //** Flags that the new size is to dump it into the segment
        } else if (fh->data_size > 0) { //** Need to move it to the segment
            if (_lio_wait_for_tier_change_ok(fh) == 0) {
                //** copy the data to the segment
                tbx_tbuf_single(&tbuf, fh->data_size, fh->data);
                iov.len = fh->data_size; iov.offset = 0;
                err = gop_sync_exec(segment_write(fh->seg, fh->lc->da, NULL, 1, &iov, &tbuf, 0, fh->lc->timeout));
                if (err == OP_STATE_SUCCESS) {
                    tbx_atomic_set(fh->modified, 1);  //** Flag it as modified
                    _metadata_free(fh);
                }
                _lio_release_tier_change_flag(fh);
            }
        }
    } else { //** Needs to be in the data tier
        if (fh->data_size < 0) {   //** Currently in the segment
            if (_lio_wait_for_tier_change_ok(fh) == 0) {
                //** Get the data from the segment
                n = segment_size(fh->seg);
                iov.len = (n>new_size) ? new_size : n;  iov.offset = 0;
                _metadata_free(fh);
                if (new_size > 0) {
                    _metadata_grow(fh, new_size);
                }
                tbx_tbuf_single(&tbuf, new_size, fh->data);
                err = (new_size > 0) ? gop_sync_exec(segment_read(fh->seg, fh->lc->da, NULL, 1, &iov, &tbuf, 0, fh->lc->timeout)) : OP_STATE_SUCCESS;
                if (err == OP_STATE_SUCCESS) {
                    tbx_atomic_set(fh->modified, 1);  //** Flag it as modified
                    fh->max_data_allocated = new_size;
                    fh->data_size = iov.len;
                    gop_sync_exec(lio_segment_truncate(fh->seg, fh->lc->da, 0, fh->lc->timeout));  //** Truncate the segment
                }
                _lio_release_tier_change_flag(fh);
            }
        } else if (fh->max_data_allocated < new_size) { //** Need to grow it
            _metadata_grow(fh, new_size);
        }
    }
    if (do_lock) apr_thread_mutex_unlock(fh->lock);


    return;
}

//*************************************************************************
// _lio_dec_in_flight_and_unlock - Dec's the in_flight counter and also
//    releases the segment_lock().  If needed it will reqcuire the locks
//    to notify a pending adjust tier call.
//
//    **NOTE: we have the segment lock on entry! **
//*************************************************************************

void _lio_dec_in_flight_and_unlock(lio_fd_t *fd, int in_flight)
{
    fd->fh->in_flight -= in_flight;
    if ((fd->fh->adjust_tier_pending > 0) && (fd->fh->in_flight == 0)) {
        //**Need to raise the flag so release the lock and get them in the
        //**proper order
        segment_unlock(fd->fh->seg);
        apr_thread_mutex_lock(fd->fh->lock);
        segment_lock(fd->fh->seg);
        //** Make sure the condition is still valid
        if ((fd->fh->adjust_tier_pending > 0) && (fd->fh->in_flight == 0)) {
            apr_thread_cond_broadcast(fd->fh->cond);
        }
        segment_unlock(fd->fh->seg);
        apr_thread_mutex_unlock(fd->fh->lock);
    } else {
        segment_unlock(fd->fh->seg);
    }
}

//*************************************************************************
// lio_tier_check_and_handle - checks if the data tier needs to be adjusted
//     If so it will do that and also handle the request if the tier
//     is in the metadata
//
//     If the operation was handled internally then 1 is returned
//*************************************************************************

int lio_tier_check_and_handle(lio_rw_op_t *op, int rw_mode, int *in_flight)
{
    int i;
    ex_off_t oend, n;
    lio_file_handle_t *fh = op->fd->fh;
    ex_tbx_iovec_t *iov = op->iov;

    *in_flight = 1;

    //** Check and handle reads
    apr_thread_mutex_lock(fh->lock);
    if (rw_mode == 0) {
        if (fh->data_size == -1) { //** Kick out the data is in in the segment
            apr_thread_mutex_unlock(fh->lock);
            return(OP_STATE_RETRY);   //** Nothing to do so return
        }

        //** If we made it here then it's in the buffer to handle them
        _lio_metadata_io_fn(op, rw_mode);
        apr_thread_mutex_unlock(fh->lock);
        *in_flight = 0;
        return(OP_STATE_SUCCESS);
    }

    //** See which tier the data sits in
    if (fh->data_size == -1) {  //**Stored in the segment
        apr_thread_mutex_unlock(fh->lock);
        return(OP_STATE_RETRY);   //** Nothing to do so return
    }

    //** IF we made it here then we should be in the metadata buffer

    //** Find the largest size to see if we need to grow it or switch tiers
    oend = iov[0].offset + iov[0].len;
    for (i=1; i<op->n_iov; i++) {
        n = iov[i].offset + iov[i].len;
        if (n > oend) oend = n;
    }

    if (oend > fh->lc->small_files_in_metadata_max_size) { //** Need to flip tiers
        lio_adjust_data_tier(fh, oend, 0);
    } else { //** We handle it in the data tier
        if (oend > fh->max_data_allocated) { //** Need to grow the space
            _metadata_grow(fh, oend);
        }
        if (oend > fh->data_size) fh->data_size = oend;
        _lio_metadata_io_fn(op, rw_mode);
        apr_thread_mutex_unlock(fh->lock);
        *in_flight = 0;
        return(OP_STATE_SUCCESS);
    }
    apr_thread_mutex_unlock(fh->lock);
    return(OP_STATE_RETRY);
}

//*************************************************************************

gop_op_status_t lio_myopen_fn(void *arg, int id)
{
    lio_fd_op_t *op = (lio_fd_op_t *)arg;
    lio_config_t *lc = op->lc;
    lio_file_handle_t *fh;
    lio_fd_t *fd;
    char *exnode, *data;
    ex_id_t ino, vid;
    ex_off_t data_size;
    lio_exnode_exchange_t *exp;
    gop_op_status_t status;
    int dtype, err, exec_flag;
    lio_segment_errors_t serr;

    status = gop_success_status;

    //** Check if it exists
    dtype = lio_exists(lc, op->creds, op->path);

    exec_flag = OS_OBJECT_EXEC_FLAG & op->mode;  //** Peel off the exec flag for use on new files only

    if ((op->mode & (LIO_WRITE_MODE|LIO_CREATE_MODE)) != 0) {  //** Writing and they want to create it if it doesn't exist
        if (dtype == 0) { //** Need to create it
            err = gop_sync_exec(lio_create_gop(lc, op->creds, op->path, OS_OBJECT_FILE_FLAG|exec_flag, NULL, NULL));
            if (err != OP_STATE_SUCCESS) {
                info_printf(lio_ifd, 1, "ERROR creating file(%s)!\n", op->path);
                log_printf(1, "ERROR creating file(%s)!\n", op->path);
                notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EIO\n", op->path, op->mode);
                free(op->path);
                *op->fd = NULL;
                _op_set_status(status, OP_STATE_FAILURE, -EIO);
                return(status);
            }
        } else if ((dtype & OS_OBJECT_DIR_FLAG) > 0) { //** It's a dir so fail
            info_printf(lio_ifd, 1, "Destination(%s) is a dir!\n", op->path);
            log_printf(1, "ERROR: Destination(%s) is a dir!\n", op->path);
            notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EISDIR\n", op->path, op->mode);
            free(op->path);
            *op->fd = NULL;
            _op_set_status(status, OP_STATE_FAILURE, -EISDIR);
            return(status);
        } else if (op->mode & LIO_EXCL_MODE) { //** This file shouldn't exist with this flag so kick out
            info_printf(lio_ifd, 1, "ERROR file(%s) already exists and EXCL is set!\n", op->path);
            log_printf(1, "ERROR file(%s) already exists and EXCL is set!\n", op->path);
            notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EEXIST\n", op->path, op->mode);
            free(op->path);
            *op->fd = NULL;
            _op_set_status(status, OP_STATE_FAILURE, -EEXIST);
            return(status);
        }
    } else if (dtype == 0) { //** No file so return an error
        info_printf(lio_ifd, 20, "Destination(%s) doesn't exist!\n", op->path);
        log_printf(1, "ERROR: Destination(%s) doesn't exist!\n", op->path);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=ENOTDIR\n", op->path, op->mode);
        free(op->path);
        *op->fd = NULL;
        _op_set_status(status, OP_STATE_FAILURE, -ENOTDIR);
        return(status);
    }

    //** Make the space for the FD
    tbx_type_malloc_clear(fd, lio_fd_t, 1);
    tbx_random_get_bytes(&(fd->id), sizeof(fd->id));
    fd->path = op->path;
    fd->mode = op->mode;
    fd->creds = op->creds;
    fd->lc = lc;
    fd->read_gop = lio_read_ex_gop_aio;
    fd->write_gop = lio_write_ex_gop_aio;

    exnode = NULL;
    if (lio_load_file_handle_attrs(lc, op->creds, op->path, &ino, &exnode, &data, &data_size) != 0) {
        log_printf(1, "ERROR loading attributes! fname=%s\n", op->path);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EIO\n", op->path, op->mode);
        free(fd);
        *op->fd = NULL;
        free(op->path);
        if (data) free(data);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        return(status);
    }

    //** Load the exnode and get the default view ID
    exp = lio_exnode_exchange_text_parse(exnode);
    vid = exnode_exchange_get_default_view_id(exp);
    if (vid == 0) {  //** Make sure the vid is valid.
        log_printf(1, "ERROR loading exnode! fname=%s\n", op->path);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EIO\n", op->path, op->mode);
        free(fd);
        *op->fd = NULL;
        free(op->path);
        if (data) free(data);
        lio_exnode_exchange_destroy(exp);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        return(status);
    }

    lio_lock(lc);
    fh = _lio_get_file_handle(lc, vid);
    log_printf(2, "fname=%s fh=%p\n", op->path, fh);

    if (fh != NULL) { //** Already open so just increment the ref count and return a new fd
        fh->ref_count++;
        fd->fh = fh;
        lio_unlock(lc);
        *op->fd = fd;
        if (data) free(data);
        lio_exnode_exchange_destroy(exp);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=SUCCESS\n", op->path, op->mode);
        return(gop_success_status);
    }

    //** New file to open
    tbx_type_malloc_clear(fh, lio_file_handle_t, 1);
    fh->vid = vid;
    fh->ref_count++;
    fh->lc = lc;
    fh->fname = strdup(fd->path);
    fh->data = data;
    fh->data_size = data_size;
    fh->max_data_allocated = data_size;
    assert_result(apr_pool_create(&(fh->mpool), NULL), APR_SUCCESS);   //** These are used for data tiering
    apr_thread_mutex_create(&(fh->lock), APR_THREAD_MUTEX_DEFAULT, fh->mpool);
    apr_thread_cond_create(&(fh->cond), fh->mpool);

    //** Load it
    fh->ex = lio_exnode_create();
    if (lio_exnode_deserialize(fh->ex, exp, lc->ess) != 0) {
        log_printf(0, "ERROR: Bad exnode! fname=%s\n", fd->path);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EIO\n", op->path, op->mode);
        goto cleanup;
    }

    //** Get the default view to use
    fh->seg = lio_exnode_default_get(fh->ex);
    if (fh->seg == NULL) {
        log_printf(0, "ERROR: No default segment!  Aborting! fname=%s\n", fd->path);
        _op_set_status(status, OP_STATE_FAILURE, -EIO);
        notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=EIO\n", op->path, fd->id, op->mode);
        goto cleanup;
    }

    if (lc->calc_adler32) fh->write_table = tbx_list_create(0, &skiplist_compare_ex_off, NULL, NULL, NULL);

    //Add it to the file open table
    _lio_add_file_handle(lc, fh);
    lio_unlock(lc);  //** Now we can release the lock

    fd->fh = fh;
    *op->fd = fd;

    if ((op->mode & LIO_WRITE_MODE) > 0) {  //** For write mode we check for a few more flags
        if ((op->mode & LIO_TRUNCATE_MODE) > 0) { //** See if they want the file truncated also
            status = gop_sync_exec_status(lio_truncate_gop(fd, 0));
            if (status.op_status != OP_STATE_SUCCESS) goto cleanup;

            //** We just truncated the file and removed all the allocations so let's update the exnode in the lserver
            memset(&serr, 0, sizeof(serr));  //** There aren't any errors to post
            err = lio_update_exnode_attrs(fd, &serr);
            if (err > 1) {  //** There was a problem with the update but we won't kick out since we will try again on file close
                log_printf(0, "ERROR updating exnode during open() with truncate flag! fname=%s\n", fd->path);
            }
        }

        if ((op->mode & LIO_APPEND_MODE) > 0) { //** Append to the end of the file
            segment_lock(fh->seg);
            fd->curr_offset = 0;
            segment_unlock(fh->seg);
        }
    }

    lio_adjust_data_tier(fh, lio_size(fd), 1);  //** See if we need to move the data between teirs

    lio_exnode_exchange_destroy(exp);  //** Clean up

    notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s fd=" XIDT " mode=%d STATUS=SUCCESS\n", op->path, fd->id, op->mode);
    return(status);

cleanup:  //** We only make it here on a failure
    log_printf(1, "ERROR in cleanup! fname=%s\n", op->path);

    lio_unlock(lc);

    notify_printf(lc->notify, 1, op->creds, "OPEN: fname=%s mode=%d STATUS=ERROR\n", op->path, op->mode);

    lio_exnode_destroy(fh->ex);
    lio_exnode_exchange_destroy(exp);
    free(fd->path);
    if (fh->data) free(fh->data);
    free(fh);
    free(fd);
    *op->fd = NULL;

    return(status);
}


//*************************************************************************

gop_op_generic_t *lio_open_gop(lio_config_t *lc, lio_creds_t *creds, char *path, int mode, char *id, lio_fd_t **fd, int max_wait)
{
    lio_fd_op_t *op;

    tbx_type_malloc_clear(op, lio_fd_op_t, 1);

    op->lc = lc;
    op->creds = creds;
    op->mode = mode;
    op->id = id;
    op->path = strdup(path);
    op->fd = fd;
    op->max_wait = max_wait;

    return(gop_tp_op_new(lc->tpc_unlimited, NULL, lio_myopen_fn, (void *)op, free, 1));
}


//*************************************************************************
// lio_close_gop - Rotuines for closing a previously opened file
//*************************************************************************

//*************************************************************************

gop_op_status_t lio_myclose_fn(void *arg, int id)
{
    lio_fd_t *fd = (lio_fd_t *)arg;
    lio_config_t *lc = fd->lc;
    lio_file_handle_t *fh;
    gop_op_status_t status;
    char *key[6] = {"system.exnode", "system.exnode.size", "os.timestamp.system.modify_data", NULL, NULL, NULL };
    char *val[6];
    int err, v_size[6];
    char ebuf[128];
    lio_segment_errors_t serr;
    ex_off_t final_size;
    apr_time_t now;
    int n, modified;
    double dt;

    log_printf(1, "fname=%s modified=" AIT " count=%d\n", fd->path, tbx_atomic_get(fd->fh->modified), fd->fh->ref_count);
    tbx_log_flush();

    status = gop_success_status;

    //** Get the handles
    fh = fd->fh;

    //** We don't decrement the ref count immediately to avoid another thread from thinking they are the only user
    lio_lock(lc);
    if (fh->ref_count > 1) {  //** Somebody else has it open as well
        fh->ref_count--;  //** Remove ourselves
        lio_unlock(lc);
        goto finished;
    }
    lio_unlock(lc);

    final_size = lio_size_fh(fh);

    //** Flush and truncate everything which could take some time
    modified = tbx_atomic_get(fh->modified);
    if (modified != 0) {
        log_printf(1, "FLUSH/TRUNCATE fname=%s final_size=" XOT " modified=%d\n", fd->path, final_size, modified);
        now = apr_time_now();
        gop_sync_exec(lio_truncate_gop(fd, final_size));
        dt = apr_time_now() - now;
        dt /= APR_USEC_PER_SEC;
        log_printf(1, "TRUNCATE fname=%s dt=%lf\n", fd->path, dt);
        now = apr_time_now();
        gop_sync_exec(lio_flush_gop(fd, 0, -1));
        dt = apr_time_now() - now;
        dt /= APR_USEC_PER_SEC;
        log_printf(1, "FLUSH fname=%s dt=%lf\n", fd->path, dt);
    }

    log_printf(5, "starting update process fname=%s modified=%d\n", fd->path, modified);

    //** See if we need to change tiers
    lio_adjust_data_tier(fh, lio_size(fd), 1);

    //** Ok no one has the file opened so teardown the segment/exnode
    //** IF not modified just tear down and clean up
    if (modified == 0) {
        //*** See if we need to update the error counts
        lio_get_error_counts(lc, fh->seg, &serr);
        n = lio_encode_error_counts(&serr, key, val, ebuf, v_size, 0);
        if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
            log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fd->path, serr.hard, serr.soft, serr.write);
            if (serr.hard>0) _op_set_status(status, OP_STATE_FAILURE, -EIO);
        }
        if (n > 0) {
            err = lio_multiple_setattr_op(lc, fd->creds, fd->path, NULL, key, (void **)val, v_size, n);
            if (err != OP_STATE_SUCCESS) {
                log_printf(0, "ERROR updating exnode! fname=%s\n", fd->path);
            }
        }

        //** Check again that no one else has opened the file
        lio_lock(lc);
        fh->ref_count--;  //** Remove ourselves and destroy fh within the lock
        if (fh->ref_count > 0) {  //** Somebody else opened it while we were flushing buffers
            lio_unlock(lc);
            goto finished;
        }

        //** Tear everything down
        lio_exnode_destroy(fh->ex);
        _lio_remove_file_handle(lc, fh);
        lio_unlock(lc);

        //** Shutdown the Work Queue context if needed
        //** This is done outside the lock in case a siginfo is triggered
        if (fh->wq_ctx != NULL) {
            wq_context_destroy(fh->wq_ctx);
            fh->wq_ctx = NULL;
        }

        if (fh->write_table != NULL) lio_store_and_release_adler32(lc, fd->creds, fh->write_table, fd->path);
        if (fh->remove_on_close == 1) status = gop_sync_exec_status(lio_remove_gop(lc, fd->creds, fd->path, NULL, lio_exists(lc, fd->creds, fd->path)));

        if (fh->fname) free(fh->fname);
        if (fh->data) free(fh->data);
        apr_thread_cond_destroy(fh->cond);
        apr_pool_destroy(fh->mpool);
        free(fh);
        goto finished;
    }

    //** Get any errors that may have occured
    lio_get_error_counts(lc, fh->seg, &serr);

    now = apr_time_now();

    //** Update the exnode and misc attributes
    err = lio_update_exnode_attrs(fd, &serr);
    if (err > 1) {
        log_printf(0, "ERROR updating exnode! fname=%s\n", fd->path);
    }

    if ((serr.hard>0) || (serr.soft>0) || (serr.write>0)) {
        log_printf(1, "ERROR: fname=%s hard_errors=%d soft_errors=%d write_errors=%d\n", fd->path, serr.hard, serr.soft, serr.write);
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "ATTR_UPDATE fname=%s dt=%lf\n", fd->path, dt);

    lio_lock(lc);  //** MAke sure no one else has opened the file while we were trying to close
    log_printf(1, "fname=%s ref_count=%d\n", fd->path, fh->ref_count);

    fh->ref_count--;  //** Ready to tear down so go ahead and decrement and destroy the fh inside the lock if Ok
    if (fh->ref_count > 0) {  //** Somebody else opened it while we were flushing buffers
        lio_unlock(lc);
        goto finished;
    }

    //** Clean up
    now = apr_time_now();
    lio_exnode_destroy(fh->ex); //** This is done in the lock to make sure the exnode isn't loaded twice
    _lio_remove_file_handle(lc, fh);

    lio_unlock(lc);

    //** Shutdown the Work Queue context if needed
    //** This is done outside the lock in case a siginfo is triggered
    if (fh->wq_ctx != NULL) {
        wq_context_destroy(fh->wq_ctx);
        fh->wq_ctx = NULL;
    }

    dt = apr_time_now() - now;
    dt /= APR_USEC_PER_SEC;
    log_printf(1, "lio_exnode_destroy fname=%s dt=%lf\n", fd->path, dt);
    if (fh->write_table != NULL) lio_store_and_release_adler32(lc, fd->creds, fh->write_table, fd->path);


    if (fh->remove_on_close) status = gop_sync_exec_status(lio_remove_gop(lc, fd->creds, fd->path, NULL, lio_exists(lc, fd->creds, fd->path)));
    if (fh->fname) free(fh->fname);
    if (fh->data) free(fh->data);
    apr_thread_cond_destroy(fh->cond);
    apr_pool_destroy(fh->mpool);
    free(fh);

    if (serr.hard != 0) status = gop_failure_status;
    log_printf(1, "hard=%d soft=%d status=%d\n", serr.hard, serr.soft, status.op_status);

finished:
    notify_printf(lc->notify, 1, fd->creds, "CLOSE: fname=%s fd=" XIDT " read_ops=" XOT " read_bytes=" XOT " read_error_ops=" XOT " read_error_bytes=" XOT
         " write_ops=" XOT " write_bytes=" XOT " write_error_ops=" XOT " write_error_bytes=" XOT "\n",
         fd->path, fd->id, fd->tally_ops[0], fd->tally_bytes[0], fd->tally_error_ops[0], fd->tally_error_bytes[0],
         fd->tally_ops[1], fd->tally_bytes[1], fd->tally_error_ops[1], fd->tally_error_bytes[1]);

    if (fd->path != NULL) free(fd->path);
    free(fd);

    return(status);
}

//*************************************************************************

gop_op_generic_t *lio_close_gop(lio_fd_t *fd)
{
    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_myclose_fn, (void *)fd, NULL, 1));
}


//*************************************************************************
// lio_read_gop_XXXX - The various read routines
//*************************************************************************

//*************************************************************************

gop_op_generic_t *lio_read_ex_gop_aio(lio_rw_op_t *op)
{
    return(gop_tp_op_new(op->fd->lc->tpc_unlimited, NULL, lio_read_ex_fn_aio, (void *)op, free, 1));
}

//*************************************************************************

gop_op_generic_t *lio_read_ex_gop_wq(lio_rw_op_t *op)
{
    tbx_tbuf_var_t tv;
    ex_off_t total;

    //** Handle some edge cases using the old method
    if (op->n_iov > 1) return(lio_read_ex_gop_aio(op));

    tbx_tbuf_var_init(&tv);
    total = tbx_tbuf_size(op->buffer);
    tv.nbytes = total;
    if (tbx_tbuf_next(op->buffer, op->boff, &tv) != TBUFFER_OK) return(lio_read_ex_gop_aio(op));
    if ((ex_off_t)tv.nbytes != (total-op->boff)) return(lio_read_ex_gop_aio(op));
    return(wq_op_new(op->fd->fh->wq_ctx, op, 0));
}

//*************************************************************************

gop_op_generic_t *lio_read_ex_gop(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = n_iov;
    op->iov = ex_iov;
    op->buffer = buffer;
    op->boff = boff;
    op->rw_hints = rw_hints;

    return(fd->read_gop(op));
}

//*************************************************************************

int lio_read_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_read_ex_gop(fd, n_iov, ex_iov, buffer, boff, rw_hints));
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_readv_gop(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    ex_off_t offset;
    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_vec(&(op->buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(fd->read_gop(op));
}

//*************************************************************************

int lio_readv(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_readv_gop(fd, iov, n_iov, size, off, rw_hints));
    return(status.error_code);
}

//*****************************************************************
// _lio_read_gop - Generates a read op.
//    NOTE: Uses the LIO readahead hints
// Return values 1 = Read beyond EOF so client should return gop_dummy(op_success_Status)
//               0 = Normal status. should call gop_read_ex
//               < 0 Bad command and the value is the error status to return
//*****************************************************************

int _lio_read_gop(lio_rw_op_t *op, lio_fd_t *fd, char *buf, ex_off_t size, off_t user_off, lio_segment_rw_hints_t *rw_hints)
{
    ex_off_t ssize, pend, rsize, rend, dr, off, readahead;

    //** If using WQ routines then disable readahead
    readahead = (fd->fh->wq_ctx) ? 0 : fd->fh->lc->readahead;

    //** Determine the offset
    off = (user_off < 0) ? fd->curr_offset : user_off;

    //** Do the read op
    ssize = lio_size(fd);
    pend = off + size;
    log_printf(0, "ssize=" XOT " off=" XOT " len=" XOT " pend=" XOT " readahead=" XOT " trigger=" XOT "\n", ssize, off, size, pend, fd->fh->lc->readahead, fd->fh->lc->readahead_trigger);
    if (pend > ssize) {
        if (off > ssize) {
            // offset is past the end of the segment
            return(1);
        } else {
            size = ssize - off;  //** Tweak the size based on how much data there is
        }
    }
    log_printf(0, "tweaked len=" XOT "\n", size);
    if (size <= 0) {
        log_printf(0, "Clipped tweaked len\n");
        return(1);
    }

    rsize = size;
    if (readahead <= 0) goto finished;

    rend = pend + readahead;  //** Tweak based on readahead

    segment_lock(fd->fh->seg);
    dr = pend - fd->fh->readahead_end;
    if ((dr > 0) || ((-dr) > fd->fh->lc->readahead_trigger)) {
        rsize = rend - off;
        if (rend > ssize) {
            if (off <= ssize) {
                rsize = ssize - off;  //** Tweak the size based on how much data there is
            }
        }

        fd->fh->readahead_end = rend;  //** Update the readahead end
    }
    segment_unlock(fd->fh->seg);

finished:
    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_single(op->buffer, size, buf);  //** This is the buffer size
    ex_iovec_single(op->iov, off, rsize); //** This is the buffer+readahead.  The extra doesn't get stored in the buffer.  Just in page cache.
    return(0);
}

//*****************************************************************

gop_op_generic_t *lio_read_gop(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    gop_op_status_t status;
    int err;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    err = _lio_read_gop(op, fd, buf, size, off, rw_hints);
    if (err == 0) {
        return(fd->read_gop(op));
    } else if (err == 1) {
        free(op);
        return(gop_dummy(gop_success_status));
    } else {
        free(op);
        _op_set_status(status, OP_STATE_FAILURE, err);
        return(gop_dummy(status));
    }

    return(NULL);  //** Never make it here
}

//*****************************************************************

int lio_read(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_read_gop(fd, buf, size, off, rw_hints));
    return(status.error_code);
}


//*****************************************************************
// lio_write_gop_XXXX - The various write routines
//*****************************************************************

//*************************************************************************

gop_op_generic_t *lio_write_ex_gop_aio(lio_rw_op_t *op)
{
    return(gop_tp_op_new(op->fd->lc->tpc_unlimited, NULL, lio_write_ex_fn_aio, (void *)op, free, 1));
}

//*************************************************************************

gop_op_generic_t *lio_write_ex_gop_wq(lio_rw_op_t *op)
{
    tbx_tbuf_var_t tv;
    ex_off_t total;

    //** Handle some edge cases using the old method
    if (op->n_iov > 1) return(lio_write_ex_gop_aio(op));

    tbx_tbuf_var_init(&tv);
    total = tbx_tbuf_size(op->buffer);
    tv.nbytes = total;
    if (tbx_tbuf_next(op->buffer, op->boff, &tv) != TBUFFER_OK) return(lio_write_ex_gop_aio(op));
    if ((ex_off_t)tv.nbytes != (total-op->boff)) return(lio_write_ex_gop_aio(op));

    return(wq_op_new(op->fd->fh->wq_ctx, op, 1));
}

//*************************************************************************

gop_op_generic_t *lio_write_ex_gop(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = n_iov;
    op->iov = iov;
    op->buffer = buffer;
    op->boff = boff;
    op->rw_hints = rw_hints;

    return(fd->write_gop(op));
}

//*************************************************************************

int lio_write_ex(lio_fd_t *fd, int n_iov, ex_tbx_iovec_t *ex_iov, tbx_tbuf_t *buffer, ex_off_t boff, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_write_ex_gop(fd, n_iov, ex_iov, buffer, boff, rw_hints));
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_writev_gop(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    tbx_type_malloc_clear(op, lio_rw_op_t, 1);
    ex_off_t offset;

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_vec(&(op->buffer_dummy), size, n_iov, iov);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(fd->write_gop(op));
}

//*************************************************************************

int lio_writev(lio_fd_t *fd, tbx_iovec_t *iov, int n_iov, ex_off_t size, ex_off_t off, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_writev_gop(fd, iov, n_iov, size, off, rw_hints));
    return(status.error_code);
}

//*************************************************************************

gop_op_generic_t *lio_write_gop(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    lio_rw_op_t *op;
    ex_off_t offset;

    tbx_type_malloc_clear(op, lio_rw_op_t, 1);

    op->fd = fd;
    op->n_iov = 1;
    op->iov = &(op->iov_dummy);
    op->buffer = &(op->buffer_dummy);
    op->boff = 0;
    op->rw_hints = rw_hints;

    tbx_tbuf_single(op->buffer, size, buf);
    offset = (off < 0) ? fd->curr_offset : off;
    ex_iovec_single(op->iov, offset, size);
    return(fd->write_gop(op));
}

//*************************************************************************

int lio_write(lio_fd_t *fd, char *buf, ex_off_t size, off_t off, lio_segment_rw_hints_t *rw_hints)
{
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_write_gop(fd, buf, size, off, rw_hints));
    return(status.error_code);
}

//***********************************************************************
//  All the various LIO copy routines and truncate:)
//***********************************************************************

#define LIO_COPY_BUFSIZE (20*1024*1024)

typedef struct {
    FILE *sffd, *dffd;
    lio_fd_t *slfd, *dlfd;
    ex_off_t bufsize;
    ex_off_t offset;
    ex_off_t offset2;
    ex_off_t len;
    char *buffer;
    lio_copy_hint_t hints;
    lio_segment_rw_hints_t *rw_hints;
    int truncate;
    int which_align;
} lio_cp_fn_t;


//***********************************************************************
// local2local_real - Does the actual local2local copy operation
//***********************************************************************

gop_op_status_t local2local_real(lio_cp_fn_t *op, char *buffer, ex_off_t bufsize_in)
{
    char *rb, *wb, *tb;
    ex_off_t bufsize, err, modulo;
    ex_off_t rpos, wpos, rlen, wlen, tlen, nbytes, got, block_size;
    ex_off_t initial_len, base_len, pplen;
    gop_op_status_t status;
    apr_time_t loop_start;
    double dt_loop;

    //** Set up the buffers
    modulo = (op->which_align == 0) ? op->offset : op->offset2;
    got = (modulo < 0) ? 0 : modulo;  //** Get the starting offset, -1=no seek
    block_size = 4096;
    tlen = (bufsize_in / 2 / block_size) - 1;
    pplen = block_size - (got % block_size);
    if (tlen <= 0) {
        bufsize = bufsize_in / 2;
        initial_len = base_len = bufsize;
    } else {
        base_len = tlen * block_size;
        bufsize = base_len + block_size;
        initial_len = base_len + pplen;
    }

   //** The buffer is split for R/W
    rb = buffer;
    wb = &(buffer[bufsize]);

    nbytes = op->len;
    status = gop_success_status;

    //** Read the initial block
    rpos = op->offset;
    wpos = op->offset2;

    if (rpos != -1) {
        lseek(fileno(op->sffd), rpos, SEEK_SET); //** Move to the start of the read

    } else {
        rpos = 0;
    }
    if (wpos != -1) {
        lseek(fileno(op->dffd), wpos, SEEK_SET); //** Move to the start of the write
    } else {
        wpos = 0;
    }

    if (nbytes < 0) {
        err = posix_fadvise(fileno(op->sffd), rpos, 0, POSIX_FADV_SEQUENTIAL);
        rlen = initial_len;
    } else {
        err = posix_fadvise(fileno(op->sffd), rpos, nbytes, POSIX_FADV_SEQUENTIAL);
        rlen = (nbytes > initial_len) ? initial_len : nbytes;
    }

    wlen = 0;
    rpos += rlen;
    if (nbytes > 0) nbytes -= rlen;

    loop_start = apr_time_now();
    got = tbx_dio_read(op->sffd, rb, rlen, -1);
    if (got == -1) {
        log_printf(1, "ERROR from fread=%d  rlen=" XOT " got=" XOT "\n", errno, rlen, got);
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
        tlen = rlen;
        rlen = wlen;
        wlen = tlen;

        log_printf(1, "wpos=" XOT " rlen=" XOT " wlen=" XOT " nbytes=" XOT "\n", wpos, rlen, wlen, nbytes);
        loop_start = apr_time_now();

        //** Start the write
        err = tbx_dio_write(op->dffd, wb, wlen, -1);
	if (err != wlen) {
            log_printf(1, "ERROR write failed! wpos=" XOT " len=" XOT " err=" XOT " errno=%d ferror=%d\n", wpos, wlen, err, errno, ferror(op->dffd));
            goto finished;
        }
        wpos += wlen;

        //** Read in the next block
        if (nbytes < 0) {
            rlen = bufsize;
        } else {
            rlen = (nbytes > bufsize) ? bufsize : nbytes;
        }
        if (rlen > 0) {
            got = tbx_dio_read(op->sffd, rb, rlen, -1);
            if (got == -1) { //** Got an error
                log_printf(1, "ERROR read failed and not EOF! errno=%d\n", errno);
                goto finished;
            }
            rlen = got;
            rpos += rlen;
            if (nbytes > 0) nbytes -= rlen;
        }

        dt_loop = apr_time_now() - loop_start;
        dt_loop /= (double)APR_USEC_PER_SEC;

        log_printf(1, "dt_loop=%lf nleft=" XOT " rlen=" XOT " err=%d\n", dt_loop, nbytes, rlen, errno);
    } while (rlen > 0);

    if (op->truncate == 1) {  //** Truncate if wanted
        if (!ftruncate(fileno(op->dffd), ftell(op->dffd))) {
            log_printf(10, "ERROR with truncate!\n");
        }
    }

finished:
    return(status);
}


//***********************************************************************
// lio_cp_local2local - Copies a local file to LIO
//***********************************************************************

gop_op_status_t lio_cp_local2local_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    char *buffer;
    ex_off_t bufsize;

    buffer = op->buffer;
    bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE : op->bufsize;

    if (buffer == NULL) { //** Need to make it ourself
        tbx_type_malloc(buffer, char, bufsize);
    }

    status = local2local_real(op, buffer, bufsize);

    //** Clean up
    if (op->buffer == NULL) free(buffer);

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_local2local_gop(FILE *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, int truncate, lio_segment_rw_hints_t *rw_hints, int which_align)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->offset = src_offset;
    op->offset2 = dest_offset;
    op->len = len;
    op->sffd = sfd;
    op->dffd = dfd;
    op->truncate = truncate;
    op->rw_hints = rw_hints;
    op->which_align = which_align;

    return(gop_tp_op_new(lio_gc->tpc_unlimited, NULL, lio_cp_local2local_fn, (void *)op, free, 1));

}

//***********************************************************************
// lio_cp_local2lio - Copies a local file to LIO
//***********************************************************************

gop_op_status_t lio_cp_local2lio_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status = gop_success_status;
    char *buffer;
    ex_off_t bufsize, len, got, off, len2, max_eof;
    lio_file_handle_t *lfh = op->dlfd->fh;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t iov;
    int nfd, localbuf;
    FILE *ffd = op->sffd;

    localbuf = 0;
    buffer = op->buffer;
    bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;
    if (bufsize < lfh->lc->small_files_in_metadata_max_size) {
        bufsize = lfh->lc->small_files_in_metadata_max_size;
        localbuf = 1;
    }
    if ((buffer == NULL) || localbuf) { //** Need to make it ourself
        tbx_type_malloc(buffer, char, bufsize+1);
    }

    tbx_tbuf_single(&tbuf, bufsize, buffer);

    //** See if we have a pipe or a normal file
    max_eof = -1;
    len = 0;
    if (op->len == -1) {
        nfd = fileno(ffd);
        len = lseek(nfd, 0, SEEK_CUR);  //** Get the current position
        if (len != -1) {
            op->len = lseek(nfd, 0, SEEK_END) - len;
            max_eof = op->len;
            lseek(nfd, len, SEEK_SET);
        }
    }

    //** Read the initial block
    got = (op->len != -1) ? op->len + op->offset : op->offset;
    if (got > lfh->lc->small_files_in_metadata_max_size) {  //** Goes in the segment
        lio_adjust_data_tier(lfh, lfh->lc->small_files_in_metadata_max_size+1, 0);
        status = gop_sync_exec_status(segment_put_gop(lfh->lc->tpc_unlimited, lfh->lc->da, op->rw_hints, ffd, lfh->seg, op->offset, op->len, bufsize, buffer, op->truncate, 3600));
        goto cleanup;
    }

    //** Let's read the initial block
    len = (op->len == -1) ? lfh->lc->small_files_in_metadata_max_size-op->offset : op->len-op->offset;
    got = tbx_dio_read(ffd, buffer, len, -1);

    if ((got != len) || (max_eof == got)) { //** Hit the EOF so we can figure out how big things are
        off = got + op->offset;
        if (off <= lfh->lc->small_files_in_metadata_max_size) { //** Fits in the metadata
            if (lfh->data_size != -1) { //** Already in metadata so just update
                if (off > lfh->max_data_allocated) { //** Need to grow the space
                    _metadata_grow(lfh, off);
                }
                lfh->modified = 1;
                memcpy(lfh->data + op->offset, buffer, got);
                if ((op->offset+got) > lfh->data_size) lfh->data_size = op->offset + got;
            } else { //** Stored in the segment currently
                if (op->truncate) {  //** We can truncate so switch tiers and store the data
                    lio_adjust_data_tier(lfh, off, 0);
                    memcpy(lfh->data + op->offset, buffer, got);
                    if ((op->offset+got) > lfh->data_size) lfh->data_size = op->offset + got;
                } else { //** Dump it in the segment
                    iov.offset = op->offset; iov.len = got;
                    status = gop_sync_exec_status(segment_write(lfh->seg, lfh->lc->da, NULL, 1, &iov, &tbuf, 0, lfh->lc->timeout));
                }
            }
        } else { //** To big so dump in segment
            if (op->truncate) {
                len2 = off;
            } else {
                len2 = lio_size_fh(lfh);
                if (len2 < off) len2 = off;
            }
            lio_adjust_data_tier(lfh, len2, 0);
            iov.offset = op->offset; iov.len = got;
            status = gop_sync_exec_status(segment_write(lfh->seg, lfh->lc->da, NULL, 1, &iov, &tbuf, 0, lfh->lc->timeout));
        }

        goto cleanup;
    }

    //** Force it to be in the segment
    lio_adjust_data_tier(lfh, lfh->lc->small_files_in_metadata_max_size+1, 0);
    iov.offset = op->offset; iov.len = got;
    status = gop_sync_exec_status(segment_write(lfh->seg, lfh->lc->da, NULL, 1, &iov, &tbuf, 0, lfh->lc->timeout));
    if (status.op_status == OP_STATE_SUCCESS) {
        if (op->len != -1) op->len -= got;
        op->offset += got;
        status = gop_sync_exec_status(segment_put_gop(lfh->lc->tpc_unlimited, lfh->lc->da, op->rw_hints, ffd, lfh->seg, op->offset, op->len, bufsize, buffer, op->truncate, 3600));
    }

cleanup:
    tbx_atomic_set(lfh->modified, 1); //** Flag it as modified so the new exnode gets stored

    //** Clean up
    if (localbuf) free(buffer);

    notify_printf(op->dlfd->lc->notify, 1, op->dlfd->creds, "COPY_WRITE: fname=%s fd=" XIDT " STATUS=%s\n", op->dlfd->path, op->dlfd->id, ((status.op_status == OP_STATE_SUCCESS) ? "SUCCESS" : "FAIL"));

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_local2lio_gop(FILE *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len, int truncate, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->offset = offset;
    op->len = len;
    op->sffd = sfd;
    op->dlfd = dfd;
    op->truncate = truncate;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(dfd->lc->tpc_unlimited, NULL, lio_cp_local2lio_fn, (void *)op, free, 1));

}

//***********************************************************************
// metadata_lio2local - Copies the data from an LStore file with data stored as
//       metadata to a local file
//***********************************************************************

gop_op_status_t metadata_lio2local(lio_cp_fn_t *op)
{
    gop_op_status_t status = gop_success_status;
    ex_off_t got;
    lio_file_handle_t *lfh = op->slfd->fh;
    FILE *ffd = op->dffd;

    if (op->len == -1) op->len = lfh->data_size - op->offset;
    if ((op->offset > lfh->data_size) || ((op->offset + op->len) > lfh->data_size)) return(gop_failure_status);
    got = tbx_dio_write(ffd, lfh->data + op->offset, op->len, -1);
    if (got != op->len) status = gop_failure_status;
    return(status);
}

//***********************************************************************
// lio_cp_lio2local - Copies a LIO file to a local file
//***********************************************************************

gop_op_status_t lio_cp_lio2local_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    char *buffer;
    ex_off_t bufsize;
    lio_file_handle_t *lfh = op->slfd->fh;
    FILE *ffd = op->dffd;


    if (lfh->data_size == -1) {   //** Data stored in the segment
        buffer = op->buffer;
        bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;
        if (buffer == NULL) { //** Need to make it ourself
            tbx_type_malloc(buffer, char, bufsize+1);
        }

        status = gop_sync_exec_status(segment_get_gop(lfh->lc->tpc_unlimited, lfh->lc->da, op->rw_hints, lfh->seg, ffd, op->offset, op->len, bufsize, buffer, 3600));

        //** Clean up
        if (op->buffer == NULL) free(buffer);
    } else {  //** Stored as metadata
        status = metadata_lio2local(op);
    }


    notify_printf(op->slfd->lc->notify, 1, op->slfd->creds, "COPY_READ: fname=%s fd=" XIDT " STATUS=%s\n", op->slfd->path, op->slfd->id, ((status.op_status == OP_STATE_SUCCESS) ? "SUCCESS" : "FAIL"));

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_lio2local_gop(lio_fd_t *sfd, FILE *dfd, ex_off_t bufsize, char *buffer, ex_off_t offset, ex_off_t len, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->offset = offset;
    op->len = len;
    op->slfd = sfd;
    op->dffd = dfd;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(sfd->lc->tpc_unlimited, NULL, lio_cp_lio2local_fn, (void *)op, free, 1));
}

//***********************************************************************
// cp_md2seg - Copies the data from between metadata buffers
//***********************************************************************

gop_op_status_t cp_md2md(lio_cp_fn_t *op, ex_off_t dend, ex_off_t dsize)
{
    lio_file_handle_t *sfh = op->slfd->fh;
    lio_file_handle_t *dfh = op->dlfd->fh;
    ex_off_t dfinal, send;

    //**Check that the source range is good
    send = op->offset + op->len;
    if (send > sfh->data_size) return(gop_failure_status);

    //** Make sure the destination is in the metadata tier
    dfinal = (dend > dsize) ? dend : dsize;
    lio_adjust_data_tier(dfh, dfinal, 0);

    //** Now do the copy
    memcpy(dfh->data + op->offset2, sfh->data + op->offset, op->len);
    if (dend > dfh->data_size) dfh->data_size = dend;
    tbx_atomic_set(dfh->modified, 1);

    return(gop_success_status);
}

//***********************************************************************
// cp_md2seg - Copies the data from the metadata to a segment
//***********************************************************************

gop_op_status_t cp_md2seg(lio_cp_fn_t *op, ex_off_t dsize)
{
    gop_op_status_t status = gop_success_status;
    lio_file_handle_t *sfh = op->slfd->fh;
    lio_file_handle_t *dfh = op->dlfd->fh;
    ex_off_t send;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t iov;

    //**Check that the source range is good
    send = op->offset + op->len;
    if (send > sfh->data_size) return(gop_failure_status);

    //** Make sure the destination is in the segment tier
    lio_adjust_data_tier(dfh, dfh->lc->small_files_in_metadata_max_size+1, 0);

    //** Setup the buffer for the write
    tbx_tbuf_single(&tbuf, op->len, sfh->data + op->offset);
    iov.len = op->len; iov.offset = op->offset2;

    //** And dump the data
    status = gop_sync_exec_status(segment_write(dfh->seg, dfh->lc->da, NULL, 1, &iov, &tbuf, 0, dfh->lc->timeout));
    if (status.op_status == OP_STATE_SUCCESS) {
        if (op->truncate == 1) gop_sync_exec(lio_segment_truncate(dfh->seg, dfh->lc->da, dsize, dfh->lc->timeout));  //** Truncate the segment
        tbx_atomic_set(dfh->modified, 1);
    }

    return(status);
}

//***********************************************************************
// cp_seg2md - Copies the data from a segment to metadata
//***********************************************************************

gop_op_status_t cp_seg2md(lio_cp_fn_t *op, ex_off_t dend, ex_off_t dsize)
{
    gop_op_status_t status = gop_success_status;
    lio_file_handle_t *sfh = op->slfd->fh;
    lio_file_handle_t *dfh = op->dlfd->fh;
    ex_off_t dfinal, send;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t iov;

    //**Check that the source range is good
    send = op->offset + op->len;
    if (send > sfh->data_size) return(gop_failure_status);

    //** Make sure the destination is in the metadata tier
    dfinal = (dend > dsize) ? dend : dsize;
    lio_adjust_data_tier(dfh, dfinal, 0);

    //** Get the data from the segment and dump it in the dest
    tbx_tbuf_single(&tbuf, op->len, dfh->data + op->offset2);
    iov.len = op->len; iov.offset = op->offset;
    status = gop_sync_exec_status(segment_read(sfh->seg, sfh->lc->da, NULL, 1, &iov, &tbuf, 0, sfh->lc->timeout));
    if (status.op_status == OP_STATE_SUCCESS) tbx_atomic_set(dfh->modified, 1);
    if (dend > dfh->data_size) dfh->data_size = dend;

    return(status);
}

//***********************************************************************
// lio_cp_lio2lio - Copies a LIO file to another LIO file
//***********************************************************************

gop_op_status_t lio_cp_lio2lio_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    lio_file_handle_t *sfh = op->slfd->fh;
    lio_file_handle_t *dfh = op->dlfd->fh;
    char *buffer;
    ex_off_t bufsize;
    ex_off_t dend, dsize;
    int used, dmd;
    const int sigsize = 10*1024;
    char sig1[sigsize], sig2[sigsize];

    //** See if some of the data is stored as metadata.
    dsize = lio_size(op->slfd);
    if (op->len == -1) {
        op->len = dsize - op->offset;
    }
    dend = op->offset2 + op->len;
    if (op->truncate == 1) {
        dsize = dend;
    } else if (dend > dsize) {
        dsize = dend;
    }

    //** This tells us if after the operation completes where the data should reside.
    dmd = ((dend < dfh->lc->small_files_in_metadata_max_size) || (dsize < dfh->lc->small_files_in_metadata_max_size)) ? 1 : 0;
    if (sfh->data_size != -1) { //** Source is MD
        if (dmd) { //** Dest is MD
            status = cp_md2md(op, dend, dsize);
        } else {
            status = cp_md2seg(op, dsize);
        }
        goto finished;
    } else if (dmd) {  //** Source is segment and dest is MD
        status = cp_seg2md(op, dend, dsize);
        goto finished;
    }

    //** If we made it there then both source and dest should be in the segment
    lio_adjust_data_tier(dfh, dfh->lc->small_files_in_metadata_max_size+1, 0);  //** Make sure the destintation is in the segment tier

    //** Check if we can do a depot->depot direct copy
    used = 0;
    segment_signature(sfh->seg, sig1, &used, sigsize);
    used = 0;
    segment_signature(dfh->seg, sig2, &used, sigsize);

    status = gop_failure_status;
    if ((strcmp(sig1, sig2) == 0) && ((op->hints & LIO_COPY_INDIRECT) == 0)) {
        status = gop_sync_exec_status(segment_clone(sfh->seg, dfh->lc->da, &(dfh->seg), CLONE_STRUCT_AND_DATA, NULL, dfh->lc->timeout));
    }

    //** If the signatures don't match or the clone failed do a slow indirect copy passing through the client
    if (status.op_status == OP_STATE_FAILURE) {
        buffer = op->buffer;
        bufsize = (op->bufsize <= 0) ? LIO_COPY_BUFSIZE-1 : op->bufsize-1;

        if (buffer == NULL) { //** Need to make it ourself
            tbx_type_malloc(buffer, char, bufsize+1);
        }
        status = gop_sync_exec_status(lio_segment_copy_gop(dfh->lc->tpc_unlimited, dfh->lc->da, op->rw_hints, sfh->seg, dfh->seg, op->offset, op->offset2, op->len, bufsize, buffer, 1, dfh->lc->timeout));

        //** Clean up
        if (op->buffer == NULL) free(buffer);
    }

finished:
    notify_printf(op->slfd->lc->notify, 1, op->slfd->creds, "COPY_READ: fname=%s fd=" XIDT " STATUS=%s\n", op->slfd->path, op->slfd->id, ((status.op_status == OP_STATE_SUCCESS) ? "SUCCESS" : "FAIL"));
    notify_printf(op->dlfd->lc->notify, 1, op->dlfd->creds, "COPY_WRITE: fname=%s fd=" XIDT " STATUS=%s\n", op->dlfd->path, op->dlfd->id, ((status.op_status == OP_STATE_SUCCESS) ? "SUCCESS" : "FAIL"));

    tbx_atomic_set(dfh->modified, 1); //** Flag it as modified so the new exnode gets stored

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_cp_lio2lio_gop(lio_fd_t *sfd, lio_fd_t *dfd, ex_off_t bufsize, char *buffer, ex_off_t src_offset, ex_off_t dest_offset, ex_off_t len, int hints, lio_segment_rw_hints_t *rw_hints)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->buffer = buffer;
    op->bufsize = bufsize;
    op->offset = src_offset;
    op->offset2 = dest_offset;
    op->len = len;
    op->slfd = sfd;
    op->dlfd = dfd;
    op->hints = hints;
    op->rw_hints = rw_hints;

    return(gop_tp_op_new(dfd->lc->tpc_unlimited, NULL, lio_cp_lio2lio_fn, (void *)op, free, 1));
}

//*************************************************************************
// lio_file_copy_op - Actual cp function.  Copies a regex to a dest *dir*
//*************************************************************************

gop_op_status_t lio_file_copy_op(void *arg, int id)
{
    lio_cp_file_t *cp = (lio_cp_file_t *)arg;
    gop_op_status_t status, close_status;
    FILE *sffd, *dffd;
    lio_fd_t *slfd, *dlfd;
    int already_exists, ftype, flag;
    char *buffer;
    struct stat stat_link, stat_object;

    buffer = NULL;

    if (((cp->src_tuple.is_lio == 0) && (cp->dest_tuple.is_lio == 0)) && (cp->enable_local == 0)) {  //** Not allowed to both go to disk
        info_printf(lio_ifd, 0, "Both source(%s) and destination(%s) are local files! enable_local=%d\n", cp->src_tuple.path, cp->dest_tuple.path, cp->enable_local);
        return(gop_failure_status);
    }

    already_exists = 0;  //** Let's us know if we remove the destination in case of a failure.

    if (cp->src_tuple.is_lio == 0) {  //** Source is a local file and dest is lio (or local if enabled)
        ftype = lio_os_local_filetype(cp->src_tuple.path);
        sffd = fopen(cp->src_tuple.path, "r");
        if (sffd) tbx_dio_init(sffd);

        if (cp->dest_tuple.is_lio == 1) {
            info_printf(lio_ifd, 0, "copy %s %s@%s:%s\n", cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds, NULL), cp->dest_tuple.lc->obj_name, cp->dest_tuple.path);
            already_exists = lio_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path); //** Track if the file was already there for cleanup
            gop_sync_exec(lio_open_gop(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, lio_fopen_flags("w"), NULL, &dlfd, 60));

            if ((sffd == NULL) || (dlfd == NULL)) { //** Got an error
                if (sffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
                if (dlfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
                status = gop_failure_status;
            } else {
                tbx_malloc_align(buffer, getpagesize(), cp->bufsize);
                status = gop_sync_exec_status(lio_cp_local2lio_gop(sffd, dlfd, cp->bufsize, buffer, 0, -1, 1, cp->rw_hints));
            }
            if (dlfd != NULL) {
                close_status = gop_sync_exec_status(lio_close_gop(dlfd));
                if (close_status.op_status != OP_STATE_SUCCESS) status = close_status;
                if (status.op_status == OP_STATE_SUCCESS) {
                    if ((ftype & OS_OBJECT_EXEC_FLAG) != (already_exists & OS_OBJECT_EXEC_FLAG)) { //** Need to either set or unset the exec flag
                        flag = (ftype & OS_OBJECT_EXEC_FLAG) ? 1 : 0;
                        gop_sync_exec(os_object_exec_modify(cp->dest_tuple.lc->os, cp->dest_tuple.creds, cp->dest_tuple.path, flag));
                    }
                }
            }
            if (sffd != NULL) { tbx_dio_finish(sffd, 0); fclose(sffd); }
        } else if (cp->enable_local == 1) {  //** local2local copy
            info_printf(lio_ifd, 0, "copy %s %s\n", cp->src_tuple.path, cp->dest_tuple.path);
            ftype = os_local_filetype_stat(cp->src_tuple.path, &stat_link, &stat_object);
            already_exists = lio_os_local_filetype(cp->dest_tuple.path); //** Track if the file was already there for cleanup
            dffd = fopen(cp->dest_tuple.path, "w");
            if (dffd) tbx_dio_init(dffd);

            if ((sffd == NULL) || (dffd == NULL)) { //** Got an error
                if (sffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
                if (dffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
                status = gop_failure_status;
            } else {
                tbx_malloc_align(buffer, getpagesize(), cp->bufsize);
                status = gop_sync_exec_status(lio_cp_local2local_gop(sffd, dffd, cp->bufsize, buffer, 0, 0, -1, 1, cp->rw_hints, 0));
            }
            if (dffd != NULL) { tbx_dio_finish(dffd, 0); fclose(dffd); }
            if (sffd != NULL) { tbx_dio_finish(sffd, 0); fclose(sffd); }

            if (status.op_status == OP_STATE_SUCCESS) {
                if ((ftype & OS_OBJECT_EXEC_FLAG) != (already_exists & OS_OBJECT_EXEC_FLAG)) { //** Need to either set or unset the exec flag
                    if (ftype & OS_OBJECT_EXEC_FLAG) chmod(cp->dest_tuple.path, stat_object.st_mode);
                }
            }
        }
    } else if (cp->dest_tuple.is_lio == 0) {  //** Source is lio and dest is local
        info_printf(lio_ifd, 0, "copy %s@%s:%s %s\n", an_cred_get_id(cp->src_tuple.creds, NULL), cp->src_tuple.lc->obj_name, cp->src_tuple.path, cp->dest_tuple.path);
        already_exists = lio_os_local_filetype(cp->dest_tuple.path); //** Track if the file was already there for cleanup
        ftype = lio_exists(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path); //** Get the source's type

        gop_sync_exec(lio_open_gop(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, lio_fopen_flags("r"), NULL, &slfd, 60));
        dffd = fopen(cp->dest_tuple.path, "w");
        if (dffd) tbx_dio_init(dffd);

        if ((dffd == NULL) || (slfd == NULL)) { //** Got an error
            if (slfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
            if (dffd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
            status = gop_failure_status;
        } else {
            tbx_malloc_align(buffer, getpagesize(), cp->bufsize);
            status = gop_sync_exec_status(lio_cp_lio2local_gop(slfd, dffd, cp->bufsize, buffer, 0, -1, cp->rw_hints));
        }
        if (slfd != NULL) gop_sync_exec(lio_close_gop(slfd));
        if (dffd != NULL) { tbx_dio_finish(dffd, 0); fclose(dffd); }
        if (status.op_status == OP_STATE_SUCCESS) {
            if ((ftype & OS_OBJECT_EXEC_FLAG) != (already_exists & OS_OBJECT_EXEC_FLAG)) { //** Need to either set or unset the exec flag
                already_exists = os_local_filetype_stat(cp->dest_tuple.path, &stat_link, &stat_object);
                if (ftype & OS_OBJECT_EXEC_FLAG)  {
                    if (stat_object.st_mode & S_IRUSR) stat_object.st_mode |= S_IXUSR;
                    if (stat_object.st_mode & S_IRGRP) stat_object.st_mode |= S_IXGRP;
                    if (stat_object.st_mode & S_IROTH) stat_object.st_mode |= S_IXOTH;
                    chmod(cp->dest_tuple.path, stat_object.st_mode);
                } else {
                    if (stat_object.st_mode & S_IRUSR) stat_object.st_mode ^= S_IXUSR;
                    if (stat_object.st_mode & S_IRGRP) stat_object.st_mode ^= S_IXGRP;
                    if (stat_object.st_mode & S_IROTH) stat_object.st_mode ^= S_IXOTH;
                    chmod(cp->dest_tuple.path, stat_object.st_mode);
                }
            }
        }
    } else {               //** both source and dest are lio
        info_printf(lio_ifd, 0, "copy %s@%s:%s %s@%s:%s\n", an_cred_get_id(cp->src_tuple.creds, NULL), cp->src_tuple.lc->obj_name, cp->src_tuple.path, an_cred_get_id(cp->dest_tuple.creds, NULL), cp->dest_tuple.lc->obj_name, cp->dest_tuple.path);
        ftype = lio_exists(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path); //** Get the source's type
        already_exists = lio_exists(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path); //** Track if the file was already there for cleanup
        gop_sync_exec(lio_open_gop(cp->src_tuple.lc, cp->src_tuple.creds, cp->src_tuple.path, LIO_READ_MODE, NULL, &slfd, 60));
        gop_sync_exec(lio_open_gop(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, lio_fopen_flags("w"), NULL, &dlfd, 60));
        if ((dlfd == NULL) || (slfd == NULL)) { //** Got an error
            if (slfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening source file!  path=%s\n", cp->src_tuple.path);
            if (dlfd == NULL) info_printf(lio_ifd, 0, "ERROR: Failed opening destination file!  path=%s\n", cp->dest_tuple.path);
            status = gop_failure_status;
        } else {
            tbx_malloc_align(buffer, getpagesize(), cp->bufsize);
            status = gop_sync_exec_status(lio_cp_lio2lio_gop(slfd, dlfd, cp->bufsize, buffer, 0, 0, -1, cp->slow, cp->rw_hints));
        }
        if (slfd != NULL) gop_sync_exec(lio_close_gop(slfd));
        if (dlfd != NULL) {
            close_status = gop_sync_exec_status(lio_close_gop(dlfd));
            if (close_status.op_status != OP_STATE_SUCCESS) status = close_status;
            if (status.op_status == OP_STATE_SUCCESS) {
                if ((ftype & OS_OBJECT_EXEC_FLAG) != (already_exists & OS_OBJECT_EXEC_FLAG)) { //** Need to either set or unset the exec flag
                    flag = (ftype & OS_OBJECT_EXEC_FLAG) ? 1 : 0;
                    gop_sync_exec(os_object_exec_modify(cp->dest_tuple.lc->os, cp->dest_tuple.creds, cp->dest_tuple.path, flag));
                }
            }
        }
    }

    if (buffer != NULL) free(buffer);

    if ((status.op_status != OP_STATE_SUCCESS) && (already_exists == 0)) { //** Copy failed so remove the destination if needed
        log_printf(5, "Failed with copy. Removing destination: %s\n", cp->dest_tuple.path);
        gop_sync_exec(lio_remove_gop(cp->dest_tuple.lc, cp->dest_tuple.creds, cp->dest_tuple.path, NULL, 0));
    }

    return(status);
}

//*************************************************************************
// lio_cp_create_dir - Ensures the new directory exists and updates the valid
//     dir table
//*************************************************************************

int lio_cp_create_dir(tbx_list_t *table, lio_path_tuple_t tuple)
{
    int i, n, err, error_code, skip_insert;
    struct stat s;
    char *dname = tuple.path;
    char *dstate;

    error_code = 0;
    n = strlen(dname);
    for (i=1; i<n; i++) {
        if ((dname[i] == '/') || (i==n-1)) {
            dstate = tbx_list_search(table, dname);
            if (dstate == NULL) {  //** Need to make the dir
                skip_insert = 0;
                if (i<n-1) dname[i] = 0;
                if (tuple.is_lio == 0) { //** Local dir
                    err = mkdir(dname, S_IRWXU|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH);
                    if (err != 0) { //** Check if it was already created by someone else
                        err = stat(dname, &s);
                    }
                    err = (err == 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
                } else {
                    err = gop_sync_exec(lio_create_gop(tuple.lc, tuple.creds, dname, OS_OBJECT_DIR_FLAG, NULL, NULL));
                    if (err != OP_STATE_SUCCESS) {  //** See if it was created by someone else
                        err = lio_exists(tuple.lc, tuple.creds, dname);
                        err = ((err & OS_OBJECT_DIR_FLAG) > 0) ? OP_STATE_SUCCESS : OP_STATE_FAILURE;
                        skip_insert = 1;  //** Either an error or it already exists so don't add it to the list
                    }
                }

                //** Add the path to the table
                if (err != OP_STATE_SUCCESS) error_code = 1;
                if (skip_insert == 0) tbx_list_insert(table, dname, dname);

                if (i<n-1) dname[i] = '/';
            }
        }
    }

    return(error_code);
}

//*************************************************************************
// lio_path_copy_op - Copies a regex to a dest *dir*
//*************************************************************************

gop_op_status_t lio_path_copy_op(void *arg, int id)
{
    lio_cp_path_t *cp = (lio_cp_path_t *)arg;
    lio_unified_object_iter_t *it;
    lio_path_tuple_t create_tuple;
    int ftype, prefix_len, slot, count, nerr;
    char *dstate;
    char dname[OS_PATH_MAX];
    char *fname, *dir, *file;
    tbx_list_t *dir_table;
    lio_cp_file_t *cplist, *c;
    gop_op_generic_t *gop;
    gop_opque_t *q;
    gop_op_status_t status;

    log_printf(15, "START src=%s dest=%s max_spawn=%d bufsize=" XOT " enable_local=%d\n", cp->src_tuple.path, cp->dest_tuple.path, cp->max_spawn, cp->bufsize, cp->enable_local);
    tbx_log_flush();

    it = lio_unified_object_iter_create(cp->src_tuple, cp->path_regex, cp->obj_regex, cp->obj_types, cp->recurse_depth);
    if (it == NULL) {
        info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation src_path=%s\n", cp->src_tuple.path);
        return(gop_failure_status);
    }

    tbx_type_malloc_clear(cplist, lio_cp_file_t, cp->max_spawn);
    dir_table = tbx_list_create(0, &tbx_list_string_compare, tbx_list_string_dup, tbx_list_simple_free, NULL);

    if (cp->force_dest_create) {
        create_tuple = cp->dest_tuple;
        lio_cp_create_dir(dir_table, create_tuple);
    }

    q = gop_opque_new();
    nerr = 0;
    slot = 0;
    count = 0;
    while ((ftype = lio_unified_next_object(it, &fname, &prefix_len)) > 0) {
        snprintf(dname, OS_PATH_MAX, "%s/%s", cp->dest_tuple.path, &(fname[prefix_len+1]));

        if ((ftype & OS_OBJECT_DIR_FLAG) > 0) { //** Got a directory
            dstate = tbx_list_search(dir_table, dname);
            if (dstate == NULL) { //** New dir so have to check and possibly create it
                create_tuple = cp->dest_tuple;
                create_tuple.path = dname;
                lio_cp_create_dir(dir_table, create_tuple);
            }

            free(fname);  //** Clean up
            continue;  //** Nothing else to do so go to the next file.
        }

        lio_os_path_split(dname, &dir, &file);
        dstate = tbx_list_search(dir_table, dir);
        if (dstate == NULL) { //** New dir so have to check and possibly create it
            create_tuple = cp->dest_tuple;
            create_tuple.path = dir;
            lio_cp_create_dir(dir_table, create_tuple);
        }
        if (dir) {
            free(dir);
            dir = NULL;
        }
        if (file) {
            free(file);
            file = NULL;
        }

        c = &(cplist[slot]);
        c->src_tuple = cp->src_tuple;
        c->src_tuple.path = fname;
        c->dest_tuple = cp->dest_tuple;
        c->dest_tuple.path = strdup(dname);
        c->bufsize = cp->bufsize;
        c->slow = cp->slow;
        c->enable_local = cp->enable_local;

        gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, lio_file_copy_op, (void *)c, NULL, 1);
        gop_set_myid(gop, slot);
        log_printf(1, "gid=%d i=%d sname=%s dname=%s enable_local=%d\n", gop_id(gop), slot, fname, dname, c->enable_local);
        gop_opque_add(q, gop);

        count++;

        if (count >= cp->max_spawn) {
            gop = opque_waitany(q);
            slot = gop_get_myid(gop);
            c = &(cplist[slot]);
            status = gop_get_status(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                nerr++;
                info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path);
            }
            free(c->src_tuple.path);
            free(c->dest_tuple.path);
            gop_free(gop, OP_DESTROY);
        } else {
            slot = count;
        }
    }

    lio_unified_object_iter_destroy(it);

    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        slot = gop_get_myid(gop);
        c = &(cplist[slot]);
        log_printf(15, "slot=%d fname=%s\n", slot, c->src_tuple.path);
        if (status.op_status != OP_STATE_SUCCESS) {
            nerr++;
            info_printf(lio_ifd, 0, "Failed with path %s\n", c->src_tuple.path);
        }
        free(c->src_tuple.path);
        free(c->dest_tuple.path);
        gop_free(gop, OP_DESTROY);
    }

    gop_opque_free(q, OP_DESTROY);

    free(cplist);
    tbx_list_destroy(dir_table);

    status = gop_success_status;
    if (nerr > 0) {
        status.op_status = OP_STATE_FAILURE;
        status.error_code = nerr;
    }
    return(status);
}


//***********************************************************************
// The misc I/O routines: lio_seek, lio_tell, lio_size, etc
//***********************************************************************


//***********************************************************************
// lio_seek - Sets the file position
//***********************************************************************

ex_off_t lio_seek(lio_fd_t *fd, ex_off_t offset, int whence)
{
    ex_off_t moveto;

    switch (whence) {
    case (SEEK_SET) :
        moveto = offset;
        break;
    case (SEEK_CUR) :
        segment_lock(fd->fh->seg);
        moveto = offset + fd->curr_offset;
        segment_unlock(fd->fh->seg);
        break;
    case (SEEK_END) :
        moveto = segment_size(fd->fh->seg) - offset;
        break;
    default :
        return(-EINVAL);
    }

    //** Check if the seek is out of range
    if (moveto < 0) return(-ERANGE);

    segment_lock(fd->fh->seg);
    fd->curr_offset = moveto;
    segment_unlock(fd->fh->seg);

    return(moveto);
}

//***********************************************************************
// lio_tell - Return the current position
//***********************************************************************

ex_off_t lio_tell(lio_fd_t *fd)
{
    ex_off_t offset;

    segment_lock(fd->fh->seg);
    offset = fd->curr_offset;
    segment_lock(fd->fh->seg);

    return(offset);
}


//***********************************************************************
// lio_size_fh - Return the file size using the file handle
//***********************************************************************

ex_off_t lio_size_fh(lio_file_handle_t *fh)
{
    ex_off_t size = -1;

    apr_thread_mutex_lock(fh->lock);
    if (fh->data_size >= 0) size = fh->data_size;
    apr_thread_mutex_unlock(fh->lock);

    if (size < 0) size = segment_size(fh->seg);
    return(size);
}
//***********************************************************************
// lio_size - Return the file size
//***********************************************************************

ex_off_t lio_size(lio_fd_t *fd)
{
    return(lio_size_fh(fd->fh));
}

//***********************************************************************
// lio_block_size - Return the block size for R/W
//***********************************************************************

ex_off_t lio_block_size(lio_fd_t *fd, int block_type)
{
    return(segment_block_size(fd->fh->seg, block_type));
}

//***********************************************************************
// lio_flush_fn - Flush a file to disk
//***********************************************************************

gop_op_status_t lio_flush_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    lio_file_handle_t *fh = op->slfd->fh;
    ex_off_t lo = op->offset;
    ex_off_t hi = op->offset2;
    int err;

    status = gop_success_status;  //** Default status

    apr_thread_mutex_lock(fh->lock);
    if (fh->data_size > -1) { //** Data is stored as an attribute.
        if (tbx_atomic_get(fh->modified)) { //** Changed so flush it to the LServer
            err = lio_update_exnode_attrs(op->slfd, NULL);
            tbx_atomic_set(fh->modified, 0);
            if (err) status = gop_failure_status;
        }
        apr_thread_mutex_unlock(fh->lock);
        return(status);
    }
    apr_thread_mutex_unlock(fh->lock);

    status = gop_sync_exec_status(segment_flush(fh->seg, fh->lc->da, lo, (hi == -1) ? segment_size(fh->seg)+1 : hi, fh->lc->timeout));
    return(status);
}

//***********************************************************************
// lio_flush_gop - Returns a flush GOP operation
//***********************************************************************

gop_op_generic_t *lio_flush_gop(lio_fd_t *fd, ex_off_t lo, ex_off_t hi)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->offset = lo;
    op->offset2 = hi;
    op->slfd = fd;

    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_flush_fn, (void *)op, free, 1));


    return(segment_flush(fd->fh->seg, fd->fh->lc->da, lo, (hi == -1) ? segment_size(fd->fh->seg)+1 : hi, fd->fh->lc->timeout));
}

//***********************************************************************
// lio_cache_pages_drop - Drops pages in the given range from the cache
//***********************************************************************

int lio_cache_pages_drop(lio_fd_t *fd, ex_off_t lo, ex_off_t hi)
{
    return(lio_segment_cache_pages_drop(fd->fh->seg, lo, hi));
}

//***********************************************************************
// lio_truncate - Truncates an open LIO file
//***********************************************************************

gop_op_status_t lio_truncate_fn(void *arg, int id)
{
    lio_cp_fn_t *op = (lio_cp_fn_t *)arg;
    gop_op_status_t status;
    lio_file_handle_t *fh = op->slfd->fh;

    status = gop_success_status;  //** Default status

    //** Go ahead and adjust the tier based on the new size. Then we'll do the truncate.
    apr_thread_mutex_lock(fh->lock);
    lio_adjust_data_tier(op->slfd->fh, op->bufsize, 0);

    if (fh->data_size > -1) { //** Data is stored as an attribute.
        if (fh->max_data_allocated < op->bufsize) { //** Growing the file
            _metadata_grow(fh, op->bufsize);
            fh->data_size = op->bufsize;
        } else if (fh->data_size > op->bufsize) {   //** Shrinking
            memset(fh->data + op->bufsize, 0, fh->data_size - op->bufsize);
            fh->data_size = op->bufsize;
        }
        apr_thread_mutex_unlock(fh->lock);
    } else { //** Data is in the segment
        apr_thread_mutex_unlock(fh->lock);
        status = gop_sync_exec_status(lio_segment_truncate(fh->seg, fh->lc->da, op->bufsize, fh->lc->timeout));
    }

    //** Adjust the file position
    segment_lock(fh->seg);
    tbx_atomic_set(fh->modified, 1);
    op->slfd->curr_offset = op->bufsize;
//    _lio_dec_in_flight_and_unlock(op->slfd);
    segment_unlock(fh->seg);

    return(status);
}

//***********************************************************************

gop_op_generic_t *lio_truncate_gop(lio_fd_t *fd, ex_off_t newsize)
{
    lio_cp_fn_t *op;

    tbx_type_malloc_clear(op, lio_cp_fn_t, 1);

    op->bufsize = newsize;
    op->slfd = fd;

    return(gop_tp_op_new(fd->lc->tpc_unlimited, NULL, lio_truncate_fn, (void *)op, free, 1));
}

//***********************************************************************

gop_op_generic_t *lio_segment_tool_gop(lio_fd_t *fd, ex_id_t segment_id, const char *stype, const char *match_section, const char *args_section, tbx_inip_file_t *afd, int dryrun, int timeout)
{
    return(segment_tool(fd->fh->seg, fd->lc->da, segment_id, stype, match_section, args_section, afd, dryrun, timeout));
}
