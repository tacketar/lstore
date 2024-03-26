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

//***********************************************************************
//  Routines for testing the various segment_log routines
//***********************************************************************

// Verification tasks
//
// -Generate a base with an empty log and read back
// -Clone the base structure and the use segment_copy to copy the data and verify.
// -Write to the log and read back
// -merge_with base and verify
// -Write to log
// -Replace the clones base with current log(A)
// -Write to the clone and verify B+A+base
// -clone2 = clone (structure and data). Verify the contents
// -Clone A's structure again and call it C.
// -Replace C's base with B.
//    Write to C and verify C+B+A+base changes
// -Clone C's structure and data and verify


#include <assert.h>
#include <tbx/assert_result.h>
//#include "exnode.h"
#include <lio/ex3.h>
#include <tbx/random.h>
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include "thread_pool.h"
//#include "segment_log_priv.h"
#include <lio/lio.h>
#include <lio/segment.h>
#include "../src/lio/segment/log.h"

#define validate_change(buffer, control, seg, len) \
    memset(buffer, 0, bufsize); \
log_printf(0, "verify read\n"); \
    assert_result(gop_sync_exec(segment_read(seg, lio_gc->da, NULL, 1, &ex_iov, &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS); \
    assert_result(compare_buffers_print(buffer, control, len, 0, __LINE__), 0)

//*************************************************************************
// dummy_data - Fills the buffer with either fixed or random data
//*************************************************************************

void dummy_data(char *buf, char val, int len, int do_random)
{
    char *valid = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int i, n, n_valid;

    if (do_random == 0) {
        memset(buf, val, len);
        buf[len] = 0;
        return;
    }

    n_valid = strlen(valid)-1;
    for (i=0; i<len; i++) {
        n = (do_random == 1) ? tbx_random_get_int64(0, n_valid) : (i%n_valid);
        buf[i] = valid[n];
    }
    buf[len] = 0;
}


//*************************************************************************
// dump_string - Dumps the string replacing a NULL byte with a "."
//*************************************************************************

void dump_string(char *buf, int len, int parent_line)
{
    int i;
    char c;

    for (i=0; i<len; i++) {
        c = (buf[i] == 0) ? '.' : buf[i];
        tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], parent_line, "%c", c);
    }
    tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], parent_line, "\n");
}

//*************************************************************************
// compare_buffers_print - FInds the 1st index where the buffers differ
//*************************************************************************

int compare_buffers_print(char *b1, char *b2, int len, ex_off_t offset, int parent_line)
{
    int i, k, mode, last, ok, err, last_byte;
    ex_off_t start, end;
    char *text = "0123456789";

    err = 0;
    mode = (b1[0] == b2[0]) ? 0 : 1;
    start = offset;
    last = len - 1;

    log_printf(0, "Printing comparision breakdown -- Single byte matches are suppressed (len=%d). Calling line=%d\n", len, parent_line);
    b1[len] = 0;
    b2[len] = 0;
    k = (len > 100) ? 100 : len;
    log_printf(0, "    ");
    for (i=10; i<k; i += 10) {
        tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], __LINE__, "%10d", i);
    }
    tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], __LINE__, "\n");

    log_printf(0, "   ");
    for (i=0; i<k; i += 10) {
        tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], __LINE__, "%s", text);
    }
    tbx_mlog_printf(1, _log_module_index, 0, __func__, _mlog_file_table[_log_module_index], __LINE__, "\n");

//    log_printf(15, "b1=%s\n", b1);
//    log_printf(15, "b2=%s\n", b2);
    log_printf(15, "b1="); dump_string(b1, len, parent_line);
    log_printf(15, "b2="); dump_string(b2, len, parent_line);

    for (i=0; i<len; i++) {
        if (mode == 0) {  //** Matching range
            if ((b1[i] != b2[i]) || (last == i)) {
                last_byte = ((last == i) && (b1[i] == b2[i])) ? 1 : 0;
                end = offset + i-1 + last_byte;
                k = end - start + 1;
                log_printf(0, "  MATCH : " XOT " -> " XOT " (%d bytes)\n", start, end, k);

                start = offset + i;
                mode = 1;
            }
        } else {
            if ((b1[i] == b2[i]) || (last == i)) {
                ok = 0;  //** Suppress single byte matches
                if (last != i) {
                    if (b1[i+1] == b2[i+1]) ok = 1;
                }
                if ((ok == 1) || (last == i)) {
                    last_byte = ((last == i) && (b1[i] != b2[i])) ? 1 : 0;
                    end = offset + i-1 + last_byte;
                    k = end - start + 1;
                    log_printf(0, "  DIFFER: " XOT " -> " XOT " (%d bytes)\n", start, end, k);

                    start = offset + i;
                    mode = 0;
                    err = 1;
                }
            }
        }
    }

//i=(b1[last] == b2[last]) ? 0 : 1;
//log_printf(0, "last compare=%d lst=%d\n", i, last);

    tbx_log_flush();
    return(err);
}

//*************************************************************************
// dump_and_reload_check - Dumps the segment to disk and reloads it verifying the results
//*************************************************************************

void dump_and_reload_check(lio_segment_t *seg, char *control, int len)
{
//    lio_exnode_exchange_t *exp;
//    lio_exnode_t *ex;


//    ex = lio_exnode_create();
//    exp = lio_exnode_exchange_create(EX_TEXT);
//    lio_exnode_serialize(fd->fh->ex, exp);
    
}



//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int random_base = 1;
    int random_add = 1;
    int random_update = 1;
    int chunk_size = 10;
    int n_chunks = 10;
    int bufsize = 2 * n_chunks * chunk_size;
    int len = n_chunks * chunk_size;
    char base_data[bufsize+1];
    char buffer[bufsize+1];
    char log1_data[bufsize+1];
    char log2_data[bufsize+1];
    char log3_data[bufsize+1];
    int sig_bsize=100*1024;
    char sig[sig_bsize];
    int sig_used;
    tbx_tbuf_t tbuf;
    ex_tbx_iovec_t ex_iov, ex_iov_table[n_chunks];
    int i, off, err;
    char *fname = NULL;
    lio_segment_rw_hints_t hints;
    lio_exnode_t *ex;
    lio_exnode_exchange_t *exp;
    lio_segment_t *seg, *clone, *clone2, *clone3;
    lio_seglog_priv_t *s;
    gop_opque_t *q;
    FILE *fd;

    if (argc < 2) {
        printf("\n");
        printf("log_test LIO_COMMON_OPTIONS log.ex3\n");
        lio_print_options(stdout);
        printf("    log.ex3 - Log file to use.  IF the file is not empty all it's contents are truncated\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    //** This is the remote file to download
    i = 1;
    fname = argv[i];
    i++;
    if (fname == NULL) {
        printf("Missing log file!\n");
        return(2);
    }

    //** Load it
    exp = lio_exnode_exchange_load_file(fname);

    //** and parse the remote exnode
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, lio_gc->ess) != 0) {
        printf("ERROR parsing exnode!  Aborting!\n");
        abort();
    }

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }
    s = (lio_seglog_priv_t *)seg->priv;

    lio_exnode_exchange_destroy(exp);  //** Don't need the exhcnage anymore

    //** Verify the type
    if (strcmp(segment_type(seg), SEGMENT_TYPE_LOG) != 0) {
        printf("Invalid exnode type.  Segment should be a single level log but got a type of %s\n", segment_type(seg));
        abort();
    }

    //** Now get the base type.  It should NOT be a log
    if (strcmp(segment_type(s->base_seg), SEGMENT_TYPE_LOG) == 0) {
        printf("Log segments base should NOT be another log segment!\n");
        abort();
    }


    //** Truncate the log and base
    q = gop_opque_new();
    gop_opque_add(q, lio_segment_truncate(s->table_seg, lio_gc->da, 0, lio_gc->timeout));
    gop_opque_add(q, lio_segment_truncate(s->data_seg, lio_gc->da, 0, lio_gc->timeout));
    gop_opque_add(q, lio_segment_truncate(s->base_seg, lio_gc->da, 0, lio_gc->timeout));
    err = opque_waitall(q);
    if (err != OP_STATE_SUCCESS) {
        printf("Error with truncate of initial log segment!\n");
        abort();
    }
    s->file_size = 0;
    s->data_size = 0;
    s->log_size = 0;

    //*************************************************************************
    //--------------------- Testing starts here -------------------------------
    //*************************************************************************

    //*************************************************************************
    //------- Generate a base with an empty log and read back -----------------
    //*************************************************************************
    //** Make the base buffer and write it
    dummy_data(base_data, 'b', len, random_base);
    base_data[len] = '\0';
    tbx_tbuf_single(&tbuf, bufsize, base_data);
    ex_iovec_single(&ex_iov, 0, len);
    assert_result(gop_sync_exec(segment_write(s->base_seg, lio_gc->da, NULL, 1, &ex_iov, &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    s->file_size = len;  //** Since we're peeking we have to adjust the file size
    tbx_tbuf_single(&tbuf, bufsize, buffer);  //** Read it directly back from the base to make sure that works
    validate_change(buffer, base_data, s->base_seg, len);

    //** Do the same for the log
    validate_change(buffer, base_data, seg, len);

    //*************************************************************************
    //-- Clone the base structure and the use segment_copy to copy the data and verify --
    //*************************************************************************
    clone = NULL;
    assert_result(gop_sync_exec(segment_clone(seg, lio_gc->da, &clone, CLONE_STRUCTURE, NULL, lio_gc->timeout)), OP_STATE_SUCCESS);
    assert_result(gop_sync_exec(lio_segment_copy_gop(lio_gc->tpc_unlimited, lio_gc->da, NULL, seg, clone, 0, 0, bufsize, chunk_size, buffer, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    validate_change(buffer, base_data, clone, len);

    //*************************************************************************
    //-------------------- Write to the log and read back ---------------------
    //*************************************************************************
    //** We are writing 1's to the even chunks
    memcpy(log1_data, base_data, bufsize);
    dummy_data(buffer, '1', chunk_size, random_add);
    for (i=0; i<n_chunks; i+=2) {
        memcpy(&(log1_data[i*chunk_size]), buffer, chunk_size);
        ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, chunk_size);
        gop_opque_add(q, segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[i]), &tbuf, 0, lio_gc->timeout));
    }
    assert_result(opque_waitall(q), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Verify the base wasn't touched
    validate_change(buffer, base_data, s->base_seg, len);

    //*************************************************************************
    //------------------- Write to the log in update mode----------------------
    //*************************************************************************
    //** We are writing A's to the even chunks
    memcpy(log1_data, base_data, bufsize);
    dummy_data(buffer, 'A', chunk_size, random_add);
    memset(&hints, 0, sizeof(hints));
    hints.log_write_update = 1;
    for (i=0; i<n_chunks; i+=2) {
        memcpy(&(log1_data[i*chunk_size]), buffer, chunk_size);
        ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, chunk_size);
        gop_opque_add(q, segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[i]), &tbuf, 0, lio_gc->timeout));
    }
    assert_result(opque_waitall(q), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Update mode doesn't touch the base so lets add some writes that straddleexisting log entries
    //** Left overlap
    dummy_data(buffer, 'L', chunk_size, random_update);
    off = 2*chunk_size - 2;
log_printf(0, "CHANGE: buf=%s len=%d -- off=%d change=(%d, %d)\n", buffer, chunk_size, off, 2*chunk_size, 2*chunk_size + chunk_size -2 - 1);
    memcpy(&(log1_data[2*chunk_size]), buffer+2, chunk_size - 2);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Right overlap
    dummy_data(buffer, 'R', chunk_size, random_update);
    off = 4*chunk_size + 2;
    memcpy(&(log1_data[off]), buffer, chunk_size - 2);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Full interior
    dummy_data(buffer, 'I', chunk_size, random_update);
    off = 6*chunk_size + 1;
    memcpy(&(log1_data[off]), buffer, chunk_size - 2);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size-2);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Write completely missing a log entry. So no change should appear
    dummy_data(buffer, 'm', chunk_size, random_update);
    off = 3*chunk_size;
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Larger than log entry
   dummy_data(buffer, 'V', chunk_size+2, random_update);
    off = 8*chunk_size - 1;
    memcpy(&(log1_data[off+1]), buffer+1, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size+2);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Span multiple log entries
    memset(buffer, 's', 3*chunk_size);
    dummy_data(buffer, 's', 3*chunk_size, random_update);
    off = 2*chunk_size + 2;
    memcpy(&(log1_data[off]), buffer, chunk_size-2);
    memcpy(&(log1_data[4*chunk_size]), buffer+2*chunk_size-2, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, 3*chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //*************************************************************************
    //------ Do a few tests with adjacent, non-overlapping intervals ----------
    //*************************************************************************

    //** Fill in the gap between 2 intervals in add mode
    dummy_data(buffer, 'g', chunk_size, random_add);
    off = 3*chunk_size;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Now do an update
    dummy_data(buffer, 'e', 2*chunk_size, random_update);
    off = 2*chunk_size + 5;
    memcpy(&(log1_data[off]), buffer, 2*chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, 2*chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //*************************************************************************
    //------------------- Overlapping interval tests --------------------------
    //*************************************************************************

    //** Overwrite a portion of an existing range
    dummy_data(buffer, 'q', chunk_size, random_add);
    off = 3*chunk_size+3;
    memcpy(&(log1_data[off]), buffer, 3);
    ex_iovec_single(&(ex_iov_table[0]), off, 3);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Span 2 intervals
    dummy_data(buffer, 'w', chunk_size, random_add);
    off = 2*chunk_size+2;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Completely cover a couple of intervals
    dummy_data(buffer, 's', 2*chunk_size+2, random_add);
    off = 2*chunk_size-1;
    memcpy(&(log1_data[off]), buffer, 2*chunk_size+2);
    ex_iovec_single(&(ex_iov_table[0]), off, 2*chunk_size+2);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Try and do an update over a subset
    dummy_data(buffer, 't', chunk_size, random_update);
    off = 2*chunk_size;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, &hints, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Add an interval that overlaps base+range
    dummy_data(buffer, 'y', chunk_size, random_add);
    off = 6*chunk_size-3;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Add an interval that overlaps range+base
    dummy_data(buffer, 'u', chunk_size, random_add);
    off = 7*chunk_size-3;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Add interval that spans interval+base+interval
    dummy_data(buffer, 'y', 2*chunk_size, random_add);
    off = 6*chunk_size+5;
    memcpy(&(log1_data[off]), buffer, 2*chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, 2*chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //*************************************************************************
    //------------------- Merge_with base and verify --------------------------
    //*************************************************************************
    assert_result(gop_sync_exec(lio_slog_merge_with_base_gop(seg, lio_gc->da, chunk_size, buffer, 1, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //** Verify it was smashed into the base
    validate_change(buffer, log1_data, s->base_seg, len);

    //*************************************************************************
    //--------------- Write to the new empty log and verify -------------------
    //*************************************************************************
    //** We are writing 2's to *most* of the odd chunks
    memcpy(log1_data, buffer, bufsize);
    dummy_data(buffer, '2', chunk_size, random_add);
    for (i=1; i<n_chunks; i+=2) {
        memcpy(&(log1_data[i*chunk_size]), buffer, chunk_size);
        ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, chunk_size);
        gop_opque_add(q, segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[i]), &tbuf, 0, lio_gc->timeout));
    }
    assert_result(opque_waitall(q), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log1_data, seg, len);

    //*************************************************************************
    //------- Now lets try a few operations that grow/shrink the file ---------
    //*************************************************************************
    //** Grow the file with a write
    len = 10*chunk_size + chunk_size;
    dummy_data(buffer, 'a', chunk_size, random_add);
    off = 10*chunk_size;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    ex_iovec_single(&ex_iov, 0, len);  //** This is used for the buffer operation
    validate_change(buffer, log1_data, seg, len);

    //** Shrink it
    len = 5*chunk_size;
    assert_result(gop_sync_exec(lio_segment_truncate(seg, lio_gc->da, len, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    ex_iovec_single(&ex_iov, 0, len);  //** This is used for the buffer operation
    validate_change(buffer, log1_data, seg, len);

    //** Grow it again
    len = 5*chunk_size + chunk_size;
    dummy_data(buffer, 'b', chunk_size, random_add);
    off = 5*chunk_size;
    memcpy(&(log1_data[off]), buffer, chunk_size);
    ex_iovec_single(&(ex_iov_table[0]), off, chunk_size);
    assert_result(gop_sync_exec(segment_write(seg, lio_gc->da, NULL, 1, &(ex_iov_table[0]), &tbuf, 0, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    ex_iovec_single(&ex_iov, 0, len);  //** This is used for the buffer operation
    validate_change(buffer, log1_data, seg, len);

    //** Grow it with a truncate op
    off = len;
    len = 8*chunk_size;
    memset(log1_data + off, 0, len - off + 1);
    assert_result(gop_sync_exec(lio_segment_truncate(seg, lio_gc->da, len, lio_gc->timeout)), OP_STATE_SUCCESS);

    //** Read it back
    ex_iovec_single(&ex_iov, 0, len);  //** This is used for the buffer operation
    validate_change(buffer, log1_data, seg, len);

    //*************************************************************************
    //---------- Replace the clones base with seg(Log1) and verify ------------
    //*************************************************************************
    assert_result(gop_sync_exec(segment_remove(clone, lio_gc->da, lio_gc->timeout)), OP_STATE_SUCCESS);
    segment_destroy(clone);
    clone = NULL;
    assert_result(gop_sync_exec(segment_clone(seg, lio_gc->da, &clone, CLONE_STRUCTURE, NULL, lio_gc->timeout)), OP_STATE_SUCCESS);

sig_used = 0;
log_printf(0, "BASE signature start\n");
segment_signature(seg, sig, &sig_used, sig_bsize);
log_printf(0, "BASE signature end\n");

    s = (lio_seglog_priv_t *)clone->priv;

    segment_destroy(s->base_seg);  //** Destroy the unsed base
    s->base_seg = seg;
    s->file_size = segment_size(seg);

sig_used = 0;
log_printf(0, "CLONE signature start\n");
segment_signature(clone, sig, &sig_used, sig_bsize);
log_printf(0, "CLONE signature end\n");
tbx_log_flush();

    //** Read it back
    validate_change(buffer, log1_data, clone, len);

    //*************************************************************************
    //---------- Write to the clones log and verify (now have 2 logs) ---------
    //*************************************************************************
    memcpy(log2_data, log1_data, len);
    dummy_data(buffer, '3', 1.5*chunk_size, random_add);
    for (i=0; i<n_chunks; i+=4) {
        memcpy(&(log2_data[i*chunk_size]), buffer, 1.5*chunk_size);
        ex_iovec_single(&(ex_iov_table[i]), i*chunk_size, 1.5*chunk_size);
        gop_opque_add(q, segment_write(clone, lio_gc->da, NULL, 1, &(ex_iov_table[i]), &tbuf, 0, lio_gc->timeout));
    }
    assert_result(opque_waitall(q), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log2_data, clone, len);

    //*************************************************************************
    //---- clone2 = clone (structure and data). Verify the contents -----------
    //*************************************************************************
    clone2 = NULL;
    assert_result(gop_sync_exec(segment_clone(clone, lio_gc->da, &clone2, CLONE_STRUCT_AND_DATA, NULL, lio_gc->timeout)), OP_STATE_SUCCESS);
    validate_change(buffer, log2_data, clone2, len);

    //** We don't need this anymore so destroy it
    assert_result(gop_sync_exec(segment_remove(clone2, lio_gc->da, lio_gc->timeout)), OP_STATE_SUCCESS);
    segment_destroy(clone2);

    //*************************************************************************
    //---------------- Clone2 = clone's structure *only* ----------------------
    //*************************************************************************
    clone2 = NULL;
    assert_result(gop_sync_exec(segment_clone(clone, lio_gc->da, &clone2, CLONE_STRUCTURE, NULL, lio_gc->timeout)), OP_STATE_SUCCESS);

    //*************************************************************************
    //-------------- Replace clone2's base with clone and verify --------------
    //*************************************************************************
    s = (lio_seglog_priv_t *)clone2->priv;
    assert_result(gop_sync_exec(segment_remove(s->base_seg, lio_gc->da, lio_gc->timeout)), OP_STATE_SUCCESS);
    segment_destroy(s->base_seg);

    s->base_seg = clone;
    s->file_size = segment_size(clone);

    //** Read it back
    validate_change(buffer, log2_data, clone2, len);

    //*************************************************************************
    //----------- Write to Clone2 and verify (now have 3 logs) ----------------
    //*************************************************************************
    memcpy(log3_data, log2_data, bufsize);
    dummy_data(buffer, '4', chunk_size, random_add);
    for (i=0; i<n_chunks; i+=2) {
        memcpy(&(log3_data[i*chunk_size + chunk_size/3]), buffer, chunk_size);
        ex_iovec_single(&(ex_iov_table[i]), i*chunk_size + chunk_size/3, chunk_size);
        gop_opque_add(q, segment_write(clone2, lio_gc->da, NULL, 1, &(ex_iov_table[i]), &tbuf, 0, lio_gc->timeout));
    }
    assert_result(opque_waitall(q), OP_STATE_SUCCESS);

    //** Read it back
    validate_change(buffer, log3_data, clone2, len);

sig_used = 0;
log_printf(0, "CLONE2 signature start\n");
segment_signature(clone2, sig, &sig_used, sig_bsize);
log_printf(0, "CLONE2 signature end\n");
tbx_log_flush();

    //** Dump it to disk for later retrieval
    exp = lio_exnode_exchange_create(EX_TEXT);
    segment_serialize(clone2, exp);

    //** Dump it to disk
    fd = NULL;
    assert((fd = fopen("/tmp/clone_exact.ex3", "w")) != NULL);
    fprintf(fd, "%s\n", exp->text.text);
    fprintf(fd, "[view]\n");
    fprintf(fd, "default = " XOT "\n", segment_id(clone2));
    fprintf(fd, "segment = " XOT "\n", segment_id(clone2));
    fclose(fd);

    lio_exnode_exchange_destroy(exp);


    //*************************************************************************
    // -- clone3 = clone2 structure and contents and verify
    //*************************************************************************
    clone3 = NULL;
    assert_result(gop_sync_exec(segment_clone(clone2, lio_gc->da, &clone3, CLONE_STRUCT_AND_DATA, NULL, lio_gc->timeout)), OP_STATE_SUCCESS);
    validate_change(buffer, log3_data, clone3, len);

    //** Dump it to disk for later retrieval
    exp = lio_exnode_exchange_create(EX_TEXT);
    segment_serialize(clone3, exp);

    //** Dump it to disk
    assert((fd = fopen("/tmp/clone.ex3", "w")) != NULL);
    fprintf(fd, "%s\n", exp->text.text);
    fprintf(fd, "[view]\n");
    fprintf(fd, "default = " XOT "\n", segment_id(clone3));
    fprintf(fd, "segment = " XOT "\n", segment_id(clone3));
    fclose(fd);

    lio_exnode_exchange_destroy(exp);

    //*************************************************************************
    //----- Now clean up all the segment and read them back from disk ---------
    //*************************************************************************

    //** Clean up the various segments we used for testing
    tbx_obj_get(&(seg->obj));   //** Need to increment the original segment since it's embedded in the exnode
    segment_destroy(clone3);
    segment_destroy(clone2);

    lio_exnode_destroy(ex);   //** This has the original seg and this call properly destroys it.

    //** Now read back the normal cloned segment
    exp = lio_exnode_exchange_load_file("/tmp/clone.ex3");
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, lio_gc->ess) != 0) {
        printf("ERROR parsing exnode!  Aborting!\n");
        abort();
    }

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    lio_exnode_exchange_destroy(exp);  //** Don't need the exhcnage anymore

    //** And verify everything is good
    validate_change(buffer, log3_data, seg, len);

    //** Destroy it
    lio_exnode_destroy(ex);

    //** ---- Now read back the exact cloned segment
    exp = lio_exnode_exchange_load_file("/tmp/clone_exact.ex3");
    ex = lio_exnode_create();
    if (lio_exnode_deserialize(ex, exp, lio_gc->ess) != 0) {
        printf("ERROR parsing exnode!  Aborting!\n");
        abort();
    }

    //** Get the default view to use
    seg = lio_exnode_default_get(ex);
    if (seg == NULL) {
        printf("No default segment!  Aborting!\n");
        abort();
    }

    lio_exnode_exchange_destroy(exp);  //** Don't need the exhcnage anymore

    //** And verify everything is good
    validate_change(buffer, log3_data, seg, len);

    //** Destroy it
    lio_exnode_destroy(ex);

    //*************************************************************************
    //--------------------- Testing Finished -------------------------------
    //*************************************************************************

    gop_opque_free(q, OP_DESTROY);

    //** Clean up
//    assert_result(gop_sync_exec(segment_remove(seg, lio_gc->da, lio_gc->timeout)), OP_STATE_SUCCESS);

    lio_shutdown();

    return(0);
}
