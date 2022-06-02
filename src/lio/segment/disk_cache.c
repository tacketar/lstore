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
#include <tbx/io.h>
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

//**Hack until packaged version of RocksDB catches up with git
#ifdef _ROCKSDB_CANCEL_MISSING
void rocksdb_cancel_all_background_work(rocksdb_t* db, unsigned char wait);
#endif

//** Cache global modes
#define SDC_MODE_BYPASS  0    //** Waiting on the lock
#define SDC_MODE_ACQUIRE 1    //** Have the lock butneed to do some house cleaning
#define SDC_MODE_CACHE   2    //** Full control of the shared cache

// Forward declaration
const lio_segment_vtable_t lio_disk_cache_vtable;

//** DB Key's

typedef struct {  //** Dirty DB key: {sid, page}
    ex_id_t sid;
    ex_off_t page;
} sdc_dirty_key_t;

typedef struct {  //** Full LRU last_used DB key: {last_used, sid}.  **NOTE: this index is stored in REVERSE order to make iterating possible
    ex_id_t sid;
    apr_time_t last_used;
} sdc_lru_key_t;

typedef struct {  //**Cache data DB key: {sid, page}
    ex_id_t sid;
    ex_off_t page;
} sdc_data_key_t;

//** DB Records

typedef struct { //** Open files DB record
    ex_id_t magic;
    char path[];   //* Flexible array for the path
}  sdc_open_files_record_t;

typedef struct {  // Full SID info DB record
    ex_id_t magic;
    apr_time_t last_used;
} sdc_sinfo_record_t;

//********************

typedef struct { //** Generic container for a DB
    char *path;                     //** Path to the DB
    rocksdb_t *db;                  //** DB
    rocksdb_comparator_t *compare;  //** Compare function
    rocksdb_writeoptions_t *wopts;  //** Generic option for Write
    rocksdb_readoptions_t *ropts;   //** Generic option for Read
} sdc_db_t;

typedef struct {  //** Bypass open file info
    int did_write;
    int is_open;
} sdc_bypass_open_t;

typedef struct { //** Shared caching structure
    char *group;                 //** Ini file group
    char *loc;                   //** Directory with all the DB's in it
    int mode;                    //** Cahcing mode: STANDBY, ACQUIRE, CACHE
    int shutdown;                //** Flag to trigger a shutdown
    sdc_db_t db_open;            //** Open files DB
    sdc_db_t db_dirty;           //** Dirty pages DB
    sdc_db_t db_sinfo;           //** Full Segment info (magic, last used) DB
    sdc_db_t db_lru;             //** LRU/ last used DB
    sdc_db_t db_data;            //** File Page data DB
    apr_hash_t *bypass_open_files;  //** Files opened while in bypass mode
    apr_thread_mutex_t *lock;    //** Lock
    apr_thread_t *lock_thread;   //** Lock thread
    apr_pool_t *pool;            //** Memory pool
    FILE *fd_lock;               //** Shared lock file
} sdc_context_t;

typedef struct {
    char *fname;
    char *qname;
    gop_thread_pool_context_t *tpc;
    tbx_atomic_int_t hard_errors;
    tbx_atomic_int_t soft_errors;
    tbx_atomic_int_t write_errors;
    lio_segment_t *child_seg;
    sdc_context_t *ctx;
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

    FILE *fd = tbx_io_fopen(s->fname, "r+");
    if (fd == NULL) fd = tbx_io_fopen(s->fname, "w+");

    log_printf(15, "segdc_rw_func: fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " mode=%d\n", s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, srw->mode);
    tbx_log_flush();
    tbx_tbuf_var_init(&tbv);

    blen = srw->len;
    boff = srw->boff;
    bleft = blen;
    err_cnt = 0;
    for (i=0; i<srw->n_iov; i++) {
        tbx_io_fseeko(fd, srw->iov[i].offset, SEEK_SET);
        bleft = srw->iov[i].len;
        err = gop_success_status;
        while ((bleft > 0) && (err.op_status == OP_STATE_SUCCESS)) {
            tbv.nbytes = bleft;
            tbx_tbuf_next(srw->buffer, boff, &tbv);
            blen = tbv.nbytes;
            if (srw->mode == 0) {
                nbytes = tbx_io_readv(fileno(fd), tbv.buffer, tbv.n_iov);
            } else {
                nbytes = tbx_io_writev(fileno(fd), tbv.buffer, tbv.n_iov);
            }

            int ib = blen;
            int inb = nbytes;
            log_printf(15, "segdc_rw_func: fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " blen=%d nbytes=%d err_cnt=%d\n", s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, ib, inb, err_cnt);
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
        log_printf(15, "segdc_rw_func: ERROR fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
        tbx_atomic_inc(s->hard_errors);
        if (srw->mode != 0) tbx_atomic_inc(s->write_errors);
    }

    log_printf(15, "segdc_rw_func: fname=%s n_iov=%d off[0]=" XOT " len[0]=" XOT " bleft=" XOT " err_cnt=%d\n", s->fname, srw->n_iov, srw->iov[0].offset, srw->iov[0].len, bleft, err_cnt);
    tbx_log_flush();
    tbx_io_fclose(fd);
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
// segdc_tool - Returns the tool GOP
//***********************************************************************

gop_op_generic_t *segdc_tool(lio_segment_t *seg, data_attr_t *da, ex_id_t sid, const char *stype, const char *match_section, const char *args_section, tbx_inip_file_t *fd, int dryrun, int timeout)
{
    segdc_priv_t *s = (segdc_priv_t *)seg->priv;

    return(segment_tool(s->child_seg, da, sid, stype, match_section, args_section, fd, dryrun, timeout));
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

//***********************************************************************
// --------- Context related functions go below here ----------
//***********************************************************************

//****************************************************************************
// db_sid_compare_op - Comparator routine for the SID DB
//****************************************************************************

int db_sid_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    ex_id_t aid = *(ex_id_t *)a;
    ex_id_t bid = *(ex_id_t *)b;

    if (aid > bid) {
        return(1);
    } else if (aid < bid) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

void db_generic_compare_destroy(void *arg) { return; }
const char *db_sid_compare_name(void *arg) { return("SID"); }

//****************************************************************************
// db_page_compare_op - Comparator routine for the pages pages DB
//****************************************************************************

int db_page_compare_op(void *arg, const char *aptr, size_t alen, const char *bptr, size_t blen)
{
    sdc_context_t *sdc = (sdc_context_t *)arg;
    sdc_dirty_key_t *a = (sdc_dirty_key_t *)aptr;
    sdc_dirty_key_t *b = (sdc_dirty_key_t *)bptr;
    int n;

    n = db_sid_compare_op(sdc, (char *)&(a->sid), sizeof(ex_id_t), (char *)&(b->sid), sizeof(ex_id_t));
    if (n == 0) {
        if (a->page > b->page) {
            return(1);
        } else if (a->page == b->page) {
            return(0);
        }
        return(-1);
    }

    return(n);
}

const char *db_page_compare_name(void *arg) { return("PAGE"); }

//****************************************************************************
// db_lru_compare_op - Comparator routine for the LRU or  last used DB
//    **** NOTE: This is sorts the objects in reverse order, ie oldest->newest.
//****************************************************************************

int db_lru_compare_op(void *arg, const char *aptr, size_t alen, const char *bptr, size_t blen)
{
    sdc_lru_key_t *a = (sdc_lru_key_t *)aptr;
    sdc_lru_key_t *b = (sdc_lru_key_t *)bptr;

    //** check base on last used
    if (a->last_used > b->last_used) {
        return(-1);
    } else if (a->last_used < b->last_used) {
        return(1);
    }

    //** So the partition and the time stamps are the same so compare the whole SIDs
    if (a->sid > b->sid) {
        return(-1);
    } else if (a->sid == b->sid) {
        return(0);
    }

    return(1);
}

const char *db_lru_compare_name(void *arg) { return("LRU"); }

//*************************************************************************
// sdc_open_db - Opens the RocksDB database using the given path
//*************************************************************************

void sdc_open_db(sdc_db_t *dbc, char *db_path, rocksdb_comparator_t *cmp, int do_wipe)
{
    rocksdb_options_t *opts, *opts2;
    char *errstr = NULL;

    dbc->path = strdup(db_path);

    if (do_wipe != 0) { //** Wipe the DB if requested
        opts2 = rocksdb_options_create();
        rocksdb_destroy_db(opts2, db_path, &errstr);
        rocksdb_options_destroy(opts2);
    }

    //** Try opening it for real
    opts = rocksdb_options_create();
    rocksdb_options_set_comparator(opts, cmp);
    rocksdb_options_set_create_if_missing(opts, 1);

    dbc->db = rocksdb_open(opts, db_path, &errstr);
    if (errstr != NULL) {  //** An Error occured
        fprintf(stderr, "ERROR: Failed Opening/Creating %s. DB error:%s\n", db_path, errstr);
        exit(1);
    }

    rocksdb_options_destroy(opts);

    //** And the generic RW opt
    dbc->wopts = rocksdb_writeoptions_create();
    dbc->ropts = rocksdb_readoptions_create();
}

//*************************************************************************
// sdc_close_db - Closes the RocksDB database
//*************************************************************************

void sdc_close_db(sdc_db_t *dbc)
{
    rocksdb_cancel_all_background_work(dbc->db, 1);
    rocksdb_close(dbc->db);
    rocksdb_comparator_destroy(dbc->compare);
    rocksdb_writeoptions_destroy(dbc->wopts);
    rocksdb_readoptions_destroy(dbc->ropts);

    free(dbc->path);
}

//***********************************************************************
// dbc_iter_next - Gets the next key for the data DB
//***********************************************************************

void *dbc_iter_key_advance(sdc_context_t *sdc, rocksdb_iterator_t *it, int nbytes, int do_advance)
{
    size_t n = 0;
    void *kptr = NULL;

    if(rocksdb_iter_valid(it) != 0) {
        if (do_advance) rocksdb_iter_next(it);
        kptr = (void *)rocksdb_iter_key(it, &n);
        if ((size_t)nbytes != n) return(NULL);
    }

    return(kptr);

}

//*************************************************************************
// dbc_data_iter_delete_sid - Removes all the data entries with the SID and
//     advances the iterator to the next sid.
//*************************************************************************

void dbc_data_iter_delete_sid(sdc_context_t *sdc, rocksdb_iterator_t *it, ex_id_t sid)
{
    sdc_data_key_t key, *kptr;
    char *errstr;

    kptr = dbc_iter_key_advance(sdc, it, sizeof(sdc_data_key_t), 0);
    do {
        if (kptr->sid == sid) {
            key = *kptr;
            rocksdb_delete(sdc->db_sinfo.db, sdc->db_sinfo.wopts, (const char *)&key, sizeof(sdc_data_key_t), &errstr);
            if (errstr != NULL) { free(errstr); errstr = NULL; }
            kptr = dbc_iter_key_advance(sdc, it, sizeof(sdc_data_key_t), 1);
        } else {   //** SID has changed so kick out
            return;
        }
    } while (kptr);
}


//***********************************************************************
//  sdc_dirty_cleanup - Cleans up the open files and dirty pages DBs
//    These should be empty if they aren't then we could have an internal
//    issue so throw an error to trigger a data/sinfo DB consistency check
//***********************************************************************

int sdc_dirty_cleanup(sdc_context_t *sdc)
{
    rocksdb_iterator_t *it_dirty;
    sdc_db_t *db_open, *db_dirty;
    sdc_dirty_key_t *dkptr;
    sdc_dirty_key_t dkey;
//    ex_off_t offset;
//    size_t nbytes;
    char *path;

    db_open = &(sdc->db_open);
    db_dirty = &(sdc->db_dirty);

    //** Make the iterator
    it_dirty = rocksdb_create_iterator(db_dirty->db, db_dirty->ropts);

    //** Iterate through the dirty pages
    dkey.sid = 0; dkey.page = 0;
    while (rocksdb_iter_valid(it_dirty) != 0) {
        dkptr = dbc_iter_key_advance(sdc, it_dirty, sizeof(sdc_dirty_key_t), 1);

        if (dkptr->sid != dkey.sid) { //** Got a new SID
            //** Flush the existing data to disk
            
            //** See if we can find it in the open files

            //** Make a new table
        } else {  //** Same SID so see if the offset is an increment

        }

    }

    //** Now purge the open files and dirty DB's
    path = strdup(db_open->path);
    sdc_close_db(db_open);
    sdc_open_db(db_open, path, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_sid_compare_op, db_sid_compare_name), 0);
    free(path);

    path = strdup(db_dirty->path);
    sdc_close_db(db_dirty);
    sdc_open_db(db_dirty, path, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_page_compare_op, db_page_compare_name), 0);
    free(path);

    //** Cleanup
    rocksdb_iter_destroy(it_dirty);
    return(0);
}

//***********************************************************************
//  sdc_data_cleanup - Cleans up the sinfo and data DBs and also
//     regenerates the LRU
//***********************************************************************

int sdc_data_cleanup(sdc_context_t *sdc)
{
    rocksdb_iterator_t *it_sinfo, *it_data;
    sdc_db_t *db_sinfo, *db_data, *db_lru;
    sdc_lru_key_t klru;
    sdc_data_key_t key_data, *key_data_ptr;
    sdc_sinfo_record_t *rsinfo;
    ex_id_t key_sinfo, *key_sinfo_ptr, sid;
    char *path, *errstr;
    size_t n;

    db_sinfo = &(sdc->db_sinfo);
    db_data = &(sdc->db_data);
    db_lru = &(sdc->db_lru);

    //** Destroy the LRU table and recreate it
    path = strdup(db_lru->path);
    sdc_close_db(db_lru);
    sdc_open_db(db_lru, path, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_lru_compare_op, db_lru_compare_name), 1);
    free(path);

    //** Make the iterators
    it_sinfo = rocksdb_create_iterator(db_sinfo->db, db_sinfo->ropts);
    it_data = rocksdb_create_iterator(db_data->db, db_data->ropts);

    //** Set up things for looping
    key_data.sid = 0; key_data.page = 0;
    key_sinfo = 0;
    rocksdb_iter_seek(it_sinfo, (void *)&key_sinfo, sizeof(ex_id_t));
    rocksdb_iter_seek(it_data, (void *)&key_data, sizeof(sdc_data_key_t));
    key_data_ptr = dbc_iter_key_advance(sdc, it_data, sizeof(sdc_data_key_t), 0);
    key_sinfo_ptr = dbc_iter_key_advance(sdc, it_sinfo, sizeof(ex_id_t), 0);
    while ((key_data_ptr) || (key_sinfo_ptr)) {
        if ((key_data_ptr) && (key_sinfo_ptr)) { //** Got 2 valid keys
            if (key_data_ptr->sid == *key_sinfo_ptr) { //** Same SIDS so skip to the next key for both
                //** Add the LRU entry
                rsinfo = (void *)rocksdb_iter_key(it_sinfo, &n);
                klru.sid = *key_sinfo_ptr; klru.last_used = rsinfo->last_used;
                rocksdb_put(db_lru->db, db_lru->wopts, (const char *)&klru, sizeof(sdc_lru_key_t), (const char *)NULL, 0, &errstr);
                if (errstr != NULL) { free(errstr); errstr = NULL; }

                //**Advance to the next entry on both
                key_sinfo_ptr = dbc_iter_key_advance(sdc, it_sinfo, sizeof(ex_id_t), 1);
                key_data.sid = key_data_ptr->sid + 1;
                key_data.page = 0;
                rocksdb_iter_seek(it_data, (const char *)&key_data, sizeof(sdc_data_key_t));
                key_data_ptr = dbc_iter_key_advance(sdc, it_data, sizeof(sdc_data_key_t), 0);
            } else if (key_data_ptr->sid < *key_sinfo_ptr) { //** Delete the data SID entries
                sid = key_data_ptr->sid;
                dbc_data_iter_delete_sid(sdc, it_data, sid);  //** This also advances the pointer to the next SID
                key_data_ptr = dbc_iter_key_advance(sdc, it_data, sizeof(sdc_data_key_t), 0);
            } else {   //** Delete the sinfo SID entry
                sid = *key_sinfo_ptr;
                key_sinfo_ptr = dbc_iter_key_advance(sdc, it_sinfo, sizeof(ex_id_t), 1);
                rocksdb_delete(db_sinfo->db, db_sinfo->wopts, (const char *)&sid, sizeof(ex_id_t), &errstr);
                if (errstr != NULL) { free(errstr); errstr = NULL; }
            }
        } else if (key_data_ptr) { //** Only the data DB has a key
            sid = key_data_ptr->sid;
            dbc_data_iter_delete_sid(sdc, it_data, sid);  //** This also advances the pointer to the next SID
            key_data_ptr = dbc_iter_key_advance(sdc, it_data, sizeof(sdc_data_key_t), 0);
        } else {   //** Only the sinfo DB has a valid key
            sid = *key_sinfo_ptr;
            rocksdb_delete(db_sinfo->db, db_sinfo->wopts, (const char *)&sid, sizeof(ex_id_t), &errstr);
            if (errstr != NULL) { free(errstr); errstr = NULL; }
        }
    }

    //** Cleanup
    rocksdb_iter_destroy(it_sinfo);
    rocksdb_iter_destroy(it_data);

    return(0);
}

//***********************************************************************
// sdc_shared_lock_try - Attempts to get the global shared lock
//***********************************************************************

int sdc_shared_lock_try(sdc_context_t *sdc)
{
    int n;

    n = (flock(fileno(sdc->fd_lock), LOCK_EX|LOCK_NB) == 0) ? 1 : 0;
    if (n) {
        fprintf(sdc->fd_lock, "%-20d\n", getpid());
    }
    return(n);
}

//***********************************************************************
//  sdc_import_cache - We've got the lock so import the cache
//***********************************************************************

void sdc_import_cache(sdc_context_t *sdc)
{
    char fname[4096];
    int err;

    //** Open the DBs
    //*** Create/Open the DB for tracking open files ***
    snprintf(fname, sizeof(fname), "%s/open", sdc->loc);
    sdc_open_db(&(sdc->db_open), fname, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_sid_compare_op, db_sid_compare_name), 0);

    //*** Create/Open the DB for tracking dirty pages ***
    snprintf(fname, sizeof(fname), "%s/dirty", sdc->loc);
    sdc_open_db(&(sdc->db_dirty), fname, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_page_compare_op, db_page_compare_name), 0);

    //*** Create/Open the DB for segment info ***
    snprintf(fname, sizeof(fname), "%s/sinfo", sdc->loc);
    sdc_open_db(&(sdc->db_sinfo), fname, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_sid_compare_op, db_sid_compare_name), 0);

    //*** Create/Open the DB for the LRU / last used DB ***
    snprintf(fname, sizeof(fname), "%s/lru", sdc->loc);
    sdc_open_db(&(sdc->db_lru), fname, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_lru_compare_op, db_lru_compare_name), 0);

    //*** Create/Open the DB for tracking data pages ***
    snprintf(fname, sizeof(fname), "%s/data", sdc->loc);
    sdc_open_db(&(sdc->db_data), fname, rocksdb_comparator_create(sdc, db_generic_compare_destroy, db_page_compare_op, db_page_compare_name), 0);

    //** change the mode to let everyone know we're acquiring the lock
    apr_thread_mutex_lock(sdc->lock);
    sdc->mode = SDC_MODE_ACQUIRE;
    apr_thread_mutex_unlock(sdc->lock);

    //** This is temporary and will be replaced in the future when implementing a proper handoff to another process
    err = sdc_dirty_cleanup(sdc);   //** We always check the dirty and Open DB just to be safe
    if (err) sdc_data_cleanup(sdc); //** Do a data/sinfo cleanup and regenerate the LRU

    //** Change the mode to let everyone know we're ready
    apr_thread_mutex_lock(sdc->lock);
    sdc->mode = SDC_MODE_CACHE;
    apr_thread_mutex_unlock(sdc->lock);
}

//***********************************************************************
//  sdc_lock_thread - Monitors the lock file and attempts to acquire it
//***********************************************************************

void *sdc_shared_lock_thread(apr_thread_t *th, void *arg)
{
    sdc_context_t *sdc = arg;
    int kick_out = 0;

    tbx_monitor_thread_create(MON_MY_THREAD, "sdc_shared_lock_thread");
    while (sdc_shared_lock_try(sdc) == 0) {
        apr_sleep(apr_time_from_sec(1));  //** Sleep

        apr_thread_mutex_lock(sdc->lock);    //** check if we should give up
        kick_out = sdc->shutdown;
        apr_thread_mutex_unlock(sdc->lock);

        if (kick_out) goto finished; //** If so kick out
    }

    //** Alright we got the lock sp now
    //** Make sure the initialization has completed
    while (lio_gc == NULL) {
        apr_sleep(apr_time_from_sec(1));  //** Sleep
    }
    apr_sleep(apr_time_from_sec(1));  //** Sleep to make sure the pointer is fully stored

    //** Now wait until lio_init has finished
    apr_thread_mutex_lock(lio_gc->lock);
    while (lio_gc->init_complete == 0) {
        apr_thread_mutex_unlock(lio_gc->lock);
        apr_sleep(apr_time_from_sec(1));  //** Sleep
        apr_thread_mutex_lock(lio_gc->lock);
    }
    apr_thread_mutex_unlock(lio_gc->lock);

    //** Now import the cache.
    sdc_import_cache(sdc);

finished:
    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
//  sdc_import_cache - Set up things to bypass the cache
//***********************************************************************

void sdc_bypass_cache(sdc_context_t *sdc)
{
    //** Set the initial mode
    sdc->mode = SDC_MODE_BYPASS;

    //** This is where we track all the open files while in bypass mode
    sdc->bypass_open_files = apr_hash_make(sdc->pool);

}

//***********************************************************************
// segment_disk_cache_context_create - Creates the global disk cache context
//***********************************************************************

void *segment_disk_cache_context_create(void *arg, tbx_inip_file_t *fd, char *grp)
{
    sdc_context_t *sdc;
    char fname[4096];

    tbx_type_malloc_clear(sdc, sdc_context_t, 1);

    sdc->group = strdup(grp);
    sdc->loc = tbx_inip_get_string(fd, grp, "loc", "/lio/cache");

    //** See if we can get the lock
    snprintf(fname, sizeof(fname), "%s/lock_pid", sdc->loc);
    sdc->fd_lock = tbx_io_fopen(fname, "w+");
    if (!sdc->fd_lock) {
        log_printf(0, "ERROR: Unable to open lock file: %s\n", fname);
        fprintf(stderr, "ERROR: Unable to open lock file: %s\n", fname);
        abort();
    }

    sdc_bypass_cache(sdc);  //** Configure cache bypass mode while we wait

    // ** Launch the backend thread
    tbx_thread_create_assert(&(sdc->lock_thread), NULL, sdc_shared_lock_thread, (void *)sdc, sdc->pool);

    return(sdc);
}

//***********************************************************************
// segment_disk_cache_context_destroy - Destroys the disk cache context
//***********************************************************************

void segment_disk_cache_context_destroy(void *arg)
{
    sdc_context_t *sdc = (sdc_context_t *)arg;
    apr_status_t value;

    //** Let everyone know we're shutting down
    apr_thread_mutex_lock(sdc->lock);
    sdc->shutdown = 1;
    apr_thread_mutex_unlock(sdc->lock);

    if (sdc->lock_thread != NULL) { //** Need to check the lock thread and shut it down
        apr_thread_join(&value, sdc->lock_thread);
    }

    //** Close all the DBs
    sdc_close_db(&(sdc->db_open));
    sdc_close_db(&(sdc->db_dirty));
    sdc_close_db(&(sdc->db_sinfo));
    sdc_close_db(&(sdc->db_lru));
    sdc_close_db(&(sdc->db_data));

    tbx_io_fclose(sdc->fd_lock);  //** This also releases the lock

    //** Clean up
    free(sdc->group);
    free(sdc->loc);
    free(sdc);
}

//***********************************************************************
// Static function table
//***********************************************************************

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
        .tool = segdc_tool,
        .block_size = segdc_block_size,
        .serialize = segdc_serialize,
        .deserialize = segdc_deserialize,
};
