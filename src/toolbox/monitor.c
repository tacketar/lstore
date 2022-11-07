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

#include <unistd.h>
#include <sys/syscall.h>
#include <apr_pools.h>
#include <apr_hash.h>
#include <apr_time.h>
#include <apr_thread_mutex.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/io.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/monitor.h>

#define BUFSIZE 16384
#define TID_INDEX 256
#define EMPTY_LABEL ""

typedef struct {
    apr_time_t dt;
    int32_t tid;
    char cmd;
} __attribute__((__packed__)) mon_header_t;

typedef struct {
    tbx_mon_object_t a;
    tbx_mon_object_t b;
} __attribute__((__packed__)) mon_group_t;

typedef struct {
    tbx_mon_object_t obj;
    int16_t n;
    tbx_mon_object_t a[];
} __attribute__((__packed__)) mon_multigroup_t;

typedef struct {
    int32_t tid;
    tbx_mon_object_t a;
} __attribute__((__packed__)) mon_thread_group_t;

typedef struct {
    tbx_mon_object_t obj;
    int16_t nbytes;
    char text[];
} __attribute__((__packed__)) mon_text_t;

typedef struct {
    tbx_mon_object_t obj;
    int64_t n;
    int16_t nbytes;
    char text[];
} __attribute__((__packed__)) mon_text_int_t;

typedef struct {
    int32_t tid;
    int16_t nbytes;
    char text[];
} __attribute__((__packed__)) mon_thread_text_t;

typedef struct {
    tbx_mon_object_t obj;
    int64_t n;
} __attribute__((__packed__)) mon_int_t;

typedef struct {
    tbx_mon_object_t obj;
    int64_t n[2];
} __attribute__((__packed__)) mon_int2_t;

typedef struct {
    int64_t n;
    int32_t tid;
} __attribute__((__packed__)) mon_thread_int_t;

union __attribute__((__packed__)) mon_union_u {
    tbx_mon_object_t object;
    int32_t tid;
    mon_group_t group;
    mon_multigroup_t mgroup;
    mon_thread_group_t thread_group;
    mon_text_t text;
    mon_text_int_t text_int;
    mon_thread_text_t thread_text;
    mon_int_t  integer;
    mon_int2_t  integer2;
    mon_thread_int_t  thread_integer;
};


typedef struct {
    mon_header_t header;
    union mon_union_u rec;
} __attribute__((__packed__)) mon_record_t;

typedef struct {
    int16_t size;
    mon_record_t record;
} __attribute__((__packed__)) mon_full_record_t;

typedef struct {
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    char *fname;
    FILE *fd;
} mon_ctx_t;

typedef struct {
    apr_hash_t *obj_hash[257];
} mon_hash_t;

typedef struct {
    apr_pool_t *mpool;
    mon_hash_t tracking;
    mon_hash_t labels;
    apr_hash_t *connections;
    char **type_label;
} mon_process_t;

int monitor_state = 0;
mon_ctx_t *ctx = NULL;

//************************************************************************************
//  Misc bookkeeping routines
//************************************************************************************

int tbx_monitor_enabled() { return(monitor_state); }

//************************************************************************************

int tbx_monitor_create(const char *fname)
{
    if (!ctx) {
        tbx_type_malloc_clear(ctx, mon_ctx_t, 1);
        assert_result(apr_pool_create(&(ctx->mpool), NULL), APR_SUCCESS);
        apr_thread_mutex_create(&(ctx->lock), APR_THREAD_MUTEX_DEFAULT, ctx->mpool);
    }

    apr_thread_mutex_lock(ctx->lock);
    if (ctx->fname) free(ctx->fname);
    ctx->fname = strdup(fname);
    if (monitor_state == 1) ctx->fd = tbx_io_fopen(ctx->fname, "a");
    apr_thread_mutex_unlock(ctx->lock);

    return((ctx->fd ? 0 : 1));
}

//************************************************************************************

void tbx_monitor_destroy()
{
    if (!ctx) return;

    apr_thread_mutex_lock(ctx->lock);
    if (ctx->fd) {
        tbx_io_fclose(ctx->fd);
        ctx->fd = NULL;
    }
    if (ctx->fname) free(ctx->fname);
    monitor_state = 0;
    apr_thread_mutex_unlock(ctx->lock);
}

//************************************************************************************

void tbx_monitor_flush()
{
    if (!ctx) return;

    apr_thread_mutex_lock(ctx->lock);
    fflush(ctx->fd);
    apr_thread_mutex_unlock(ctx->lock);
}

//************************************************************************************

void tbx_monitor_set_state(int n)
{
    if (!ctx) return;

    apr_thread_mutex_lock(ctx->lock);
    if (monitor_state == n) goto done;
    if (n == 0) {  //** Stopping monitoring
        fflush(ctx->fd);
        fclose(ctx->fd);
        ctx->fd = NULL;
    } else {  //**Starting monitoring again
        ctx->fd = tbx_io_fopen(ctx->fname, "a");  //** We are appending
    }
    monitor_state = (ctx->fd) ? 1 : 0;

done:
    apr_thread_mutex_unlock(ctx->lock);
}

//************************************************************************************
//  Record store routines
//************************************************************************************

tbx_mon_object_t *tbx_monitor_object_fill(tbx_mon_object_t *obj, unsigned char type, uint64_t id)
{
    obj->type = type;
    obj->id = id;
    return(obj);
}

//************************************************************************************

void monitor_write(mon_full_record_t *r, int cmd)
{
    if (!ctx) return;

    r->record.header.cmd = cmd;
    r->record.header.tid = tbx_atomic_thread_id;

    apr_thread_mutex_lock(ctx->lock);
    if (monitor_state == 1) {
        r->record.header.dt = apr_time_now();
        tbx_io_fwrite(r, r->size + sizeof(uint16_t), 1, ctx->fd);
    }
    apr_thread_mutex_unlock(ctx->lock);
}

//************************************************************************************

void _obj_text_record(int cmd, tbx_mon_object_t *obj, const char *fmt, va_list ap)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];

    r = (mon_full_record_t *)buffer;

    r->record.rec.text.nbytes = (fmt) ? vsnprintf(r->record.rec.text.text, BUFSIZE - sizeof(mon_full_record_t) - 1, fmt, ap) : 0;
    r->size = sizeof(mon_header_t) + sizeof(mon_text_t) + r->record.rec.text.nbytes + 1;
    r->record.rec.text.obj = *obj;

    monitor_write(r, cmd);
}

//************************************************************************************

void _obj_text_int_record(int cmd, tbx_mon_object_t *obj, int64_t n, const char *fmt, va_list ap)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];

    r = (mon_full_record_t *)buffer;

    r->record.rec.text_int.nbytes = (fmt) ? vsnprintf(r->record.rec.text_int.text, BUFSIZE - sizeof(mon_full_record_t) - 1, fmt, ap) : 0;
    r->size = sizeof(mon_header_t) + sizeof(mon_text_int_t) + r->record.rec.text_int.nbytes + 1;
    r->record.rec.text_int.obj = *obj;
    r->record.rec.text_int.n = n;

    monitor_write(r, cmd);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_obj_create(tbx_mon_object_t *obj, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_CREATE, obj, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 3, 4)))
void tbx_monitor_obj_create_irate(tbx_mon_object_t *obj, const int64_t n, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_int_record(MON_REC_OBJ_CREATE_IRATE, obj, n, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 3, 4)))
void tbx_monitor_obj_create_quick(unsigned char type, uint64_t id, const char *fmt, ...)
{
    va_list args;
    tbx_mon_object_t obj;

    if (monitor_state == 0) return;

    tbx_monitor_object_fill(&obj, type, id);

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_CREATE, &obj, fmt, args);
    va_end(args);
}

//************************************************************************************

void tbx_monitor_obj_destroy(tbx_mon_object_t *obj)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(tbx_mon_object_t);
    r->record.rec.object = *obj;

    monitor_write(r, MON_REC_OBJ_DESTROY);
}

//************************************************************************************

void tbx_monitor_obj_destroy_irate(tbx_mon_object_t *obj, const int64_t n)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_int_t);
    r->record.rec.integer.obj = *obj;
    r->record.rec.integer.n = n;

    monitor_write(r, MON_REC_OBJ_DESTROY_IRATE);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_obj_destroy_message(tbx_mon_object_t *obj, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_DESTROY_MSG, obj, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 3, 4)))
void tbx_monitor_obj_destroy_message_irate(tbx_mon_object_t *obj, const int64_t n, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_int_record(MON_REC_OBJ_DESTROY_MSG_IRATE, obj, n, fmt, args);
    va_end(args);
}

//************************************************************************************

void tbx_monitor_obj_destroy_quick(unsigned char type, uint64_t id)
{
    tbx_mon_object_t obj;

    if (monitor_state == 0) return;

    tbx_monitor_obj_destroy(tbx_monitor_object_fill(&obj, type, id));
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_obj_label(tbx_mon_object_t *obj, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_LABEL, obj, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 3, 4)))
void tbx_monitor_obj_label_irate(tbx_mon_object_t *obj, const int64_t n, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_int_record(MON_REC_OBJ_LABEL_IRATE, obj, n, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_obj_message(tbx_mon_object_t *obj, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_MESSAGE, obj, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 3, 4)))
void tbx_monitor_obj_message_quick(unsigned char type, uint64_t id, const char *fmt, ...)
{
    va_list args;
    tbx_mon_object_t obj;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _obj_text_record(MON_REC_OBJ_MESSAGE, tbx_monitor_object_fill(&obj, type, id), fmt, args);
    va_end(args);
}

//************************************************************************************

void tbx_monitor_obj_integer(tbx_mon_object_t *obj, int64_t n)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_int_t);
    r->record.rec.integer.obj = *obj;
    r->record.rec.integer.n = n;

    monitor_write(r, MON_REC_OBJ_INT);
}

//************************************************************************************

void tbx_monitor_obj_integer2(tbx_mon_object_t *obj, int64_t n1, int64_t n2)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_int2_t);
    r->record.rec.integer2.obj = *obj;
    r->record.rec.integer2.n[0] = n1;
    r->record.rec.integer2.n[1] = n2;

    monitor_write(r, MON_REC_OBJ_INT2);
}

//************************************************************************************

void _obj_group_ungroup(int cmd, tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_group_t);
    r->record.rec.group.a = *a;
    r->record.rec.group.b = *b;

    monitor_write(r, cmd);
}

//************************************************************************************

void tbx_monitor_obj_reference(tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    _obj_group_ungroup(MON_REC_OBJ_REFERENCE, a, b);
}

//************************************************************************************

void tbx_monitor_obj_reference_bulk(tbx_mon_object_t *obj, tbx_mon_object_t *a, int n)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;

    r->record.rec.mgroup.n = n;
    r->size = sizeof(mon_header_t) + sizeof(mon_multigroup_t) + sizeof(tbx_mon_object_t)*n;
    r->record.rec.mgroup.obj = *obj;
    memcpy(r->record.rec.mgroup.a, a, sizeof(tbx_mon_object_t)*n);

    monitor_write(r, MON_REC_OBJ_REFERENCE_BULK);
}

//************************************************************************************

void tbx_monitor_obj_reference_chain(tbx_mon_object_t *obj)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(tbx_mon_object_t);
    r->record.rec.object = *obj;

    monitor_write(r, MON_REC_OBJ_REFERENCE_CHAIN);
}

//************************************************************************************

void tbx_monitor_obj_group(tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    _obj_group_ungroup(MON_REC_OBJ_GROUP, a, b);
}

//************************************************************************************

void tbx_monitor_obj_ungroup(tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    _obj_group_ungroup(MON_REC_OBJ_UNGROUP, a, b);
}

//************************************************************************************

void tbx_monitor_obj_group_directed(tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    _obj_group_ungroup(MON_REC_OBJ_GROUP_DIRECTED, a, b);
}

//************************************************************************************

void tbx_monitor_obj_ungroup_directed(tbx_mon_object_t *a, tbx_mon_object_t *b)
{
    _obj_group_ungroup(MON_REC_OBJ_UNGROUP_DIRECTED, a, b);
}


//************************************************************************************
// Thread monitoring routines
//************************************************************************************

//************************************************************************************

void _thread_text_record(int cmd, int32_t tid, const char *fmt, va_list ap)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];

    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;

    r->record.rec.thread_text.tid = (tid == MON_MY_THREAD) ? tbx_atomic_thread_id : tid;
    r->record.rec.thread_text.nbytes = vsnprintf(r->record.rec.thread_text.text, BUFSIZE - sizeof(mon_full_record_t) - 1, fmt, ap);
    r->size = sizeof(mon_header_t) + sizeof(mon_thread_text_t) + r->record.rec.thread_text.nbytes + 1;

    monitor_write(r, cmd);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_thread_create(int32_t tid, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _thread_text_record(MON_REC_THREAD_CREATE, tid, fmt, args);
    va_end(args);
}

//************************************************************************************

void tbx_monitor_thread_destroy(int32_t tid)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(int32_t);
    r->record.rec.tid = (tid == MON_MY_THREAD) ? tbx_atomic_thread_id : tid;

    monitor_write(r, MON_REC_THREAD_DESTROY);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_thread_label(int32_t tid, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _thread_text_record(MON_REC_THREAD_LABEL, tid, fmt, args);
    va_end(args);
}

//************************************************************************************

__attribute__((format (printf, 2, 3)))
void tbx_monitor_thread_message(int32_t tid, const char *fmt, ...)
{
    va_list args;

    if (monitor_state == 0) return;

    va_start(args, fmt);
    _thread_text_record(MON_REC_THREAD_MESSAGE, tid, fmt, args);
    va_end(args);
}

//************************************************************************************

void tbx_monitor_thread_integer(int32_t tid, int64_t n)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_int_t);
    r->record.rec.thread_integer.tid = (tid == MON_MY_THREAD) ? tbx_atomic_thread_id : tid;
    r->record.rec.thread_integer.n = n;

    monitor_write(r, MON_REC_THREAD_INT);
}

//************************************************************************************

void _thread_group_ungroup(int cmd, tbx_mon_object_t *a, int32_t tid)
{
    mon_full_record_t *r;
    char buffer[BUFSIZE];
    if (monitor_state == 0) return;

    r = (mon_full_record_t *)buffer;
    r->size = sizeof(mon_header_t) + sizeof(mon_thread_group_t);
    r->record.rec.thread_group.a = *a;
    r->record.rec.thread_group.tid = (tid == MON_MY_THREAD) ? tbx_atomic_thread_id : tid;
    monitor_write(r, cmd);
}

//************************************************************************************

void tbx_monitor_thread_group(tbx_mon_object_t *a, int32_t tid)
{
    _thread_group_ungroup(MON_REC_THREAD_GROUP, a, tid);
}

//************************************************************************************

void tbx_monitor_thread_ungroup(tbx_mon_object_t *a, int32_t tid)
{
    _thread_group_ungroup(MON_REC_THREAD_UNGROUP, a, tid);
}

//************************************************************************************
//  Fetching routines
//************************************************************************************

//************************************************************************************

FILE *tbx_monitor_open(const char *fname)
{
    FILE *fd;

    fd = tbx_io_fopen(fname, "r");
    if (!fd) {
        fprintf(stderr, "ERROR: Unable to open file: %s\n", fname);
    }

    return(fd);
}

//************************************************************************************

void tbx_monitor_close(FILE *fd)
{
    if (fd) tbx_io_fclose(fd);
}

//************************************************************************************
//   Parsing routines
//************************************************************************************

//************************************************************************************

int tbx_monitor_get_next(FILE *fd, int *cmd, int32_t *tid, apr_time_t *dt, tbx_mon_object_t *a, tbx_mon_object_t *b, char **text, int *text_size, int32_t *b_tid, int64_t *n, tbx_mon_object_t *b_array)
{
    int16_t nbytes;
    char buffer[BUFSIZE];
    mon_record_t *r = (mon_record_t *)buffer;

    //** Read the record size
    if (tbx_io_fread(&nbytes, sizeof(nbytes), 1, fd) != 1) {
        if (feof(fd)) return(1);
        fprintf(stderr, "ERROR: Unable to read next record and EOF not reached\n");
        return(-1);
    }

    //** And the actual record
    if (tbx_io_fread(r, nbytes, 1, fd) != 1) {
        fprintf(stderr, "ERROR: Partial record!\n");
        return(-2);
    }

    //** Now parse out the information based on the command
    *cmd = r->header.cmd;
    *tid = r->header.tid;
    *dt = r->header.dt;

    switch (r->header.cmd) {
    case (MON_REC_OBJ_CREATE):
    case (MON_REC_OBJ_LABEL):
    case (MON_REC_OBJ_MESSAGE):
    case (MON_REC_OBJ_DESTROY_MSG):
        *a = r->rec.text.obj;
        *text_size = r->rec.text.nbytes;
        *text = (*text_size) ? strdup(r->rec.text.text) : NULL;
        break;
    case (MON_REC_OBJ_CREATE_IRATE):
    case (MON_REC_OBJ_LABEL_IRATE):
    case (MON_REC_OBJ_DESTROY_MSG_IRATE):
        *a = r->rec.text_int.obj;
        *n = r->rec.text_int.n;
        *text_size = r->rec.text_int.nbytes;
        *text = (*text_size) ? strdup(r->rec.text_int.text) : NULL;
        break;
    case (MON_REC_OBJ_DESTROY):
        *a = r->rec.object;
        break;
    case (MON_REC_OBJ_INT):
    case (MON_REC_OBJ_DESTROY_IRATE):
        *a = r->rec.integer.obj;
        *n = r->rec.integer.n;
        break;
    case (MON_REC_OBJ_INT2):
        *a = r->rec.integer2.obj;
        n[0] = r->rec.integer2.n[0];
        n[1] = r->rec.integer2.n[1];
        break;
    case (MON_REC_OBJ_GROUP):
    case (MON_REC_OBJ_GROUP_DIRECTED):
    case (MON_REC_OBJ_UNGROUP):
    case (MON_REC_OBJ_UNGROUP_DIRECTED):
    case (MON_REC_OBJ_REFERENCE):
        *a = r->rec.group.a;
        *b = r->rec.group.b;
        break;
    case (MON_REC_OBJ_REFERENCE_BULK):
        *a = r->rec.mgroup.obj;
        *n = r->rec.mgroup.n;
        memcpy(b_array, r->rec.mgroup.a, sizeof(tbx_mon_object_t)*(*n));
        break;
    case (MON_REC_OBJ_REFERENCE_CHAIN):
        *a = r->rec.object;
        break;
    case (MON_REC_THREAD_CREATE):
    case (MON_REC_THREAD_LABEL):
    case (MON_REC_THREAD_MESSAGE):
        *b_tid = r->rec.thread_text.tid;
        *text_size = r->rec.thread_text.nbytes;
        *text = (*text_size) ? strdup(r->rec.thread_text.text) : NULL;
        break;
    case (MON_REC_THREAD_DESTROY):
        *b_tid = r->rec.tid;
        break;
    case (MON_REC_THREAD_INT):
        *b_tid = r->rec.thread_integer.tid;
        *n = r->rec.thread_integer.n;
        break;
    case (MON_REC_THREAD_GROUP):
    case (MON_REC_THREAD_UNGROUP):
        *a = r->rec.thread_group.a;
        *b_tid = r->rec.thread_group.tid;
        break;
    default:
        fprintf(stderr, "ERROR: Unkown cmd! cmd=%d\n", r->header.cmd);
    }

    return(0);
}

//************************************************************************************

typedef struct {  //** This just allows us to unify the TID and objects in the way they are handled
    int type;
    uint64_t id;
    int64_t count;  //** this is actually returned
} _parse_obj_t;

typedef struct {
    uint64_t id;
    int64_t count;
    char *label;
    char *buffer;
    int label_size;
    int n_ref;
    apr_time_t start_time;
    _parse_obj_t ref;
    _parse_obj_t *r_array;
} _label_entry_t;

typedef struct {
    uint64_t id;
    int count;
} _track_entry_t;

typedef struct {
    int type;
    uint64_t id;
} __attribute__((__packed__)) _obj_key_t;

typedef struct {
    _obj_key_t me;
    apr_pool_t *mpool;
    apr_hash_t *connections;
} _obj_connections_t;

//************************************************************************************

void _mon_obj_label_set(mon_process_t *mp, _parse_obj_t *obj, int64_t count, char *text, int text_size, apr_time_t dt)
{
    apr_hash_t *hash = mp->labels.obj_hash[obj->type];
    _label_entry_t *entry;

    //** Make sure the hash exists
    if (!hash) {
        mp->labels.obj_hash[obj->type] = apr_hash_make(mp->mpool);
        hash = mp->labels.obj_hash[obj->type];
    }

    entry = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
    if (entry) {  //** Already exists so just update the entry
        if (entry->label) free(entry->label);
        if (count >= 0) entry->count = count;
        entry->label = text;
        entry->label_size = text_size;
        if (dt > 0) entry->start_time = dt;
    } else {  //** Got to make a new entry
        tbx_type_malloc_clear(entry, _label_entry_t, 1);
        entry->id = obj->id;
        entry->count = count;
        entry->label = text;
        entry->label_size = text_size;
        if (dt > 0) entry->start_time = dt;
        apr_hash_set(hash, &(entry->id), sizeof(entry->id), entry);
    }
}

//************************************************************************************

char *_mon_obj_label_get(mon_process_t *mp, _parse_obj_t *obj)
{
    apr_hash_t *hash = mp->labels.obj_hash[obj->type];
    _label_entry_t *entry, *rentry;
    obj->count = -1;

    //** Make sure the hash exists
    if (!hash) return(NULL);

    entry = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
    if (entry) {  //** Got it
        obj->count = entry->count;

        //** See if we have a reference
        if (entry->ref.id != 0) {
            if (mp->labels.obj_hash[entry->ref.type]) {
                rentry = apr_hash_get(mp->labels.obj_hash[entry->ref.type], &(entry->ref.id), sizeof(entry->ref.id));
                if (rentry) {
                    if (!entry->buffer) {
                        tbx_type_malloc_clear(entry->buffer, char, 1024);
                    }
                    snprintf(entry->buffer, 1024, "%s  ref: %s=" I64T " ref_label=%s", entry->label, mp->type_label[entry->ref.type], entry->ref.id, rentry->label);
                    return(entry->buffer);
                }
            } else {
                fprintf(stderr, "ERROR: missing type hash! type=%d label=%s\n", entry->ref.type, entry->label);
            }
        }
        return(entry->label);
    }

    return(NULL);
}

//************************************************************************************

apr_time_t _mon_obj_label_destroy(mon_process_t *mp, _parse_obj_t *obj)
{
    apr_hash_t *hash;
    _label_entry_t *entry;
    _track_entry_t *te;
    apr_time_t stime = 0;

    //** See if it's tracked and if so go ahead an untrack it
    hash = mp->tracking.obj_hash[obj->type];
    if (hash) {
        te = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
        if (te) {  //** Got it
            te->count--;
            if (te->count <= 0) {
                apr_hash_set(hash, &(obj->id), sizeof(obj->id), NULL);
                free(te);
            }
        }
    }

    //** Now do the same but for the label
    hash = mp->labels.obj_hash[obj->type];
    if (!hash) return(0);

    entry = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
    if (entry) {  //** Got it
        apr_hash_set(hash, &(obj->id), sizeof(obj->id), NULL);
        stime = entry->start_time;
        if (entry->label) free(entry->label);
        if (entry->buffer) free(entry->buffer);
        if (entry->n_ref > 1) free(entry->r_array);
        free(entry);
    }

    return(stime);
}

//************************************************************************************

void _mon_obj_label_reference(mon_process_t *mp, _parse_obj_t *obj, _parse_obj_t *ref)
{
    apr_hash_t *hash = mp->labels.obj_hash[obj->type];
    _label_entry_t *entry;

    //** Make sure the hash exists
    if (!hash) return;

    entry = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
    if (!entry) return;

    entry->n_ref = 1;
    entry->ref = *ref;
    entry->r_array = &(entry->ref);
}

//************************************************************************************

void _mon_obj_label_reference_bulk(mon_process_t *mp, _parse_obj_t *obj, tbx_mon_object_t *ref, int n)
{
    apr_hash_t *hash = mp->labels.obj_hash[obj->type];
    _label_entry_t *entry;
    int i;

    //** Make sure the hash exists
    if (!hash) return;

    entry = apr_hash_get(hash, &(obj->id), sizeof(obj->id));
    if (!entry) return;

    entry->n_ref = n;
    tbx_type_malloc(entry->r_array, _parse_obj_t, n);
    for (i=0; i<n; i++) {
        entry->r_array[i].type = ref[i].type;
        entry->r_array[i].id = ref[i].id;
    }
}

//************************************************************************************

__attribute__((format (printf, 4, 5)))
void _mon_printf(FILE *fd, apr_time_t dt, int32_t tid, const char *fmt, ...)
{
    va_list args;
    apr_time_exp_t texp;

    //** Calculate the time offsets
    apr_time_exp_lt(&texp, dt);

    //** Print the header
    if (texp.tm_year == 69) {  //** Looks like we are using a relative time
        fprintf(fd, "HELLO [dt=%dh%02dm%02ds%06du tid=%d] ", texp.tm_hour, texp.tm_min, texp.tm_sec, texp.tm_usec, tid);
    } else {   //** Full time format
        texp.tm_year += 1900;
        fprintf(fd, "[t=%04dy%02dm%02dd:%dh%02dm%02ds%06du tid=%d] ", texp.tm_year, texp.tm_mon, texp.tm_mday, texp.tm_hour, texp.tm_min, texp.tm_sec, texp.tm_usec, tid);
    }

    //** And the rest of the line
    va_start(args, fmt);
    vfprintf(fd, fmt, args);
    va_end(args);
}


//************************************************************************************

void _ref_dump_indent(FILE *fd, apr_time_t dt, int32_t tid, int indent_how_much)
{
    int i;
    if (indent_how_much>0) _mon_printf(fd, dt, tid, "    ");
    for (i=1; i<indent_how_much; i++) fprintf(fd, "    ");
}

//************************************************************************************

void _mon_obj_reference_dump(mon_process_t *mp, _parse_obj_t obj, FILE *fd, apr_time_t dt, int32_t tid, int row, int col)
{
    apr_hash_t *hash;
    _label_entry_t *entry;
    int i;

    hash = mp->labels.obj_hash[obj.type];
    if (!hash) { fprintf(fd, "-C\n"); return; }

    entry = apr_hash_get(hash, &(obj.id), sizeof(obj.id));
    if (!entry) { fprintf(fd, "-D\n"); return; }

    fprintf(fd, " --> REF[%d,%d] n_refs=%d: %s=" I64T " label=%s", row, col, entry->n_ref, mp->type_label[obj.type], obj.id, entry->label);
    if (entry->n_ref == 0) fprintf(fd, "-E\n");
    for (i=0; i<entry->n_ref; i++) {
        _mon_obj_reference_dump(mp, entry->r_array[i], fd, dt, tid, row+i, col+1);
        if ((i+1) < entry->n_ref) _ref_dump_indent(fd, dt, tid, col+1);
    }
}

//************************************************************************************

void _mon_obj_reference_chain(mon_process_t *mp, _parse_obj_t *robj, FILE *fd, apr_time_t dt, int32_t tid)
{
    _mon_obj_reference_dump(mp, *robj, fd, dt, tid, 0, 0);
}

//************************************************************************************

void _mon_obj_count_delta(mon_process_t *mp, _parse_obj_t *a, int delta)
{
    apr_hash_t *hash = mp->tracking.obj_hash[a->type];
    _track_entry_t *entry;

    //** Make sure the hash exists
    if (!hash) {
        mp->tracking.obj_hash[a->type] = apr_hash_make(mp->mpool);
        hash = mp->tracking.obj_hash[a->type];
    }

    entry = apr_hash_get(hash, &(a->id), sizeof(a->id));
    if (entry) {  //** Already exists so just update the entry
        entry->count += delta;
        if ((entry->count <= 0) && (delta != 0)) {
            apr_hash_set(hash, &(a->id), sizeof(a->id), NULL);
            free(entry);
        }
    } else if (delta >= 0) {  //** Got to make a new entry
        tbx_type_malloc_clear(entry, _track_entry_t, 1);
        entry->id = a->id;
        entry->count = delta;
        apr_hash_set(hash, &(entry->id), sizeof(a->id), entry);
    }
}

//************************************************************************************

void _mon_obj_group(mon_process_t *mp, _parse_obj_t *a, _parse_obj_t *b)
{
    _mon_obj_count_delta(mp, a, 1);
    _mon_obj_count_delta(mp, b, 1);
}

//************************************************************************************

void _mon_obj_ungroup(mon_process_t *mp, _parse_obj_t *a, _parse_obj_t *b)
{
    _mon_obj_count_delta(mp, a, -1);
    _mon_obj_count_delta(mp, b, -1);
}

//************************************************************************************

void _mon_tid_auto_group(mon_process_t *mp, int64_t tid, _parse_obj_t *a)
{
    apr_hash_t *tid_hash = mp->tracking.obj_hash[TID_INDEX];
    _track_entry_t *entry;

    if (!tid_hash) return;

    entry = apr_hash_get(tid_hash, &tid, sizeof(tid));
    if (entry) { //** We're tracking everything new in this thread
        _mon_obj_count_delta(mp, a, 1);
    }
}

//************************************************************************************

char *_mon_obj_is_tracked(mon_process_t *mp, _parse_obj_t *a, int dump_everything)
{
    apr_hash_t *hash = mp->tracking.obj_hash[a->type];
    _track_entry_t *entry;
    char *label;

    //** If we dump everything then just fetch the label and return
    if (dump_everything) goto get_label;

    //** Make sure the hash exists
    if (!hash) return(NULL);

    entry = apr_hash_get(hash, &(a->id), sizeof(a->id));
    if (!entry) return(NULL);

get_label:
    label = _mon_obj_label_get(mp, a);
    if (!label) label = EMPTY_LABEL;
    return(label);
}

//************************************************************************************

apr_time_t _convert_str2time(const char *str)
{
    apr_time_t dt = 0;
    int fin;
    char *tmp, *bstate, *token, *ptr, *next;
    apr_time_exp_t texp;

    memset(&texp, 0, sizeof(texp));

    tmp = strdup(str);
    ptr = strchr(tmp, ':');
    next = ptr;
    if (strchr(tmp, ':') != NULL) { //** Got a full time including the year
        token = tbx_stk_string_token(tmp, ":", &bstate, &fin);

        //** See if we have a year
        ptr = strchr(token, 'y');
        if (ptr) {
            ptr[0] = '\0';
            sscanf(token, "%d", &texp.tm_year);
            texp.tm_year -= 1900;
            next = ptr + 1;
        }

        //** Now check for the month
        ptr = strchr(next, 'm');
        if (ptr) {
            ptr[0] = '\0';
            sscanf(token, "%d", &texp.tm_mon);
            next = ptr + 1;
        }

        //** Now check for the day
        ptr = strchr(next, 'd');
        if (ptr) {
            ptr[0] = '\0';
            sscanf(token, "%d", &texp.tm_mday);
            next = ptr + 1;
        }

        token = tbx_stk_string_token(NULL, ":", &bstate, &fin);
    } else {
        token = tmp;
    }

    //** Now parse the h/m/s/u
    //** See if we have an hour
    ptr = strchr(token, 'h');
    if (ptr) {
        ptr[0] = '\0';
        sscanf(token, "%d", &texp.tm_hour);
        next = ptr + 1;
    }

    //** Now check for the minutes
    ptr = strchr(next, 'm');
    if (ptr) {
        ptr[0] = '\0';
        sscanf(token, "%d", &texp.tm_min);
        next = ptr + 1;
    }

    //** Now check for the seconds
    ptr = strchr(next, 's');
    if (ptr) {
        ptr[0] = '\0';
        sscanf(token, "%d", &texp.tm_sec);
        next = ptr + 1;
    }

    //** Now check for the usecs
    ptr = strchr(next, 'u');
    if (ptr) {
        ptr[0] = '\0';
        sscanf(token, "%d", &texp.tm_usec);
        next = ptr + 1;
    }

    //** Do the conversion
    apr_time_exp_get(&dt, &texp);

    return(dt);
}

//************************************************************************************
// time2string
//************************************************************************************

char * time2string(char *buf, apr_time_t dt)
{
    int hours, min, sec, usec;

    //** Calculate the time offsets
    hours  = apr_time_sec(dt) / 3600;
    min = (apr_time_sec(dt) / 60) % 60;
    sec = apr_time_sec(dt) % 60;
    usec = dt % apr_time_from_sec(1);

    if (hours) {
        sprintf(buf, "%dh%02dm%02ds%06du", hours, min, sec, usec);
    } else if (min) {
        sprintf(buf, "%02dm%02ds%06du", min, sec, usec);
    } else if (sec) {
        sprintf(buf, "%02ds%06du", sec, usec);
    } else {
        sprintf(buf, "%06du", usec);
    }
    return(buf);
}

//************************************************************************************

char *print_final_dt(_parse_obj_t *aop, int64_t count, apr_time_t ptime, char *buf)
{
    char tstr[128], ppbuf[128];
    double rate;

    if (count < 0) {
        if (aop->count >= 0) count = aop->count;
    }

    if (count >= 0) {
        rate = count;
        rate *= (double)(APR_USEC_PER_SEC)/(double)(ptime);
        snprintf(buf, 256, "dt=%s count=" I64T " rate=%sB/s\n", time2string(tstr, ptime), count, tbx_stk_pretty_print_double_with_scale(1024, rate, ppbuf));
    } else {
        snprintf(buf, 256, "dt=%s\n", time2string(tstr, ptime));
    }

    return(buf);
}

//************************************************************************************
// tbx_monitor_parse_log - Parses the monitor log file
//************************************************************************************

int tbx_monitor_parse_log(const char *fname, const char **obj_types, const char *stime, tbx_mon_object_t *obj_list, int n_obj, int32_t *tid_list, int n_tid, FILE *fd_out)
{
    mon_process_t mp;
    FILE *fd_in;
    int cmd, dump_everything;
    tbx_mon_object_t a_obj, b_obj;
    tbx_mon_object_t b_array[4096];
    int32_t tid, b_tid;
    int i;
    _parse_obj_t aop, bop, btp;
    apr_time_t dt, dt_offset, ptime;
    char *text;
    int text_size;
    int64_t n[2];
    char *type_label[257];
    char *alabel, *blabel;
    char s[20];
    char pbuf[16384], dtbuf[256];
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    _label_entry_t *le;
    _track_entry_t *te;

    fd_in = tbx_io_fopen(fname, "r");
    if (!fd_in) {
        fprintf(stderr, "ERROR: Unable to open monitor file: %s\n", fname);
        return(-1);
    }

    //** Make the base structure
    memset(&mp, 0, sizeof(mp));
    apr_pool_create(&(mp.mpool), NULL);
    mp.connections = apr_hash_make(mp.mpool);

    //** Make the type labels
    type_label[TID_INDEX] = "tid";
    for (i=0; i<256; i++) {
        if (obj_types[i] == NULL) {
            snprintf(s, sizeof(s), "type_%d", i);
            type_label[i] = strdup(s);
        } else {
            type_label[i] = (char *)obj_types[i];
        }
    }

    mp.type_label = type_label;

    //** Set up what we track
    dump_everything = ((n_obj == 0) && (n_tid == 0)) ? 1 : 0;
    for (i=0; i<n_obj; i++) {
        aop.type = obj_list[i].type;
        aop.id = obj_list[i].id;
        _mon_obj_count_delta(&mp, &aop, 10000);
    }
    for (i=0; i<n_tid; i++) {
        aop.type = TID_INDEX;
        aop.id = tid_list[i];
        _mon_obj_count_delta(&mp, &aop, 10000);
    }

    //** Figure out the offset
    dt_offset = 0;
    if (stime) {
        if (strcmp(stime, "-") == 0) {
            dt_offset = 1234567;
        } else {
            dt_offset = _convert_str2time(stime);
        }
    }

    memset(&a_obj, 0, sizeof(a_obj));
    memset(&b_obj, 0, sizeof(b_obj));
    tid = b_tid = 0;
    while (tbx_monitor_get_next(fd_in, &cmd, &tid, &dt, &a_obj, &b_obj, &text, &text_size, &b_tid, n, b_array) == 0) {
        //**Adjust the time
        if (dt_offset == 1234567) dt_offset = dt;
        dt = dt - dt_offset;

        //** Copy the parameters over to the unified object structure
        aop.type = a_obj.type; aop.id = a_obj.id;  bop.type = b_obj.type; bop.id = b_obj.id;
        btp.type = TID_INDEX; btp.id = b_tid;

        switch (cmd) {
        case (MON_REC_OBJ_CREATE):
            _mon_obj_label_set(&mp, &aop, -1, text, text_size, dt);
            _mon_tid_auto_group(&mp, tid, &aop);
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "CREATE: %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel);
            }
            break;
        case (MON_REC_OBJ_CREATE_IRATE):
            _mon_obj_label_set(&mp, &aop, n[0], text, text_size, dt);
            _mon_tid_auto_group(&mp, tid, &aop);
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "CREATE: %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel);
            }
            break;
        case (MON_REC_OBJ_LABEL):
            _mon_obj_label_set(&mp, &aop, -1, text, text_size, 0);
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "LABEL: %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel);
            }
            break;
        case (MON_REC_OBJ_LABEL_IRATE):
            _mon_obj_label_set(&mp, &aop, n[0], text, text_size, 0);
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "LABEL: %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel);
            }
            break;
        case (MON_REC_OBJ_MESSAGE):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "MESSAGE: %s=" LU " label=%s message=%s\n", type_label[aop.type], aop.id, alabel, text);
            }
            if (text) free(text);
            break;
        case (MON_REC_OBJ_DESTROY):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                snprintf(pbuf, sizeof(pbuf), "DESTROY: %s=" LU " label=%s ", type_label[aop.type], aop.id, alabel);
            }
            ptime = dt - _mon_obj_label_destroy(&mp, &aop);
            if (alabel) {
                _mon_printf(fd_out, dt, tid, "%s %s", pbuf, print_final_dt(&aop, -1, ptime, dtbuf));
            }
            break;
        case (MON_REC_OBJ_DESTROY_IRATE):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                snprintf(pbuf, sizeof(pbuf), "DESTROY: %s=" LU " label=%s ", type_label[aop.type], aop.id, alabel);
            }
            ptime = dt - _mon_obj_label_destroy(&mp, &aop);
            if (alabel) {
                _mon_printf(fd_out, dt, tid, "%s %s", pbuf, print_final_dt(&aop, n[0], ptime, dtbuf));
            }
            break;
        case (MON_REC_OBJ_DESTROY_MSG):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                snprintf(pbuf, sizeof(pbuf), "DESTROY: %s=" LU " label=%s message=%s ", type_label[aop.type], aop.id, alabel, text);
            }
            ptime = dt - _mon_obj_label_destroy(&mp, &aop);
            if (alabel) {
                _mon_printf(fd_out, dt, tid, "%s %s", pbuf, print_final_dt(&aop, -1, ptime, dtbuf));
            }
            break;
        case (MON_REC_OBJ_DESTROY_MSG_IRATE):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                snprintf(pbuf, sizeof(pbuf), "DESTROY: %s=" LU " label=%s message=%s ", type_label[aop.type], aop.id, alabel, text);
            }
            ptime = dt - _mon_obj_label_destroy(&mp, &aop);
            if (alabel) {
                _mon_printf(fd_out, dt, tid, "%s %s", pbuf, print_final_dt(&aop, n[0], ptime, dtbuf));
            }
            break;
        case (MON_REC_OBJ_INT):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "COUNT: %s=" LU " label=%s count=" I64T "\n", type_label[aop.type], aop.id, alabel, n[0]);
            }
            break;
        case (MON_REC_OBJ_INT2):
            if ((alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "COUNT2: %s=" LU " label=%s count[0]=" I64T " count[1]=" I64T "\n", type_label[aop.type], aop.id, alabel, n[0], n[1]);
            }
            break;
        case (MON_REC_OBJ_REFERENCE):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            if (alabel != NULL) {
                blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
                _mon_printf(fd_out, dt, tid, "REFERENCE: %s=" LU " label=%s ----> %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel, type_label[bop.type], bop.id, blabel);
                _mon_obj_label_reference(&mp, &aop, &bop);
            }
            break;
        case (MON_REC_OBJ_REFERENCE_BULK):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            if (alabel != NULL) {
                _mon_printf(fd_out, dt, tid, "REFERENCE_BULK: n_ref=" I64T " %s=" LU " label=%s", n[0], type_label[aop.type], aop.id, alabel);
                for (i=0; i< n[0]; i++) {
                    bop.type = b_array[i].type; bop.id = b_array[i].id;
                    blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
                    fprintf(fd_out, " ----> REF[%d] %s=" LU " label=%s", i, type_label[bop.type], bop.id, blabel);
                }
                fprintf(fd_out, "\n");
                _mon_obj_label_reference_bulk(&mp, &aop, b_array, n[0]);
            }
            break;
        case (MON_REC_OBJ_REFERENCE_CHAIN):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            if (alabel != NULL) {
                _mon_printf(fd_out, dt, tid, "REFERENCE_CHAIN: ");
                _mon_obj_reference_chain(&mp, &aop, fd_out, dt, tid);
                _mon_printf(fd_out, dt, tid, "REFERENCE_CHAIN: END\n");
            }
            break;
        case (MON_REC_OBJ_GROUP):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
            if ((alabel != NULL) || (blabel != NULL)) {
                _mon_printf(fd_out, dt, tid, "GROUP: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel, type_label[bop.type], bop.id, blabel);
                _mon_obj_group(&mp, &aop, &bop);
            }
            break;
        case (MON_REC_OBJ_GROUP_DIRECTED):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            if (alabel != NULL) {
                blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
                _mon_printf(fd_out, dt, tid, "GROUP_DIRECTED: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel, type_label[bop.type], bop.id, blabel);
                _mon_obj_group(&mp, &aop, &bop);
            }
            break;
        case (MON_REC_OBJ_UNGROUP):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
            if ((alabel != NULL) || (blabel != NULL)) {
                _mon_printf(fd_out, dt, tid, "UNGROUP: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel, type_label[bop.type], bop.id, blabel);
                _mon_obj_ungroup(&mp, &aop, &bop);
            }
            break;
        case (MON_REC_OBJ_UNGROUP_DIRECTED):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            if (alabel != NULL) {
                blabel = _mon_obj_is_tracked(&mp, &bop, dump_everything);
                _mon_printf(fd_out, dt, tid, "UNGROUP_DIRECTED: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[aop.type], aop.id, alabel, type_label[bop.type], bop.id, blabel);
                _mon_obj_ungroup(&mp, &aop, &bop);
            }
            break;
        case (MON_REC_THREAD_CREATE):
            _mon_obj_label_set(&mp, &btp, -1, text, text_size, dt);
            if ((alabel = _mon_obj_is_tracked(&mp, &btp, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "CREATE: %s=" LU " label=%s\n", type_label[btp.type], btp.id, alabel);
            }
            break;
        case (MON_REC_THREAD_LABEL):
            _mon_obj_label_set(&mp, &btp, -1, text, text_size, 0);
            if ((alabel = _mon_obj_is_tracked(&mp, &btp, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "LABEL: %s=" LU " label=%s\n", type_label[btp.type], btp.id, alabel);
            }
            break;
        case (MON_REC_THREAD_MESSAGE):
            if ((alabel = _mon_obj_is_tracked(&mp, &btp, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "MESSAGE: %s=" LU " label=%s message=%s\n", type_label[btp.type], btp.id, alabel, text);
            }
            if (text) free(text);
            break;
        case (MON_REC_THREAD_DESTROY):
            if ((alabel = _mon_obj_is_tracked(&mp, &btp, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "DESTROY: %s=" LU " label=%s\n", type_label[btp.type], btp.id, alabel);
            }
            _mon_obj_label_destroy(&mp, &btp);
            break;
        case (MON_REC_THREAD_INT):
            if ((alabel = _mon_obj_is_tracked(&mp, &btp, dump_everything)) != NULL) {
                _mon_printf(fd_out, dt, tid, "COUNT: %s=" LU " label=%s count=" I64T "\n", type_label[btp.type], btp.id, alabel, n[0]);
            }
            break;
        case (MON_REC_THREAD_GROUP):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            blabel = _mon_obj_is_tracked(&mp, &btp, dump_everything);
            if ((alabel != NULL) || (blabel != NULL)) {
                _mon_obj_group(&mp, &btp, &aop);
                _mon_printf(fd_out, dt, tid, "GROUP: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[btp.type], btp.id, blabel, type_label[aop.type], aop.id, alabel);
            }
            break;
        case (MON_REC_THREAD_UNGROUP):
            alabel = _mon_obj_is_tracked(&mp, &aop, dump_everything);
            blabel = _mon_obj_is_tracked(&mp, &btp, dump_everything);
            if (alabel || blabel) {
                _mon_printf(fd_out, dt, tid, "UNGROUP: %s=" LU " label=%s %s=" LU " label=%s\n", type_label[btp.type], btp.id, blabel, type_label[aop.type], aop.id, alabel);
                _mon_obj_ungroup(&mp, &btp, &aop);
            }
            break;
        default:
            fprintf(stderr, "ERROR: Unkown cmd! cmd=%d\n", cmd);
            return(-3);
            break;
        }
    }

    //** Cleanup
    tbx_io_fclose(fd_in);

    //** Cleanup the types as needed
    for (i=0; i<256; i++) {
        if (obj_types[i] == NULL) free(type_label[i]);
    }

    //** And the hashes
    for (i=0; i<257; i++) {
        if (mp.tracking.obj_hash[i]) {
            for (hi=apr_hash_first(NULL, mp.tracking.obj_hash[i]); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, NULL, &hlen, (void **)&te);
                free(te);
            }
        }
        if (mp.labels.obj_hash[i]) {
            for (hi=apr_hash_first(NULL, mp.labels.obj_hash[i]); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, NULL, &hlen, (void **)&le);
                if (le->label) free(le->label);
                free(le);
            }
        }
    }

    apr_pool_destroy(mp.mpool);

    return(0);
}
