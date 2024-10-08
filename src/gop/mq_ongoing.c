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
// MQ Ongoing task implementation
//***********************************************************************

#define _log_module_index 222

#include <apr.h>
#include <apr_errno.h>
#include <assert.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>
#include <tbx/notify.h>
#include "gop/gop.h"
#include "gop/opque.h"
#include "gop/types.h"
#include "mq_helpers.h"
#include "mq_ongoing.h"

#ifdef ENABLE_MQ_DEBUG
    #define MQ_DEBUG(...) __VA_ARGS__
    #define MQ_DEBUG_NOTIFY(fmt, ...) if (tbx_notify_handle) _tbx_notify_printf(tbx_notify_handle, 1, NULL, __func__, __LINE__, fmt, ## __VA_ARGS__ )
#else
    #define MQ_DEBUG_NOTIFY(fmt, ...)
    #define MQ_DEBUG(...)
#endif

typedef struct {
    gop_mq_ongoing_t *mqon;
    gop_mq_ongoing_object_t *oo;
    gop_mq_ongoing_handle_t *ohandle;
    int tweak_remove_pending;
} ongoing_close_defer_t;

//***********************************************************************
// ongoing_response_status - Handles a response that just returns the status
//***********************************************************************

gop_op_status_t ongoing_response_status(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    gop_op_status_t status;
    char date[128];
    FILE *fd = NULL;

    log_printf(5, "START\n");

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);
    log_printf(5, "END status=%d %d\n", status.op_status, status.error_code);
    if (status.op_status != OP_STATE_SUCCESS) {
        MQ_DEBUG_NOTIFY("ONGOING_CLIENT: ERROR -- Got a failed heartbeat! status.op_status=%d status.error_code=%d\n", status.op_status, status.error_code);

        //** See if we can log the error"
        if (task->ctx->fname_errors) {
            fd = fopen(task->ctx->fname_errors, "a");
            if (fd) {
                fprintf(fd, "%s -- ERROR: ONGOING_HEARTBEAT Failed!!\n", tbx_stk_pretty_print_time(apr_time_now(), 1, date));
                fclose(fd);
            }
        }
    }

    MQ_DEBUG_NOTIFY("ONGOING_HEARTBEAT_STATUS: status.op_status=%d status.error_code=%d\n", status.op_status, status.error_code);

    return(status);
}

//***********************************************************************
// ongoing_heartbeat_thread - Sends renewal heartbeats for ongoing objects
//***********************************************************************

void *ongoing_heartbeat_thread(apr_thread_t *th, void *data)
{
    gop_mq_ongoing_t *on = (gop_mq_ongoing_t *)data;
    apr_time_t timeout = apr_time_make(on->check_interval, 0);
    gop_op_generic_t *gop;
    mq_msg_t *msg;
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    apr_hash_index_t *hi, *hit;
    gop_opque_t *q;
    char *id;
    gop_mq_msg_hash_t *remote_hash;
    apr_time_t now;
    apr_ssize_t id_len;
    int n, k, pending_end, added, got;
    char *remote_host_string;
    MQ_DEBUG(int pending_start;)

    tbx_monitor_thread_create(MON_MY_THREAD, "ongoing_heartbeat_thread");

    q = gop_opque_new();

    apr_thread_mutex_lock(on->lock);
    do {
        now = apr_time_now() - apr_time_from_sec(5);  //** Give our selves a little buffer
        log_printf(5, "Loop Start now=" TT "\n", apr_time_now());
        added = 0;
        MQ_DEBUG(pending_start = gop_opque_tasks_left(q);)
        for (hit = apr_hash_first(NULL, on->table); hit != NULL; hit = apr_hash_next(hit)) {
            apr_hash_this(hit, (const void **)&remote_hash, &id_len, (void **)&table);

            k = apr_hash_count(table->table);
            if (tbx_log_level() > 1) {
                remote_host_string = mq_address_to_string(table->remote_host);
                log_printf(5, "host=%s count=%d\n", remote_host_string, k);
                free(remote_host_string);
            }

            for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
                log_printf(5, "id=%s now=" TT " next_check=" TT "\n", oh->id,
                                    ((apr_time_t) apr_time_sec(apr_time_now())),
                                    ((apr_time_t) apr_time_sec(oh->next_check)));
                if (now > oh->next_check) {
                    log_printf(5, "id=%s sending HEARTBEAT EXEC SUBMIT nows=" TT "hb=%d\n",
                                        oh->id,
                                        ((apr_time_t) apr_time_sec(apr_time_now())),
                                        oh->heartbeat);
                    tbx_log_flush();
                    //** Form the message
                    msg = gop_mq_make_exec_core_msg(table->remote_host, 1);
                    gop_mq_msg_append_mem(msg, ONGOING_KEY, ONGOING_SIZE, MQF_MSG_KEEP_DATA);
                    gop_mq_msg_append_mem(msg, oh->id, oh->id_len, MQF_MSG_KEEP_DATA);
                    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

                    //** Make the gop
                    gop = gop_mq_op_new(on->mqc, msg, ongoing_response_status, NULL, NULL, oh->heartbeat);
                    gop_set_private(gop, oh);
                    gop_opque_add(q, gop);

                    oh->in_progress++; //** Flag it as in progress so it doesn't get deleted behind the scenes
                    added++;
                }
            }

        }
        log_printf(5, "Loop end now=" TT "\n", apr_time_now());

        //** See if anything completes
        apr_thread_mutex_unlock(on->lock);
        gop = gop_waitany_timed(opque_get_gop(q), 1);  //** Wait up to 1 second
        apr_thread_mutex_lock(on->lock);

        //** Dec the counters of anything that completed
        got = 0;
        while (gop) {
            got++;
            log_printf(5, "gid=%d gotone status=%d now=" TT "\n", gop_id(gop), (gop_get_status(gop)).op_status,
                    ((apr_time_t) apr_time_sec(apr_time_now())));
            oh = gop_get_private(gop);

            //** Update the next check
            oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);

            //** Check if we get rid of it
            oh->in_progress--;
            if (oh->count <= 0) {  //** Need to delete it
                apr_hash_set(table->table, id, id_len, NULL);
                free(oh->id);
                free(oh);
            }

            gop_free(gop, OP_DESTROY);

            //** See if another has finished
            gop = gop_get_next_finished(opque_get_gop(q));
        }

        pending_end = gop_opque_tasks_left(q);

        if (pending_end > 1) {
            MQ_DEBUG_NOTIFY( "ONGOING_CLIENT: WARNING -- pending_start=%d sent=%d recved=%d pending_end=%d\n", pending_start, added, got, pending_end);
        }

        MQ_DEBUG_NOTIFY( "ONGOING_HEARTBEAT: pending_start=%d sent=%d recved=%d pending_end=%d\n", pending_start, added, got, pending_end);

        now = apr_time_now();
        log_printf(5, "sleeping %d now=%" APR_TIME_T_FMT "\n", on->check_interval, now);

        //** Sleep until time for the next heartbeat or time to exit
        if (on->shutdown == 0) apr_thread_cond_timedwait(on->thread_cond, on->lock, timeout);
        n = on->shutdown;

        now = apr_time_now() - now;
        log_printf(5, "main loop bottom n=%d dt=%" APR_TIME_T_FMT " sec=" TT "\n", n, now,
                        ((apr_time_t) apr_time_sec(now)));
    } while (n == 0);

    log_printf(5, "CLEANUP\n");

    //** We now need to wait for everything to finish. But we don't care about processing them since we areexiting
    opque_waitall(q);

    //** Go ahead and logout
    for (hit = apr_hash_first(NULL, on->table); hit != NULL; hit = apr_hash_next(hit)) {
        apr_hash_this(hit, (const void **)&remote_hash, &id_len, (void **)&table);

        for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
            //** Form the message
            msg = gop_mq_make_exec_core_msg(table->remote_host, 1);
            gop_mq_msg_append_mem(msg, ONGOING_LOGOUT_KEY, ONGOING_LOGOUT_SIZE, MQF_MSG_KEEP_DATA);
            gop_mq_msg_append_mem(msg, oh->id, oh->id_len, MQF_MSG_KEEP_DATA);
            gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

            //** Make the gop and send it
            gop = gop_mq_op_new(on->mqc, msg, ongoing_response_status, NULL, NULL, oh->heartbeat);
            gop_set_private(gop, oh);
            gop_opque_add(q, gop);
        }
    }


    //** Wait for the response
    opque_waitall(q);

    //** Final cleanup
    for (hit = apr_hash_first(NULL, on->table); hit != NULL; hit = apr_hash_next(hit)) {
        apr_hash_this(hit, (const void **)&remote_hash, &id_len, (void **)&table);

        for (hi = apr_hash_first(NULL, table->table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&id, &id_len, (void **)&oh);
            apr_hash_set(table->table, id, id_len, NULL);
            free(oh->id);
            free(oh);
        }

        apr_hash_set(on->table, &(table->remote_host_hash), sizeof(gop_mq_msg_hash_t), NULL);
        gop_mq_msg_destroy(table->remote_host);
        free(table);
    }

    log_printf(5, "EXITING\n");

    apr_thread_mutex_unlock(on->lock);

    gop_opque_free(q, OP_DESTROY);

    tbx_monitor_thread_destroy(MON_MY_THREAD);

    return(NULL);
}


//***********************************************************************
// gop_mq_ongoing_host_inc - Adds a host for ongoing heartbeats
//***********************************************************************

void gop_mq_ongoing_host_inc(gop_mq_ongoing_t *on, mq_msg_t *remote_host, char *my_id, int id_len, int heartbeat)
{
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    gop_mq_msg_hash_t hash;
    char *remote_host_string;

    apr_thread_mutex_lock(on->lock);

    char *str = mq_address_to_string(remote_host);
    log_printf(5, "remote_host=%s\n", str);
    free(str);

    hash = gop_mq_msg_hash(remote_host);
    table = apr_hash_get(on->table, &hash, sizeof(gop_mq_msg_hash_t));  //** Look up the remote host

    if (tbx_log_level() > 5) {
        remote_host_string = mq_address_to_string(remote_host);
        log_printf(5, "remote_host=%s hb=%d table=%p\n", remote_host_string, heartbeat, table);
        free(remote_host_string);
    }
    if (table == NULL) { //** New host so add it
        tbx_type_malloc_clear(table, gop_ongoing_table_t, 1);
        table->table = apr_hash_make(on->mpool);FATAL_UNLESS(table->table != NULL);
        table->remote_host = gop_mq_msg_new();
        gop_mq_msg_append_msg(table->remote_host, remote_host, MQF_MSG_AUTO_FREE);
        table->remote_host_hash = hash;
        apr_hash_set(on->table, &(table->remote_host_hash), sizeof(gop_mq_msg_hash_t), table);
    }

    table->count++;

    oh = apr_hash_get(table->table, my_id, id_len);  //** Look up the id
    if (oh == NULL) { //** New host so add it
        tbx_type_malloc_clear(oh, gop_ongoing_hb_t, 1);
        tbx_type_malloc(oh->id, char, id_len);
        memcpy(oh->id, my_id, id_len);
        oh->id_len = id_len;
        oh->heartbeat = heartbeat / on->send_divisor;
        if (oh->heartbeat < 1) oh->heartbeat = 1;

        if (tbx_log_level() > 5) {
            remote_host_string = mq_address_to_string(remote_host);
            log_printf(5, "remote_host=%s final hb=%d \n", remote_host_string, oh->heartbeat);
            free(remote_host_string);
        }
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        apr_hash_set(table->table, oh->id, id_len, oh);
    }

    oh->count++;
    MQ_DEBUG_NOTIFY( "ONGOING_INC -- host=%s count=%d\n", oh->id, oh->count);
    apr_thread_mutex_unlock(on->lock);
}

//***********************************************************************
// gop_mq_ongoing_host_dec - Decrements the tracking count to a host for ongoing heartbeats
//***********************************************************************

void gop_mq_ongoing_host_dec(gop_mq_ongoing_t *on, mq_msg_t *remote_host, char *id, int id_len)
{
    gop_ongoing_hb_t *oh;
    gop_ongoing_table_t *table;
    gop_mq_msg_hash_t hash;

    hash = gop_mq_msg_hash(remote_host);
    apr_thread_mutex_lock(on->lock);
    table = apr_hash_get(on->table, &hash, sizeof(gop_mq_msg_hash_t));  //** Look up the host
    if (table == NULL) goto fail;

    table->count--;

    oh = apr_hash_get(table->table, id, id_len);  //** Look up the host
    if (oh != NULL) {
        oh->count--;
        MQ_DEBUG_NOTIFY( "ONGOING_DEC -- host=%s count=%d\n", oh->id, oh->count);
        if ((oh->count <= 0) && (oh->in_progress == 0)) {  //** Can delete the entry
            apr_hash_set(table->table, id, id_len, NULL);
            free(oh->id);
            free(oh);
        }
    }

fail:
    apr_thread_mutex_unlock(on->lock);
}

//***********************************************************************
// gop_mq_ongoing_add - Adds an onging object to the tracking tables
//***********************************************************************

gop_mq_ongoing_object_t *gop_mq_ongoing_add(gop_mq_ongoing_t *mqon, bool auto_clean, char *id, int id_len, intptr_t key, void *handle, gop_mq_ongoing_fail_fn_t on_fail, void *on_fail_arg, gop_mq_ongoing_can_fail_fn_t on_canfail, void *on_canfail_arg)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;

    tbx_type_malloc(ongoing, gop_mq_ongoing_object_t, 1);
    ongoing->handle = handle;
    ongoing->key = key;
    ongoing->on_fail = on_fail;
    ongoing->on_fail_arg = on_fail_arg;
    ongoing->on_canfail = on_canfail;
    ongoing->on_canfail_arg = on_canfail_arg;
    ongoing->count = 0;
    ongoing->auto_clean = auto_clean;

    log_printf(5, "host=%s len=%d handle=%p key=%" PRIdPTR "\n", id, id_len, handle, ongoing->key);

    apr_thread_mutex_lock(mqon->lock);

    //** Make sure the host entry exists
    oh = apr_hash_get(mqon->id_table, id, id_len);
    if (oh == NULL) {
        log_printf(5, "new host=%s\n", id);

        tbx_type_malloc(oh, gop_mq_ongoing_host_t, 1);
        tbx_type_malloc(oh->id, char, id_len+1);
        memcpy(oh->id, id, id_len);
        oh->id[id_len] = 0;  //** NULL terminate the string
        oh->id_len = id_len;

        oh->heartbeat = 60;
        sscanf(id, "%d:", &(oh->heartbeat));
        log_printf(5, "heartbeat interval=%d\n", oh->heartbeat);
        oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
        assert_result(apr_pool_create(&(oh->mpool), NULL), APR_SUCCESS);
        oh->table = apr_hash_make(oh->mpool);

        apr_hash_set(mqon->id_table, oh->id, id_len, oh);
    }

    //** Add the object
    apr_hash_set(oh->table, &(ongoing->key), sizeof(intptr_t), ongoing);

    apr_thread_mutex_unlock(mqon->lock);

    return(ongoing);
}


//***********************************************************************
// gop_mq_ongoing_get - Retreives the handle if it's active
//***********************************************************************

void *gop_mq_ongoing_get(gop_mq_ongoing_t *mqon, char *id, int id_len, intptr_t key, gop_mq_ongoing_handle_t *ohandle)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;
    void *ptr = NULL;

    apr_thread_mutex_lock(mqon->lock);

    memset(ohandle, 0, sizeof(gop_mq_ongoing_handle_t));

    //** Get the host entry
    oh = apr_hash_get(mqon->id_table, id, id_len);
    ohandle->oh = oh;
    log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
    if (oh != NULL) {
        ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
        ohandle->oo = ongoing;
        if (ongoing != NULL) {
            log_printf(6, "Found!\n");
            ptr = ongoing->handle;
            ongoing->count++;
            oh->hb_count++;  //** Add a heartbeat since we're using it
        }
    }

    apr_thread_mutex_unlock(mqon->lock);

    return(ptr);
}

//***********************************************************************
// gop_mq_ongoing_release - Releases the handle active handle
//***********************************************************************

void gop_mq_ongoing_release(gop_mq_ongoing_t *mqon, gop_mq_ongoing_handle_t *ohandle)
{
    gop_mq_ongoing_object_t *ongoing = ohandle->oo;

    apr_thread_mutex_lock(mqon->lock);

    if ((ohandle->oh) && (ohandle->oo)) {
        ongoing->count--;
        ohandle->oh->hb_count++;  //** Add a heartbeat since we're using it
        if (ongoing->count <= 0) {    //** No references
            if (ongoing->remove == 1) { //** Flagged for removal
                ohandle->oh->remove_pending--;  //** Remove the pending count
                free(ongoing);
            } else {
                apr_thread_cond_broadcast(mqon->object_cond); //** Let gop_mq_ongoing_remove() and ongoing_fail() know it's Ok to reap
            }
        } else {
            apr_thread_cond_broadcast(mqon->object_cond); //** Let gop_mq_ongoing_remove() and ongoing_fail() know it's Ok to reap
        }
    } else {
        if (tbx_notify_handle) tbx_notify_printf(tbx_notify_handle, 1, NULL, "ERROR: Invalid handle: oh=%p oo=%p\n", ohandle->oh, ohandle->oo);
    }

    apr_thread_mutex_unlock(mqon->lock);

    return;
}

//***********************************************************************
// gop_mq_ongoing_remove - Removes an onging object from the tracking table
//***********************************************************************

void *gop_mq_ongoing_remove(gop_mq_ongoing_t *mqon, char *id, int id_len, intptr_t key, int do_wait)
{
    gop_mq_ongoing_object_t *ongoing;
    gop_mq_ongoing_host_t *oh;
    void *ptr = NULL;

    apr_thread_mutex_lock(mqon->lock);

    //** Get the host entry
    oh = apr_hash_get(mqon->id_table, id, id_len);
    log_printf(6, "looking for host=%s len=%d oh=%p key=%" PRIdPTR "\n", id, id_len, oh, key);
    if (oh != NULL) {
        log_printf(6, "Found host=%s key=%" PRIdPTR "\n", oh->id, key);

        ongoing = apr_hash_get(oh->table, &key, sizeof(intptr_t));  //** Lookup the object
        if (ongoing != NULL) {
            apr_hash_set(oh->table, &key, sizeof(intptr_t), NULL);  //** Clear the key so no one else finds it
            ptr = ongoing->handle;
            if (do_wait == 0) {
                if (ongoing->count > 0) { //** Someone else has it so don't destroy it just yet
                    if (ongoing->remove == 0) {
                        ongoing->remove = 1;
                        oh->remove_pending++;
                    }
                } else {  //** Clear it and destroy it
                    if (ongoing->remove) oh->remove_pending--;
                    free(ongoing);
                }
            } else { //** Wait for the refs to clear before freeing it
                while (ongoing->count > 0) {
                    apr_thread_cond_wait(mqon->object_cond, mqon->lock);
                }
                free(ongoing);
            }
        }
    }

    apr_thread_mutex_unlock(mqon->lock);

    return(ptr);
}

//***********************************************************************
//  *ongoing_close_wait - Waits until the OO's count is cleared before
//      Calling the close object
//***********************************************************************

gop_op_status_t ongoing_close_wait(void *task_arg, int tid)
{
    ongoing_close_defer_t *defer = task_arg;
    gop_op_generic_t *gop;
    int count, count_prev;
    MQ_DEBUG(int has_oh = (defer->ohandle) ? 1 : 0;)

    MQ_DEBUG(
        if (has_oh) {
            MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: START oo->count=%d oo->remove=%d oh->remove_pending=%d\n", defer->oo->count, defer->oo->remove, defer->ohandle->oh->remove_pending);
        } else {
            MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: START oo->count=%d oo->remove=%d oh->remove_pending=MISSING\n", defer->oo->count, defer->oo->remove);
        }
     )

    //** Wait for the references to clear
    apr_thread_mutex_lock(defer->mqon->lock);
    count_prev = -1;
    count = (defer->oo->auto_clean == 1) ?  defer->oo->count-1 : defer->oo->count;
    while (count > 0) {
        if (count != count_prev) {
            MQ_DEBUG(
                if (has_oh) {
                    MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: LOOP-checking canfail count=%d oo->count=%d oo->remove=%d oh->remove_pending=%d tweak=%d\n", count, defer->oo->count, defer->oo->remove, defer->ohandle->oh->remove_pending, defer->tweak_remove_pending);
                } else {
                    MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: LOOP-checking canfail count=%d oo->count=%d oo->remove=%d oh->remove_pending=MISSING tweak=%d\n", count, defer->oo->count, defer->oo->remove, defer->tweak_remove_pending);
                }
            )
            if (defer->oo->on_canfail) {
                if (defer->oo->on_canfail(defer->oo->on_canfail_arg, defer->oo->handle, count) == 1) break;  //** Kick out of the loop
            }
        }
        apr_thread_cond_wait(defer->mqon->object_cond, defer->mqon->lock);
        count_prev = count;
        count = (defer->oo->auto_clean == 1) ?  defer->oo->count-1 : defer->oo->count;
    }
    apr_thread_mutex_unlock(defer->mqon->lock);

    MQ_DEBUG(
        if (has_oh) {
            MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: KICKOUT oo->count=%d oo->remove=%d oh->remove_pending=%d tweak=%d autoclean=%d\n", defer->oo->count, defer->oo->remove, defer->ohandle->oh->remove_pending, defer->tweak_remove_pending, defer->oo->auto_clean);
        } else {
            MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: KICKOUT oo->count=%d oo->remove=%d oh->remove_pending=MISSING tweak=%d autoclean=%d\n", defer->oo->count, defer->oo->remove, defer->tweak_remove_pending, defer->oo->auto_clean);
        }
    )
    //** Now we can call the on_fail task
    gop = defer->oo->on_fail(defer->oo->on_fail_arg, defer->oo->handle);
    gop_sync_exec(gop);

    if (defer->tweak_remove_pending && defer->ohandle) {
        apr_thread_mutex_lock(defer->mqon->lock);
        defer->ohandle->oh->remove_pending--;
        apr_thread_mutex_unlock(defer->mqon->lock);
    }

    if (defer->ohandle) free(defer->ohandle);
    if (defer->oo->auto_clean) free(defer->oo);
    free(defer);

    MQ_DEBUG_NOTIFY( "ONGOING_CLOSE_WAIT: END\n");

    return(gop_success_status);
}

//***********************************************************************
// _mq_ongoing_close - Closes all ongoing objects associated with the connection
//     NOTE:  Assumes the ongoing_lock is held by the calling process
//***********************************************************************

int _mq_ongoing_close(gop_mq_ongoing_t *mqon, gop_mq_ongoing_host_t *oh, gop_opque_t *q)
{
    apr_hash_index_t *hi;
    char *key;
    apr_ssize_t klen;
    gop_mq_ongoing_object_t *oo;
    gop_mq_ongoing_handle_t *ohandle;
    gop_op_generic_t *gop;
    ongoing_close_defer_t *defer;
    int ntasks, tweak_remove_pending;

    int n = apr_hash_count(oh->table);
    log_printf(2, "closing host=%s task_count=%d now=" TT " next_check=" TT " hb=%d\n", oh->id, n, apr_time_now(), oh->next_check, oh->heartbeat);

    ntasks = 0;

    for (hi = apr_hash_first(NULL, oh->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&oo);

        tweak_remove_pending = 0;
        ohandle = NULL;
        if (oo->auto_clean) {
            apr_hash_set(oh->table, key, klen, NULL);  //** I'm cleaning up so remove it from the table
            tbx_type_malloc_clear(ohandle, gop_mq_ongoing_handle_t, 1);
            ohandle->oo = oo;
            ohandle->oh = oh;
            ohandle->oo->count++;  //** Add ourselves so we can do clean up if needed still ??????? Is this still needed
            MQ_DEBUG_NOTIFY( "ONGOING_CLOSE: oo->count=%d oo->remove=%d oh->remove_pending=%d\n", oo->count, oo->remove, oh->remove_pending);
            if (oo->remove == 0) {
                oo->remove = 1;
                oh->remove_pending++;
                tweak_remove_pending = 1;
            }

        }

        MQ_DEBUG_NOTIFY( "ONGOING_CLOSE: DEFER oo->count=%d oo->remove=%d oh->remove_pending=%d  oo->auto_clean\n", oo->count, oo->remove, oh->remove_pending, oo->auto_clean);
        tbx_type_malloc_clear(defer, ongoing_close_defer_t, 1);
        defer->mqon = mqon;
        defer->oo = oo;
        defer->ohandle = ohandle;
        defer->tweak_remove_pending = tweak_remove_pending;
        gop = gop_tp_op_new(mqon->tp_fail, "mq_onfail", ongoing_close_wait, defer, NULL, 1);
        gop_opque_add(q, gop);

        ntasks++;
    }

    MQ_DEBUG_NOTIFY( "ONGOING_CLOSE -- host=%s hb_count=%d ntasks=%d\n", oh->id, oh->hb_count, ntasks);
    return(ntasks);
}


//***********************************************************************
// mq_ongoing_logout_cb - Handles an ongoing logout
//***********************************************************************

void mq_ongoing_logout_cb(void *arg, gop_mq_task_t *task)
{
    gop_mq_ongoing_t *mqon = (gop_mq_ongoing_t *)arg;
    gop_mq_frame_t *fid, *fuid;
    char *id;
    gop_mq_ongoing_host_t *oh;
    int fsize;
    mq_msg_t *msg, *response;
    gop_opque_t *q;
    gop_op_status_t status;

    log_printf(1, "Processing incoming request. EXEC START now=" TT "\n",
                    ((apr_time_t) apr_time_sec(apr_time_now())));
    tbx_log_flush();

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    log_printf(2, "looking up %s\n", id);

    //** Do the lookup and update the heartbeat timeout
    apr_thread_mutex_lock(mqon->lock);
    oh = apr_hash_get(mqon->id_table, id, fsize);
    if (oh != NULL) {
        q = gop_opque_new();
        oh->next_check = 0;   //** Flag it as being closed
        _mq_ongoing_close(mqon, oh, q);
        apr_thread_mutex_unlock(mqon->lock);
        status =  (opque_waitall(q) == 0) ? gop_success_status : gop_failure_status;
        gop_opque_free(q, OP_DESTROY);
        MQ_DEBUG_NOTIFY( "ONGOING_LOGOUT_CB: id=%s id_len=%d status=%d\n", id, fsize, status.op_status);
    } else {
        apr_thread_mutex_unlock(mqon->lock);
        status = gop_failure_status;
        MQ_DEBUG_NOTIFY( "ONGOING_LOGOUT_CB: ERROR -- Unknown host! id=%s id_len=%d\n", id, fsize);
    }

    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(mqon->server_portal, gop_mq_task_new(mqon->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// mq_ongoing_cb - Processes a heartbeat for clients with ongoing open handles
//***********************************************************************

void mq_ongoing_cb(void *arg, gop_mq_task_t *task)
{
    gop_mq_ongoing_t *mqon = (gop_mq_ongoing_t *)arg;
    gop_mq_frame_t *fid, *fuid;
    char *id;
    gop_mq_ongoing_host_t *oh;
    int fsize;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(1, "Processing incoming request. EXEC START now=" TT "\n",
                    ((apr_time_t) apr_time_sec(apr_time_now())));
    tbx_log_flush();

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    fid = mq_msg_pop(msg);  //** This is the ID for responses
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fuid = mq_msg_pop(msg);  //** Host/user ID
    gop_mq_get_frame(fuid, (void **)&id, &fsize);

    log_printf(2, "looking up %s\n", id);

    //** Do the lookup and update the heartbeat timeout
    apr_thread_mutex_lock(mqon->lock);
    oh = apr_hash_get(mqon->id_table, id, fsize);
    if (oh != NULL) {
        oh->hb_count++;
        MQ_DEBUG_NOTIFY( "ONGOING_CB -- heartbeat host=%s count=%d\n", oh->id, oh->hb_count);
        status = gop_success_status;
        log_printf(2, "Updating heartbeat for %s hb=%d expire=" TT "\n", id, oh->heartbeat, oh->next_check);
    } else {
        status = gop_failure_status;
        MQ_DEBUG_NOTIFY( "ONGOING_CB: ERROR -- heartbeat Unknown host! id=%s id_len=%d\n", id, fsize);
    }
    apr_thread_mutex_unlock(mqon->lock);

    gop_mq_frame_destroy(fuid);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, fid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(mqon->server_portal, gop_mq_task_new(mqon->mqc, response, NULL, NULL, 30));
}

//***********************************************************************
// mq_ongoing_server_thread - Checks to make sure heartbeats are being
//     being received from clients with open handles
//***********************************************************************

void *mq_ongoing_server_thread(apr_thread_t *th, void *data)
{
    gop_mq_ongoing_t *mqon = (gop_mq_ongoing_t *)data;
    gop_mq_ongoing_handle_t *ohandle;
    apr_hash_index_t *hi;
    gop_mq_ongoing_host_t *oh;
    char *key;
    apr_ssize_t klen;
    apr_time_t now, timeout;
    int n, ntasks;
    gop_opque_t *q;
    gop_op_generic_t *gop;

    tbx_monitor_thread_create(MON_MY_THREAD, "mq_ongoing_seerver_thread");

    timeout = apr_time_make(mqon->check_interval, 0);

    q = gop_opque_new();
    opque_start_execution(q);

    apr_thread_mutex_lock(mqon->lock);
    do {
        //** Cycle through checking each host's heartbeat
        now = apr_time_now();
        n = apr_hash_count(mqon->id_table);
        log_printf(10, "now=" TT " heartbeat table size=%d\n", apr_time_now(), n);
        ntasks = 0;
        for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);

            log_printf(10, "host=%s now=" TT " next_check=" TT "\n", oh->id,
                    ((apr_time_t) apr_time_sec(apr_time_now())),
                    ((apr_time_t) apr_time_sec(oh->next_check)));
            if ((oh->next_check < now) && (oh->next_check > 0)) { //** Expired heartbeat so check if we shutdown everything associated with the connection
                if (oh->hb_count == 0) {
                    MQ_DEBUG_NOTIFY( "ONGOING_SERVER: ERROR -- Failed heartbeat! Closing! id=%s\n", oh->id);
                    ntasks += _mq_ongoing_close(mqon, oh, q);
                    oh->next_check = 0;  //** Skip next time around
                } else {
                    if (oh->hb_count <= 2) {   //** Throw a warning that we probably missed some HBs
                        if (tbx_notify_handle) {
                            _tbx_notify_printf(tbx_notify_handle, 1, NULL, __func__, __LINE__, "ONGOING_SERVER: WARNING -- host=%s hb_count=%d\n", oh->id, oh->hb_count);
                        }
                    }
                    oh->next_check = apr_time_now() + apr_time_from_sec(oh->heartbeat);
                    MQ_DEBUG_NOTIFY( "ONGOING_SERVER: Host heartbeat -- host=%s hb_count=%d next_check=" TT "\n", oh->id, oh->hb_count, oh->next_check);
                    oh->hb_count = 0;
                }
            } else if (oh->next_check == 0) { //** See if everything has cleaned up
                MQ_DEBUG_NOTIFY( "ONGOING_SERVER: Host cleanup -- host=%s hb_count=%d\n", oh->id, oh->hb_count);
                if ((apr_hash_count(oh->table) == 0) && (oh->remove_pending <= 0)) { //** Safe to clean up
                    apr_hash_set(mqon->id_table, key, klen, NULL);
                    free(oh->id);
                    apr_pool_destroy(oh->mpool);
                    free(oh);
                }
            }
        }

        while ((gop = opque_get_next_finished(q)) != NULL) {
            ohandle = gop_get_private(gop);
            if (ohandle) {  //** Clean up
                ohandle->oo->count--;   //** Dec our place holder to keep the oo and oh alive
                if (ohandle->oo->count > 0) {   //** Somebody is still using it so flag it for release if needed. Shouldn't need to though.
                    MQ_DEBUG_NOTIFY( "ONGOING_SERVER: oo->count=%d oo->remove=%d oh->remove_pending=%d\n", ohandle->oo->count, ohandle->oo->remove, ohandle->oh->remove_pending);
                    if (ohandle->oo->remove == 0) {
                        ohandle->oo->remove = 1;
                        ohandle->oh->remove_pending++;
                    }
                } else if (ohandle->oo->remove) {   //** We're the last one so we can safely delete it
                    MQ_DEBUG_NOTIFY( "ONGOING_SERVER: oo->count=%d oo->remove=%d oh->remove_pending=%d\n", ohandle->oo->count, ohandle->oo->remove, ohandle->oh->remove_pending);
                    ohandle->oh->remove_pending--;
                    free(ohandle->oo);
                }
                free(ohandle);
            }
            gop_free(gop, OP_DESTROY);
        }

        MQ_DEBUG_NOTIFY( "ONGOING_SERVER: Loop end\n");

        //** Sleep until time for the next heartbeat or time to exit
        apr_thread_cond_timedwait(mqon->thread_cond, mqon->lock, timeout);
        n = mqon->shutdown;

        log_printf(15, "loop end n=%d\n", n);
    } while (n == 0);

    log_printf(15, "CLEANUP\n");

    //** Submit all the remaining tasks
    for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);
        if (oh->next_check > 0) ntasks += _mq_ongoing_close(mqon, oh, q);  //** Only shut down pending hosts
    }

    //** Wait for them all to complete
    apr_thread_mutex_unlock(mqon->lock);
    opque_waitall(q);
    apr_thread_mutex_lock(mqon->lock);

    //** Clean up all the pending tasks
    while ((gop = opque_waitany(q)) != NULL) {
        ohandle = gop_get_private(gop);
        if (ohandle) {  //** Clean up
            ohandle->oo->count--;   //** Dec our place holder to keep the oo and oh alive
            if (ohandle->oo->count > 0) {   //** Somebody is still using it so flag it for release
                if (ohandle->oo->remove == 0) {
                    ohandle->oo->remove = 1;
                    ohandle->oh->remove_pending++;
                }
            } else if (ohandle->oo->remove) {   //** We're the last one so we can safely delete it
                ohandle->oh->remove_pending--;
                free(ohandle->oo);
            }
            free(ohandle);
        }
        gop_free(gop, OP_DESTROY);
    }

    //** Finally destroy everything
    for (hi = apr_hash_first(NULL, mqon->id_table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &klen, (void **)&oh);
        if (oh->next_check > 0) ntasks += _mq_ongoing_close(mqon, oh, q);  //** Only shut down pending hosts

        while ((apr_hash_count(oh->table) > 0) || (oh->remove_pending > 0)) {
            n = apr_hash_count(oh->table);
            log_printf(5, "waiting on host=%s nleft=%d\n", oh->id, n);
            apr_thread_mutex_unlock(mqon->lock);
            usleep(10000);  //** Sleep and see if if clears up
            apr_thread_mutex_lock(mqon->lock);
        }

        apr_hash_set(mqon->id_table, key, klen, NULL);
        free(oh->id);
        apr_pool_destroy(oh->mpool);
        free(oh);
    }

    log_printf(15, "FINISHED\n");

    apr_thread_mutex_unlock(mqon->lock);

    log_printf(15, "EXITING\n");

    gop_opque_free(q, OP_DESTROY);

    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
// gop_mq_ongoing_destroy - Destroys an ongoing task tracker
//***********************************************************************

void gop_mq_ongoing_destroy(gop_mq_ongoing_t *mqon)
{
    apr_status_t value;
    gop_mq_command_table_t *ctable;

    //** Wake up the ongoing thread
    apr_thread_mutex_lock(mqon->lock);
    mqon->shutdown = 1;
    apr_thread_cond_broadcast(mqon->thread_cond);
    apr_thread_mutex_unlock(mqon->lock);

    //** Wait for it to shutdown
    if (mqon->ongoing_server_thread) {
        ctable = gop_mq_portal_command_table(mqon->server_portal);
        gop_mq_command_set(ctable, ONGOING_KEY, ONGOING_SIZE, mqon, NULL);

        apr_thread_join(&value, mqon->ongoing_server_thread);
    }

    if (mqon->ongoing_heartbeat_thread) apr_thread_join(&value, mqon->ongoing_heartbeat_thread);

    apr_pool_destroy(mqon->mpool);
    free(mqon);
}

//***********************************************************************
// gop_mq_ongoing_create - Creates an object to handle ongoing tasks
//***********************************************************************

gop_mq_ongoing_t *gop_mq_ongoing_create(gop_mq_context_t *mqc, gop_mq_portal_t *server_portal, int check_interval, int mode, gop_thread_pool_context_t *tp_fail)
{
    gop_mq_ongoing_t *mqon;
    gop_mq_command_table_t *ctable;

    tbx_type_malloc_clear(mqon, gop_mq_ongoing_t, 1);

    mqon->mqc = mqc;
    mqon->server_portal = server_portal;
    mqon->check_interval = check_interval;
    mqon->send_divisor = 4;
    mqon->tp_fail = tp_fail;   //** This is used for handling client failures that still have a ref

    assert_result(apr_pool_create(&(mqon->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(mqon->lock), APR_THREAD_MUTEX_DEFAULT, mqon->mpool);
    apr_thread_cond_create(&(mqon->thread_cond), mqon->mpool);
    apr_thread_cond_create(&(mqon->object_cond), mqon->mpool);

    if (mode & ONGOING_SERVER) {
        mqon->id_table = apr_hash_make(mqon->mpool);
        FATAL_UNLESS(mqon->id_table != NULL);

        ctable = gop_mq_portal_command_table(server_portal);
        gop_mq_command_set(ctable, ONGOING_KEY, ONGOING_SIZE, mqon, mq_ongoing_cb);
        gop_mq_command_set(ctable, ONGOING_LOGOUT_KEY, ONGOING_LOGOUT_SIZE, mqon, mq_ongoing_logout_cb);
        tbx_thread_create_assert(&(mqon->ongoing_server_thread), NULL, mq_ongoing_server_thread, (void *)mqon, mqon->mpool);
    }

    if (mode & ONGOING_CLIENT) {
        mqon->table = apr_hash_make(mqon->mpool);
        FATAL_UNLESS(mqon->table != NULL);

        tbx_thread_create_assert(&(mqon->ongoing_heartbeat_thread), NULL, ongoing_heartbeat_thread, (void *)mqon, mqon->mpool);
    }
    return(mqon);
}


