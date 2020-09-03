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

#include <apr_pools.h>
#include <apr_time.h>
#include <apr_thread_cond.h>
#include <apr_thread_mutex.h>
#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h>
#include <tbx/log.h>
#include <tbx/que.h>
#include <tbx/type_malloc.h>

struct tbx_que_s {
    apr_pool_t *mpool;
    apr_thread_cond_t *get_cond;
    apr_thread_cond_t *put_cond;
    apr_thread_mutex_t *lock;
    char *array;
    int n_objects;
    int object_size;
    int slot;
    int n_used;
    int get_waiting;
    int put_waiting;
    int finished;
};

//************************************************************************************

void tbx_que_set_finished(tbx_que_t *q)
{
    apr_thread_mutex_lock(q->lock);
    q->finished = TBX_QUE_FINISHED;
    apr_thread_mutex_unlock(q->lock);

}

//************************************************************************************

int tbx_que_is_finished(tbx_que_t *q)
{
    int done;

    apr_thread_mutex_lock(q->lock);
    done = q->finished;
    apr_thread_mutex_unlock(q->lock);

    return(done);
}


//************************************************************************************

int tbx_que_count(tbx_que_t *q)
{
    int n;

    apr_thread_mutex_lock(q->lock);
    n = q->n_used;
    apr_thread_mutex_unlock(q->lock);
    return(n);
}

//************************************************************************************

void tbx_que_destroy(tbx_que_t *q)
{

    apr_thread_mutex_destroy(q->lock);
    apr_thread_cond_destroy(q->get_cond);
    apr_thread_cond_destroy(q->put_cond);
    apr_pool_destroy(q->mpool);

    free(q->array);
    free(q);
    return;
}

//************************************************************************************

tbx_que_t *tbx_que_create(int n_objects, int object_size)
{
    tbx_que_t *q;

    tbx_type_malloc_clear(q, tbx_que_t, 1);
    tbx_type_malloc_clear(q->array, char, n_objects*object_size);

    assert_result(apr_pool_create(&(q->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(q->lock), APR_THREAD_MUTEX_DEFAULT, q->mpool);
    apr_thread_cond_create(&(q->get_cond), q->mpool);
    apr_thread_cond_create(&(q->put_cond), q->mpool);

    q->n_objects = n_objects;
    q->object_size = object_size;
    q->slot = 0;
    q->n_used = 0;
    q->get_waiting = 0;
    q->put_waiting = 0;

    return(q);
}

//************************************************************************************
// tbx_que_{bulk}_put commands
//************************************************************************************

int _tbx_que_bulk_put(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt, apr_time_t stime)
{
    int i, n, slot;

    while (1) {
        if (q->n_used < q->n_objects) {  //** Got space
            if (objects != NULL) {
                i = q->n_objects - q->n_used;
                n = (i > n_objects) ? n_objects : i;
                for (i=0; i<n; i++) {
                    slot = (q->n_used + q->slot) % q->n_objects;
                    memcpy(q->array + slot*q->object_size, (char *)objects + i*q->object_size, q->object_size);
                    q->n_used++;
                }

            }

            //** Check if someone is waiting
            if (q->get_waiting > 0) apr_thread_cond_broadcast(q->get_cond);
            if (q->put_waiting > 0) apr_thread_cond_broadcast(q->put_cond);

            return(n);
        }

        //** See if we timed out
        if ((apr_time_now()-stime) > dt) return(TBX_QUE_TIMEOUT);

        //** See if we always block
        if (dt == TBX_QUE_BLOCK) stime = apr_time_now();

        //** If we made it here we have to wait for a slot to become free
        q->put_waiting++;
        apr_thread_cond_timedwait(q->put_cond, q->lock, dt);
        q->put_waiting--;
    }

    return(0);
}

//************************************************************************************

int tbx_que_bulk_put(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt)
{
    int n;
    apr_time_t stime;

    stime = apr_time_now();
    apr_thread_mutex_lock(q->lock);
    if ((q->n_used == 0) && (q->finished == TBX_QUE_FINISHED)) {
        n = TBX_QUE_FINISHED;
    } else {
        n = _tbx_que_bulk_put(q, n_objects, objects, dt, stime);
    }
    apr_thread_mutex_unlock(q->lock);

    return(n);
}

//************************************************************************************

int tbx_que_put(tbx_que_t *q, void *object, apr_time_t dt)
{
    int n;

    n = tbx_que_bulk_put(q, 1, object, dt);

    return((n == 1) ? 0 : n);
}

//************************************************************************************
// tbx_que_{bulk}_get commands
//************************************************************************************

int _tbx_que_bulk_get(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt, apr_time_t stime)
{
    int i, n;

    while (1) {
        if (q->n_used > 0) {  //** Got an object
            n = (q->n_used > n_objects) ? n_objects : q->n_used;
            if (objects != NULL) {
                for (i=0; i<n; i++) {
                    q->slot = q->slot % q->n_objects;
                    memcpy((char *)objects + i*q->object_size, q->array + q->slot*q->object_size, q->object_size);
                    q->n_used--;
                    q->slot++;
                }
            }

            //** Check if someone is waiting
            if (q->get_waiting > 0) apr_thread_cond_broadcast(q->get_cond);
            if (q->put_waiting > 0) apr_thread_cond_broadcast(q->put_cond);

            return(n);
        }

        //** See if we timed out
        if ((apr_time_now()-stime) > dt) return(TBX_QUE_TIMEOUT);

        //** See if we always block
        if (dt == TBX_QUE_BLOCK) stime = apr_time_now();

        //** If we made it here we have to wait for some data
        q->get_waiting++;
        apr_thread_cond_timedwait(q->get_cond, q->lock, dt);
        q->get_waiting--;
    }

    return(0);
}

//************************************************************************************

int tbx_que_bulk_get(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt)
{
    int n;
    apr_time_t stime;

    stime = apr_time_now();
    apr_thread_mutex_lock(q->lock);
    if ((q->n_used == 0) && (q->finished == TBX_QUE_FINISHED)) {
        n = TBX_QUE_FINISHED;
    } else {
        n = _tbx_que_bulk_get(q, n_objects, objects, dt, stime);
    }
    apr_thread_mutex_unlock(q->lock);

    return(n);
}

//************************************************************************************

int tbx_que_get(tbx_que_t *q, void *object, apr_time_t dt)
{
    int n;

    n = tbx_que_bulk_get(q, 1, object, dt);

    return((n == 1) ? 0 : n);
}
