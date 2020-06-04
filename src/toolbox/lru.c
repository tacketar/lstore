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
#include <apr_hash.h>
#include <apr_thread_mutex.h>
#include <tbx/log.h>
#include <tbx/lru.h>
#include <tbx/type_malloc.h>

typedef struct {
    void *object;          //** Object;
    tbx_stack_ele_t *ele;  //** Stack location
} _lru_entry_t;

struct tbx_lru_s {
    tbx_lru_key_fn_t key;       //** Routine to getthe key from a stored item
    tbx_lru_clone_fn_t clone;   //** Routine to clone a user item and store it
    tbx_lru_copy_fn_t copy;     //** Routine to update an exsiting object with new information
    tbx_lru_free_fn_t free;     //** Routine to free an item
    void *global_arg;           //** Global argument for destroying an item
    tbx_stack_t *que;           //** LRU queue
    tbx_stack_t *unused;        //** LRU queue of unused entries
    apr_hash_t *hash;           //** Hash for direct key lookup
    int max_items;              //** Max number of items to store before destruction
    apr_pool_t *pool;           //** Pool used for hash and lock
    apr_thread_mutex_t *lock;   //** Thread-saftey lock
};

//************************************************************************************
// Helper LRU routines for simple malloc/free type objects.
//    These routines assume the void *arg points to an integer which contains the size
//    of the object being manipulated.
//************************************************************************************

void *tbx_lru_clone_default(void *arg, void *ptr)
{
    int *n = (int *)arg;
    void *object;

    object = malloc(*n);
    memcpy(object, ptr, *n);
    return(object);
}

//************************************************************************************

void tbx_lru_copy_default(void *arg, void *src, void *dest)
{
    int *n = (int *)arg;
    memcpy(dest, src, *n);
}

//************************************************************************************

void tbx_lru_free_default(void *arg, void *ptr)
{
    free(ptr);
}

//************************************************************************************

int tbx_lru_count(tbx_lru_t *lru)
{
    return(tbx_stack_count(lru->que));
}

//************************************************************************************
// LRU iterator routines - NOTE: these routines are not thread safe!
//     Calling non-iterator routines during use can cause issues!
//************************************************************************************

void *tbx_lru_first(tbx_lru_t *lru)
{
    _lru_entry_t *entry;

    tbx_stack_move_to_top(lru->que);
    entry = tbx_stack_get_current_data(lru->que);
    return(entry ? entry->object : NULL);
}

//************************************************************************************
// LRU iterator routines - NOTE: these routines are not thread safe!
//     Calling non-iterator routines during use can cause issues!
//************************************************************************************

void *tbx_lru_next(tbx_lru_t *lru)
{
    _lru_entry_t *entry;

    tbx_stack_move_down(lru->que);
    entry = tbx_stack_get_current_data(lru->que);
    return(entry ? entry->object : NULL);
}

//************************************************************************************
// tbx_lru_get - Retreives an object and copies the data into the provied buffer
//   On success 0 is returned otherwise the object could not be found
//************************************************************************************

int tbx_lru_get(tbx_lru_t *lru, void *key, int klen, void *object)
{
    _lru_entry_t *entry;
    int err = 1;

    apr_thread_mutex_lock(lru->lock);
    entry = apr_hash_get(lru->hash, key, klen);
    if (entry == NULL) goto finished;  //** Not found;

    //** Got it so copy it
    lru->copy(lru->global_arg, entry->object, object);

    //** Move it to the top
    tbx_stack_move_to_ptr(lru->que, entry->ele);
    tbx_stack_unlink_current(lru->que, 0);
    tbx_stack_move_to_top(lru->que);
    tbx_stack_link_insert_above(lru->que, entry->ele);

    //** Flag no issues
    err = 0;

finished:
    apr_thread_mutex_unlock(lru->lock);
    return(err);
}

//************************************************************************************
//  _lru_make_room - Deletes the LRU entry
//    NOTE: Assumes lock is already held
//************************************************************************************

void _lru_make_room(tbx_lru_t *lru)
{
    _lru_entry_t *entry;
    void *key;
    int klen;

    //** Remove it from the que
    tbx_stack_move_to_bottom(lru->que);
    entry = tbx_stack_get_current_data(lru->que);
    tbx_stack_unlink_current(lru->que, 1);

    //** And also the hash
    lru->key(lru->global_arg, entry->object, &key, &klen);
    apr_hash_set(lru->hash, key, klen, NULL);

    //** Finally put it on the unused que
    tbx_stack_move_to_bottom(lru->unused);
    tbx_stack_link_insert_below(lru->unused, entry->ele);
}

//************************************************************************************
// _lru_get_entry - Returns an unsed entry or makes a new one if needed
//************************************************************************************

_lru_entry_t *_lru_get_entry(tbx_lru_t *lru)
{
   _lru_entry_t *entry;

   if (tbx_stack_count(lru->unused) > 0) {
       tbx_stack_move_to_bottom(lru->unused);
       entry = tbx_stack_get_current_data(lru->unused);
       tbx_stack_unlink_current(lru->unused, 1);
   } else {
       tbx_type_malloc_clear(entry, _lru_entry_t, 1);
   }

   return(entry);
}

//************************************************************************************
// tbx_lru_put - Stores an object in the LRU.  If it exists it's updated otherwise the
//   object is cloned and stored.
//************************************************************************************

void tbx_lru_put(tbx_lru_t *lru, void *object)
{
    _lru_entry_t *entry;
    void *key;
    int klen;

    lru->key(lru->global_arg, object, &key, &klen);

    apr_thread_mutex_lock(lru->lock);

    entry = apr_hash_get(lru->hash, key, klen);
    if (entry == NULL) { //** Doesn't exist so need to add it
        if (tbx_stack_count(lru->que) >= lru->max_items) {  //** Need to delete the LRU entry
            _lru_make_room(lru);
        }
        entry = _lru_get_entry(lru);
        if (entry->ele) {
            tbx_stack_move_to_top(lru->que);
            tbx_stack_link_insert_above(lru->que, entry->ele);
        } else {
            tbx_stack_push(lru->que, entry);
            entry->ele = tbx_stack_get_current_ptr(lru->que);
        }
    } else { //** Move it to the top
        tbx_stack_move_to_ptr(lru->que, entry->ele);
        tbx_stack_unlink_current(lru->que, 0);
        tbx_stack_move_to_top(lru->que);
        tbx_stack_link_insert_above(lru->que, entry->ele);
    }

    //** Got it so copy it it
    if (entry->object == NULL) {
        entry->object = lru->clone(lru->global_arg, object);
    } else {
        lru->copy(lru->global_arg, object, entry->object);
    }

    lru->key(lru->global_arg, entry->object, &key, &klen);
    apr_hash_set(lru->hash, key, klen, entry);
    apr_thread_mutex_unlock(lru->lock);
}

//************************************************************************************
// tbx_lru_delete - Deletes the object associated with the key if it exists.
//************************************************************************************

void tbx_lru_delete(tbx_lru_t *lru, void *key, int klen)
{
    _lru_entry_t *entry;

    apr_thread_mutex_lock(lru->lock);

    entry = apr_hash_get(lru->hash, key, klen);
    if (entry == NULL) goto finished; //** Doesn't exist so kick out

    //** Remove it from the que
    tbx_stack_move_to_ptr(lru->que, entry->ele);
    tbx_stack_unlink_current(lru->que, 0);

    //** And also the hash
    apr_hash_set(lru->hash, key, klen, NULL);

    //** Finally put it on the unused que
    tbx_stack_move_to_bottom(lru->unused);
    tbx_stack_link_insert_below(lru->unused, entry->ele);

finished:
    apr_thread_mutex_unlock(lru->lock);
}

//************************************************************************************
//  tbx_lru_destroy - Destroy an LRU queue
//************************************************************************************

void tbx_lru_destroy(tbx_lru_t *lru)
{
    _lru_entry_t *entry;

    //** Free the used que objects
    while ((entry = tbx_stack_pop(lru->que)) != NULL) {
        lru->free(lru->global_arg, entry->object);
        free(entry);
    }
    tbx_stack_free(lru->que, 0);

    //** Free the unused que objects
    while ((entry = tbx_stack_pop(lru->unused)) != NULL) {
        lru->free(lru->global_arg, entry->object);
        free(entry);
    }
    tbx_stack_free(lru->unused, 0);

    apr_thread_mutex_destroy(lru->lock);
    apr_pool_destroy(lru->pool);  //** This also destroys the hash

    free(lru);
    return;
}

//************************************************************************************
// tbx_lru_create - Create an LRU queue.
//************************************************************************************

tbx_lru_t *tbx_lru_create(int n_objects, tbx_lru_key_fn_t get_key, tbx_lru_clone_fn_t clone_item,
            tbx_lru_copy_fn_t copy_item, tbx_lru_free_fn_t free_item, void *free_global_arg)
{
    tbx_lru_t *lru;

    tbx_type_malloc_clear(lru, tbx_lru_t, 1);
    lru->que = tbx_stack_new();
    lru->unused = tbx_stack_new();
    lru->max_items = n_objects;
    lru->key = get_key;
    lru->clone = clone_item;
    lru->copy = copy_item;
    lru->free = free_item;
    lru->global_arg = free_global_arg;

    assert_result(apr_pool_create(&(lru->pool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(lru->lock), APR_THREAD_MUTEX_DEFAULT, lru->pool);
    lru->hash = apr_hash_make(lru->pool);

    return(lru);
}
