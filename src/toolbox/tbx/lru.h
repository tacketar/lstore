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

#pragma once
#ifndef ACCRE_LRU_H_INCLUDED
#define ACCRE_LRU_H_INCLUDED

#include <apr_time.h>
#include <apr_thread_mutex.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*tbx_lru_key_fn_t)(void *global, void *object, void **key, int *len);     //** Access a stored objects key and length
typedef void *(*tbx_lru_clone_fn_t)(void *global, void *data);   //** Clones an user object and stores it
typedef void (*tbx_lru_copy_fn_t)(void *global, void *src,void *dest);  //** Updates an existing object
typedef void (*tbx_lru_free_fn_t)(void *global, void *data);     //** Free's a cloned object

struct tbx_lru_s;
typedef struct tbx_lru_s tbx_lru_t;

// Functions
TBX_API int tbx_lru_count(tbx_lru_t *lru);
TBX_API void *tbx_lru_first(tbx_lru_t *lru);
TBX_API void *tbx_lru_next(tbx_lru_t *lru);
TBX_API tbx_lru_t *tbx_lru_create(int n_objects, tbx_lru_key_fn_t key, tbx_lru_clone_fn_t clone_item,
            tbx_lru_copy_fn_t copy_item, tbx_lru_free_fn_t free_item, void *free_global_arg);
TBX_API void tbx_lru_destroy(tbx_lru_t *lru);
TBX_API int tbx_lru_get(tbx_lru_t *lru, void *key, int klen, void *object);
TBX_API void tbx_lru_put(tbx_lru_t *lru, void *object);
TBX_API void tbx_lru_delete(tbx_lru_t *lru, void *key, int klen);

//** These are helper routines and assumes the void *arg points to an integer with the fixed size of the object
TBX_API void *tbx_lru_clone_default(void *arg, void *ptr);
TBX_API void tbx_lru_copy_default(void *arg, void *src, void *dest);
TBX_API void tbx_lru_free_default(void *arg, void *ptr);

#ifdef __cplusplus
}
#endif

#endif
