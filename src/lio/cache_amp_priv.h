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
//*************************************************************************
//*************************************************************************

#ifndef __CACHE_AMP_PRIV_H_
#define __CACHE_AMP_PRIV_H_


#ifdef __cplusplus
extern "C" {
#endif

#include "cache.h"

#define CAMP_ACCESSED 1  //** Page has been accessed
#define CAMP_TAG      2  //** Tag page for pretech
#define CAMP_OLD      4  //** Page has been recycled without a hit

typedef struct page_amp_t page_amp_t;
struct page_amp_t {
    cache_page_t page;  //** Actual page
    tbx_stack_ele_t *ele;   //** LRU position
    ex_off_t stream_offset;
    int bit_fields;
};

typedef struct amp_page_stream_t amp_page_stream_t;
struct amp_page_stream_t {
    ex_off_t last_offset;
    ex_off_t nbytes;
    int prefetch_size;
    int trigger_distance;
};

typedef struct amp_stream_table_t amp_stream_table_t;
struct amp_stream_table_t {
    int   max_streams;
    amp_page_stream_t *stream_table;
    tbx_list_t *streams;
    int index;
    int start_apt_pages;
};

typedef struct cache_amp_t cache_amp_t;
struct cache_amp_t {
    tbx_stack_t *stack;
    tbx_stack_t *waiting_stack;
    tbx_stack_t *pending_free_tasks;
    tbx_pc_t *free_pending_tables;
    tbx_pc_t *free_page_tables;
    apr_thread_cond_t *dirty_trigger;
    apr_thread_t *dirty_thread;
    apr_time_t dirty_max_wait;
    ex_off_t max_bytes;
    ex_off_t bytes_used;
    ex_off_t dirty_bytes_trigger;
    ex_off_t prefetch_in_process;
    ex_off_t async_prefetch_threshold;
    ex_off_t min_prefetch_size;
    double   dirty_fraction;
    int      max_streams;
    int      flush_in_progress;
    int      limbo_pages;
};

typedef struct amp_page_wait_t amp_page_wait_t;
struct amp_page_wait_t {
    apr_thread_cond_t *cond;
    ex_off_t  bytes_needed;
};

#ifdef __cplusplus
}
#endif

#endif



