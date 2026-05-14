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
#ifndef ACCRE_TYPE_MALLOC_H_INCLUDED
#define ACCRE_TYPE_MALLOC_H_INCLUDED

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <tbx/assert_result.h>
#include <tbx/visibility.h>

//** This always gets exported
TBX_API void tbx_type_malloc_stats_printf(FILE *fd);

//** If we have enabled profiling then export the functions
#ifdef ENABLE_TBX_TYPE_MALLOC_PROFILE
    TBX_API void *_tm_malloc(size_t nbytes);
    TBX_API void *_tm_realloc(void *ptr, size_t nbytes);
    TBX_API void *_tm_aligned_alloc(size_t alignment, size_t nbytes);
    TBX_API void _tm_free(void *ptr);
    #define _tm_wrap_malloc  _tm_malloc
    #define _tm_wrap_realloc _tm_realloc
    #define _tm_wrap_aligned_alloc _tm_aligned_alloc
    #define _tm_wrap_free _tm_free
#else  //** Profiling is disabled so just alias to the stock routines
    #define _tm_wrap_malloc malloc
    #define _tm_wrap_realloc realloc
    #define _tm_wrap_aligned_alloc aligned_alloc
    #define _tm_wrap_free free
#endif

//** These are what applications use.
#define tbx_fudge_align_size(n, align) {n = n/(align); n = (n==0) ? align : (align)*n; }
#define tbx_ptr_is_aligned(var, align) \
    (((uintptr_t)(const void *)(var)) % (align) == 0)

#define tbx_malloc(var, size) var = _tm_wrap_malloc(size)
#define tbx_malloc_align(var, align, size) var = _tm_wrap_aligned_alloc(align, size)
#define tbx_type_malloc_clear(var, type, count) \
            tbx_type_malloc(var, type, count)
#define tbx_type_malloc(var, type, count) \
            do { \
                var = (type *)_tm_wrap_malloc((count) * sizeof(type)); \
                memset(var, 0, (count)*sizeof(type)); \
                FATAL_UNLESS(var != NULL); \
            } while(0) \

#define tbx_type_realloc(var, type, count) \
            var = (type *)_tm_wrap_realloc(var, sizeof(type)*(count)); FATAL_UNLESS(var != NULL)
#define tbx_type_memclear(var, type, count) \
            memset(var, 0, sizeof(type)*(count))
#define tbx_free _tm_wrap_free
#endif
