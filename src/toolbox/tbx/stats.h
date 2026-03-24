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

#ifndef __TBX_STATS_H_
#define __TBX_STATS_H_

#include <tbx/atomic_counter.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TBX_STATS_ENABLED

#ifdef TBX_STATS_ENABLED
    #define TBX_STATS_GET(a) tbx_atomic_get(a)
    #define TBX_STATS_INC(a) tbx_atomic_inc(a)
    #define TBX_STATS_ADD(a, delta) tbx_atomic_add(a, delta)
    #define TBX_STATS_SUB(a, delta) tbx_atomic_sub(a, delta)
    #define TBX_STATS_CODE(...) __VA_ARGS__
#else
    #define TBX_STATS_GET(a)
    #define TBX_STATS_INC(a)
    #define TBX_STATS_ADD(a, delta)
    #define TBX_STATS_SUB(a, delta)
    #define TBX_STATS_CODE(...)
#endif

#define TBX_STATS_TYPE_COUNT  0
#define TBX_STATS_TYPE_BYTE   1
#define TBX_STATS_TYPE_TIME_1 2
#define TBX_STATS_TYPE_TIME_2 3

typedef struct {
    int type;
    tbx_atomic_int_t submitted;
    tbx_atomic_int_t finished;
    tbx_atomic_int_t errors;
} tbx_stats_t;


TBX_API void tbx_stats_printf(FILE *fd, tbx_stats_t *stats, char **labels, int n_stats);

#ifdef __cplusplus
}
#endif

#endif
