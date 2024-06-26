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
#ifndef ACCRE_QUE_H_INCLUDED
#define ACCRE_QUE_H_INCLUDED

#include <tbx/assert_result.h>
#include <tbx/visibility.h>
#include <apr_time.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TBX_QUE_BLOCK 123456789  //** Only return when the get/put completes

#define TBX_QUE_TIMEOUT  -1
#define TBX_QUE_FINISHED -2

struct tbx_que_s;
typedef struct tbx_que_s tbx_que_t;

TBX_API int tbx_que_count(tbx_que_t *q);
TBX_API void tbx_que_set_finished(tbx_que_t *q);
TBX_API int tbx_que_get_finished(tbx_que_t *q);
TBX_API int tbx_que_is_finished(tbx_que_t *q);
TBX_API tbx_que_t *tbx_que_create(int n_objects, int object_size);
TBX_API void tbx_que_destroy(tbx_que_t *q);
TBX_API int tbx_que_get(tbx_que_t *q, void *object, apr_time_t dt);
TBX_API int tbx_que_bulk_get(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt);
TBX_API int tbx_que_put(tbx_que_t *q, void *object, apr_time_t dt);
TBX_API int tbx_que_bulk_put(tbx_que_t *q, int n_objects, void *objects, apr_time_t dt);

#ifdef __cplusplus
}
#endif

#endif
