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
#ifndef ACCRE_APR_POOL_WRAPPER_H_INCLUDED
#define ACCRE_APR_POOL_WRAPPER_H_INCLUDED

#include <tbx/assert_result.h>
#include <tbx/visibility.h>
#include <apr_pools.h>

#ifdef __cplusplus
extern "C" {
#endif

TBX_API apr_status_t tbx_apr_pool_create(apr_pool_t **new_pool, apr_pool_t *parent);
TBX_API void tbx_apr_pool_destroy(apr_pool_t *pool);

#ifdef __cplusplus
}
#endif

#endif
