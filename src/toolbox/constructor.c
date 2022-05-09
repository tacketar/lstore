/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License") TBX_IO_DECLARE_END
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <dlfcn.h>
#include <apr_errno.h>
#include <apr_general.h>
#include <tbx/assert_result.h>
#include <tbx/io.h>

#include "tbx/constructor_wrapper.h"

void _log_init();

#ifdef ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_PREPRAGMA_ARGS(tbx_construct_fn)
#endif
ACCRE_DEFINE_CONSTRUCTOR(tbx_construct_fn)
#ifdef ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_CONSTRUCTOR_POSTPRAGMA_ARGS(tbx_construct_fn)
#endif

#ifdef ACCRE_DESTRUCTOR_PREPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_PREPRAGMA_ARGS(tbx_destruct_fn)
#endif
ACCRE_DEFINE_DESTRUCTOR(tbx_destruct_fn)
#ifdef ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS
#pragma ACCRE_DESTRUCTOR_POSTPRAGMA_ARGS(tbx_destruct_fn)
#endif

static void tbx_construct_fn() {
    tbx_io_init(RTLD_DEFAULT, 0);  //** Init all the IO wrapper functions
    apr_status_t ret = apr_initialize();
    FATAL_UNLESS(ret == APR_SUCCESS);

    _log_init();
}

static void tbx_destruct_fn() {
    apr_terminate();
}
