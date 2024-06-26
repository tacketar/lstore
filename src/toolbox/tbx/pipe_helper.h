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
#ifndef ACCRE_QUE_PIPE_HELPER_H_INCLUDED
#define ACCRE_QUE_PIPE_HELPER_H_INCLUDED

#include <tbx/assert_result.h>
#include <tbx/visibility.h>
#include <apr_time.h>

#ifdef __cplusplus
extern "C" {
#endif

TBX_API int tbx_pipe_open(int pipefd[2]);
TBX_API void tbx_pipe_close(int pipefd[2]);
TBX_API int tbx_pipe_get(int pipefd[2], void *object, int object_size, apr_time_t dt);
TBX_API int tbx_pipe_put(int pipefd[2], void *object, int object_size, apr_time_t dt);

#ifdef __cplusplus
}
#endif

#endif
