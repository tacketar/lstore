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

#ifndef __IO_WRAPPER_H_
#define __IO_WRAPPER_H_

#include <ibp/visibility.h>
#include <gop/opque.h>
#include <gop/types.h>

#ifdef __cplusplus
extern "C" {
#endif

IBP_API void ibp_io_mode_set(int sync_transfer, int print_progress, int nthreads);
IBP_API void ibp_io_start(gop_opque_t *q);
IBP_API int ibp_io_waitall(gop_opque_t *q);

#ifdef __cplusplus
}
#endif

#endif

