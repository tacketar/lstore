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

#ifndef __MISC_STATS_H_
#define __MISC_STATS_H_

#include <tbx/stats.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LIO_MISC_STATS_SLOT_FS_LIO2LOCAL     0
#define LIO_MISC_STATS_SLOT_FS_LOCAL2LIO     1
#define LIO_MISC_STATS_SLOT_FS_LIO2LIO       2
#define LIO_MISC_STATS_SLOT_LISTXATTR        3
#define LIO_MISC_STATS_SLOT_TAPEXATTR        4
#define LIO_MISC_STATS_SLOT_FLL_LISTXATTR    5
#define LIO_MISC_STATS_SLOT_FLL_GETXATTR     6
#define LIO_MISC_STATS_SLOT_FLL_READ         7
#define LIO_MISC_STATS_SLOT_LIO_LOCAL2LOCAL  8
#define LIO_MISC_STATS_SLOT_LIO_LOCAL2LIO    9
#define LIO_MISC_STATS_SLOT_LIO_LIO2LOCAL   10
#define LIO_MISC_STATS_SLOT_LIO_LIO2LIO     11
#define LIO_MISC_STATS_SLOT_LIO_FILE_COPY   12
#define LIO_MISC_STATS_SLOT_FOBJ_PC         13
#define LIO_MISC_STATS_SLOT_FOBJ_TASK_PC    14
#define LIO_MISC_STATS_SLOT_CACHE_COND_PC   15
#define LIO_MISC_STATS_SLOT_JERASE_READ     16
#define LIO_MISC_STATS_SLOT_JERASE_WRITE    17

#define LIO_MISC_STATS_SIZE   18

extern tbx_stats_t _misc_stats[LIO_MISC_STATS_SIZE];

#ifdef __cplusplus
}
#endif

#endif
