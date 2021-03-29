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

//***********************************************************************
// Disk based caching segment support
//***********************************************************************

#ifndef _SEGMENT_DISK_CACHE_H_
#define _SEGMENT_DISK_CACHE_H_

#include <gop/opque.h>
#include <gop/types.h>

#include "ds.h"
#include "ex3.h"
#include "ex3/types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SEGMENT_TYPE_DISK_CACHE "disk_cache"

lio_segment_t *segment_disk_cache_load(void *arg, ex_id_t id, lio_exnode_exchange_t *ex);
lio_segment_t *segment_disk_cache_create(void *arg);
void segment_disk_cache_set_child(lio_segment_t *seg, lio_segment_t *child_seg);

#ifdef __cplusplus
}
#endif

#endif
