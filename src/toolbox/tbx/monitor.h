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
#ifndef ACCRE_MONITOR_H_INCLUDED
#define ACCRE_MONITOR_H_INCLUDED

#include <tbx/visibility.h>
#include <inttypes.h>
#include <stdio.h>
#include <apr_time.h>

#ifdef __cplusplus
extern "C" {
#endif

//** Record/command types
#define MON_REC_UNKNOWN         0
#define MON_REC_OBJ_CREATE      1
#define MON_REC_OBJ_DESTROY     2
#define MON_REC_OBJ_LABEL       3
#define MON_REC_OBJ_MESSAGE     4
#define MON_REC_OBJ_INT         5
#define MON_REC_OBJ_GROUP       6
#define MON_REC_OBJ_UNGROUP     7
#define MON_REC_THREAD_CREATE   8
#define MON_REC_THREAD_DESTROY  9
#define MON_REC_THREAD_LABEL   10
#define MON_REC_THREAD_MESSAGE 11
#define MON_REC_THREAD_INT     12
#define MON_REC_THREAD_GROUP   13
#define MON_REC_THREAD_UNGROUP 14

#define MON_MY_THREAD 0

typedef struct {
    uint64_t       id;
    unsigned char type;
} __attribute__((__packed__)) tbx_mon_object_t;

// Functions
TBX_API int tbx_monitor_create(const char *fname);
TBX_API void tbx_monitor_destroy();
TBX_API void tbx_monitor_set_state(int n);
TBX_API int tbx_monitor_enabled();
TBX_API void tbx_monitor_flush();

TBX_API tbx_mon_object_t *tbx_monitor_object_fill(tbx_mon_object_t *obj, unsigned char type, uint64_t id);
TBX_API void tbx_monitor_obj_create(tbx_mon_object_t *obj, const char *fmt, ...);
TBX_API void tbx_monitor_obj_destroy(tbx_mon_object_t *obj);
TBX_API void tbx_monitor_obj_label(tbx_mon_object_t *obj, const char *fmt, ...);
TBX_API void tbx_monitor_obj_message(tbx_mon_object_t *obj, const char *fmt, ...);
TBX_API void tbx_monitor_obj_integer(tbx_mon_object_t *obj, int64_t n);
TBX_API void tbx_monitor_obj_group(tbx_mon_object_t *a, tbx_mon_object_t *b);
TBX_API void tbx_monitor_obj_ungroup(tbx_mon_object_t *a, tbx_mon_object_t *b);

TBX_API void tbx_monitor_thread_create(int32_t tid, const char *fmt, ...);
TBX_API void tbx_monitor_thread_destroy(int32_t tid);
TBX_API void tbx_monitor_thread_label(int32_t tid, const char *fmt, ...);
TBX_API void tbx_monitor_thread_message(int32_t tid, const char *fmt, ...) ;

TBX_API void tbx_monitor_thread_group(tbx_mon_object_t *a, int32_t tid);
TBX_API void tbx_monitor_thread_ungroup(tbx_mon_object_t *a, int32_t tid);

TBX_API void tbx_monitor_obj_destroy_quick(unsigned char type, uint64_t id);
TBX_API void tbx_monitor_obj_message_quick(unsigned char type, uint64_t id, const char *fmt, ...);
TBX_API void tbx_monitor_obj_create_quick(unsigned char type, uint64_t id, const char *fmt, ...);

TBX_API FILE *tbx_monitor_open(const char *fname);
TBX_API void tbx_monitor_close(FILE *fd);
TBX_API int tbx_monitor_get_next(FILE *fd, int *cmd, int32_t *tid, apr_time_t *dt, tbx_mon_object_t *a, tbx_mon_object_t *b, char **text, int *text_size, int32_t *b_tid, int64_t *n);
TBX_API int tbx_monitor_parse_log(const char *fname, const char **obj_types, const char *stime, tbx_mon_object_t *obj_list, int n_obj, int32_t *tid_list, int n_tid, FILE *fd_out);
#ifdef __cplusplus
}
#endif

#endif
