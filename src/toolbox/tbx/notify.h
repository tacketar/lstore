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

#ifndef ACCRE_TBX_NOTIFY_H_INCLUDED
#define ACCRE_TBX_NOTIFY_H_INCLUDED

#include <tbx/visibility.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct tbx_notify_s tbx_notify_t;
typedef struct tbx_notify_iter_s tbx_notify_iter_t;

// Optional Extern to install a common notify handle for everyone to use
extern TBX_API tbx_notify_t *tbx_notify_handle;

// Functions
TBX_API void tbx_notify_print_running_config(tbx_notify_t *nlog, FILE *fd, int print_section_heading);
TBX_API void tbx_notify_vprintf(tbx_notify_t *nlog, int do_lock, const char *user, const char *fmt, va_list ap);
TBX_API void tbx_notify_monitor_vprintf(tbx_notify_t *nlog, int do_lock, const char *user, const char *mfmt, uint64_t id, const char *label, const char *efmt, va_list ap);
TBX_API void tbx_notify_printf(tbx_notify_t *nlog, int do_lock, const char *user, const char *fmt, ...);
TBX_API void tbx_notify_monitor_printf(tbx_notify_t *nlog, int do_lock, const char *user, const char *mfmt, uint64_t id, const char *label, const char *efmt, ...);
TBX_API tbx_notify_t *tbx_notify_create(tbx_inip_file_t *ifd, const char *text, char *section);
TBX_API void tbx_notify_destroy(tbx_notify_t *nlog);

TBX_API char *tbx_notify_iter_next(tbx_notify_iter_t *nli);
TBX_API void tbx_notify_iter_current_time(tbx_notify_iter_t *nli, int *year, int *month, int *day, int *line);
TBX_API tbx_notify_iter_t *tbx_notify_iter_create(char *prefix, int year, int month, int day, int line);
TBX_API void tbx_notify_iter_destroy(tbx_notify_iter_t *nli);

#ifdef __cplusplus
}
#endif

#endif
