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

#ifndef ACCRE_LIO_NOTIFY_H_INCLUDED
#define ACCRE_LIO_NOTIFY_H_INCLUDED

#include <lio/visibility.h>
#include <lio/authn.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct notify_s notify_t;
typedef struct notify_iter_s notify_iter_t;

// Functions
LIO_API void notify_print_running_config(notify_t *nlog, FILE *fd, int print_section_heading);
LIO_API void notify_printf(notify_t *nlog, int do_lock, lio_creds_t *creds, const char *fmt, ...);
LIO_API void notify_monitor_printf(notify_t *nlog, int do_lock, lio_creds_t *creds, const char *mfmt, uint64_t id, const char *label, const char *efmt, ...);
LIO_API notify_t *notify_create(tbx_inip_file_t *ifd, const char *text, char *section);
LIO_API void notify_destroy(notify_t *nlog);

LIO_API char *notify_iter_next(notify_iter_t *nli);
LIO_API void notify_iter_current_time(notify_iter_t *nli, int *year, int *month, int *day, int *line);
LIO_API notify_iter_t *notify_iter_create(char *prefix, int year, int month, int day, int line);
LIO_API void notify_iter_destroy(notify_iter_t *nli);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_AUTHN_ABSTRACT_H_INCLUDED ^ */
