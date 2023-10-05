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
#include <tbx/notify.h>

#ifdef __cplusplus
extern "C" {
#endif

// Functions
LIO_API void notify_printf(tbx_notify_t *nlog, int do_lock, lio_creds_t *creds, const char *fmt, ...);
LIO_API void notify_monitor_printf(tbx_notify_t *nlog, int do_lock, lio_creds_t *creds, const char *mfmt, uint64_t id, const char *label, const char *efmt, ...);

#ifdef __cplusplus
}
#endif

#endif
