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
// Routines for handling generic notifications
//***********************************************************************

#define _log_module_index 154

#include <apr_pools.h>
#include <apr_thread_mutex.h>
#include <errno.h>
#include <libgen.h>
#include <regex.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <tbx/assert_result.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/notify.h>
#include <tbx/io.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include <tbx/notify.h>
#include <time.h>
#include <unistd.h>
#include "lio/os.h"
#include "authn.h"

//***********************************************************************
// notify_printf - Logs an operation
//***********************************************************************

void notify_printf(tbx_notify_t *nlog, int do_lock, lio_creds_t *creds, const char *fmt, ...)
{
    va_list args;
    char *uid;
    int len;

    if (creds) {
        uid = (char *)an_cred_get_descriptive_id(creds, &len);
        if (!uid) uid = "(null)";
    } else {
        uid ="(null)";
    }

    va_start(args, fmt);
    tbx_notify_vprintf(nlog, do_lock, uid, fmt, args);
    va_end(args);
}

//***********************************************************************
// notify_monitor_printf - Logs an operation
//***********************************************************************

void notify_monitor_printf(tbx_notify_t *nlog, int do_lock, lio_creds_t *creds, const char *mfmt, uint64_t id, const char *label, const char *efmt, ...)
{
    va_list args;
    char *uid;
    int len;

    if (creds) {
        uid = (char *)an_cred_get_descriptive_id(creds, &len);
        if (!uid) uid = "(null)";
    } else {
        uid ="(null)";
    }

    va_start(args, efmt);
    tbx_notify_monitor_vprintf(nlog, do_lock, uid, mfmt, id, label, efmt, args);
    va_end(args);
}


