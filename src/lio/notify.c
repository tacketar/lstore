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
#include <tbx/io.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include <time.h>
#include <unistd.h>
#include "lio/notify.h"
#include "lio/os.h"
#include "authn.h"

extern const char *__progname;

struct notify_s { //** This is used for notifications
    char *section;
    char *fname;
    char *fname_from_config;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    FILE *fd;
    struct tm tm_open;
    int pid_append;
    int exec_append;
};

struct notify_iter_s { //** Notification log iterator
    FILE *fd;
    char *prefix;
    char buffer[3*OS_PATH_MAX];
    int year;
    int month;
    int day;
    int line;
};

//***********************************************************************
// notify_log_open_check - Makes sure the fd is correct based on the time
//***********************************************************************

void notify_open_check(notify_t *nlog, time_t *now, struct tm *tm_now)
{
    char fname[OS_PATH_MAX];

    //** Get the current time
    *now = time(NULL);
    localtime_r(now, tm_now);

    //** If it's the same day just return
    if ((tm_now->tm_mday == nlog->tm_open.tm_mday) &&
        (tm_now->tm_mon == nlog->tm_open.tm_mon) &&
        (tm_now->tm_year == nlog->tm_open.tm_year)) return;

    //** The day is different so we need to close and reopen the log
    if (nlog->fd) tbx_io_fclose(nlog->fd);

    snprintf(fname, sizeof(fname), "%s.%d-%02d-%02d", nlog->fname, 1900+tm_now->tm_year, 1+tm_now->tm_mon, tm_now->tm_mday); fname[sizeof(fname)-1] = '\0';
    nlog->fd = tbx_io_fopen(fname, "a");
    if (nlog->fd == NULL) {
        log_printf(0, "ERROR opening activity_log (%s)!\n", fname);
    }
}

//***********************************************************************
// notify_printf - Logs an operation
//***********************************************************************

void notify_printf(notify_t *nlog, int do_lock, lio_creds_t *creds, const char *fmt, ...)
{
    va_list args;
    char date[128], *uid;
    int len;
    time_t now;
    struct tm tm_now;

    if (nlog->fname == NULL) return;

    if (do_lock) apr_thread_mutex_lock(nlog->lock);

    notify_open_check(nlog, &now, &tm_now);
    if (nlog->fd == NULL) goto failed;

    //** Add the header
    uid = (char *)an_cred_get_descriptive_id(creds, &len);
    if (!uid) uid = "(null)";
    asctime_r(&tm_now, date);
    date[strlen(date)-1] = '\0';  //** Peel of the return
    fprintf(nlog->fd, "[%s (" TT ") %s] ", date, now, uid);

    //** Print the user text
    va_start(args, fmt);
    vfprintf(nlog->fd, fmt, args);
    va_end(args);

    fflush(nlog->fd);

failed:
    if (do_lock) apr_thread_mutex_unlock(nlog->lock);
}


//***********************************************************************
// notify_print_running_config - Dumps the running notify config
//***********************************************************************

void notify_print_running_config(notify_t *nlog, FILE *fd, int print_section_heading)
{
    if (print_section_heading) fprintf(fd, "[%s]\n", nlog->section);
    fprintf(fd, "fname = %s\n", nlog->fname_from_config);
    fprintf(fd, "pid_append = %d\n", nlog->pid_append);
    fprintf(fd, "exec_append = %d\n", nlog->exec_append);
    fprintf(fd, "# Working prefix: %s\n", nlog->fname);
    fprintf(fd, "\n");

    return;
}

//***********************************************************************
// notify_create - Creates a notification service
//    section - Section to use from the IniFile or character string
//    ifd     - If non-null get the data from the INI file
//    text    - If ifd == NULL then this is used for retreiving the info for the notify structure
//***********************************************************************

notify_t *notify_create(tbx_inip_file_t *ifd, const char *text, char *section)
{
    notify_t *nlog;
    tbx_inip_file_t *fd;
    int n;
    uint64_t pid;
    char *fname;

    tbx_type_malloc_clear(nlog, notify_t, 1);
    nlog->section = strdup(section);

    if (ifd) {
        fd = ifd;
    } else {
        fd = tbx_inip_string_read(text, 1);
    }

    //** This acts as teh base location for the notification log
    nlog->fname_from_config = tbx_inip_get_string(ifd, section, "fname", "/lio/log/notify");
    nlog->fname = strdup(nlog->fname_from_config);

    //** See if we add the executable to the name
    nlog->exec_append = tbx_inip_get_integer(ifd, section, "exec_append", 0);
    if (nlog->exec_append) {
        n = strlen(nlog->fname) + 1 + strlen(__progname) + 1 + 1;
        tbx_type_malloc_clear(fname, char, n);
        pid = getpid();
        snprintf(fname, n-1, "%s.%s", nlog->fname, __progname);
        free(nlog->fname);
        nlog->fname = fname;
    }

    //** And possibly the PID
    nlog->pid_append = tbx_inip_get_integer(ifd, section, "pid_append", 0);
    if (nlog->pid_append) {
        n = strlen(nlog->fname) + 1 + 20;
        tbx_type_malloc_clear(fname, char, n);
        pid = getpid();
        snprintf(fname, n-1, "%s." LU, nlog->fname, pid);
        free(nlog->fname);
        nlog->fname = fname;
    }

    assert_result(apr_pool_create(&(nlog->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(nlog->lock), APR_THREAD_MUTEX_DEFAULT, nlog->mpool);

    if (!ifd) tbx_inip_destroy(fd);

    return(nlog);
}

//***********************************************************************
// notify_destroy - Destroys the notification logging service
//***********************************************************************

void notify_destroy(notify_t *nlog)
{
    if (nlog->section) free(nlog->section);
    if (nlog->fname_from_config) free(nlog->fname_from_config);
    if (nlog->fname) free(nlog->fname);
    apr_pool_destroy(nlog->mpool);
    free(nlog);
}

//***********************************************************************
//  ni_open - Opens the preconfigured notify log file and positions the iterator
//      to the next line.  Returns 0 on success
//***********************************************************************

int ni_open(notify_iter_t *ni)
{
    char fname[OS_PATH_MAX+1];
    int i;

    snprintf(fname, OS_PATH_MAX, "%s.%04d-%02d-%02d", ni->prefix, ni->year, ni->month, ni->day); fname[OS_PATH_MAX] = '\0';
    ni->fd = tbx_io_fopen(fname, "r");
    if (ni->fd == NULL) return(-1);

    //** Skip to the requested line
    for (i=0; i<ni->line; i++) {
        if (fgets(ni->buffer, sizeof(ni->buffer), ni->fd) == NULL) {
            log_printf(0, "Reached EOF while skipping lines. Moving to line %d fgets=NULL on line %d  fname=%s\n", ni->line, i, fname);
        }
    }
    return(0);
}

//***********************************************************************
// ni_next_fd - Attempts to open the next log file. On success 1 is
//     returned otherwise 0 signifies no other logs are available
//***********************************************************************

int ni_next_fd(notify_iter_t *ni)
{
    time_t now, dt;
    struct tm tm_dt;

    now = time(NULL);

    memset(&tm_dt, 0, sizeof(struct tm));
    tm_dt.tm_year = ni->year - 1900;
    tm_dt.tm_mon = ni->month - 1;
    tm_dt.tm_mday = ni->day + 1;
    tm_dt.tm_isdst = 0;
    dt = mktime(&tm_dt);

    while (dt < now) {
        ni->year = tm_dt.tm_year + 1900;
        ni->month = tm_dt.tm_mon + 1;
        ni->day = tm_dt.tm_mday;
        ni->line = 0;
        if (ni_open(ni) == 0) return(1);

        //** If we made it here no valid file so skip to the next
        tm_dt.tm_mday++;
        dt = mktime(&tm_dt);
    }

    return(0);
}

//***********************************************************************
// ni_next_entry - Attempts to read the next entry fro mthe log file.
//     If the EOF is reached NULL is returned and the ni FD is closed.
//     A subsequent call to ni_next_fd() will attempt to move to the next
//     file.
//***********************************************************************

char *ni_next_entry(notify_iter_t *ni)
{
    char *entry;
    int n;

    if (!ni->fd) return(NULL);

    entry = fgets(ni->buffer, sizeof(ni->buffer), ni->fd);
    if (entry) {
        ni->line++;
        n = strlen(entry);
        if (entry[n-1] == '\n') entry[n-1] = '\0';
        return(entry);
    }

    //** Nothing left in the file so cleanup and return;
    tbx_io_fclose(ni->fd);
    ni->fd = NULL;
    return(NULL);
}

//***********************************************************************
// notify_iter_next - Returns the next line of text in the log
//***********************************************************************

char *notify_iter_next(notify_iter_t *ni)
{
    char *entry;

again:
    if (ni->fd == NULL) { //** Open the next log file
        if (ni_next_fd(ni) == 0) return(NULL);
    }

    entry = ni_next_entry(ni);
    if (entry == NULL) goto again;

    return(entry);
}

//***********************************************************************
// notify_iter_current_time - Returns the current timestamp/line just returned
//***********************************************************************

void notify_iter_current_time(notify_iter_t *ni, int *year, int *month, int *day, int *line)
{
    *year = ni->year;
    *month = ni->month;
    *day = ni->day;
    *line = ni->line;
}

//***********************************************************************
// notify_iter_create - Creates a notification iterator
//***********************************************************************

notify_iter_t *notify_iter_create(char *prefix, int year, int month, int day, int line)
{
    notify_iter_t *ni;

    tbx_type_malloc_clear(ni, notify_iter_t, 1);
    ni->prefix = strdup(prefix);
    ni->year = year;
    ni->month = month;
    ni->day = day;
    ni->line = line;
    ni_open(ni);

    return(ni);
}

//***********************************************************************
// notify_iter_destroy - Destroys a notification iterator
//***********************************************************************

void notify_iter_destroy(notify_iter_t *ni)
{
    if (ni->fd) tbx_io_fclose(ni->fd);
    if (ni->prefix) free(ni->prefix);
    free(ni);
}

