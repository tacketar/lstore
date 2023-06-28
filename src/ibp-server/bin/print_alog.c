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

#include <assert.h>
#include <time.h>
#include "ibp_server.h"
#include "activity_log.h"
#include <tbx/constructor.h>
#include <tbx/log.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    long pos, eof_pos;
    uint64_t ui;
    ibp_time_t t;
    alog_file_header_t ah;
    char start_time[256], end_time[256];

    tbx_construct_fn_static();

    _alog_init_constants();

    if (argc < 2) {
        printf("print_alog filename\n");
        return (0);
    }

    activity_log_t *alog = activity_log_open(argv[1], 0, ALOG_READ);

    pos = ftell(alog->fd);
    fseek(alog->fd, 0, SEEK_END);
    eof_pos = ftell(alog->fd);
    fseek(alog->fd, pos, SEEK_SET);

    //** Print the header **
    ah = get_alog_header(alog);
    printf("------------------------------------------------------------------\n");
    ui = eof_pos;
    printf("Activity log: %s  (" LU " bytes)\n", argv[1], ui);
    printf("Version: " LU "\n", ah.version);
    printf("Current State: " LU " (1=GOOD, 0=BAD)\n", ah.state);
    t = ibp2apr_time(ah.start_time);
    apr_ctime(start_time, t);
    t = ibp2apr_time(ah.end_time);
    apr_ctime(end_time, t);
    printf("Start date: %s (" LU ")\n", start_time, ibp2apr_time(ah.start_time));
    printf("  End date: %s (" LU ")\n", end_time, ibp2apr_time(ah.end_time));
    printf("------------------------------------------------------------------\n\n");


    do {
        pos = ftell(alog->fd);
    } while (activity_log_read_next_entry(alog, stdout) == 0);

    pos = ftell(alog->fd);
    if (pos != eof_pos) {
        printf("print_alog: Processing aborted due to short record!  curr_pos = %ld eof = %ld\n",
               pos, eof_pos);
    }

    activity_log_close(alog);

    apr_terminate();

    return (0);
}
