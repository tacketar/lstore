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

#include <stdio.h>
#include <tbx/stats.h>
#include <tbx/string_token.h>


#define XOT  "%" PRId64    //int64_t

//***********************************************************************
// tbx_stats_printf - Prints the stats structure
//***********************************************************************

void tbx_stats_printf(FILE *fd, tbx_stats_t *stats, char **labels, int n_stats)
{
    int i, optype;
    int64_t finished, submitted, pending, errors;
    char ppbuf1[100], ppbuf2[100], ppbuf3[100];;

    for (i=0; i<n_stats; i++) {
        finished = TBX_STATS_GET(stats[i].finished);
        submitted = TBX_STATS_GET(stats[i].submitted);
        errors = TBX_STATS_GET(stats[i].errors);
        pending = submitted - finished;
        optype = stats[i].type;
        if (optype == TBX_STATS_TYPE_COUNT) {
            fprintf(fd, "    %30s[%02d]:  submitted=" XOT " finished=" XOT " pending=" XOT " errors=" XOT "\n", labels[i], i, submitted, finished, pending, errors);
        } else if (optype == TBX_STATS_TYPE_BYTE) {
            fprintf(fd, "    %30s[%02d]:  submitted=%s finished=%s pending=%s\n", labels[i], i,
                tbx_stk_pretty_print_double_with_scale(1024, submitted, ppbuf1),
                tbx_stk_pretty_print_double_with_scale(1024, finished, ppbuf2),
                tbx_stk_pretty_print_double_with_scale(1024, pending, ppbuf3));
        } else if (optype == TBX_STATS_TYPE_TIME_1) {
            fprintf(fd, "    %30s[%02d]:  TIME=%s\n", labels[i], i, tbx_stk_pretty_print_time(submitted, 1, ppbuf1));
        } else if (optype == TBX_STATS_TYPE_TIME_2) {
            fprintf(fd, "    %30s[%02d]:  READ=%s  WRITE=%s\n", labels[i], i, tbx_stk_pretty_print_time(submitted, 1, ppbuf1), tbx_stk_pretty_print_time(finished, 1, ppbuf2));
        }
    }
}
