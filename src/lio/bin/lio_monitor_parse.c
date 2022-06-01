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
#include <tbx/lio_monitor.h>
#include <tbx/string_token.h>

const char *type_label[256];

//*************************************************************************

void populate_types()
{
    memset(type_label, 0, sizeof(type_label));

    type_label[MON_INDEX_NSSEND] = "nssend";
    type_label[MON_INDEX_NSRECV] = "nsrecv";
    type_label[MON_INDEX_GOP] = "gid";
    type_label[MON_INDEX_QUE] = "qid";
    type_label[MON_INDEX_HPSEND]  = "hpsend";
    type_label[MON_INDEX_HPRECV]  = "hprecv";

    type_label[MON_INDEX_IBP] = "ibp";

    type_label[MON_INDEX_LIO]   = "lio";
    type_label[MON_INDEX_AUTHN] = "authn";
    type_label[MON_INDEX_DS]    = "ds";
    type_label[MON_INDEX_RS]    = "rs";
    type_label[MON_INDEX_OS]    = "os";
    type_label[MON_INDEX_OSAZ]  = "osaz";
    type_label[MON_INDEX_SEG]   = "sid";
    type_label[MON_INDEX_FS]   = "fsid";
}

//*************************************************************************

void print_help()
{
    int i;

    fprintf(stderr, "lio_monitor_parse [-t tid] [-o type id] [--dt time] [--out fname.out] --in fname.mon\n");
    fprintf(stderr, "    --in fname.mon  - Monitoring log file to parse\n");
    fprintf(stderr, "    --out fname.out - Human readable output file.  Defaults to stdout\n");
    fprintf(stderr, "    --dt  time  - Offset the absolute time using the value provided.  If '-' then use the 1st record as the base time.\n");
    fprintf(stderr, "    -t tid     - Thread to track. Multiple entries supported.\n");
    fprintf(stderr, "    -o type id - Object to track. Multiple entries supported.\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "Object types:\n");
    for (i=0; i<256; i++) {
        if (type_label[i]) fprintf(stderr, "    %d - %s\n", i, type_label[i]);
    }
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option;
    char *fname = NULL;
    char *fname_out = NULL;
    char *stime;
    int n_obj, n_tid;
    int32_t tid_list[1024];
    tbx_mon_object_t obj_list[1024];
    FILE *fd_out = stdout;

    populate_types();

    if (argc < 2) {
        print_help();
        return(1);
    }

    i = 0;
    stime = NULL;
    i++;
    n_tid = n_obj = 0;
    if (i<argc) {
        do {
            start_option = i;

            if (strcmp(argv[i], "-t") == 0) {  //** Thread object
                i++;
                tid_list[n_tid] = tbx_stk_string_get_integer(argv[i]);
fprintf(stderr, "TRACKING_TID n_tid=%d tid=%d\n", n_tid, tid_list[n_tid]);
                n_tid++;
                i++;
            } else if (strcmp(argv[i], "-o") == 0) {
                i++;
                obj_list[n_obj].type = tbx_stk_string_get_integer(argv[i]);
                i++;
                obj_list[n_obj].id = tbx_stk_string_get_integer(argv[i]);
                i++;
fprintf(stderr, "TRACKING_OBJ n_obj=%d type=%d id=%ld\n", n_obj, obj_list[n_obj].type, obj_list[n_obj].id);
                n_obj++;
            } else if (strcmp(argv[i], "--dt") == 0) {
                i++;
                stime = argv[i];
fprintf(stderr, "TIME OFFSET: %s\n", stime);
                i++;
            } else if (strcmp(argv[i], "--in") == 0) {
                i++;
                fname = argv[i];
                i++;
            } else if (strcmp(argv[i], "--out") == 0) {
                i++;
                fname_out = argv[i];
                fd_out = fopen(fname_out, "w");
                if (!fd_out) {
                    fprintf(stderr, "ERROR: Unable to open output file: %s\n", fname_out);
                    return(1);
                }
                i++;
            }
        } while ((start_option - i < 0) && (i<argc));
    }

    if (!fname) {
        fprintf(stderr, "ERROR: missing filename!\n");
        return(1);
    }

    i = tbx_monitor_parse_log(fname, type_label, stime, obj_list, n_obj, tid_list, n_tid, fd_out);
    if (fname_out) fclose(fd_out);

    return(i);
}


