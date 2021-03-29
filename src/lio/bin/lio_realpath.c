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

#define _log_module_index 190

#include <errno.h>
#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>

#include <lio/blacklist.h>
#include <lio/cache.h>
#include <lio/lio.h>
#include <lio/os.h>

char *sep = NULL;

//*************************************************************************
// realpath_fn - Actual realpath function
//*************************************************************************

gop_op_status_t realpath_fn(void *arg, int id)
{
    lio_path_tuple_t *tuple = (lio_path_tuple_t *)arg;
    char realpath[OS_PATH_MAX];
    gop_op_status_t status;

    status = gop_sync_exec_status(lio_realpath_gop(tuple->lc, tuple->creds, tuple->path, realpath));

    if (status.op_status == OP_STATE_SUCCESS) {
        if (sep) {
            fprintf(stdout, "%s%s%s\n", tuple->path, sep, realpath);
        } else {
            fprintf(stdout, "%s\n", realpath);
        }
    } else {
        fprintf(stderr, "ERROR: %s\n", tuple->path);
        status = gop_failure_status;
    }

    return(status);
}


//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_index, start_option, return_code;
    char *path;
    tbx_stdinarray_iter_t *it;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    lio_path_tuple_t *tuple;

    if (argc < 2) {
        printf("\n");
        printf("lio_realpath LIO_COMMON_OPTIONS [-2 sep] file1 file2 ...\n");
        lio_print_options(stdout);
        printf("    -2              - Print both path and realpath:\n");
        printf("                          path->realpath\n");
        printf("    file*           - New files to create\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-2") == 0) { //** Print both path and realpath
            i++;
            sep = argv[i];
            i++;
        }
    } while (start_option - i < 0);

    start_index = i;

    //** This is the file to resolve
    if (argv[i] == NULL) {
        fprintf(stderr, "Missing file!\n");
        return(2);
    }


    //** Spawn the tasks
    q = gop_opque_new();
    opque_start_execution(q);
    return_code = 0;
    it = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while (1) {
        path = tbx_stdinarray_iter_next(it);
        if (path) {
            tbx_type_malloc(tuple, lio_path_tuple_t, 1);
            *tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple->is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                free(path);
                free(tuple);
                return_code = EINVAL;
                continue;
            }
            free(path);

            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, realpath_fn, tuple, NULL, 1);
            gop_set_private(gop, tuple);
            gop_opque_add(q, gop);
        }

        if ((gop_opque_tasks_left(q) > lio_parallel_task_count) || (path == NULL)) {
            gop = opque_waitany(q);
            if (!gop) break;

            status = gop_get_status(gop);
            tuple = gop_get_private(gop);
            if (status.op_status != OP_STATE_SUCCESS) {
                return_code = EIO;
            }
            lio_path_release(tuple);
            free(tuple);
            gop_free(gop, OP_DESTROY);
        }
    }

    gop_opque_free(q, OP_DESTROY);

    tbx_stdinarray_iter_destroy(it);
    lio_shutdown();

    return(return_code);
}


