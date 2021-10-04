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

#define _log_module_index 207

#include <rocksdb/c.h>
#include <apr.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <ibp/types.h>
#include <lio/authn.h>
#include <lio/ds.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/que.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include "warmer_helpers.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option, rg_mode, ftype, prefix_len, return_code, n_parts;
    char *fname, *path;
    char *keys[] = { "system.exnode", "system.inode", "system.write_errors" };
    char *vals[3];
    char *db_base = "/lio/log/warm";
    int v_size[3];
    os_object_iter_t *it;
    lio_os_regex_table_t *rp_single, *ro_single;
    void *piter;
    lio_path_tuple_t tuple;
    int recurse_depth = 10000;
    warm_prep_db_t *wdb;

    n_parts = 1024;
    return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_warm_prep_walk LIO_COMMON_OPTIONS [-db DB_output_dir] [-rd recurse_depth] [-n_partitions] LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -db DB_output_dir - Output Directory for the DBes. Default is %s\n", db_base);
        printf("    -rd recurse_depth - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -n_partitions n   - NUmber of partitions for managing workload. Defaults to %d\n", n_parts);
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //*** Parse the path args
    rp_single = ro_single = NULL;
    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db") == 0) { //** DB output base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-n_partitions") == 0) { //** Number of partitions to manage workload
            i++;
            n_parts = atoi(argv[i]);
            i++;
        }

    } while ((start_option < i) && (i<argc));
    start_option = i;

    if (rg_mode == 0) {
        if (i>=argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_option--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    wdb = create_prep_db(db_base, n_parts);  //** Create the DB

    piter = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));
    while ((path = tbx_stdinarray_iter_next(piter)) != NULL) {
        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                free(path);
                return_code = EINVAL;
                continue;
            }
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
        } else {
            rg_mode = 0;  //** Use the initial rp
        }
        free(path);  //** No longer needed.  lio_path_resolve will strdup

        v_size[0] = v_size[1] = -tuple.lc->max_attr; v_size[2] = -tuple.lc->max_attr;
        it = lio_create_object_iter_alist(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE_FLAG, recurse_depth, keys, (void **)vals, v_size, 3);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            goto finished;
        }

        while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
            update_warm_prep_db(lio_ifd, wdb, fname, vals, v_size);
            free(fname);
        }

        lio_destroy_object_iter(lio_gc, it);
        if (ftype < 0) {
            fprintf(stderr, "ERROR getting the next object!\n");
            return_code = EIO;
        }

        lio_path_release(&tuple);
        if (rp_single != NULL) {
            lio_os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            lio_os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
    }

finished:
    close_prep_db(wdb);  //** Close the DBs

    tbx_stdinarray_iter_destroy(piter);
    lio_shutdown();

    return(return_code);
}


