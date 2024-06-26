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

#define _log_module_index 200

#include <errno.h>
#include <gop/gop.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/lio.h>
#include <lio/os.h>

#define MAX_SET 1000


//*************************************************************************
// load_file - Loads a file from disk
//*************************************************************************

void load_file(char *fname, char **val, int *v_size)
{
    FILE *fd;
    int i;

    *v_size = 0;
    *val = NULL;

    fd = fopen(fname, "r");
    if (fd == NULL) {
        info_printf(lio_ifd, 0, "ERROR opeing file=%s!  Exiting\n", fname);
        exit(1);
    }
    fseek(fd, 0, SEEK_END);

    i = ftell(fd);
    tbx_type_malloc(*val, char, i+1);
    (*val)[i] = 0;
    *v_size = i;

    fseek(fd, 0, SEEK_SET);
    if (fread(*val, i, 1, fd) != 1) { //**
        info_printf(lio_ifd, 0, "ERROR reading file=%s! Exiting\n", fname);
        exit(1);
    }
    fclose(fd);

}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, rg_mode, start_option, start_index, err, fin, nfailed;
    lio_path_tuple_t tuple;
    lio_os_regex_table_t *rp_single, *ro_single;
    os_object_iter_t *it;
    tbx_stdinarray_iter_t *it_args;
    os_fd_t *fd;  //** This is just used for manipulating symlink attributes
    char *bstate, *path;
    char *key[MAX_SET];
    char *val[MAX_SET];
    int v_size[MAX_SET];
    int n_keys;
    char *dkey[MAX_SET], *tmp;
    char *sobj[MAX_SET], *skey[MAX_SET];
    int n_skeys, return_code;
    int ftype, prefix_len;
    char *fname;

    memset(dkey, 0, sizeof(dkey));
    memset(sobj, 0, sizeof(sobj));
    memset(skey, 0, sizeof(skey));

    char *delims = "=";

    int recurse_depth = 10000;
    int obj_types = OS_OBJECT_ANYFILE_FLAG;
    return_code = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_setattr LIO_COMMON_OPTIONS [-rd recurse_depth] [-t object_types] -as key=value | -ar key | -af key=vfilename | -al key=obj_path/dkey LIO_PATH_OPTIONS\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("\n");
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        lio_print_object_type_options(stdout, obj_types);
        printf("    -delim c           - Key/value delimeter characters.  Defauls is %s.\n", delims);
        printf("    -as key=value      - Breaks up the literal string into the key/value pair and stores it.\n");
        printf("    -ar key            - Remove the key.\n");
        printf("    -af key=vfilename  - Similar to -as but the value is loaded from the given vfilename.\n");
        printf("    -al key=sobj_path/skey - Symlink the key to another objects (sobj_path) key(skey).\n");
        printf("\n");
        printf("       NOTE: Multiple -as/-ar/-af/-al clauses are allowed\n\n");
        return(1);
    }

    lio_init(&argc, &argv);


    //*** Parse the args
    rp_single = ro_single = NULL;
    n_keys = 0;
    n_skeys = 0;

    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-t") == 0) {  //** Object types
            i++;
            obj_types = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-delim") == 0) {  //** Get the delimiter
            i++;
            delims = argv[i];
            i++;
        } else if (strcmp(argv[i], "-as") == 0) {  //** String attribute
            i++;
            key[n_keys] = tbx_stk_string_token(argv[i], delims, &bstate, &fin);
            val[n_keys] = bstate;  //** Everything else is the value
            v_size[n_keys] = strlen(val[n_keys]);
            if (strcmp(val[n_keys], "") == 0) val[n_keys] = NULL;
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-ar") == 0) {  //** Remove the attribute
            i++;
            key[n_keys] = tbx_stk_string_token(argv[i], delims, &bstate, &fin);
            val[n_keys] = NULL;
            v_size[n_keys] = -1;
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-af") == 0) {  //** File attribute
            i++;
            key[n_keys] = tbx_stk_string_token(argv[i], delims, &bstate, &fin);
            load_file(tbx_stk_string_token(NULL, delims, &bstate, &fin), &(val[n_keys]), &(v_size[n_keys]));
            n_keys++;
            i++;
        } else if (strcmp(argv[i], "-al") == 0) {  //** Symlink attributes
            i++;
            dkey[n_skeys] = tbx_stk_string_token(argv[i], delims, &bstate, &fin);
            tmp = tbx_stk_string_token(NULL, delims, &bstate, &fin);
            lio_os_path_split(tmp, &(sobj[n_skeys]), &(skey[n_skeys]));
            n_skeys++;
            i++;
        }
    } while ((start_option - i < 0) && (i<argc));
    start_index = i;

    if (rg_mode == 0) {
        if (start_index >= argc) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_index--;  //** The 1st entry will be the rp created in lio_parse_path_options
    }

    nfailed = 0;
    it_args = tbx_stdinarray_iter_create(argc-start_index, (const char **)(argv+start_index));
    while (1) {
        if (rg_mode == 0) {
            path = tbx_stdinarray_iter_next(it_args);
            if (!path) break;

            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                return_code = EINVAL;
                free(path);
                lio_path_release(&tuple);
                continue;
            }
            free(path);
            rp_single = lio_os_path_glob2regex(tuple.path);
            if (!rp_single) {  //** Got a bad path
                info_printf(lio_ifd, 0, "ERROR: processing path=%s\n", tuple.path);
                lio_path_release(&tuple);
                continue;
            }
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        if (n_keys > 0) {
            err = gop_sync_exec(lio_regex_object_set_multiple_attrs_gop(tuple.lc, tuple.creds, NULL, rp_single,  ro_single, obj_types, recurse_depth, key, (void **)val, v_size, n_keys));
            if (err != OP_STATE_SUCCESS) {
                return_code = EIO;
                info_printf(lio_ifd, 0, "ERROR with operation! \n");
                nfailed++;
            }
        }

        if (n_skeys > 0) {  //** For symlink attrs we have to manually iterate
            it = lio_create_object_iter(tuple.lc, tuple.creds, rp_single, ro_single, obj_types, NULL, recurse_depth, NULL, 0);
            if (it == NULL) {
                info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
                nfailed++;
                return_code = EIO;
                goto finished;
            }

            while ((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) {
                err = gop_sync_exec(os_open_object(tuple.lc->os, tuple.creds, fname, OS_MODE_READ_IMMEDIATE, NULL, &fd, 30));
                if (err != OP_STATE_SUCCESS) {
                    info_printf(lio_ifd, 0, "ERROR: opening file: %s.  Skipping.\n", fname);
                    nfailed++;
                } else {
                    //** Do the symlink
                    err = gop_sync_exec(os_symlink_multiple_attrs(tuple.lc->os, tuple.creds, sobj, skey, fd, dkey, n_skeys));
                    if (err != OP_STATE_SUCCESS) {
                        return_code = EIO;
                        info_printf(lio_ifd, 0, "ERROR: with linking file: %s\n", fname);
                        nfailed++;
                    }

                    //** Close the file
                    err = gop_sync_exec(os_close_object(tuple.lc->os, fd));
                    if (err != OP_STATE_SUCCESS) {
                        return_code = EIO;
                        info_printf(lio_ifd, 0, "ERROR: closing file: %s\n", fname);
                        nfailed++;
                    }
                }

                free(fname);
            }

            lio_destroy_object_iter(tuple.lc, it);

            if (ftype < 0) {
                fprintf(stderr, "ERROR getting the next object!\n");
                return_code = EIO;
                goto finished;
            } 
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
    for (i=0; i<n_skeys; i++) {
        if (sobj[i] != NULL) free(sobj[i]);
        if (skey[i] != NULL) free(skey[i]);
    }

    tbx_stdinarray_iter_destroy(it_args);
    lio_shutdown();

    return(return_code);
}


