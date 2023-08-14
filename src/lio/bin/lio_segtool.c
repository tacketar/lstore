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

#define _log_module_index 208

#include <apr.h>
#include <apr_errno.h>
#include <apr_hash.h>
#include <apr_pools.h>
#include <apr_signal.h>
#include <apr_thread_mutex.h>
#include <apr_time.h>
#include <assert.h>
#include <gop/gop.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <limits.h>
#include <lio/segment.h>
#include <math.h>
#include <signal.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <strings.h>
#include <tbx/assert_result.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>
#include <tbx/stack.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#include <lio/authn.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>

static lio_creds_t *creds;
lio_path_tuple_t *tuple_list;
char **argv_list = NULL;

int shutdown_now = 0;
apr_thread_mutex_t *shutdown_lock;
apr_pool_t *shutdown_mpool;

ex_id_t segment_id = 0;
char *segment_type = NULL;
int recurse_depth = 1000;
int seg_depth = 1000;
int print_mode = 0;
int dryrun = 0;
int timeout = 60;
char *argfile = NULL;
char *args_section = "args";
char *match_section = "match";
tbx_inip_file_t *afd;

typedef struct {
    char *fname;
    lio_config_t *lc;
    int n_matches;
} segtool_task_t;

//*************************************************************************
//  signal_shutdown - QUIT signal handler
//*************************************************************************

void signal_shutdown(int sig)
{
    char date[128];
    apr_ctime(date, apr_time_now());

    log_printf(0, "Shutdown requested on %s\n", date);
    info_printf(lio_ifd, 0, "========================= Shutdown requested on %s ======================\n", date);

    apr_thread_mutex_lock(shutdown_lock);
    shutdown_now = 1;
    apr_thread_mutex_unlock(shutdown_lock);

    return;
}

//*************************************************************************
//  install_signal_handler - Installs the QUIT handler
//*************************************************************************

void install_signal_handler()
{
    //** Make the APR stuff
    assert_result(apr_pool_create(&shutdown_mpool, NULL), APR_SUCCESS);
    apr_thread_mutex_create(&shutdown_lock, APR_THREAD_MUTEX_DEFAULT, shutdown_mpool);

    //***Attach the signal handler for shutdown
    shutdown_now = 0;
    apr_signal_unblock(SIGQUIT);
    apr_signal(SIGQUIT, signal_shutdown);
}

//*************************************************************************
// ini_args_process - Dumps the match and segment args into the args FD
//*************************************************************************

void ini_args_process(int *argc, char ***argv)
{
    char **myargv = *argv;
    int i;
    tbx_stack_t *hints;

    //** Mangle the args as needed
    for (i=0; i<(*argc); i++) {
        if (strncmp(myargv[i], "--args-", 7) == 0) { //** Got a possible match
            if (strcmp(myargv[i], "--args-add") == 0) {
                myargv[i] = "--ini-hint-add";
            } else if (strcmp(myargv[i], "--args-remove") == 0) {
                myargv[i] = "--ini-hint-remove";
            } else if (strcmp(myargv[i], "--args-replace") == 0) {
                myargv[i] = "--ini-hint-replace";
            } else if (strcmp(myargv[i], "--args-default") == 0) {
                myargv[i] = "--ini-hint-default";
            }
        }
    }

    //** Now add them to the args FD
    hints = tbx_stack_new();
    tbx_inip_hint_options_parse(hints, myargv, argc);
    tbx_inip_hint_list_apply(afd, hints);  //** Apply the hints
    tbx_inip_hint_list_destroy(hints);     //** and cleanup
}


//*************************************************************************
//  segtool_task - Does the actual segment check/mod call
//*************************************************************************

gop_op_status_t segtool_task(void *arg, int id)
{
    segtool_task_t *task = (segtool_task_t *)arg;
    lio_fd_t *fd;
    gop_op_status_t status;
    int err;

    task->n_matches = 0;
    err = gop_sync_exec(lio_open_gop(task->lc, creds, task->fname, lio_fopen_flags("r"), NULL, &fd, 60));
    if (err != OP_STATE_SUCCESS) {
        fprintf(stderr, "ERROR: Unable to open file: %s\n", task->fname);
        return(gop_failure_status);
    }

    status = gop_sync_exec_status(lio_segment_tool_gop(fd, segment_id, segment_type, match_section, args_section, afd, dryrun, timeout));
    if (status.op_status == OP_STATE_SUCCESS) task->n_matches = status.error_code;

    gop_sync_exec(lio_close_gop(fd));

    return(status);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, slot, start_option, rg_mode, ftype, prefix_len, err;
    ex_off_t good, bad, submitted, segs_modded, objs_modded;
    void *piter;
    gop_opque_t *q;
    gop_op_generic_t *gop;
    gop_op_status_t status;
    os_object_iter_t *it;
    lio_os_regex_table_t *rp_single, *ro_single;
    lio_path_tuple_t static_tuple, tuple;
    segtool_task_t *w;
    char *path, *fname;

    if (argc < 2) {
        printf("\n");
        printf("lio_segtool LIO_COMMON_OPTIONS [-rd recurse_depth] --segment SID | --stype STYPE\n");
        printf("            --seg-depth N  [--match-section ini_match_section] [--args-section ini_args_section] [--args ini_args_file]\n");
        printf("            [--dry-run] | [--print-match] [--print-mismatch]\n");
        printf("            [ARGS-OPTIONS]\n");
        printf("            LIO_PATH_OPTIONS | -\n");
        lio_print_options(stdout);
        lio_print_path_options(stdout);
        printf("    -rd recurse_depth  - Max recursion depth on directories. Defaults to %d\n", recurse_depth);
        printf("    -                  - If no file is given but a single dash is used the files are taken from stdin\n");
        printf("    --segment SID      - Only target a specific segment with the given SID\n");
        printf("    --stype STYPE      - Make an initial match for all segments with the given segment type\n");
        printf("    --seg-depth        - How many times to apply the args to when recursing through an exnode. The default is %d\n", seg_depth);
        printf("    --match-section ini_match_section - Use the section in the ARGS file for more fine grained selection. Defaults to %s\n", match_section);
        printf("    --args-section ini_args_section   - Use this section in the ARGS file for parameters to modify. Defaults to %s\n", args_section);
        printf("    --args ini_args_file  - INI file containing the args. The default is to construct the INI file from the ARGS-OPTIONS if not specified\n");
        printf("    --dry-run          - Don't actually apply any changes just list files that match\n");
        printf("    --print-match      - Print files that match the selection criteria\n");
        printf("    --print-mismatch   - Just print the files that don't match\n");
        printf("    [ARGS-OPTIONS]     - Same as INI_OPTIONS except the prefix is '--args' vs '--ini-hint'\n");
        printf("                         Extra match and segment options can be passed by storing them in the appropriate INI section.\n");
        printf("\n");
        return(1);
    }

    lio_init(&argc, &argv);
    argv_list = argv;

    err = 0;

    //*** Parse the path args
    rp_single = ro_single = NULL;
    rg_mode = lio_parse_path_options(&argc, argv, lio_gc->auto_translate, &static_tuple, &rp_single, &ro_single);

    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-rd") == 0) { //** Recurse depth
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--seg-depth") == 0) { //** Recurse depth for the segments
            i++;
            seg_depth = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--segment") == 0) { //** Target a specfic segment SID
            i++;
            segment_id = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--dryrun") == 0) { //** Just report which files would be effected but don't actually do anything
            i++;
            dryrun = 1;
        } else if (strcmp(argv[i], "--stype") == 0) { //** Target a specfic segment type
            i++;
            segment_type = argv[i];
            i++;
        } else if (strcmp(argv[i], "--match-section") == 0) { //** INI section to use for the match
            i++;
            match_section = argv[i];
            i++;
        } else if (strcmp(argv[i], "--args-section") == 0) { //** INI section to use for the segment args
            i++;
            args_section = argv[i];
            i++;
        } else if (strcmp(argv[i], "--args") == 0) { //** Args file to use if any
            i++;
            argfile = argv[i];
            i++;
        } else if (strcmp(argv[i], "--print-match") == 0) { //** Print matches
            i++;
            print_mode += 1;
        } else if (strcmp(argv[i], "--print-mismatch") == 0) { //** Print mismatches
            i++;
            print_mode += 2;
        }

    } while ((start_option < i) && (i<argc));
    start_option = i;

    //** Open the INI args file if specified otherwise make a dummy
    if (argfile == NULL) {
        afd = tbx_inip_string_read("\n", 1);
    } else {
        afd = tbx_inip_string_read(argfile, 1);
    }

    if (afd == NULL) {
        fprintf(stderr, "ERROR: Problem opening arg file: %s\n", argfile);
        err = 1;
        goto finished;
    }

    //**Parse the match and segment args
    ini_args_process(&argc, &argv);

    if (rg_mode == 0) {
        if (argc <= start_option) {
            info_printf(lio_ifd, 0, "Missing directory!\n");
            return(2);
        }
    } else {
        start_option--;  //** Ther 1st entry will be the rp created in lio_parse_path_options
    }

    //** Make the path iterator
    piter = tbx_stdinarray_iter_create(argc-start_option, (const char **)&(argv[start_option]));

    q = gop_opque_new();
    opque_start_execution(q);

    slot = 0;
    submitted = good = bad = segs_modded = objs_modded = 0;
    tbx_type_malloc_clear(w, segtool_task_t, lio_parallel_task_count);

    install_signal_handler();

    apr_thread_mutex_lock(shutdown_lock);
    while (((path = tbx_stdinarray_iter_next(piter)) != NULL) && (shutdown_now == 0)) {
        apr_thread_mutex_unlock(shutdown_lock);
        log_printf(5, "path=%s argc=%d rg_mode=%d\n", path, argc, rg_mode);

        if (rg_mode == 0) {
            //** Create the simple path iterator
            tuple = lio_path_resolve(lio_gc->auto_translate, path);
            if (tuple.is_lio < 0) {
                fprintf(stderr, "Unable to parse path: %s\n", path);
                err = EINVAL;
                free(path);
                lio_path_release(&tuple);
                continue;
            }
            lio_path_wildcard_auto_append(&tuple);
            rp_single = lio_os_path_glob2regex(tuple.path);
            free(path);
            if (!rp_single) {  //** Got a bad path
                info_printf(lio_ifd, 0, "ERROR: processing path=%s\n", tuple.path);
                lio_path_release(&tuple);
                continue;
            }
        } else {
            rg_mode = 0;  //** Use the initial rp
        }

        creds = tuple.lc->creds;

        it = lio_create_object_iter(tuple.lc, tuple.creds, rp_single, ro_single, OS_OBJECT_FILE_FLAG, NULL, recurse_depth, NULL, 0);
        if (it == NULL) {
            info_printf(lio_ifd, 0, "ERROR: Failed with object_iter creation\n");
            err = EIO;
            goto finished;
        }

        apr_thread_mutex_lock(shutdown_lock);
        while (((ftype = lio_next_object(tuple.lc, it, &fname, &prefix_len)) > 0) && (shutdown_now == 0)) {
            apr_thread_mutex_unlock(shutdown_lock);

            submitted++;
            w[slot].fname = fname;
            w[slot].lc = tuple.lc;
            w[slot].n_matches = 0;
            gop = gop_tp_op_new(lio_gc->tpc_unlimited, NULL, segtool_task, (void *)&(w[slot]), NULL, 1);;
            gop_set_myid(gop, slot);
            log_printf(0, "gid=%d i=%d fname=%s\n", gop_id(gop), slot, fname);
            gop_opque_add(q, gop);

            if (submitted >= lio_parallel_task_count) {
                gop = opque_waitany(q);
                status = gop_get_status(gop);
                slot = gop_get_myid(gop);
                if (status.op_status == OP_STATE_SUCCESS) {
                    good++;
                    segs_modded += w[slot].n_matches;
                    if (w[slot].n_matches > 0) objs_modded++;
                    if ((print_mode & 1) && (w[slot].n_matches > 0)) {
                       info_printf(lio_ifd, 0, "SUCCESS: n_match=%d %s\n", w[slot].n_matches, w[slot].fname);
                    } else if ((print_mode & 2) && (w[slot].n_matches == 0)) {
                       info_printf(lio_ifd, 0, "SUCCESS: n_match=%d %s\n", w[slot].n_matches, w[slot].fname);
                    }
                } else {
                    bad++;
                    if (print_mode & 2) {
                       info_printf(lio_ifd, 1, "FAILURE: %s\n", w[slot].fname);
                    }
                }

                if (w[slot].fname) { free(w[slot].fname); w[slot].fname = NULL; }
                gop_free(gop, OP_DESTROY);
            } else {
                slot++;
            }

            apr_thread_mutex_lock(shutdown_lock);
        }
        apr_thread_mutex_unlock(shutdown_lock);

        lio_destroy_object_iter(tuple.lc, it);
        if (ftype < 0) {
            fprintf(stderr, "ERROR getting the next object!\n");
            err = EIO;
        }
        if (rp_single != NULL) {
            lio_os_regex_table_destroy(rp_single);
            rp_single = NULL;
        }
        if (ro_single != NULL) {
            lio_os_regex_table_destroy(ro_single);
            ro_single = NULL;
        }
        lio_path_release(&tuple);
    }
    apr_thread_mutex_unlock(shutdown_lock);

    //** Wait for everything to complete
    while ((gop = opque_waitany(q)) != NULL) {
        status = gop_get_status(gop);
        slot = gop_get_myid(gop);
        if (status.op_status == OP_STATE_SUCCESS) {
            good++;
            segs_modded += w[slot].n_matches;
            if (w[slot].n_matches > 0) objs_modded++;
            if ((print_mode & 1) && (w[slot].n_matches > 0)) {
                info_printf(lio_ifd, 0, "SUCCESS: n_match=%d %s\n", w[slot].n_matches, w[slot].fname);
            } else if ((print_mode & 2) && (w[slot].n_matches == 0)) {
                info_printf(lio_ifd, 0, "SUCCESS: n_match=%d %s\n", w[slot].n_matches, w[slot].fname);
            }
        } else {
            bad++;
            if (print_mode & 2) {
                info_printf(lio_ifd, 1, "FAILURE: %s\n", w[slot].fname);
            }
        }

        if (w[slot].fname) { free(w[slot].fname); w[slot].fname = NULL; }
        gop_free(gop, OP_DESTROY);
    }


    gop_opque_free(q, OP_DESTROY);

    apr_thread_mutex_destroy(shutdown_lock);
    apr_pool_destroy(shutdown_mpool);

    info_printf(lio_ifd, 0, "--------------------------------------------------------------------\n");
    info_printf(lio_ifd, 0, "Submitted: " XOT "   Success: " XOT "   Fail: " XOT "   Objects modified: " XOT "   Segments modified: " XOT "\n", submitted, good, bad, objs_modded, segs_modded);

    if (submitted != (good+bad)) {
        info_printf(lio_ifd, 0, "ERROR FAILED self-consistency check! Submitted != Success+Fail\n");
        err = EFAULT;
    }
    if (bad > 0) {
        info_printf(lio_ifd, 0, "ERROR Some files failed inspection!\n");
        err = EIO;
    }

    free(w);

    tbx_stdinarray_iter_destroy(piter);

finished:
    lio_shutdown();

    return(err);
}
