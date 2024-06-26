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
//#include <apr_thread_pool.h>
#include <gop/tp.h>
#include <gop/gop.h>
//#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/list.h>
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <time.h>
#include "os.h"

int wait_time = 20;

#define PATH_LEN 4096

// *************************************************************************
//  Object service test program
// *************************************************************************


// **********************************************************************************
int qcompare(const void *s1, const void *s2)
{
    return strcmp(* (char * const *) s1, * (char * const *) s2);
}


// **********************************************************************************
// path_scan_and_check - Does a regex scan of the given path and does a comparison
//    of the results
// **********************************************************************************

int path_scan_and_check(char *path, char **match, int n_match, int recurse_depth, int object_types)
{
    os_object_iter_t *it;
    lio_os_regex_table_t *regex;
    int err, size_err, i, n, n_max, j, plen;
    char **name;

    regex = lio_os_path_glob2regex(path);
    it = os_create_object_iter(lio_gc->os, lio_gc->creds, regex, NULL, object_types, NULL, recurse_depth, NULL, 0);
    if (it == NULL) {
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", path);
        return(1);
    }

    n_max = n_match + 1;
    tbx_type_malloc_clear(name, char *, n_max);
    n = 0;
    while (os_next_object(lio_gc->os, it, &(name[n]), &plen) > 0) {
        n++;
        if (n>=n_max) {
            n_max = n_max + 5;
            tbx_type_realloc(name, char *, n_max);
        }
    }
    os_destroy_object_iter(lio_gc->os, it);
    lio_os_regex_table_destroy(regex);


    // ** Sort both lists and do the comparison
    qsort(match, n_match, sizeof(char *), qcompare);
    qsort(name, n, sizeof(char *), qcompare);

    size_err = 0;
    if (n != n_match) {
        size_err = -1;
        log_printf(0, "ERROR: Sizes don't match n_match=%d found=%d\n", n_match, n);
    }
    j = (n>n_match) ? n_match : n;

    err = 0;
    for (i=0; i<j; i++) {
        if (strcmp(match[i], name[i]) != 0) {
            err = i;
            break;
        }
    }

    if ((err != 0) || (size_err != 0)) {
        log_printf(0,"-------------Initial slot mismatch=%d (-1 if sizes don't match)-------------------------------\n", err);
        for (i=0; i<n_match; i++) log_printf(0, "match[%d]=%s\n", i, match[i]);
        for (i=0; i<n; i++) log_printf(0, "found[%d]=%s\n", i, name[i]);
        log_printf(0,"----------------------------------------------------------------------------------------------\n");
    }

    for (i=0; i<n; i++) free(name[i]);
    free(name);

    if (size_err != 0) err = size_err;
    return(err);
}


// **********************************************************************************
//  os_create_remove_tests - Just does object tests: create, remove, recurse
// **********************************************************************************

int os_create_remove_tests(char *prefix)
{
    lio_object_service_fn_t *os = lio_gc->os;
    lio_creds_t  *creds = lio_gc->creds;
    int err, i;
    char foo_path[PATH_LEN];
    char bar_path[PATH_LEN];
    char hfoo_path[PATH_LEN];
    char *match_path[10];
    os_fd_t *foo_fd;
    lio_os_regex_table_t *regex;
    int nfailed = 0;

    for (i=0; i<10; i++) {
        tbx_type_malloc_clear(match_path[i], char, PATH_LEN);
    }

    // ** Create FILE foo
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Rename it
    snprintf(bar_path, PATH_LEN, "%s/bar", prefix);
    err = gop_sync_exec(os_move_object(os, creds, foo_path, bar_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: moving file src: %s  dest: %s err=%d\n", foo_path, bar_path, err);
        return(nfailed);
    }


    // ** Verify foo is gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  file existst after rename: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify it's there under the new name bar
    err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }

    // ** Make a softlink foo->bar
    err = gop_sync_exec(os_symlink_object(os, creds, bar_path, foo_path, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: linking file src: %s  dest: %s err=%d\n", bar_path, foo_path, err);
        return(nfailed);
    }

    // ** Verify it exists
    err = gop_sync_exec(os_exists(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost bar! %s\n", bar_path);
        return(nfailed);
    }

    // ** hardlink hfoo->foo
    snprintf(hfoo_path, PATH_LEN, "%s/hfoo", prefix);
    err = gop_sync_exec(os_hardlink_object(os, creds, foo_path, hfoo_path, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: hard linking file src: %s  dest: %s err=%d\n", foo_path, hfoo_path, err);
        return(nfailed);
    }

    // ** Verify they all exist
    err = gop_sync_exec(os_exists(os, creds, hfoo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
        return(nfailed);
    }
    err = gop_sync_exec(os_exists(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", foo_path);
        return(nfailed);
    }
    err = gop_sync_exec(os_exists(os, creds, bar_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", bar_path);
        return(nfailed);
    }

//abort();

    // ** Remove bar
    err = gop_sync_exec(os_remove_object(os, creds, bar_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: removing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify the state
    err = gop_sync_exec(os_exists(os, creds, hfoo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
        return(nfailed);
    }
    err = gop_sync_exec(os_exists(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", foo_path);
        return(nfailed);
    }
    err = gop_sync_exec(os_exists(os, creds, bar_path));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Still have %s\n", bar_path);
        return(nfailed);
    }

//abort();

    // ** Remove foo
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: removing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** And verify the state
    err = gop_sync_exec(os_exists(os, creds, hfoo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Lost %s\n", hfoo_path);
        return(nfailed);
    }
    err = gop_sync_exec(os_exists(os, creds, foo_path));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Still have %s\n", foo_path);
        return(nfailed);
    }

//abort();

    // ** Remove hfoo
    err = gop_sync_exec(os_remove_object(os, creds, hfoo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: removing file: %s err=%d\n", hfoo_path, err);
        return(nfailed);
    }

    // ** MAke sure it's gone
    err = gop_sync_exec(os_exists(os, creds, hfoo_path));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Still have %s\n", hfoo_path);
        return(nfailed);
    }

//abort();


    // ** Create DIRECTORY foodir
    snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create foodir/foo
    snprintf(foo_path, PATH_LEN, "%s/foodir/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create DIRECTORY foodir/bardir
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bardir exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create foodir/bardir/foobar
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bardir/foobar exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create foodir/bardir/bar
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/bar", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bardir/bar exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Do a regex scan for foo*/bar*/foo*
    // ** This should return foodir/bardir/foobar
    snprintf(foo_path, PATH_LEN, "%s/foo*/bar*/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo*/bar*/foo* err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo* with recursion
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    err = path_scan_and_check(foo_path, match_path, 3, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo* err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foodir/bardir/foo* with recursion
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foodir/bardir/foo* err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo*/bardir/foo* with recursion
    snprintf(foo_path, PATH_LEN, "%s/foo*/bardir/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo*/bardir/foo* err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo*/bardir/foo* with recursion
    snprintf(foo_path, PATH_LEN, "%s/foo*/bardir/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo*/bardir/foo* err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo*/bar*/foobar with recursion
    snprintf(foo_path, PATH_LEN, "%s/foo*/bar*/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    err = path_scan_and_check(foo_path, match_path, 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo*/bar*/foo* err=%d\n", err);
        return(nfailed);
    }


    // ** Create DIRECTORY foodir/bardir/subdir
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bardir/subdir exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }


    // ** Create foodir/bardir/subdir/last
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bardir/subdir/last exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

//abort();

    // ** Create DIRECTORY LINK foodir/linkdir -> fooddir/bardir/subdir
    snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir", prefix);
    snprintf(bar_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
    err = gop_sync_exec(os_symlink_object(os, creds, bar_path, foo_path, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating link dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/linkdir exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Do a regex scan for foo* with recursion and following symlinks
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    snprintf(match_path[3], PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
    snprintf(match_path[4], PATH_LEN, "%s/foodir/linkdir/last", prefix);
    err = path_scan_and_check(foo_path, match_path, 5, 1000, OS_OBJECT_FILE_FLAG|OS_OBJECT_FOLLOW_SYMLINK_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo* following symlinks err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo* with recursion and skipping symlinks
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    snprintf(match_path[3], PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
    err = path_scan_and_check(foo_path, match_path, 4, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo* skipping symlinks err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foodir/linkdir with recursion
    snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir/*", prefix);
    err = path_scan_and_check(foo_path, &(match_path[4]), 1, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foodir/linkdir err=%d\n", err);
        return(nfailed);
    }

    // ** Make a symlink loop  foodir/loop -> fooddir
    snprintf(foo_path, PATH_LEN, "%s/foodir/loop", prefix);
    snprintf(bar_path, PATH_LEN, "..");
    err = gop_sync_exec(os_symlink_object(os, creds, bar_path, foo_path, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating loop dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/loop exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }


    // ** Do a regex scan for foo* with recursion and skipping symlinks
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    snprintf(match_path[3], PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
    err = path_scan_and_check(foo_path, match_path, 4, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo* skipping symlinks with a loop err=%d\n", err);
        return(nfailed);
    }

    // ** Do a regex scan for foo* with recursion and FOLLOWING symlinks
    // ** The OS looping logic should kick us out of the infinite loop
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/foobar", prefix);
    snprintf(match_path[2], PATH_LEN, "%s/foodir/bardir/subdir/last", prefix);
    snprintf(match_path[3], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[4], PATH_LEN, "%s/foodir/linkdir/last", prefix);
    snprintf(match_path[5], PATH_LEN, "%s/foodir/loop/foodir/bardir/bar", prefix);
    snprintf(match_path[6], PATH_LEN, "%s/foodir/loop/foodir/bardir/foobar", prefix);
    snprintf(match_path[7], PATH_LEN, "%s/foodir/loop/foodir/bardir/subdir/last", prefix);
    snprintf(match_path[8], PATH_LEN, "%s/foodir/loop/foodir/foo", prefix);
    err = path_scan_and_check(foo_path, match_path, 9, 1000, OS_OBJECT_FILE_FLAG|OS_OBJECT_FOLLOW_SYMLINK_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo* FOLLOWING symlinks with a loop err=%d\n", err);
        return(nfailed);
    }

    // ** Remove foodir/loop
    snprintf(foo_path, PATH_LEN, "%s/foodir/loop", prefix);
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: filed removing loop link directory: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/loop was removed
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! opened a removed dir dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Remove foodir/linkdir
    snprintf(foo_path, PATH_LEN, "%s/foodir/linkdir", prefix);
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: filed removing link directory: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/linkdir was removed
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops! opened a removed dir dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Try removing foodir/bardir/subdir.  This should fail since it has files in it
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  I could rmdir a non-empty directory: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Do a rm -r foodir/bardir/subdir
    snprintf(foo_path, PATH_LEN, "%s/foodir/bardir/subdir", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY_FLAG, 1000));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
        return(nfailed);
    }
    lio_os_regex_table_destroy(regex);

    // ** Verify foodir/bardir/subdir is gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Do an rm -r foo*/bar*/foo*
    snprintf(foo_path, PATH_LEN, "%s/foo*/bar*/foo*", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY_FLAG, 1000));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
        return(nfailed);
    }
    lio_os_regex_table_destroy(regex);

    // ** Verify that foodir/foo, foodir/bardir/bar still exist
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/foodir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/foodir/bardir/bar", prefix);
    err = path_scan_and_check(foo_path, match_path, 2, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: foo*/bar*/foo* err=%d\n", err);
        return(nfailed);
    }


    // ** Rename it foodir->bardir
    snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
    snprintf(bar_path, PATH_LEN, "%s/bardir", prefix);
    err = gop_sync_exec(os_move_object(os, creds, foo_path, bar_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: moving file src: %s  dest: %s err=%d\n", foo_path, bar_path, err);
        return(nfailed);
    }


    // ** Verify foodir is gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  file existst after rename: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verift it's there under the new name bar
    // ** Verify that foodir/foo, foodir/bardir/bar still exist
    snprintf(foo_path, PATH_LEN, "%s/bardir/*", prefix);
    snprintf(match_path[0], PATH_LEN, "%s/bardir/foo", prefix);
    snprintf(match_path[1], PATH_LEN, "%s/bardir/bardir/bar", prefix);
    err = path_scan_and_check(foo_path, match_path, 2, 1000, OS_OBJECT_FILE_FLAG);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Regex scan: bardir err=%d\n", err);
        return(nfailed);
    }
    err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }


    // ** Remove everything else rm -r bardir
    snprintf(foo_path, PATH_LEN, "%s/bardir", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY_FLAG, 1000));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    lio_os_regex_table_destroy(regex);

    for (i=0; i<10; i++) {
        free(match_path[i]);
    }

    log_printf(0, "PASSED!\n");

    return(nfailed);
}

// **********************************************************************************
//  attr_check - Checks that the regex key/val mataches
// **********************************************************************************

int attr_check(os_attr_iter_t *it, char **key, char **val, int *v_size, int n)
{
    lio_object_service_fn_t *os = lio_gc->os;
    char *ikey, *ival;
    int hits[n];
    int i, i_size, err;

    for (i=0; i<n; i++) hits[i] = 0;

    err = 0;
    while (os_next_attr(os, it, &ikey, (void **)&ival, &i_size) == 0) {
        for (i=0; i<n; i++) {
            if (strcmp(key[i], ikey) == 0) {
                hits[i] = 1;
                if (strcmp(val[i], ival) != 0) {
                    err += 1;
                    hits[i] = -1;
                    log_printf(0, "ERROR bad value key=%s val=%s should be mval=%s\n", ikey, ival, val[i]);
                }
                break;
            }
        }

        free(ikey);
        free(ival);
        ikey = ival = NULL;
    }

    for (i=0; i<n; i++) {
        if (hits[i] == 0) {
            err += 1;
            log_printf(0, "ERROR missed key=%s\n", key[i]);
        }
    }

    return(err);
}


// **********************************************************************************
//  os_attribute_tests - Tests attribute manipulation
// **********************************************************************************

int os_attribute_tests(char *prefix)
{
    lio_object_service_fn_t *os = lio_gc->os;
    lio_creds_t  *creds = lio_gc->creds;
    char foo_path[PATH_LEN];
    char bar_path[PATH_LEN];
    char root_path[PATH_LEN];
    char fpath[PATH_LEN];
    lio_os_regex_table_t *regex, *attr_regex, *obj_regex;
    os_fd_t *foo_fd, *bar_fd, *root_fd;
    os_attr_iter_t *it;
    os_object_iter_t *oit;
    char *key, *val, *rval, *fname;
    char **mkey, **mval, **mrval;
    char *match[10];
    char *path[10];
    int i, err, v_size, n, nstart, j, plen;
    int m_size[10];
    int64_t dt, now, got;
    int nfailed = 0;

    tbx_type_malloc(mkey, char *, 10);
    tbx_type_malloc(mval, char *, 10);
    tbx_type_malloc(mrval, char *, 10);
    for (i=0; i<10; i++) {
        tbx_type_malloc_clear(match[i], char, PATH_LEN);
    }

    // ** Create FILE foo
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    now = time(0);

    // ** Verify foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }


    //** get_attr(os.create) for foo
    v_size = -1000;
    key = "os.create";
    rval=NULL;
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    sscanf((char *)rval, I64T, &got);
    dt = got - now;
    if (labs(dt) > 5) {
        nfailed++;
        log_printf(0, "ERROR: attributre timestamp is off attr=%s dt=" XOT " should be less than 5\n", key, dt);
        return(nfailed);
    }
    free(rval);

    // ** Create FILE LINK bar->foo
    snprintf(bar_path, PATH_LEN, "%s/bar", prefix);
    err = gop_sync_exec(os_symlink_object(os, creds, foo_path, bar_path, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify bar exists
    err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &bar_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }

    //** get_attr(type) for foo
    v_size = -1000;
    key = "os.type";
    rval=NULL;
    val = "1";
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }
    free(rval);

    //** get_attr(type) bar
    v_size = -1000;
    val = "5";
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }
    free(rval);

    //** get_attr(link) for foo
    v_size = -1000;
    key = "os.link";
    rval=NULL;
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (v_size != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=NULL got=%s\n", key, rval);
        return(nfailed);
    }
    free(rval);

    //** get_attr(link) bar
    v_size = -1000;
    val = foo_path;
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }
    free(rval);

    //** Check out the timestamps
    //** make /bar{user.timestamp}
    v_size = -1000;
    key = "os.timestamp.user.timestamp";
    val="my_ts";
    err = gop_sync_exec(os_set_attr(os, creds, bar_fd, key, val, strlen(val)));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    //** Read it back through the VA
    v_size = -1000;
    rval=NULL;
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (v_size > 0) {
        if (strstr(rval, val) == NULL) {
            nfailed++;
            log_printf(0, "ERROR: Cant find my tag in key=%s timestamp=%s tag=%s err=%d\n", key, rval, val, err);
        }
        free(rval);
    } else {
        nfailed++;
        log_printf(0, "ERROR: val missing\n");
        return(nfailed);
    }

    //** And directly
    v_size = -1000;
    rval=NULL;
    key = "user.timestamp";
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (v_size > 0) {
        if (strstr(rval, val) == NULL) {
            nfailed++;
            log_printf(0, "ERROR: Cant find my tag in key=%s timestamp=%s tag=%s err=%d\n", key, rval, val, err);
        }
        free(rval);
    } else {
        nfailed++;
        log_printf(0, "ERROR: val missing\n");
        return(nfailed);
    }

    //** Just get the time
    v_size = -1000;
    rval=NULL;
    key = "os.timestamp";
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (v_size <= 0) {
        nfailed++;
        log_printf(0, "ERROR: val missing\n");
        return(nfailed);
    }
    free(rval);

    //** Make an attribute for root/prefix "/"
    snprintf(root_path, PATH_LEN, "%s", prefix);
    err = gop_sync_exec(os_open_object(os, creds, root_path, OS_MODE_READ_IMMEDIATE, "me", &root_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", root_path, err);
        return(nfailed);
    }

    // ** set_attr(foo1=foo1) root
    v_size = -1000;
    key = "user.foo1";
    val="foo1";
    err = gop_sync_exec(os_set_attr(os, creds, root_fd, key, val, strlen(val)));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    // ** get_attr(user.foo1) for root
    v_size = -1000;
    key = "user.foo1";
    rval=NULL;
    err = gop_sync_exec(os_get_attr(os, creds, root_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=foo1 got=%s\n", key, val);
        return(nfailed);
    }
    free(rval);

    //** Now softlink /bar{user.link_root} -> /{foo1}
    //** Now do a link between attributes
    mkey[0] = "user.foo1";
    mval[0] = "user.link_root";
    err = gop_sync_exec(os_symlink_attr(os, creds, root_path, mkey[0], bar_fd, mval[0]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: with attr link: err=%d\n", err);
        return(nfailed);
    }

    //** Verify it worked
    key = "user.link_root";
    v_size = -1000;
    err = gop_sync_exec(os_get_attr(os, creds, bar_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attrs err=%d\n", err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }

//abort();

    free(rval);

    //** Now remove the foo1 attribute for root
    v_size = -1;
    key = "user.foo1";
    val=NULL;
    err = gop_sync_exec(os_set_attr(os, creds, root_fd, key, val, v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    //** Close root
    err = gop_sync_exec(os_close_object(os, root_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", root_path, err);
        return(nfailed);
    }


    //** Close bar
    err = gop_sync_exec(os_close_object(os, bar_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    //** And remove it
    err = gop_sync_exec(os_remove_object(os, creds, bar_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: removing file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }


    // ** set_attr(foo1=foo1);
    v_size = -1000;
    key = "user.foo1";
    val="foo1";
    log_printf(15, "PTR1 before val=%s sizeof(void)=" ST " sizeof(void *)=" ST " sizeof(void**)=" ST "\n", val, sizeof(void), sizeof(void *), sizeof(void **));
    log_printf(15, "PTR1 before val=%s sizeof(char)=" ST " sizeof(char *)=" ST " sizeof(char**)=" ST "\n", val, sizeof(char), sizeof(char *), sizeof(char **));
    err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
    log_printf(15, "PTR1 after val=%s\n", val);
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    // ** get_attr(foo1);
    v_size = -1000;
    key = "user.foo1";
    rval=NULL;
    log_printf(15, "PTR1 after2 val=%s\n", val);
    log_printf(15, "PTR rval=%p *rval=%s\n", &rval, rval);
    tbx_log_flush();
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    log_printf(15, "PTR rval=%p *rval=%s v_size=%d\n", &rval, rval, v_size);
    tbx_log_flush();
    log_printf(15, "PTR1 after3 val=%s\n", val);
    tbx_log_flush();
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=foo1 got=%s\n", key, val);
        return(nfailed);
    }
    free(rval);

    // ** set_attr(foo2=foo2);
    key = "user.foo2";
    val="foo2";
    err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    // ** get_attr(foo2);
    v_size = -1000;
    rval = NULL;
    log_printf(15, "CHECK val=%s\n", val);
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    log_printf(15, "CHECK2 val=%s\n", val);
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }
    free(rval);

    // ** set_mult_attr(foo1=FOO1,foo3=foo3, bar1=bar1);
    mkey[0] = "user.foo1";
    mval[0]="FOO1";
    m_size[0] = strlen(mval[0]);
    mkey[1] = "user.foo3";
    mval[1]="foo3";
    m_size[1] = strlen(mval[1]);
    mkey[2] = "user.bar1";
    mval[2]="bar1";
    m_size[2] = strlen(mval[2]);
    err = gop_sync_exec(os_set_multiple_attrs(os, creds, foo_fd, mkey, (void **)mval, m_size, 3));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting multple err=%d\n", err);
        return(nfailed);
    }


    // ** get_mult_attr(foo1,foo3,bar1);
    m_size[0] = m_size[1] = m_size[2] = -1000;
    mrval[0] = mrval[1] = mrval[2] = NULL;
    err = gop_sync_exec(os_get_multiple_attrs(os, creds, foo_fd, mkey, (void **)mrval, m_size, 3));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
        return(nfailed);
    }
    for (i=0; i<3; i++) {
        if (strcmp(mval[i], mrval[i]) != 0) {
            nfailed++;
            log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
            return(nfailed);
        }

        free(mrval[i]);
    }

    // ** get_attr(*);
    mkey[0] = "user.foo1";
    mval[0]="FOO1";
    m_size[0] = strlen(mval[0]);
    mkey[1] = "user.foo2";
    mval[1]="foo2";
    m_size[1] = strlen(mval[1]);
    mkey[2] = "user.foo3";
    mval[2]="foo3";
    m_size[2] = strlen(mval[2]);
    mkey[3] = "user.bar1";
    mval[3]="bar1";
    m_size[3] = strlen(mval[3]);
    regex = lio_os_path_glob2regex("user.*");
    it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
    err = attr_check(it, mkey, mval, m_size, 4);
    lio_os_regex_table_destroy(regex);
    os_destroy_attr_iter(os, it);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Scanning attr regex!\n");
        return(nfailed);
    }

    // ** get_attr(foo*);
    regex = lio_os_path_glob2regex("user.foo*");
    it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
    err = attr_check(it, mkey, mval, m_size, 3);
    lio_os_regex_table_destroy(regex);
    os_destroy_attr_iter(os, it);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Scanning attr regex!\n");
        return(nfailed);
    }

    // ** set_mult_attr(foo1="",foo2="");
    mkey[0] = "user.foo1";
    mval[0]=NULL;
    m_size[0] = -1;
    mkey[1] = "user.foo2";
    mval[1]=NULL;
    m_size[1] = -1;
    err = gop_sync_exec(os_set_multiple_attrs(os, creds, foo_fd, mkey, (void **)mval, m_size, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting multple err=%d\n", err);
        return(nfailed);
    }


    // ** get_attr(foo*).. should just get foo3=foo3 back
    mkey[0] = "user.foo3";
    mval[0]="foo3";
    m_size[0] = strlen(mval[0]);
    regex = lio_os_path_glob2regex("user.*");
    it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
    err = attr_check(it, mkey, mval, m_size, 1);
    lio_os_regex_table_destroy(regex);
    os_destroy_attr_iter(os, it);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Scanning attr regex!\n");
        return(nfailed);
    }

    // ** set_attr(foo2=foo2);  (We're getting ready to do a multi attr rename)
    key = "user.foo2";
    val="foo2";
    err = gop_sync_exec(os_set_attr(os, creds, foo_fd, key, val, strlen(val)));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: setting attr=%s err=%d\n", key, err);
        return(nfailed);
    }

    // ** get_attr(foo2);
    v_size = -1000;
    rval = NULL;
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&rval, &v_size));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(nfailed);
    }
    if (strcmp(val, rval) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", key, val, rval);
        return(nfailed);
    }
    free(rval);

    // ** Now rename foo2->bar2 and foo3->bar3
    mkey[0] = "user.foo2";
    mval[0] = "user.bar2";
    mkey[1] = "user.foo3";
    mval[1] = "user.bar3";
    err = gop_sync_exec(os_move_multiple_attrs(os, creds, foo_fd, mkey, mval, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting renaming mult err=%d\n", err);
        return(nfailed);
    }


    // ** Verify it worked
    mkey[0] = "user.bar2";
    mval[0]="foo2";
    m_size[0] = strlen(mval[0]);
    mkey[1] = "user.bar3";
    mval[1]="foo3";
    m_size[1] = strlen(mval[1]);
    regex = lio_os_path_glob2regex("user.*");
    it = os_create_attr_iter(os, creds, foo_fd, regex, -1000);
    err = attr_check(it, mkey, mval, m_size, 2);
    lio_os_regex_table_destroy(regex);
    os_destroy_attr_iter(os, it);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: Scanning attr regex!\n");
        return(nfailed);
    }


    // ** Close foo
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }


    // ** Now we need to set up a recursive mult attribute write
    // ** Right now we have foo.  But we need to also do a subdir to verify recursion.
    // ** So we need to add foodir/foo as well.
    // ** Create DIRECTORY foodir
    snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_DIR_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create foodir/foo
    snprintf(foo_path, PATH_LEN, "%s/foodir/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Create foodir/bar
    snprintf(foo_path, PATH_LEN, "%s/foodir/bar", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foodir/bar exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Now do the recursive attr set
    log_printf(0, "Starting recursive attribute set\n");
    mkey[0] = "user.foo1";
    mval[0] = "FOO1";
    m_size[0] = 4;
    mkey[1] = "user.bar2";
    mval[1] = "BAR2";
    m_size[1] = 4;
    snprintf(foo_path, PATH_LEN, "%s/foo*", prefix);
    obj_regex = lio_os_path_glob2regex("foo*");
    regex = lio_os_path_glob2regex(foo_path);
    err = gop_sync_exec(os_regex_object_set_multiple_attrs(os, creds, "me", regex, obj_regex, OS_OBJECT_FILE_FLAG, 1000, mkey, (void **)mval, m_size, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR:with recurse attr set: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    lio_os_regex_table_destroy(regex);
    log_printf(0, "Finished recursive attribute set\n");


    //** Verify it took
    snprintf(foo_path, PATH_LEN, "%s/*", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    attr_regex = lio_os_regex2table("^user\\.((foo1)|(bar2))$");
    oit = os_create_object_iter(lio_gc->os, lio_gc->creds, regex, NULL, OS_OBJECT_FILE_FLAG, attr_regex, 1000, &it, -1000);
    if (oit == NULL) {
        nfailed++;
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", foo_path);
        return(nfailed);
    }

    n = 0;
    while (os_next_object(lio_gc->os, oit, &fname, &plen) > 0) {
        //** Check which file it is foo or foodir/foo or foodir/bar(which should have been skipped)
        nstart = n;
        j = 0;
        snprintf(fpath, PATH_LEN, "%s/foo", prefix);
        if (strcmp(fpath, fname) == 0) n = n | 1;
        snprintf(fpath, PATH_LEN, "%s/foodir/foo", prefix);
        if (strcmp(fpath, fname) == 0) n = n | 2;
        snprintf(fpath, PATH_LEN, "%s/foodir/bar", prefix);
        if (strcmp(fpath, fname) == 0) {
            n = n | 4;
            j = 1;
        }

        if (n == nstart) {
            nfailed++;
            log_printf(0, "ERROR:  EXtra file!  fname=%s\n", fname);
            return(nfailed);
        }

        v_size = -1000;
        m_size[0] = m_size[1] = 0;
        while (os_next_attr(os, it, &key, (void **)&val, &v_size) == 0) {
            if (j == 0) {
                if (strcmp(key, "user.foo1") == 0) {
                    m_size[0] = 1;
                    if (strcmp(val, mval[0]) != 0) {
                        nfailed++;
                        log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, key, val, mval[0]);
                        return(nfailed);
                    }
                } else if (strcmp(key, "user.bar2") == 0) {
                    m_size[1] = 1;
                    if (strcmp(val, mval[1]) != 0) {
                        nfailed++;
                        log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, key, val, mval[1]);
                        return(nfailed);
                    }
                }
            } else {
                if ((strcmp(key, "user.foo1") == 0) || (strcmp(key, "user.bar2") == 0)) {
                    nfailed++;
                    log_printf(0, "ERROR Oops!  fname=%s has key=%s when it shouldnt\n", fname, key);
                    return(nfailed);
                }
            }

            free(key);
            free(val);
            v_size = -1000;
        }

        if (((m_size[0] + m_size[1]) != 2) && (j == 0)) {
            nfailed++;
            log_printf(0, "ERROR fname=%s missed an attr hit[0]=%d hit[1]=%d\n", fname, m_size[0], m_size[1]);
            return(nfailed);
        }

        free(fname);
    }
    os_destroy_object_iter(os, oit);
    lio_os_regex_table_destroy(regex);
    lio_os_regex_table_destroy(obj_regex);
    lio_os_regex_table_destroy(attr_regex);

    if (n != 7) {
        nfailed++;
        log_printf(0, "ERROR missed a file n=%d\n", n);
        return(nfailed);
    }

    //** Do the same thing but with a fixed attr list
    snprintf(foo_path, PATH_LEN, "%s/*", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    obj_regex = lio_os_path_glob2regex("foo*");
//         os_create_object_iter_alist(os, c, path, obj_regex, otypes, depth, key, val, v_size, n_keys)
    mrval[0] = mrval[1] = NULL;
    m_size[0] = m_size[1] = -1000;
    log_printf(0, "Doing iter_alist check\n");
    oit = os_create_object_iter_alist(lio_gc->os, lio_gc->creds, regex, obj_regex, OS_OBJECT_FILE_FLAG, 1000, mkey, (void **)mrval, m_size, 2);
    if (oit == NULL) {
        nfailed++;
        log_printf(0, "ERROR: Failed with object_iter creation %s\n", foo_path);
        return(nfailed);
    }

    //** Do the actual check
    n = 0;
    while (os_next_object(lio_gc->os, oit, &fname, &plen) > 0) {
        //** Check which file it is foo or foodir/foo or foodir/bar(which should have been skipped)
        nstart = n;
        snprintf(fpath, PATH_LEN, "%s/foo", prefix);
        if (strcmp(fpath, fname) == 0) n = n | 1;
        snprintf(fpath, PATH_LEN, "%s/foodir/foo", prefix);
        if (strcmp(fpath, fname) == 0) n = n | 2;

        if (n == nstart) {
            nfailed++;
            log_printf(0, "ERROR:  EXtra file!  fname=%s\n", fname);
            return(nfailed);
        }

        for (i=0; i<2; i++) {
            if (strcmp(mrval[i], mval[i]) != 0) {
                nfailed++;
                log_printf(0, "ERROR bad value fname=%s key=%s val=%s should be mval=%s\n", fname, mkey[i], mrval[i], mval[i]);
                return(nfailed);
            }
            if (mrval[i] != NULL) free(mrval[i]);
        }

        free(fname);
    }
    os_destroy_object_iter(os, oit);
    lio_os_regex_table_destroy(regex);
    lio_os_regex_table_destroy(obj_regex);

    if (n != 3) {
        nfailed++;
        log_printf(0, "ERROR missed a file n=%d\n", n);
        return(nfailed);
    }


    //** Do a mult attr copy betwen foo and foodir/bar
    snprintf(bar_path, PATH_LEN, "%s/foodir/bar", prefix); //QWERTY
    err = gop_sync_exec(os_open_object(os, creds, bar_path, OS_MODE_READ_IMMEDIATE, "me", &bar_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", bar_path, err);
        return(nfailed);
    }
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    mkey[0] = "user.bar2";
    mval[0] = "user.bar2";
    mkey[1] = "user.bar3";
    mval[1] = "user.dummy";
    log_printf(15, "COPY_START\n");
    tbx_log_flush();
    err = gop_sync_exec(os_copy_multiple_attrs(os, creds, foo_fd, mkey, bar_fd, mval, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: with mutl attr copy: err=%d\n", err);
        return(nfailed);
    }
    log_printf(0, "COPY_END\n");
    tbx_log_flush();


    //** Verify it worked
    mkey[0] = "user.bar2";
    mkey[1] = "user.dummy";
    mval[0] = "BAR2";
    mval[1] = "foo3";
    m_size[0] = m_size[1] = -1000;
    mrval[0] = mrval[1] = NULL;
    err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
        return(nfailed);
    }
    for (i=0; i<2; i++) {
        if (strcmp(mval[i], mrval[i]) != 0) {
            nfailed++;
            log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
            return(nfailed);
        }

        free(mrval[i]);
    }

    //** Now do a link between attributes
    mkey[0] = "user.bar2";
    mval[0] = "user.link_bar2";
    mkey[1] = "user.bar3";
    mval[1] = "user.link_bar3";
    path[0] = foo_path;
    path[1] = foo_path;
    log_printf(15, "LINK_START\n");
    tbx_log_flush();
    err = gop_sync_exec(os_symlink_multiple_attrs(os, creds, path, mkey, bar_fd, mval, 2));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: with multi attr link: err=%d\n", err);
        return(nfailed);
    }

    //** Verify it worked
    mkey[0] = "user.link_bar2";
    mkey[1] = "user.link_bar3";
    mval[0] = "BAR2";
    mval[1] = "foo3";
    m_size[0] = m_size[1] = -1000;
    mrval[0] = mrval[1] = NULL;
    err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 2));
//abort();
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
        return(nfailed);
    }
    for (i=0; i<2; i++) {
        if (strcmp(mval[i], mrval[i]) != 0) {
            nfailed++;
            log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
            return(nfailed);
        }

        free(mrval[i]);
    }

    //** Now get the type of bar user.link_bar2 and user.bar2 os.create
    mkey[0] = "os.attr_type.user.link_bar2";
    mval[0] = "5";
    mkey[1] = "os.attr_type.user.bar2";
    mval[1] = "1";
    mkey[2] = "os.attr_type.os.create";
    mval[2] = "64";
    m_size[0] = m_size[1] = m_size[2] = -1000;
    mrval[0] = mrval[1] = mrval[2] = NULL;
    err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 3));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: getting mult attrs err=%d\n", err);
        return(nfailed);
    }
    for (i=0; i<3; i++) {
        if (strcmp(mval[i], mrval[i]) != 0) {
            nfailed++;
            log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[i], mval[i], mrval[i]);
            return(nfailed);
        }

        free(mrval[i]);
    }

    //** Now get the type of bar user.link_bar2 and user.bar2 os.create
    mkey[0] = "os.attr_link.user.link_bar2";
    mval[0] = match[0];
    sprintf(match[0], "%s/foo/user.bar2", prefix);
    mkey[1] = "os.attr_link.user.bar2";
    mval[1] = NULL;
    mkey[2] = "os.attr_link.os.create";
    mval[2] = NULL;
    m_size[0] = m_size[1] = m_size[2] = -1000;
    mrval[0] = mrval[1] = mrval[2] = NULL;
    err = gop_sync_exec(os_get_multiple_attrs(os, creds, bar_fd, mkey, (void **)mrval, m_size, 3));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  Error getting mult attrs!  err=%d\n", err);
        return(nfailed);
    }
    if (strcmp(mval[0], mrval[0]) != 0) {
        nfailed++;
        log_printf(0, "ERROR: val mismatch attr=%s should be=%s got=%s\n", mkey[0], mval[0], mrval[0]);
        return(nfailed);
    }
    free(mrval[0]);


    for (i=1; i<2; i++) {
        if (mrval[i] != NULL) {
            nfailed++;
            log_printf(0, "ERROR: val mismatch attr=%s should not have a link got=%s\n", mkey[i], mrval[i]);
            return(nfailed);
        }
    }
    log_printf(15, "LINK_END\n");
    tbx_log_flush();


    //** Close the 2 files
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file foo_fd err=%d\n", err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, bar_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file bar_fd err=%d\n", err);
        return(nfailed);
    }


    //** rm foo
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** And verify it's gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Can open the recently deleted file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    //** Remove everything else rm -r foodir
    snprintf(foo_path, PATH_LEN, "%s/foodir", prefix);
    regex = lio_os_path_glob2regex(foo_path);
    err = gop_sync_exec(os_remove_regex_object(os, creds, regex, NULL, OS_OBJECT_ANY_FLAG, 1000));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: rm -r %s failed with err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops!  I could access the removed dir: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    lio_os_regex_table_destroy(regex);

    log_printf(0, "PASSED!\n");

    for (i=0; i<10; i++) free(match[i]);
    free(mval);
    free(mkey);
    free(mrval);
    return(nfailed);
}

// **********************************************************************************
// check_lock_state - Validates the lock state
// **********************************************************************************

int check_lock_state(os_fd_t *foo_fd, char **active, int n_active, char **pending, int n_pending, char *lock_attr)
{
    lio_object_service_fn_t *os = lio_gc->os;
    lio_creds_t  *creds = lio_gc->creds;
    int err, v_size, ai, pi, fin;
    char *key, *val, *lval, *tmp, *bstate;
    tbx_inip_file_t *ifd;
    tbx_inip_group_t *grp;
    tbx_inip_element_t *ele;

    v_size = -10000;
    key = lock_attr;
    val=NULL;
    log_printf(5, "BEFORE get_attr lock\n");
    apr_time_t dt = apr_time_now();
    err = gop_sync_exec(os_get_attr(os, creds, foo_fd, key, (void **)&lval, &v_size));
    dt = apr_time_now() - dt;
    int sec = apr_time_sec(dt);
    log_printf(5, "AFTER get_attr lock err=%d dt=%d\n", err, sec);
    if (err != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR: getting attr=%s err=%d\n", key, err);
        return(1);
    }

    err = 0;

    ifd = tbx_inip_string_read(lval, 1);
    grp = tbx_inip_group_find(ifd, lock_attr);

    ele = tbx_inip_ele_first(grp);
    ai = pi = 0;
    while (ele != NULL) {
        key = tbx_inip_ele_get_key(ele);
        if (strcmp(key, "active_id") == 0) {
            if (ai < n_active) {
                val = tbx_inip_ele_get_value(ele);
                tmp = tbx_stk_string_token(val, ":", &bstate, &fin);
                if (strcmp(tmp, active[ai]) != 0) {
                    err++;
                    log_printf(0, "Active slot mismatch!  active_id[%d]=%s should be %s\n", ai, val, active[ai]);
                }
            } else {
                err++;
                log_printf(0, "To many Active slots!  active_id[%d]=%s should n_active=%d\n", ai, val, n_active);
            }
            ai++;
        } else if (strcmp(key, "pending_id") == 0) {
            if (pi < n_pending) {
                val = tbx_inip_ele_get_value(ele);
                tmp = tbx_stk_string_token(val, ":", &bstate, &fin);
                if (strcmp(tmp, pending[pi]) != 0) {
                    err++;
                    log_printf(0, "Pending slot mismatch!  pending_id[%d]=%s should be %s\n", pi, val, pending[pi]);
                }
            } else {
                err++;
                log_printf(0, "To many Pending slots!  pending_id[%d]=%s should n_pending=%d\n", pi, val, n_pending);
            }
            pi++;
        }

        //** Move to the next segmnet to load
        ele = tbx_inip_ele_next(ele);
    }

    if ((ai != n_active) || ( pi != n_pending)) {
        err++;
        log_printf(0, "Incorrect slot count.  n_active=%d found=%d  **  n_pending=%d found=%d\n", n_active, ai, n_pending, pi);
    }

    if (err != 0) {
        log_printf(0, "-------------------Printing Lock attribute---------------------\n");
        slog_printf(0, "%s", lval);
        log_printf(0, "---------------------------------------------------------------\n");
    }

    tbx_inip_destroy(ifd);

    free(lval);

    return(err);
}

// **********************************************************************************

void lock_pause()
{
    usleep(0.25*1000000);
}

// **********************************************************************************
//  os_locking_tests - Tests object locking routines
// **********************************************************************************

int os_locking_tests(char *prefix)
{
    lio_object_service_fn_t *os = lio_gc->os;
    lio_creds_t  *creds = lio_gc->creds;
    char foo_path[PATH_LEN];
    os_fd_t *foo_fd;
    int err, i;
    os_fd_t *fd_read[5], *fd_write[3], *fd_abort[2];
    char *task[10];
    gop_opque_t *q;
    gop_op_generic_t *gop, *gop_read[5], *gop_write[3], *gop_abort[2];
    apr_time_t start, dt;
    int sec;
    int nfailed = 0;
    char *lock_attr = "os.lock";

    memset(fd_read, 0, sizeof(fd_read));
    memset(fd_write, 0, sizeof(fd_write));
    memset(fd_abort, 0, sizeof(fd_abort));

    q = gop_opque_new();
    opque_start_execution(q);

    //** Create the file foo
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    start = apr_time_now();

    //** Spawn 3 open(foo, READ)=r{0,1,2} tasks
    gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r0", &fd_read[0], wait_time);
    gop_read[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r1", &fd_read[1], wait_time);
    gop_read[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r2", &fd_read[2], wait_time);
    gop_read[2] = gop;
    gop_opque_add(q, gop);
    lock_pause();


    //** Spawn 2 open(foo, WRITE)=w{0,1} tasks
    gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w0", &fd_write[0], wait_time);
    gop_write[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w1", &fd_write[1], wait_time);
    gop_write[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    //** Spawn 2 open(foo, WRITE)=a{0,1} tasks.  These will be aborted opens
    gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "a0", &fd_abort[0], 4);  //** This time is short to auto timeout
    gop_abort[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "a1", &fd_abort[1], wait_time);
    gop_abort[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();


    //** Spawn 2 open(foo, READ)=r{3,4} task
    gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r3", &fd_read[3], wait_time);
    gop_read[3] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    gop = os_open_object(os, creds, foo_path, OS_MODE_READ_BLOCKING, "r4", &fd_read[4], wait_time);
    gop_read[4] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    //** Spawn 1 open(foo, WRITE)=w2 task
    gop = os_open_object(os, creds, foo_path, OS_MODE_WRITE_BLOCKING, "w2", &fd_write[2], wait_time);
    gop_write[2] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "STATE:  active=r0,r1,r2  pending=w0,w1,a0,a1,r3,r4,w2 dt=%d\n", sec);
    tbx_log_flush();


    //** Wait for the opens to complete
    gop_waitany(gop_read[0]);
    gop_waitany(gop_read[1]);
    gop_waitany(gop_read[2]);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After open(read) completes dt=%d\n", sec);

    //** Verify the above state: active r0,r1,r2 pending w0,w1,r3,r4,w2
    task[0] = "r0";
    task[1] = "r1";
    task[2] = "r2";
    task[3] = "w0";
    task[4] = "w1";
    task[5] = "a0";
    task[6] = "a1";
    task[7] = "r3";
    task[8] = "r4";
    task[9] = "w2";
    err = check_lock_state(foo_fd, &(task[0]), 3, &(task[3]), 7, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 1 dt=%d\n", sec);

    //** Close the 3 open tasks(r0,r1,r2).
    err = gop_sync_exec(os_close_object(os, fd_read[0]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, fd_read[1]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, fd_read[2]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    log_printf(0, "STATE: active=w0  pending=w1,a0,a1,r3,r4,w2\n");
    tbx_log_flush();

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "after read closing dt=%d\n", sec);

    //** Wait for the opens to complete
    gop_waitany(gop_write[0]);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "after write[0] closing dt=%d\n", sec);

    //** Verify state. This should leave you with w0 active and w1,a0,a1,r3,r4,w2
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 6, lock_attr);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 2 dt=%d\n", sec);

    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }


    log_printf(0, "STATE: ABORT_TIMEOUT(a0)  active=w0  pending=w1,a1,r3,r4,w2\n");
    tbx_log_flush();

    //** Let's do the aborts now
    err = gop_waitall(gop_abort[0]);  //** This should timeout on it's own
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Open succeded for a0 when it should have timed out file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After abort[0] dt=%d\n", sec);

    //** Verify the state. active=w0  pending=w1,a1,r3,r4,w2
    for (i=5; i<9; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 5, lock_attr);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 3 dt=%d\n", sec);

    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    log_printf(0, "STATE: ABORT_CMD(a1)  active=w0  pending=w1,r3,r4,w2\n");
    tbx_log_flush();

    //** Issue an abort for a1
    err = gop_sync_exec(os_abort_open_object(os, gop_abort[1]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Aborting a1 file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After abort[1] dt=%d\n", sec);

    //** Verify the state. active=w0  pending=w1,r3,r4,w2
    for (i=5; i<8; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 4, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Close w0.
    err = gop_sync_exec(os_close_object(os, fd_write[0]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After close w0 dt=%d\n", sec);

    log_printf(0, "STATE: active=w1  pending=r3,r4,w2\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_write[1]);

    //** Verify state: active=w1, pending=r3,r4,w2
    err = check_lock_state(foo_fd, &(task[4]), 1, &(task[5]), 3, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Close w1;
    err = gop_sync_exec(os_close_object(os, fd_write[1]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After close w1 dt=%d\n", sec);

    log_printf(0, "STATE: active=r3,r4  pending=w2\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_read[3]);
    gop_waitany(gop_read[4]);

    //** Verify state: active=r3,r4 pending=w2
    err = check_lock_state(foo_fd, &(task[5]), 2, &(task[7]), 1, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Close r3, r4
    err = gop_sync_exec(os_close_object(os, fd_read[3]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_close_object(os, fd_read[4]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }


    log_printf(0, "STATE: active=w2  pending=\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_write[2]);

    //** Verify state active=w2  pending=
    err = check_lock_state(foo_fd, &(task[7]), 1, NULL, 0, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Close w2;
    err = gop_sync_exec(os_close_object(os, fd_write[2]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    log_printf(0, "Performing lock test cleanup\n");
    tbx_log_flush();

    //** Close the inital foo_fd
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    //** rm foo
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** And verify it's gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Can open the recently deleted file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    gop_opque_free(q, OP_DESTROY);

    log_printf(0, "PASSED!\n");
    return(nfailed);
}

//--------------------------------

// **********************************************************************************
//  os_user_locking_tests - Tests object user locking routines
// **********************************************************************************

int os_user_locking_tests(char *prefix)
{
    lio_object_service_fn_t *os = lio_gc->os;
    lio_creds_t  *creds = lio_gc->creds;
    char foo_path[PATH_LEN];
    os_fd_t *foo_fd;
    int err, i;
    os_fd_t *fd_read[5], *fd_write[3], *fd_abort[2];
    char *task[10];
    gop_opque_t *q;
    gop_op_generic_t *gop, *gop_read[5], *gop_write[3], *gop_abort[2];
    apr_time_t start, dt;
    int sec;
    int nfailed = 0;
    char *lock_attr = "os.lock.user";

    memset(fd_read, 0, sizeof(fd_read));
    memset(fd_write, 0, sizeof(fd_write));
    memset(fd_abort, 0, sizeof(fd_abort));

    q = gop_opque_new();
    opque_start_execution(q);

    //** Create the file foo
    snprintf(foo_path, PATH_LEN, "%s/foo", prefix);
    err = gop_sync_exec(os_create_object(os, creds, foo_path, OS_OBJECT_FILE_FLAG, "me"));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: creating file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** Verify foo exists
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    start = apr_time_now();

    //** Spawn 3 lock(foo, READ)=r{0,1,2} tasks
    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "r0", &fd_read[0], wait_time)) == OP_STATE_SUCCESS);
log_printf(0, "ULOCK: fd_read[0]=%p\n", fd_read[0]); tbx_log_flush();
    gop = os_lock_user_object(os, fd_read[0], OS_MODE_READ_BLOCKING, wait_time);
log_printf(0, "ULOCK: fd_read[0]=%p after lock_user gop\n", fd_read[0]); tbx_log_flush();
    gop_read[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "r1", &fd_read[1], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_read[1], OS_MODE_READ_BLOCKING, wait_time);
    gop_read[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "r2", &fd_read[2], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_read[2], OS_MODE_READ_BLOCKING, wait_time);
    gop_read[2] = gop;
    gop_opque_add(q, gop);
    lock_pause();


    //** Spawn 2 lock(foo, WRITE)=w{0,1} tasks
    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "w0", &fd_write[0], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_write[0], OS_MODE_WRITE_BLOCKING, wait_time);
    gop_write[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "w1", &fd_write[1], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_write[1], OS_MODE_WRITE_BLOCKING, wait_time);
    gop_write[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    //** Spawn 2 lock(foo, WRITE)=a{0,1} tasks.  These will be aborted opens
    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "a0", &fd_abort[0], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_abort[0], OS_MODE_WRITE_BLOCKING, 4); //** This time is short to auto timeout
    gop_abort[0] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "a1", &fd_abort[1], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_abort[1], OS_MODE_WRITE_BLOCKING, 4); //** This time is short to auto timeout
    gop_abort[1] = gop;
    gop_opque_add(q, gop);
    lock_pause();


    //** Spawn 2 lock(foo, READ)=r{3,4} task
    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "r3", &fd_read[3], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_read[3], OS_MODE_READ_BLOCKING, wait_time);
    gop_read[3] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "r4", &fd_read[4], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_read[4], OS_MODE_READ_BLOCKING, wait_time);
    gop_read[4] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    //** Spawn 1 lock(foo, WRITE)=w2 task
    assert(gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "w2", &fd_write[2], wait_time)) == OP_STATE_SUCCESS);
    gop = os_lock_user_object(os, fd_write[2], OS_MODE_WRITE_BLOCKING, wait_time);
    gop_write[2] = gop;
    gop_opque_add(q, gop);
    lock_pause();

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "STATE:  active=r0,r1,r2  pending=w0,w1,a0,a1,r3,r4,w2 dt=%d\n", sec);
    tbx_log_flush();


    //** Wait for the inital read locks to complete
    gop_waitany(gop_read[0]);
    gop_waitany(gop_read[1]);
    gop_waitany(gop_read[2]);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After open(read) completes dt=%d\n", sec);

    //** Verify the above state: active r0,r1,r2 pending w0,w1,r3,r4,w2
    task[0] = "r0";
    task[1] = "r1";
    task[2] = "r2";
    task[3] = "w0";
    task[4] = "w1";
    task[5] = "a0";
    task[6] = "a1";
    task[7] = "r3";
    task[8] = "r4";
    task[9] = "w2";
    err = check_lock_state(foo_fd, &(task[0]), 3, &(task[3]), 7, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 1 dt=%d\n", sec);

    //** Release the 3 read locks(r0,r1,r2).
    err = gop_sync_exec(os_lock_user_object(os, fd_read[0], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_lock_user_object(os, fd_read[1], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_lock_user_object(os, fd_read[2], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    log_printf(0, "STATE: active=w0  pending=w1,a0,a1,r3,r4,w2\n");
    tbx_log_flush();

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "after read closing dt=%d\n", sec);

    //** Wait for the next write lock to complete
    gop_waitany(gop_write[0]);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "after write[0] lock dt=%d\n", sec);

    //** Verify state. This should leave you with w0 active and w1,a0,a1,r3,r4,w2
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 6, lock_attr);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 2 dt=%d\n", sec);

    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }


    log_printf(0, "STATE: ABORT_TIMEOUT(a0)  active=w0  pending=w1,a1,r3,r4,w2\n");
    tbx_log_flush();

    //** Let's do the aborts now
    err = gop_waitall(gop_abort[0]);  //** This should timeout on it's own
    if (err == OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Open succeded for a0 when it should have timed out file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After abort[0] dt=%d\n", sec);

    //** Verify the state. active=w0  pending=w1,a1,r3,r4,w2
    for (i=5; i<9; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 5, lock_attr);

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After check_lock 3 dt=%d\n", sec);

    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    log_printf(0, "STATE: ABORT_CMD(a1)  active=w0  pending=w1,r3,r4,w2\n");
    tbx_log_flush();

    //** Issue an abort for a1
    err = gop_sync_exec(os_abort_lock_user_object(os, gop_abort[1]));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Aborting a1 file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After abort[1] dt=%d\n", sec);

    //** Verify the state. active=w0  pending=w1,r3,r4,w2
    for (i=5; i<8; i++) task[i] = task[i+1];  //** Drop a0 fro mthe list
    err = check_lock_state(foo_fd, &(task[3]), 1, &(task[4]), 4, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Unlock w0.
    err = gop_sync_exec(os_lock_user_object(os, fd_write[0], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After close w0 dt=%d\n", sec);

    log_printf(0, "STATE: active=w1  pending=r3,r4,w2\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_write[1]);

    //** Verify state: active=w1, pending=r3,r4,w2
    err = check_lock_state(foo_fd, &(task[4]), 1, &(task[5]), 3, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Unlock w1
    err = gop_sync_exec(os_lock_user_object(os, fd_write[1], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    dt = apr_time_now() - start;
    sec = apr_time_sec(dt);
    log_printf(0, "After close w1 dt=%d\n", sec);

    log_printf(0, "STATE: active=r3,r4  pending=w2\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_read[3]);
    gop_waitany(gop_read[4]);

    //** Verify state: active=r3,r4 pending=w2
    err = check_lock_state(foo_fd, &(task[5]), 2, &(task[7]), 1, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Unlock r3, r4
    err = gop_sync_exec(os_lock_user_object(os, fd_read[3], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }
    err = gop_sync_exec(os_lock_user_object(os, fd_read[4], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    log_printf(0, "STATE: active=w2  pending=\n");
    tbx_log_flush();

    //** Wait for the opens to complete
    gop_waitany(gop_write[2]);

    //** Verify state active=w2  pending=
    err = check_lock_state(foo_fd, &(task[7]), 1, NULL, 0, lock_attr);
    if (err != 0) {
        nfailed++;
        log_printf(0, "ERROR: checking state\n");
        return(nfailed);
    }

    //** Unlock w2;
    err = gop_sync_exec(os_lock_user_object(os, fd_write[2], OS_MODE_UNLOCK, wait_time));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Unlocking file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    log_printf(0, "Performing lock test cleanup\n");
    tbx_log_flush();

    //** Close all the open fd
    assert(gop_sync_exec(os_close_object(os, fd_read[0])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_read[1])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_read[2])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_read[3])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_read[4])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_write[0])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_write[1])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_write[2])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_abort[0])) == OP_STATE_SUCCESS);
    assert(gop_sync_exec(os_close_object(os, fd_abort[1])) == OP_STATE_SUCCESS);

    //** Close the inital foo_fd
    err = gop_sync_exec(os_close_object(os, foo_fd));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: Closing file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    //** rm foo
    err = gop_sync_exec(os_remove_object(os, creds, foo_path));
    if (err != OP_STATE_SUCCESS) {
        nfailed++;
        log_printf(0, "ERROR: opening file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    // ** And verify it's gone
    err = gop_sync_exec(os_open_object(os, creds, foo_path, OS_MODE_READ_IMMEDIATE, "me", &foo_fd, wait_time));
    if (err != OP_STATE_FAILURE) {
        nfailed++;
        log_printf(0, "ERROR: Oops! Can open the recently deleted file: %s err=%d\n", foo_path, err);
        return(nfailed);
    }

    gop_opque_free(q, OP_DESTROY);

    log_printf(0, "PASSED!\n");
    return(nfailed);
}
