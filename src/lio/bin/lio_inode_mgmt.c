/*
   Copyright 2025 Vanderbilt University

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

#include <lio/ex3_fwd.h>
#include <lio/lio.h>
#include <tbx/stack.h>
#include <tbx/append_printf.h>
#include <tbx/stdinarray_iter.h>
#include <tbx/type_malloc.h>
#include "os.h"
#include "inode_lut.h"
#include "inode.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>


os_inode_lut_t *ilut = NULL;

int loop_inode_hack = 0;   //QWERT REmove me after testing

//*************************************************************************
// do_generate_walk - Walks the path adding objects
//*************************************************************************

void do_generate_walk(os_inode_ctx_t *ctx, const char *path, int recurse_depth)
{
    void *walk;
    char *fname;
    ex_id_t inode, parent;
    int ftype, prefix_len, len;

    //** Create the iterator
    walk = walk_create(path, recurse_depth);

    //** Process entries
    while (walk_next(walk, &fname, &inode, &ftype, &parent, &prefix_len, &len) == 0) {
//fprintf(stderr, "do_generate_walk: fname=%s inode=" XIDT " ftype=%d parent=" XIDT " de=%s prefix_len=%d len=%d\n", fname, inode, ftype, parent, fname + prefix_len, prefix_len, len);

        if (inode == OS_INODE_MISSING) fprintf(stdout, "%s: %s\n", OS_MSG_NO_INODE, fname);
        os_inode_put(ctx, inode, parent, ftype,len-prefix_len, fname + prefix_len);
        free(fname);
    }

    //** Cleanup
    walk_destroy(walk);

}

//*************************************************************************
// generate_db - Updates or generates an existing Inode DB
//*************************************************************************

int generate_db(const char *db_prefix, int n_shards, int recurse_depth, int start_option, int argc, char **argv)
{
    os_inode_ctx_t *ctx;
    tbx_stdinarray_iter_t *it;
    char *path;

    //** Open the Inode DB
    ctx = os_inode_open_ctx(db_prefix, OS_INODE_OPEN_CREATE_IF_NEEDED|OS_INODE_OPEN_READ_WRITE, n_shards);
    if (ctx == NULL) {
        fprintf(stderr, "ERROR: Unable to open DB ctx! prefix=%s mode=%d n_shards=%d\n", db_prefix, OS_INODE_OPEN_CREATE_IF_NEEDED|OS_INODE_OPEN_READ_WRITE, n_shards);
        return(1);
    }

    //** Create the argv iterator
    it = tbx_stdinarray_iter_create(argc-start_option, (const char **)(argv+start_option));

    //** Loop over entries
    while ((path = tbx_stdinarray_iter_next(it)) != NULL) {
//fprintf(stderr, "generate_db: path=%s\n", path);
        do_generate_walk(ctx, path, recurse_depth);
        free(path);
    }

    tbx_stdinarray_iter_destroy(it);

//fprintf(stderr, "generate_db: closing ctx\n");
    //** Close the DB
    os_inode_close_ctx(ctx);

//fprintf(stderr, "generate_db: exiting\n");

    return(0);
}

//*************************************************************************
// changllog_print - Dumps an entry to the changlog
//*************************************************************************

void changelog_print(FILE *fd, os_inode_db_t *db, const char *error, ex_id_t ino, int ftype)
{
    int i, n;
    char *dentry[OS_PATH_MAX];
    ex_id_t inode[OS_PATH_MAX];
    int ft[OS_PATH_MAX];
    char path[OS_PATH_MAX];

    if (os_inode_lookup_path_db(db, ino, inode, ft, dentry, &n) == 0) {
        os_inode_lookup2path(path, n, inode, dentry);
        fprintf(fd, "%s " XIDT " %d %s\n", error, ino, ftype, path);
    } else {
        fprintf(fd, "ERROR-LUT error=%s " XIDT "\n", error, ino);
    }

    for (i=0; i<n; i++) {
        if (dentry[i]) free(dentry[i]);
    }
}

//*************************************************************************
// check_missing_db - Checks that all the entries in the src DB appear in the dest
//*************************************************************************

int check_missing_db(os_inode_db_t *src, os_inode_db_t *dest)
{
    int i, len, n_errs, ftype;
    rocksdb_iterator_t *it;
    char *buf, *errstr;
    size_t nbytes;
    os_inode_shard_t *ss, *ds;
    os_inode_rec_t *r;
    ex_id_t parent;
    char *de;

    n_errs = 0;

    //** Loop over all the shards
    for (i=0; i<src->n_shards; i++) {
        ss = src->shard[i];
        ds = dest->shard[i];

        //** Make The src iterator
        it = rocksdb_create_iterator(ss->db, ss->ropt);
        rocksdb_iter_seek_to_first(it);

        //** Cycle though checking for issues
        while (rocksdb_iter_valid(it) > 0) {
            buf = (char *)rocksdb_iter_value(it, &nbytes);
            de = NULL;
            if (nbytes == 0) { goto next; }

            //** Look it up in the dest shard
            r = (os_inode_rec_t *)buf;
            if (os_inode_shard_get(ds, r->inode, &parent, &ftype, &len, &de) != 0) {
                changelog_print(stdout, src, "MISSING-INODE", r->inode, r->ftype);
                goto next;
            }

//fprintf(stdout, "check_missing_db: SRC inode=" XIDT " parent=" XIDT " ftype=%d de=%s\n", r->inode, r->dentry.parent_inode, r->ftype, r->dentry.dentry);
//fprintf(stdout, "check_missing_db: DST inode=" XIDT " parent=" XIDT " ftype=%d de=%s\n", r->inode, parent, ftype, de);
            //** Check the parent is the same
            if (parent != r->dentry.parent_inode) {
                changelog_print(stdout, src, "MISMATCH-PARENT", r->inode, r->ftype);
                goto next;
            }

            //** Check the dentry is the same
            if (strcmp(r->dentry.dentry, de) != 0) {
                changelog_print(stdout, src, "MISMATCH-DENTRY", r->inode, r->ftype);
                goto next;
            }

            //** Check the ftype is the same
            if (ftype != r->ftype) {
                changelog_print(stdout, src, "MISMATCH-FTYPE", r->inode, r->ftype);
                goto next;
            }

//fprintf(stdout, "MATCH: shard=%d src_inode=" XIDT "\n", i, r->inode);
next:
            if (de) free(de);
            rocksdb_iter_next(it);

            errstr = NULL;
            rocksdb_iter_get_error(it, &errstr);
            if (errstr != NULL) {
                fprintf(stderr, "ERROR: RocksDB iterator error! path=%s shard=%d errstr=%s\n", src->prefix, i, errstr);
                fflush(stderr);
            }
        }

        //** Cleanup
        rocksdb_iter_destroy(it);
    }

    return(n_errs);
}

//*************************************************************************
// check_missing - Checks that all the entries in the src appear in the dest
//*************************************************************************

int check_missing(os_inode_ctx_t *src_ctx, os_inode_ctx_t *dest_ctx)
{
    int n_errs;

    n_errs = 0;

    //** check for inconsistencies in the inode DB
//fprintf(stdout, "check_missing: INODE check\n");
    n_errs += check_missing_db(src_ctx->inode, dest_ctx->inode);

    //** Do the same for the dir DB
//fprintf(stdout, "check_missing: DIR check\n");
    n_errs += check_missing_db(src_ctx->dir, dest_ctx->dir);

    return(n_errs);
}

//*************************************************************************
// check_orphans_db - Checks that all the entries in the dest DB appear in the dest
//*************************************************************************

int check_orphans_db(os_inode_db_t *src, os_inode_db_t *dest)
{
    int i, len, n_errs, ftype;
    rocksdb_iterator_t *it;
    char *buf, *errstr;
    size_t nbytes;
    os_inode_shard_t *ss, *ds;
    os_inode_rec_t *r;
    ex_id_t parent;
    char *de;

    n_errs = 0;

    //** Loop over all the shards
    for (i=0; i<src->n_shards; i++) {
        ss = src->shard[i];
        ds = dest->shard[i];

        //** Make The src iterator
        it = rocksdb_create_iterator(ds->db, ds->ropt);
        rocksdb_iter_seek_to_first(it);

        //** Cycle though checking for issues
        while (rocksdb_iter_valid(it) > 0) {
            buf = (char *)rocksdb_iter_value(it, &nbytes);
            de = NULL;
            if (nbytes == 0) { goto next; }

            //** Look it up in the source shard
            //** We don't have to check the individual fields because they would have been caught
            //** during the missing run.
            r = (os_inode_rec_t *)buf;
            if (os_inode_shard_get(ss, r->inode, &parent, &ftype, &len, &de) != 0) {
                changelog_print(stdout, dest, "ORPHAN-INODE", r->inode, r->ftype);
            }

//fprintf(stdout, "CHECK: shard=%d dest_inode=" XIDT " de=%s\n", i, r->inode, r->dentry.dentry);
next:
            if (de) free(de);
            rocksdb_iter_next(it);

            errstr = NULL;
            rocksdb_iter_get_error(it, &errstr);
            if (errstr != NULL) {
                printf("ERROR: RocksDB iterator error! path=%s shard=%d errstr=%s\n", src->prefix, i, errstr);
                fflush(stdout);
            }
        }

        //** Cleanup
        rocksdb_iter_destroy(it);
    }

    return(n_errs);
}

//*************************************************************************
// check_orphans - Looks for orphaned entries in the dest DB
//*************************************************************************

int check_orphans(os_inode_ctx_t *src_ctx, os_inode_ctx_t *dest_ctx)
{
    int n_errs;

    n_errs = 0;

    //** check for inconsistencies in the inode DB
//fprintf(stdout, "check_orphans: INODE check\n");
    n_errs += check_orphans_db(src_ctx->inode, dest_ctx->inode);

    //** Do the same for the dir DB
//fprintf(stdout, "check_orphans: DIR check\n");
    n_errs += check_orphans_db(src_ctx->dir, dest_ctx->dir);

    return(n_errs);
}

//*************************************************************************
// changelog_generate - Generates the missing entries from the src in the destination
//   and also does the reverse looking for possibly orphaned entries in the dest
//   if enabled
//*************************************************************************

void changelog_generate(int check_for_orphans, const char *fsrc, const char *fdest)
{
    os_inode_ctx_t *src_ctx, *dest_ctx;

    //** Open the DB's
    src_ctx = os_inode_open_ctx(fsrc, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY, -1);
    if (src_ctx == NULL) {
        fprintf(stderr, "ERROR: Unable to open source ctx! prefix=%s mode=%d\n", fsrc, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY);
        return;
    }

    dest_ctx = os_inode_open_ctx(fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY, -1);
    if (dest_ctx == NULL) {
        fprintf(stderr, "ERROR: Unable to open destination ctx! prefix=%s mode=%d\n", fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY);
        os_inode_close_ctx(src_ctx);
        return;
    }

    if (os_inode_compare_shard_count(src_ctx, dest_ctx) != 0) {
        fprintf(stderr, "ERROR: OOPS! Source and destination shard sizes are different!  src=%s dest=%s\n", fsrc, fdest);
        goto oops;
    }

    //** Do the consistency from from the src->dest
    check_missing(src_ctx, dest_ctx);

    //** See if we do the reverse.
    if (check_for_orphans) check_orphans(src_ctx, dest_ctx);

oops:
    //** Close everything
    os_inode_close_ctx(src_ctx);
    os_inode_close_ctx(dest_ctx);

    return;
}

//*************************************************************************
// changelog_merge - Attempts to merge/reconcile the changelog into the dest DB
//*************************************************************************

void changelog_merge(const char *changelog, const char *fdest, os_inode_lut_t *ilut)
{
    os_inode_ctx_t *ctx;
    FILE *fdin;
    char entry[OS_PATH_MAX + 1024];

    //** Open the changelog
    fdin = fopen(changelog, "r");
    if (fdin == NULL) {
        fprintf(stderr, "ERROR: Unable to open destination ctx! prefix=%s mode=%d\n", fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY);
        return;
    }

    //** Open the dest in RW mode
    ctx = os_inode_open_ctx(fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_WRITE, -1);
    if (ctx == NULL) {
        fclose(fdin);
        fprintf(stderr, "ERROR: Unable to open destination ctx! prefix=%s mode=%d\n", fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_WRITE);
        return;
    }

    //** Loop over the entries
    while (fgets(entry, sizeof(entry), fdin)) {
        os_inode_process_entry(ctx, ilut, entry, stdout);
    }

    //** clean up
    fclose(fdin);
    os_inode_close_ctx(ctx);
    return;
}

//*************************************************************************

int do_lookup_check(ex_id_t *inode, int *ftype, char **dentry, int n)
{
    char path[OS_PATH_MAX];
    ex_id_t my_inode;
    int err, my_ftype;

    os_inode_lookup2path(path, n, inode, dentry);
    err = walk_info(walk_arg, path, &my_inode, &my_ftype);
    if (err) return(1);

    if (my_inode != inode[0]) err |= 2;
    if (my_ftype != ftype[0]) err |= 4;
    return(err);
}

//*************************************************************************

void perform_lookups(int do_check, const char *fdest, int start_option, int argc, char **argv)
{
    os_inode_ctx_t *ctx;
    tbx_stdinarray_iter_t *it;
    char *entry;
    ex_id_t ino;
    int err;

    int n, cnt, i;
    char *dentry[OS_PATH_MAX];
    char path[OS_PATH_MAX];
    ex_id_t inode[OS_PATH_MAX];
    int ftype[OS_PATH_MAX];

    //** Open the dest in RW mode
    ctx = os_inode_open_ctx(fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY, -1);
    if (ctx == NULL) {
        fprintf(stderr, "ERROR: Unable to open destination ctx! prefix=%s mode=%d\n", fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY);
        return;
    }

    //** Create the argv iterator
    it = tbx_stdinarray_iter_create(argc-start_option, (const char **)(argv+start_option));

    //** Loop over entries
    cnt = 0;
    while ((entry = tbx_stdinarray_iter_next(it)) != NULL) {
        sscanf(entry, XIDT, &ino);
        free(entry);

        err = (ilut) ? os_inode_lookup_path_with_lut(ctx, ilut, ino, inode, ftype, dentry, &n) :
                       os_inode_lookup_path(ctx, ino, inode, ftype, dentry, &n);
//fprintf(stderr, "%d: lookup_err=%d n=%d\n", cnt, err, n);

        if (err == 0) {
            if (do_check) {
                i=do_lookup_check(inode, ftype, dentry, n);
                fprintf(stdout, "%d: CHECK %d\n", cnt, i);
            }

            os_inode_lookup2path(path, n, inode, dentry);
            fprintf(stdout, "%d: PATH %s\n", cnt, path);
            for (i=n-1; i>=0; i--) {
                if (dentry[i]) free(dentry[i]);
            }

            fprintf(stdout, "%d: INODE ", cnt);
            for (i=n-1; i>=0; i--) {
                if (i >= n-1) {
                    fprintf(stdout, XIDT, inode[i]);
                } else {
                    fprintf(stdout, "/" XIDT, inode[i]);
                }
            }
            fprintf(stdout, "\n");

            fprintf(stdout, "%d: FTYPE ", cnt);
            for (i=n-1; i>=0; i--) {
                if (i >= n-1) {
                    fprintf(stdout, "%X", ftype[i]);
                } else {
                    fprintf(stdout, "/%X", ftype[i]);
                }
            }
            fprintf(stdout, "\n");
        } else {
            fprintf(stdout, "%d: ERROR: " XIDT "\n", cnt, ino);
        }

        cnt++;
    }

    tbx_stdinarray_iter_destroy(it);

    //** Close the DB
    os_inode_close_ctx(ctx);
}

//*************************************************************************

void perform_deletes(int do_check, const char *fdest, int start_option, int argc, char **argv)
{
    os_inode_ctx_t *ctx;
    tbx_stdinarray_iter_t *it;
    char *entry;
    ex_id_t ino;
    int cnt;

    //** Open the dest in RW mode
    ctx = os_inode_open_ctx(fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_WRITE, -1);
    if (ctx == NULL) {
        fprintf(stderr, "ERROR: Unable to open destination ctx! prefix=%s mode=%d\n", fdest, OS_INODE_OPEN_EXISTS|OS_INODE_OPEN_READ_ONLY);
        return;
    }

    //** Create the argv iterator
    it = tbx_stdinarray_iter_create(argc-start_option, (const char **)(argv+start_option));

    //** Loop over entries
    cnt = 0;
    while ((entry = tbx_stdinarray_iter_next(it)) != NULL) {
        sscanf(entry, XIDT, &ino);
        free(entry);

//** FIXME
if (loop_inode_hack) {
    char *de = NULL;
    int ftype = 0, len = 0;
    ex_id_t parent = 0;
    os_inode_get(ctx, ino, &parent, &ftype, &len, &de);
    parent = ino;
    os_inode_put(ctx, ino, parent, ftype, len, de);
    fprintf(stderr, "LOOPING: inode=" XIDT "\n", ino);
    if (de) free(de);
} else {
        os_inode_del(ctx, ino, 0);
//fprintf(stderr, "%d: delete inode=" XIDT "\n", cnt, ino);
}
        cnt++;
    }

    tbx_stdinarray_iter_destroy(it);

    //** Close the DB
    os_inode_close_ctx(ctx);
}


//*************************************************************************
//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, mode, n_shards, recurse_depth, start_option, do_check, lio_mode;
    int n_lut_max, n_epoch, n_new_epoch;
    double shrink;
    char *fsrc, *fdest, *changelog;

    if (argc < 2) {
        printf("\n");
        printf("lio_inode_mgmt\n");
        printf("    [-c LStore_URL]\n");
        printf("    --generate n_shards prefix recurse_depth file_list |\n");
        printf("    [--missing | --fsck] --src src_db --dest dest_db |\n");
        printf("    --merge changelog --dest dest_db |\n");
        printf("    --lut n_lut_max shrink_fraction n_epoch n_new_epoch |\n");
        printf("    --src src_db --lookup inode_list |\n");
        printf("    --src src_db --lookup-check inode_list |\n");
        printf("    --src src_db --delete inode_list \n");
        printf("\n");
        printf("    -c LStore_URL              - All paths are LStore and use this config.\n");
        printf("                               - NOTE: This MUST be the initial argument!\n");
        printf("    --generate n_shards db_prefix recurse_depth - Create an Inode DB using the number of shards at the prefix provided\n");
        printf("                                 The 1st entry in the file_list is stored as the base OS prefix for --merge\n");
        printf("    --src src_db               - Source Inode DB\n");
        printf("    --dest dest_db             - Destination Inode DB\n");
        printf("    --missing                  - Generate a list of missing entries to stdout from dest_db from src_db\n");
        printf("    --fsck                     - Similar to --missing but does a reverse reconciliation as well.\n");
        printf("                                 This should only be done on full file system walks\n");
        printf("    --merge changelog          - Merges the entries from the changelog into the dest_db\n");
        printf("    --lut n_lut_max shrink_fraction n_epoch n_new_epoch - Add an in-memory LUT in front of the Inode DB\n");
        printf("          n_lut_max            - Max entries allowed in the LUT\n");
        printf("          shrink_fraction      - How much to scale back entries when n_lut_max is hit\n");
        printf("          n_epoch              - How many epochs to tally for use in shrinking\n");
        printf("          n_new_epoch          - How many new entries before triggering a new epoch\n");
        printf("    --lookup | --lookup-check  - Looks up Inodes in the src_db and prints to stdout and checks for existence if enabled\n");
        printf("                                 Check error codes are:\n");
        printf("                                     0 - Success\n");
        printf("                                     1 - Object missing. Unable to stat file.\n");
        printf("                                     2 - Inode mismatch\n");
        printf("                                     4 - ftype mismatch\n");
        printf("    --delete                   - List of inodes int the DB to delete\n");
        printf("    inode_list                 - List of inodes to lookup\n");
        printf("    file_list                  - List of files and directorires to use with --generate\n");
        printf("\n");
        exit(0);
    }

    //** See if we are running in local or LStore mode
    lio_mode = 0;
    changelog = NULL;
    if (strcmp(argv[1], "-c") == 0) {
fprintf(stderr, "LSTORE MODE\n");
        lio_init(&argc, &argv);  //** Load the LStore config
fprintf(stderr, "LSTORE MODE after init\n"); fflush(stderr);
        lio_mode = 1;

        os_inode_walk_setup(1);
    } else {
fprintf(stderr, "LOCAL MODE\n");
        os_inode_walk_setup(0);
    }

fprintf(stderr, "argc=%d\n", argc);
    //** Parse the args
    do_check = 0;
    mode = -1;
    fsrc = fdest = NULL;
    i = 1;
    do {
        start_option = i;
fprintf(stderr, "ARGV[%d]=%s\n", i, argv[i]);
        if (strcmp(argv[i], "--generate") == 0) { //** Generating or updating a DB
            i++;
            n_shards = atoi(argv[i]);
            i++;
            fdest = argv[i];
            i++;
            recurse_depth = atoi(argv[i]);
            i++;
            generate_db(fdest, n_shards, recurse_depth, i, argc, argv);
            if (lio_mode) lio_shutdown();
            return(0);
        } else if (strcmp(argv[i], "--missing") == 0) {
            mode = 1;
            i++;
        } else if (strcmp(argv[i], "--fsck") == 0) {
            mode = 2;
            i++;
        } else if (strcmp(argv[i], "--src") == 0) {
            i++;
            fsrc = argv[i];
            i++;
        } else if (strcmp(argv[i], "--dest") == 0) {
            i++;
            fdest = argv[i];
            i++;
        } else if (strcmp(argv[i], "--merge") == 0) {
            mode = 3;
            i++;
            changelog = argv[i];
            i++;
        } else if (strcmp(argv[i], "--lookup") == 0) {
            mode = 4;
            i++;
        } else if (strcmp(argv[i], "--lookup-check") == 0) {
            mode = 4;
            do_check = 1;
            i++;
        } else if (strcmp(argv[i], "--delete") == 0) {
            mode = 5;
            i++;
} else if (strcmp(argv[i], "--loop") == 0) {  //*QWERT Delete me
   mode = 5;
   loop_inode_hack = 1;
   i++;
        } else if (strcmp(argv[i], "--lut") == 0) {
            i++;
            n_lut_max = atoi(argv[i]);
            i++;
            shrink = atof(argv[i]);
            i++;
            n_epoch = atoi(argv[i]);
            i++;
            n_new_epoch = atoi(argv[i]);
            i++;
            ilut = os_inode_lut_create(n_lut_max, shrink, n_epoch, n_new_epoch, 0);
        }
    } while ((start_option < i) && (i<argc));
    start_option = i;

    if (mode == 1) {
        if (fsrc == NULL) { fprintf(stderr, "ERROR: changelog missing undefined --src\n"); exit(1); }
        if (fdest == NULL) { fprintf(stderr, "ERROR: changelog missing undefined --dest\n"); exit(1); }
        changelog_generate(0, fsrc, fdest);
    } else if (mode == 2) {
        if (fsrc == NULL) { fprintf(stderr, "ERROR: changelog orphan undefined --src\n"); exit(1); }
        if (fdest == NULL) { fprintf(stderr, "ERROR: changelog orphan undefined --dest\n"); exit(1); }
        changelog_generate(1, fsrc, fdest);
    } else if (mode == 3) {
        if (fdest == NULL) { fprintf(stderr, "ERROR: changelog merge undefined --dest\n"); exit(1); }
        if (changelog == NULL) { fprintf(stderr, "ERROR: merge undefined changelog"); exit(1); }
        changelog_merge(changelog, fdest, ilut);
    } else if (mode == 4) {
        if (fdest == NULL) { fprintf(stderr, "ERROR: undefined --dest\n"); exit(1); }
        perform_lookups(do_check, fdest, start_option, argc, argv);
    } else if (mode == 5) {
        if (fdest == NULL) { fprintf(stderr, "ERROR: undefined --dest\n"); exit(1); }
        perform_deletes(do_check, fdest, start_option, argc, argv);
    }

    if (ilut)  os_inode_lut_destroy(ilut);

fprintf(stderr, "EXITING-----------------------------\n");
    if (lio_mode) lio_shutdown();

    return(0);
}