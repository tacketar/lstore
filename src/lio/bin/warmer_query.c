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

#include <assert.h>
#include <rocksdb/c.h>
#include <apr_pools.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <tbx/type_malloc.h>
#include <tbx/string_token.h>
#include <tbx/stack.h>

#include <lio/authn.h>
#include <lio/ds.h>
#include <lio/ex3.h>
#include <lio/lio.h>
#include <lio/os.h>
#include <lio/rs.h>

#include "warmer_helpers.h"

typedef struct {
    char *rid;
    ex_off_t nbytes;
    ex_off_t ngood;
    ex_off_t nbad;
} rid_summary_t;

//*************************************************************************
// warmer_inode_summary_part - Summarizes the inode DB partition
//*************************************************************************

void warmer_inode_summary_part(warm_db_t *inode_db, ex_off_t *nfiles, ex_off_t *nwrite, ex_off_t *ngood, ex_off_t *nbad, ex_off_t *nbad_caps)
{
    rocksdb_iterator_t *it;
    char *buf;
    size_t nbytes;
    int state, nfailed;
    char *errstr;
    char *name;

    //** Create the iterator
    it = rocksdb_create_iterator(inode_db->db, inode_db->ropt);
    rocksdb_iter_seek_to_first(it);

    while (rocksdb_iter_valid(it) > 0) {
        buf = (char *)rocksdb_iter_value(it, &nbytes);
        if (nbytes == 0) { goto next; }

        if (warm_parse_inode(buf, nbytes, &state, &nfailed, &name) != 0) { goto next; }

        (*nfiles)++;
        if (state & WFE_SUCCESS) {
            (*ngood)++;
        } else if (state & WFE_FAIL) {
            (*nbad)++;
        }
        if (state & WFE_WRITE_ERR) { (*nwrite)++; }

        (*nbad_caps) += nfailed;
        free(name);
next:
        rocksdb_iter_next(it);

        errstr = NULL;
        rocksdb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// warmer_inode_summary - Summarizes the inode DB
//*************************************************************************

void warmer_inode_summary(warm_results_db_t *r, ex_off_t *nfiles, ex_off_t *nwrite, ex_off_t *ngood, ex_off_t *nbad, ex_off_t *nbad_caps)
{
    int i;

    *nfiles = 0;
    *nwrite = 0;
    *ngood = 0;
    *nbad = 0;
    *nbad_caps = 0;

    for (i=0; i<r->n_partitions; i++) {
        warmer_inode_summary_part(r->p[i]->inode, nfiles, nwrite, ngood, nbad, nbad_caps);
    }
}

//*************************************************************************
// warmer_rid_summary_part - Generate list based on the RID partition
//*************************************************************************

void warmer_rid_summary_part(warm_db_t *rid_db, tbx_stack_t *rid_stack, rid_summary_t *totals)
{
    rocksdb_iterator_t *it;
    size_t nbytes;
    ex_off_t bsize;
    char *buf;
    int n;
    int state;
    ex_id_t inode;
    char *errstr, *rec_rid, *drid, *last;
    const char *key;
    rid_summary_t *rid;

    //** Create the iterator
    it = rocksdb_create_iterator(rid_db->db, rid_db->ropt);
    rocksdb_iter_seek_to_first(it);

    bsize = 0;
    rid = NULL;
    while (rocksdb_iter_valid(it) > 0) {
        key = rocksdb_iter_key(it, &nbytes);
        drid = strdup(key);
        rec_rid = tbx_stk_string_token(drid, "|", &last, &n);
        sscanf(tbx_stk_string_token(NULL, "|", &last, &n), XIDT, &inode);
        if ((rid==NULL) || (strcmp(rec_rid, rid->rid) != 0)) { //** New RID
            if (rid) {
                tbx_stack_push(rid_stack, rid);
                totals->nbytes += rid->nbytes;
                totals->ngood += rid->ngood;
                totals->nbad += rid->nbad;
            }
            tbx_type_malloc_clear(rid, rid_summary_t, 1);
            rid->rid = rec_rid;
        } else {
            free(drid);
        }

        buf = (char *)rocksdb_iter_value(it, &nbytes);
        if (warm_parse_rid(buf, nbytes, &inode, &bsize, &state) != 0) { goto next; }

        rid->nbytes += bsize;
        if (state & WFE_SUCCESS) {
            rid->ngood++;
        } else {
            rid->nbad++;
        }

next:
        rocksdb_iter_next(it);

        errstr = NULL;
        rocksdb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }
    }

    if (rid) {
        tbx_stack_push(rid_stack, rid);
        totals->nbytes += rid->nbytes;
        totals->ngood += rid->ngood;
        totals->nbad += rid->nbad;
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// warmer_rid_summary - Generate list based on the RID
//*************************************************************************

void warmer_rid_summary(warm_results_db_t *r, tbx_stack_t *rid_stack, rid_summary_t *totals)
{
    int i;

    for (i=0; i<r->n_partitions; i++) {
        warmer_rid_summary_part(r->p[i]->rid, rid_stack, totals);
    }
}

//*************************************************************************
// warmer_summary - Print the DB summary
//*************************************************************************

void warmer_summary(FILE *fd, warm_results_db_t *r)
{
    tbx_stack_t *rids;
    char ppbuf[128];
    ex_off_t nfiles, nwrite, ngood, nbad, nbad_caps, total;
    rid_summary_t totals;
    rid_summary_t *rid;

    //** Get the inode/file summary
    warmer_inode_summary(r, &nfiles, &nwrite, &ngood, &nbad, &nbad_caps);

    //** Print it
    fprintf(fd, "Inode Summary ---------  Files: " XOT " Success: " XOT " Failed: " XOT " Failed caps: " XOT " Write errors: " XOT "\n", nfiles, ngood, nbad,  nbad_caps, nwrite);
    fprintf(fd, "\n");

    //** Now do the same for the RIDs
    rids = tbx_stack_new();
    memset(&totals, 0, sizeof(totals));
    warmer_rid_summary(r, rids, &totals);

    fprintf(fd, "                                                              Allocations\n");
    fprintf(fd, "                 RID Key                    Size       Total       Good         Bad\n");
    fprintf(fd, "----------------------------------------  ---------  ----------  ----------  ----------\n");

    while ((rid = tbx_stack_pop(rids)) != NULL) {
        total = rid->ngood + rid->nbad;
        fprintf(fd, "%-40s  %s  %10" PXOT "  %10" PXOT "  %10" PXOT "\n", rid->rid,
            tbx_stk_pretty_print_double_with_scale_full(1024, (double)rid->nbytes, ppbuf, 1),
            total, rid->ngood, rid->nbad);
        free(rid->rid);
        free(rid);
    }
    tbx_stack_free(rids, 0);

    fprintf(fd, "----------------------------------------  ---------  ----------  ----------  ----------\n");
    total = totals.ngood + totals.nbad;
    fprintf(fd, "%-40s  %s  %10" PXOT "  %10" PXOT "  %10" PXOT "\n", "Totals",
        tbx_stk_pretty_print_double_with_scale_full(1024, (double)totals.nbytes, ppbuf, 1),
        total, totals.ngood, totals.nbad);

}

//*************************************************************************
// warmer_query_inode_part - Generate list based on inode partition
//*************************************************************************

void warmer_query_inode_part(warm_db_t *inode_db, int mode, int fonly)
{
    rocksdb_iterator_t *it;
    char *buf;
    size_t nbytes;
    int we;
    int state, nfailed;
    char *errstr;
    char *name;

    //** Create the iterator
    it = rocksdb_create_iterator(inode_db->db, inode_db->ropt);
    rocksdb_iter_seek_to_first(it);

    while (rocksdb_iter_valid(it) > 0) {
        buf = (char *)rocksdb_iter_value(it, &nbytes);
        if (nbytes == 0) { goto next; }

        if (warm_parse_inode(buf, nbytes, &state, &nfailed, &name) != 0) { goto next; }

        if ((state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", name);
            } else {
                we = ((state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|%d|%d\n", name, nfailed, we);
            }
        }

        free(name);
next:
        rocksdb_iter_next(it);

        errstr = NULL;
        rocksdb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }
    }

    //** Cleanup
    rocksdb_iter_destroy(it);
}

//*************************************************************************
// warmer_query_inode - Generate list based on inode
//*************************************************************************

void warmer_query_inode(warm_results_db_t *r, int mode, int fonly)
{
    int i;

    for (i=0; i<r->n_partitions; i++) {
        warmer_query_inode_part(r->p[i]->inode, mode, fonly);
    }
}


//*************************************************************************
// warmer_query_rid_part - Generate list based on the RID partition
//*************************************************************************

ex_off_t warmer_query_rid_part(char *rid_key, warm_db_t *inode_db, warm_db_t *rid_db, int mode, int fonly, ex_off_t total_bytes)
{
    rocksdb_iterator_t *it;
    ex_off_t bytes_found;
    size_t nbytes;
    ex_off_t bsize;
    char *buf;
    int we, n;
    int state, nfailed;
    ex_id_t inode;
    char *errstr, *match, *rec_rid, *drid, *last, *name;
    const char *rid;

    //** Create the iterator
    n = strlen(rid_key) + 1 + 1 + 1;
    tbx_type_malloc(match, char, n);
    n = sprintf(match, "%s|0", rid_key) + 1;

    it = rocksdb_create_iterator(rid_db->db, rid_db->ropt);
    rocksdb_iter_seek(it, match, n);

    bytes_found = 0;
    bsize = 0;
    while (rocksdb_iter_valid(it) > 0) {
        rid = rocksdb_iter_key(it, &nbytes);
        drid = strdup(rid);
        rec_rid = tbx_stk_string_token(drid, "|", &last, &n);
        sscanf(tbx_stk_string_token(NULL, "|", &last, &n), XIDT, &inode);
        if (strcmp(rec_rid, rid_key) != 0) { //** Kick out
            free(drid);
            break;
        }

        buf = (char *)rocksdb_iter_value(it, &nbytes);
        if (warm_parse_rid(buf, nbytes, &inode, &bsize, &state) != 0) { goto next; }
        buf = (char *)rocksdb_get(inode_db->db, inode_db->ropt, (const char *)&inode, sizeof(ex_id_t), &nbytes, &errstr);
        if (nbytes == 0) { goto next; }

        if (warm_parse_inode(buf, nbytes, &state, &nfailed, &name) != 0) {
            free(buf);
            goto next;
        }
        free(buf);

        if ((state & mode) > 0) {
            if (fonly == 1) {
                printf("%s\n", name);
            } else {
                we = ((state & WFE_WRITE_ERR) > 0) ? 1 : 0;
                printf("%s|" XOT "|%d|%d\n", name, bsize, nfailed, we);
            }
        }

        free(name);
next:
        free(drid);
        rocksdb_iter_next(it);

        errstr = NULL;
        rocksdb_iter_get_error(it, &errstr);
        if (errstr != NULL) { printf("ERROR: %s\n", errstr); fflush(stdout); }

        //** Accumulate the space and see if we kick out
        bytes_found += bsize;
        if (total_bytes > 0) {
            if (bytes_found > total_bytes) break;
        }

    }

    //** Cleanup
    rocksdb_iter_destroy(it);
    free(match);

    return(bytes_found);
}

//*************************************************************************
// warmer_query_rid - Generate list based on the RID
//*************************************************************************

void warmer_query_rid(char *rid_key, warm_results_db_t *r, int mode, int fonly, ex_off_t total_bytes)
{
    int i;
    ex_off_t nbytes, nleft;

    nbytes = 0;
    nleft = total_bytes;
    for (i=0; i<r->n_partitions; i++) {
        nbytes += warmer_query_rid_part(rid_key, r->p[i]->inode, r->p[i]->rid, mode, fonly, nleft);
        if (total_bytes > 0) {
            nleft = total_bytes - nbytes;
            if (nleft <= 0) break;
        }
    }
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option, summary;
    int fonly, mode;
    char *db_base = "/lio/log/warm";
    char *rid_key;
    warm_results_db_t *results;
    ex_off_t total_bytes;

    total_bytes = -1;  //** Default to print all files

    if (argc < 2) {
        printf("\n");
        printf("warmer_query -db DB_input_dir [-fonly] [-r rid_key [-b size]] [-w] [-s] [-f]\n");
        printf("    -db DB_input_dir   - Input directory for the DBes. DEfault is %s\n", db_base);
        printf("    -fonly             - Only print the filename\n");
        printf("    -r  rid_key        - Use RID for filtering matches instead of all files\n");
        printf("    -b  size           - Print enough files using this amount of space on the RID.\n");
        printf("                         Units are supported. The default is to print all files.\n");
        printf("    -w                 - Print files containing write errors\n");
        printf("    -s                 - Print files that were sucessfully warmed\n");
        printf("    -f                 - Print files that failed warming\n");
        printf("    --summary          - Print a summary of files and RIDs. Use of this option disables other selection options\n");
        printf("\n");
        printf("If no secondary modifier is provided all files matching the initial filter are printed\n");
        return(1);
    }

    i=1;
    fonly = 0;
    summary = 0;
    mode = 0;
    rid_key = NULL;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db") == 0) { //** DB output base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "--summary") == 0) { //** Summary mode
            i++;
            summary = 1;
        } else if (strcmp(argv[i], "-r") == 0) { //** Use the RID name for selection
            i++;
            rid_key = argv[i];
            i++;
        } else if (strcmp(argv[i], "-b") == 0) { //** Use the RID name for selection
            i++;
            total_bytes = tbx_stk_string_get_integer(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-w") == 0) { //** Filter further by printing only files with write errors
            i++;
            mode |= WFE_WRITE_ERR;
        } else if (strcmp(argv[i], "-s") == 0) { //** Further filter by printing successfully warmed files
            i++;
            mode |= WFE_SUCCESS;
        } else if (strcmp(argv[i], "-f") == 0) { //** Only print failed files
            i++;
            mode |= WFE_FAIL;
        } else if (strcmp(argv[i], "-fonly") == 0) { //** Only print the file name
            i++;
            fonly = 1;
        }

    } while ((start_option < i) && (i<argc));

    //** Default is to print everything
    if (mode == 0) mode = WFE_SUCCESS|WFE_FAIL|WFE_WRITE_ERR;

    //** Open the DBs
    if ((results = open_results_db(db_base, DB_OPEN_EXISTS)) == NULL) {
        printf("ERROR:  Failed opening the warmer DB! path=%s\n", db_base);
        return(1);
    }

    if (summary == 1) {
        warmer_summary(stdout, results);
    } else if (rid_key == NULL) {
        warmer_query_inode(results, mode, fonly);
    } else {
        warmer_query_rid(rid_key, results, mode, fonly, total_bytes);
    }

    //** Close the DBs
    close_results_db(results);

    return(0);
}


