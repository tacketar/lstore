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
//***********************************************************************
// Warmer helper definitions
//***********************************************************************

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <rocksdb/c.h>
#include <stdio.h>
#include <time.h>
#include <lio/ex3.h>
#include <lio/os.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include "warmer_helpers.h"

//=========================================================================
//   Generic routines for opening/closing a RocksDB
//=========================================================================

//****************************************************************************
// get_partitions - Retreives the partition info for the DB
//****************************************************************************

int get_partitions(char *prefix)
{
    int n;
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/n_partitions", prefix);

    fd = fopen(path, "r");
    if (fd == NULL) {
        return(-1);
    }

    n = -1;
    if (fscanf(fd, "%d", &n) == 0) n = -1;
    fclose(fd);

    return(n);
}

//****************************************************************************
// put_partitions - Stores the partition info for the DB
//****************************************************************************

void put_partitions(char *prefix, int n)
{
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/n_partitions", prefix);

    fd = fopen(path, "w");
    if (fd == NULL) {
        fprintf(stderr, "ERROR: Unable to create partitions file!. path=%s\n", path); fflush(stderr);
        exit(1);
    }

    fprintf(fd, "%d", n);
    fclose(fd);
}

//*************************************************************************
// open_a_db - Opens a rocksDB database
//*************************************************************************

warm_db_t *open_a_db(char *db_path, rocksdb_comparator_t *cmp, int mode)
{
    warm_db_t *db;
    rocksdb_options_t *opts, *opts2;
    char *errstr = NULL;

    tbx_type_malloc_clear(db, warm_db_t, 1);

    if ((mode == DB_OPEN_WIPE_CLEAN) || ( mode == DB_OPEN_EXISTS) || (mode == DB_OPEN_CREATE_ONLY)) { //** Wipe the DB if requested or make sure it existst
        opts2 = rocksdb_options_create();
        rocksdb_options_set_error_if_exists(opts2, 1);
        rocksdb_options_set_create_if_missing(opts2, 1);

        db->db = rocksdb_open(opts2, db_path, &errstr);
        if (errstr != NULL) {  //** It already exists
            free(errstr);
            errstr = NULL;

            if (mode == DB_OPEN_WIPE_CLEAN) { //** Remove it
                if (errstr != NULL) {  //** Got an error so just kick out
                    fprintf(stderr, "ERROR: Failed removing %s for fresh DB. DB error:%s\n", db_path, errstr); fflush(stderr);
                    exit(1);
                }
                db->db = NULL;
            } else if (mode == DB_OPEN_CREATE_ONLY) {  //** Already there
                fprintf(stderr, "ERROR: Already exists and DB_OPEN_CREATE_ONLY is set!  DB:%s\n", db_path); fflush(stderr);
                exit(1);
            }
        }

        if (db->db) {  //** Close it if needed
            rocksdb_cancel_all_background_work(db->db, 1);
            rocksdb_close(db->db);
        }

        rocksdb_options_destroy(opts2);
    }

    //** Try opening it for real
    opts = rocksdb_options_create();
    if (cmp) rocksdb_options_set_comparator(opts, cmp);
    if ((mode == DB_OPEN_CREATE_ONLY) || (mode == DB_OPEN_CREATE_IF_NEEDED) || (mode == DB_OPEN_WIPE_CLEAN)) rocksdb_options_set_create_if_missing(opts, 1);

    db->db = rocksdb_open(opts, db_path, &errstr);
    if (errstr != NULL) {  //** An Error occured
        fprintf(stderr, "ERROR: Failed Opening/Creating %s. DB error:%s\n", db_path, errstr);
        exit(1);
    }

    rocksdb_options_destroy(opts);

    db->wopt = rocksdb_writeoptions_create();
    db->ropt = rocksdb_readoptions_create();
    db->cmp = cmp;

    return(db);
}

//*************************************************************************
// create_a_db - Creates a rocksDB database using the given path
//*************************************************************************

warm_db_t *create_a_db(char *db_path, rocksdb_comparator_t *cmp)
{
    return(open_a_db(db_path, cmp, DB_OPEN_WIPE_CLEAN));
}

//*************************************************************************
// close_a_db - Closes a rocksDB database
//*************************************************************************

void close_a_db(warm_db_t *db)
{
    rocksdb_cancel_all_background_work(db->db, 1);
    rocksdb_close(db->db);
    rocksdb_writeoptions_destroy(db->wopt);
    rocksdb_readoptions_destroy(db->ropt);
    free(db);
}

//=========================================================================
//  Routines used for generating the final warming DBs and used by warmer_query.
//=========================================================================

//*************************************************************************
// warm_put_inode - Puts an entry in the inode DB
//*************************************************************************

int warm_put_inode(warm_db_t *db, ex_id_t inode, int state, int nfailed, char *name)
{
    unsigned char buf[OS_PATH_MAX + 3*10];
    char *errstr = NULL;
    int n;

    n = 0;
    n += tbx_zigzag_encode(0, buf + n);        //** Version
    n += tbx_zigzag_encode(state, buf + n);    //** State
    n += tbx_zigzag_encode(nfailed, buf + n);  //** nFailed

    strncpy((char *)(&buf[n]), name, sizeof(buf)-n);  //** File name
    buf[sizeof(buf)-1] = 0;  //** Force a NULL terminated string
    n += strlen((const char *)buf+n);

    rocksdb_put(db->db, db->wopt, (const char *)&inode, sizeof(ex_id_t), (const char *)buf, n, &errstr);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// warm_parse_inode - Pareses an entry from the inode DB
//      On error 1 is returned and 0 is represents success
//*************************************************************************

int warm_parse_inode(char *sbuf, int bufsize, int *state, int *nfailed, char **name)
{
    unsigned char *ubuf = (unsigned char *)sbuf;
    int64_t n64;
    int n, version, ds;

    n = 0;
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); version = n64;  //** Version
    if (version  != 0) return(1);
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); *state = n64;   //** State
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); *nfailed = n64; //** nFailed

    n64 = bufsize - n;
    ds = (n64 < OS_PATH_MAX) ? n64+1 : OS_PATH_MAX+1;
    tbx_type_malloc(*name, char, ds);
    strncpy(*name, sbuf+n, ds-1);  //** File name
    (*name)[ds-1] = 0;  //** MAke sure it's NULL terminated

    return(0);
}

//*************************************************************************
// warm_put_rid - Puts an entry in the RID DB
//*************************************************************************

int warm_put_rid(warm_db_t *db, char *rid, ex_id_t inode, ex_off_t nbytes, int state)
{
    char *errstr = NULL;
    char *key;
    unsigned char buf[4*16];
    int n, klen;

    klen = strlen(rid) + 1 + 20 + 1;
    tbx_type_malloc(key, char, klen);
    klen = sprintf(key, "%s|" XIDT, rid, inode) + 1;

    n = 0;
    n += tbx_zigzag_encode(0, buf + n);        //** Version
    n += tbx_zigzag_encode(inode, buf + n);    //** Inode
    n += tbx_zigzag_encode(nbytes, buf + n);   //** size
    n += tbx_zigzag_encode(state, buf + n);    //** state

    rocksdb_put(db->db, db->wopt, key, klen, (const char *)buf, n, &errstr);

    free(key);

    if (errstr != NULL) {
        log_printf(0, "ERROR: %s\n", errstr);
        free(errstr);
    }

    return((errstr == NULL) ? 0 : 1);
}

//*************************************************************************
// warm_parse_rid - Pareses an entry from the RID DB
//      On error 1 is returned and 0 is represents success
//*************************************************************************

int warm_parse_rid(char *sbuf, int bufsize, ex_id_t *inode, ex_off_t *nbytes, int *state)
{
    unsigned char *ubuf = (unsigned char *)sbuf;
    int64_t n64;
    int n, version;

    n = 0;
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); version = n64;  //** Version
    if (version != 0) return(1);
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); *inode = n64;   //** Inode
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); *nbytes = n64;  //** size
    n += tbx_zigzag_decode(ubuf + n, bufsize - n, &n64); *state = n64;   //** State

    return(0);
}



//*************************************************************************
//  create_results_part__db - Creates the results DB partition using the given base directory
//*************************************************************************

warm_results_db_part_t *create_results_part_db(char *db_base, int n_part)
{
    char *db_path;
    warm_results_db_part_t *p;

    tbx_type_malloc_clear(p, warm_results_db_part_t, 1);

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); p->inode = create_a_db(db_path, NULL);
    sprintf(db_path, "%s/rid", db_base); p->rid = create_a_db(db_path, NULL);
    free(db_path);

    return(p);
}

//*************************************************************************
// create_results_db - Creates a DB for the warmer results
//*************************************************************************

warm_results_db_t *create_results_db(char *db_base, int n_partitions)
{
    int i;
    char *db_path;
    warm_results_db_t *r;

    tbx_type_malloc_clear(r, warm_results_db_t, 1);
    tbx_type_malloc_clear(r->p, warm_results_db_part_t *, n_partitions);
    r->n_partitions = n_partitions;

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1 + 20);
    for (i=0; i<r->n_partitions; i++) {
        sprintf(db_path, "%s/%d", db_base, i);
        if (mkdir(db_path, 0) != 0) {   //** Kick out if we fail
            fprintf(stderr, "ERROR: Unable to make partition directory! directory: %s\bn", db_path); fflush(stderr);
            abort();
        }

        r->p[i] = create_results_part_db(db_path, i);
    }
    free(db_path);

    //** Store the partitions
    put_partitions(db_base, n_partitions);

    return(r);
}

//*************************************************************************
//  open_results_part__db - Open the results DB partition using the given base directory
//*************************************************************************

warm_results_db_part_t *open_results_part_db(char *db_base, int mode, int n_part)
{
    char *db_path;
    warm_results_db_part_t *p;

    tbx_type_malloc_clear(p, warm_results_db_part_t, 1);

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); p->inode = open_a_db(db_path, NULL, mode);
    sprintf(db_path, "%s/rid", db_base); p->rid = open_a_db(db_path, NULL, mode);
    free(db_path);

    return(p);
}

//*************************************************************************
//  open_results_db - Opens the warmer results DBs
//*************************************************************************

warm_results_db_t *open_results_db(char *db_base, int mode)
{
    char *db_path;
    warm_results_db_t *r;
    int i;

    i = get_partitions(db_base);
    if (i <= 0) {
        fprintf(stderr, "ERROR: Unable to get partition info! directory: %s\bn", db_base); fflush(stderr);
        abort();
    }

    tbx_type_malloc_clear(r, warm_results_db_t, 1);
    r->n_partitions = i;
    tbx_type_malloc_clear(r->p, warm_results_db_part_t *, r->n_partitions);

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1 + 20);
    for (i=0; i<r->n_partitions; i++) {
        sprintf(db_path, "%s/%d", db_base, i);
        r->p[i] = open_results_part_db(db_path, mode, i);
    }
    free(db_path);

    return(r);
}

//*************************************************************************
//  close_results_db_part - Closes the results DB parttion
//*************************************************************************

void close_results_db_part(warm_results_db_part_t *p)
{
    close_a_db(p->inode);
    close_a_db(p->rid);

    free(p);
}


//*************************************************************************
//  close_results_db - Closes the results DB
//*************************************************************************

void close_results_db(warm_results_db_t *r)
{
    int i;

    for (i=0; i<r->n_partitions; i++) {
        close_results_db_part(r->p[i]);
    }
    free(r->p);
    free(r);
}

//=========================================================================
// Routines for generating the PREP DBs
//=========================================================================

//****************************************************************************
// inode_compare_op - Comparator routine for the inode DB
//     Order is: {mod(inode), inode}
//****************************************************************************

int inode_compare_op(void *arg, const char *a, size_t alen, const char *b, size_t blen)
{
    ex_id_t aid = *(ex_id_t *)a;
    ex_id_t bid = *(ex_id_t *)b;

    if (aid > bid) {
        return(1);
    } else if (aid < bid) {
        return(-1);
    }

    //** If made it here then the keys are identical
    return(0);
}

//****************************************************************************

void default_compare_destroy(void *arg) { return; }
const char *inode_compare_name(void *arg) { return("INODE"); }

//****************************************************************************
// rid_compare_op - Comparator routine for the RID prep DB
//     Order is: {mod(inode), rid, inode, mcap}
//****************************************************************************

int rid_compare_op(void *arg, const char *aptr, size_t alen, const char *bptr, size_t blen)
{
    rid_prep_key_t *a = (rid_prep_key_t *)aptr;
    rid_prep_key_t *b = (rid_prep_key_t *)bptr;
    int n;

    //** Compare based on the RID
    n = strcmp(a->strings, b->strings);
    if (n != 0) return(n);


    //** If made it here then the RIDs are the same so check using the ID
    if (a->id > b->id) {
        return(1);
    } else if (a->id < b->id) {
        return(-1);
    }

    //** If made it here then we just compare the mcaps
    return(strcmp(a->strings + a->rid_len + 2, b->strings + b->rid_len + 2));
}

//****************************************************************************

const char *rid_compare_name(void *arg) { return("RID"); }


//****************************************************************************
// warm_prep_get_timestamp - Retreives the timestamp info for the DB
//****************************************************************************

int warm_prep_get_timestamp(char *prefix, int *year, int *month, int *day, int *line)
{
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/timestamp", prefix);

    fd = fopen(path, "r");
    if (fd == NULL) {
        return(-1);
    }

    if (fscanf(fd, "%d-%d-%d-%d", year, month, day, line) != 4) {
        log_printf(0, "Unable to parse timestamp: fname=%s Format should be: %%d-%%d-%%d-%%d (year,month,day,line)\n", path);
        fprintf(stderr, "Unable to parse timestamp: fname=%s Format should be: %%d-%%d-%%d-%%d (year,month,day,line)\n", path);
    }
    fclose(fd);

    return(0);
}

//****************************************************************************
// warm_prep_put_timestamp - Stores the timestamp for the DB
//****************************************************************************

void warm_prep_put_timestamp(char *prefix, int year, int month, int day, int line)
{
    FILE *fd;
    char path[OS_PATH_MAX];

    sprintf(path, "%s/timestamp", prefix);

    fd = fopen(path, "w");
    if (fd == NULL) {
        fprintf(stderr, "ERROR: Unable to create timestamp file!. path=%s\n", path); fflush(stderr);
        exit(1);
    }

    fprintf(fd, "%d-%d-%d-%d", year, month, day, line);
    fclose(fd);
}

//****************************************************************************
// open_prep_db_part - Opens a prep DB partition
//****************************************************************************

warm_prep_db_part_t *open_prep_db_part(char *db_part_dir, int mode)
{
    warm_prep_db_part_t *p;
    char path[OS_PATH_MAX];
    struct stat sbuf;

    tbx_type_malloc_clear(p, warm_prep_db_part_t, 1);

    //** Check if we need to make the parent directory for the partition
    if ((mode == DB_OPEN_WIPE_CLEAN) || (mode == DB_OPEN_CREATE_IF_NEEDED) || (mode == DB_OPEN_CREATE_ONLY)) {
        if (stat(db_part_dir, &sbuf) == 0) {
            if (!S_ISDIR(sbuf.st_mode)) {  //** Exists but not a directory so throw an error
                fprintf(stderr, "ERROR: Partition is NOT a directory! directory: %s\bn", db_part_dir); fflush(stderr);
                abort();
            }
        } else { //** Need to go ahead and make the directory
            if (mkdir(db_part_dir, 0) != 0) {   //** Kick out if we fail
                fprintf(stderr, "ERROR: Unable to make partition directory! directory: %s\bn", db_part_dir); fflush(stderr);
                abort();
            }
        }
    }

    p->inode_cmp = rocksdb_comparator_create(p, default_compare_destroy,
        inode_compare_op, inode_compare_name);
    p->rid_cmp = rocksdb_comparator_create(p, default_compare_destroy,
        rid_compare_op, rid_compare_name);

    sprintf(path, "%s/prep_inode", db_part_dir);
    p->inode = open_a_db(path, p->inode_cmp, mode);
    sprintf(path, "%s/prep_rid", db_part_dir);
    p->rid = open_a_db(path, p->rid_cmp, mode);
    sprintf(path, "%s/prep_write", db_part_dir);
    p->write_errors = open_a_db(path, NULL, mode);
    sprintf(path, "%s/prep_missing_exnode", db_part_dir);
    p->missing_exnode_errors = open_a_db(path, NULL, mode);

    return(p);
}

//****************************************************************************
// open_prep_db - Opens a prep DB
//****************************************************************************

warm_prep_db_t *open_prep_db(char *db_dir, int mode, int n_partitions)
{
    warm_prep_db_t *wdb;
    int i;
    char path[OS_PATH_MAX];

    tbx_type_malloc_clear(wdb, warm_prep_db_t, 1);

    wdb->n_partitions = (n_partitions > 0) ? n_partitions : get_partitions(db_dir);
    if (wdb->n_partitions <= 0) {
        fprintf(stderr, "ERROR: n_partitions=%d DB=%s\n", wdb->n_partitions, db_dir); fflush(stderr);
        exit(1);
    }

    tbx_type_malloc_clear(wdb->p, warm_prep_db_part_t *, wdb->n_partitions);
    for (i=0; i<wdb->n_partitions; i++) {
        sprintf(path, "%s/%d", db_dir, i);
        wdb->p[i] = open_prep_db_part(path, mode);
    }

    return(wdb);
}

//****************************************************************************
// open_prep_db - Opens a prep DB
//****************************************************************************

warm_prep_db_t *create_prep_db(char *db_dir, int n_partitions)
{
    warm_prep_db_t *wdb;
    time_t now;
    struct tm tm_now;

    wdb = open_prep_db(db_dir, DB_OPEN_CREATE_ONLY, n_partitions);

    //** Store the partitions
    put_partitions(db_dir, n_partitions);

    //** Store the timestamp
    now = time(NULL);
    localtime_r(&now, &tm_now);
    warm_prep_put_timestamp(db_dir, 1900+tm_now.tm_year, tm_now.tm_mon+1, tm_now.tm_mday, 0);

    return(wdb);
}

//****************************************************************************
// close_prep_db_pasrt - Close a prep DB partition
//****************************************************************************

void close_prep_db_part(warm_prep_db_part_t *p)
{
    close_a_db(p->inode);
    close_a_db(p->rid);
    close_a_db(p->write_errors);
    close_a_db(p->missing_exnode_errors);

    rocksdb_comparator_destroy(p->inode_cmp);
    rocksdb_comparator_destroy(p->rid_cmp);

    free(p);
}

//****************************************************************************
// close_prep_db - Close a prep DB
//****************************************************************************

void close_prep_db(warm_prep_db_t *wdb)
{
    int i;

    for (i=0; i<wdb->n_partitions; i++) {
        close_prep_db_part(wdb->p[i]);
    }
    free(wdb->p);
    free(wdb);
}

//****************************************************************************
// prep_warm_rid_db_put - Stores an entry in the RID prep DB
//****************************************************************************

void prep_warm_rid_db_put(warm_prep_db_t *wdb, ex_id_t inode, char *rid_key, char *mcap, ex_off_t nbytes)
{
    int nbuf = 100*1024;
    char rbuf[nbuf];
    int n_rid, n_mcap, n, modulo;
    rid_prep_key_t *rkey;
    char *errstr;

    n_rid = strlen(rid_key);
    n_mcap = strlen(mcap);
    n = sizeof(rid_prep_key_t) + n_rid + n_mcap + 2;
    if (n > nbuf) {
        rkey = malloc(n);
    } else {
        rkey = (rid_prep_key_t *)rbuf;
    }
    rkey->id = inode;
    rkey->rid_len = n_rid;
    rkey->mcap_len = n_mcap;
    memcpy(rkey->strings, rid_key, n_rid+1);
    memcpy(rkey->strings + n_rid+1, mcap, n_mcap+1);

    errstr = NULL;
    modulo = inode % wdb->n_partitions;
    rocksdb_put(wdb->p[modulo]->rid->db, wdb->p[modulo]->rid->wopt, (const char *)rkey, n, (const char *)&nbytes, sizeof(ex_off_t), &errstr);
    if (rkey != (rid_prep_key_t *)rbuf) free(rkey);

    if (errstr != NULL) {
        log_printf(0, "ERROR: RID DB put id=" LU " rid_key=%s error=%s\n", inode, rid_key, errstr);
        free(errstr);
    }
    return;
}

//****************************************************************************
// prep_warm_rid_db_parse - Parses the RID prep DB key entry
//****************************************************************************

int prep_warm_rid_db_parse(warm_prep_db_t *wdb, char *buffer, int blen, ex_id_t *inode, char **rid_key, char **mcap)
{
    rid_prep_key_t *rkey = (rid_prep_key_t *)buffer;

    *inode = rkey->id;
    *rid_key = strndup(rkey->strings, rkey->rid_len);
    *mcap = strndup(rkey->strings + rkey->rid_len + 1, rkey->mcap_len);
    return(0);
}

//****************************************************************************
// prep_warm_inode_db_put - Stores an entry in the inode prep DB
//****************************************************************************

void prep_warm_inode_db_put(warm_prep_db_t *wdb, ex_id_t inode, char *fname, int n_allocs, apr_hash_t *rids, int free_rids)
{
    int nbuf = 100*1024;
    char rbuf[nbuf];
    inode_value_t *v;
    char *rid_key;
    char *rid_buffer;
    char *errstr;
    int n_rids, ntotal, n, n_v;
    int fname_len, modulo;
    apr_hash_index_t *hi;
    apr_ssize_t hlen;

    //** Figure out how many RIDs and also how much space is needed to store it
    n_rids = apr_hash_count(rids);
    ntotal = n_rids * 1025;
    if (ntotal>nbuf) {
        tbx_type_malloc(rid_buffer, char, ntotal);
    } else {
        rid_buffer = rbuf;
    }

    n = 0;
    for (hi=apr_hash_first(NULL, rids); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&rid_key, &hlen, NULL);
        strcpy(rid_buffer + n, rid_key);
        n += hlen + 1;
    }

    //** Form the strucure
    fname_len = strlen(fname);
    n_v = sizeof(inode_value_t) + fname_len+1 + n;
    v = malloc(n_v);
    v->n_allocs = n_allocs;
    v->n_rids = n_rids;
    v->fname_len = fname_len;
    v->rid_list_len = n;
    strcpy(v->strings, fname);
    if (n > 0) memcpy(v->strings + fname_len+1, rid_buffer, n);

    //** Store it
    errstr = NULL;
    modulo = inode % wdb->n_partitions;
    rocksdb_put(wdb->p[modulo]->inode->db, wdb->p[modulo]->inode->wopt, (const char *)&inode, sizeof(ex_id_t), (const char *)v, n_v, &errstr);
    free(v);

    if (errstr != NULL) {
        log_printf(0, "ERROR: inode DB put id=" LU " fname=%s error=%s\n", inode, fname, errstr);
        free(errstr);
    }

    if (rbuf != rid_buffer) free(rid_buffer);

    return;
}



//*************************************************************************
//  update_warm_prep_db - Parses the file information and adds it to the DBs
//*************************************************************************

void update_warm_prep_db(tbx_log_fd_t *ifd, warm_prep_db_t *wdb, char *fname, char **vals, int *v_size)
{
    tbx_inip_file_t *fd;
    tbx_inip_group_t *g;
    warm_prep_db_part_t *p;
    char *etext, *mcap, *errstr;
    ex_id_t inode, *iptr;
    int n_allocs, modulo;
    ex_off_t nbytes;
    size_t ns;
    apr_pool_t *mpool;
    apr_hash_t *rids;
    apr_hash_index_t *hi;
    apr_ssize_t hlen;

    log_printf(15, "fname=%s\n", fname);

    //** Make the misc structs
    apr_pool_create(&mpool, NULL);
    rids = apr_hash_make(mpool);
    n_allocs = 0;

    //** Translate the attributes with some sanity checking

    //** inode
    if (v_size[1] > 0) {
        sscanf(vals[1], XIDT, &inode);
        free(vals[1]);
        v_size[1] = 0;
    } else {
        info_printf(ifd, 0, "ERROR: Missing inode! fname=%s\n", fname);
        if (v_size[0] > 0) {
            free(vals[0]);
            v_size[0] = 0;
        }
        return;
    }

    modulo = inode % wdb->n_partitions;
    p = wdb->p[modulo];

    //** write_error
    if (v_size[2] > 0) {
        errstr = NULL;
info_printf(ifd, 0, "WRITE_ERROR: fname=%s inode=" XIDT " mod=%d\n", fname, inode, modulo);
        info_printf(ifd, 0, "WRITE_ERROR: fname=%s inode=" XIDT "\n", fname, inode);
        rocksdb_put(p->write_errors->db, p->write_errors->wopt, (const char *)&inode, sizeof(inode), NULL, 0, &errstr);
        free(vals[2]);
        v_size[2] = 0;
        if (errstr) {
            info_printf(ifd, 0, "ERROR: RocksDB error putting write_error entry! fname=%s errstr=%s\n", fname, errstr);
            free(errstr);
        }
    }

    //** Now the exnode
    if (v_size[0] <= 0) {
        errstr = NULL;
        info_printf(ifd, 0, "MISSING_EXNODE_ERROR: fname=%s inode=" XIDT "\n", fname, inode);
        rocksdb_put(p->missing_exnode_errors->db, p->missing_exnode_errors->wopt, (const char *)&inode, sizeof(inode), NULL, 0, &errstr);
        free(vals[2]);
        v_size[2] = 0;
        if (errstr) {
            info_printf(ifd, 0, "ERROR: RocksDB error putting missing_exnode_error entry! fname=%s errstr=%s\n", fname, errstr);
            free(errstr);
        }
        goto no_exnode;
    } else {  //** Got an exnode so check if we need to clear it
        errstr = NULL;
        iptr = (ex_id_t *)rocksdb_get(p->missing_exnode_errors->db, p->missing_exnode_errors->ropt, (const char *)&inode, sizeof(ex_id_t), &ns, &errstr);
        if (errstr) free(errstr);
        if (iptr != NULL) { //** Got a match so delete
            errstr = NULL;
            rocksdb_delete(p->missing_exnode_errors->db, p->missing_exnode_errors->wopt, (const char *)&inode, sizeof(ex_id_t), &errstr);
            if (errstr) {
                info_printf(ifd, 0, "ERROR: RocksDB error removing missing_exnode_error entry! fname=%s errstr=%s\n", fname, errstr);
                free(errstr);
            }
        }
    }

    fd = tbx_inip_string_read(vals[0], 1);

    g = tbx_inip_group_first(fd);
    while (g) {
        if (strncmp(tbx_inip_group_get(g), "block-", 6) == 0) { //** Got a data block
            //** Get the manage cap first
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "manage_cap", NULL);
            if (!etext) {
                info_printf(ifd, 1, "MISSING_MCAP_ERROR: fname=%s  block=%s\n", fname, tbx_inip_group_get(g));
                goto next;
            }
            mcap = tbx_stk_unescape_text('\\', etext);
            free(etext);

            //** Get the RID key
            etext = tbx_inip_get_string(fd, tbx_inip_group_get(g), "rid_key", NULL);
            if (etext == NULL) {
                etext = strdup("catchall");  //** Dummy placeholder
            }
            apr_hash_set(rids, etext, APR_HASH_KEY_STRING, "DUMMY");

            //** Get the data size and update the counts
            nbytes = tbx_inip_get_integer(fd, tbx_inip_group_get(g), "max_size", 0);
            n_allocs++;

            //** Add the entry to the RID DB
            prep_warm_rid_db_put(wdb, inode, etext, mcap, nbytes);
            free(mcap);
        }
next:
        g = tbx_inip_group_next(g);
    }

    tbx_inip_destroy(fd);
    v_size[0] = 0;
    free(vals[0]);

no_exnode:
    //** Store the inode entry
    prep_warm_inode_db_put(wdb, inode, fname, n_allocs, rids, 1);

    //** Cleanup
    for (hi=apr_hash_first(NULL, rids); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&etext, &hlen, NULL);
        free(etext);
    }
    apr_pool_destroy(mpool);
}
