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

#include <rocksdb/c.h>
#include <stdio.h>
#include <lio/ex3.h>
#include <lio/os.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>
#include "warmer_helpers.h"

//=========================================================================
//   Generic routines for opening/closing a RocksDB
//=========================================================================

//*************************************************************************
// open_a_db - Opens a rocksDB database
//*************************************************************************

warm_db_t *open_a_db(char *db_path, rocksdb_comparator_t *cmp, int wipe_clean)
{
    warm_db_t *db;
    rocksdb_options_t *opts, *opts2;
    char *errstr = NULL;

    tbx_type_malloc_clear(db, warm_db_t, 1);

    if (wipe_clean != 0) { //** Wipe the DB if requested
        opts2 = rocksdb_options_create();
        rocksdb_options_set_error_if_exists(opts2, 1);

        db->db = rocksdb_open(opts2, db_path, &errstr);
        if (errstr != NULL) {  //** It already exists so need to remove it first
            free(errstr);
            errstr = NULL;

            //** Remove it
            rocksdb_destroy_db(opts2, db_path, &errstr);
            if (errstr != NULL) {  //** Got an error so just kick out
                fprintf(stderr, "ERROR: Failed removing %s for fresh DB. DB error:%s\n", db_path, errstr);
                exit(1);
            }
        } else {  //** Close it
            rocksdb_cancel_all_background_work(db->db, 1);
            rocksdb_close(db->db);
        }
        rocksdb_options_destroy(opts2);
    }

    //** Try opening it for real
    opts = rocksdb_options_create();
    if (cmp) rocksdb_options_set_comparator(opts, cmp);
    rocksdb_options_set_create_if_missing(opts, 1);

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
// create_a_db - Creates  a rocksDB database using the given path
//*************************************************************************

warm_db_t *create_a_db(char *db_path, rocksdb_comparator_t *cmp)
{
    return(open_a_db(db_path, cmp, 1));
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
//  create_warm_db - Creates the DBs using the given base directory
//*************************************************************************

void create_warm_db(char *db_base, warm_db_t **inode_db, warm_db_t **rid_db)
{
    char *db_path;

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = create_a_db(db_path, NULL);
    sprintf(db_path, "%s/rid", db_base); *rid_db = create_a_db(db_path, NULL);
    free(db_path);
}

//*************************************************************************
//  open_warm_db - Opens the warmer DBs
//*************************************************************************

int open_warm_db(char *db_base, warm_db_t **inode_db, warm_db_t **rid_db)
{
    char *db_path;

    tbx_type_malloc(db_path, char, strlen(db_base) + 1 + 5 + 1);
    sprintf(db_path, "%s/inode", db_base); *inode_db = open_a_db(db_path, NULL, 0);
    if (inode_db == NULL) { free(db_path); return(1); }

    sprintf(db_path, "%s/rid", db_base); *rid_db = open_a_db(db_path, NULL, 0);
    if (rid_db == NULL) { free(db_path); return(2); }

    free(db_path);
    return(0);
}

//*************************************************************************
//  close_warm_db - Closes the DBs
//*************************************************************************

void close_warm_db(warm_db_t *inode, warm_db_t *rid)
{
    close_a_db(inode);
    close_a_db(rid);
}

