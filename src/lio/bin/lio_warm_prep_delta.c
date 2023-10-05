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
#include <lio/notify.h>
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

//#include "os.h"
#include "warmer_helpers.h"

#define CL_STATE_ADD    1
#define CL_STATE_DEL    2

typedef struct {   //** Structure for storing the interesting log entries
    char *fname;
    char *fname2;
    char *vals[3];
    int v_size[3];
    ex_id_t inode;
    int state;
    int write_error_state;   //** 0=not changed, 1=WE set, 2=WE cleared
} clog_obj_info_t;

typedef struct { //** Parsed interesting clog entry
    char *fname;
    char *fname2;
    int clog_type;
    int ftype;
    ex_id_t inode;
} clog_entry_t;

void warm_object_delete(warm_prep_db_t *wdb, clog_obj_info_t *obj);

//*************************************************************************
// parse_date - Parses the date supplied. OF the form YYYY-MM-DD
//*************************************************************************

void parse_date(char *text, int *year, int *month, int *day)
{
    *year = *month = *day = 0;
    sscanf(text, "%d-%d-%d", year, month, day);
}


//*************************************************************************
// get_only_nonsymlink_file - Fetches the ftype and moves the char pointer beyond it
//*************************************************************************

int get_only_nonsymlink_file(char *text, int *n)
{
    int m, ftype;

    //** Skip the whitespace
    while (text[*n] == ' ') {
        if (text[*n] == 0) return(0);
        (*n)++;
    }

    //** Skip to the ","
    m = *n;
    while (text[m] != ',') {
        if (text[m] == 0) return(0);
        m++;
    }
    text[m] = '\0'; //** Terminate it
    ftype = 0;
    sscanf(text + *n, "%d", &ftype);
    text[m] = ',';

    if (ftype & OS_OBJECT_FILE_FLAG) {
        if ((ftype & OS_OBJECT_SYMLINK_FLAG) == 0) {
            *n = m + 1;
            return(ftype);
        }
    }

    return(0);
}
//*************************************************************************
//  interesting_clog_entry - Does a quick check to see if the change log
//       is interesting for the warming proces
//  NOTE: text is modified if the clog entry is interesting
//*************************************************************************

int interesting_clog_entry(char *text, clog_entry_t *ce)
{
    int i, j, n, type, fin;
    char *etext, *bstate;

    type = 0;

    //** Skip over the timestamp
    j = 0;
    for (i=0; text[i] != 0; i++) {
        if (text[i] == ']') {
            j = i + 1;
            while (text[j] == ' ') {
                if (text[j] == 0) return(0);
                j++;
            }
            break;
        }
    }

    if (strncmp("CREATE(", text + j, 7) == 0) {
        n = j + 7;
        if ((ce->ftype = get_only_nonsymlink_file(text, &n)) != 0) type = 1;
    } else if (strncmp("REMOVE(", text + j, 7) == 0) {
        n = j + 7;
        if ((ce->ftype = get_only_nonsymlink_file(text, &n)) != 0) type = 2;
    } else if (strncmp("ATTR_WRITE(system.exnode,", text + j, 25) == 0) {
        n = j + 25;
        if ((ce->ftype = get_only_nonsymlink_file(text, &n)) != 0) type = 3;
    } else if (strncmp("ATTR_WRITE(system.write_errors,", text + j, 31) == 0) {
        n = j + 31;
        if ((ce->ftype = get_only_nonsymlink_file(text, &n)) != 0) type = 4;
    } else if (strncmp("ATTR_REMOVE(system.write_errors,", text + j, 32) == 0) {
        n = j + 32;
        if ((ce->ftype = get_only_nonsymlink_file(text, &n)) != 0) type = 5;
    }

    if (type > 0) { //** Got something interesting so get the fname
        ce->clog_type = type;
        etext = tbx_stk_escape_string_token(text + n, " ,)", '\\', 1, &bstate, &fin);
        ce->fname = tbx_stk_unescape_text('\\', etext);
    } else if (strncmp("MOVE(", text + j, 5) == 0) {
        n = j + 5;
        ce->ftype = get_only_nonsymlink_file(text, &n);
        if (ce->ftype > 0) {
            type = 6;
            ce->clog_type = type;
            etext = tbx_stk_escape_string_token(text + n, " ,)", '\\', 1, &bstate, &fin);
            ce->fname = tbx_stk_unescape_text('\\', etext);
            etext = tbx_stk_escape_string_token(NULL, " ,)", '\\', 1, &bstate, &fin);
            ce->fname2 = tbx_stk_unescape_text('\\', etext);
        }
   }

    return(type);
}

//*************************************************************************
//  update_hash_for_move - Updates the hash fore renames
//*************************************************************************

//*************************************************************************
// process_clog_move - Processes a move clog entry. This is a "best-effort"
//     approach in that we aren't trying to track renames everywhere since
//     we are keying off the inode worst case is we have mismatched fnames
//     from the actual caps reported but the inode will be correct.
//
//     This just attempts to make sure that quick renames are captured by
//     just tracking changes as they occur in the clog run.
//
//     Properly tracking them in the clog and the prep DBs doesn't scale
//     well without a retooling of the prep DB to make it look more like
//     the distributed OS since a top-level directory change can generate
//     a lot of fname changes. Hence the current approach.
//*************************************************************************

void process_clog_move(warm_prep_db_t *wdb, clog_entry_t *ce, apr_hash_t *obj_hash)
{
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    clog_obj_info_t *obj;
    char *fn;
    int prefix_len, dir, m, len;
    tbx_stack_t stack;

    tbx_stack_init(&stack);

    log_printf(15, "ftype=%d fname=%s fname2=%s\n", ce->ftype, ce->fname, ce->fname2);

    dir = 0;
    m = 0;
    if (ce->ftype & OS_OBJECT_DIR_FLAG) {
        dir = 1;
        prefix_len = strlen(ce->fname);
        m = strlen(ce->fname2);
    }

    //** Since the inode is the primary key just need to iterate over the object hash
    for (hi=apr_hash_first(NULL, obj_hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&obj);
        if (dir == 0) {  //** It's a normal file
            if (strcmp(obj->fname, ce->fname) == 0) {
                apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, NULL);
                free(obj->fname);
                obj->fname = ce->fname2;
                ce->fname2 = NULL;
                free(ce->fname);
                apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, obj);
                return;   //** We can kick out
            }
        } else { //** It's a directory
            if (strncmp(ce->fname, obj->fname, prefix_len) == 0) { //** Maybe a match
                if (obj->fname[prefix_len] == '/')  { //** Got a match
                    len = strlen(obj->fname) + m + 1;
                    tbx_type_malloc(fn, char, len);
                    snprintf(fn, len, "%s%s", ce->fname2, obj->fname + prefix_len);
                    obj->fname2 = fn;
                    tbx_stack_push(&stack, obj);
                }
            }
        }
    }

    //** Swing the fname entries as needed
    while ((obj = tbx_stack_pop(&stack)) != NULL) {
        apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, NULL);
        free(obj->fname);
        obj->fname = obj->fname2;
        obj->fname2 = NULL;
        apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, obj);
    }

    //** Free it up if not used
    if (ce->fname) free(ce->fname);
    if (ce->fname2) free(ce->fname2);
    return;
}
//*************************************************************************
// process_clog_entry - Add's the entry to the appropriate que
//*************************************************************************

void process_clog_entry(warm_prep_db_t *wdb, clog_entry_t *ce, apr_hash_t *obj_hash)
{
//    char *errstr;
    clog_obj_info_t *obj;
    clog_obj_info_t dobj;


    //** A move operation is a little different so see if that's what's being done
    if (ce->clog_type == 6)  {
        process_clog_move(wdb, ce, obj_hash);
        return;
    }

    //** See if we already have it loaded
    obj = apr_hash_get(obj_hash, ce->fname, APR_HASH_KEY_STRING);
    if  (ce->clog_type == 2) { //** Remove op
        if (obj) {  //** Clear it from the hash
            apr_hash_set(obj_hash, ce->fname, APR_HASH_KEY_STRING, NULL);
            free(obj->fname);
            free(obj);
        }
        //** Also check the DBs
        dobj.fname = ce->fname;
        dobj.inode = ce->inode;
        warm_object_delete(wdb, &dobj);

        free(ce->fname);   //** Free the entry fname
        return;    //** We can kick out also
    }

    if (!obj) {  //** If not load it
        tbx_type_malloc_clear(obj, clog_obj_info_t, 1);
        obj->fname = ce->fname;

        //** finally add it
        apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, obj);
    } else {
        free(ce->fname);  //** Already have the object so free up the fname
    }

    //** Process the record
    switch(ce->clog_type) {
        case 1:  //** Create
            obj->state |= CL_STATE_ADD;
            return;
        case 3: //** Exnode update
            obj->state |= CL_STATE_ADD|CL_STATE_DEL;
            return;
        case 4: //** Write error set
            info_printf(lio_ifd, 0, "WRITE_ERROR_SET: %s\n", obj->fname); //** Log it for a quick followup repair.  WRITE_ERROR clears are missed but that's ok.
            obj->write_error_state = 1;
            return;
        case 5: //** Write error clear
            info_printf(lio_ifd, 0, "WRITE_ERROR_CLEAR: %s\n", obj->fname); //** Log it for a quick followup repair.  WRITE_ERROR clears are missed but that's ok.
            obj->write_error_state = 2;
            return;
    }

    return;
}

//*************************************************************************
// warm_rid_delete - Removes the allocations associated with the inode for the RID
//*************************************************************************

void warm_rid_delete(warm_prep_db_t *wdb, char *rid, clog_obj_info_t *obj)
{
    rocksdb_iterator_t *it;
    rid_prep_key_t *rkey;
    rid_prep_key_t *rkey_dummy;
    warm_prep_db_part_t *p;
    char *errstr;
    size_t nbytes;
    int n, nr, modulo;

    modulo = obj->inode % wdb->n_partitions;
    p = wdb->p[modulo];

    //** Make the iterator
    it = rocksdb_create_iterator(p->rid->db, p->rid->ropt);
    nr = strlen(rid);
    n = sizeof(rid_prep_key_t) + nr + 1 + 1;
    rkey_dummy = malloc(n); memset(rkey_dummy, 0, n);
    rkey_dummy->id = obj->inode;
    rkey_dummy->rid_len = nr;
    memcpy(rkey_dummy->strings, rid, nr+1);
    rkey_dummy->mcap_len = 0;
    rocksdb_iter_seek(it, (const char *)rkey_dummy, n);

    //** Iterate over the entries for the RID
    while (rocksdb_iter_valid(it)) {
        rkey = (rid_prep_key_t *)rocksdb_iter_key(it, &nbytes);
        if (!rkey) break;  //** Kick out if we hit the end
        if (rkey->id != obj->inode) break;

        errstr = NULL;
        rocksdb_delete(p->rid->db, p->rid->wopt, (const char *)rkey, nbytes, &errstr);
        if (errstr) {
            info_printf(lio_ifd, 0, "ERROR: RocksDB error removing RID entry! fname=%s RID=%s errstr=%s\n", obj->fname, rid, errstr);
            free(errstr);
        }

        rocksdb_iter_next(it);
    }
    rocksdb_iter_destroy(it); //** Destroy the iterator
    free(rkey_dummy);
}

//*************************************************************************
// warm_obj_delete - Deletes the object from the DBs
//*************************************************************************

void warm_object_delete(warm_prep_db_t *wdb, clog_obj_info_t *obj)
{
    char *rid, *errstr;
    inode_value_t *v;
    warm_prep_db_part_t *p;
    size_t nbytes;
    int i, j, n, modulo;

    modulo = obj->inode % wdb->n_partitions;
    p = wdb->p[modulo];

    //** Get the inode DB entry which has the list of RIDs
    errstr = NULL;
    v = (inode_value_t *)rocksdb_get(p->inode->db, p->inode->ropt, (const char *)&(obj->inode), sizeof(ex_id_t), &nbytes, &errstr);
    if (errstr) free(errstr);
    if (v == NULL) { //** Skip it if we can't find the inode entry
        log_printf(0, "ERROR: Can't locate inode entry for inode=" LU " fname=%s\n", obj->inode, obj->fname);
        return;
    }

    n = sizeof(inode_value_t) + v->fname_len+1 + v->rid_list_len;
    if (n != (int)nbytes) {
        log_printf(0, "ERROR: incorrect inode value size! fname=%s inode=" XIDT " got hlen=" ST " should be %d\n", obj->fname, obj->inode, nbytes, n);
        info_printf(lio_ifd, 0, "ERROR: incorrect inode value size! fname=%s inode=" XIDT " got hlen=" ST " should be %d\n", obj->fname, obj->inode, nbytes, n);
        goto error;
    }

    //** Skip over the filename
    rid = v->strings + v->fname_len+1;
    n = n - sizeof(inode_value_t) - v->fname_len - 1;
    j = 0;

    //** Now delete all the caps for the inode on each RID
    for (i=0; i<v->n_rids; i++) {
        n = n - j;
        if (n<=0) {
            log_printf(0, "ERROR: Ran out of RID string space! fname=%s inode=" XIDT " i=%d n_allocs=%d\n", obj->fname, obj->inode, i, v->n_allocs);
            info_printf(lio_ifd, 0, "ERROR: Ran out of RID string space! fname=%s inode=" XIDT " i=%d n_allocs=%d\n", obj->fname, obj->inode, i, v->n_allocs);
            goto error;
        }
        warm_rid_delete(wdb, rid, obj);
        j = strlen(rid)+1;
        rid = rid + j;
    }

    //** Finally go ahead and delete the inode DB entry
    errstr = NULL;
    rocksdb_delete(p->inode->db, p->inode->wopt, (const char *)&(obj->inode), sizeof(ex_id_t), &errstr);
    if (errstr) {
        info_printf(lio_ifd, 0, "ERROR: RocksDB error removing write_error entry! fname=%s errstr=%s\n", obj->fname, errstr);
        free(errstr);
    }

error:
    free(v);
}

//*************************************************************************
// process_objects - Processes all the object's in the hash
//*************************************************************************

void process_objects(warm_prep_db_t *wdb, apr_hash_t *obj_hash)
{
    char *errstr;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    clog_obj_info_t *obj;
    int err, i, modulo;
    char *keys[] = { "system.exnode", "system.inode" };

    for (hi=apr_hash_first(NULL, obj_hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&obj);

        //** Fetch the object info
        for (i=0; i<3; i++) obj->v_size[i] = -lio_gc->max_attr;
        err = lio_get_multiple_attrs(lio_gc, lio_gc->creds, obj->fname, NULL, keys, (void **)(obj->vals), obj->v_size, 2, 0);
        if (err != OP_STATE_SUCCESS) {
            log_printf(0, "ERROR: failed getting the attributes for fname=%s\n", obj->fname);
            info_printf(lio_ifd, 0, "ERROR: Failed getting attributes for fname=%s\n", obj->fname); //** Log it for a quick followup lio_warm
            goto next;
        }

        //** Go ahead and parse the inode
        if (obj->v_size[1] > 0) {
            if (obj->inode == 0) {
                obj->inode = 0;
                sscanf(obj->vals[1], XIDT, &(obj->inode));
            }
        } else {
            log_printf(0, "ERROR: Missing inode! fname=%s\n", obj->fname);
            info_printf(lio_ifd, 0, "ERROR: Missing inode! fname=%s\n", obj->fname); //** Log it for a quick followup lio_warm
            goto next;
        }


        //** Process it
        if (obj->state & CL_STATE_DEL) warm_object_delete(wdb, obj);
        if (obj->state & CL_STATE_ADD) {
            info_printf(lio_ifd, 0, "ADD: inode=" XIDT " fname=%s\n", obj->inode, obj->fname); //** Log it for a quick followup lio_warm
            update_warm_prep_db(lio_ifd, wdb, obj->fname, obj->vals, obj->v_size);
        }

        modulo = obj->inode % wdb->n_partitions;

        if (obj->write_error_state == 1) {  //** Write error set
            info_printf(lio_ifd, 0, "WRITE_ERROR: %s\n", obj->fname); //** Log it for a quick followup repair.  WRITE_ERROR clears are missed but that's ok.
            errstr = NULL;
            rocksdb_put(wdb->p[modulo]->write_errors->db, wdb->p[modulo]->write_errors->wopt, (const char *)&(obj->inode), sizeof(ex_id_t), NULL, 0, &errstr);
            if (errstr) {
                info_printf(lio_ifd, 0, "ERROR: RocksDB error putting write_error entry! fname=%s errstr=%s\n", obj->fname, errstr);
                free(errstr);
            }
        } else if (obj->write_error_state == 2) {  //** Write error clear
            errstr = NULL;
            rocksdb_delete(wdb->p[modulo]->write_errors->db, wdb->p[modulo]->write_errors->wopt, (const char *)&(obj->inode), sizeof(ex_id_t), &errstr);
            if (errstr) {
                info_printf(lio_ifd, 0, "ERROR: RocksDB error removing write_error entry! fname=%s errstr=%s\n", obj->fname, errstr);
                free(errstr);
            }
        }

next:
        apr_hash_set(obj_hash, obj->fname, APR_HASH_KEY_STRING, NULL);
        for (i=0; i<3; i++) {
            if (obj->v_size[i] > 0) free(obj->vals[i]);
        }
        free(obj->fname);
        free(obj);

    }
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, start_option, return_code, count, last;
    char *text;
    char *db_base = "/lio/log/warm";
    char *clog_base = "/lio/log/os_file.log";
    char *clog_date = NULL;
    int clog_line = -1;
    int year, month, day, line;
    tbx_notify_iter_t *ci;
    warm_prep_db_t *wdb;
    clog_obj_info_t *obj;
    apr_pool_t *mpool;
    apr_hash_t *obj_hash;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    clog_entry_t ce;

    return_code = 0;
    count = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_warm_prep_delta LIO_COMMON_OPTIONS [-count n] [-db DB_base_dir] [-cdir change_dir] [-date yyyy-mm-dd] [-line N]\n");
        lio_print_options(stdout);
        printf("    -count n          - Post an update every n objects processed\n");
        printf("    -db DB_base_dir   - Directory for the DBes to update. Default is %s\n", db_base);
        printf("    -cdir change_dir  - Prefix containing the change logs. Default is %s\n", clog_base);
        printf("    -date yyyy-mm-dd  - Change log date to start adding records. Default is to pick up from the last update.\n");
        printf("    -line N           - Initial line from change log date to start adding. Default is to pick up from the last update.\n");
        return(1);
    }

    lio_init(&argc, &argv);

    //** Parse the args
    i=1;
    do {
        start_option = i;

        if (strcmp(argv[i], "-db") == 0) { //** DB base directory
            i++;
            db_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-cdir") == 0) { //** change lol base directory
            i++;
            clog_base = argv[i];
            i++;
        } else if (strcmp(argv[i], "-date") == 0) { //** change log start date
            i++;
            parse_date(argv[0], &year, &month, &day);
            clog_date = argv[i];
            i++;
        } else if (strcmp(argv[i], "-date") == 0) { //** change log start date
            i++;
            clog_line = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "-count") == 0) { //** They want ongoing updates
            i++;
            count = atoi(argv[i]);
            i++;
        }
    } while ((start_option < i) && (i<argc));

    //** Make the change log hashes
    assert_result(apr_pool_create(&mpool, NULL), APR_SUCCESS);
    obj_hash = apr_hash_make(mpool);

    //** Get the DB's clog timestamp
    if (!clog_date) {
        warm_prep_get_timestamp(db_base, &year, &month, &day, &line);
        if (clog_line > 0) line = clog_line;
    }

    //** Open the existing prep DB
    wdb = open_prep_db(db_base, DB_OPEN_EXISTS, -1);

    //** Filter the logs
    ci = tbx_notify_iter_create(clog_base, year, month, day, line);
    i = 0;
    last = 0;
    while ((text = tbx_notify_iter_next(ci)) != NULL) {
        if (interesting_clog_entry(text, &ce)) {
            process_clog_entry(wdb, &ce, obj_hash);
            i++;
            if ((count > 0) && ((i/count) != last)) {
                last = i/count;
                fprintf(stderr, "Processed %d objects\n", i);
            }
        }
    }

    //** Get the last record processed
    tbx_notify_iter_current_time(ci, &year, &month, &day, &line);

    tbx_notify_iter_destroy(ci);

    //** Process the entries
    process_objects(wdb, obj_hash);

    close_prep_db(wdb);  //** Close the DBs

    //** Update the last record processed for the DB
    warm_prep_put_timestamp(db_base, year, month, day, line);

    //** Cleanup the hashes
    for (hi=apr_hash_first(NULL, obj_hash); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&obj);
        free(obj->fname);
        if (obj->v_size[0] > 0) free(obj->vals[0]);
        if (obj->v_size[1] > 0) free(obj->vals[1]);
        free(obj);
    }

    lio_shutdown();

    return(return_code);
}


