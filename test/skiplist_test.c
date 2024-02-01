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

#define _log_module_index 175

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/skiplist.h>
#include <tbx/atomic_counter.h>

//*************************************************

tbx_sl_key_t *dup_int(tbx_sl_key_t *a)
{
    int n = *(int *)a;
    int *p;

    p = malloc(sizeof(int));
    *p = n;

    return(p);
}

//*************************************************

int main(int argc, char **argv)
{
    int n_max, rnd_val_max, rnd_max, l_max, dummy, i, j, k, best, best_n, n, err, *key_list, *data_list, *found_list;
    int min_key, max_key, rnd_min_delete, rnd_max_delete;
    double p;
    tbx_sl_t *sl;
    tbx_sl_iter_t *it;
    int *key, *data;
    int check_slot = 0;

    if (argc < 7) {
        printf("sk_test [-d log_level] rnd_val_max n_max random_max iter_min_delete inter_max_delete l_max p\n");
        exit(1);
    }

    tbx_log_open("stdout", 0);

    i = 1;
    if (strcmp(argv[i], "-d") == 0) {
        i++;
        j = atoi(argv[i]);
        i++;
        tbx_set_log_level(j);
    }
    rnd_val_max = atol(argv[i]);
    i++;
    n_max = atol(argv[i]);
    i++;
    rnd_max = atol(argv[i]);
    i++;
    rnd_min_delete = atol(argv[i]);
    i++;
    rnd_max_delete = atol(argv[i]);
    i++;
    l_max = atol(argv[i]);
    i++;
    p = atof(argv[i]);
    i++;

    check_slot = n_max / 2;

    sl = tbx_sl_new_full(l_max, p, 1, &tbx_sl_compare_int, dup_int, tbx_sl_free_simple, NULL);

    //** Make sure everything works fine with an empty list
    i = 12345;
    it = tbx_sl_iter_new();
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&i, 0);
    tbx_sl_next(it, (tbx_sl_key_t **)&key, (tbx_sl_data_t **)&data);
    if (data != NULL) {
        printf("ERROR got something from an EMPTY list\n");
    }

    key_list = (int *)malloc(sizeof(int)*n_max);
    data_list = (int *)malloc(sizeof(int)*n_max);
    found_list = (int *)malloc(sizeof(int)*n_max);

    //** Insert phase
    min_key = max_key = -1;

    for (i=0; i<n_max; i++) {
        key_list[i] = rnd_val_max * (1.0*rand())/RAND_MAX;
        if (i>=(n_max-2)) key_list[i] = key_list[0];  //** Force dups

        if ((i==0) || (min_key > key_list[i])) min_key = key_list[i];
        if ((i==0) || (max_key < key_list[i])) max_key = key_list[i];

        found_list[i] = 0;  //** Go ahead and clear the found array
        data_list[i] = i;
        printf("inserting i=%d key=%d\n", i, key_list[i]);
        err = tbx_sl_insert(sl, (tbx_sl_key_t *)&(key_list[i]), (tbx_sl_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
        }
    }


    printf("********** min_key=%d    max_key=%d **********\n", min_key, max_key);

    //**Check phase
    printf("Checking for each key using an iterator\n");
    for (i=0; i<n_max; i++) {
        printf("Looking for key[%d]=%d\n", i, key_list[i]);
        tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&(key_list[i]), 0);
        j = 0;
        do {
            err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
            if (err != 0) {
                printf("Err locating key_list[%d]=%d\n", i, key_list[i]);
            }
            if ((*key == key_list[i]) && (data == &(data_list[i]))) j = 1;
        } while ((err == 0) && (j == 0) && (*key == key_list[i]));

        if (j == 0) {
            printf("ERROR locating key_list[%d]=%d\n", i, key_list[i]);
        } else {
            printf("Found key[%d]=%d key=%d data=%d\n", i, key_list[i], *key, *data);
        }
    }

    //** Use the iterator to scan down through everything
    printf("Walking the list of keys\n");
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)NULL, 0);
    err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    i = 0;
    j = -1;
    while (err == 0) {
        printf("i=%d key=%d data=%d\n", i, *key, *data);
        if (found_list[*data] != 0) {
            printf("ERROR: already marked key! i=%d key=%d data=%d\n", i, *key, *data);
        }
        found_list[*data] = 1;
        if (j > *key) {
            printf("ERROR: prev key > curr key! prev=%d curr=%d\n", j, *key);
        }
        j = *key;

        err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    }

    for (i=0; i<n_max; i++) {
        if (found_list[i] == 0) {
            printf("ERROR: Skipped i=%d key=%d!\n", i, key_list[i]);
        }
    }

    //** Check that I can get the start/end keys

    printf("Checking access to the 1st key\n");
    key = tbx_sl_first_key(sl);
    if (*key != min_key) {
        printf("ERROR getting 1st key! min_key=%d got=%d\n", min_key, *key);
    }

    printf("Checking query for min_key-1\n");
    j = min_key - 1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (*key != min_key) {
        printf("ERROR getting 1st key using min_key-1! min_key-1=%d got=%d\n", j, *key);
    }

    printf("Checking query for min_key\n");
    j = min_key;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (*key != min_key) {
        printf("ERROR getting 1st key using min_key! min_key=%d got=%d\n", j, *key);
    }

    printf("Checking query for min_key+1\n");
    j = min_key + 1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (*key <= min_key) {
        printf("ERROR querying min_key+1! min_key+1=%d got=%d\n", j, *key);
    }


    printf("Checking access to the last key\n");
    key = tbx_sl_key_last(sl);
    if (*key != max_key) {
        printf("ERROR getting last key! max_key=%d got=%d\n", max_key, *key);
    }

    printf("Checking query for max_key-1\n");
    j = max_key - 1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (*key < j) {
        printf("ERROR with last key query using max_key-1! max_key-1=%d got=%d\n", j, *key);
    }

    printf("Checking query for max_key\n");
    j = max_key;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (*key != j) {
        printf("ERROR getting last key using max_key! max_key=%d got=%d\n", j, *key);
    }

    printf("Checking query for max_key+1\n");
    j = max_key + 1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    if (key != NULL) {
        printf("ERROR getting key using max_key+1! max_key+1=%d got=%d should be NULL\n", j, *key);
    }

    //** Iterate through the list to verify order
    printf("Iterating through the list to verify order\n");
    j = -1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)NULL, 0);
    err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    i = 0;
    while (err == 0) {
        printf("i=%d key=%d data=%d\n", i, *key, *data);
        if (j>*key) {
            printf("ERROR! in order! i=%d prev=%d curr=%d\n", i, j, *key);
        }
        err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
        i++;
    }
    printf("After iteration through the list.  i=%d n_max=%d\n", i, n_max);
    if (i<n_max) {
        printf("ERROR Incorrect number of items!\n");
    }

    printf("Checking that we return the key or the next higher key\n");
    j = key_list[check_slot]-1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
    if (*key < j) {
        printf("ERROR! key<j (%d<%d)!!!!!\n", *key, j);
    }

    printf("Checking that round down works\n");
    j = key_list[check_slot]+1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, -1);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
    if (*key > j) {
        printf("ERROR! key>j (%d>%d)!!!!!\n", *key, j);
    }

    printf("min_key:  Checking that round down works\n");
    j = min_key+1;
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, -1);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("Checking for j=%d min_key=%d got key=%d\n", j, min_key, *key);
    if (*key > j) {
        printf("ERROR! key>j (%d>%d)!!!!!\n", *key, j);
    }


    j = key_list[check_slot];
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
    if (*key != j) {
        printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
    }

    printf("Performing random checks\n");
    dummy = -1;
    for (i=0; i<rnd_max; i++) {
        n = rand();
        tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&n, 0);
        err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
        best = -1;
        best_n = -1;
        for (j=0; j<n_max; j++) {
            k = key_list[j] - n;
            if (k > 0) {
                if ((best == -1) || (best > k)) {
                    best = k;
                    best_n = key_list[j];
                }
            }
        }

        if (key == NULL) {
            key = &dummy;
            //printf("ERROR (OK) NULL returned\n");
        }
        if (*key != best_n) {
            j = *key - n;
            printf("ERROR checking n=%d skiplist best=%d dt=%d  ---  scan best=%d dt=%d err=%d\n", n, *key, j, best_n, best, err);
        }
    }

    printf("Performing random round down checks\n");
    dummy = -1;
    for (i=0; i<rnd_max; i++) {
        n = rand();
        tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&n, -1);
        err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
        best = -1;
        best_n = -1;
        for (j=0; j<n_max; j++) {
            k = n - key_list[j];
            if (k > 0) {
                if ((best == -1) || (best > k)) {
                    best = k;
                    best_n = key_list[j];
                }
            }
        }

        if (key == NULL) {
            key = &dummy;
            //printf("ERROR (OK) NULL returned\n");
        }
        if (*key != best_n) {
            j = n - *key;
            printf("ERROR checking n=%d skiplist best=%d dt=%d  ---  scan best=%d dt=%d err=%d\n", n, *key, j, best_n, best, err);
        }
    }

    //** Do iter removal/insertion
    printf("Performing iterator remove/insertion using the range: %d..%d\n", rnd_min_delete, rnd_max_delete);
    i = 0;
    do {
        n = rnd_min_delete;
        tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&n, 0);
        err = tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
        if ((key != NULL) && (*key >= rnd_min_delete) && (*key <= rnd_max_delete)) {
            j = *key;
            tbx_sl_iter_remove(it);
            key_list[*data] = rnd_val_max * (1.0*rand())/RAND_MAX;
            printf("iter remove remove_key=%d new_key=%d data=%d\n", j, key_list[*data], *data);

            tbx_sl_insert(sl, (tbx_sl_key_t *)&(key_list[*data]), (tbx_sl_data_t *)data);
        } else {
            i = 1;
        }
    } while (i == 0);


    printf("Deleting everything\n");

    //** Now delete everything manually
//  for (i=0; i<n_max; i++) {
    for (i=n_max-1; i>=0; i--) {
        printf("removing key[%d]=%d\n", i, key_list[i]);
        fflush(stdout);
        fflush(stderr);
        err = tbx_sl_remove(sl, (tbx_sl_key_t *)&(key_list[i]), (tbx_sl_data_t *)&(data_list[i]));
        if (data_list[i] != i) {
            printf("ERROR removing key_list[%d]=%d data_list[i]=%d\n", i, key_list[i], data_list[i]);
            fflush(stdout);
            fflush(stderr);
        }
        data_list[i] = -1;

        if (err != 0) {
            printf("ERROR removing key_list[%d]=%d\n", i, key_list[i]);
            fflush(stdout);
            fflush(stderr);
        }
    }

    //** Reset the data
    for (i=0; i<n_max; i++) {
        data_list[i] = i;
    }

    //** Now insert a couple of elements and "empty and repeat"
    printf("Checking empty_skiplist\n");
    for (i=0; i<n_max; i++) {
        err = tbx_sl_insert(sl, (tbx_sl_key_t *)&(key_list[i]), (tbx_sl_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("empty 1 ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
        }
    }
    j = key_list[check_slot];
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("empty 1 Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
    if (*key != j) {
        printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
    }

    tbx_sl_empty(sl);

    for (i=0; i<n_max; i++) {
        err = tbx_sl_insert(sl, (tbx_sl_key_t *)&(key_list[i]), (tbx_sl_data_t *)&(data_list[i]));
        if (err != 0) {
            printf("empty 2 ERROR inserting key_list[%d]=%d\n", i, key_list[i]);
        }
    }
    j = key_list[check_slot];
    tbx_sl_iter_search_init(it, sl, (tbx_sl_key_t *)&j, 0);
    tbx_sl_next(it, (tbx_sl_key_t *)&key, (tbx_sl_data_t *)&data);
    printf("empty 2 Checking for j=%d key_list[%d]=%d got key=%d\n", j, check_slot, key_list[check_slot], *key);
    if (*key != j) {
        printf("ERROR! key!=j (%d!=%d)!!!!!\n", *key, j);
    }

    tbx_sl_iter_del(it);
    tbx_sl_del(sl);

    fflush(stdout);
    tbx_log_flush();

    free(key_list);
    free(data_list);
    free(found_list);

    return(0);
}

