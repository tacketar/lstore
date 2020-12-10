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

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/lru.h>

#define N_MAX 5

typedef struct {
    int key;
    int value;
    int found;
    int used;
} object_t;

const int object_size = sizeof(object_t);

//*******************************************************************************

void get_key(void *global, void *ptr, void **key, int *klen)
{
    object_t *obj = (object_t *)ptr;

    *key = (void *)&(obj->key);
    *klen = sizeof(int);
}

//*******************************************************************************

int check_lru(tbx_lru_t *lru, object_t *objects)
{
    int i, err, found;
    object_t *obj;

    err = 0;

    //** Clear the state
    for (i=0; i<N_MAX; i++) {
        objects[i].found = 0;
    }

    //** Do the scan
    obj = tbx_lru_first(lru);
    while (obj) {
        found = 0;
        for (i=0; i<N_MAX; i++) {
            if (obj->key == objects[i].key) {
                if (obj->value == objects[i].value) {
                    objects[i].found = 1;
                    found = 1;
                    break;
                }
                err++;
                printf("BAD_VALUE: i=%d key=%d value=%d got=%d found=%d used=%d\n", i, objects[i].key, objects[i].value, obj->value, objects[i].found, objects[i].used);
            }
        }

        if (found == 0) {
            err++;
            printf("NO_MATCH: key=%d value=%d\n", obj->key, obj->value);
        }
        obj = tbx_lru_next(lru);
    }

    //** Now check if everything went as expected
    for (i=0; i<N_MAX; i++) {
        if (objects[i].found == 0) {
            if (objects[i].used == 1) {
                err++;
                printf("MISSING: i=%d key=%d value=%d found=%d used=%d\n", i, objects[i].key, objects[i].value, objects[i].found, objects[i].used);
            }
        } else if (objects[i].used == 0) {
            if (objects[i].found == 1) {
                err++;
                printf("UNUSED: i=%d key=%d value=%d found=%d used=%d\n", i, objects[i].key, objects[i].value, objects[i].found, objects[i].used);
            }
        }
    }

    return(err);
}

//*******************************************************************************
//*******************************************************************************

int main(int argc, char **argv)
{
    int i, err;
    object_t obj[N_MAX], object;
    tbx_lru_t *lru;

    tbx_log_open("stdout", 1);
    tbx_set_log_level(20);

    //** Make the LRU
    lru = tbx_lru_create(N_MAX, get_key, tbx_lru_clone_default, tbx_lru_copy_default, tbx_lru_free_default, (void *)&object_size);

    //** Initialize the state
    for (i=0; i<N_MAX; i++) {
        obj[i].key = i; obj[i].value = i; obj[i].found = 0; obj[i].used = 1;
        tbx_lru_put(lru, &(obj[i]));
    }

    printf("=====Initial test after fill\n");
    err = check_lru(lru, obj);

    printf("=====check if we can find all the keys\n");
    for (i=0; i<N_MAX; i++) {
        if (tbx_lru_get(lru, &(obj[i].key), sizeof(int), &object) != 0) {
            err++;
            printf("ERROR: Can't find key=%d!\n", obj[i].key);
        } else {
            if ((object.key != obj[i].key) && (object.value != obj[i].value)) {
                err++;
                printf("ERROR: Found a key but it's not correct! key=%d value=%d got=%d got_value=%d\n", obj[i].key, obj[i].value, object.key, object.value);
            }
        }
    }
    printf("=====Roll off key=0\n");
    obj[0].key = 100; obj[0].value = 100;
    tbx_lru_put(lru, &(obj[0]));
    err = check_lru(lru, obj);

    printf("=====Delete the key=100\n");
    obj[0].key = 100; obj[0].value = 100; obj[0].used = 0;
    tbx_lru_delete(lru, &(obj[0].key), sizeof(int));
    err = check_lru(lru, obj);

    printf("=====Verify key=100 can't be found\n");
    if (tbx_lru_get(lru, &(obj[0].key), sizeof(int), &object) == 0) {
        err++;
        printf("ERROR: Found deleted key!\n");
    }

    printf("=====Update the value for key=1 to -1\n");
    obj[1].value = -1;
    tbx_lru_put(lru, &(obj[1]));
    if (tbx_lru_get(lru, &(obj[1].key), sizeof(int), &object) == 0) {
        if ((obj[1].value != object.value) || (obj[1].key != object.key)) {
            err++;
            printf("ERROR: Found key=1 but wrong key or value! value=%d got_value=%d got_key=%d\n", obj[1].value, object.value, object.key);
        }
    } else {
        err++;
        printf("ERROR: Missing updated key!\n");
    }
    err = check_lru(lru, obj);

    printf("=====Roll off key=2 by adding 2 more keys\n");
    obj[0].key = 101; obj[0].value = 101; obj[0].used = 1;
    tbx_lru_put(lru, &(obj[0]));
    obj[2].key = 102; obj[2].value = 102;
    tbx_lru_put(lru, &(obj[2]));
    err = check_lru(lru, obj);

    //** Cleanup
    tbx_lru_destroy(lru);

    if (err == 0) {
        printf("lru_test: PASSED\n");
    } else {
        printf("lru_test: FAILED\n");
    }
    return(err);
}

