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

#include "allocation.h"
#include "resource.h"
#include <stdio.h>
#include <assert.h>
#include <tbx/constructor.h>

//***********************************************************************************
//***********************************************************************************
//***********************************************************************************

int main(int argc, const char **argv)
{
    if (argc < 6) {
        printf("mkfs.resource RID type device db_location n_partitions [max_mbytes]\n");
        printf("\n");
        printf("RID          - Resource ID (integer)\n");
        printf("type         - Type or resource. Currently only 'dir' is supported\n");
        printf("device       - Device to be used for the resource.\n");
        printf("db_location  - Base directory to use for storing the DBes for the resource.\n");
        printf("n_partitions - Number of subdirectory partitions to use for storing allocations\n");
        printf("max_mbytes   - Max number of MB to use.  If missing it defaults to the entire disk.\n");
        printf("\n");
        return (1);
    }

    int err = 0;
    rid_t rid;
    ibp_off_t nbytes = 0;
    int n_partitions;

    if (argc > 6) nbytes = 1024 * 1024 * atoll(argv[6]);

    n_partitions = atoll(argv[5]);
    tbx_construct_fn_static();

    if (ibp_str2rid((char *) argv[1], &rid) != 0) {
        printf("Invalid RID format!  RID=%s\n", argv[1]);
    } else {
        err = mkfs_resource(rid, (char *) argv[2], (char *) argv[3], (char *) argv[4], nbytes, n_partitions);
    }

    apr_terminate();

    return (err);
}
