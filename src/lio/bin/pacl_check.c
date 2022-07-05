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

#include <tbx/log.h>
#include <tbx/iniparse.h>
#include <lio/path_acl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int i, ll, start_option, seed;
    tbx_inip_file_t *ifd;
    char *pacl_fname;
    char *check_path = NULL;
    path_acl_context_t *pa;

    if (argc < 2) {
        printf("\n");
        printf("pacl_check [-d log_level] [--path map_path] --seed n] path_acl.ini\n");
        printf("    --path map_path - Optional path to check where it maps\n");
        return(1);
    }

    i = 1;
    seed = -1;
    ll = -1;
    do {
        start_option = i;
        if (strcmp(argv[i], "-d") == 0) { //** Leg level default is -1
            i++;
            ll = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--path") == 0) { //** Want to check a path as well
            i++;
            check_path = argv[i];
            i++;
        } else if (strcmp(argv[i], "--seed") == 0) { //** Want to check a path as well
            i++;
            seed = atoi(argv[i]);
            i++;
        }
    } while ((start_option < i) && (i<argc));
    start_option = i;

    //** This is the source file
    pacl_fname = argv[start_option];
    if (pacl_fname == NULL) {
        printf("ERROR: Missing source file!\n");
        return(2);
    }

    tbx_log_open("stdout", 1);
    tbx_set_log_level(ll);

    //** Open the file
    ifd = tbx_inip_file_read(pacl_fname, 1);
    if (ifd == NULL) {
        printf("ERROR: Unable to open file %s\n", pacl_fname);
        return(3);
    }

    //** Load the path acl
    pa = pacl_create(ifd, "/tmp/lfs.XXXXXX");
    if (pa == NULL) {
        printf("ERROR: Failed to parse the pacl file %s\n", pacl_fname);
        return(4);
    }

    printf("Valid pacl file\n");

    //** See if we need to check a path
    if (check_path) {
        printf("Checking path %s\n", check_path);
        i = pacl_path_probe(pa, check_path, stdout, seed);
        printf("path_acl_probe()=%d\n", i);
    }

    //** Cleanup
    pacl_destroy(pa);
    tbx_inip_destroy(ifd);

    return(0);
}


