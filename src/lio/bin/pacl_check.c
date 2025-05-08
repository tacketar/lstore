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
#include <lio/os.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/xattr.h>

//*************************************************************************
//*************************************************************************

//** Old C7 gcc 4.8.5 compiler optimizes arg processing loop and generates a potential signed overflow
int __attribute__((optimize("O0"))) main(int argc, char **argv)
{
    int i, ll, do_acl_tree, start_option, seed, do_print_config, do_print_tree, ftype;
    tbx_inip_file_t *ifd;
    char *pacl_fname;
    char *check_path = NULL;
    char *apply_path = NULL;
    void *acl;
    int asize;
    path_acl_context_t *pa;
    uid_t uid;
    gid_t gid;
    mode_t mode;

    if (argc < 2) {
        printf("\n");
        printf("pacl_check [-d log_level] [--path map_path | --path-tree map_path] [--seed n] [--print-tree | --print-config] [--apply-acl path] path_acl.ini\n");
        printf("    --path map_path      - Optional path to check where it maps\n");
        printf("    --path-tree map_path - Optional path to check where it maps and print an annotated ACL tree\n");
        printf("    --apply-acl path     - Apply the map_path ACLs to the given file or directory contained in path\n");
        printf("    --print-config       - Dump the parsed config to stdout\n");
        printf("    --print-tree         - Dump the Path ACL tree to stdout\n");
        return(1);
    }

    do_print_config = 0;
    do_print_tree = 0;
    do_acl_tree = 0;
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
        } else if (strcmp(argv[i], "--path-tree") == 0) { //** Want to check a path and print the ACL tree
            i++;
            do_acl_tree = 1;
            check_path = argv[i];
            i++;
        } else if (strcmp(argv[i], "--apply-acl") == 0) { //** Want to apply the ACL to a real object
            i++;
            apply_path = argv[i];
            i++;
        } else if (strcmp(argv[i], "--seed") == 0) { //** Want to check a path as well
            i++;
            seed = atoi(argv[i]);
            i++;
        } else if (strcmp(argv[i], "--print-config") == 0) { //** Print the parsed config
            i++;
            do_print_config = 1;
        } else if (strcmp(argv[i], "--print-tree") == 0) { //** Print the parsed config
            i++;
            do_print_tree = 1;
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
        tbx_inip_destroy(ifd);
        printf("ERROR: Failed to parse the pacl file %s\n", pacl_fname);
        return(4);
    }

    printf("Valid pacl file\n");

    //** See if we need to check a path
    if (check_path) {
        printf("Checking path %s\n", check_path);
        i = pacl_path_probe(pa, check_path, do_acl_tree, stdout, seed);
        printf("path_acl_probe()=%d\n", i);
    }

    //** See if we need to apply the ACL to a real file
    if (apply_path) {
        if (check_path == NULL) {
            printf("ERROR: A --path map_path is required to select the source ACL!\n");
        } else {
            ftype = lio_os_local_filetype(apply_path);
            if (ftype == 0) {
                printf("ERROR: Unable to apply ACL. Object does not exist! path=%s\n", check_path);
            } else {
                if (pacl_lfs_get_acl(pa, check_path, ftype, &acl, &asize, &uid, &gid, &mode) != 0) {
                    printf("ERROR: Failed getting LFS acl!\n");
                } else {
                    printf("Applying ACL to path %s (lio_fype=%d)\n", apply_path, ftype);
                    printf("   Primary UID: %u  Primary GID: %u  mode: %o\n", uid, gid, mode);
                    if (setxattr(apply_path, "system.posix_acl_access", acl, asize, 0) != 0) {
                        printf("ERROR: setxattr() errno=%d. May need to be root for this to work\n", errno);
                    }
                    if (chown(apply_path, uid, gid) != 0) {
                        printf("ERROR: chown() errno=%d. May need to be root for this to work\n", errno);
                    }
                }
            }
        }
    }


    if (do_print_config) {
        printf("\n");
        printf("========================= Path ACL Config BEGIN =================================\n");
        pacl_print_running_config(pa, stdout);
        printf("========================= Path ACL Config END =================================\n");
    }

    if (do_print_tree) {
        printf("\n");
        printf("========================= Path ACL TREE BEGIN =================================\n");
        pacl_print_tree(pa, check_path, stdout);
        printf("========================= Path ACL TREE END =================================\n");
    }

    //** Cleanup
    pacl_destroy(pa);
    tbx_inip_destroy(ifd);

    return(0);
}


