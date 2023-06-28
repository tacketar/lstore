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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <tbx/constructor.h>
#include <tbx/network.h>
#include <tbx/net_sock.h>
#include <tbx/log.h>
#include <tbx/dns_cache.h>
#include <tbx/string_token.h>
#include "cmd_send.h"

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024 * 1024;
    char buffer[bufsize], *bstate;
    int i, start_option, port, force_rebuild, timeout, slen;
    char *host, *rid, *msg, *merge_snap;
    tbx_ns_t *ns;

    if (argc < 4) {
        printf("ibp_attach_rid [--rebuild] [--merge snap] host port RID [msg]\n");
        printf("--rebuild   - Rebuild RID databases added. Same as force_rebuild=1 in config file.\n");
        printf("              This takes signifcantly longer to start should\n");
        printf("              not be required unless the DB itself is corrupt\n");
        printf("--merge     - Use this RID DB snapshot to fill in gaps if needed\n");
        printf("\n");
        return (0);
    }

    i = 1;
    force_rebuild = 0;
    merge_snap = "-";
    do {
        start_option = i;
        if (strcmp(argv[i], "--rebuild") == 0) {
            force_rebuild = 2;
            i++;
        } else if (strcmp(argv[i], "--merge") == 0) {
            i++;
            merge_snap = argv[i];
            i++;
        }
    } while ((start_option < i) && (i<argc));

    host = argv[i];
    i++;
    port = atoi(argv[i]);
    i++;
    rid = argv[i];
    i++;

    timeout = 60;
    msg = "";
    if (argc > i)
        msg = argv[i];

    slen = strlen(msg);
    sprintf(buffer, "1 91 %s %d %s %d %d %s\n", rid, force_rebuild, merge_snap, timeout, slen, msg);   // IBP_INTERNAL_RID_MOUNT command

    tbx_construct_fn_static();

    tbx_dnsc_startup_sized(10);

    ns = cmd_send(host, port, buffer, &bstate, timeout);
    if (ns == NULL)
        return (-1);
    if (bstate != NULL)
        free(bstate);

    //** Close the connection
    tbx_ns_close(ns);

    apr_terminate();

    return (0);
}
