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

//**** Mode states
#define RES_MODE_WRITE  1
#define RES_MODE_READ   2
#define RES_MODE_MANAGE 4

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024 * 1024;
    char buffer[bufsize], *bstate;
    int i, port, mode, timeout;
    char *host, *rid;
    tbx_ns_t *ns;

    if (argc < 4) {
        printf("ibp_rid_mode host port RID [read] [write] [manage]\n");
        printf("\n");
        return (0);
    }

    timeout = 20;

    i = 1;

    host = argv[i];
    i++;
    port = atoi(argv[i]);
    i++;
    rid = argv[i];
    i++;

    mode = 0;
    while (i < argc) {
        if (strcasecmp(argv[i], "read") == 0) {
            mode |= RES_MODE_READ;
        } else if (strcasecmp(argv[i], "write") == 0) {
            mode |= RES_MODE_WRITE;
        } else if (strcasecmp(argv[i], "manage") == 0) {
            mode |= RES_MODE_MANAGE;
        }

        i++;
    }

    sprintf(buffer, "1 90 %s %d %d\n", rid, mode, timeout);     // IBP_INTERNAL_SET_MODE command

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
