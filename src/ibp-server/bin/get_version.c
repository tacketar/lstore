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
#include <unistd.h>
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

//** This is a hack to not have to have the ibp source
#define IBP_OK 1
#define IBP_E_OUT_OF_SOCKETS -66

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024 * 1024;
    char buffer[bufsize], *bstate;
    int n;
    tbx_ns_timeout_t dt;
    tbx_ns_t *ns;
    char cmd[512];
    char *host;
    int port = 6714;
    int timeout = 15;


    if (argc < 2) {
        printf("get_version -a | host [port timeout]\n");
        printf("   -a   -Use the local host and default port\n");
        return (0);
    }

    if (strcmp(argv[1], "-a") == 0) {
        host = (char *) malloc(1024);
        gethostname(host, 1023);
    } else {
        host = argv[1];
    }

    if (argc > 2)
        port = atoi(argv[2]);
    if (argc == 4)
        timeout = atoi(argv[3]);
    tbx_ns_timeout_set(&dt, timeout, 0);

    sprintf(cmd, "1 4 5 %d\n", timeout);        // IBP_ST_VERSION command

    tbx_construct_fn_static();

    tbx_dnsc_startup_sized(10);

    ns = cmd_send(host, port, cmd, &bstate, timeout);
    if (ns == NULL)
        return (-1);
    if (bstate != NULL)
        free(bstate);

    //** Read the result.  Termination occurs when the line "END" is read.
    //** Note that server_ns_readline strips the "\n" from the end of the line
    n = NS_OK;
    while (n == NS_OK) {
        n = server_ns_readline(ns, buffer, bufsize, dt);
        if (n == NS_OK) {
            if (strcmp(buffer, "END") == 0) {
                n = NS_OK + 1;
            } else {
                printf("%s\n", buffer);
            }
        }
    }

    //** Close the connection
    tbx_ns_destroy(ns);

    tbx_dnsc_shutdown();
    apr_terminate();
    return (0);
}
