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
#include <tbx/type_malloc.h>
#include "cmd_send.h"

//** This is a hack to not have to have the ibp source
#define IBP_OK 1
#define IBP_PING 38
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
    char host_buffer[1024];
    char date[512];
    char *host;
    int port = 6714;
    int timeout = 30;
    apr_time_t dt_start, dt_end, dt_total, dt_depot, dt1;

    if (argc < 2) {
        printf("ibp_ping -a | host [port timeout]\n");
        printf("   -a   -Use the local host and default port\n");
        return (0);
    }

    if (strcmp(argv[1], "-a") == 0) {
        host = host_buffer;
        gethostname(host, 1023);
    } else {
        host = argv[1];
    }

    if (argc > 2)
        port = atoi(argv[2]);
    if (argc == 4)
        timeout = atoi(argv[3]);
    tbx_ns_timeout_set(&dt, timeout, 0);

    sprintf(cmd, "1 %d %d\n", IBP_PING, timeout);        // IBP_ST_VERSION command

    tbx_construct_fn_static();

    tbx_dnsc_startup_sized(10);

tbx_log_open("ibp_ping.log", 0); //LAGGY

    dt_depot = 0;
    dt_start = apr_time_now();
log_printf(0, "LAGGY: BEFORE cmd_send\n");
    ns = cmd_send(host, port, cmd, &bstate, timeout);
log_printf(0, "LAGGY: AFTER cmd_send\n");

    dt_end = apr_time_now();
    if (ns == NULL)
        return (-1);
    if (bstate != NULL)
        tbx_free(bstate);

    //** Read the result.
    //** Note that server_ns_readline strips the "\n" from the end of the line
    printf("Depot RID info -------------------------\n");
    n = NS_OK;
    while (n == NS_OK) {
        n = server_ns_readline(ns, buffer, bufsize, dt);
        if (n == NS_OK) {
            if (strcmp(buffer, "END") == 0) {
                n = NS_OK + 1;
            } else {
                printf("%s\n", buffer);
            }

            if (strncmp(buffer, "TOTAL(us):", 10) == 0) {
                bstate = strstr(buffer, "dt=");
                if (bstate) {
                    dt_depot = atol( bstate + 3);
                }
            }
        }
    }
    dt_end = apr_time_now();

    printf("--------------------\n");
    apr_ctime(date, dt_start);
    printf("CLIENT:Start: " TT "us   %s\n", dt_start, date);
    apr_ctime(date, dt_end);
    printf("CLIENT:End: " TT "us   %s\n", dt_end, date);
    dt_total = dt_end - dt_start;
    printf("CLIENT:DT: " TT "us   " TT "ms\n", dt_total, apr_time_as_msec(dt_total));
    dt1 = dt_total - dt_depot;
    printf("NETWORK:DT: " TT "us   " TT "ms\n", dt1, apr_time_as_msec(dt1));

    //** Close the connection
    tbx_ns_destroy(ns);

tbx_log_flush(); //LAGGY
    tbx_dnsc_shutdown();
    apr_terminate();
    return (0);
}
