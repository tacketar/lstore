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
#include <time.h>
#include <assert.h>
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include <tbx/constructor.h>
#include <tbx/network.h>
#include <tbx/net_sock.h>
#include <tbx/log.h>
#include <tbx/dns_cache.h>
#include <tbx/fmttypes.h>
#include <tbx/type_malloc.h>
#include "subnet.h"
#include "print_alloc.h"
#include "cmd_send.h"


int parse_cap(char *cap, char **host, int *port, char **rid, char **key, char **kid)
{
    char *bstate;
    int finished = 0;

    char *ptr;
    ptr = tbx_stk_string_token(cap, "/", &bstate, &finished);   //** gets the ibp:/
    log_printf(15, "1 ptr=%s\n", ptr);
    ptr = tbx_stk_string_token(NULL, ":", &bstate, &finished);
    ptr = &(ptr[1]);            //** Skip the extra "/"
    log_printf(15, "2 ptr=%s\n", ptr);
    *host = ptr;                //** This should be the host name
    sscanf(tbx_stk_string_token(NULL, "/", &bstate, &finished), "%d", port);

    *rid = tbx_stk_string_token(NULL, "#", &bstate, &finished);
    *key = tbx_stk_string_token(NULL, "/", &bstate, &finished);
    *kid = tbx_stk_string_token(NULL, "/", &bstate, &finished);

    log_printf(15, "parse_cap: CAP=%s * parsed=%s:%d/%s#%s/%s\n", cap, *host, *port, *rid,
               *key, *kid);

    if (finished == 1)
        log_printf(0, "parse_cap:  Error parsing cap %s\n", cap);

    return (finished);
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int bufsize = 1024 * 1024;
    char buffer[bufsize], *bstate, *result;
    Allocation_t a;
    Allocation_history_db_t h;
    int used, start_option, print_blocks;
    int err, npos, state;
    int n, offset, len, ndata, i, k;
    int max_wait = 10;
    ibp_off_t header_bs, bs;
    apr_time_t to;
    int nblocks, cs_type;
    tbx_ns_timeout_t dt;
    char *fname = NULL;
    FILE *fd;
    char *host, *rid, *key, *kid;
    int port;
    tbx_ns_t *ns;

    header_bs = -1;
    bs = -1;

//printf("sizeof(Allocation_t)=" LU " ALLOC_HEADER=%d\n", sizeof(Allocation_t), ALLOC_HEADER);

    if (argc < 3) {
        printf
        ("get_alloc [-d debug_level] [--print_blocks] [--file fname offset len] [--cap full_ibp_capability]|[host port RID ID] [--file fname offset len]\n");
        printf("      --file stores a portion of the allocation to fname based on the given offset and length\n");
        printf("          fname - Filename of where to store data. If stdout or stderr redirects to that device\n");
        printf("          data_offset is the offset relative to the start of data, after the header.\n");
        printf("          len of 0 means return all data available starting from offset\n");
        printf("      --print_blocks  Prints the chksum block information if available\n");
        printf("      -d debug_level  Sets the debug level.  Default is 0.\n");
        printf("\n");
        return (0);
    }

    tbx_construct_fn_static();

    i = 1;

    host = NULL;
    offset = -2;
    len = 0;
    ndata = 0;
    print_blocks = 0;
    do {
        start_option = i;
        if (strcmp("--cap", argv[i]) == 0) {
            i++;
            parse_cap(argv[i], &host, &port, &rid, &key, &kid);
            log_printf(15, "get_alloc: parsed=%s:%d/%s#%s/%s\n", host, port, rid, key,
                       kid);
            i++;
        } else if (strcmp("--print_blocks", argv[i]) == 0) {
            i++;
            print_blocks = 1;
        } else if (strcmp("--file", argv[i]) == 0) {
            i++;
            fname = argv[i];
            i++;
            offset = atoi(argv[i]);
            i++;
            len = atoi(argv[i]);
            i++;
        } else if (strcmp("-d", argv[i]) == 0) {
            i++;
            k = atoi(argv[i]);
            i++;
            tbx_set_log_level(k);
        }
        log_printf(15, "get_alloc: while loop bottom i=%d argc=%d\n", i, argc);
    } while ((start_option < i) && (i < argc));

    log_printf(15, "get_alloc: after while loop\n");


    if (host == NULL) {
        host = argv[i];
        i++;
        port = atoi(argv[i]);
        i++;
        rid = argv[i];
        i++;
        kid = argv[i];
        i++;
    }

    tbx_dnsc_startup_sized(10);

    sprintf(buffer, "2 %d %s %s %d %d %d 10\n", INTERNAL_GET_ALLOC, rid, kid,
            print_blocks, offset, len);

    ns = cmd_send(host, port, buffer, &bstate, max_wait);
    if (ns == NULL)
        return (-1);
    tbx_ns_timeout_set(&dt, max_wait, 0);

    result = bstate;
    nblocks = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    state = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    cs_type = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    header_bs = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    bs = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    ndata = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));
    log_printf(15, "parsed: nb=%d s=%d cs_type=%d hbs=" I64T " bs=" I64T " ndata=%d\n", nblocks,
               state, cs_type, header_bs, bs, ndata);
    free(result);

    //** Read the Allocation ***
    to = apr_time_now() + apr_time_make(max_wait, 0);
    err = server_ns_read_block(ns, to, (char *) &a, sizeof(a));
    if (err != NS_OK) {
        printf("get_alloc:  Error reading allocation!  err=%d\n", err);
        abort();
    }

    //** and now the history
    to = apr_time_now() + apr_time_make(max_wait, 0);
    err = server_ns_readline(ns, buffer, sizeof(buffer), to);
    h.n_read = atoi(tbx_stk_string_token(buffer, " ", &bstate, &err)); if (h.n_read > 0) {  tbx_type_malloc_clear(h.read_ts, Allocation_rw_ts_t, h.n_read); }
    h.n_write = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err));  if (h.n_write > 0) { tbx_type_malloc_clear(h.write_ts, Allocation_rw_ts_t, h.n_write); }
    h.n_manage = atoi(tbx_stk_string_token(NULL, " ", &bstate, &err)); if (h.n_manage > 0) { tbx_type_malloc_clear(h.manage_ts, Allocation_manage_ts_t, h.n_manage); }

    n = server_ns_read_block(ns, to, (char *)(h.read_ts), sizeof(Allocation_rw_ts_t)*h.n_read);
    n = server_ns_read_block(ns, to, (char *)(h.write_ts), sizeof(Allocation_rw_ts_t)*h.n_write);
    n = server_ns_read_block(ns, to, (char *)(h.manage_ts), sizeof(Allocation_manage_ts_t)*h.n_manage);

    used = 0;
    print_allocation(buffer, &used, sizeof(buffer) - 1, &a, &h, state, cs_type, header_bs, bs);
    printf("%s", buffer);

    //** Now read in the block information
    if ((nblocks > 0) && (print_blocks == 1)) {
        printf("\n\n");
        printf(" Block      Bytes    State          Chksum (calculated if error)\n");
        printf("-------  ----------  -----   -----------------------------------------\n");
        for (i = 0; i < nblocks; i++) {
            n = server_ns_readline(ns, buffer, bufsize, dt);
            printf("%s\n", buffer);
        }
    }
    //** Lastly print any data that is requested.
    if (offset > -1) {
        i = 1;
        if (strcmp(fname, "stdout") == 0) {
            fd = stdout;
            i = 0;
        } else if (strcmp(fname, "stderr") == 0) {
            fd = stderr;
            i = 0;
        } else if ((fd = fopen(fname, "w")) == NULL) {
            printf("get_alloc: Can't open file %s!\n", fname);
        }

        printf("\n");
        printf("From offset %d storing %d bytes in %s\n", offset, len, fname);

        ndata = len;
        while (ndata > 0) {
            npos = (bufsize > ndata) ? ndata : bufsize;
            n = server_ns_read(ns, buffer, npos, dt);
            if (n > 0) {
                fwrite(buffer, n, 1, fd);
                ndata = ndata - n;
            }
        }

        fclose(fd);
    }

    printf("\n");

    tbx_ns_destroy(ns);

    if (h.n_read > 0) free(h.read_ts);
    if (h.n_write > 0) free(h.write_ts);
    if (h.n_manage > 0) free(h.manage_ts);

    apr_terminate();

    return (0);
}
