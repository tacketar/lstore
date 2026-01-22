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

//*************************************************************
//  ZeroMQ implementation of the MQ subsystem.
//  This is mainly a wrapper around 0MQ calls with an
//  extension of the 0MQ socket types
//*************************************************************

#include <apr_signal.h>
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/errno.h>
#include <sys/signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <tbx/fmttypes.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/stack.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>


#include "mq_portal.h"
#include "mq_helpers.h"

#define SERVER_CONFIG "authorized_keys"
#define CLIENT_CONFIG "known_hosts"

//*************************************************************
//   Native routines
//*************************************************************

//*************************************************************

void zero_native_destroy(gop_mq_socket_context_t *ctx, gop_mq_socket_t *socket)
{
    zmq_close(socket->arg);
    free(socket);
}

//*************************************************************

void encrypt_socket(gop_mq_socket_t *socket, char *id, int server_mode)
{
    char fname[PATH_MAX];
    char public_key[41], secret_key[41];
    char *home, *section, *etext, *key, *key_prefix;
    tbx_inip_file_t *ifd;
    int k, retry;
    struct stat sbuf;

    key_prefix = getenv(LIO_ENV_KEY_PREFIX);
    retry = 0;
again:
    if (!server_mode) {  //** Client mode
        if (key_prefix) {
            snprintf(fname, sizeof(fname)-1, "%s/%s", key_prefix, CLIENT_CONFIG); fname[sizeof(fname)-1] = '\0';
        } else if (retry == 0) {    //** Try getting them from the user's local directory first
            home = getenv("HOME");
            snprintf(fname, sizeof(fname)-1, "%s/.lio/%s", home, CLIENT_CONFIG); fname[sizeof(fname)-1] = '\0';
        } else {    //** and if that fails try getting the server public key from the global common location
            retry = 2;  //** This will kick us out on the next round
            snprintf(fname, sizeof(fname)-1, "/etc/lio/%s", CLIENT_CONFIG); fname[sizeof(fname)-1] = '\0';
        }
    } else {   //** Server mode.  Keys are always in their local directory
        if (key_prefix) {
            snprintf(fname, sizeof(fname)-1, "%s/%s", key_prefix, SERVER_CONFIG); fname[sizeof(fname)-1] = '\0';
        } else {
            home = getenv("HOME");
            snprintf(fname, sizeof(fname)-1, "%s/.lio/%s", home, SERVER_CONFIG ); fname[sizeof(fname)-1] = '\0';
        }
    }

    ifd = (stat(fname, &sbuf) == 0) ? tbx_inip_file_read(fname, 1) : NULL;
    if (!ifd) {
        if ((!server_mode) || (retry == 0)) {  //** Let's try again
            if (key_prefix) {
                key_prefix = NULL;
            } else if (retry == 0) {
                retry = 1;
            } else {
                goto fail;
            }
            goto again;
        }

        if (server_mode) {
            if (key_prefix) {    //** Try again using the normal location
                key_prefix = NULL;
                goto again;
            }
        }
        goto fail;  //** No encryption to load so fail
    }

    section = tbx_inip_get_string(ifd, "mappings", id, NULL);
    if (!section) {    //** No matching secion in the file
        if (retry == 0) {  //** Retry with the other config file
            if (key_prefix) {
                key_prefix = NULL;
            } else {
                retry = 1;
            }
            tbx_inip_destroy(ifd);
            goto again;
        } else {          //** No mappings to load so no encryption
            goto fail;
        }
    }

    if (server_mode) {
        etext = tbx_inip_get_string(ifd, section, "secret_key", NULL);
        if (!etext) {
            log_printf(0, "ERROR Missing secret key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR Missing secret key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }
        key = tbx_stk_unescape_text('\\', etext);
        if (zmq_setsockopt(socket->arg, ZMQ_CURVE_SECRETKEY, key, strlen(key)+1) != 0) {
            log_printf(0, "ERROR with secret key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR with secret key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }
        free(etext); free(key);

        etext = tbx_inip_get_string(ifd, section, "public_key", NULL);
        if (!etext) {
            log_printf(0, "ERROR Missing public key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR Missing public key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }
        key = tbx_stk_unescape_text('\\', etext);
        if (zmq_setsockopt(socket->arg, ZMQ_CURVE_PUBLICKEY, key, strlen(key)+1) != 0) {
            log_printf(0, "ERROR with public key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR with public key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }
        free(etext); free(key);

        k=1;
        zmq_setsockopt(socket->arg, ZMQ_CURVE_SERVER, &k, sizeof(k));
    } else {
        etext = tbx_inip_get_string(ifd, section, "public_key", NULL);
        if ((!etext) && (retry == 0)) {  //** Let's try again
            if (key_prefix) {
                key_prefix = NULL;
            } else {
                retry = 1;
            }
            free(section);
            tbx_inip_destroy(ifd);
            goto again;
        }
        if (!etext) {
            log_printf(0, "ERROR Missing public key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR Missing public key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }

        //** Set our encryption keys
        zmq_curve_keypair(public_key, secret_key);
        zmq_setsockopt(socket->arg, ZMQ_CURVE_PUBLICKEY, public_key, 41);
        zmq_setsockopt(socket->arg, ZMQ_CURVE_SECRETKEY, secret_key, 41);

        key = tbx_stk_unescape_text('\\', etext);
        if (zmq_setsockopt(socket->arg, ZMQ_CURVE_SERVERKEY, key, strlen(key)+1) != 0) {
            log_printf(0, "ERROR with server public key for host: %s  section=%s\n", id, section); tbx_log_flush();
            fprintf(stderr, "ERROR with server public key for host: %s  section=%s\n", id, section); fflush(stderr);
            exit(1);
        }
        free(etext); free(key);
    }

    free(section);
    tbx_inip_destroy(ifd);
    return;

fail:
    //** No config to load so no encryption
    fprintf(stderr, "ABORT: No matching entry encryption keys for: %s\n", id);
    log_printf(-1, "ABORT: No matching entry encryption keys for: %s\n", id);
    exit(1);
}

//*************************************************************

int zero_native_bind(gop_mq_socket_t *socket, const char *format, ...)
{
    va_list args;
    int err, n, v;
    char id[256];

    va_start(args, format);
    snprintf(id, 255, format, args);
    n = mq_id_bytes(id, strlen(id));
    if (socket->type != MQ_PAIR) {
        err = zmq_setsockopt(socket->arg, MQ_IDENTITY, id, n);
        if (err != 0) {
            log_printf(0, "ERROR setting socket identity! id=%s err=%d errno=%d\n", id, err, errno);
            return(-1);
        }

        v = 100*1000;
        zmq_setsockopt(socket->arg, ZMQ_RCVHWM, &v, sizeof(v));
    } else {
        id[0] = 0;
    }

    if (id[n] == '|') {
        n++;
    } else {
        n = 0;
    }

    //** See if we should enable encryption
    if (socket->type != MQ_PAIR) encrypt_socket(socket, id + n, 1);

    err = zmq_bind(socket->arg, &(id[n]));
    n = errno;
    va_end(args);

    log_printf(0, "id=!%s! err=%d errno=%d\n", id, err, n);

    return((err == -1) ? -1 : 0);
}

//*************************************************************

int zero_native_connect(gop_mq_socket_t *socket, const char *format, ...) {
    va_list args;
    int err, n;
    char buf[255], id[512];

    if (socket->type != MQ_PAIR) {
        int i = 1;
        zmq_setsockopt(socket->arg, ZMQ_ROUTER_MANDATORY, &i, sizeof(i));
        i = 100*1000;
        zmq_setsockopt(socket->arg, ZMQ_RCVHWM, &i, sizeof(i));
    }

    va_start(args, format);
    //** Set the ID
    if (socket->type != MQ_PAIR) {
        gethostname(buf, sizeof(buf));
        snprintf(id, sizeof(id), "%s:" I64T , buf, tbx_random_get_int64(1, 1000000)); id[sizeof(id)-1] = '\0';
        err = zmq_setsockopt(socket->arg, MQ_IDENTITY, id, strlen(id));
        if (err != 0) {
            log_printf(0, "ERROR setting socket identity! id=%s err=%d errno=%d\n", id, err, errno);
            return(-1);
        }
        log_printf(4, "Unique hostname created = %s\n", id);
    }

    snprintf(id, 255, format, args);
    n = mq_id_bytes(id, strlen(id));
    if (id[n] == '|') {
        n++;
    } else {
        n = 0;
    }

    //** See if we should enable encryption
    if (socket->type != MQ_PAIR) encrypt_socket(socket, id + n, 0);

    err = zmq_connect(socket->arg, &(id[n]));

    va_end(args);

    return(err);
}

//*************************************************************

int zero_native_disconnect(gop_mq_socket_t *socket, const char *format, ...)
{
    va_list args;
    int err;
    char id[256];

    va_start(args, format);

    snprintf(id, 255, format, args);
    err = zmq_disconnect(socket->arg, id);
    va_end(args);

    return(err);
}

//*************************************************************

void *zero_native_poll_handle(gop_mq_socket_t *socket)
{
    return(socket->arg);
}

//*************************************************************

int zero_native_monitor(gop_mq_socket_t *socket, char *address, int events)
{
    return(zmq_socket_monitor(socket->arg, address, events));
}

//*************************************************************

int _send_frame(gop_mq_socket_t *socket, char *data, int len, int flags)
{
    int loop, bytes;

    int count = 0;

    loop = 0;
    do {
        bytes = zmq_send(socket->arg, data, len, flags);
        if (bytes == -1) {
            if ((errno == EHOSTUNREACH) || (errno == EAGAIN)) {
                usleep(100);
            } else {
                log_printf(15, "ERROR: sending frame=%d len=%d bytes=%d errno=%d loop=%d\n", count, len, bytes, errno, loop);
                return(-1);
            }
        }
        loop++;
    } while ((bytes == -1) && (loop < 10));

    return(bytes);
}

//*************************************************************

int zero_native_send(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f, *fn;
    int n, bytes, len;

    int count = 0;

    n = 0;
    f = gop_mq_msg_first(msg);
    if (f->len > 1) {
        log_printf(5, "dest=!%.*s! nframes=%d\n", f->len, (char *)(f->data), tbx_stack_count(msg));
    } else {
        log_printf(5, "dest=(single byte) nframes=%d\n", tbx_stack_count(msg));
    }

    while ((fn = gop_mq_msg_next(msg)) != NULL) {
        len = (count > 0) ? f->len : mq_id_bytes(f->data, f->len); //** 1st frame we need to tweak the address
        bytes = _send_frame(socket, f->data, len, ZMQ_SNDMORE);
        if (bytes == -1) { return(-1); }  //** Kick out on an error
        n = n + len;
        count++;
        f = fn;
    }

    if (f != NULL) {
        bytes = _send_frame(socket, f->data, f->len, 0);
        if (bytes == -1) { return(-1); }  //** Kick out on an error
        n = n + len;
    }

    return(0);
}


//*************************************************************

int zero_native_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;
    int n, nframes, rc;
    int64_t more = 0;
    size_t msize = sizeof(more);

    if ((flags & MQ_DONTWAIT) > 0) {
        more = 0;
        rc = zmq_getsockopt (socket->arg, ZMQ_EVENTS, &more, &msize);
        log_printf(5, "more=" I64T "\n", more);
        FATAL_UNLESS(rc == 0);
        if ((more & ZMQ_POLLIN) == 0) return(-1);
    }

    n = 0;
    nframes = 0;
    do {
        tbx_type_malloc(f, gop_mq_frame_t, 1);
        gop_mq_frame_t *prevent_overwrite = f;
        FATAL_UNLESS(prevent_overwrite == f);

        rc = zmq_msg_init(&(f->zmsg));
        FATAL_UNLESS(rc == 0);
        rc = zmq_msg_recv(&(f->zmsg), socket->arg, flags);
        FATAL_UNLESS(rc != -1);

        rc = zmq_getsockopt (socket->arg, ZMQ_RCVMORE, &more, &msize);
        FATAL_UNLESS(rc == 0);

        f->len = zmq_msg_size(&(f->zmsg));
        f->data = zmq_msg_data(&(f->zmsg));
        f->auto_free = MQF_MSG_INTERNAL_FREE;

        gop_mq_msg_append_frame(msg, f);
        n += f->len;
        nframes++;
        log_printf(5, "more=" I64T "\n", more);
        FATAL_UNLESS(prevent_overwrite == f);
    } while (more > 0);

    log_printf(5, "total bytes=%d nframes=%d\n", n, nframes);

    return((n>0) ? 0 : -1);
}

//*************************************************************

gop_mq_socket_t *zero_create_native_socket(gop_mq_socket_context_t *ctx, int stype)
{
    gop_mq_socket_t *s;
    int i;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = stype;
    s->arg = zmq_socket(ctx->arg, stype);
    if (s->arg == NULL) {
        free(s);
        log_printf(0, "ERROR creating the socket!\n");
        return(NULL);
    }
    i = 0; zmq_setsockopt(s->arg, ZMQ_LINGER, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_SNDHWM, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_RCVHWM, &i, sizeof(i));
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_native_recv;

    return(s);
}


//*************************************************************
//   TRACE_ROUTER routines
//      Send: Pop and route
//      Recv: Append sender
//*************************************************************

//*************************************************************

int zero_trace_router_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;

    int n = zero_native_recv(socket, msg, flags);

    if (n != -1) {  //** Move the sender from the top to the bottom
        f = mq_msg_pop(msg);
        gop_mq_msg_append_frame(msg, f);
    }
    return(n);
}

//*************************************************************

gop_mq_socket_t *zero_create_trace_router_socket(gop_mq_socket_context_t *ctx)
{
    gop_mq_socket_t *s;
    int i;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = MQ_TRACE_ROUTER;
    s->arg = zmq_socket(ctx->arg, ZMQ_ROUTER);
    FATAL_UNLESS(s->arg);
    i = 0; zmq_setsockopt(s->arg, ZMQ_LINGER, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_SNDHWM, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_RCVHWM, &i, sizeof(i));
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_trace_router_recv;

    return(s);
}

//*************************************************************
//   SIMPLE_ROUTER routines
//      Send: Pop and route
//      Recv: pass thru
//*************************************************************

//*************************************************************

int zero_simple_router_recv(gop_mq_socket_t *socket, mq_msg_t *msg, int flags)
{
    gop_mq_frame_t *f;

    int n = zero_native_recv(socket, msg, flags);

    if (n != -1) {  //** Remove sender which 0MQ router added
        f = mq_msg_pop(msg);
        gop_mq_frame_destroy(f);
    }
    return(n);
}

//*************************************************************

gop_mq_socket_t *zero_create_simple_router_socket(gop_mq_socket_context_t *ctx)
{
    gop_mq_socket_t *s;
    int i;

    tbx_type_malloc_clear(s, gop_mq_socket_t, 1);

    s->type = MQ_SIMPLE_ROUTER;
    s->arg = zmq_socket(ctx->arg, ZMQ_ROUTER);
    i = 0; zmq_setsockopt(s->arg, ZMQ_LINGER, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_SNDHWM, &i, sizeof(i));
    i = 100000; zmq_setsockopt(s->arg, ZMQ_RCVHWM, &i, sizeof(i));
    s->destroy = zero_native_destroy;
    s->bind = zero_native_bind;
    s->connect = zero_native_connect;
    s->disconnect = zero_native_disconnect;
    s->poll_handle = zero_native_poll_handle;
    s->monitor = zero_native_monitor;
    s->send = zero_native_send;
    s->recv = zero_simple_router_recv;

    return(s);
}


//*************************************************************
// zero_create_socket  - Creates an MQ socket based o nthe given type
//*************************************************************

gop_mq_socket_t *zero_create_socket(gop_mq_socket_context_t *ctx, int stype)
{
    gop_mq_socket_t *s = NULL;
    log_printf(15, "\t\tstype=%d\n", stype);
    switch (stype) {
    case MQ_DEALER:
    case MQ_PAIR:
        s = zero_create_native_socket(ctx, stype);
        break;
    case MQ_TRACE_ROUTER:
        s = zero_create_trace_router_socket(ctx);
        break;
    case MQ_SIMPLE_ROUTER:
        s = zero_create_simple_router_socket(ctx);
        break;
    default:
        log_printf(0, "Unknown socket type: %d\n", stype);
        free(s);
        s = NULL;
    }

    return(s);
}

//*************************************************************
//  zero_socket_context_destroy - Destroys the 0MQ based context
//*************************************************************

void zero_socket_context_destroy(gop_mq_socket_context_t *ctx)
{
    //** Kludge to get around race issues in 0mq when closing sockets manually vs letting
    //** zctx_destroy() close them
    zmq_ctx_destroy(ctx->arg);
    free(ctx);
}

//*************************************************************
//  gop_zero_socket_context_new - Creates a new MQ context based on 0MQ
//*************************************************************

gop_mq_socket_context_t *gop_zero_socket_context_new()
{
    gop_mq_socket_context_t *ctx;

    tbx_type_malloc_clear(ctx, gop_mq_socket_context_t, 1);

    ctx->arg = zmq_ctx_new();
    FATAL_UNLESS(ctx->arg != NULL);
    ctx->create_socket = zero_create_socket;
    ctx->destroy = zero_socket_context_destroy;

    //** Disable the 0mq SIGINT/SIGTERM signale handler
    apr_signal(SIGINT, NULL);
    apr_signal(SIGTERM, NULL);

    return(ctx);
}
