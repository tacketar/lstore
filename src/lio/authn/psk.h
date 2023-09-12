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

//***********************************************************************
// Pre-shared Key AuthN implementation
//***********************************************************************

#ifndef _AUTHN_PSK_H_
#define _AUTHN_PSK_H_

#include <gop/mq.h>
#include <gop/mq_ongoing.h>
#include <lio/authn.h>
#include <lio/notify.h>
#include <apr_thread_cond.h>

#include "authn.h"
#ifdef __cplusplus
extern "C" {
#endif

#define AUTHN_TYPE_PSK_CLIENT "authn_psk_client"
#define AUTHN_TYPE_PSK_SERVER "authn_psk_server"

#define SALT_BYTES 32

#define PSK_CLIENT_AUTHN_KEY      "authn_psk_client"
#define PSK_CLIENT_AUTHN_KEY_SIZE sizeof(PSK_CLIENT_AUTHN_KEY)

typedef struct {
    mq_msg_t *remote_host;         //** Address of the Remote OS server
    char *remote_host_string;      //** Stringified version of Remote OS server
    gop_mq_context_t *mqc;         //** Portal for connecting to the remote AuthN server
    char *section;
    char *host_id;
    int host_id_len;
    int heartbeat;
    gop_mq_ongoing_t *ongoing;     //** Ongoing handle to reap credentials
    notify_t *notify;
} lio_authn_psk_client_priv_t;

typedef struct {
    apr_hash_t *accounts;   //** List of all the account
    apr_hash_t *creds;      //** List of all the creds
    apr_pool_t *mpool;      //** Needs it's own separate pool since apr_hash doesn't have a destroy
}  psk_context_t;

typedef struct {
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    apr_pool_t *mpool;
    gop_mq_context_t *mqc;         //** Portal for connecting to the remote AuthN server
    gop_mq_portal_t *server_portal;
    gop_mq_ongoing_t *ongoing;     //** Ongoing handle to reap credentials
    psk_context_t *ctx;
    char *section;
    char *fname;
    char *hostname;                //** Optional host if using dedicated portal
    int check_interval;
    int ongoing_interval;
    int shutdown;
    time_t modify_time;
    gop_thread_pool_context_t *tpc;
    notify_t *notify;
} lio_authn_psk_server_priv_t;

lio_authn_t *authn_psk_client_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);
lio_authn_t *authn_psk_server_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);

#ifdef __cplusplus
}
#endif

#endif
