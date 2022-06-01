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
// Server side Pre-shared Key AuthN
//***********************************************************************

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sodium.h>
#include <ex3/system.h>
#include <gop/mq.h>
#include <gop/mq_helpers.h>
#include <gop/mq_stream.h>
#include <tbx/apr_wrapper.h>
#include <tbx/assert_result.h>
#include <tbx/chksum.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "authn.h"
#include "psk.h"
#include "service_manager.h"

#define PSK_HANDLE_LEN 32

typedef struct {
    char *account;
    char *key;
    int key_len;
    tbx_stack_t *creds;
    psk_context_t *old_ctx;
} psk_account_t;

typedef struct {
    lio_creds_t c;
    lio_authn_t *an;
    psk_account_t *a;
    tbx_stack_ele_t *ele;
    char handle[PSK_HANDLE_LEN];
    int count;
} psk_creds_t;

//***********************************************************************
// _psk_destroy -Destroys the PSK context
//***********************************************************************

void _psk_destroy(lio_authn_t *an)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    psk_context_t *ctx = ap->ctx;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    psk_account_t *pa;
    psk_creds_t *pc;

    //** Teardown the accounts and all creds
    for (hi=apr_hash_first(NULL, ctx->accounts); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&pa);
        while ((pc = tbx_stack_pop(pa->creds)) != NULL) {
            if (pc->c.id != NULL) free(pc->c.id);
            if (pc->c.descriptive_id) free(pc->c.descriptive_id);
            free(pc);
        }

        tbx_stack_free(pa->creds, 0);
        free(pa->account);
        free(pa->key);
        free(pa);
    }

    //** This destroys both hashes.  Everything in the creds hash was handled in the account purge
    apr_pool_destroy(ctx->mpool);
    free(ctx);
}

//***********************************************************************
// _psk_load - Load the creds for use. Each cred has the form:
//
//   [account-<account>]
//   key=<Pre-shared key>
//***********************************************************************

void _psk_load(lio_authn_t *an)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    tbx_inip_file_t *fd;
    psk_context_t *ctx;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *value, *account, *etpsk;
    psk_account_t *a;
    psk_creds_t *pc;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    char *psk, *text;
    int n;

    fd = tbx_inip_file_read(ap->fname, 1);
    if (!fd) {
        log_printf(0, "ERROR: Can't open the PSK file! fname:%s\n", ap->fname);
        fprintf(stderr, "ERROR: Can't open the PSK file! fname:%s\n", ap->fname);
        return;
    }

    //** Make the new context
    tbx_type_malloc_clear(ctx, psk_context_t, 1);
    assert_result(apr_pool_create(&(ctx->mpool), NULL), APR_SUCCESS);
    ctx->accounts = apr_hash_make(ctx->mpool);
    ctx->creds = apr_hash_make(ctx->mpool);

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        if (strncmp("account-", tbx_inip_group_get(ig), 8) == 0) { //** Got a PSK
            value = tbx_inip_group_get(ig);
            account = value + 8;  //** Skip over the prefix
            ele = tbx_inip_ele_first(ig);
            etpsk = NULL;
            while (ele != NULL) {
                key = tbx_inip_ele_get_key(ele);
                value = tbx_inip_ele_get_value(ele);
                if (strcmp(key, "key") == 0) etpsk = value;  //** Got a PSK
                ele = tbx_inip_ele_next(ele);
            }

            if (account && etpsk) { //** Got a valid entry
                //** Convert the PSK to binary
                text = tbx_stk_unescape_text('\\', etpsk);
                n = strlen(text);
                tbx_type_malloc_clear(psk, char, n);  //** The actual size needed is 0.8*strlen(text)+1
                zmq_z85_decode((unsigned char *)psk, text);
                n = 0.8*n;
                free(text);

                //** See if it exists in the old context
                a = (ap->ctx) ? apr_hash_get(ap->ctx->accounts, account, APR_HASH_KEY_STRING) : NULL;
                if (a) {  //** It's in the old table
                    if ((n == a->key_len) && (memcmp(a->key, psk, n) == 0)) {  //** And the PSK's are the same so just move everything over
                        free(psk);
                        apr_hash_set(ctx->accounts, a->account, APR_HASH_KEY_STRING, a);  //** Add it to the new one
                        apr_hash_set(ap->ctx->accounts, a->account, APR_HASH_KEY_STRING, NULL);  //** Add remove it from the old one

                        //** Now add all the creds to the new table and remove them from the old
                        tbx_stack_move_to_top(a->creds);

                        while ((pc = tbx_stack_get_current_data(a->creds)) != NULL) {
                            apr_hash_set(ctx->creds, pc->handle, PSK_HANDLE_LEN, pc);
                            apr_hash_set(ap->ctx->creds, pc->handle, PSK_HANDLE_LEN, NULL);
                            tbx_stack_move_down(a->creds);
                        }
                    } else {
                        if (tbx_stack_count(a->creds) == 0) { //** No creds so can safely remove
                            apr_hash_set(ap->ctx->accounts, a->account, APR_HASH_KEY_STRING, NULL);  //** Add remove it from the old one
                            tbx_stack_free(a->creds, 0);
                            free(a->account);
                            free(a->key);
                            free(a);
                        }
                        a = NULL;
                    }
                }

                //** check if it's new
                if (!a) {
                    tbx_type_malloc_clear(a, psk_account_t, 1);
                    a->creds = tbx_stack_new();
                    a->account = strdup(account);
                    a->key = psk;
                    a->key_len = n;
                    apr_hash_set(ctx->accounts, a->account, APR_HASH_KEY_STRING, a);  //** Add it to the new one
                }
            }
        }

        ig = tbx_inip_group_next(ig);
    }

    tbx_inip_destroy(fd);

    //** Handle the remainders by flagging them to be purged
    if (ap->ctx) {
        if (apr_hash_count(ap->ctx->accounts) > 0) {
            for (hi=apr_hash_first(NULL, ap->ctx->accounts); hi != NULL; hi = apr_hash_next(hi)) {
                apr_hash_this(hi, NULL, &hlen, (void **)&a);
                a->old_ctx = ap->ctx;
            }
        } else { //** We can go ahead and clean up
            apr_pool_destroy(ap->ctx->mpool);
            free(ap->ctx);
        }
    }
    ap->ctx = ctx;  //** Swing the context
}

//***********************************************************************
//  psk_check_thread - checks for changes in the PSK's
//***********************************************************************

void *psk_check_thread(apr_thread_t *th, void *data)
{
    lio_authn_t *an = data;
    lio_authn_psk_server_priv_t *ap = an->priv;
    apr_time_t dt;
    struct stat sbuf;
    time_t modify_time;

    tbx_monitor_thread_create(MON_MY_THREAD, "psk_check_thread: Monitoring file=%s", ap->fname);
    dt = apr_time_from_sec(ap->check_interval);
    sbuf.st_mtime = 0; stat(ap->fname, &sbuf); modify_time = sbuf.st_mtime;

    apr_thread_mutex_lock(ap->lock);
    do {
        log_printf(5, "LOOP START check_interval=%d\n", ap->check_interval);
        tbx_monitor_thread_message(MON_MY_THREAD, "Checking");
        if (stat(ap->fname, &sbuf) != 0) {
            tbx_monitor_thread_message(MON_MY_THREAD, "ERROR: PSK file missing!");
            log_printf(1, "PSK file missing!!! Using old definition. fname=%s\n", ap->fname);
        } else if (modify_time != sbuf.st_mtime) {  //** File changed so reload it
            modify_time = sbuf.st_mtime;
            tbx_monitor_thread_message(MON_MY_THREAD, "Reloading");
            _psk_load(an);  //** Do a quick check and see if the file has changed
        }
        if (ap->shutdown == 0) apr_thread_cond_timedwait(ap->cond, ap->lock, dt);
    } while (ap->shutdown == 0);
    apr_thread_mutex_unlock(ap->lock);

    return(NULL);
}

//***********************************************************************
// authn_psk_server_get_type - Returns the type
//***********************************************************************

char *authn_psk_server_get_type(lio_creds_t *c)
{
    return(AUTHN_TYPE_PSK_SERVER);
}

//***********************************************************************
// apsk_cred_init - Returns the internal creds if they exists
//***********************************************************************

lio_creds_t *apsk_cred_init(lio_authn_t *an, int type, void **args)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    lio_creds_t *c = NULL;
    psk_creds_t *pc;

    if (type == AUTHN_INIT_LOOKUP) {
        apr_thread_mutex_lock(ap->lock);
        pc = apr_hash_get(ap->ctx->creds, args[0], *(int *)args[1]);
        if (pc) {
            pc->count++;
            c = &(pc->c);
        }
        apr_thread_mutex_unlock(ap->lock);
    }
    return(c);
}

//***********************************************************************
//  apsk_cred_destroy - Releases the credential
//***********************************************************************

void apsk_cred_destroy(lio_creds_t *c)
{
    psk_creds_t *pc = c->priv;
    lio_authn_psk_server_priv_t *ap = pc->an->priv;
    psk_context_t *ctx;
    psk_account_t *a;

    apr_thread_mutex_lock(ap->lock);
    pc->count--;
    if (pc->count == 0) {  //** This one is done so reap it
        if (pc->c.id != NULL) free(pc->c.id);
        if (pc->c.descriptive_id) free(pc->c.descriptive_id);
        tbx_stack_move_to_ptr(pc->a->creds, pc->ele);
        tbx_stack_delete_current(pc->a->creds, 0, 0);

        apr_hash_set(ap->ctx->creds, pc->c.handle, pc->c.handle_len, NULL);

        a = pc->a; //** Get the account before freeing the creds
        free(pc);

        if (a->old_ctx) { //** check if we do garbage collection
            ctx = a->old_ctx;
            if (tbx_stack_count(a->creds) == 0) {
                apr_hash_set(ctx->accounts, a->account, APR_HASH_KEY_STRING, NULL);
                free(a->account);
                free(a->key);
                tbx_stack_free(a->creds, 0);
                free(a);

                if (apr_hash_count(ctx->accounts) == 0) { //** Nothing left
                    apr_pool_destroy(ctx->mpool);
                    free(ctx);
                }
            }
        }
    }
    apr_thread_mutex_unlock(ap->lock);

    return;
}

//***********************************************************************
// apsk_login - Does the Actual AuthN
//***********************************************************************

lio_creds_t *apsk_login(lio_authn_t *an, char *id, int id_len, char *did, int did_len, char *encrypted, int encrypted_len, char *nonce, int nonce_len, char **nonce_new, char **p_new, int *p_len)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    psk_creds_t *pc;
    psk_account_t *a;
    lio_creds_t *c = NULL;
    int n;
    char decrypted[encrypted_len+1024];

    apr_thread_mutex_lock(ap->lock);
    a = apr_hash_get(ap->ctx->accounts, id, APR_HASH_KEY_STRING);
    if (!a) goto fail;

    //** Decrypt the message
    if (nonce_len != crypto_secretbox_NONCEBYTES) goto fail;
    n = encrypted_len-crypto_secretbox_MACBYTES;
    if (n < SALT_BYTES) goto fail;
    if (crypto_secretbox_open_easy((unsigned char *)decrypted, (unsigned char *)encrypted, encrypted_len, (unsigned char *)nonce, (unsigned char *)a->key) != 0) goto fail;

    decrypted[n] = '\0';

    //** Validate the did matches
    if (strcmp(did, decrypted + SALT_BYTES) != 0) goto fail;

    //** Make the new creds
    tbx_type_malloc_clear(pc, psk_creds_t, 1);
    cred_default_init(&(pc->c), NULL);
    pc->an = an;
    pc->count = 1;
    pc->c.priv = pc;
    pc->c.destroy = apsk_cred_destroy;
    pc->c.id = strdup(id); pc->c.id_len = strlen(id);
    pc->c.descriptive_id = strdup(did); pc->c.descriptive_id_len = strlen(did);
    tbx_random_get_bytes(pc->handle, PSK_HANDLE_LEN);
    pc->c.handle = pc->handle;
    pc->c.handle_len = PSK_HANDLE_LEN;
    pc->a = a;
    tbx_stack_push(a->creds, pc);
    pc->ele = tbx_stack_get_current_ptr(a->creds);
    apr_hash_set(ap->ctx->creds, pc->c.handle, pc->c.handle_len, pc);
    c = &(pc->c);

    //** Package it up using the original salt
    tbx_type_malloc_clear(*nonce_new, char, crypto_secretbox_NONCEBYTES);
    tbx_random_get_bytes(*nonce_new, crypto_secretbox_NONCEBYTES);  //** This is the nonce
    *p_len = crypto_secretbox_MACBYTES + SALT_BYTES + c->handle_len;
    tbx_type_malloc_clear(*p_new, char, *p_len);
    memcpy(decrypted + SALT_BYTES, c->handle, c->handle_len);
    crypto_secretbox_easy((unsigned char *)(*p_new), (unsigned char *)decrypted, SALT_BYTES+c->handle_len, (unsigned char *)(*nonce_new), (unsigned char *)a->key);

fail:
    apr_thread_mutex_unlock(ap->lock);

    return(c);
}

//***********************************************************************
// apsk_cred_logout_fn - Actual cred logout routine
//***********************************************************************

gop_op_status_t apsk_cred_logout_fn(void *arg, int id)
{
    lio_creds_t *c = arg;

    an_cred_destroy(c);

    return(gop_success_status);
}

//***********************************************************************
// apsk_cred_logout_gop - Does the logout when clients close
//***********************************************************************

gop_op_generic_t *apsk_cred_logout_gop(void *arg, void *handle)
{
    lio_authn_t *an = arg;
    lio_authn_psk_server_priv_t *ap = an->priv;
    lio_creds_t *c = handle;

    return(gop_tp_op_new(ap->tpc, NULL, apsk_cred_logout_fn, (void *)c, NULL, 1));
}

//***********************************************************************
// apsk_authn_cb - Processes the Login callback
//***********************************************************************

void apsk_authn_cb(void *arg, gop_mq_task_t *task)
{
    lio_authn_t *an = arg;
    lio_creds_t *c;
    lio_authn_psk_server_priv_t *ap = an->priv;
    gop_mq_frame_t *cid, *fid, *fdid, *fnonce, *fpacket, *fhb;
    char *id, *did, *encrypted, *nonce, *nonce_new, *return_packet, *hb;
    int id_len, did_len, encrypted_len, nonce_len, hb_len, return_len;
    mq_msg_t *msg, *response;
    gop_op_status_t status;

    log_printf(5, "Processing incoming request\n");

    //** Parse the command.
    msg = task->msg;
    gop_mq_remove_header(msg, 0);

    cid = mq_msg_pop(msg);  //** This is the command ID
    gop_mq_frame_destroy(mq_msg_pop(msg));  //** Drop the application command frame

    fid = mq_msg_pop(msg);  //** This has the Account/ID
    gop_mq_get_frame(fid, (void **)&id, &id_len);
    fdid = mq_msg_pop(msg);  //** This has the Descriptive Account/ID
    gop_mq_get_frame(fdid, (void **)&did, &did_len);
    fnonce = mq_msg_pop(msg);  //** This has the nonce
    gop_mq_get_frame(fnonce, (void **)&nonce, &nonce_len);
    fpacket = mq_msg_pop(msg);  //** This has the encrypted packet
    gop_mq_get_frame(fpacket, (void **)&encrypted, &encrypted_len);

    //** Make the new creds
    c = apsk_login(an, id, id_len, did, did_len, encrypted, encrypted_len, nonce, nonce_len, &nonce_new, &return_packet, &return_len);
    if (c) {
        status = gop_success_status;
        fhb = mq_msg_pop(msg);  //** This has the heartbeat from for tracking
        gop_mq_get_frame(fhb, (void **)&hb, &hb_len);
        gop_mq_ongoing_add(ap->ongoing, 1, hb, hb_len, (void *)c, (gop_mq_ongoing_fail_fn_t)apsk_cred_logout_gop, an);
        gop_mq_frame_destroy(fhb);
    } else {
        gop_mq_frame_destroy(mq_msg_pop(msg));  //** This has the heartbeat from for tracking which we don't need on failure
        status = gop_failure_status;
    }

    gop_mq_frame_destroy(fid);
    gop_mq_frame_destroy(fdid);
    gop_mq_frame_destroy(fpacket);
    gop_mq_frame_destroy(fnonce);

    //** Form the response
    response = gop_mq_make_response_core_msg(msg, cid);
    gop_mq_msg_append_frame(response, gop_mq_make_status_frame(status));  //** Status
    if (c) {  //** Add the nonce and creds packet
        gop_mq_msg_append_mem(response, nonce_new, crypto_secretbox_NONCEBYTES, MQF_MSG_AUTO_FREE);  //** Nonce
        gop_mq_msg_append_mem(response, return_packet, return_len, MQF_MSG_AUTO_FREE);  //** Creds handle
    }
    gop_mq_msg_append_mem(response, NULL, 0, MQF_MSG_KEEP_DATA);  //** Empty frame

    //** Lastly send it
    gop_mq_submit(ap->server_portal, gop_mq_task_new(ap->mqc, response, NULL, NULL, 30));
    log_printf(5, "END status=%d\n", status.op_status);
}

//***********************************************************************
// apsk_server_account_print_'running_config - Prints the running config accounts
//***********************************************************************

void apsk_server_accounts_print_running_config(lio_authn_t *an, FILE *fd)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    psk_context_t *ctx = ap->ctx;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    psk_account_t *pa;
    psk_creds_t *pc;
    char key_z85[128];
    char *key_escaped;
    int n;

    fprintf(fd, "#----------------PSK Accounts start------------------\n");

    apr_thread_mutex_lock(ap->lock);
    for (hi=apr_hash_first(NULL, ctx->accounts); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&pa);
        fprintf(fd, "[account-%s]\n", pa->account);
        zmq_z85_encode(key_z85, (unsigned char *)pa->key, pa->key_len);
        key_escaped = tbx_stk_escape_text(TBX_INIP_ESCAPE_CHARS, '\\', key_z85);
        n = strlen(key_escaped);
        if (n<40) {
            fprintf(fd, "ERROR: Key is to short! encrypted keylen=%d\n", pa->key_len);
        } else {
            key_escaped[8] = '\0';
            fprintf(fd, "key=%s...%s\n", key_escaped, key_escaped + n - 8);
        }
        free(key_escaped);
        tbx_stack_move_to_top(pa->creds);
        while ((pc = tbx_stack_get_current_data(pa->creds)) != NULL) {
            fprintf(fd, "creds=%s\n", an_cred_get_descriptive_id(&(pc->c), NULL));
            tbx_stack_move_down(pa->creds);
        }
        fprintf(fd, "\n");
    }
    apr_thread_mutex_unlock(ap->lock);

    fprintf(fd, "#----------------PSK Accounts end------------------\n");
    fprintf(fd, "\n");
}

//***********************************************************************
// apsk_server_print_running_config - Prints the running config
//***********************************************************************

void apsk_server_print_running_config(lio_authn_t *an, FILE *fd, int print_section_heading)
{
    lio_authn_psk_server_priv_t *ap = an->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", ap->section);
    fprintf(fd, "type = %s\n", AUTHN_TYPE_PSK_SERVER);
    if (ap->hostname) {
        fprintf(fd, "address = %s\n", ap->hostname);
        fprintf(fd, "ongoing_interval = %d #seconds\n", ap->ongoing_interval);
    } else {
        fprintf(fd, "# Using global host portal for address\n");
        fprintf(fd, "# Using global host portal for ongoing_interval\n");
    }
    fprintf(fd, "fname = %s\n", ap->fname);
    fprintf(fd, "check_interval = %d\n", ap->check_interval);
    fprintf(fd, "\n");

    //** Now dump the accounts and active credentials
    apsk_server_accounts_print_running_config(an, fd);
}

//***********************************************************************
// authn_psk_server_destroy - Destroys the PSK server AuthN service
//***********************************************************************

void authn_psk_server_destroy(lio_authn_t *an)
{
    lio_authn_psk_server_priv_t *ap = an->priv;
    apr_status_t value;

    ap->shutdown = 1;
    apr_thread_cond_broadcast(ap->cond);
    apr_thread_mutex_unlock(ap->lock);

    //** Wait for it to shutdown
    apr_thread_join(&value, ap->check_thread);

    //** Remove the server portal if needed)
    if (ap->hostname) {
        gop_mq_portal_remove(ap->mqc, ap->server_portal);
        gop_mq_ongoing_destroy(ap->ongoing);  //** Shutdown the ongoing thread and task
        gop_mq_portal_destroy(ap->server_portal);
        free(ap->hostname);
    }

    _psk_destroy(an);

    if (ap->section) free(ap->section);
    if (ap->fname) free(ap->fname);

    apr_thread_mutex_destroy(ap->lock);
    apr_thread_cond_destroy(ap->cond);
    apr_pool_destroy(ap->mpool);
    free(ap);
    free(an);
}

//***********************************************************************
// authn_psk_server_create - Create a Fake AuthN service
//***********************************************************************

lio_authn_t *authn_psk_server_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section)
{
    lio_authn_t *an;
    lio_authn_psk_server_priv_t *ap;
    gop_mq_command_table_t *ctable;

    tbx_type_malloc(an, lio_authn_t, 1);
    tbx_type_malloc(ap, lio_authn_psk_server_priv_t, 1);
    an->priv = ap;

    ap->section = strdup(section);
    ap->fname = tbx_inip_get_string(ifd, section, "fname", "psk.cfg");  //** Get the file to monitor
    ap->check_interval = tbx_inip_get_integer(ifd, section, "check_interval", 60);
    ap->ongoing_interval = tbx_inip_get_integer(ifd, section, "check_interval", 60);
    ap->hostname = tbx_inip_get_string(ifd, section, "address", NULL);

    //** Make the locks and cond variables
    assert_result(apr_pool_create(&(ap->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(ap->lock), APR_THREAD_MUTEX_DEFAULT, ap->mpool);
    apr_thread_cond_create(&(ap->cond), ap->mpool);

    an->print_running_config = apsk_server_print_running_config;
    an->cred_init = apsk_cred_init;
    an->destroy = authn_psk_server_destroy;

    //** Load the credentials
    _psk_load(an);

    //** Spawn the thread checking for changes
    tbx_thread_create_assert(&(ap->check_thread), NULL, psk_check_thread, (void *)an, ap->mpool);

    //** Get the thread pool
    ap->tpc = lio_lookup_service(ess, ESS_RUNNING, ESS_TPC_UNLIMITED); FATAL_UNLESS(ap->tpc != NULL);

    //** Get the MQC
    ap->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ); FATAL_UNLESS(ap->mqc != NULL);

    //** Install the commands in the server portal and create the ongoing object if needed
    if (ap->hostname) {
        ap->server_portal = gop_mq_portal_create(ap->mqc, ap->hostname, MQ_CMODE_SERVER);
    } else {
        ap->server_portal = lio_lookup_service(ess, ESS_RUNNING, ESS_SERVER_PORTAL); FATAL_UNLESS(ap->server_portal != NULL);
    }

    //** Get the command table and install our command
    ctable = gop_mq_portal_command_table(ap->server_portal);
    gop_mq_command_set(ctable, PSK_CLIENT_AUTHN_KEY, PSK_CLIENT_AUTHN_KEY_SIZE, an, apsk_authn_cb);

    //** See if we need to set up our own ongoing process
    if (ap->hostname) {
        ap->ongoing = gop_mq_ongoing_create(ap->mqc, ap->server_portal, ap->ongoing_interval, ONGOING_SERVER);
        FATAL_UNLESS(ap->ongoing != NULL);

        //** This is to handle client stream responses
        gop_mq_command_set(ctable, MQS_MORE_DATA_KEY, MQS_MORE_DATA_SIZE, ap->ongoing, gop_mqs_server_more_cb);
        gop_mq_portal_install(ap->mqc, ap->server_portal);
    } else {
        ap->ongoing = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_SERVER); FATAL_UNLESS(ap->ongoing != NULL);
    }

    return(an);
}
