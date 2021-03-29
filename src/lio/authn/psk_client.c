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
// client side Pre-shared Key AuthN
//***********************************************************************

#include <sys/types.h>
#include <sys/stat.h>
#include <linux/limits.h>
#include <sodium.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gop/mq_helpers.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>
#include <ex3/system.h>

#include "authn.h"
#include "psk.h"
#include "service_manager.h"

typedef struct {
    char *psk;
    char *salt;
    lio_creds_t *c;
} psk_exchange_state_t;

static lio_authn_psk_client_priv_t psk_default_options = {
    .remote_host_string = "${host}",
};

//***********************************************************************
// authn_psk_client_get_type - Returns the type
//***********************************************************************

char *authn_psk_client_get_type(lio_creds_t *c)
{
    return(AUTHN_TYPE_PSK_CLIENT);
}

//***********************************************************************
// authn_psk_client_cred_destroy - Destroy the psk_client credentials
//***********************************************************************

void authn_psk_client_cred_destroy(lio_creds_t *c)
{
    if (c->handle != NULL) free(c->handle);
    if (c->id != NULL) free(c->id);
    if (c->descriptive_id != NULL) free(c->descriptive_id);
    free(c);
}

//***********************************************************************
// psk_response_exchange - Handles the PSK exchange response
//***********************************************************************

gop_op_status_t psk_response_exchange(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    psk_exchange_state_t *pxs = task->arg;
    lio_creds_t *c = pxs->c;
    lio_authn_t *an = c->priv;
    lio_authn_psk_client_priv_t *ap = an->priv;
    gop_op_status_t status;
    unsigned char *nonce;
    char *data, *decrypted;
    int len, nonce_len;

    log_printf(10, "Processing PSK response. account=%s\n", an_cred_get_id(c, NULL));

    //** Parse the response
    gop_mq_remove_header(task->response, 1);

    //** Get the status frame
    status = gop_mq_read_status_frame(gop_mq_msg_first(task->response), 0);

    log_printf(10, "Processing PSK response. account=%s status=%d\n", an_cred_get_id(c, NULL), status.op_status);

    if (status.op_status != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR: PSK exchange failed! account:%s\n", an_cred_get_id(c, NULL));
        fprintf(stderr, "ERROR: PSK exchange failed! account:%s\n", an_cred_get_id(c, NULL));
        exit(1);
    }

    //** Get the nonce
    gop_mq_get_frame(gop_mq_msg_next(task->response), (void **)&nonce, &nonce_len);
    if (nonce_len != crypto_secretbox_NONCEBYTES) {
        log_printf(0, "ERROR: PSK exchange failed! Missing nonce! account:%s nonce_len=%d\n", an_cred_get_id(c, NULL), nonce_len);
        fprintf(stderr, "ERROR: PSK exchange failed! Missing nonce! account:%s nonce_len=%d\n", an_cred_get_id(c, NULL), nonce_len);
        exit(2);
    }

    //** This is the frame with the encrypted handle
    gop_mq_get_frame(gop_mq_msg_next(task->response), (void **)&data, &len);
log_printf(0, "nonce_len=%d packet_len=%d\n", nonce_len, len); tbx_log_flush();
fprintf(stderr, "nonce_len=%d packet_len=%d\n", nonce_len, len); fflush(stderr);
char xbuf[256];
tbx_chksum_bin2hex(nonce_len, nonce, xbuf);
log_printf(0, "account=%s nonce=%s\n", an_cred_get_id(c, NULL), xbuf);
tbx_chksum_bin2hex(len, (unsigned char *)data, xbuf);
log_printf(0, "account=%s return=%s\n", an_cred_get_id(c, NULL), xbuf);
tbx_chksum_bin2hex(crypto_secretbox_KEYBYTES, (unsigned char *)pxs->psk, xbuf);
log_printf(0, "account=%s key=%s\n", an_cred_get_id(c, NULL), xbuf);
tbx_log_flush();
    if (data == NULL) {
        log_printf(0, "ERROR: PSK exchange failed! Missing creds handle! account:%s\n", an_cred_get_id(c, NULL));
        fprintf(stderr, "ERROR: PSK exchange failed! Missing creds handle! account:%s\n", an_cred_get_id(c, NULL));
        exit(3);
    }

    //** Decrypt the message
    tbx_type_malloc_clear(decrypted, char, len);
    if (crypto_secretbox_open_easy((unsigned char *)decrypted, (unsigned char *)data, len, nonce, (unsigned char *)pxs->psk) != 0) {
        free(decrypted);
        log_printf(0, "ERROR: PSK exchange failed! Forged message! account:%s\n", an_cred_get_id(c, NULL));
        fprintf(stderr, "ERROR: PSK exchange failed! Forged message! account:%s\n", an_cred_get_id(c, NULL));
        exit(4);
    }

    //** Validate it by checking the salt
    if (memcmp(decrypted, pxs->salt, SALT_BYTES) != 0) {
        free(decrypted);
        log_printf(0, "ERROR: PSK exchange failed! Salt doesn't match! account:%s\n", an_cred_get_id(c, NULL));
        fprintf(stderr, "ERROR: PSK exchange failed! Salt doesn't match! account:%s\n", an_cred_get_id(c, NULL));
        exit(5);
    }

    //** Get the handle
    c->handle_len = len - crypto_secretbox_MACBYTES - SALT_BYTES;
log_printf(0, "account=%s handle_len=%d\n", an_cred_get_id(c, NULL), c->handle_len);
    tbx_type_malloc(c->handle, char, c->handle_len);
    memcpy(c->handle, decrypted + SALT_BYTES, c->handle_len);

    free(decrypted);

char buf[2*c->handle_len+1];
tbx_chksum_bin2hex(c->handle_len, (unsigned char *)c->handle, buf);
log_printf(0, "account=%s did=%s hlen=%d handle=%s\n", an_cred_get_id(c, NULL), an_cred_get_descriptive_id(c, NULL), c->handle_len, buf);

    //** Add it to the tracking
    gop_mq_ongoing_host_inc(ap->ongoing, ap->remote_host, ap->host_id, ap->host_id_len, ap->heartbeat);
    return(status);
}

//***********************************************************************
// _psk_exchange - Does the key exchange with the server
//***********************************************************************

void psk_exchange(lio_authn_t *an, lio_creds_t *c, char *key)
{
    lio_authn_psk_client_priv_t *ap = an->priv;
    mq_msg_t *msg;
    int len;
    int n;
    char *ptr;
    char salt[SALT_BYTES];
    unsigned char nonce[crypto_secretbox_NONCEBYTES];
    char bufin[1024*10];
    char bufout[1024*10];
    psk_exchange_state_t pxs;


    //** Get the salt and make the box
    tbx_random_get_bytes(salt, SALT_BYTES);
    tbx_random_get_bytes(nonce, sizeof(nonce));
char xbuf[1024];
tbx_chksum_bin2hex(sizeof(nonce), nonce, xbuf);
log_printf(0, "account=%s sizeof(nonce)=%lu nonce=%s\n", an_cred_get_id(c, &len), sizeof(nonce), xbuf);
tbx_chksum_bin2hex(crypto_secretbox_KEYBYTES, (unsigned char *)key, xbuf);
log_printf(0, "account=%s key=%s\n", an_cred_get_id(c, &len), xbuf);

    memcpy(bufin, salt, SALT_BYTES);
    ptr = an_cred_get_descriptive_id(c, &len);
    memcpy(bufin + SALT_BYTES, ptr, len+1);
    n = SALT_BYTES+len+1;
    crypto_secretbox_easy((unsigned char *)bufout, (unsigned char *)bufin, n, nonce, (unsigned char *)key);
    pxs.psk = key;
    pxs.salt = salt;
    pxs.c = c;

tbx_chksum_bin2hex(crypto_secretbox_MACBYTES+n, (unsigned char *)bufout, xbuf);
log_printf(0, "account=%s packet=%s\n", an_cred_get_id(c, &len), xbuf);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(ap->remote_host, 1);
    gop_mq_msg_append_mem(msg, PSK_CLIENT_AUTHN_KEY, PSK_CLIENT_AUTHN_KEY_SIZE, MQF_MSG_KEEP_DATA);
    ptr = an_cred_get_id(c, &len); gop_mq_msg_append_mem(msg, ptr, len+1, MQF_MSG_KEEP_DATA);
log_printf(0, "id=%s id_len=%d\n", ptr, len+1);
    ptr = an_cred_get_descriptive_id(c, &len); gop_mq_msg_append_mem(msg, ptr, len+1, MQF_MSG_KEEP_DATA);
log_printf(0, "did=%s did_len=%d\n", ptr, len+1);
    gop_mq_msg_append_mem(msg, nonce, sizeof(nonce), MQF_MSG_KEEP_DATA);
log_printf(0, "MACBYTES=%d package=%d box_len=%d\n", crypto_secretbox_MACBYTES,n,crypto_secretbox_MACBYTES+n);
    gop_mq_msg_append_mem(msg, bufout, crypto_secretbox_MACBYTES+n, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, ap->host_id, ap->host_id_len, MQF_MSG_KEEP_DATA);  //** Heartbeat frame
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    if (gop_sync_exec(gop_mq_op_new(ap->mqc, msg, psk_response_exchange, &pxs, NULL, 60)) != OP_STATE_SUCCESS) {
        log_printf(0, "ERROR: Failed to connect to PSK server! account:%s\n", an_cred_get_id(c, NULL)); tbx_log_flush();
        fprintf(stderr, "ERROR: Failed to connecto to PSK server! account:%s\n", an_cred_get_id(c, NULL)); fflush(stderr);
        exit(1);
    }

log_printf(0, "SUCCESS!\n");
    return;
}

//***********************************************************************
// _get_psk - Get's the PSK. The format for the PSK file is
//
// [psk]
// account=<account>
// key=<psk_key>
//
//***********************************************************************

void get_psk(lio_authn_t *an, lio_creds_t *c, char *psk_name, char *a)
{
    struct stat st;
    tbx_inip_file_t *fd;
    int n;
    char account_section[128];
    char *account, *psk, *etext, *text;

log_printf(0, "PSK START account=%s\n", a);

    //** Check the perms
    if (stat(psk_name, &st) != 0) {
        log_printf(0, "ERROR: PSK file missing! fname:%s\n", psk_name);
        fprintf(stderr, "ERROR: PSK file missing! fname:%s\n", psk_name);
        exit(1);
    }
    if ((st.st_mode & (S_IRWXO|S_IRWXG|S_IXUSR)) > 0) {
        log_printf(0, "ERROR: PSK file has pad perms! Should only have user RW perms.  fname:%s\n", psk_name);
        fprintf(stderr, "ERROR: PSK file has pad perms! Should only have user RW perms.  fname:%s\n", psk_name);
        exit(1);
    }

    //** Get the account and PSK
    fd = tbx_inip_file_read(psk_name);
    if (!fd) {
        log_printf(0, "ERROR: Can't open the PSK file! fname:%s\n", psk_name);
        fprintf(stderr, "ERROR: Can't open the PSK file! fname:%s\n", psk_name);
        exit(1);
    }

    //** Determine the account to use
    if (a) {
        account = strdup(a);
    } else {
        account = tbx_inip_get_string(fd, "default", "account", NULL);
        if (!account) {
            log_printf(0, "ERROR: PSK default account undefined! fname:%s\n", psk_name);
            fprintf(stderr, "ERROR: PSK default account undefined! fname:%s\n", psk_name);
            exit(1);
        }
    }
    snprintf(account_section, sizeof(account_section)-1, "account-%s", account); account_section[sizeof(account_section)-1] = '\0';

    etext = tbx_inip_get_string(fd, account_section, "key", NULL);
    if (!etext) {
        log_printf(0, "ERROR: PSK key missing! account=%s fname:%s\n", account, psk_name);
        fprintf(stderr, "ERROR: PSK key missing! account=%s fname:%s\n", account, psk_name);
        exit(1);
    }
    tbx_inip_destroy(fd);

    //** Convert the PSK to binary
    text = tbx_stk_unescape_text('\\', etext);
    n = strlen(text);
    tbx_type_malloc_clear(psk, char, n);  //** The actual size needed is 0.8*strlen(text)+1
    zmq_z85_decode((unsigned char *)psk, text);
log_printf(0, "PSK account=%s text=%s len=%lu n=%d shouldbe=%d\n", account, text, strlen(text), n, crypto_secretbox_KEYBYTES); tbx_log_flush();
    free(etext); free(text);

    //** Set the default ID's
    cred_default_set_ids(c, account);

    //** Do the validation with the server
    psk_exchange(an, c, psk);

    free(account);
    memset(psk, 0, n); free(psk);  //** Clear the key before freeing it

    return;
}

//***********************************************************************
// authn_psk_client_cred_init - Creates a PSK client AuthN credential
//***********************************************************************

lio_creds_t *authn_psk_client_cred_init(lio_authn_t *an, int type, void **args)
{
    lio_creds_t *c;
    char fname[PATH_MAX];
//    char *account;
    char *home;

    c = cred_default_create(NULL);
    c->priv = an;
    c->get_type = authn_psk_client_get_type;
    c->destroy = authn_psk_client_cred_destroy;

    //** Load the PSK Key
//    account = (args[0] == NULL) ? "default" : (char *)args[0];
//    snprintf(fname, sizeof(fname)-1, "~/.lio/%s.psk", fn); fname[sizeof(fname)-1] = '\0';
    home = getenv("HOME");
    snprintf(fname, sizeof(fname)-1, "%s/.lio/accounts.psk", home); fname[sizeof(fname)-1] = '\0';
    get_psk(an, c, fname, (char *)args[0]);


    return(c);
}

//***********************************************************************
// apsk_server_print_running_config - Prints the running config
//***********************************************************************

void apsk_client_print_running_config(lio_authn_t *an, FILE *fd, int print_section_heading)
{
    lio_authn_psk_client_priv_t *ap = an->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", ap->section);
    fprintf(fd, "type = %s\n", AUTHN_TYPE_PSK_CLIENT);
    fprintf(fd, "remote_address = %s\n", ap->remote_host_string);
    fprintf(fd, "\n");
}

//***********************************************************************
// authn_psk_client_destroy - Destroys the PSK client AuthN service
//***********************************************************************

void authn_psk_client_destroy(lio_authn_t *an)
{
    lio_authn_psk_client_priv_t *ap = an->priv;

    if (ap->section) free(ap->section);
    free(ap->remote_host_string);
    gop_mq_msg_destroy(ap->remote_host);
    free(ap);
    free(an);
}

//***********************************************************************
// authn_psk_client_create - Create a Fake AuthN service
//***********************************************************************

lio_authn_t *authn_psk_client_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section)
{
    lio_authn_t *an;
    lio_authn_psk_client_priv_t *ap;

    tbx_type_malloc(an, lio_authn_t, 1);
    tbx_type_malloc_clear(ap, lio_authn_psk_client_priv_t, 1);
    an->priv = ap;
    an->print_running_config = apsk_client_print_running_config;
    an->cred_init = authn_psk_client_cred_init;
    an->destroy = authn_psk_client_destroy;

    ap->section = strdup(section);
    ap->remote_host_string = tbx_inip_get_string(ifd, section, "remote_address", psk_default_options.remote_host_string);
    ap->remote_host = gop_mq_string_to_address(ap->remote_host_string);

    ap->mqc = lio_lookup_service(ess, ESS_RUNNING, ESS_MQ); FATAL_UNLESS(ap->mqc != NULL);
    ap->ongoing = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_CLIENT); FATAL_UNLESS(ap->ongoing != NULL);
    ap->host_id = lio_lookup_service(ess, ESS_RUNNING, ESS_ONGOING_HOST_ID); FATAL_UNLESS(ap->host_id != NULL);
    ap->host_id_len = strlen(ap->host_id)+1;
    return(an);
}
