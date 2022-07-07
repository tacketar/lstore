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

#include <ex3/system.h>
#include <gop/gop.h>
#include <gop/gop.h>
#include <gop/mq_helpers.h>
#include <gop/mq_stream.h>
#include <gop/opque.h>
#include <lio/lio.h>
#include <lio.h>
#include <stdlib.h>
#include <strings.h>
#include <tbx/apr_wrapper.h>
#include <tbx/random.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/varint.h>

#define RC_GET_REMOTE_CONFIG_KEY    "rc_get_config"
#define RC_GET_REMOTE_CONFIG_SIZE   13


typedef struct {    //** Individual Config object
    char *object;      //** Config fname
    char **account;    //** Array of valid accounts that can access the config
    int n_account;     //** Number of accounts in the list. If 0 then anyone can access it
} rc_object_t;

typedef struct {   //** Structure holding all the configs
    apr_pool_t *mpool;
    apr_hash_t *table;
} rc_configs_t;

typedef struct {
    gop_mq_context_t  *mqc;
    gop_mq_portal_t *server_portal;
    lio_authn_t *authn;
    apr_pool_t *mpool;
    apr_thread_mutex_t *lock;
    apr_thread_cond_t *cond;
    apr_thread_t *check_thread;
    rc_configs_t *cfgs;
    char *prefix;
    char *host;
    char *section;
    char *config_file;
    int check_interval;
    int shutdown;
    time_t modify_time;
} rc_t;

typedef struct {
    uint64_t id;
    uint8_t zbuf[32];
    time_t *timestamp;
    mq_msg_t *rc_host;
    char *rc_fname;
    char **config;
    rc_t *rc;
} rc_op_t;

static rc_t rc_default_options = {
    .section = NULL,
    .host = NULL,
    .check_interval = 60,
    .prefix = "/etc/lio/clients",
    .config_file = "/etc/lio/remote_config.cfg"
};

static rc_t *rc_server = NULL;

static char *mq_config = "[mq]\n"
                         "min_conn = 1\n"
                         "max_conn = 1\n";

//****************************************************************************
//  rc_parse - Parses the remote config string into a host and config
//     filename. RC string format:
//
//          MQ_NAME|HOST:port,...@RC_NAME
//          lstore://user@MQ_NAME|HOST:port:cfg:section:/fname
//          user@MQ_NAME|HOST:port:cfg:section:/fname
//          user@HOST:port:cfg:section:/fname
//          user@HOST:/fname
//          @:/fname
//
//     IF the string can't be parsed 1 is returned.  Othersie 0 is returned
//     for success.
//****************************************************************************

int rc_parse(char *remote_config, char **rc_host, char **rc_fname)
{
    int i;
    char *fname, *rc, *bstate;

    rc = strdup(remote_config);
    *rc_fname = NULL;
    *rc_host = tbx_stk_string_token(rc, "@", &bstate, &i);
    fname = tbx_stk_string_token(NULL, "@", &bstate, &i);

    if (!*rc_host) {
        if (rc) free(rc);
        return(1);
    }

    *rc_fname = (fname) ? strdup(fname) : "lio";
    return(0);
}

//***********************************************************************
// rcc_response_get_config - Handles the get remote config response (CLIENT)
//***********************************************************************

gop_op_status_t rcc_response_get_config(void *task_arg, int tid)
{
    gop_mq_task_t *task = (gop_mq_task_t *)task_arg;
    rc_op_t *arg = task->arg;
    gop_mq_frame_t *f;
    gop_op_status_t status;
    char *config;
    uint8_t *zbuf;
    time_t ts;
    int n_config, n;
    int64_t n64;

    log_printf(5, "START\n");

    status = gop_success_status;

    //** Parse the response
    gop_mq_remove_header(task->response, 0);

    //** Timestamp frame
    f = gop_mq_msg_next(task->response);
    gop_mq_get_frame(f, (void **)&zbuf, &n);
    n64 = 0;
    tbx_zigzag_decode(zbuf, n, &n64);
    ts = n64;
    log_printf(5, "timestamp=" TT "\n", ts); tbx_log_flush();

    //** Config frame
    f = gop_mq_msg_next(task->response);
    gop_mq_get_frame(f, (void **)&config, &n_config);
    if (n_config > 0) {
        log_printf(5, "n_config=%d config=%s\n", n_config, config);
    } else {
        log_printf(5, "n_config=%d config=NULL\n", n_config);
    }
    if ((n_config <= 0) && (ts == 0)) {
        log_printf(0, " ERROR: Empty config! n=%d\n", n_config);
        status = gop_failure_status;
        goto fail;
    }

    if (n_config > 0) {
        *arg->config = strndup(config, n_config);
        log_printf(5, "rc_config_len=%d\n", n_config);
    }

    *arg->timestamp = ts;

    //** Clean up
fail:
    log_printf(5, "END gid=%d status=%d\n", gop_id(task->gop), status.op_status);

    return(status);
}


//***********************************************************************
// rcc_get_config_op - Gets the remote LIO config string (CLIENT)
//***********************************************************************

gop_op_generic_t *rcc_get_config_op(rc_t *rc, lio_creds_t *creds, mq_msg_t *rc_host, char *rc_fname, char **config, time_t *timestamp, int timeout)
{
    mq_msg_t *msg;
    char dt[128];
    rc_op_t *arg;
    gop_op_generic_t *gop;
    void *chandle;
    int len;
    int64_t n64;

    tbx_type_malloc_clear(arg, rc_op_t, 1);
    tbx_random_get_bytes(&(arg->id), sizeof(arg->id));
    arg->rc_host = rc_host;
    arg->rc_fname = rc_fname;
    arg->config = config;
    arg->timestamp = timestamp;
    n64 = *timestamp;
    len = tbx_zigzag_encode(n64, arg->zbuf);

    //** Form the message
    msg = gop_mq_make_exec_core_msg(rc_host, 1);
    gop_mq_msg_append_mem(msg, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, rc_fname, strlen(rc_fname), MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, arg->zbuf, len, MQF_MSG_KEEP_DATA);
    if (creds) {
        log_printf(5, "creds=%s\n", an_cred_get_descriptive_id(creds, &len));
        chandle = an_cred_get_handle(creds, &len);
    } else {
        log_printf(5, "creds=NULL\n");
        chandle = NULL; len = 0;
    }
    gop_mq_msg_append_mem(msg, chandle, len, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Make the gop
    gop = gop_mq_op_new(rc->mqc, msg, rcc_response_get_config, arg, free, timeout);
    gop_set_private(gop, arg);

    log_printf(5, "mqid=%s timeout=%d gid=%d\n", gop_mq_id2str((char *)&(arg->id), sizeof(uint64_t), dt, sizeof(dt)), timeout, gop_id(gop));

    return(gop);
}

//***********************************************************************
// rc_can_access - Validates the config is available and the account is
//    allowed to access it
//***********************************************************************

int rc_can_access(rc_t *rc, lio_creds_t *creds, char *fname)
{
    int can_access = 0;
    int i;
    char *account;
    rc_object_t *obj;

    apr_thread_mutex_lock(rc->lock);
    obj = apr_hash_get(rc->cfgs->table, fname, APR_HASH_KEY_STRING);
    if (obj) {
        if (obj->n_account > 0) {
            if (creds) {
                account = an_cred_get_id(creds, &i);
                for (i=0; i<obj->n_account; i++) {
                    log_printf(5, "RCS: account[%d]=%s\n", i, obj->account[i]);
                    if (strcmp(account, obj->account[i]) == 0) {
                        can_access = 1;
                        log_printf(5, "RCS: can_access=1\n");
                        break;
                    }
                }
            }
        } else {
            can_access = 1;
        }
    }
    apr_thread_mutex_unlock(rc->lock);

    log_printf(5, "RCS: can_access=%d\n", can_access);
    return(can_access);
}

//***********************************************************************
// rcs_config_send - Sends the configuration back (SERVER)
//***********************************************************************

void rcs_config_send(rc_t *rc, lio_creds_t *creds, gop_mq_frame_t *fid, mq_msg_t *address, char *fname, time_t timestamp)
{
    mq_msg_t *msg;
    char *config, *path;
    int nbytes, n;
    uint8_t *buf;
    struct stat st;

    //** Form the core message
    msg = gop_mq_msg_new();
    gop_mq_msg_append_mem(msg, MQF_VERSION_KEY, MQF_VERSION_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_mem(msg, MQF_RESPONSE_KEY, MQF_RESPONSE_SIZE, MQF_MSG_KEEP_DATA);
    gop_mq_msg_append_frame(msg, fid);


    //** Form the full path
    nbytes = strlen(rc->prefix) + 1 + strlen(fname) + 4 + 1;
    tbx_type_malloc(path, char, nbytes);
    snprintf(path, nbytes, "%s/%s.cfg", rc->prefix, fname);
    if (creds) {
        log_printf(5, "RCS: full_path=%s creds=%s\n", path, an_cred_get_descriptive_id(creds, &n));
    } else {
        log_printf(5, "RCS: full_path=%s creds=NULL\n", path);
    }

    //** Add the config and timestamp
    if (rc_can_access(rc, creds, fname)) {
        //** Check the timestamp
        config = NULL;
        nbytes = 0;
        if (stat(path, &st) == 0) {
            log_printf(5, "RCS: path=%s ts_user=" TT " ts_server= " TT "\n", path, timestamp, st.st_mtime);
            if (timestamp != st.st_mtime) {
                tbx_inip_file2string_jail(path, &config, &nbytes, rc->prefix);
                if (nbytes > 0) nbytes++; //** Make sure and send the NULL terminator
            }
            timestamp = st.st_mtime;
        } else {
            timestamp = 0;
        }
    } else {
        config = NULL;
        nbytes = 0;
        timestamp = 0;
    }
    free(path);
    tbx_type_malloc_clear(buf, uint8_t, 32);
    n = tbx_zigzag_encode(timestamp, buf);
    gop_mq_msg_append_mem(msg, buf, n, MQF_MSG_AUTO_FREE);
    gop_mq_msg_append_mem(msg, config, nbytes, MQF_MSG_AUTO_FREE);

    log_printf(5, "nbytes=%d ts=" TT " config=%s\n", nbytes, timestamp, config);

    //** End with an empty frame
    gop_mq_msg_append_mem(msg, NULL, 0, MQF_MSG_KEEP_DATA);

    //** Now address it
    gop_mq_msg_apply_return_address(msg, address, 0);

    //** Lastly send it
    gop_mq_submit(rc->server_portal, gop_mq_task_new(rc->mqc, msg, NULL, NULL, 30));
}

//***********************************************************************
// rcs_get_config_cb - Processes the new config request (SERVER)
//***********************************************************************

void rcs_get_config_cb(void *arg, gop_mq_task_t *task)
{
    rc_t *rc = (rc_t *)arg;
    gop_mq_frame_t *f, *fid;
    mq_msg_t *msg;
    int bufsize = 4096;
    char buffer[bufsize];
    char *data;
    int n, do_config;
    int64_t n64;
    lio_creds_t *creds;
    void *cred_args[2];
    time_t timestamp;
    int len;
    void *ptr;

    do_config = -1;
    creds = NULL;
    log_printf(5, "Processing incoming remote config request\n");

    //** Parse the command
    msg = task->msg;  //** Don't have to worry about msg cleanup.  It's handled at a higher level
    gop_mq_remove_header(msg, 0);

    //** Get the ID frame
    fid = mq_msg_pop(msg);
    gop_mq_get_frame(fid, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        goto fail;
    }

    log_printf(5, "mqid=%s\n", gop_mq_id2str(data, n, buffer, bufsize));

    //** This is the actual RC command frame
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (mq_data_compare(data, n, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE) != 0) {
        log_printf(0, " ERROR: Bad ID size!  Got %d should be greater than 0\n", n);
        gop_mq_frame_destroy(f);
        goto fail;
    }
    gop_mq_frame_destroy(f);

    //** Get the file name
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, (void **)&data, &n);
    if (n == 0) {
        log_printf(0, "ERROR: Bad config size!\n");
        gop_mq_frame_destroy(f);
        goto fail;
    }

    if (n>(int)sizeof(buffer)) {
        buffer[0] = 0;
    } else {
        strncpy(buffer, data, n);
        buffer[n] = 0;
    }
    gop_mq_frame_destroy(f);

    //** Get the timestamp
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, &ptr, &len);
    if (len != 0) {
        n64 = 0;
        tbx_zigzag_decode(ptr, len, &n64);
        timestamp = n64;
    } else {
        timestamp = 0;
    }
    gop_mq_frame_destroy(f);

    //** Get the creds
    f = mq_msg_pop(msg);
    gop_mq_get_frame(f, &ptr, &len);
    if (len != 0) {
        log_printf(5, "RCS: attempting to lookup handle handle_len=%d\n", len);
        cred_args[0] = ptr;
        cred_args[1] = &len;
        creds = authn_cred_init(rc_server->authn, AUTHN_INIT_LOOKUP, cred_args);
    } else {
        log_printf(5, "RCS: No creds provided\n");
    }
    gop_mq_frame_destroy(f);

    log_printf(15, "n=%d fname=%s\n", n, buffer);

    //** Empty frame
    f = gop_mq_msg_first(msg);
    if (!f) {
        if (creds) {
            log_printf(0, "Invalid message! cred=%s\n", an_cred_get_descriptive_id(creds, NULL));
        } else {
            log_printf(0, "Invalid message! cred=NULL\n");
        }
        rcs_config_send(rc, creds, fid, msg, "NULL", timestamp);
        goto fail;
    } else {
        gop_mq_get_frame(f, (void **)&data, &n);
        if (n != 0) {
            log_printf(0, " ERROR:  Missing initial empty frame!\n");
            rcs_config_send(rc, creds, fid, msg, "NULL", timestamp);
            goto fail;
        }
    }
    //** Everything else is the address so **
    //** Now handle the response
    rcs_config_send(rc, creds, fid, msg, buffer, timestamp);

fail:
    if (creds) an_cred_destroy(creds);  //** Release the creds

    log_printf(5, "END incoming request do_config=%d\n", do_config);

    return;
}

//***********************************************************************
// rc_client_get_config - Returns the remote config if available
//***********************************************************************

int rc_client_get_config(gop_mq_context_t *mqc, lio_creds_t *creds, char *rc_string, char *mq_default, char **config, char **obj_name, char **rc_user, time_t *timestamp)
{
    rc_t rc;
    mq_msg_t *address;
    char *rc_host, *rc_file, *rc_section, *mq, s[1024];
    int err, port, n;
    tbx_inip_file_t *ifd;

    *config = NULL;
    mq = (mq_default) ? strdup(mq_default) : strdup(LIO_MQ_NAME_DEFAULT);
    port = 6711;
    rc_file = strdup("lio");
    rc_section = strdup("lio");
    rc_host = NULL;
    lio_parse_path(rc_string, rc_user, &mq, &rc_host, &port, &rc_file, &rc_section, NULL);

    //** Make the object name
    n = 9 + sizeof(mq) + 1 + sizeof(rc_host) + 1 + 6 + sizeof(rc_file) + 1 + sizeof(rc_section) + 20;
    tbx_type_malloc(*obj_name, char, n);
    snprintf(*obj_name, n, "lstore://%s|%s:%d:%s:%s", mq, rc_host, port, rc_file, rc_section);
    snprintf(s, sizeof(s), "%s|tcp://%s:%d", mq, rc_host, port);
    log_printf(10, "address=%s file=%s section=%s\n", s, rc_file, rc_section);
    address = gop_mq_string_to_address(s);

    memset(&rc, 0, sizeof(rc));
    if (mqc) {
        rc.mqc = mqc;
    } else {
        ifd = tbx_inip_string_read(mq_config, 1);
        rc.mqc = gop_mq_create_context(ifd, "mq");
        tbx_inip_destroy(ifd);
    }

    err = gop_sync_exec(rcc_get_config_op(&rc, creds, address, rc_file, config, timestamp, 60));
    gop_mq_msg_destroy(address);

    if (!mqc) gop_mq_destroy_context(rc.mqc);

    if (rc_host) free(rc_host);
    if (rc_file) free(rc_file);
    if (rc_section) free(rc_section);
    if (mq) free(mq);

    return((err == OP_STATE_SUCCESS) ? 0 : 1);
}

//***********************************************************************
// rc_print_running_config - Prints the running remote config server
//***********************************************************************

void rc_print_running_config(FILE *fd)
{
    int i;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    rc_object_t *obj;

    if (!rc_server) return;

    apr_thread_mutex_lock(rc_server->lock);

    fprintf(fd, "[%s]\n", rc_server->section);
    if (rc_server->host) {
        fprintf(fd, "host = %s\n", rc_server->host);
    } else {
        fprintf(fd, "# Using global host\n");
    }
    fprintf(fd, "prefix = %s\n", rc_server->prefix);
    fprintf(fd, "config_file = %s\n", rc_server->config_file);
    fprintf(fd, "\n");

    if (rc_server->cfgs) {
        for (hi=apr_hash_first(NULL, rc_server->cfgs->table); hi != NULL; hi = apr_hash_next(hi)) {
            apr_hash_this(hi, NULL, &hlen, (void **)&obj);
            fprintf(fd, "[config-%s]\n", obj->object);
            for (i=0; i<obj->n_account; i++) fprintf(fd, "account = %s\n", obj->account[i]);
            fprintf(fd, "\n");
        }
    }

    apr_thread_mutex_unlock(rc_server->lock);
}

//***********************************************************************
// _rc_configs_destroy - Destroys the config
//***********************************************************************

void _rc_configs_destroy(rc_configs_t *cfgs)
{
    int i;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    rc_object_t *obj;

    for (hi=apr_hash_first(NULL, cfgs->table); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, NULL, &hlen, (void **)&obj);
        apr_hash_set(cfgs->table, obj->object, APR_HASH_KEY_STRING, NULL);
        for (i=0; i<obj->n_account; i++) free(obj->account[i]);
        if (obj->account) free(obj->account);
        free(obj->object);
        free(obj);
    }
    apr_pool_destroy(cfgs->mpool);
    free(cfgs);
}

//***********************************************************************
// _rc_configs_create - Creates and parses the config file
//
//  The XXXXX maps to a file in the specified prefix directory
//  The YYYYY map to valid user accounts that are allowed to read the config.
//  If no accounts are listed then the config is assumed to be publically
//  accessible.
//
//  [config-XXXXX_1]
//  account = YYYYY_1
//  ...
//  account = YYYYY_n
//
//  [config-XXXXX_n]

//***********************************************************************

rc_configs_t  *_rc_configs_create(tbx_inip_file_t *fd)
{
    rc_configs_t *cfgs;
    rc_object_t *obj;
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *key, *config;
    tbx_stack_t *stack;
    int i;

    tbx_type_malloc_clear(cfgs, rc_configs_t, 1);
    assert_result(apr_pool_create(&(cfgs->mpool), NULL), APR_SUCCESS);
    cfgs->table = apr_hash_make(cfgs->mpool);

    stack = tbx_stack_new();

    ig = tbx_inip_group_first(fd);
    while (ig != NULL) {
        if (strncmp("config-", tbx_inip_group_get(ig),7) == 0) { //** Got a prefix
            key = tbx_inip_group_get(ig);
            config = key + 7;  //** Skip over the prefix
            ele = tbx_inip_ele_first(ig);
            while (ele != NULL) {
                if (strcmp(tbx_inip_ele_get_key(ele), "account") == 0) { //** Got an account
                    tbx_stack_push(stack, tbx_inip_ele_get_value(ele));
                }

                ele = tbx_inip_ele_next(ele);
            }

            log_printf(10, "config=%s n_stack=%d\n", config, tbx_stack_count(stack));
            tbx_type_malloc_clear(obj, rc_object_t, 1);
            obj->object = strdup(config);
            obj->n_account = tbx_stack_count(stack);
            if (obj->n_account > 0) {
                tbx_type_malloc(obj->account, char *, obj->n_account);
                for (i=0; i<obj->n_account; i++) {
                    obj->account[i] = strdup(tbx_stack_pop(stack));
                }
            }
            apr_hash_set(cfgs->table, obj->object, APR_HASH_KEY_STRING, obj);
        }

        ig = tbx_inip_group_next(ig);
    }

    tbx_stack_free(stack, 0);
    return(cfgs);
}

//***********************************************************************
// _rc_configs_load - Loads the config
//     NOTE: Assumes the RC lock is held
//***********************************************************************

void _rc_configs_load(rc_t *rc)
{
    struct stat sbuf;
    tbx_inip_file_t *ifd;

    if (stat(rc->config_file, &sbuf) != 0) {
        log_printf(1, "RC configs file missing!!! Using old definition. fname=%s\n", rc->config_file);
        return;
    }

    if (rc->modify_time != sbuf.st_mtime) {  //** File changed so reload it
        log_printf(5, "RELOADING data\n");
        ifd = tbx_inip_file_read(rc->config_file, 1);
        if (rc->cfgs) _rc_configs_destroy(rc->cfgs);
        rc->cfgs = _rc_configs_create(ifd);
        tbx_inip_destroy(ifd);
        rc->modify_time = sbuf.st_mtime;
    }
}

//***********************************************************************
//  rc_check_thread - checks for changes in the RC configs
//***********************************************************************

void *rc_check_thread(apr_thread_t *th, void *data)
{
    rc_t *rc = data;
    apr_time_t dt;

    tbx_monitor_thread_create(MON_MY_THREAD, "rc_check_thread: monitoring file=%s", rc->config_file);
    dt = apr_time_from_sec(rc->check_interval);

    apr_thread_mutex_lock(rc->lock);
    do {
        log_printf(5, "LOOP START check_interval=%d\n", rc->check_interval);
        tbx_monitor_thread_message(MON_MY_THREAD, "Running check");

        _rc_configs_load(rc);  //** Do a quick check and see if the file has changed

        if (rc->shutdown == 0) apr_thread_cond_timedwait(rc->cond, rc->lock, dt);
    } while (rc->shutdown == 0);
    apr_thread_mutex_unlock(rc->lock);

    tbx_monitor_thread_destroy(MON_MY_THREAD);
    return(NULL);
}

//***********************************************************************
// rc_server_install - Installs the Remote config server
//***********************************************************************

int rc_server_install(lio_config_t *lc, char *section)
{
    gop_mq_command_table_t *ctable;

    tbx_type_malloc_clear(rc_server, rc_t, 1);

    assert_result(apr_pool_create(&(rc_server->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(rc_server->lock), APR_THREAD_MUTEX_DEFAULT, rc_server->mpool);
    apr_thread_cond_create(&(rc_server->cond), rc_server->mpool);

    rc_server->section = strdup(section);
    rc_server->mqc = lc->mqc;
    rc_server->host = tbx_inip_get_string(lc->ifd, section, "host", rc_default_options.host);
    rc_server->prefix = tbx_inip_get_string(lc->ifd, section, "prefix", rc_default_options.prefix);
    rc_server->config_file = tbx_inip_get_string(lc->ifd, section, "config_file", rc_default_options.config_file);

    rc_server->authn = lio_lookup_service(lc->ess, ESS_RUNNING, ESS_AUTHN);

    log_printf(5, "Starting remote config server\n");
    log_printf(5, "Client config path: %s\n", rc_server->prefix);

    //** Get the server portal and add our commands
    if (rc_server->host) {
        log_printf(5, "Creating new portal. host=%s\n", rc_server->prefix);
        rc_server->server_portal = gop_mq_portal_create(lio_gc->mqc, rc_server->host, MQ_CMODE_SERVER);
    } else {
        log_printf(5, "Using default portal.\n");
        rc_server->server_portal = lio_lookup_service(lc->ess, ESS_RUNNING, ESS_SERVER_PORTAL);
        FATAL_UNLESS(rc_server->server_portal != NULL);
    }
    rc_server->check_interval = tbx_inip_get_integer(lc->ifd, section, "check_interval", rc_default_options.check_interval);

    //** Load the initial config
    rc_server->modify_time = 0;
    _rc_configs_load(rc_server);

    //** Launch the check thread
    tbx_thread_create_assert(&(rc_server->check_thread), NULL, rc_check_thread, (void *)rc_server, rc_server->mpool);

    //** Install the command
    ctable = gop_mq_portal_command_table(rc_server->server_portal);
    gop_mq_command_set(ctable, RC_GET_REMOTE_CONFIG_KEY, RC_GET_REMOTE_CONFIG_SIZE, rc_server, rcs_get_config_cb);

    //** Start it if needed
    if (rc_server->host)  gop_mq_portal_install(lio_gc->mqc, rc_server->server_portal);

    return(0);
}

//***********************************************************************
// rc_server_destroy - Removes the remote config server
//***********************************************************************

void rc_server_destroy()
{
    apr_status_t value;

    if (!rc_server) return;

    log_printf(10, "Shutting down\n");

    apr_thread_mutex_lock(rc_server->lock);
    rc_server->shutdown = 1;
    apr_thread_cond_broadcast(rc_server->cond);
    apr_thread_mutex_unlock(rc_server->lock);

    //** Wait for it to shutdown
    apr_thread_join(&value, rc_server->check_thread);

    //** Remove and destroy the server portal if needed
    if (rc_server->host) {
        free(rc_server->host);
        gop_mq_portal_remove(lio_gc->mqc, rc_server->server_portal);
        gop_mq_portal_destroy(rc_server->server_portal);
    }

    _rc_configs_destroy(rc_server->cfgs);

    apr_thread_mutex_destroy(rc_server->lock);
    apr_thread_cond_destroy(rc_server->cond);
    apr_pool_destroy(rc_server->mpool);

    if (rc_server->prefix) free(rc_server->prefix);
    if (rc_server->section) free(rc_server->section);
    if (rc_server->config_file) free(rc_server->config_file);
    free(rc_server);
    rc_server = NULL;
}
