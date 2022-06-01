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

#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <apr_time.h>
#include <apr_signal.h>
#include <sys/types.h>
#include <unistd.h>
#include "ibp_server.h"
#include "debug.h"
#include <tbx/log.h>
#include <tbx/assert_result.h>
#include <tbx/dns_cache.h>
#include <tbx/fork_helper.h>
#include <tbx/siginfo.h>
#include <tbx/type_malloc.h>
#include <tbx/lio_monitor.h>
#include "lock_alloc.h"
#include "activity_log.h"

//***** This is just used in the parallel mounting of the resources ****
typedef struct {
    apr_thread_t *thread_id;
    tbx_inip_file_t *keyfile;
    char *group;
    char *snap_prefix;
    char *merge_prefix;
    int force_resource_rebuild;
} pMount_t;

//*****************************************************************************
// parallel_mount_resource - Mounts a resource in a separate thread
//*****************************************************************************

void *parallel_mount_resource(apr_thread_t *th, void *data)
{
    pMount_t *pm = (pMount_t *) data;
    Resource_t *r;

    tbx_type_malloc_clear(r, Resource_t, 1);

    int err = mount_resource(r, pm->keyfile, pm->group,
                             pm->force_resource_rebuild, global_config->server.lazy_allocate,
                             global_config->truncate_expiration, pm->snap_prefix, pm->merge_prefix);

    if (err != 0) {
        log_printf(0, "parallel_mount_resource:  Error mounting resource!!!!!\n");
        exit(-10);
    }

    free(pm->group);

    r->rl_index = resource_list_insert(global_config->rl, r);

    //** Launch the garbage collection threads
//   launch_resource_cleanup_thread(r);  *** Not safe to do this here due to fork() becing called after mount

    apr_thread_exit(th, 0);
    return (0);                 //** Never gets here but suppresses compiler warnings
}


//*****************************************************************************
// check_rid - tries to create an allocation and read it back to
//    detect drive issues
//*****************************************************************************

#define _CHECK_VALUE 12345

int rid_check_write(Resource_t *r)
{
    osd_fd_t *fd;
    osd_id_t id = RES_CHECK_ID;
    int n;
    int val = _CHECK_VALUE;

    //** Create the ID
    osd_create_id(r->dev, CHKSUM_NONE, 0, 0, id);

    //** Now try and open it
    fd = osd_open(r->dev, id, OSD_WRITE_MODE);
    if (fd == NULL) {
        log_printf(0, "ERROR:  Can't open check file! rid=%s\n", r->name);
        return (1);
    }

    //** Dump the data
    n = osd_write(r->dev, fd, 0, sizeof(val), &val);
    if (n != sizeof(val)) {
        log_printf(10, "ERROR: Can't write whole record! r: %s\n", r->name);
        osd_close(r->dev, fd);
        return (1);
    }
    osd_close(r->dev, fd);

    return(0);
}

//-------------------------------------------------

int rid_check_read(Resource_t *r)
{
    osd_fd_t *fd;
    osd_id_t id = RES_CHECK_ID;
    int n, val;

    //** TRy and open it
    fd = osd_open(r->dev, id, OSD_READ_MODE);
    if (fd == NULL) {
        log_printf(0, "ERROR:  Can't open check file! rid=%s\n", r->name);
        return (1);
    }

    //** Get the data
    n = osd_read(r->dev, fd, 0, sizeof(val), &val);
    if (n != sizeof(val)) {
        log_printf(10, "ERROR: Can't read whole record! r: %s\n", r->name);
        osd_close(r->dev, fd);
        return (1);
    }
    osd_close(r->dev, fd);

    //** Verify it
    if (val != _CHECK_VALUE) {
        log_printf(10, "ERROR: Can't read whole record! r: %s\n", r->name);
        return(1);
    }

    return(0);
}

//-------------------------------------------------

int resource_check(Resource_t *r)
{
    int err = 1;
    if (rid_check_write(r) == 0) {
        if (rid_check_read(r) == 0) err = 0;
    }

    //** Always clean up
    osd_remove(r->dev, OSD_PHYSICAL_ID, RES_CHECK_ID);
    return(err);
}

//*****************************************************************************
// resource_health_check - Does periodic health checks on the RIDs
//*****************************************************************************

void *resource_health_check(apr_thread_t *th, void *data)
{
    Resource_t *r;
    resource_list_iterator_t it;
    apr_time_t next_check;
    ibp_task_t task;
    apr_time_t dt, dt_wait;
    Cmd_internal_mount_t *cmd;
    tbx_stack_t *eject;
    FILE *fd;
    char *rname, *rid_name, *data_dir, *data_device;
    int i, j;
    pid_t pid;
    int err;

    eject = tbx_stack_new();

    memset(&task, 0, sizeof(task));
    cmd = &(task.cmd.cargs.mount);
    dt_wait = apr_time_from_sec(global_config->server.rid_check_interval);
    next_check = apr_time_now() + dt_wait;
    dt = apr_time_from_sec(global_config->server.eject_timeout);

    apr_thread_mutex_lock(shutdown_lock);
    while (shutdown_now == 0) {
        if (apr_time_now() > next_check) {
            log_printf(5, "Running RID check\n");
            it = resource_list_iterator(global_config->rl);
            j = 0;
            while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
                err = resource_check(r);
                if (err == 0) {
                    r->last_good_check = apr_time_now();        //** this should be done in resource.c But I'm the only one that ever touches the routine
                } else if (apr_time_now() > (r->last_good_check + dt)) {
                    strncpy(cmd->crid, global_config->rl->res[r->rl_index].crid, sizeof(cmd->crid)-1);
                    cmd->crid[sizeof(cmd->crid)-1] = '\0';
                    strncpy(cmd->msg, "Health check failed. Ejecting drive.", sizeof(cmd->msg)-1);
                    cmd->msg[sizeof(cmd->msg)-1] = '\0';

                    //** Push the failed drive on the ejected stack
                    j++;
                    tbx_stack_push(eject, strdup(r->data_pdev));
                    tbx_stack_push(eject, strdup(r->device));
                    tbx_stack_push(eject, strdup(cmd->crid));

                    apr_thread_mutex_unlock(shutdown_lock);
                    cmd->delay = 10;
                    handle_internal_umount(&task);
                    apr_thread_mutex_lock(shutdown_lock);
                }
            }
            resource_list_iterator_destroy(global_config->rl, &it);

            log_printf(5, "Finished RID check tbx_stack_count(eject)=%d\n", tbx_stack_count(eject));

            if ((j > 0) && (global_config->server.rid_eject_script != NULL)) {  //** Ejected something so run the eject program
                //** Make the RID list file and name
                i = strlen(global_config->server.rid_eject_tmp_path) + 1 + 6 + 30;
                tbx_type_malloc(rname, char, i);
                snprintf(rname, i, "%s/eject." TT, global_config->server.rid_eject_tmp_path,
                         apr_time_now());
                fd = fopen(rname, "w");
                if (fd == NULL) {
                    log_printf(0, "ERROR: failed to create RID eject temp file: %s\n", rname);
                    goto bail;
                }
                //** Line format: total # of RIDs | Good RIDs | Ejected/Bad RIDs
                i = j + resource_list_n_used(global_config->rl);
                fprintf(fd, "%d|%d|%d\n", i, resource_list_n_used(global_config->rl), j);

                //** Cycle though the good RIDs printing RID info
                //** Line format: RID|data_directory|data_device|status(0=good|1=bad)
                it = resource_list_iterator(global_config->rl);
                while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
                    fprintf(fd, "%s|%s|%s|%d\n", global_config->rl->res[r->rl_index].crid,
                            r->device, r->data_pdev, 0);
                }
                resource_list_iterator_destroy(global_config->rl, &it);

                //** Now do the same for the ejected drives
                for (i = 0; i < j; i++) {
                    rid_name = tbx_stack_pop(eject);
                    data_dir = tbx_stack_pop(eject);
                    data_device = tbx_stack_pop(eject);
                    fprintf(fd, "%s|%s|%s|%d\n", rid_name, data_dir, data_device, 1);
                    free(rid_name);
                    free(data_dir);
                    free(data_device);
                }
                fclose(fd);

                //** Now spawn the child process to do it's magic
                pid = fork();
                if (pid == 0) { //** Child process
                    execl(global_config->server.rid_eject_script,
                          global_config->server.rid_eject_script, rname, NULL);
                    exit(0);    //** Should never get here
                } else if (pid == -1) { //** Fork error
                    log_printf(0, "FORK error!!!! rname=%s\n", rname);
                }
bail:
                free(rname);
            }

            next_check =
                apr_time_now() + apr_time_from_sec(global_config->server.rid_check_interval);
        }

        apr_thread_cond_timedwait(shutdown_cond, shutdown_lock, dt_wait);
    }
    apr_thread_mutex_unlock(shutdown_lock);

    tbx_stack_free(eject, 1);

    apr_thread_exit(th, 0);
    return (0);                 //** Never gets here but suppresses compiler warnings
}

//*****************************************************************************
// log_preamble - Print the initial log file output
//*****************************************************************************

void log_preamble(Config_t *cfg)
{
    char buffer[100 * 1024];
    int used = 0;
    apr_time_t t = get_starttime();
    apr_ctime(buffer, t);

    log_printf(0, "\n");
    log_printf(0, "*****************************************************************\n");
    log_printf(0, "Starting ibp_server on %s\n", buffer);
    log_printf(0, "*****************************************************************\n");
    log_printf(0, "\n");

    log_printf(0, "*********************Printing configuration file **********************\n\n");


    print_config(buffer, &used, sizeof(buffer), cfg);
    log_printf(0, "%s", buffer);

    log_printf(0, "*****************************************************************\n\n");
}


//*****************************************************************************
//  parse_config_prefork - Parses the config file(fname) and initializes the config
//                 data structures (cfg) for things before calling FORK.
//*****************************************************************************

int parse_config_prefork(tbx_inip_file_t *keyfile, Config_t *cfg, int force_rebuild)
{
    Server_t *server;
    int i, j, k, n, error;
    char *str;

    server = &(cfg->server);
    error = 0;

    //** Get the logging info.  It's used for checking if we can fork
    server->logfile = "ibp.log";
    server->log_overwrite = 0;  //QWERT????
    server->log_level = 0;
    server->log_maxsize = 100;
    server->debug_level = 0;

    server->logfile = tbx_inip_get_string(keyfile, "server", "log_file", server->logfile);
    server->log_level = tbx_inip_get_integer(keyfile, "server", "log_level", server->log_level);
    server->log_maxsize =
        tbx_inip_get_integer(keyfile, "server", "log_maxsize", server->log_maxsize) * 1024 * 1024;
    server->debug_level =
        tbx_inip_get_integer(keyfile, "server", "debug_level", server->debug_level);

    //** Determine the max number of threads
    server->max_threads = 64;
    server->max_threads = tbx_inip_get_integer(keyfile, "server", "threads", server->max_threads);

    //** Find out how many resources we have
    n = 0;
    tbx_inip_group_t *igrp = tbx_inip_group_first(keyfile);
    while (igrp) {
        str = tbx_inip_group_get(igrp);
        if (strncmp("resource", str, 8) == 0) n++;
        igrp = tbx_inip_group_next(igrp);
    }

    //** Make sure we have enough fd's
    i = sysconf(_SC_OPEN_MAX);
    j = 3 * server->max_threads + 2 * n + 64;
    if (i < j) {
        k = (i - 2 * n - 64) / 3;
        log_printf(0,
                   "ibp_server: ERROR Too many threads!  Current threads=%d, n_resources=%d, and max fd=%d.\n",
                   server->max_threads, n, i);
        log_printf(0,
                   "ibp_server: Either make threads < %d or increase the max fd > %d (ulimit -n %d)\n",
                   k, j, j);
        error = 1;
    }

    return(error);
}

//*****************************************************************************
//  parse_config_postfork - Parses the config file(fname) and initializes the config
//                 data structure (cfg) after the FORK.
//*****************************************************************************

int parse_config_postfork(tbx_inip_file_t *keyfile, Config_t *cfg, int force_rebuild, char *merge_snap)
{
    Server_t *server;
    char *str, *bstate, *start_snap_prefix;
    int val, k, i, timeout_ms;
    char iface_default[1024];
    apr_time_t t;
    pMount_t *pm, *pmarray;

    // *** Initialize the data structure to default values ***
    server = &(cfg->server);
    server->max_pending = 16;
    server->min_idle = apr_time_make(60, 0);
    server->stats_size = 5000;
    timeout_ms = 1 * 1000;      //** Wait 1 sec
//  tbx_ns_timeout_set(&(server->timeout), 1, 0);  //**Wait 1sec
    server->timeout_secs = timeout_ms / 1000;
    server->timestamp_interval = 60;
    server->password = DEFAULT_PASSWORD;
    server->max_warm = 1000;
    server->lazy_allocate = 1;
    server->backoff_scale = 1.0 / 10;
    server->backoff_max = 30;
    server->big_alloc_enable = (sizeof(off_t) > 4) ? 1 : 0;
    server->splice_enable = 0;
    server->alog_name = "ibp_activity.log";
    server->alog_max_size = 50;
    server->alog_max_history = 1;
    server->alog_host = NULL;
    server->alog_port = 0;
    server->port = IBP_PORT;
    server->return_cap_id = 1;
    server->rid_check_interval = 30;
    server->eject_timeout = 35;
    server->rid_log = "/log/rid.log";
    server->rid_eject_script = NULL;
    server->rid_eject_tmp_path = "/tmp";
    cfg->monitor_fname = "/log/ibp.mon";
    cfg->monitor_enable = 0;
    cfg->db_mem = 256;
    cfg->force_resource_rebuild = force_rebuild;
    cfg->truncate_expiration = 0;
    cfg->soft_fail = -1;

    // *** Parse the Server settings ***
    server->port = tbx_inip_get_integer(keyfile, "server", "port", server->port);

    //** Make the default interface
    gethostname(iface_default, sizeof(iface_default));
    i = strlen(iface_default);
    tbx_append_printf(iface_default, &i, sizeof(iface_default), ":%d", server->port);

    char *iface_str = tbx_inip_get_string(keyfile, "server", "interfaces", iface_default);

    //** Determine the number of interfaces
    char *list[100];
    i = 0;
    list[i] = tbx_stk_string_token(iface_str, ";", &bstate, &k);
    while (strcmp(list[i], "") != 0) {
        i++;
        list[i] = tbx_stk_string_token(NULL, ";", &bstate, &k);
    }

    server->n_iface = i;

    //** Now parse and store them
    server->iface = (interface_t *) malloc(sizeof(interface_t) * server->n_iface);
    interface_t *iface;
    for (i = 0; i < server->n_iface; i++) {
        iface = &(server->iface[i]);
        iface->hostname = tbx_stk_string_token(list[i], ":", &bstate, &k);
        if (sscanf(tbx_stk_string_token(NULL, " ", &bstate, &k), "%d", &(iface->port)) != 1) {
            iface->port = server->port;
        }
    }

    server->max_pending =
        tbx_inip_get_integer(keyfile, "server", "max_pending", server->max_pending);
    t = 0;
    t = tbx_inip_get_integer(keyfile, "server", "min_idle", t);
    if (t != 0)
        server->min_idle = apr_time_make(t, 0);
    val = tbx_inip_get_integer(keyfile, "server", "max_network_wait_ms", timeout_ms);

    int sec = val / 1000;
    int us = val - 1000 * sec;
    us = us * 1000;             //** Convert from ms->us
    server->timeout_secs = sec;
//log_printf(0, "parse_config: val=%d sec=%d us=%d\n", val, sec, us);
    tbx_ns_timeout_set(&(server->timeout), sec, us);    //**Convert it from ms->us

    server->stats_size = tbx_inip_get_integer(keyfile, "server", "stats_size", server->stats_size);
    server->password = tbx_inip_get_string(keyfile, "server", "password", server->password);
    server->max_warm =
        tbx_inip_get_integer(keyfile, "server", "max_warm", server->max_warm);
    server->lazy_allocate =
        tbx_inip_get_integer(keyfile, "server", "lazy_allocate", server->lazy_allocate);
    server->big_alloc_enable =
        tbx_inip_get_integer(keyfile, "server", "big_alloc_enable", server->big_alloc_enable);
    server->splice_enable =
        tbx_inip_get_integer(keyfile, "server", "splice_enable", server->splice_enable);
    server->backoff_scale =
        tbx_inip_get_double(keyfile, "server", "backoff_scale", server->backoff_scale);
    server->backoff_max =
        tbx_inip_get_double(keyfile, "server", "backoff_max", server->backoff_max);

    server->return_cap_id =
        tbx_inip_get_integer(keyfile, "server", "return_cap_id", server->return_cap_id);

    cfg->db_mem = tbx_inip_get_integer(keyfile, "server", "db_mem", cfg->db_mem);

    server->alog_name = tbx_inip_get_string(keyfile, "server", "activity_file", server->alog_name);
    server->alog_max_size =
        tbx_inip_get_integer(keyfile, "server", "activity_maxsize",
                             server->alog_max_size) * 1024 * 1024;
    server->alog_max_history =
        tbx_inip_get_integer(keyfile, "server", "activity_max_history", server->alog_max_history);
    server->alog_host = tbx_inip_get_string(keyfile, "server", "activity_host", server->alog_host);
    server->alog_port = tbx_inip_get_integer(keyfile, "server", "activity_port", server->alog_port);

    server->rid_check_interval =
        tbx_inip_get_integer(keyfile, "server", "rid_check_interval", server->rid_check_interval);
    server->eject_timeout =
        tbx_inip_get_integer(keyfile, "server", "eject_timeout", server->eject_timeout);
    server->rid_log = tbx_inip_get_string(keyfile, "server", "rid_log", server->rid_log);
    server->rid_eject_script =
        tbx_inip_get_string(keyfile, "server", "rid_eject_script", server->rid_eject_script);
    server->rid_eject_tmp_path =
        tbx_inip_get_string(keyfile, "server", "rid_eject_tmp_path", server->rid_eject_tmp_path);


    if (force_rebuild == 0) {   //** The command line option overrides the file
        cfg->force_resource_rebuild =
            tbx_inip_get_integer(keyfile, "server", "force_resource_rebuild",
                                 cfg->force_resource_rebuild);
    }
    cfg->truncate_expiration =
        tbx_inip_get_integer(keyfile, "server", "truncate_duration", cfg->truncate_expiration);

    i = tbx_inip_get_integer(keyfile, "server", "soft_fail", 0);
    cfg->soft_fail = (i == 0) ? -1 : 0;

    cfg->monitor_fname = tbx_inip_get_string(keyfile, "server", "monitor_fname", cfg->monitor_fname);
    cfg->monitor_enable = tbx_inip_get_integer(keyfile, "server", "monitor_enable", cfg->monitor_enable);

    //*** Do some initial config of the log and debugging info ***
    tbx_log_open(cfg->server.logfile, 1);
    tbx_set_log_level(cfg->server.log_level);
    set_debug_level(cfg->server.debug_level);
    tbx_set_log_maxsize(cfg->server.log_maxsize);

    tbx_monitor_create(cfg->monitor_fname);
    tbx_monitor_set_state(cfg->monitor_enable);

    //*** Now iterate through each resource which is assumed to be all groups beginning with "resource" ***
    //*** NOTE: RocksDB Will lock based on the PID which changes pre/post fork so this MUST be done
    //***        AFTER the fork()!
    apr_pool_t *mount_pool;
    apr_pool_create(&mount_pool, NULL);
    k = tbx_inip_group_count(keyfile);
    tbx_type_malloc_clear(pmarray, pMount_t, k - 1);
    tbx_inip_group_t *igrp = tbx_inip_group_first(keyfile);
    val = 0;
    start_snap_prefix = snap_prefix("snap", 1);
    for (i = 0; i < k; i++) {
        str = tbx_inip_group_get(igrp);
        if (strncmp("resource", str, 8) == 0) {
            pm = &(pmarray[val]);
            pm->keyfile = keyfile;
            pm->group = strdup(str);
            pm->force_resource_rebuild = cfg->force_resource_rebuild;
            pm->snap_prefix = strdup(start_snap_prefix);
            pm->merge_prefix = (merge_snap) ? strdup(merge_snap) : NULL;
            apr_thread_create(&(pm->thread_id), NULL, parallel_mount_resource, (void *) pm,
                              mount_pool);

            val++;
        }

        igrp = tbx_inip_group_next(igrp);
    }
    free(start_snap_prefix);

    //** Wait for all the threads to join **
    apr_status_t dummy;
    for (i = 0; i < val; i++) {
        pm = &(pmarray[i]);
        apr_thread_join(&dummy, pm->thread_id);
        if (pm->snap_prefix) free(pm->snap_prefix);
        if (pm->merge_prefix) free(pm->merge_prefix);
    }

    free(pmarray);

    if (val < 0) {
        printf("parse_config:  No resources defined!!!!\n");
        abort();
    }

    return (0);
}

//*****************************************************************************
//*****************************************************************************

void cleanup_config(Config_t *cfg)
{
    Server_t *server;
    int i;

    server = &(cfg->server);

    if (server->rid_eject_script)
        free(server->rid_eject_script);
    if (server->rid_eject_tmp_path)
        free(server->rid_eject_tmp_path);
    free(server->password);
    free(server->logfile);
    free(server->default_acl);
    free(server->rid_log);

    for (i = 0; i < server->n_iface; i++) {
        free(server->iface[i].hostname);
    }
    free(server->iface);
}

//*****************************************************************************
//*****************************************************************************

void signal_shutdown(int sig)
{
    char date[128];
    apr_ctime(date, apr_time_now());

    log_printf(0, "Shutdown requested on %s\n", date);

    apr_thread_mutex_lock(shutdown_lock);
    shutdown_now = 1;
    apr_thread_cond_broadcast(shutdown_cond);
    apr_thread_mutex_unlock(shutdown_lock);

    signal_taskmgr();
    tbx_network_wakeup(global_network);

    return;
}

//*****************************************************************************
// ibp_shutdown - Shuts down everything
//*****************************************************************************

int ibp_shutdown(Config_t *cfg)
{
    int err;
    Resource_t *r;
    resource_list_iterator_t it;

    //** Close all the resources **
    it = resource_list_iterator(cfg->rl);
    while ((r = resource_list_iterator_next(cfg->rl, &it)) != NULL) {
        if ((err = umount_resource(r)) != 0) {
            char tmp[RID_LEN];
            log_printf(0, "ibp_server: Error closing Resource %s!  Err=%d\n",
                       ibp_rid2str(r->rid, tmp), err);
        }
        free(r);
    }
    resource_list_iterator_destroy(cfg->rl, &it);

    tbx_monitor_destroy();

    return (0);
}

//***************************************************************
//** monitor_check_fn - See if we need to adjust the monitoring state
//***************************************************************

void monitor_check_fn(void *arg, FILE *fd)
{
    Config_t *cfg = (Config_t *)arg;
    char buffer[PATH_MAX];
    FILE *mfd;

    snprintf(buffer, sizeof(buffer), "%s-enable", cfg->monitor_fname);
    buffer[PATH_MAX-1] = '\0';
    if ((mfd=fopen(buffer, "r")) != NULL) {
        fclose(mfd);
        if (cfg->monitor_enable == 0) {
            fprintf(fd, "#-------- Enabling monitoring logs fname:%s\n\n", cfg->monitor_fname);
            cfg->monitor_enable = 1;
            tbx_monitor_set_state(1);
            return;  //** Kick out
        }
    }

    snprintf(buffer, sizeof(buffer), "%s-disable", cfg->monitor_fname);
    buffer[PATH_MAX] = '\0';
    if ((mfd=fopen(buffer, "r")) != NULL) {
        fclose(mfd);
        if (cfg->monitor_enable == 1) {
            fprintf(fd, "#-------- Disabling monitoring logs fname:%s\n\n", cfg->monitor_fname);
            cfg->monitor_enable = 0;
            tbx_monitor_set_state(0);
            return;  //** Kick out
        }
    }
}

//***************************************************************
// server_dump_fn - Dump the server config
//***************************************************************

void server_dump_fn(void *arg, FILE *fd)
{
    Config_t *cfg = (Config_t *)arg;
    char buffer[100*1024];
    int used = 0;

    //** Adjust the monitoring state 1st
    monitor_check_fn(arg, fd);

    //** Then dump the config
    print_config(buffer, &used, sizeof(buffer), cfg);
    fprintf(fd, "%s", buffer);

    return;
}

//*****************************************************************************
// snap_all_rids - Call the DB snap for each mounted resource
//*****************************************************************************

void snap_all_rids(char *prefix, int add_time, FILE *fd)
{
    int err;
    char *my_prefix;
    Resource_t *r;
    resource_list_iterator_t it;

    my_prefix = snap_prefix(prefix, add_time);

    //** Snapping al lthe RIDs
    fprintf(fd, "Starting SNAP of all RIDs. Prefix:%s\n", my_prefix);
    it = resource_list_iterator(global_config->rl);
    while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
        fprintf(fd, "Snapping rid=%s\n", r->name); fflush(fd);
        err = snap_resource(r, my_prefix, fd);
        fprintf(fd, "   rid=%s snap_status=%d\n", r->name, err); fflush(fd);
    }
    resource_list_iterator_destroy(global_config->rl, &it);
    fprintf(fd, "Finished SNAP of all RIDs\n");
}

//*****************************************************************************
// snap_all_rids_handler - Signal handler to snap all the RIDs
//*****************************************************************************

void snap_all_rids_handler(void *arg, FILE *fd)
{
    snap_all_rids("snap", 0, fd);
}


//*****************************************************************************
// configure_signals - Configures the signals
//*****************************************************************************

void configure_signals()
{

    //***Attach the signal handler for shutdown
    apr_signal_unblock(SIGQUIT);
    apr_signal(SIGQUIT, signal_shutdown);

    //** Want everyone to ignore SIGPIPE messages
#ifdef SIGPIPE
    apr_signal_block(SIGPIPE);
#endif

    //** Add the handler for dumping the config
    tbx_siginfo_install("/log/ibp_dump.cfg", SIGUSR1);
    tbx_siginfo_handler_add(SIGUSR1, server_dump_fn, global_config);

    //** Lastly set up the handler for the doing a DB snap
    tbx_siginfo_install("/log/snap.log", SIGRTMIN);
    tbx_siginfo_handler_add(SIGRTMIN, snap_all_rids_handler, NULL);
}

//*****************************************************************************
//*****************************************************************************
//*****************************************************************************

int main(int argc, const char **argv)
{
    Config_t config;
    char *config_file, *merge_snap;
    int i;
    apr_thread_t *rid_check_thread;
    apr_status_t dummy;

    assert_result(apr_initialize(), APR_SUCCESS);
    assert_result(apr_pool_create(&global_pool, NULL), APR_SUCCESS);
    tbx_random_startup();

    shutdown_now = 0;

    global_config = &config;    //** Make the global point to what's loaded
    memset(global_config, 0, sizeof(Config_t)); //** init the data
    global_network = NULL;

    if (argc < 2) {
        printf("ibp_server [-d] [--rebuild] [--merge snap-prefix] config_file\n\n");
        printf("--rebuild   - Rebuild RID databases. Same as force_rebuild=1 in config file.\n");
        printf("              This takes signifcantly longer to start the IBP server and should\n");
        printf("              not be required unless the DB itself is corrupt\n");
        printf("--merge     - Use this DB snapshot across all RIDs to fill in gaps if needed\n");
        printf("              The snap-prefix is the local directory in the RID metadata directory\n");
        printf("              containing the snap to use. Typically these snaps have the name\n");
        printf("              snap-YYYY-MM-DD_HH:MM:SS.  By default on startup it attempts to use\n");
        printf("              the previously oldest snap if the option is not used.\n");
        printf("-d          - Run as a daemon\n");
        printf("config_file - Configuration file\n");
        return (0);
    }

    int daemon = 0;
    int force_rebuild = 0;
    merge_snap = NULL;
    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-d") == 0) {
            daemon = 1;
        } else if (strcmp(argv[i], "--rebuild") == 0) {
            force_rebuild = 1;
        } else if (strcmp(argv[i], "--merge") == 0) {
            i++;  //** Skip over the flag
            merge_snap = (char *)argv[i];
            i++;  //** And the prefix
        }
    }

    config_file = (char *) argv[argc - 1];
    global_config->config_file = config_file;

    //*** Open the config file *****
    printf("Config file: %s\n\n", config_file);

    tbx_inip_file_t *keyfile;

    //** Default log file
    tbx_log_open("/log/ibp.log", 1);

    //* Load the config file
    keyfile = tbx_inip_file_read(config_file, 1);
    if (keyfile == NULL) {
        log_printf(0, "ibp_load_config:  Error parsing config file! file=%s\n", config_file);
        return (-1);
    }

    set_starttime();

    config.rl = create_resource_list(1);

    //** Parse the global options needed before we can safely fork ***
    if (parse_config_prefork(keyfile, &config, force_rebuild) != 0) exit(1); //** Kick out if an error

    //***Launch as a daemon if needed***
    if (daemon == 1) {          //*** Launch as a daemon ***
        if ((strcmp(config.server.logfile, "stdout") == 0) ||
            (strcmp(config.server.logfile, "stderr") == 0)) {
            log_printf(0, "Can't launch as a daemom because log_file is either stdout or stderr\n");
            log_printf(0, "Running in normal mode\n");
        } else {
            i = tbx_fork(config.server.logfile);  //** The parent terminates in the call
            if (i != 0) {
                fprintf(stderr, "ERROR attempting to fork(). err=%d\n", i);
                fprintf(stderr, "Normally this occurs because the new stdout/err files can't be created.\n");
                fprintf(stderr, "The prefix is config.server.logfile=%s\n", config.server.logfile);
                exit(1);
            }
        }
    }

    //** Loadhe rest of the config after forking.
    parse_config_postfork(keyfile, &config, force_rebuild, merge_snap);

    init_thread_slots(2 * config.server.max_threads);   //** Make pigeon holes

    tbx_dnsc_startup_sized(1000);
    init_subnet_list(config.server.iface[0].hostname);

    //*** Install the commands: loads Vectable info and parses config options only ****
    install_commands(keyfile);

    tbx_inip_destroy(keyfile);  //Free the keyfile context

    log_preamble(&config);

    configure_signals();        //** Setup the signal handlers

    //*** Set up the shutdown variables
    apr_thread_mutex_create(&shutdown_lock, APR_THREAD_MUTEX_DEFAULT, global_pool);
    apr_thread_cond_create(&shutdown_cond, global_pool);

    init_stats(config.server.stats_size);
    lock_alloc_init();

    //*** Initialize all command data structures.  This is mainly 3rd party commands ***
    initialize_commands();

    //** Launch the garbage collection threads ...AFTER fork!!!!!!
    resource_list_iterator_t it;
    Resource_t *r;
    it = resource_list_iterator(global_config->rl);
    while ((r = resource_list_iterator_next(global_config->rl, &it)) != NULL) {
        launch_resource_cleanup_thread(r);
    }
    resource_list_iterator_destroy(global_config->rl, &it);

    //** Launch the RID health checker thread
    apr_thread_create(&rid_check_thread, NULL, resource_health_check, NULL, global_pool);

    //*** Start the activity log ***
    alog_open();

    server_loop(&config);       //***** Main processing loop ******

    //** Wait forthe healther checker thread to complete
    apr_thread_join(&dummy, rid_check_thread);

    //*** Shutdown the activity log ***
    alog_close();

    //*** Destroy all the 3rd party structures ***
    destroy_commands();

    lock_alloc_destroy();

    destroy_thread_slots();

    ibp_shutdown(&config);

    free_resource_list(config.rl);

    free_stats();
    cleanup_config(&config);
    log_printf(0, "main: Completed shutdown. Exiting\n");  tbx_log_flush();

    apr_terminate();

    return (0);
}
