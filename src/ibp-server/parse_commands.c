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

//*****************************************************************
//*****************************************************************

#include <string.h>
#include <limits.h>
#include <apr_time.h>
#include "ibp_ClientLib.h"
#include "ibp_server.h"
#include <tbx/log.h>
#include "debug.h"
#include "allocation.h"
#include "resource.h"
#include <tbx/network.h>
#include <tbx/type_malloc.h>
#include "ibp_task.h"
#include "ibp-server_version.h"
#include "ibp_time.h"
#include <tbx/network.h>
#include <tbx/chksum.h>

//*****************************************************************
// get_command_timeout - Gets the command timeout from the NS
//      and stores it i nt he cmd data structure
//*****************************************************************

int get_command_timeout(ibp_task_t *task, char **bstate)
{
    apr_time_t t;
    int fin;

    t = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), TT, &t);
    task->cmd_timeout = apr_time_now() + apr_time_make(t, 0);
    if (t == 0) {
        log_printf(1,
                   "get_command_timeout: Bad timeout value " TT
                   " changing to 2 for LoRS compatibility\n", t);
        task->cmd_timeout = apr_time_now() + apr_time_make(2, 0);
    }

    return (1);
}

//*****************************************************************
// parse_key - Splits the key into RID and cap. Format is RID#key
//*****************************************************************

int parse_key(char **bstate, Cap_t *cap, rid_t *rid, char *crid, int ncrid)
{
    int finished;
    char *tmp = tbx_stk_string_token(NULL, " #", bstate, &finished);

    if (crid != NULL) {         //** Store the character version if requested
        crid[ncrid - 1] = '\0';
        strncpy(crid, tmp, ncrid - 1);
    }
    //** Check the validity of the RID
    if (rid) {
        if (ibp_str2rid(tmp, rid) != 0) {
            log_printf(5, "parse_key: Bad RID: %s\n", tmp);
            return (-1);
        }
    }

    //** Lastly Get the key
    cap->v[sizeof(Cap_t) - 1] = '\0';
    strncpy(cap->v, tbx_stk_string_token(NULL, " ", bstate, &finished), sizeof(Cap_t) - 1);
    log_printf(10, "parse_key: cap=%s\n", cap->v);
    log_printf(10, "parse_key: RID=%s\n", tmp);

    return (0);
}

//*****************************************************************
//  parse_key2 - Assumes bstate is at the start of the string with
//    the next fields being of the form: "rid#key id". It advances
//    bstate and parse the fields and stores it in the capid.
//*****************************************************************

int parse_key2(char **bstate, cap_id_t * cap_id, rid_t * rid, char *crid, int ncrid)
{
    int err, fin;
    char *tmp;

    //** Parse the RID%key
    err = parse_key(bstate, &(cap_id->cap), rid, crid, ncrid);
    if (err != 0) return(err);

    //** Now get the allocation ID
    tmp = tbx_stk_string_token(NULL, " ", bstate, &fin);
    err = sscanf(tmp, LU , &(cap_id->id));
    if (err != 1) {
        log_printf(10, "ERROR parsing ID!\n");
        return(-1);
    }

    return(0);
}

//*****************************************************************
// parse_chksum - Parses and stores the chksum.  Upon success
//     0 is returned.
//*****************************************************************

int parse_chksum(ibp_task_t *task, char **bstate)
{
    int type, fin;
    int64_t bsize;
    tbx_chksum_t cs;

    type = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &type);
    bsize = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), I64T, &bsize);

    if (tbx_chksum_type_valid(type) == 0) {
        log_printf(10, "parse_chksum:  Invalid chksum type!  type=%d\n", type);
        return (IBP_E_CHKSUM_TYPE);
    }

    if (bsize < 1) {
        log_printf(10, "parse_chksum:  Invalid chksum blocksize!  bsize=" I64T "\n", bsize);
        return (IBP_E_CHKSUM_BLOCKSIZE);
    }

    log_printf(15, "parse_chksum:  type=%d size= " I64T "\n", type, bsize);

    tbx_chksum_set(&cs, type);
    return (tbx_ns_chksum_set(&(task->ncs), &cs, bsize));
}

//*****************************************************************
// read_allocate - Reads an allocate command
//
// 1.3:
//    version IBP_ALLOCATE IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d         %d              %d         %d     %ll   %llu   %d
// 1.4:
//    version IBP_ALLOCATE RID IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d         %d      %llu         %d         %d     %ll   %llu  %d
//
//    version IBP_ALLOCATE_CHKSUM chksum_type blocksize RID IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d             %d               %d      int32   %d      %d         %d     %ll   %llu   %d
//
//    version IBP_SPLIT_ALLOCATE mkey mtypekey IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT\n
//      %d           %d           %s     %s            %d         %d     %ll   %llu   %d
//
//    version IBP_SPLIT_ALLOCATE_CHKSUM chksum_type blocksize mkey mtypekey IBP_SOFT|IBP_HARD TYPE DURATION SIZE TIMEOUT \n
//      %d             %d               %d      int32          %s     %s            %d         %d     %ll   %llu   %d
//
//  TYPE: IBP_BYTEARRAY | IBP_BUFFER | IBP_FIFO | IBP_CIRQ
//*****************************************************************

int read_allocate(ibp_task_t *task, char **bstate)
{
    int d, fin;
    ibp_off_t len;
    Cmd_state_t *cmd = &(task->cmd);

    fin = 0;

    debug_printf(1, "read_allocate:  Starting to process buffer\n");

    Allocation_t *a = &(cmd->cargs.allocate.a);
    rid_t *rid = &(cmd->cargs.allocate.rid);
    Cmd_allocate_t *ca = &(cmd->cargs.allocate);

    ca->cs_type = -1;
    ca->cs_blocksize = 0;

    memset(a, 0, sizeof(Allocation_t));
    memset(rid, 0, sizeof(rid_t));

    //** Parse the chksumming options if applicable
    if (cmd->version > IBPv031) {
        if ((cmd->command == IBP_ALLOCATE_CHKSUM) || (cmd->command == IBP_SPLIT_ALLOCATE_CHKSUM)) {
            d = -1;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &d);
            ca->cs_type = d;
            if ((tbx_chksum_type_valid(d) == 0) && (d != CHKSUM_NONE)) {
                log_printf(10, "read_allocate: bad chksum_type=%d\n", d);
                send_cmd_result(task, IBP_E_CHKSUM_TYPE);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: bad chksum_type=%d", cmd->name, d);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }

            len = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), I64T, &len);
            ca->cs_blocksize = len;
            if ((len > (int64_t) 2147483648) || ((len <= 0) && (d != CHKSUM_NONE))) {
                log_printf(10, "read_allocate: bad chksum blocksize =" I64T "\n", len);
                send_cmd_result(task, IBP_E_CHKSUM_BLOCKSIZE);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: bad chksum_blocksize=" I64T, cmd->name, len);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }

        }
    }
    //** Parse the RID ***
    if (cmd->version > IBPv031) {
        if ((cmd->command != IBP_SPLIT_ALLOCATE) && (cmd->command != IBP_SPLIT_ALLOCATE_CHKSUM)) {
            char *tmp = tbx_stk_string_token(NULL, " ", bstate, &fin);
            log_printf(15, "read_allocate:  cmd(rid)=%s\n", tmp);
            if (ibp_str2rid(tmp, rid) != 0) {
                log_printf(10, "read_allocate: Bad RID: %s\n", tmp);
                send_cmd_result(task, IBP_E_INVALID_RID);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid RID: %s", cmd->name, tmp);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }
        } else {                //** IBP_SPLIT_ALLOCATE/IBP_SPLIT_ALLOCATE_CHKSUM
            if (parse_key2(bstate, &(cmd->cargs.allocate.master_cap), rid, NULL, 0) != 0) {
                log_printf(10, "read_allocate: Bad RID/mcap!\n");
                send_cmd_result(task, IBP_E_INVALID_RID);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid RID/mcap", cmd->name);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }
        }
    } else {
        ibp_empty_rid(rid);     //** Don't care which resource we use
    }

    char dummy[RID_LEN];
    log_printf(15, "read_allocate:  rid=%s\n", ibp_rid2str(*rid, dummy));

    //***Reliability: IBP_HARD | IBP_SOFT ***
    d = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &d);
    a->reliability = d;
    if (d == IBP_HARD) {
        a->reliability = ALLOC_HARD;
    } else if (d == IBP_SOFT) {
        a->reliability = ALLOC_SOFT;
    } else {
        log_printf(1, "read_allocate: Bad reliability: %d\n", d);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad reliability=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    //***Type: IBP_BYTEARRAY | IBP_BUFFER | IBP_FIFO | IBP_CIRQ ***
    d = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &d);
    a->type = d;
    if ((d != IBP_BYTEARRAY) && (d != IBP_BUFFER) && (d != IBP_FIFO) && (d != IBP_CIRQ)) {
        log_printf(1, "read_allocate: Bad type: %d\n", d);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad type=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //================ Disabling all allocation types except BYTEARRAYS ================
    if (a->type != IBP_BYTEARRAY) {
        log_printf(1, "read_allocate: Only support IBP_BYTEARRAY!  type: %d\n", d);
        send_cmd_result(task, IBP_E_TYPE_NOT_SUPPORTED);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Only support IBP_BYTEARRAY!  type=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);

    }
    //================ Disabling all allocation types except BYTEARRAYS ================

    //*** Duration ***
    d = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &d);
    a->expiration = ibp_time_now() + d;
    if (d == 0) {
        log_printf(1, "read_allocate: Bad duration: %d\n", d);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad duration=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    } else if (d == -1) {       //** Infinite duration is requested
        a->expiration = INT_MAX;
    }
    //** Allocation size **

    len = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), I64T, &len);
    log_printf(1, "read_allocate: size : " I64T "\n", len);
    a->max_size = len;
    if (len == 0) {
        log_printf(1, "read_allocate: Bad size : " I64T "\n", len);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad size=%d", cmd->name, len);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Right now we only support a 2GB-1 byte allocations
    if (a->max_size > 2147483647) {
        if (global_config->server.big_alloc_enable == 0) {
            log_printf(1, "read_allocate: Size > 2GB : " I64T "\n", len);
            send_cmd_result(task, IBP_E_WOULD_EXCEED_LIMIT);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Size > 2GiB limit. len=" I64T, cmd->name, len);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
    }

    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s reliability=%d duration=%d size=" I64T, cmd->name, ibp_rid2str(*rid, dummy), a->reliability, d, a->max_size);

    debug_printf(1, "read_allocate: Successfully parsed allocate command\n");
    return (0);
}

//*****************************************************************
// read_merge_allocate - Merges 2 allocations if possible. The child
//    allocation is removed if successful.
//
// v1.4
//    version IBP_MERGE_ALLOCATE mkey mtypekey ckey ctypekey TIMEOUT\n
//      %d           %d           %s     %s     %s     %s       %d
//
//*****************************************************************

int read_merge_allocate(ibp_task_t *task, char **bstate)
{
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_merge_t *op = &(cmd->cargs.merge);
    rid_t child_rid;

    debug_printf(1, "read_merge_allocate:  Starting to process buffer\n");

    //** Parse the master key
    if (parse_key2(bstate, &(op->mkey), &(op->rid), op->crid, sizeof(op->crid)) != 0) {
        log_printf(10, "read_merge_allocate: Bad RID/mcap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad master RID/mcap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    //** Parse the child key
    if (parse_key2(bstate, &(op->ckey), &child_rid, NULL, 0) != 0) {
        log_printf(10, "read_merge_allocate: Child RID/mcap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad child RID/mcap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    //** Now compare the rid's to make sure they are the same
    if (ibp_compare_rid(op->rid, child_rid) != 0) {
        log_printf(10, "read_merge_allocate: Child/Master RID!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad child master RID", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    tbx_monitor_obj_create(&(task->mo), "[%s] Master: RID=%s id=" LU " -- child id=" LU, cmd->name, op->crid, op->mkey.id, op->ckey.id);

    get_command_timeout(task, bstate);

    debug_printf(1, "read_merge_allocate: Successfully parsed allocate command\n");
    return (0);
}


//*****************************************************************
//  read_status - Parses the IBP_STATUS command
//
// 1.4
//   version IBP_STATUS RID   IBP_ST_INQ password  TIMEOUT \n
//      %d       %d     %llu      %d         %31s       %d
//   version IBP_STATUS RID   IBP_ST_CHANGE password  TIMEOUT \n max_hard max_soft max_duration \n
//      %d       %d     %llu      %d            %31s       %d      %llu     %llu      %d
//   version IBP_STATUS  IBP_ST_RES  TIMEOUT \n
//      %d       %d         %d         %d
//   version IBP_STATUS  IBP_ST_STATS  start_time   TIMEOUT \n
//      %d       %d         %d            %d           %d
//   version IBP_STATUS  IBP_ST_VERSION TIMEOUT \n
//      %d       %d           %d           %d
//
//*****************************************************************

int read_status(ibp_task_t *task, char **bstate)
{
    int d, finished;
    Cmd_state_t *cmd = &(task->cmd);

    finished = 0;

    debug_printf(1, "read_status:  Starting to process buffer\n");

    Cmd_status_t *status = &(cmd->cargs.status);
    status->subcmd = 0;

    //*** Only way to distinguish between commands is based on the number of args.
    int nargs;
    char *dupstr = strdup(*bstate);
    char *dstate;
    tbx_stk_string_token(dupstr, " ", &dstate, &finished);
    nargs = 2;
    while (finished == 0) {
        nargs++;
        tbx_stk_string_token(NULL, " ", &dstate, &finished);
    }
    finished = 0;
    free(dupstr);

//   log_printf(10, "read_status: ns=%d nargs = %d\n", tbx_ns_getid(task->ns), nargs);

    //** Parse the RID (or the sub command) ***
    if (cmd->version > IBPv031) {
        char *tmp;
        tmp = tbx_stk_string_token(NULL, " ", bstate, &finished);
        status->crid[sizeof(status->crid) - 1] = '\0';
        if (nargs < 5) {        //** IBP_ST_RES/IBP_ST_VERSION command
            sscanf(tmp, "%d", &(status->subcmd));
//         log_printf(10, "read_status: ns=%d subcmd = %d\n", tbx_ns_getid(task->ns), status->subcmd);
        } else {
            if (ibp_str2rid(tmp, &(status->rid)) != 0) {
                log_printf(1, "read_status: Bad RID: %s\n", tmp);
                send_cmd_result(task, IBP_E_INVALID_RID);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, tmp);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }

            if (ibp_rid_is_empty(status->rid) == 1) {   //** Pick one at random
                resource_pick(global_config->rl, &(status->rid));
                log_printf(6, "read_status: Read rid=0 so picking one at random\n");
            }

            ibp_rid2str(status->rid, status->crid);

            //*** Get the sub command ***
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &(status->subcmd));
        }
    } else {
        //** Pick a random resource to use
        resource_pick(global_config->rl, &(status->rid));
        ibp_rid2str(status->rid, status->crid);
        log_printf(6, "read_status: IBP_v031 doesn't support RID.  Picked one at random rid=%s\n",
                   status->crid);

        //*** Get the sub command ***
        char *tmp = tbx_stk_string_token(NULL, " ", bstate, &finished);
        debug_printf(15, "read_status: should be getting the subcmd =!%s!\n", tmp);
        sscanf(tmp, "%d", &(status->subcmd));
    }

    //*** Process the subcommand ***
    switch (status->subcmd) {
    case IBP_ST_RES:
        ibp_empty_rid(&(status->rid));
        status->password[PASSLEN - 1] = '\0';
        strncpy(status->password, global_config->server.password, sizeof(status->password) - 1);
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_ST_RES", cmd->name);
        get_command_timeout(task, bstate);      //** Get the timeout
        break;
    case IBP_ST_VERSION:       //** Return the version
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_ST_VERSION", cmd->name);
        get_command_timeout(task, bstate);      //** Get the timeout
        break;
    case IBP_ST_STATS:         //** Stats command not standard IBP
        ibp_empty_rid(&(status->rid));
        status->password[PASSLEN - 1] = '\0';
        strncpy(status->password, global_config->server.password, sizeof(status->password) - 1);
        d = -1;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
        status->start_time = d;
        if (d == 0) {
            log_printf(1, "read_status: Invalid start time %d\n", d);
            send_cmd_result(task, IBP_E_BAD_FORMAT);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: IBP_ST_STATS Invalid start time=%d", cmd->name, d);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_ST_STATS", cmd->name);
        get_command_timeout(task, bstate);      //** Get the timeout
        break;
    case IBP_ST_INQ:
        status->password[0] = '\0';
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%31s", status->password);
        debug_printf(15, "read_status: IBP_ST_INQ password = %s\n", status->password);
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_ST_INQ", cmd->name);
        get_command_timeout(task, bstate);      //** Get the timeout
        break;
    case IBP_ST_CHANGE:
        status->password[0] = '\0';
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%31s", status->password);
        get_command_timeout(task, bstate);      //** Get the timeout

//================= IBP_ST_CHANGED NOT ALLOWED===============================
        log_printf(1, "read_status: IBP_ST_CHANGE request ignored! ns=%d\n",
                   tbx_ns_getid(task->ns));
        send_cmd_result(task, IBP_E_INVALID_CMD);
        tbx_ns_close(task->ns);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: IBP_ST_CHANGE request ignored!", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);

        int nbytes;
        char buffer[100];
        nbytes =
            server_ns_readline(task->ns, buffer, sizeof(buffer), global_config->server.timeout);
        if (nbytes < 0) {
            return (nbytes);    //** Return the error back up the food chain
        }
        //*** Grab the new hard size ***
        status->new_size[ALLOC_HARD] = 0;
        sscanf(tbx_stk_string_token(buffer, " ", bstate, &finished), LU,
               &(status->new_size[ALLOC_HARD]));

        //*** Grab the new soft size ***
        status->new_size[ALLOC_SOFT] = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), LU,
               &(status->new_size[ALLOC_SOFT]));

        //*** Grab the new hard size ***
        status->new_duration = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%ld", &(status->new_duration));

        debug_printf(1, "read_status: change request of h:" LU " s:" LU " d:%ld  ns=%d\n",
                     status->new_size[ALLOC_HARD], status->new_size[ALLOC_SOFT],
                     status->new_duration, tbx_ns_getid(task->ns));

        break;
    default:
        log_printf(1, "read_status: Unknown sub-command %d\n", status->subcmd);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Unknown sub-command=%d", cmd->name, status->subcmd);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }


    debug_printf(1, "read_status: Successfully parsed status command\n");
    return (0);
}

//*****************************************************************
//  read_manage - Reads an ibp_manage command
//
// 1.4
//    version IBP_MANAGE key typekey IBP_INCR|IBP_DECR captype timeout \n
//
//    version IBP_MANAGE key typekey IBP_CHNG|IBP_PROBE captype maxSize life reliability timeout \n
//
// 1.5
//    version IBP_ALIAS_MANAGE key typekey IBP_INCR|IBP_DECR captype mkey mtypekey timeout \n
//
//    version IBP_ALIAS_MANAGE key typekey IBP_CHNG offset len duration mkey mtypekey timeout \n
//
//    version IBP_ALIAS_MANAGE key typekey IBP_PROBE timeout \n
//
//    version IBP_MANAGE key typekey IBP_TRUNCATE new_size timeout \n
//
//*****************************************************************

int read_manage(ibp_task_t *task, char **bstate)
{
    int d, finished;
    unsigned long long int llu;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_manage_t *manage = &(cmd->cargs.manage);

    finished = 0;

    debug_printf(1, "read_manage:  Starting to process buffer\n");

    //** Parse the RID/key info
    if (parse_key2(bstate, &(manage->cap), &(manage->rid), manage->crid, sizeof(manage->crid)) != 0) {
        log_printf(10, "read_merge_allocate: Bad RID/mcap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/mcap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (global_config->soft_fail);
    }

    debug_printf(10, "read_manage: cap=%s " LU "\n", manage->cap.cap.v, manage->cap.cap.id);
    debug_printf(10, "read_mange: RID=%s\n", manage->crid);

    //*** Get the subcommand ***
    d = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
    if ((d != IBP_INCR) && (d != IBP_DECR) && (d != IBP_CHNG) && (d != IBP_PROBE)
        && (d != IBP_TRUNCATE)) {
        log_printf(1, "read_manage: Unknown sub-command %d\n", d);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: unknown sub-command=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    manage->subcmd = d;

    //** Get the cap type
    if (manage->subcmd != IBP_TRUNCATE) {
        d = -1;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
        switch (d) {
        case IBP_READCAP:
            manage->captype = READ_CAP;
            break;
        case IBP_WRITECAP:
            manage->captype = WRITE_CAP;
            break;
        case IBP_MANAGECAP:
            manage->captype = MANAGE_CAP;
            break;
        default:
            if ((manage->subcmd == IBP_INCR) || (manage->subcmd == IBP_DECR)) { //** Ignored for other commands
                log_printf(10, "read_manage:  Invalid cap type (%d)!\n", d);
                send_cmd_result(task, IBP_E_BAD_FORMAT);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid cap type=%d", cmd->name, d);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }
        }
    }

    switch (manage->subcmd) {
    case IBP_INCR:
    case IBP_DECR:
        if (cmd->command == IBP_ALIAS_MANAGE) { //** Get the "master" key if this is a alias command
            //** Strip the RID.  We only keep it for the alias
            if (parse_key2(bstate, &(manage->master_cap), NULL, NULL, 0) != 0) {
                log_printf(10, "read_merge_allocate: Bad RID/master_cap!\n");
                send_cmd_result(task, IBP_E_INVALID_RID);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/mcap for INCR/DECR", cmd->name);
                tbx_monitor_obj_destroy(&(task->mo));
                return (global_config->soft_fail);
            }

            log_printf(10, "read_manage: master cap=%s " LU "\n", manage->cap.cap.v, manage->cap.id);
        }

        get_command_timeout(task, bstate);
        tbx_monitor_obj_create(&(task->mo), "[%s] %s id=" LU, cmd->name, ((manage->subcmd==IBP_INCR) ? "IBP_INCR" : "IBP_DECR"), manage->cap.id);
        return (0);

    case IBP_PROBE:            //**Skip the unused fields - This needs to be cleaned up in the protocol!
        if (cmd->command == IBP_MANAGE) {
            tbx_stk_string_token(NULL, " ", bstate, &finished);
            tbx_stk_string_token(NULL, " ", bstate, &finished);
            tbx_stk_string_token(NULL, " ", bstate, &finished);
        }

        get_command_timeout(task, bstate);
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_PROBE id=" LU, cmd->name, manage->cap.id);
        return (0);
    case IBP_TRUNCATE:         //**Get the new size!
        //** Get the new size
        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
        manage->new_size = llu;
        log_printf(15, "read_manage: IBP_TRUNCATE new size=%llu\n", llu);

        get_command_timeout(task, bstate);
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_TRUNCATE id=" LU " new_size=" I64T, cmd->name, manage->cap.id, manage->new_size);
        return (0);
    case IBP_CHNG:
        if (cmd->command == IBP_ALIAS_MANAGE) { //** Get the new offset
            llu = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
            manage->offset = llu;
        }
        //** Get the new size
        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
        manage->new_size = llu;
        if ((llu == 0) && (manage->subcmd == IBP_CHNG)) {
            log_printf(10, "read_manage:  Invalid new size (" LU ")!\n", manage->new_size);
            send_cmd_result(task, IBP_E_WOULD_DAMAGE_DATA);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Can't truncate alloication to 0 bytes", cmd->name);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
        //**Read the new duration
        d = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
        if (cmd->command == IBP_MANAGE) {
            manage->new_duration = ibp_time_now() + d;
            if (d == 0) {
                log_printf(1, "read_manage: Bad duration: %d\n", d);
                send_cmd_result(task, IBP_E_INVALID_PARAMETER);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad duaration=%d", cmd->name, d);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            } else if (d < 0) { //** No change requested on duration
                manage->new_duration = -1;
            }
        } else {
            if (d > 0) {
                manage->new_duration = ibp_time_now() + d;
            } else {
                manage->new_duration = d;
            }
        }

        if (cmd->command == IBP_MANAGE) {
            //**Read the new reliability
            d = -1;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
            manage->new_reliability = d;
            if (d > -1) {
                if (d == IBP_HARD) {
                    manage->new_reliability = ALLOC_HARD;
                } else if (d == IBP_SOFT) {
                    manage->new_reliability = ALLOC_SOFT;
                } else if (manage->subcmd == IBP_CHNG) {
                    log_printf(1, "read_manage: Bad reliability: %d\n", d);
                    send_cmd_result(task, IBP_E_BAD_FORMAT);
                    tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad reliability=%d", cmd->name, d);
                    tbx_monitor_obj_destroy(&(task->mo));
                    return (-1);
                }
            }
        }

        if (cmd->command == IBP_ALIAS_MANAGE) { //** Get the "master" key if this is a alias command
            //** Strip the RID.  We only keep it for the alias
            if (parse_key2(bstate, &(manage->master_cap), NULL, NULL, 0) != 0) {
                log_printf(10, "read_merge_allocate: Bad RID/master_cap!\n");
                send_cmd_result(task, IBP_E_INVALID_RID);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/master cap", cmd->name);
                tbx_monitor_obj_destroy(&(task->mo));
                return (global_config->soft_fail);
            }
        }

        get_command_timeout(task, bstate);
        tbx_monitor_obj_create(&(task->mo), "[%s] IBP_CHNG id=" LU, cmd->name, manage->cap.id);
        return (0);
    default:
        log_printf(10, "read_manage:  Invalid subcmd (%d)!\n", manage->subcmd);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: invalid subcmd=%d", cmd->name, manage->subcmd);
        tbx_monitor_obj_destroy(&(task->mo));
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        return (-1);
    }

    return (-100);              //** NEver get here
}


//*****************************************************************
//  read_rename - Reads an ibp_rename command
//
// 1.5
//    version IBP_RENAME key typekey timeout\n
//
//*****************************************************************

int read_rename(ibp_task_t *task, char **bstate)
{
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_manage_t *manage = &(cmd->cargs.manage);

    debug_printf(1, "read_rename:  Starting to process buffer\n");

    if (parse_key2(bstate, &(manage->cap), &(manage->rid), manage->crid, sizeof(manage->crid)) != 0) {
        log_printf(10, "read_rename_allocate: Bad RID/cap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/cap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (global_config->soft_fail);
    }

    debug_printf(10, "read_mange: RID=%s\n", manage->crid);

    tbx_monitor_obj_create(&(task->mo), "[%s] id=" LU " rid=%s", cmd->name, manage->cap.id, manage->crid);

    get_command_timeout(task, bstate);
    return (0);
}

//*****************************************************************
//  read_alias_allocate - Reads an ibp_alias_allocate command
//
// 1.5
//    version IBP_ALIAS_ALLOCATE key typekey offset len duration timeout\n
//
//   if offset=len=0 then can use entire alocation
//   if duration=0 then use master allocations expiration
//*****************************************************************

int read_alias_allocate(ibp_task_t *task, char **bstate)
{
    int finished;
    uint64_t lu;

    Cmd_alias_alloc_t *cmd = &(task->cmd.cargs.alias_alloc);

    finished = 0;

    debug_printf(1, "read_alias_allocate:  Starting to process buffer\n");

    //** Get the cap/rid info
    if (parse_key2(bstate, &(cmd->cap), &(cmd->rid), cmd->crid, sizeof(cmd->crid)) != 0) {
        log_printf(10, "Bad RID/master_cap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/cap", task->cmd.name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (global_config->soft_fail);
    }

    log_printf(10, "read_alias_allocate: cap=%s " LU "\n", cmd->cap.cap.v, cmd->cap.id);
    log_printf(10, "read_alias_allocate: RID=%s\n", cmd->crid);

    cmd->offset = cmd->len = cmd->expiration == 0;

    //** Get the offset **
    if (sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), I64T, &(cmd->offset)) != 1) {
        log_printf(5, "read_alias_allocate: Bad offset cap= %s\n", cmd->crid);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad offset", task->cmd.name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Get the len **
    if (sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), I64T, &(cmd->len)) != 1) {
        log_printf(5, "read_alias_allocate: Bad length cap= %s\n", cmd->crid);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad length", task->cmd.name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Get the duration **
    if (sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), LU, &lu) != 1) {
        log_printf(5, "read_alias_allocate: Bad duration cap= %s\n", cmd->crid);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad duration", task->cmd.name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    cmd->expiration = 0;
    if (lu != 0) cmd->expiration = lu + ibp_time_now();

    tbx_monitor_obj_create(&(task->mo), "[%s] id=" LU, task->cmd.name, cmd->cap.id);

    get_command_timeout(task, bstate);
    return (0);
}

//*****************************************************************
//  read_validate_get_chksum - Reads an IBP_VALIDATE_CHKSUM of IBP_GET_CHKSUM command
//
// 1.4
//    version IBP_VALIDATE_CHKSUM key typekey correct_errors timeout \n
//    version IBP_GET_CHKSUM key typekey chksum_info_only timeout \n
//*****************************************************************

int read_validate_get_chksum(ibp_task_t *task, char **bstate)
{
    int finished, i;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_write_t *w = &(cmd->cargs.write);

    debug_printf(1, "read_validate_get_chksum:  Starting to process buffer: cmd=%d\n",
                 cmd->command);

    //** Get the cap/rid info
    if (parse_key2(bstate, &(w->cap), &(w->rid), w->crid, sizeof(w->crid)) != 0) {
        log_printf(10, "Bad RID/master_cap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID/cap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    debug_printf(10, "cap=%s " LU "\n", w->cap.cap.v, w->cap.id);
    debug_printf(10, "RID=%s\n", w->crid);

    //** Get the "correct_errors" or "chksum_info_only" field
    i = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &i);
    w->iovec.total_len = i;     //** Store the correct_errors value in the iovec field
    if (i < 0) {
        log_printf(10,
                   "read_validate_get_chksum:  Invalid correct_errors/chksum_info_only (%d)!\n", i);
        send_cmd_result(task, IBP_E_INV_PAR_SIZE);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid correct_errors or chksum_ino_only field=%d", cmd->name, i);
        tbx_monitor_obj_destroy(&(task->mo));
        return (global_config->soft_fail);
    }
    //** and finally the timeout
    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] id=" LU, cmd->name, w->cap.id);

    debug_printf(1, "read_validate_get_chksum: Successfully parsed\n");
    return (0);
}

//*****************************************************************
//  read_write - Reads an IBP_write command
//
// 1.4
//    version IBP_WRITE key typekey offset length timeout \n
//    version IBP_STORE key typekey length timeout \n
//
// IBP_VEC_WRITE | IBP_VEC_WRITE_APPEND
//    version IBP_VEC_WRITE key typekey n_ele offset_1 len_1 offset_2 len_2 ... offset_N len_N timeout \n
// IBP_VEC_WRITE_CHKSUM | IBP_VEC_WRITE_APPEND_CHKSUM
//    version IBP_VEC_WRITE_CHKSUM tbx_chksum_type block_size key typekey n_ele offset_1 len_1 offset_2 len_2 ... offset_N len_N timeout \n
//
//*****************************************************************

int read_write(ibp_task_t *task, char **bstate)
{
    int finished, i;
    unsigned long long int llu;
    int64_t i64t;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_write_t *w = &(cmd->cargs.write);
    ibp_iovec_t *iovec = &(w->iovec);

    finished = 0;

    debug_printf(1, "read_write:  Starting to process buffer\n");

    w->sending = 0;             //** Flag the command as nopt sending data so the handle will load the res, etc.

    //** If a chksum command then pick off the chksum info
    if ((cmd->command == IBP_WRITE_CHKSUM) || (cmd->command == IBP_STORE_CHKSUM) ||
        (cmd->command == IBP_VEC_WRITE_CHKSUM)) {
        task->enable_chksum = 1;
        i = parse_chksum(task, bstate);
        if (i != 0) {
            log_printf(10, "read_write:  Invalid chksum error(%d)!\n", i);
            send_cmd_result(task, i);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid chksum error=%d", cmd->name, i);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
    } else {
        task->enable_chksum = 0;
    }


    //** Get the cap/rid info
    if (parse_key2(bstate, &(w->cap), &(w->rid), w->crid, sizeof(w->crid)) != 0) {
        log_printf(10, "Bad RID/master_cap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid RID/cap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    debug_printf(10, "read_write: cap=%s " LU "\n", w->cap.cap.v, w->cap.id);
    debug_printf(10, "read_write: RID=%s\n", w->crid);

    if ((cmd->command == IBP_VEC_WRITE_CHKSUM) || (cmd->command == IBP_VEC_WRITE)) {
        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);       //** Get the count
        if (llu < 1) {
            log_printf(10, "read_write:  Invalid IOVEC count (%llu)!\n", llu);
            send_cmd_result(task, IBP_E_FILE_SEEK_ERROR);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC count=%llu", cmd->name, llu);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
        if (llu > IOVEC_MAX) {
            log_printf(10, "read_write:  IOVEC count too big (%llu)!\n", llu);
            send_cmd_result(task, IBP_E_INVALID_PARAMETER);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC count=%llu is to big", cmd->name, llu);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }

        iovec->n = llu;
    } else {
        iovec->n = 1;
    }

    log_printf(15, "read_write: iovec->n=%d\n", iovec->n);

    //** Read the offsets/lengths
    iovec->total_len = 0;
    iovec->transfer_total = 0;
    for (i = 0; i < iovec->n; i++) {
        if ((cmd->command == IBP_STORE) || (cmd->command == IBP_STORE_CHKSUM)) {        // offset is append
            iovec->vec[i].off = -1;
        } else {
            i64t = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), I64T, &i64t);
            if (i64t < 0) {
                log_printf(10, "read_write:  Invalid vec offset[%d] (" I64T ")!\n", i, i64t);
                send_cmd_result(task, IBP_E_FILE_SEEK_ERROR);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC offset[%d]=" I64T, cmd->name, i, i64t);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }
            iovec->vec[i].off = i64t;
        }

        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
        iovec->vec[i].len = llu;
        if (llu < 1) {
            log_printf(10, "read_write:  Invalid vec length[%d] (%llu)!\n", i, llu);
            send_cmd_result(task, IBP_E_INV_PAR_SIZE);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC len[%d]=%llu", cmd->name, i, llu);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }

        iovec->total_len = iovec->total_len + llu;
        iovec->vec[i].cumulative_len = iovec->total_len;
        log_printf(15, "read_write: iovec->vec[%d] off=" OT " len=" OT "\n", i, iovec->vec[i].off,
                   iovec->vec[i].len);
    }

    //** and finally the timeout
    get_command_timeout(task, bstate);

    debug_printf(1, "read_write: Successfully parsed io->n=%d off[0]=" I64T " len[0]=" I64T "\n",
                 w->iovec.n, w->iovec.vec[0].off, w->iovec.vec[0].len);

    tbx_monitor_obj_create(&(task->mo), "[%s] id=" LU " n_iov=%d off[0]=" I64T " len[0]=" I64T, cmd->name, w->cap.id, w->iovec.n, w->iovec.vec[0].off, w->iovec.vec[0].len);

    return (0);
}

//*****************************************************************
//  read_read - Reads an IBP_load command
//
// v1.4
//    version IBP_LOAD key typekey offset length timeout \n

// v1.4 - IBP_send command
//    version IBP_SEND key typekey remote_wcap offset length src_timeout dest_timeout dest_ClientTimeout\n//
//
// IBP_VEC_READ
//    version IBP_VEC_READ key typekey n_ele offset_1 len_1 offset_2 len_2 ... offset_N len_N timeout \n
//
// IBP_VEC_READ_CHKSUM
//    version IBP_VEC_READ_CHKSUM tbx_chksum_type blocksize key typekey n_ele offset_1 len_1 offset_2 len_2 ... offset_N len_N timeout \n
//
// IBP_PHOEBUS_SEND
//    version IBP_PHOEBUS_SEND phoebus_path|auto key typekey remote_wcap offset length src_timeout dest_timeout dest_ClientTimeout\n
//
// IBP_PUSH|IBP_PULL
//    version IBP_PUSH|IBP_PULL ctype local_key remote_wcap local_typekey local_offset remote_offset length src_timeout dest_timeout dest_ClientTimeout\n
//
//
//    ctype - Connection type:
//         IBP_TCP - Normal transfer mode
//         IBP_PHOEBUS phoebus_path|auto - USe phoebus transfer
//    phoebus_path - Comma separated List of phoebus hosts/ports, eg gateway1/1234,gateway2/4321
//        Use 'auto' to use the default phoebus path
//    *_offset - If -1 this is an append operation.  This is only valid for the following combinations:
//        IBP_PUSH - remote_offset and IBP_PULL - local_offset
//*****************************************************************

int read_read(ibp_task_t *task, char **bstate)
{
    int finished, ctype, get_remote_cap, i;
    char *path, *tmp;
    unsigned long int lu;
    long long int ll;
    long long unsigned int llu;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_read_t *r = &(cmd->cargs.read);
    ibp_iovec_t *iovec = &(r->iovec);

    memset(r, 0, sizeof(Cmd_read_t));
    finished = 0;

    debug_printf(1, "read_read:  Starting to process buffer\n");

    r->recving = 0;             //** Flag the command as not recving data so the handle will load the res, etc.

    //** If a chksum command then pick off the chksum info
    if ((cmd->command == IBP_PUSH_CHKSUM) || (cmd->command == IBP_PULL_CHKSUM) ||
        (cmd->command == IBP_SEND_CHKSUM) || (cmd->command == IBP_PHOEBUS_SEND_CHKSUM) ||
        (cmd->command == IBP_LOAD_CHKSUM) || (cmd->command == IBP_VEC_READ_CHKSUM)) {
        task->enable_chksum = 1;
        i = parse_chksum(task, bstate);
        if (i != 0) {
            log_printf(10, "read_read:  Invalid chksum error(%d)!\n", i);
            send_cmd_result(task, i);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid chksum error=%d", cmd->name, i);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
    } else {
        task->enable_chksum = 0;
    }

    r->write_mode = 1;          //** Default is to append
    r->transfer_dir = IBP_PUSH;

    ctype = IBP_TCP;
    r->path[0] = '\0';
    get_remote_cap = 0;
    switch (cmd->command) {
    case IBP_PULL:
    case IBP_PULL_CHKSUM:
        r->transfer_dir = IBP_PULL;
    //** Line below tells the compiler to ignore the fall through
    //@fallthrough@
    case IBP_PUSH:
    case IBP_PUSH_CHKSUM:
        get_remote_cap = 1;
        ctype = IBP_TCP;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &ctype);
        if ((ctype != IBP_TCP) && (ctype != IBP_PHOEBUS)) {
            log_printf(10, "read_read:  Invalid ctype(%d)!\n", ctype);
            send_cmd_result(task, IBP_E_TYPE_NOT_SUPPORTED);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid network ctype=%d", cmd->name, ctype);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
        break;
    case IBP_PHOEBUS_SEND:
    case IBP_PHOEBUS_SEND_CHKSUM:
        r->transfer_dir = IBP_PUSH;
        get_remote_cap = 1;
        ctype = IBP_PHOEBUS;
        break;
    case IBP_SEND:
    case IBP_SEND_CHKSUM:
        r->transfer_dir = IBP_PUSH;
        get_remote_cap = 1;
        break;
    }

    r->ctype = ctype;

    if (ctype == IBP_PHOEBUS) { //** Get the phoebus path
        r->path[sizeof(r->path) - 1] = '\0';
        path = tbx_stk_string_token(NULL, " ", bstate, &finished);
        if (strcmp(path, "auto") == 0)
            path = "\0";
        strncpy(r->path, path, sizeof(r->path) - 1);
        debug_printf(10, "read_read: phoebus_path=%s\n", r->path);
    }

    //** Get the cap/rid info
    if (parse_key(bstate, &(r->cap.cap), &(r->rid), r->crid, sizeof(r->crid)) != 0) {
        log_printf(10, "Bad RID/master_cap!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        return (-1);
    }

    debug_printf(10, "read_read: cap=%s " LU "\n", r->cap.cap.v, r->cap.id);

    if (get_remote_cap == 1) {  //** For send/tbx_stack_push/pull commands get the remote cap
        task->child = NULL;
        r->remote_cap[sizeof(r->remote_cap) - 1] = '\0';
        strncpy(r->remote_cap, tbx_stk_string_token(NULL, " ", bstate, &finished),
                sizeof(r->remote_cap) - 1);
        debug_printf(10, "read_read: remote_cap=%s\n", r->remote_cap);
    }

    debug_printf(10, "read_read: RID=%s\n", r->crid);

    //** Now get the allocation ID.. For some reason this isn't with the cap --PROTOCOL CHANGE????
    tmp = tbx_stk_string_token(NULL, " ", bstate, &finished);
    if (sscanf(tmp, LU , &(r->cap.id)) != 1) {
        log_printf(10, "ERROR parsing ID!\n");
        send_cmd_result(task, IBP_E_INVALID_READ_CAP);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid reap cap", cmd->name);
        tbx_monitor_obj_destroy(&(task->mo));
        return(-1);
    }

    if ((cmd->command == IBP_VEC_READ_CHKSUM) || (cmd->command == IBP_VEC_READ)) {
        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);       //** Get the count
        if (llu < 1) {
            log_printf(10, "read_read:  Invalid IOVEC count (%llu)!\n", llu);
            send_cmd_result(task, IBP_E_FILE_SEEK_ERROR);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC count=%llu", cmd->name, llu);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
        if (llu > IOVEC_MAX) {
            log_printf(10, "read_read:  IOVEC count too big (%llu)!\n", llu);
            send_cmd_result(task, IBP_E_INVALID_PARAMETER);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC count=%llu is to big", cmd->name, llu);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }

        iovec->n = llu;

        log_printf(15, "read_read: iovec->n=%d\n", iovec->n);

        iovec->total_len = 0;
        iovec->transfer_total = 0;
        for (i = 0; i < iovec->n; i++) {
            //** Get the offset
            ll = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%lld", &ll);
            iovec->vec[i].off = ll;
            if (ll < 0) {
                log_printf(10, "read_read:  Invalid offset[%d] (%llu)!\n", i, ll);
                send_cmd_result(task, IBP_E_INV_PAR_SIZE);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC offset[%d]=%llu", cmd->name, i, ll);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }
            //** Get the length
            llu = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
            iovec->vec[i].len = llu;
            if (llu < 1) {
                log_printf(10, "read_read:  Invalid length[%d] (%llu)!\n", i, llu);
                send_cmd_result(task, IBP_E_INV_PAR_SIZE);
                tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Invalid IOVEC len[%d]=%llu", cmd->name, i, ll);
                tbx_monitor_obj_destroy(&(task->mo));
                return (-1);
            }

            iovec->total_len = iovec->total_len + llu;
            iovec->vec[i].cumulative_len = iovec->total_len;
            log_printf(15, "read_read: iovec->vec[%d] off=" OT " len=" OT "\n", i,
                       iovec->vec[i].off, iovec->vec[i].len);
        }
    } else {
        iovec->n = 1;

        //** Get the offset
        ll = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%lld", &ll);
        r->iovec.vec[0].off = ll;
        if (cmd->command == IBP_PULL) {
            if (ll > -1) {      //** PULL allows the 1st cap to be append
                r->write_mode = 0;
            } else {
                r->iovec.vec[0].off = 0;
            }
        }
        //** Get the remote offset if needed
        if ((cmd->command == IBP_PUSH) || (cmd->command == IBP_PULL) ||
            (cmd->command == IBP_PUSH_CHKSUM) || (cmd->command == IBP_PULL_CHKSUM)) {
            ll = 0;
            sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%lld", &ll);
            r->remote_offset = 0;
            if ((cmd->command == IBP_PUSH) || (cmd->command == IBP_PUSH_CHKSUM)) {
                if (ll > -1) {
                    r->write_mode = 0;
                    r->remote_offset = ll;
                }
            } else if (ll > -1) {
                r->remote_offset = ll;
            }
        }
        //** Get the length
        llu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%llu", &llu);
        r->iovec.vec[0].len = llu;
        if (llu < 1) {
            log_printf(10, "read_read:  Invalid length (%llu)!\n", llu);
            send_cmd_result(task, IBP_E_INV_PAR_SIZE);
            return (-1);
        }

        iovec->vec[0].cumulative_len = iovec->vec[0].len;
        iovec->total_len = iovec->vec[0].len;
        iovec->transfer_total = 0;
        log_printf(15, "read_single iovec->n=%d off=" OT " len=" OT " cumulative_len=" OT "\n",
                   iovec->n, iovec->vec[0].off, iovec->vec[0].len, iovec->vec[0].cumulative_len);
    }

    //** Get the connections timeout
    get_command_timeout(task, bstate);

    if ((cmd->command == IBP_SEND) || (cmd->command == IBP_PHOEBUS_SEND) ||
        (cmd->command == IBP_PULL) || (cmd->command == IBP_PUSH) ||
        (cmd->command == IBP_SEND_CHKSUM) || (cmd->command == IBP_PHOEBUS_SEND_CHKSUM) ||
        (cmd->command == IBP_PULL_CHKSUM) || (cmd->command == IBP_PUSH_CHKSUM)) {
        //** Now get the remote timeouts
        lu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%lu", &lu);
        r->remote_sto = lu;
        log_printf(1, "read_read(SEND): remote_timeout value=%lu\n", lu);
        if (lu == 0) {
            log_printf(1, "read_read(SEND): Bad Remote server timeout value %lu\n", lu);
            task->cmd.state = CMD_STATE_FINISHED;
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: SEND Bad remote server timeout value", cmd->name);
            tbx_monitor_obj_destroy(&(task->mo));
            return (send_cmd_result(task, IBP_E_INVALID_PARAMETER));
        }

        lu = 0;
        sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%lu", &lu);
        r->remote_cto = lu;
        if (lu == 0) {
            log_printf(1, "read_read(SEND): Bad Remote client timeout value %lu\n", lu);
            task->cmd.state = CMD_STATE_FINISHED;
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: SEND Bad remote client timeout value", cmd->name);
            tbx_monitor_obj_destroy(&(task->mo));
            return (send_cmd_result(task, IBP_E_INVALID_PARAMETER));
        }
        log_printf(20, "read_read: remote_sto=" TT " remote_cto=" TT "\n", r->remote_sto,
                   r->remote_cto);
    }

    debug_printf(1, "read_read: Successfully parsed iovec->n=%d off[0]=" I64T " len[0]=" I64T "\n",
                 r->iovec.n, r->iovec.vec[0].off, r->iovec.vec[0].len);

    tbx_monitor_obj_create(&(task->mo), "[%s] id=" LU " n_iov=%d off[0]=" I64T "len[0]=" I64T, cmd->name, r->cap.id, r->iovec.n, r->iovec.vec[0].off, r->iovec.vec[0].len);

    return (0);
}

//*****************************************************************
//  read_rid_bulk_warm - Bulk RID warming operation
//
//  Command
//    version IBP_BULK_WARM RID duration n_caps key_1 typekey_1 ... key_n typekey_n timeout\n
//
//*****************************************************************

int read_rid_bulk_warm(ibp_task_t *task, char **bstate)
{
    int finished, i, d;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_rid_bulk_warm_t *arg = &(cmd->cargs.rid_bulk_warm);
    rid_t rid;
    char crid[128];
    cap_id_t *caps;

    finished = 0;

    debug_printf(1, "Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &finished), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    //** Get the new duration
    d = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
    if (d <= 0) {
        log_printf(1, "Bad duration: %d\n", d);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad duration=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    arg->new_duration = ibp_time_now() + d;

    //** Get the Number of caps
    d = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), "%d", &d);
    if ((d < 1) || (d > global_config->server.max_warm)) {
        log_printf(10, "Invalid n_caps (%d)!\n", d);
        send_cmd_result(task, IBP_E_INV_PAR_SIZE);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad n_caps=%d", cmd->name, d);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    arg->n_caps = d;

    //** Make the caps structure and cycle through loading them all
    tbx_type_malloc_clear(caps, cap_id_t, arg->n_caps);
    for (i=0; i<arg->n_caps; i++) {
        d = parse_key2(bstate, &caps[i], &rid, crid, sizeof(crid));
        if ((ibp_compare_rid(arg->rid, rid) != 0) || (d != 0)) {
            free(caps);
            log_printf(10, "Bad RID/master_cap!\n");
            send_cmd_result(task, IBP_E_INVALID_RID);
            tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID for cap index=%d", cmd->name, i);
            tbx_monitor_obj_destroy(&(task->mo));
            return (-1);
        }
    }
    arg->caps = caps;

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s n_cap=%d", cmd->name, arg->crid, arg->n_caps);

    get_command_timeout(task, bstate);
    return (0);
}


//*****************************************************************
//  read_internal_get_corrupt - Private command for getting the list
//    of corrupt allocations.
//
// PRIVATE command
//    version INTERNAL_GET_CORRUPT RID timeout\n
//
//*****************************************************************

int read_internal_get_corrupt(ibp_task_t *task, char **bstate)
{
    int finished;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

    finished = 0;

    debug_printf(1, "read_internal_get_alloc:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &finished), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_get_corrupt: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    debug_printf(10, "read_internal_get_corrupt: RID=%s\n", arg->crid);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s", cmd->name, arg->crid);

    get_command_timeout(task, bstate);
    return (0);
}

//*****************************************************************
//  read_internal_get_alloc - Private command for getting the raw
//    allocation.
//
// PRIVATE command
//    version INTERNAL_GET_ALLOC RID id print_blocks offset len timeout\n
//
// print_blocks = 0 no block chksum information is transferred
// offset   = -1 No allocation data is transferred. Otherwsie alloc offset
// len      = 0  All available data is returned otherwise len bytes are returned if available
//*****************************************************************

int read_internal_get_alloc(ibp_task_t *task, char **bstate)
{
    int finished, i;
    char *str;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_get_alloc_t *arg = &(cmd->cargs.get_alloc);

    finished = 0;

    debug_printf(1, "read_internal_get_alloc:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &finished), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_get_alloc: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //*** Get the id ***
    str = tbx_stk_string_token(NULL, " ", bstate, &finished);
    if (sscanf(str, LU, &(arg->id)) != 1) {
        log_printf(10, "ERROR parsing ID!\n");
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad ID");
        tbx_monitor_obj_destroy(&(task->mo));
        return(-1);
    }

    //** Determine if we print the block information
    str = tbx_stk_string_token(NULL, " ", bstate, &finished);
    sscanf(str, "%d", &(arg->print_blocks));

    //** Get the offset **
    str = tbx_stk_string_token(NULL, " ", bstate, &finished);
    sscanf(str, "%d", &i);
    arg->offset = i;
    if (i != -1)
        sscanf(str, LU, &(arg->offset));

    //** and the length
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &finished), LU, &(arg->len));

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s id=" LU " offset=" LU " len=" LU, cmd->name, arg->crid,arg->id, arg->offset, arg->len);

    debug_printf(10, "read_internal_get_alloc: RID=%s\n", arg->crid);

    get_command_timeout(task, bstate);
    return (0);
}

//*****************************************************************
// read_internal_get_config - Reads the internal get config command
//
//    timeout\n
//
//*****************************************************************

int read_internal_get_config(ibp_task_t *task, char **bstate)
{
    debug_printf(1, "read_internal_get_config:  Starting to process buffer\n");

    tbx_monitor_obj_create(&(task->mo), "[%s]", task->cmd.name);

    get_command_timeout(task, bstate);

    debug_printf(1, "read_internal_get_config: Successfully parsed allocate command\n");
    return (0);
}

//*****************************************************************
// read_internal_date_free - Reads the internal date_free command
//
//    RID size timeout timeout\n
//
//*****************************************************************

int read_internal_date_free(ibp_task_t *task, char **bstate)
{
    int d, fin;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_date_free_t *arg = &(cmd->cargs.date_free);

    fin = 0;

    debug_printf(1, "read_internal_date_free:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_date_free: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //*** Get the size ***
    arg->size = 0;
    d = sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), LU, &(arg->size));
    if (d == 0) {
        log_printf(1, "read_internal_date_free: Bad size: " LU "\n", arg->size);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad size=" LU, cmd->name, arg->size);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    get_command_timeout(task, bstate);

    debug_printf(1, "read_internal_date_free: Successfully parsed allocate command\n");

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s size=" LU, cmd->name, arg->crid, arg->size);

    return (0);
}

//*****************************************************************
// read_internal_expire_list - Reads the internal expire_list command
//
//    RID mode time(sec) count timeout\n
//
//   mode - 0=relative time, 1=absolute time
//
//*****************************************************************

int read_internal_expire_list(ibp_task_t *task, char **bstate)
{
    int d, fin;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_expire_log_t *arg = &(cmd->cargs.expire_log);

    fin = 0;

    debug_printf(1, "read_internal_expire_list:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_expire_list: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    //*** Get the mode ***
    arg->mode = 0;
    d = sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &(arg->mode));
    if (d == 0) {
        log_printf(1, "read_internal_expire_list: Bad mode: %d\n", arg->mode);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad mode=%d RID=%s", cmd->name, arg->mode, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //*** Get the time ***
    arg->start_time = 0;
    d = sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), TT, &(arg->start_time));
    if (d == 0) {
        log_printf(1, "read_internal_expire_list: Bad time: " TT "\n", arg->start_time);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad time=" TT " RID=%s", cmd->name, arg->start_time, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    if ((arg->mode == 0) && (arg->start_time != 0))
        arg->start_time = arg->start_time + ibp_time_now();

    //*** Get the record count ***
    arg->max_rec = 0;
    d = sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &(arg->max_rec));
    if (d == 0) {
        log_printf(1, "read_internal_expire_list: Bad record count: %d\n", arg->max_rec);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad record count=%d RID=%s", cmd->name, arg->max_rec, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    arg->direction = DBR_NEXT;

    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s start_time=" TT " max_rec=%d", cmd->name, arg->crid, arg->start_time, arg->max_rec);

    debug_printf(1, "read_internal_expire_list: Successfully parsed allocate command\n");
    return (0);
}

//*****************************************************************
// read_internal_undelete - Reads the internal undelete command
//
//    RID trash_type trash_id duration(sec) timeout\n
//
//*****************************************************************

int read_internal_undelete(ibp_task_t *task, char **bstate)
{
    int fin;
    int64_t duration;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_undelete_t *arg = &(cmd->cargs.undelete);

    fin = 0;

    debug_printf(1, "read_internal_undelete:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_expire_list: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //*** Get the trash_type ***
    arg->trash_type = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &(arg->trash_type));
    if ((arg->trash_type != RES_DELETE_INDEX) && (arg->trash_type != RES_EXPIRE_INDEX)) {
        log_printf(1, "read_internal_undelete: Bad Trash_type: %d\n", arg->trash_type);
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad trash_type=%d RID=%s", cmd->name, arg->trash_type, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    strncpy(arg->trash_id, tbx_stk_string_token(NULL, " ", bstate, &fin),
            sizeof(arg->trash_id) - 1);
    arg->trash_id[sizeof(arg->trash_id) - 1] = '\0';

    //*** Get the duration ***
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), LU, &duration);
    arg->duration = duration;
    if (duration == 0) {
        log_printf(1, "read_internal_undelete: Bad duration\n");
        send_cmd_result(task, IBP_E_BAD_FORMAT);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad duration=" LU " RID=%s", cmd->name, duration, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s trash_type=%d duration=" LU, cmd->name, arg->crid, arg->trash_type, duration);

    debug_printf(1, "read_internal_undelete: Successfully parsed undelete command\n");
    return (0);
}


//*****************************************************************
// read_internal_rescan - Reads the internal rescan command
//
//    RID timeout\n
//
//  NOTE:  If RID=0 then all resources are rescanned.
//*****************************************************************

int read_internal_rescan(ibp_task_t *task, char **bstate)
{
    int fin;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_rescan_t *arg = &(cmd->cargs.rescan);

    fin = 0;

    debug_printf(1, "read_internal_rescan:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_expire_list: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }

    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s", cmd->name, arg->crid);

    debug_printf(1, "read_internal_rescan: Successfully parsed rescan command\n");
    return (0);
}

//*****************************************************************
// read_internal_set_mode - Reads the internal set mode command
//
//    RID rwm_mode timeout\n
//
//*****************************************************************

int read_internal_set_mode(ibp_task_t *task, char **bstate)
{
    int fin, opt;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_mode_t *arg = &(cmd->cargs.mode);

    fin = 0;

    debug_printf(1, "read_internal_set_mode:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_set_mode: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Make sure it's not RID 0
    if (ibp_rid_is_empty(arg->rid) == 1) {
        log_printf(1, "read_internal_set_mode: Can't use RID 0!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s. Can't use 0!", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** and that it's mounted
    if (resource_lookup(global_config->rl, arg->crid) == NULL) {
        log_printf(10, "read_internal_set_mode: Invalid RID :%s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s. RID is unmounted!", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (0);
    }
    //** Get the new_mode
    opt = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &opt);
    if ((opt < 0) && (opt > 2)) {
        log_printf(1, "read_internal_set_mode: Invalid force_rebuild=%d\n", opt);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad force_rebuild=%d RID=%s", cmd->name, opt, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    arg->mode = opt;

    get_command_timeout(task, bstate);

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s mode=%d", cmd->name, arg->crid, opt);

    debug_printf(1, "read_internal_set_mode: Successfully parsed rescan command\n");
    return (0);
}

//*****************************************************************
// read_internal_mount - Reads the internal mount command
//
//    RID force_rebuild merge_snap|- timeout\n
//
//*****************************************************************

int read_internal_mount(ibp_task_t *task, char **bstate)
{
    int fin, opt;
    char *snap;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_mount_t *arg = &(cmd->cargs.mount);

    fin = 0;

    debug_printf(1, "read_internal_mount:  Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "read_internal_mount: Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Make sure it's not RID 0
    if (ibp_rid_is_empty(arg->rid) == 1) {
        log_printf(1, "read_internal_mount: Can't use RID 0!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s. Can't use 0!", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Checking if it's already mounted is done by the handle_mountroutines

    //** Get the force_rebuild flag
    opt = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &opt);
    if ((opt < 0) && (opt > 2)) {
        log_printf(1, "read_internal_mount: Invalid force_rebuild=%d\n", opt);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad force_rebuild=%d RID=%s", cmd->name, opt, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    arg->force_rebuild = opt;

    //** See if we use an existing snap for the merge
    snap = tbx_stk_string_token(NULL, " ", bstate, &fin);
    if (strcmp(snap, "-") == 0) {
        arg->merge_snap = NULL;
    } else {
        arg->merge_snap = strdup(snap);
    }

    get_command_timeout(task, bstate);

    //** Get the optional message
    opt = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &opt);
    arg->msg[0] = 0;
    if ((opt > 0) && (opt < (int) sizeof(arg->msg))) {
        strncpy(arg->msg, (*bstate), opt);
        arg->msg[opt] = 0;
        log_printf(5, "msg=!%s!\n", arg->msg);
    }
    debug_printf(1, "read_internal_mount: Successfully parsed rescan command\n");

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s force_rebuild=%d RID=%s", cmd->name, arg->crid, opt);

    return (0);
}


//*****************************************************************
// read_internal_umount - Reads the internal umount command
//
//    RID delay timeout\n
//
//*****************************************************************

int read_internal_umount(ibp_task_t *task, char **bstate)
{
    int fin, opt;
    Cmd_state_t *cmd = &(task->cmd);
    Cmd_internal_mount_t *arg = &(cmd->cargs.mount);

    fin = 0;

    debug_printf(1, "Starting to process buffer\n");

    //** Get the RID
    arg->crid[sizeof(arg->crid) - 1] = '\0';
    strncpy(arg->crid, tbx_stk_string_token(NULL, " ", bstate, &fin), sizeof(arg->crid) - 1);
    if (ibp_str2rid(arg->crid, &(arg->rid)) != 0) {
        log_printf(1, "Bad RID: %s\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Make sure it's not RID 0
    if (ibp_rid_is_empty(arg->rid) == 1) {
        log_printf(1, "Can't use RID 0!\n");
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s. Can't use 0!", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** and that it's mounted
    if (resource_lookup(global_config->rl, arg->crid) == NULL) {
        log_printf(10, "RID :%s NOT mounted\n", arg->crid);
        send_cmd_result(task, IBP_E_INVALID_RID);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad RID=%s. RID is unmounted!", cmd->name, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    //** Now get the delay between removing the RID from the list and performing the umount
    opt = -1;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &opt);
    if (opt < 1) {
        log_printf(1, "Invalid delay=%d\n", opt);
        send_cmd_result(task, IBP_E_INVALID_PARAMETER);
        tbx_monitor_obj_create(&(task->mo), "[%s] PARSE_ERROR: Bad time delay=%d. RID=%s", cmd->name, opt, arg->crid);
        tbx_monitor_obj_destroy(&(task->mo));
        return (-1);
    }
    arg->delay = opt;

    get_command_timeout(task, bstate);

    //** Get the optional message
    opt = 0;
    sscanf(tbx_stk_string_token(NULL, " ", bstate, &fin), "%d", &opt);
    arg->msg[0] = 0;
    if ((opt > 0) && (opt < (int) sizeof(arg->msg))) {
        strncpy(arg->msg, (*bstate), opt);
        arg->msg[opt] = 0;
        log_printf(5, "msg=!%s!\n", arg->msg);
    }

    tbx_monitor_obj_create(&(task->mo), "[%s] RID=%s delay=%d", cmd->name, arg->crid, opt);

    debug_printf(1, "Successfully parsed rescan command\n");
    return (0);
}
