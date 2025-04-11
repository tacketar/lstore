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

#define _log_module_index 189

#include <gop/gop.h>
#include <gop/mq.h>
#include <gop/opque.h>
#include <gop/tp.h>
#include <gop/types.h>
#include <lio/segment.h>
#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "authn.h"
#include "blacklist.h"
#include "cache.h"
#include "ex3.h"
#include "ex3/types.h"
#include "lio.h"
#include "remote_config.h"
#include "os.h"
#include "os/file.h"


//***********************************************************************
// Misc Core LIO functionality
//***********************************************************************

//***********************************************************************
// _get_shortcut_user - Helper on finding the default user for the shortcut
//
//    [default]
//    <shortcut> = <user>
//***********************************************************************

char *_get_shortcut_user(const char *fname, const char *shortcut)
{
    tbx_inip_file_t *ifd;
    struct stat sbuf;
    char *user;

    if (stat(fname, &sbuf) != 0) return(NULL);  //** File is missing

    ifd = tbx_inip_file_read(fname, 1);
    if (ifd == NULL) return(NULL);
    user = tbx_inip_get_string(ifd, "default", shortcut, NULL);
    tbx_inip_destroy(ifd);

    return(user);
}

//***********************************************************************
// lio_get_shortcut_user - Looks up the default user for the shortcut
//      ~/.lio/accounts.psk
//      /etc/lio/default.psk
//
//    label - Shortcut to look up
//***********************************************************************

char *lio_get_shortcut_user(char *label)
{
    char fname[4096];
    char *home, *user, *kname;

    user = NULL;

    //** 1st check if they've provided an env prefix
    kname = getenv(LIO_ENV_KEY_FILE);
    if (kname) { user = _get_shortcut_user(kname, label); }
    if (user) return(user);

    //** Nexzt check the user local file
    home = getenv("HOME");
    if (home) {
        snprintf(fname, sizeof(fname), "%s/.lio/accounts.psk", home);
        user = _get_shortcut_user(fname, label);
    }
    if (user) return(user);

    //** No luck so look in the global location
    snprintf(fname, sizeof(fname), "%s", "/etc/lio/known_hosts");
    user = _get_shortcut_user(fname, label);

    return(user);
}

//***********************************************************************
// lio_get_shortcut - Looks up the shortcut from the standard file locations:
//      ~/.lio/known_hosts
//      /etc/lio/known_hosts
//
//    label - Shortcut to look up
//    hints - If non-NULL then any shortcut hints are returned
//***********************************************************************

char *lio_get_shortcut(char *label, char **hints_string)
{
    char fname[4096];
    char fname_home[4096];
    char *home, *shortcut;
    tbx_inip_file_t *ifd;
    struct stat sbuf;

    //** 1st check the user local known_hosts
    home = getenv("HOME");
    if (home == NULL) goto next;
    snprintf(fname_home, sizeof(fname_home), "%s/.lio/known_hosts", home);
    if (stat(fname_home, &sbuf) != 0) goto next;  //** File is missing
    ifd = tbx_inip_file_read(fname_home, 1);
    if (ifd == NULL) goto next;
    shortcut = tbx_inip_get_string(ifd, "shortcuts", label, NULL);
    if (shortcut && hints_string) *hints_string = tbx_inip_get_string(ifd, "hints", label, NULL);

    tbx_inip_destroy(ifd);
    if (shortcut) return(shortcut);

next:
    //** No luck so look in the global location
    snprintf(fname, sizeof(fname), "%s", "/etc/lio/known_hosts");
    if (stat(fname, &sbuf) != 0) goto oops;  //** File is missing
    ifd = tbx_inip_file_read(fname, 1);
    if (ifd == NULL) goto oops;
    shortcut = tbx_inip_get_string(ifd, "shortcuts", label, NULL);
    if (shortcut && hints_string) *hints_string = tbx_inip_get_string(ifd, "hints", label, NULL);
    tbx_inip_destroy(ifd);

    if (shortcut == NULL) {
        fprintf(stderr, "ERROR: Missing shortcut: %s\n", label);
    }
    return(shortcut);

oops:
    fprintf(stderr, "ERROR: Unable to load a known hosts file: %s  or  %s\n", fname_home, fname);
    return(NULL);
}

//***********************************************************************
// scan_special - Returns the 1st occurence of the special character
//***********************************************************************

char *scan_special(char special, char *string, char escape)
{
    int i;

    for (i=0; string[i] != 0; i++) {
        if (string[i] == special) {
            if (i>0) {
                if (string[i-1] == escape) continue;
            }
            return(string + i);
        }
    }

    return(NULL);
}

//***********************************************************************
// is_special - returns 1 if the character matches and hasn't been escaped
//***********************************************************************

int is_special(char special, char *string, int offset, char escape)
{
    if (string[offset] == special) {
        if (offset > 0) {
            if (string[offset-1] == escape) return(0);
        }

        return(1);
    }

    return(0);
}

//***********************************************************************
//  lio_parse_path - Parses a path of the form:
//
//          [lstore://][user@][MQ_NAME|]HOST:[port:]cfg:section:[/fname]
//          [lstore://]user@[MQ_NAME|]HOST:[port:]cfg:[/fname]
//          [lstore://]user@[MQ_NAME|]HOST[:port][:/fname]
//          [lstore://]@:/fname
//          [lstore://][user]@@shortcut[:/fname]
//
//  startpath is required.  All other parameters are optional.  If NULL the
//     parameter is not returned.  If the parameter already has a value
//     stored, ie it's non-NULL, then the current value is freed (except for port)
//     and overwritten with the new value if it is supplied.  The value should
//     be freed by the calling program.
//
//  Returns:
//     1 if lstore:// and/or @: were encountered signifying it's an actual LStore path
//     0 if lstore:// and @: are missing so it could be a local LFS mount OR an LStore path
//    -1 if the path can't be parsed.  Usually :@ or some perm
//***********************************************************************

int lio_parse_path(char *basepath, char **user, char **mq_name, char **host, int *port, char **cfg, char **section, char **hints_string, char **path, int path_is_literal)
{
    int i, j, k, found, found2, n, ptype, uri, m, s, uri_type, got_at, got_mq;
    char *dummy, *shortcut, *c1, *c2;
    char label[2048];
    char *startpath;

    startpath = basepath;
    shortcut = NULL;
    if (hints_string) *hints_string = NULL;

try_again:
    n = strlen(startpath);
    uri_type = 0;
    got_at = 0;
    got_mq = 0;

    if (strncasecmp(startpath, "file://", 7) == 0) { //** Straight file
        k = 7;
        ptype = 0;
        uri_type = 1;
        goto handle_path;
    }

    //** Check for the "lstore://" prefix and skipp if there
    if (strncasecmp(startpath, "lstore://", 9) == 0) {
        k = 9;
        uri = k;   //** Note we have an official URI prefix
        uri_type = 2;
        ptype = 1;
    } else {
        k = 0;
        uri = 0;
        ptype = 0;
    }

    if (k<n) {
        if (is_special('/', startpath, k, '\\')) goto handle_path;  //** Just a path
    }

    if (uri_type == 0) {  //** Unknown URI which means any @ has to come before a :
        c1 = scan_special('@', startpath + k, '\\');
        if (c1) {
            c2 = scan_special(':', startpath + k, '\\');
            if (c2) {
                if (c1 > c2) {
                    ptype = -1;
                    goto kick_out;
                }
            }
        }
    }
    //** check for the '@'
    found = -1;
    for (i=k; i<n; i++) {
        if (is_special('@', startpath, i, '\\')) {
            found = i+1;
            ptype = 1;
            got_at = 1;

            if ((i>k) && (user)) {  //** Got a valid user
                if (*user == NULL) {  //** Only fetch the user if we don't already have one
                    *user = tbx_stk_unescape_strndup('\\', startpath+k, i-k);
                }
            }
            if (is_special('@', startpath, found, '\\')) { //** See if we got a shortcut
                found++;
                label[0] = '\0';
                for (j=found; j<n; j++) {
                    if (is_special(':', startpath, j, '\\')) break;
                }

                memcpy(label, startpath + found, j-found);
                label[j-found] = '\0';
                if (is_special(':', startpath, j, '\\')) j++;

                if (hints_string) {
                    if (*hints_string) free(*hints_string);
                    *hints_string = NULL;
                }
                shortcut = lio_get_shortcut(label, hints_string);
                if (shortcut) {
                    m = n - j;
                    s = m + strlen(shortcut)+2;
                    tbx_type_malloc(dummy, char, s);
                    snprintf(dummy, s, "%s:%s", shortcut, startpath + j);
                    free(shortcut);
                    if (startpath != basepath) free(startpath);
                    startpath = dummy;

                    if (*user == NULL) {   //** If we still don't have a user see if we can find a default one from the accounts files
                        *user = lio_get_shortcut_user(label);
                    }
                    goto try_again;
                }
            }
            break;
        } else if ((is_special('|', startpath, i, '\\')) || (is_special(':', startpath, i, '\\'))) { //** Got an MQ name
            if (found == -1) {
                found = uri;
                ptype = 1;
                break;
            }
        }
    }

    if ((found == -1) && (uri == 0)) {
        if (is_special('/', startpath, k, '\\')) { //** Didn't find anything else to to parse
            if (path) {
                if (*path) free(*path);
                *path = strdup(startpath + k);
            }
            goto kick_out;
        } else { //** Just have a host
            ptype = 1;
        }
    }

    //** Look for an ':' and process an '|' if encountered
    k = (found == -1) ? uri : found;
    found = -1;
    found2 = 0;
    for (i=k; i<n; i++) {
        if (is_special(':', startpath, i, '\\')) {
            found = i;
            break;
        } else if (is_special('|', startpath, i, '\\')) {  //** Got an MQ name
            got_mq = 1;
            if ((i>k) && (mq_name)) {
                if (*mq_name) free(*mq_name);
                *mq_name = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
            }
            k = i+1;  //** Move the starting forward to skip the MQ name
            found2 = 1;
        }
    }

    if (found == -1) {  //**No path.  Just a host
        if (k < n) {
            if ((uri_type == 2) || got_at || got_mq) {
                if (host) {
                    if (*host) free(*host);
                    *host = tbx_stk_unescape_strndup('\\', startpath+k, -1);
                }
            } else if (uri_type == 0) {
                ptype = 0;
                goto handle_path;
            }
        }
        goto kick_out;
    } else {
        if (k<n) {
            if ((i>k) && (host)) {
                if (*host) free(*host);
                *host = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
            }
            k = i+1;  //** Move the starting forward to skip the MQ name
        } else if (found2) { //** We have an MQ name without a host so flag an error
            ptype = -1;
            goto kick_out;
        }
    }

    if (is_special('/', startpath, k, '\\')) goto handle_path;

    //** Now check if we have a port
    found = 0;
    for (i=k; i<n; i++) {
        if (startpath[i] == ':') {
            if (i>k) {
                dummy = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
                found2 = atoi(dummy);
                free(dummy);
                if (found2 > 0) {  //** Got a valid port
                    k = i+1;
                    found = 1;
                    if (port) *port = found2;
                }
            }
            break;
        }
    }

    if (!found) { //** See if we have a port or a path for the end
        if (is_special('/', startpath, k, '\\')) goto handle_path;
        found2 = atoi(startpath+k);
        if (found2 > 0) {
           if (port) *port = found2;
           goto kick_out;
        }
    }

    if (is_special('/', startpath, k, '\\')) goto handle_path;

    //** check if we have a config name
    found = 0;
    for (i=k; i<n; i++) {
        if (is_special(':', startpath, i, '\\')) {
            if ((i>k) && (cfg)) {
                if (*cfg) free(*cfg);
                *cfg = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
            }
            found = 1;
            k = i+1;
            break;
        }
    }

    if (is_special('/', startpath, k, '\\')) goto handle_path;

    if (!found) { //** At the end and we don't have a path so it's a cfg
        if ((i>k) && (cfg)) {
            if (*cfg) free(*cfg);
            *cfg = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
            if (strcmp(*cfg, "@") == 0) {  //** Malformed path
                free(*cfg);
                *cfg = NULL;
                ptype = -1;
            }
        }
        goto kick_out;
    }

    //** check if we have a section
    for (i=k; i<n; i++) {
        if (is_special(':', startpath, i, '\\')) {
            if ((i>k) && (section)) {
                if (*section) free(*section);
                *section = tbx_stk_unescape_strndup('\\', startpath + k, i-k);
            }
            k = i+1;
            break;
        }
    }

handle_path:

    //** Anythng else is the path
    if ((k<n) && (path)) {
        if (*path) free(*path);
        *path = (path_is_literal == 0) ? tbx_stk_unescape_strndup('\\', startpath + k, -1) : strdup(startpath + k);
    }

kick_out:
    if (startpath != basepath) free(startpath);
    return(ptype);
}

//***********************************************************************
// lio_fetch_config - Returns a valid FD if the config can be opened.
//     config_name - Path to the config
//     obj_name - Object name returned if non-NULL
//     ts       - Time stamp for the config.  If the config hasn't changed
//                the FD is null. On success ts set to the time of the object
//                returned.  If the object doesn't exist the time is set to 0.
//                On entry if ts=0 the object will always be loaded.  Otherwise
//                it will be loaded if the object is newer.
//***********************************************************************

tbx_inip_file_t *lio_fetch_config(gop_mq_context_t *mqc, lio_creds_t *creds, const char *config_name, char **obj_name, time_t *ts)
{
    const char *local;
    char *hints_string;
    char *cfg;
    int offset;
    struct stat st;
    tbx_inip_file_t *ifd = NULL;

    if ((strncmp("lstore://", config_name, 9) == 0) || (strstr(config_name, "@@") != NULL)) {
        if (rc_client_get_config(mqc, creds, (char *)config_name, NULL, &cfg, &hints_string, obj_name, NULL, ts) == 0) {
            if (cfg) {
                ifd = tbx_inip_string_read_with_hints_string(cfg, hints_string, 1);
                if (ifd) tbx_inip_string_auto_destroy(ifd);
                if (hints_string) free(hints_string);
                return(ifd);
            }
            return(NULL);
        }
        return(NULL);
    }

    //** If we make it here we should be dealing with a local file
    offset = 0;
    if (strncmp("file://", config_name, 7) == 0) {
        offset = 7;
    } else if (strncmp("ini://", config_name, 6) == 0) {
        offset = 6;
    }

    //** Get the time stamp
    local = config_name + offset;
    log_printf(20, "config_name=%s local_name=%s\n", config_name, local);
    if (stat(local, &st) != 0) {
        log_printf(1, "Local file missing! Using old definition. fname=%s\n", local);
        *ts = 0;
    } else if (*ts != st.st_mtime) {  //** File changed so reload it
        *ts = st.st_mtime;
        ifd = tbx_inip_file_read(local, 1);
    }

    return(ifd);
}
//***********************************************************************

int strcmp_null(const char *s1, const char *s2)
{
    if ((s1 == NULL) && (s2 == NULL)) return(0);
    if ((s1 == NULL) && (s2 != NULL)) return(-2);
    if ((s1 != NULL) && (s2 == NULL)) return(-2);
    return(strcmp(s1,s2));
}

//***********************************************************************
//  parse_check - Does the error checking and printing
//***********************************************************************

int parse_path_check(char *uri, char *user, char *mq_name, char *host, int port, char *cfg, char *section, char *path, int path_is_literal, int err)
{
    int err2, port2, ret;
    char *user2, *mq_name2, *host2, *cfg2, *section2, *path2;

    port2 = -1;
    user2 = mq_name2 = host2 = cfg2 = section2 = path2 = NULL;
    err2 = lio_parse_path(uri, &user2, &mq_name2, &host2, &port2, &cfg2, &section2, NULL, &path2, path_is_literal);

    printf("uri=%s err=%d\n", uri, err2);

    ret = 0;
    if (err != err2) {ret=1; printf("    ERROR: err=%d  err2=%d\n", err, err2); }
    if (strcmp_null(user, user2) != 0) {ret=1; printf("    ERROR: user=%s  user2=%s\n", user, user2); }
    if (strcmp_null(mq_name, mq_name2) != 0) {ret=1; printf("    ERROR: mq_name=%s  mq_name2=%s\n", mq_name, mq_name2); }
    if (strcmp_null(host, host2) != 0) {ret=1; printf("    ERROR: host=%s  host2=%s\n", host, host2); }
    if (port != port2) {ret=1; printf("    ERROR: port=%d  port2=%d\n", port, port2); }
    if (strcmp_null(cfg, cfg2) != 0) {ret=1; printf("    ERROR: cfg=%s  cfg2=%s\n", cfg, cfg2); }
    if (strcmp_null(section, section2) != 0) {ret=1; printf("    ERROR: section=%s  section2=%s\n", section, section2); }
    if (strcmp_null(path, path2) != 0) {ret=1; printf("    ERROR: path=%s  path2=%s\n", path, path2); }

    if (user2) free(user2);
    if (mq_name2) free(mq_name2);
    if (host2) free(host2);
    if (cfg2) free(cfg2);
    if (section2) free(section2);
    if (path2) free(path2);

    return(ret);
}

//***********************************************************************
// lio_parse_path_test - Sanity checks all the permutations
//      for the LStore URI
//***********************************************************************

int lio_parse_path_test()
{
    int err;

    err = 0;
    err += parse_path_check("lstore://user@MQ|host.vampire:1234:cfg:section:/my/path", "user", "MQ", "host.vampire", 1234, "cfg", "section", "/my/path", 0, 1);
    err += parse_path_check("user@MQ|host.vampire:1234:cfg:section:/my/path", "user", "MQ", "host.vampire", 1234, "cfg", "section", "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire:1234:cfg:section:/my/path", "user", NULL, "host.vampire", 1234, "cfg", "section", "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire:cfg:section:/my/path", "user", NULL, "host.vampire", -1, "cfg", "section", "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire:/my/path", "user", NULL, "host.vampire", -1, NULL, NULL, "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire:1234", "user", NULL, "host.vampire", 1234, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("user@host.vampire:cfg:", "user", NULL, "host.vampire", -1, "cfg", NULL, NULL, 0, 1);
    err += parse_path_check("user@host.vampire:cfg:section:", "user", NULL, "host.vampire", -1, "cfg", "section", NULL, 0, 1);
    err += parse_path_check("user@host.vampire::section:", "user", NULL, "host.vampire", -1, NULL, "section", NULL, 0, 1);
    err += parse_path_check("user@host.vampire:::", "user", NULL, "host.vampire", -1, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("user@host.vampire:::/my/path", "user", NULL, "host.vampire", -1, NULL, NULL, "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire:1234:/my/path", "user", NULL, "host.vampire", 1234, NULL, NULL, "/my/path", 0, 1);
    err += parse_path_check("user@host.vampire", "user", NULL, "host.vampire", -1, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("MQ|host.vampire:cfg", NULL, "MQ", "host.vampire", -1, "cfg", NULL, NULL, 0, 1);
    err += parse_path_check("MQ|host.vampire", NULL, "MQ", "host.vampire", -1, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("lstore://host.vampire", NULL, NULL, "host.vampire", -1, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("lstore://host.vampire:cfg:/my/path", NULL, NULL, "host.vampire", -1, "cfg", NULL, "/my/path", 0, 1);
    err += parse_path_check("lstore://host.vampire:/my/path", NULL, NULL, "host.vampire", -1, NULL, NULL, "/my/path", 0, 1);
    err += parse_path_check("lstore://host.vampire:/", NULL, NULL, "host.vampire", -1, NULL, NULL, "/", 0, 1);
    err += parse_path_check("lstore://host.vampire", NULL, NULL, "host.vampire", -1, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("lstore://host.vampire:cfg", NULL, NULL, "host.vampire", -1, "cfg", NULL, NULL, 0, 1);
    err += parse_path_check("lstore://host.vampire:1234", NULL, NULL, "host.vampire", 1234, NULL, NULL, NULL, 0, 1);
    err += parse_path_check("lstore://host.vampire:1234:cfg", NULL, NULL, "host.vampire", 1234, "cfg", NULL, NULL, 0, 1);

//skip:
    err += parse_path_check("@:/", NULL, NULL, NULL, -1, NULL, NULL, "/", 0, 1);
    err += parse_path_check(":@", NULL, NULL, NULL, -1, NULL, NULL, NULL, 0, -1);
    err += parse_path_check("@:/just/a/path", NULL, NULL, NULL, -1, NULL, NULL, "/just/a/path", 0, 1);
    err += parse_path_check("\\@\\:/just/a/path", NULL, NULL, NULL, -1, NULL, NULL, "@:/just/a/path", 0, 0);
    err += parse_path_check("/just/a/path", NULL, NULL, NULL, -1, NULL, NULL, "/just/a/path", 0, 0);
    err += parse_path_check("a/relative/path", NULL, NULL, NULL, -1, NULL, NULL, "a/relative/path", 0, 0);
    err += parse_path_check("relative\\@\\:/path", NULL, NULL, NULL, -1, NULL, NULL, "relative@:/path", 0, 0);
    err += parse_path_check("relative_with_\\\\*_path", NULL, NULL, NULL, -1, NULL, NULL, "relative_with_\\*_path", 0, 0);

    printf("--------------\nerror count=%d\n", err);
    return(err);
}

//***********************************************************************
// lio_set_timestamp - Sets the timestamp val/size for a attr put
//***********************************************************************

void lio_set_timestamp(char *id, char **val, int *v_size)
{
    *val = id;
    *v_size = (id == NULL) ? 0 : strlen(id);
    return;
}

//***********************************************************************
// lio_get_timestamp - Splits the timestamp ts/id field
//***********************************************************************

void lio_get_timestamp(char *val, int *timestamp, char **id)
{
    char *bstate;
    int fin;

    *timestamp = 0;
    sscanf(tbx_stk_string_token(val, "|", &bstate, &fin), "%d", timestamp);
    if (id != NULL) *id = tbx_stk_string_token(NULL, "|", &bstate, &fin);
    return;
}

//-------------------------------------------------------------------------
//------- Universal Object Iterators
//-------------------------------------------------------------------------

//*************************************************************************
//  lio_unified_object_iter_create - Create an ls object iterator
//*************************************************************************

lio_unified_object_iter_t *lio_unified_object_iter_create(lio_path_tuple_t tuple, lio_os_regex_table_t *path_regex, lio_os_regex_table_t *obj_regex, int obj_types, int rd)
{
    lio_unified_object_iter_t *it;

    tbx_type_malloc_clear(it, lio_unified_object_iter_t, 1);

    it->tuple = tuple;
    if (tuple.is_lio == 1) {
        it->oit = os_create_object_iter(tuple.lc->os, tuple.creds, path_regex, obj_regex, obj_types, NULL, rd, NULL, 0);
    } else {
        it->lit = create_local_object_iter(path_regex, obj_regex, obj_types, rd);
    }

    return(it);
}

//*************************************************************************
//  lio_unified_object_iter_destroy - Destroys an ls object iterator
//*************************************************************************

void lio_unified_object_iter_destroy(lio_unified_object_iter_t *it)
{

    if (it->tuple.is_lio == 1) {
        os_destroy_object_iter(it->tuple.lc->os, it->oit);
    } else {
        destroy_local_object_iter(it->lit);
    }

    free(it);
}

//*************************************************************************
//  lio_unified_next_object - Returns the next object to work on
//*************************************************************************

int lio_unified_next_object(lio_unified_object_iter_t *it, char **fname, int *prefix_len)
{
    int err = 0;

    if (it->tuple.is_lio == 1) {
        err = os_next_object(it->tuple.lc->os, it->oit, fname, prefix_len);
    } else {
        err = local_next_object(it->lit, fname, prefix_len);
    }

    log_printf(15, "ftype=%d\n", err);
    return(err);
}
