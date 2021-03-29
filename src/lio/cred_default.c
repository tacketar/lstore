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
// Default cred setup
//***********************************************************************

#define _log_module_index 185

#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/type_malloc.h>

#include "authn.h"

extern char *_lio_exe_name;  //** This is set by lio_init long before we would ever be called.

//***********************************************************************

char *cdef_get_type(lio_creds_t *c)
{
    return("DEFAULT");
}

//***********************************************************************

//***********************************************************************

char *cdef_get_id(lio_creds_t *c, int *len)
{
    if (len) *len = c->id_len;
    return(c->id);
}

//***********************************************************************

char *cdef_get_descriptive_id(lio_creds_t *c, int *len)
{
    if (len) *len = c->descriptive_id_len;
    return(c->descriptive_id);
}

//***********************************************************************

void *cdef_get_handle(lio_creds_t *c, int *len)
{
    if (len) *len = c->handle_len;
    return(c->handle);
}

//***********************************************************************
// _set_id - Sets the user ID and also makes the shared handle.
//  In this case the shared handle is really just string with the format
//     id:pid:userid@hostname
//***********************************************************************

void cred_default_set_ids(lio_creds_t *c, char *id)
{
    char buffer[1024], buf2[256], buf3[512];
    uint64_t pid;
    int err;

    pid = getpid();
    err = getlogin_r(buf2, sizeof(buf2));
    if (err != 0) snprintf(buf2, sizeof(buf2), "ERROR(%d)", err);
    gethostname(buf3, sizeof(buf3));
    snprintf(buffer, sizeof(buffer), "%s:" LU ":%s:%s:%s", id, pid, buf2, buf3, _lio_exe_name);
    c->descriptive_id = strdup(buffer); c->descriptive_id_len = strlen(c->descriptive_id);
    c->id = strdup(id); c->id_len = strlen(c->id);
    return;
}

//***********************************************************************

void cdef_destroy(lio_creds_t *c)
{
    if (c->id != NULL) free(c->id);
    if (c->descriptive_id) free(c->descriptive_id);
    free(c);
}

//***********************************************************************

void cred_default_init(lio_creds_t *c, char *id)
{
    if (id) cred_default_set_ids(c, id);
    c->get_type = cdef_get_type;
    c->get_id = cdef_get_id;
    c->get_descriptive_id = cdef_get_descriptive_id;
    c->get_handle = cdef_get_handle;
    c->destroy = cdef_destroy;
}

//***********************************************************************

lio_creds_t *cred_default_create(char *id)
{
    lio_creds_t *c;
    tbx_type_malloc_clear(c, lio_creds_t, 1);

    cred_default_init(c, id);
    return(c);
}

