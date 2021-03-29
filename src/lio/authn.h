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
// Generic Authentication service
//***********************************************************************

#ifndef _AUTHN_ABSTRACT_H_
#define _AUTHN_ABSTRACT_H_

#include <tbx/iniparse.h>
#include <lio/authn.h>

#include "service_manager.h"

#ifdef __cplusplus
extern "C" {
#endif

#define AUTHN_AVAILABLE "authn_available"

#define AUTHN_INIT_DEFAULT 0   //** Normal methof for initializing creds for a client
#define AUTHN_INIT_LOOKUP  1   //** Typically used for the Server to get creds from the AuthN server

struct lio_creds_t {
    void *priv;
    char *id;             //** User ID
    char *descriptive_id;  //** User ID along with other info about the process, etc
    void *handle;         //** Handle used for interprocess communication
    int id_len;           //** Length of the ID
    int descriptive_id_len;  //** Length of the Descriptive ID
    int handle_len;       //** Length of the handle
    char *(*get_type)(lio_creds_t *creds);
    char *(*get_id)(lio_creds_t *creds, int *len);
    char *(*get_descriptive_id)(lio_creds_t *creds, int *len);
    void *(*get_handle)(lio_creds_t *creds, int *len);
    void (*destroy)(lio_creds_t *creds);
};

#define an_cred_get_type(c) (c)->get_type(c)
#define an_cred_get_id(c, len) (c)->get_id(c, len)
#define an_cred_get_descriptive_id(c, len) (c)->get_descriptive_id(c, len)
#define an_cred_get_handle(c, len) (c)->get_handle(c, len)
#define an_cred_destroy(c) (c)->destroy(c)

struct lio_authn_t {
    void *priv;
    void (*print_running_config)(lio_authn_t *an, FILE *fd, int print_section_heading);
    lio_creds_t *(*cred_init)(lio_authn_t *an, int type, void **args);
    void (*destroy)(lio_authn_t *an);
};

typedef lio_authn_t *(authn_create_t)(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section);

void cred_default_init(lio_creds_t *c, char *id);
lio_creds_t *cred_default_create(char *id);
void cred_default_set_ids(lio_creds_t *c, char *id);

#define authn_cred_init(an, type, args) (an)->cred_init(an, type, args)
#define authn_destroy(an) (an)->destroy(an)
#define authn_print_running_config(an, fd, psh) (an)->print_running_config(an, fd, psh)

#ifdef __cplusplus
}
#endif

#endif
