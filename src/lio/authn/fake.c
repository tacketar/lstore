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
// Dummy/Fake AuthN service.  Always returns success!
//***********************************************************************

#define _log_module_index 185

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>
#include <unistd.h>

#include "authn.h"
#include "fake.h"
#include "service_manager.h"

//***********************************************************************
// authn_fake_get_type - Returns the type
//***********************************************************************

char *authn_fake_get_type(lio_creds_t *c)
{
    return(AUTHN_TYPE_FAKE);
}

//***********************************************************************
// authn_fake_cred_init - Creates a Fake AuthN credential
//***********************************************************************

lio_creds_t *authn_fake_cred_init(lio_authn_t *an, int type, void **args)
{
    lio_creds_t *c;
    char *account;
    char buf[128];

    if (args[0] == NULL) { //** Default to using the local username
        if (getlogin_r(buf, sizeof(buf)) == 0) {
            account = buf;
        } else {
            account = "default";
        }
    } else {
        account = args[0];
    }

    c = cred_default_create(account);
    c->get_type = authn_fake_get_type;
    c->handle = c->descriptive_id;
    c->handle_len = strlen(c->descriptive_id)+1;

    return(c);
}

//***********************************************************************
// apsk_server_print_running_config - Prints the running config
//***********************************************************************

void authn_fake_print_running_config(lio_authn_t *an, FILE *fd, int print_section_heading)
{
    char *section = an->priv;

    if (print_section_heading) fprintf(fd, "[%s]\n", section);
    fprintf(fd, "type = %s\n", AUTHN_TYPE_FAKE );
    fprintf(fd, "\n");
}

//***********************************************************************
// authn_fake_destroy - Destroys the FAke AuthN service
//***********************************************************************

void authn_fake_destroy(lio_authn_t *an)
{
    if (an->priv) free(an->priv);
    free(an);
}

//***********************************************************************
// authn_fake_create - Create a Fake AuthN service
//***********************************************************************

lio_authn_t *authn_fake_create(lio_service_manager_t *ess, tbx_inip_file_t *ifd, char *section)
{
    lio_authn_t *an;

    tbx_type_malloc(an, lio_authn_t, 1);

    an->print_running_config = authn_fake_print_running_config;
    an->cred_init = authn_fake_cred_init;
    an->destroy = authn_fake_destroy;
    an->priv = strdup(section);

    return(an);
}
