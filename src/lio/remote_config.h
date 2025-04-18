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

#include <lio/lio.h>

void rc_print_running_config(FILE *fd);
int rc_client_get_config(gop_mq_context_t *mqc, lio_creds_t *creds, char *rc_string, char *mq_default, char **config, char **hinsts_string, char **obj_name, char **rc_user, time_t *timestamp);
int rc_server_install(lio_config_t *lc, char *section);
void rc_server_destroy();

