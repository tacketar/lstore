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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sodium.h>
#include <zmq.h>
#include <tbx/assert_result.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    char *account;
    unsigned char key_secret[1024];
    unsigned char key_public[1024];
    char key_z85[1024], *key_escaped;
    int key_pair = 0;

    if (argc < 2) {
        printf("\n");
        printf("lio_gen_psk [--pair] account|host\n");
        return(1);
    } else if (argc == 3) {
        if (strcmp(argv[1], "--pair") == 0) key_pair = 1;
    }

    account = argv[argc-1];

    if (key_pair == 0) {
        crypto_secretbox_keygen(key_secret);
        zmq_z85_encode(key_z85, key_secret, crypto_secretbox_KEYBYTES);
        key_escaped = tbx_stk_escape_text("[]\\#${}", '\\', key_z85);

        printf("[account-%s]\n", account);
        printf("key = %s\n\n", key_escaped);
        free(key_escaped);
    } else {
        zmq_curve_keypair((char *)key_public, (char *)key_secret);

        printf("# Don't forget to add the IP/host entry to the [mappings] section of ~/.lio/authorized_keys\n");
        printf("# in addition to the section containing the key provided below\n\n");
        printf("[%s]\n", account);
        key_escaped = tbx_stk_escape_text("=[]\\#${}", '\\', (char *)key_public);
        printf("public_key = %s\n", key_escaped);
        free(key_escaped);
        key_escaped = tbx_stk_escape_text("=[]\\#${}", '\\', (char *)key_secret);
        printf("secret_key = %s\n\n", key_escaped);
        free(key_escaped);


    }
    return(0);
}
