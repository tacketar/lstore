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

#include <limits.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <regex.h>
#include <tbx/normalize_path.h>

//*****************************************************************************************
// tbx_normalize_check_make -Makes a normalized check regex for use
//*****************************************************************************************

regex_t *tbx_normalize_check_make()
{
    int err;
    regex_t *r = malloc(sizeof(regex_t));

    err = regcomp(r, "(^\\./)|(^\\.\\./)|(/\\.\\./)|(/\\./)|(//)|(/$)|(/\\.$)|(/\\.\\.$)", REG_NOSUB|REG_EXTENDED);
    if (err) return(NULL);

    return(r);
}

//*****************************************************************************************
// tbx_normalize_check_destroy - Destroys a normalized check regex for use
//*****************************************************************************************

void tbx_normalize_check_destroy(regex_t *r)
{
    if (r) regfree(r);
}

//*****************************************************************************************
// tbx_path_is_normalized - Quickly dtermines if the given path is in normalized form.
//    Since this is just a check the path can be relative or absolute
//*****************************************************************************************

int tbx_path_is_normalized(regex_t *r, const char *path)
{
    int err;
    regmatch_t pmatch[1];

    err = regexec(r, path, 0, pmatch, 0);
    return((err == REG_NOMATCH) ? 1 : 0);
}

//*****************************************************************************************
// tbx_normalize_path - Normalizes a path.  The path must be an absolute path.
//     buffer should be as large as path and is used to store the updated path
//*****************************************************************************************

char *tbx_normalize_path(const char *path, char *buffer)
{
    int i, dpos, spos, n;
    int dirpos[100], ndir;

    if (path[0] != '/') return(NULL);

    ndir = 0;
    dirpos[ndir] = 0;

    spos = 0;
    dpos = 0;
    i = 0;
    while (path[i] != '\0') {
        //** We start on a '/' so see if there are any more
        i++;
        while (path[i] == '/') i++;
        if (path[i] == '\0') break;  //** Kick out

        spos = i; //** This is the 1st char in the token after the last '/'

        //** Now find the next '/'
        i++;
        while ((path[i] != '/') && (path[i] != '\0')) i++;

        //** The token exists between [spos:i-1]
        n = i-spos;
        if ((n == 1) && (path[spos] == '.')) { //** Match: /./
            continue;  //** Just drop the token
        } else if ((n == 2) && (path[spos] == '.') && (path[spos+1] == '.')) { //** Match: /../
            //** Need to roll baack an entry
            if (ndir == 0) return(NULL);
            ndir--;
            dpos = dirpos[ndir];
            buffer[dpos] = '\0';
        } else { //** Just a normal token to store
            memcpy(buffer + dpos, path + spos - 1, n+1);  //** Copy over the initial '/'

            //** Update the dir table
            dirpos[ndir] = dpos;
            ndir++;

            dpos += n+1;  //** And the destination pos
        }
    }

    if (dpos == 0) {
        buffer[0] = '/';
        buffer[1] = '\0';
    } else if (buffer[dpos] == '/') {
        buffer[dpos] = '\0';
    } else {
        buffer[dpos] = '\0';
    }

    return(buffer);
}

