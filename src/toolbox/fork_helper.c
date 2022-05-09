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

#define _log_module_index 102

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <tbx/io.h>
#include "tbx/fork_helper.h"

//***************************************************************************
// tbx_fork_redirect - Redirects STDOUT and STDERR to the given filenames
//     On success 0 is returned otherwise 1=STDOUT open failed, 2=STDERR open failed
//     is returned.
//***************************************************************************

int tbx_fork_redirect(const char *fname_stdout, const char *fname_stderr)
{
    FILE *stdout_new, *stderr_new;

    //** Make sure we can open the new output files
    stdout_new = tbx_io_fopen(fname_stdout, "w");
    if (stdout_new == NULL) return(1);
    stderr_new = tbx_io_fopen(fname_stderr, "w");
    if (stderr_new == NULL) return(2);

    //** dup them. This will silently and atomically close/reopen the std* versions
    tbx_io_dup2(fileno(stdout_new), STDOUT_FILENO);
    tbx_io_dup2(fileno(stderr_new), STDERR_FILENO);

    //** Close the original file I opened since they aren't needed anymore
    tbx_io_fclose(stdin);      //** Also  close stdin
    tbx_io_fclose(stdout_new);
    tbx_io_fclose(stderr_new);

    return(0);
}

//***************************************************************************
//  tbx_fork - Performs a fork() and std* redirect. The parent process
//      is automatically terminated and only the child process returns
//      from the call.
//
//   prefix_std is used to determine whre STDOUT and STDERR are redirected to.
//      The new versions have ".stdout" and ".stderr" appended to the prefix.
//
//   0 is returned on success and a non-zero value is returned If an error
//   occurs opening the new STDOUT/ERR files.
//***************************************************************************

int tbx_fork(const char *prefix_std)
{
    int fmax = 8192;
    char fname_stdout[fmax], fname_stderr[fmax];
    int n;

    //** The parent get's the child's PID and the child returns with 0 from fork()
    if (fork() != 0) { exit(0); };  //** Have the parent terminate

    //** Make sure we have enough space for the new file names
    n = strlen(prefix_std) + 10;
    if (n > fmax) return(4);

    //** Form the new paths
    fname_stdout[fmax-1] = '\0';
    snprintf(fname_stdout, fmax-1, "%s.stdout", prefix_std);
    fname_stderr[fmax-1] = '\0';
    snprintf(fname_stderr, fmax-1, "%s.stderr", prefix_std);

    return(tbx_fork_redirect(fname_stdout, fname_stderr));  //** Do the redirection
}

