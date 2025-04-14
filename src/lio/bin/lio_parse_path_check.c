/*
Advanced Computing Center for Research and Education Proprietary License
Version 1.0 (April 2006)

Copyright (c) 2006, Advanced Computing Center for Research and Education,
 Vanderbilt University, All rights reserved.

This Work is the sole and exclusive property of the Advanced Computing Center
for Research and Education department at Vanderbilt University.  No right to
disclose or otherwise disseminate any of the information contained herein is
granted by virtue of your possession of this software except in accordance with
the terms and conditions of a separate License Agreement entered into with
Vanderbilt University.

THE AUTHOR OR COPYRIGHT HOLDERS PROVIDES THE "WORK" ON AN "AS IS" BASIS,
WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, TITLE, FITNESS FOR A PARTICULAR
PURPOSE, AND NON-INFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Vanderbilt University
Advanced Computing Center for Research and Education
230 Appleton Place
Nashville, TN 37203
http://www.accre.vanderbilt.edu
*/

#include <stdlib.h>
#include <string.h>
#include <tbx/log.h>
#include  <lio/lio.h>


//*************************************************************************
//  Simple tool to test the lio_parse_path() routine
//*************************************************************************

int main(int argc, char **argv)
{
    int is_lio, port;
    char *ppath;
    char *user, *mq, *host, *cfg, *section, *hints, *fname;

    if (argc < 2) {
        printf("lio_parse_path_check -t | lio_path\n");
        printf("    -t        - Run the builtin path tests.\n");
        printf("    lio_path  - Parse the provided path and print the result to stdout.\n");
        return(0);
    }


    //** See if we are just running the internal tests
    if (strcmp(argv[1], "-t") == 0) { //** Do the internal tests
        return(lio_parse_path_test());
    }

    //** If we made it here we are parsing a path
    ppath = argv[1];
    user = mq = host = cfg = section = hints = fname = NULL;
    is_lio = lio_parse_path(ppath, &user, &mq, &host, &port, &cfg, &section, &hints, &fname, 1);
    printf("hints=%s\n", hints);
    printf("breakout:\n  is_lio=%d\n  user=%s\n  mq=%s\n  host=%s\n  port=%d\n  cfg=%s\n  section=%s\n  fname=%s\n", is_lio, user, mq, host, port, cfg, section, fname);

    if (user) free(user);
    if (mq) free(mq);
    if (host) free(host);
    if (cfg) free(cfg);
    if (section) free(section);
    if (fname) free(fname);
    if (hints) free(hints);

    return(0);
}

