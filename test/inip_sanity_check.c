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

#define _log_module_index 187

#include <stdlib.h>
#include <string.h>
#include <tbx/iniparse.h>
#include <tbx/log.h>

//*************************************************************************
//  Simple sanity check that all INI files included can be resolved and
//     is not malformed.
//*************************************************************************

void print_file(tbx_inip_file_t *ifd, FILE *fd, char *header)
{
    char *text;
    int n;

    text = tbx_inip_serialize(ifd);
    if (text == NULL) {
        fprintf(stderr, "ERROR serializing INI object!\n");
        return;
    }

    n = strlen(text);
    fprintf(fd, "#--------------------------------------------------------------------\n");
    fprintf(fd, "# %s (nbytes=%d)\n", header, n);
    fprintf(fd, "#--------------------------------------------------------------------\n");
    fprintf(fd, "%s", text);
    free(text);
}

//*************************************************************************
//  Doesn't actually load the file into an INI struct but instead just tries and
//     resolves any dependencies and converts it to a string for transport.
//*************************************************************************

void print_file2string(char *fname, FILE *fd)
{
    char *text = NULL;
    int n = 0;

    tbx_inip_file2string(fname, &text, &n);
    if (text == NULL) {
        fprintf(stderr, "ERROR Converting file to string!\n");
        return;
    }

    n = strlen(text);
    fprintf(fd, "#--------------------------------------------------------------------\n");
    fprintf(fd, "# %s (nbytes=%d)\n", fname, n);
    fprintf(fd, "#--------------------------------------------------------------------\n");
    fprintf(fd, "%s", text);
    free(text);
}

void print_help()
{
    printf("\n");
    printf("inip_sanity_check [INI_OPTIONS] -f INI-file\n");
    printf("inip_sanity_check  -f2s INI-file\n");
    printf("     -pi   Print the initial INI before any hints or variables substititions are made\n");
    printf("     -ph   Print the INI after hints are applied but before variables substititions are made\n");
    printf("     -pf   Print the final INI after all hints and variables substititions are made\n");
    printf("     -f    Init file to use\n");
    printf("     -f2s  Just convert the given INI file into a string without loading it into an ifd\n");
    printf("\n");
    tbx_inip_print_hint_options(stdout);
}

int main(int argc, char **argv)
{
    int n_errors, err, do_print, i, start_option;
    char *fname;
    tbx_inip_file_t *ifd;
    tbx_stack_t *hints;

    //** Load any hints we may have
    hints = tbx_stack_new();
    tbx_inip_hint_options_parse(hints, argv, &argc);

    err = 0;
    if (argc < 2) {
        print_help();
        tbx_stack_free(hints, 1);
        return(1);
    }

    tbx_log_open("stderr", 0);

    do_print = 0;
    fname = NULL;
    i=1;
    do {
        start_option = i;
        if (strcmp(argv[i], "-pi") == 0) {  //** Print initial INI
            i++;
            do_print += 1;
        } else if (strcmp(argv[i], "-ph") == 0) { //** Print after hints applied
            i++;
            do_print += 2;
        } else if (strcmp(argv[i], "-pf") == 0) {  //** Print final verion after hints and variable subs
            i++;
            do_print += 4;
        } else if (strcmp(argv[i], "-f") == 0) {  //** INI file to parse
            i++;
            fname = argv[i];
            i++;
        } else if (strcmp(argv[i], "-f2s") == 0) {  //** Just do a file to string conversion
            i++;
            print_file2string(argv[i], stdout);
            tbx_stack_free(hints, 1);
            return(0);
        }
    } while ((start_option < i) && (i<argc));

    if (fname == NULL) {
        fprintf(stderr, " ERROR: No INI file specified!\n");
        return(1);
    }

    //** Open the file
    ifd = tbx_inip_file_read(fname, 1);
    if (ifd == NULL) {
        fprintf(stderr, "ERROR: parsing file!\n");
        tbx_stack_free(hints, 1);
        return(1);
    }

    if (do_print & 1) print_file(ifd, stdout, "Initial version");

    err = tbx_inip_hint_list_apply(ifd, hints);  //** Apply the hints
    tbx_inip_hint_list_destroy(hints);           //** and cleanup
    if (err != 0) {
        fprintf(stderr, "ERROR applying hints!\n");
    }

    if (do_print & 2) print_file(ifd, stdout, "Hints applied version");

    n_errors = tbx_inip_apply_params(ifd);  //** Apply the paramters
    if (n_errors) {
        err = 1;
        fprintf(stderr, "ERROR: Undefined params! n_errors=%d\n", n_errors);
    } else {
        if (do_print & 4) print_file(ifd, stdout, "Final version");
    }

    tbx_inip_destroy(ifd);

    return(err);
}


