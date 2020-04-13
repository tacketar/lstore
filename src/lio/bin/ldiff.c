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


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

//*************************************************************************
// compare_buffers - Compares the byte ranges.  Single byte matches are suppressed
//*************************************************************************

void compare_buffers(char *b1, char *b2, int64_t len, int64_t offset, int64_t *bad_bytes, int64_t *bad_groups,
                     int64_t block_size, int *state, int64_t *state_offset, int64_t fsize, int64_t big)
{
    int mode, ok;
    int64_t start, end, i, k, last, bs, be, bl, bos, boe;

    mode = *state;
    start = *state_offset;
    be = offset + len;
    last = (be == fsize) ? len-1 : -1;

    for (i=0; i<len; i++) {
        if (mode == 0) {  //** Matching range
            if ((b1[i] != b2[i]) || (last == i)) {
                end = offset + i-1;
                k = end - start + 1;
                bs = start / block_size;
                be = end / block_size;
                bos = start % block_size;
                boe = end % block_size;
                bl = be - bs + 1;
                if (k<big) {
                    printf("  MATCH  : " I64T " -> " I64T " (" I64T " bytes) [" I64T "%%" I64T  " -> " I64T "%%" I64T " (" I64T " blocks)] **SMALL**\n", start, end, k, bs, bos, be, boe, bl);
                } else {
                    printf("  MATCH  : " I64T " -> " I64T " (" I64T " bytes) [" I64T "%%" I64T  " -> " I64T "%%" I64T " (" I64T " blocks)] **BIG**\n", start, end, k, bs, bos, be, boe, bl);
                }
                start = offset + i;
                mode = 1;
            }
        } else {
            if ((b1[i] == b2[i]) || (last == i)) {
                ok = 0;  //** Suppress single byte matches
                if (last != i) {
                    if (b1[i+1] == b2[i+1]) ok = 1;
                }
                if ((ok == 1) || (last == i)) {
                    end = offset + i-1;
                    k = end - start + 1;
                    *bad_bytes += k;
                    (*bad_groups)++;
                    bs = start / block_size;
                    be = end / block_size;
                    bos = start % block_size;
                    boe = end % block_size;
                    bl = be - bs + 1;
                    printf("  DIFFER : " I64T " -> " I64T " (" I64T " bytes) [" I64T "%%" I64T  " -> " I64T "%%" I64T " (" I64T " blocks)]\n", start, end, k, bs, bos, be, boe, bl);

                    start = offset + i;
                    mode = 0;
                }
            }
        }
    }

    *state = mode;
    *state_offset = start;
    return;
}

//*************************************************************************
//*************************************************************************

int main(int argc, char **argv)
{
    int64_t block_size, buf_size, state_offset, big;
    int64_t fsize1, fsize2, max_size, cpos, len, bad_bytes, bad_groups;
    int start_option, i, state;
    char *fname1, *fname2;
    char *buf1, *buf2;
    FILE *fd1, *fd2;

    block_size = 64 *1024;
    buf_size = 20 *1024*1024;

    if (argc < 3) {
        printf("\n");
        printf("ldiff [-b block_size] [-s buffer_size] file1 file2\n");
        printf("     -b block_size   Block size for matching to cache page boundaries.  Defaults to " I64T ".  Can use units.\n", block_size);
        printf("     -s buffer_size  Buffer size used in the comparison. This is divided in 1/2 for each buffer. Defaults to " I64T ". Cant use units.\n", buf_size);
        printf("\n");
        return(1);
    }

    //*** Parse the args
    i=1;
    if (argc > 1) {
        do {
            start_option = i;
            if (strcmp(argv[i], "-b") == 0) { //** Block size
                i++;
                block_size = tbx_stk_string_get_integer(argv[i]);
                i++;
            } else if (strcmp(argv[i], "-s") == 0) { //** Buffer size
                i++;
                buf_size = tbx_stk_string_get_integer(argv[i]);
                i++;
            }
        } while ((start_option - i < 0) && (i<argc));
    }


    if ((argc-i) < 2) {
        printf("Missing filenames!\n");
        return(1);
    }

    //** Open the files and get their sizes
    fname1 = argv[i];
    fname2 = argv[i+1];
    fd1 = fopen(fname1, "r");FATAL_UNLESS(fd1 != NULL);
    fd2 = fopen(fname2, "r");FATAL_UNLESS(fd2 != NULL);

    fseek(fd1, 0, SEEK_END);
    fseek(fd2, 0, SEEK_END);
    fsize1 = ftell(fd1);
    fsize2 = ftell(fd2);
    fseek(fd1, 0, SEEK_SET);
    fseek(fd2, 0, SEEK_SET);

    //** Print a summary of options
    printf("File 1: %s  (" I64T " bytes)\n", fname1, fsize1);
    printf("File 2: %s  (" I64T " bytes)\n", fname2, fsize2);
    if (fsize1 != fsize2) printf("WARNING:  File sizes differ!!!!!\n");
    printf("Block size: " I64T "\n", block_size);
    printf("Buffer size: " I64T "\n", buf_size);

    buf_size /= 2;  //** It's split between the 2 buffers

    tbx_type_malloc(buf1, char, buf_size);
    tbx_type_malloc(buf2, char, buf_size);

    max_size = (fsize1 < fsize2) ? fsize1 : fsize2;

    printf("\n");
    printf("Printing comparision breakdown -- Single byte matches are suppressed (max_size=" I64T ")\n", max_size);

    big = block_size/2;
    state = 0;
    state_offset = 0;
    bad_bytes = bad_groups = 0;
    for (cpos=0; cpos < max_size; cpos += buf_size) {
        len = ((cpos+buf_size) < max_size) ? buf_size : max_size - cpos;
        assert_result((int)fread(buf1, 1, len, fd1), len);
        assert_result((int)fread(buf2, 1, len, fd2), len);

        compare_buffers(buf1, buf2, len, cpos, &bad_bytes, &bad_groups, block_size, &state, &state_offset, max_size, big);
    }

    printf("\n");
    printf("Bad bytes: " I64T " Bad groups: " I64T "\n", bad_bytes, bad_groups);

    free(buf1);
    free(buf2);

    fclose(fd1);
    fclose(fd2);

    i = ((bad_bytes > 0) || (fsize1 != fsize2)) ? 1 : 0;
    return(i);
}
