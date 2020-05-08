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

#define _log_module_index 175

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/range_stack.h>
#include <tbx/atomic_counter.h>


//*******************************************************************************
//  _do_test
//*******************************************************************************

int _do_test(int i, char *in, char *out, int overlap_only)
{
    int err;
    char *final;
    tbx_stack_t *r;

    err = 0;
    r = tbx_range_stack_string2range(in, ";", overlap_only);
    final = tbx_range_stack_range2string(r, ";");
    if (strcmp(final, out) != 0) {
        err = 1;
        fprintf(stderr, "ERROR: i=%d input  =%s\n", i, in);
        fprintf(stderr, "ERROR: i=%d output =%s\n", i, final);
        fprintf(stderr, "ERROR: i=%d correct=%s\n", i, out);
    }

    free(final);
    tbx_stack_free(r, 1);

    return(err);
}


//*******************************************************************************
//*******************************************************************************

int main(int argc, char **argv)
{
    int err;

    err = 0;
    err += _do_test(0, "100:200;", "100:200;", 0);
    err += _do_test(1, "100:200;150:200;", "100:200;", 0);
    err += _do_test(2, "100:200;150:210;", "100:210;", 0);
    err += _do_test(3, "100:200;50:101;", "50:200;", 0);
    err += _do_test(4, "100:200;50:99;", "50:200;", 0);
    err += _do_test(5, "100:200;50:98;", "50:98;100:200;", 0);
    err += _do_test(6, "100:200;50:98;95:101;", "50:200;", 0);
    err += _do_test(7, "100:200;50:98;99:99;", "50:200;", 0);
    err += _do_test(8, "101:200;50:98;99:99;", "50:99;101:200;", 0);
    err += _do_test(9, "101:200;50:75;20:100;", "20:200;", 0);
    err += _do_test(10, "100:200;50:75;77:98", "50:75;77:98;100:200;", 0);
    err += _do_test(11, "100:200;50:75;77:98;76:99", "50:200;", 0);

    err += _do_test(0, "100:200;", "100:200;", 1);
    err += _do_test(1, "100:200;150:200;", "100:200;", 1);
    err += _do_test(2, "100:200;150:210;", "100:200;150:210;", 1);
    err += _do_test(2, "150:210;100:200;", "100:200;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;", "100:200;100:125;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;160:200;", "100:200;100:125;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;160:200;75:80;", "75:80;100:200;100:125;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;160:200;75:85;80:200;", "75:85;80:200;100:200;100:125;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;160:200;75:85;80:200;90:99;", "75:85;80:200;100:200;100:125;150:210;", 1);
    err += _do_test(2, "100:125;150:210;100:200;160:200;75:85;80:150;160:200;", "75:85;80:150;100:200;100:125;150:210;", 1);

    if (err == 0) {
        printf("range_stack_test: PASSED\n");
    } else {
        printf("range_stack_test: FAILED\n");
    }
    return(err);
}

