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

#include <stdio.h>
#include <stdlib.h>
#include <tbx/constructor.h>
#include "erasure_tools.h"

//#define debug_printf(...) printf(__VA_ARGS__)

int _debug = 0;

int main(int argc, char **argv)
{
  lio_erasure_plan_t *plan;
  int method, w, packet_size, data_strips, parity_strips, buffer_size, n_data_missing, n_parity_missing;
  int base_unit;
  int i, j, k;
  long long int strip_size, file_offset, poffset, file_size;
  char *fname, *pname;
  int  *erasures;

  tbx_construct_fn_static();

  if (argc < 13) {
     printf("et_decoder file_size filename file_offset parity_file parity_offset\n");
     printf("     method strip_size data_strips parity_strips w packet_size base_unit buffer_size\n");
     printf("     n_data_missing data_missing_1 ... data_missing_n\n");
     printf("     n_parity_missing parity_missing_1 ... parity_missing_n\n");
     return(1);
  }

  //** Parse the parameters **
  i = 1;
  file_size = atoll(argv[i]); i++;
  fname = argv[i]; i++;
  file_offset = atoll(argv[i]); i++;
  pname = argv[i]; i++;
  poffset = atoll(argv[i]); i++;
  method = atoi(argv[i]); i++;
  strip_size = atoll(argv[i]); i++;
  data_strips = atoi(argv[i]); i++;
  parity_strips = atoi(argv[i]); i++;
  w = atoi(argv[i]); i++;
  packet_size = atoi(argv[i]); i++;
  base_unit = atoi(argv[i]); i++;
  buffer_size = atoi(argv[i]); i++;

printf("w=%d packet=%d base=%d buffer=%d\n", w, packet_size, base_unit, buffer_size);

  //** Read in the erased data blocks
  erasures = (int *)malloc(sizeof(int)*(data_strips+parity_strips));
  n_data_missing = atoi(argv[i]); i++;
  if (n_data_missing > data_strips) {
     printf("et_decoder: Error n_data_missing > data_strips!! %d > %d\n", n_data_missing, data_strips);
     return(1);
  }

  debug_printf("n_data_missing=%d --", n_data_missing);
printf("n_data_missing=%d --", n_data_missing);
  for (j=0; j<n_data_missing; j++) {
     k = atoi(argv[i]); i++;
     if (k > data_strips) {
        printf("et_decoder: Invalid missing data stripe > data_strips!! %d > %d\n", k, data_strips);
        return(1);
     }
     erasures[j] = k - 1;
     debug_printf(" %d", k);
  }
  debug_printf("\n");

  //** Do the same for the missing parity blocks
  n_parity_missing = atoi(argv[i]); i++;
  if (n_parity_missing > parity_strips) {
     printf("et_decoder: Error n_parity_missing > parity_strips!! %d > %d\n", n_parity_missing, parity_strips);
     return(1);
  }
//printf("n_parity_missing=%d i=%d\n", n_parity_missing, i); fflush(stdout);
  debug_printf("n_parity_missing=%d --", n_parity_missing);
printf("n_parity_missing=%d --", n_parity_missing);

  for (j=0; j<n_parity_missing; j++) {
     k = atoi(argv[i]); i++;
     if (k > parity_strips) {
        printf("et_decoder: Invalid missing parity stripe > parity_strips!! %d > %d\n", k, parity_strips);
        return(1);
     }
     erasures[n_data_missing + j] = data_strips + k - 1;
     debug_printf(" %d", k);
  }
  debug_printf("\n");

  erasures[n_data_missing + n_parity_missing] = -1; //** Signifies the end of the missing list

//for(j=0; j<n_parity_missing+n_data_missing+1; j++) { printf("erasures[%d]=%d\n", j,erasures[j]); }

  plan = et_new_plan(method, strip_size, data_strips, parity_strips, w, packet_size, base_unit);

printf("plan=%p\n", plan); fflush(stdout);

  if (plan == NULL) {
     printf("et_encoder: Error creating plan!!!!!\n");
     return(1);
  }

  if ((n_data_missing+n_parity_missing) == 0) return(0);  //** Nothing to do

  return(et_decode(plan, file_size, fname, file_offset, pname, poffset, buffer_size, erasures));
}

