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
#include <string.h>
#include <tbx/constructor.h>
#include "erasure_tools.h"

int main(int argc, char **argv)
{
  int i, j, meth_index;
  int w = -1;
  int packet_low = -1;
  int packet_high = -1;
  int packet_size;
  int data_strips, parity_strips;
  long long int chunk_size, strip_size, new_size;
  float increase;
  char *meth;
  char *method[N_JE_METHODS];

  tbx_construct_fn_static();

  method[REED_SOL_VAN] = "reed_sol_van";
  method[REED_SOL_R6_OP] = "reed_sol_r6_op";
  method[CAUCHY_ORIG] = "cauchy_orig";
  method[CAUCHY_GOOD] = "cauchy_good";
  method[BLAUM_ROTH] = "blaum_roth";
  method[LIBERATION] = "liberation";
  method[LIBER8TION] = "liber8tion";
  method[RAID4] = "raid4";

  if (argc < 5) {
     printf("et_planner method chumk_size data_strips parity_strips [w] [packetsize_low] [packetsize_high]\n");
     printf("   where method is ");
     for (i=0; i<N_JE_METHODS-1; i++) printf("%s, ", method[i]);
     printf("or %s\n", method[N_JE_METHODS-1]);
     printf("\n");
     return(1);
  }

  //** Parse the mandatory constants **
  i = 1;

  meth = argv[i]; i++;
  if (strcmp(method[REED_SOL_VAN], meth) == 0) {
     meth_index = REED_SOL_VAN;
  } else  if (strcmp(method[REED_SOL_R6_OP], meth) == 0) {
     meth_index = REED_SOL_R6_OP;
  } else  if (strcmp(method[CAUCHY_ORIG], meth) == 0) {
     meth_index = CAUCHY_ORIG;
  } else  if (strcmp(method[CAUCHY_GOOD], meth) == 0) {
     meth_index = CAUCHY_GOOD;
  } else  if (strcmp(method[BLAUM_ROTH], meth) == 0) {
     meth_index = BLAUM_ROTH;
  } else  if (strcmp(method[LIBERATION], meth) == 0) {
     meth_index = LIBERATION;
  } else  if (strcmp(method[LIBER8TION], meth) == 0) {
     meth_index = LIBER8TION;
  } else  if (strcmp(method[RAID4], meth) == 0) {
     meth_index = RAID4;
  } else {
     printf("Invalid method(%s) specified.  Should be ", meth);
     for (i=0; i<N_JE_METHODS-1; i++) printf("%s, ", method[i]);
     printf("or %s\n", method[N_JE_METHODS-1]);
     printf("\n");
     return(1);
  }

//printf("meth=%s\n", meth);

  chunk_size = atoll(argv[i]); i++;
  data_strips = atoi(argv[i]); i++;
  parity_strips = atoi(argv[i]); i++;

  lio_erasure_plan_t *eplan = et_generate_plan(chunk_size, meth_index, data_strips, parity_strips, w, packet_low, packet_high);

  if (eplan == NULL) {
     printf("Error generating plan!!!!!\n");
     return(1);
  }

  w = eplan->w;
  packet_size = eplan->packet_size;;
  strip_size = eplan->strip_size;
  new_size = data_strips*strip_size;
  j = new_size - chunk_size;
  increase = (1.0*j) / chunk_size * 100;
  fprintf(stderr, "planner: Strip size=%lld New size=%lld Orig size=%lld increase=%d (%f percent)\n", strip_size, new_size, chunk_size, j, increase);

  //** Print plan
  printf("%d %lld %d %d %d %d %d\n", meth_index, strip_size, data_strips, parity_strips, w, packet_size, eplan->base_unit);
  return(0);
}

