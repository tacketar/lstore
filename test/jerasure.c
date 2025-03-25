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

int main(int argc, char **argv)
{
  lio_erasure_plan_t *plan;
  int method, w, packet_size, data_strips, parity_strips, base_unit, i, buffer_size;
  long long int strip_size, file_offset, poffset;
  char *fname, *pname;

  tbx_construct_fn_static();

  if (argc < 12) {
     printf("et_encoder filename file_offset parity_file parity_offset method strip_size data_strips parity_strips w packet_size base_unit [buffer_size]\n");
     return(1);
  }

  //** Parse the parameters **
  i = 1;

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

  buffer_size = 0;
  if (i<argc) { buffer_size = atoi(argv[i]); i++; }

  plan = et_new_plan(method, strip_size, data_strips, parity_strips, w, packet_size, base_unit);

  if (plan == NULL) {
     printf("et_encoder: Error creating plan!!!!!\n");
     return(1);
  }

  return(et_encode(plan, fname, file_offset, pname, poffset, buffer_size));
}

