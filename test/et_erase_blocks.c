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

#define BLANK_CHAR '0'

//*********************************************************************************
// wipe_block - Blanks the provided byte range of the file
//*********************************************************************************

void wipe_block(const char *fname, long long int offset, int nbytes)
{
  int i;
  const int bufsize = 1048576;
  char buffer[bufsize];
  FILE *fd = fopen(fname, "r+");

  memset(buffer, BLANK_CHAR, bufsize);

  int n = nbytes / bufsize;
  int rem = nbytes % bufsize;

  fseek(fd, offset, SEEK_SET);
  for (i=0; i<n; i++) {
     fwrite(buffer, 1, bufsize, fd);
  }
  fwrite(buffer, 1, rem, fd);

  fclose(fd);
}

//*********************************************************************************
//*********************************************************************************

int main(int argc, char **argv)
{
  int n_missing, row_data_size, row_parity_size;
  int i, j, nbytes, row, dev, n_data, n_parity;
  long long int strip_size, file_offset, poffset, bpos, file_size;
  char *fname, *pname;

  if (argc < 7) {
     printf("et_rease_blocks filename file_offset parity_file parity_offset strip_size n_data_devs n_parity_devs \n");
     printf("     n_data_missing row_missing_1 block_missing_1 ... row_missing_n block_missing_n\n");
     printf("     n_parity_missing row_missing_1 block_missing_1 ... row_missing_n block_missing_n\n");
     return(1);
  }

  //** Parse the parameters **
  i = 1;
  fname = argv[i]; i++;
  file_offset = atoll(argv[i]); i++;
  pname = argv[i]; i++;
  poffset = atoll(argv[i]); i++;
  strip_size = atoll(argv[i]); i++;
  n_data = atoll(argv[i]); i++;
  n_parity = atoll(argv[i]); i++;

  //** Get the file size
  FILE *fd = fopen(fname, "r");
  fseek(fd, 0, SEEK_END);
  file_size = ftell(fd);
  fclose(fd);

  row_data_size = n_data * strip_size;
  row_parity_size = n_parity * strip_size;
printf("n_data=%d strip_size=%lld row_data_size=%d\n", n_data, strip_size, row_data_size);

  //** Read in the erased data and parity blocks. The devices start numbering at 1
  n_missing = atoi(argv[i]); i++;
  for (j=0; j<n_missing; j++) {
     row = atoi(argv[i]); i++;
     dev = atoi(argv[i]); i++;
     bpos = file_offset + row*row_data_size + (dev-1)*strip_size;
     nbytes = ((bpos + strip_size) > file_size) ? file_size - bpos : strip_size;
     printf("Data: row=%d dev=%d bpos=%lld nbytes=%d fstart=%lld fsize=%lld\n", row, dev, bpos, nbytes, file_offset, file_size);
     wipe_block(fname, bpos, nbytes);
  }

  n_missing = atoi(argv[i]); i++;
  for (j=0; j<n_missing; j++) {
     row = atoi(argv[i]); i++;
     dev = atoi(argv[i]); i++;
     bpos = poffset + row*row_parity_size + (dev-1)*strip_size;
     printf("Parity: row=%d dev=%d bpos=%lld pstart=%lld\n", row, dev, bpos, poffset);
     wipe_block(pname, bpos, strip_size);
  }

  return(0);
}

