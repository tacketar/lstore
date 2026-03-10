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

#ifndef __ERASURE_TOOLS_ISAL_H_
#define __ERASURE_TOOLS_ISAL_H_

#include <lio/erasure_tools.h>
#include <stdio.h>
#include <isa-l/erasure_code.h>


#define BLANK_CHAR '0'
#define N_ISA_METHODS  2
#define REED_SOL_VAN_ISA  0
#define CAUCHY_ORIG_ISA  1

#ifdef __cplusplus
extern "C" {
#endif


struct lio_erasure_plan_t;

extern int _debug;
#define debug_printf(...) \
   if (_debug > 0) { \
      printf(__VA_ARGS__); \
   }

extern const char *JE_method[N_JE_METHODS];


struct lio_erasure_plan_t {    //** Contains the erasure parameters
    long long int strip_size;   //** Size of each data strip
    int method;                 //** Encoding/Decoding method used
    int data_strips;            //** Number of data devices
    int parity_strips;          //** Number of coding or parity devices
    int w;                      //** Word size
    int packet_size;            //** Chunk size for operations
    int base_unit;              //** Typically the register size in bytes
    unsigned char *encode_matrix;         //** Encoding Matrix
    unsigned char *g_tbls;
    int (*gen_g_tbls)(lio_erasure_plan_t *plan);  //**Routine to form encoding matrix
    //int *encode_bitmatrix;      //** Encoding bit Matrix
    //int **encode_schedule;      //** Encoding Schedule
    int (*form_encoding_matrix)(lio_erasure_plan_t *plan);  //**Routine to form encoding matrix
    void (*print_plan)(lio_erasure_plan_t *plan);
    int (*form_decoding_matrix)(lio_erasure_plan_t *plan);  //**Routine to form encoding matrix
    //void (*encode_block)(lio_erasure_plan_t *plan, char **ptr, int block_size);  //**Routine for encoding the block
    //int (*decode_block)(lio_erasure_plan_t *plan, char **ptr, int block_size, int *erasures);  //**Routine for decoding the block
};

int nearest_prime(int w, int which);

#ifdef __cplusplus
}
#endif


#endif
