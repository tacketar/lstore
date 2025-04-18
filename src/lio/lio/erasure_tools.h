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

/** \file
* Autogenerated public API
*/

#ifndef ACCRE_LIO_ERASURE_TOOLS_H_INCLUDED
#define ACCRE_LIO_ERASURE_TOOLS_H_INCLUDED

#include <lio/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

// Typedefs
typedef struct lio_erasure_plan_t lio_erasure_plan_t;

#define REED_SOL_VAN    0
#define REED_SOL_R6_OP  1
#define CAUCHY_ORIG     2
#define CAUCHY_GOOD     3
#define BLAUM_ROTH      4
#define LIBERATION      5
#define LIBER8TION      6
#define RAID4           7
#define N_JE_METHODS    8

// Functions
LIO_API int et_method_type(char *meth);
LIO_API lio_erasure_plan_t *et_new_plan(int method, long long int strip_size, int data_strips, int parity_strips, int w, int packet_size, int base_unit);
LIO_API lio_erasure_plan_t *et_generate_plan(long long int file_size, int method, int data_strips, int parity_strips, int w, int packet_low, int packet_high);
LIO_API void et_destroy_plan(lio_erasure_plan_t *plan);
LIO_API int et_encode(lio_erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size);
LIO_API int et_decode(lio_erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures);

#ifdef __cplusplus
}
#endif

#endif /* ^ ACCRE_LIO_ERASURE_TOOLS_H_INCLUDED ^ */ 
