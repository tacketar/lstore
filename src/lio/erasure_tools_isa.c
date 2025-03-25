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

#define _log_module_index 179

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <isa-l/erasure_code.h>  // isa-l header
#include "erasure_tools_isa.h"  // Updated struct definition

// Encoding method constants (from original)
#define ET_METHOD_REED_SOLOMON 0
#define ET_METHOD_CAUCHY_ORIGINAL 1  // Using this as Cauchy 1


static void erasure_print_plan_internal(lio_erasure_plan_t *plan)
{
    printf("Erasure plan:\n");
    printf("  strip_size: %lld\n", plan->strip_size);
    printf("  method: %s\n", plan->method == ET_METHOD_REED_SOLOMON ? "Reed-Solomon" : "Cauchy Original");
    printf("  data_strips: %d\n", plan->data_strips);
    printf("  parity_strips: %d\n", plan->parity_strips);
    printf("  w (word_size): %d\n", plan->w);
    printf("  packet_size: %d\n", plan->packet_size);
    printf("  base_unit: %d\n", plan->base_unit);
}


static int form_encoding_matrix(lio_erasure_plan_t *plan)
{
    if (plan->method == ET_METHOD_REED_SOLOMON) {
        gf_gen_rs_matrix((uint8_t *)plan->encode_bitmatrix, plan->data_strips + plan->parity_strips, plan->data_strips);
    } else if (plan->method == ET_METHOD_CAUCHY_ORIGINAL) {
        gf_gen_cauchy1_matrix((uint8_t *)plan->encode_bitmatrix, plan->data_strips + plan->parity_strips, plan->data_strips);
    } else {
        return -1;  // Invalid method
    }
    return 0;
}

static int form_decoding_matrix(lio_erasure_plan_t *plan)
{
    return 0;  // No precomputed decoding matrix needed
}

static void encode_block(lio_erasure_plan_t *plan, char **ptr, int block_size)
{
    uint8_t *tmp_data[plan->data_strips];
    uint8_t *tmp_parity[plan->parity_strips];

    for (int i = 0; i < plan->data_strips; i++) tmp_data[i] = (uint8_t *)ptr[i];
    for (int i = 0; i < plan->parity_strips; i++) tmp_parity[i] = (uint8_t *)ptr[i + plan->data_strips];

    ec_encode_data(block_size, plan->data_strips, plan->parity_strips, (uint8_t *)plan->encode_bitmatrix,
                   tmp_data, tmp_parity);
}

static int decode_block(lio_erasure_plan_t *plan, char **ptr, int block_size, int *erasures)
{
    uint8_t *tmp_data[plan->data_strips];
    uint8_t *recover_out[plan->parity_strips];
    uint8_t decode_matrix[plan->data_strips * plan->data_strips];
    uint8_t src_matrix[plan->data_strips * (plan->data_strips + plan->parity_strips)];
    int nerasures = 0;
    int src_ids[plan->data_strips];

    for (int i = 0; i < plan->data_strips + plan->parity_strips; i++) {
        if (erasures[i] != -1) nerasures++;
    }
    int src_idx = 0, out_idx = 0;
    for (int i = 0; i < plan->data_strips; i++) {
        if (erasures[i] == -1) {
            tmp_data[src_idx] = (uint8_t *)ptr[i];
            src_ids[src_idx] = i;
            src_idx++;
        } else {
            recover_out[out_idx] = (uint8_t *)ptr[i];
            out_idx++;
        }
    }
    for (int i = 0; i < plan->parity_strips; i++) {
        if (erasures[i + plan->data_strips] == -1) {
            tmp_data[src_idx] = (uint8_t *)ptr[i + plan->data_strips];
            src_ids[src_idx] = i + plan->data_strips;
            src_idx++;
        } else {
            recover_out[out_idx] = (uint8_t *)ptr[i + plan->data_strips];
            out_idx++;
        }
    }

    uint8_t *encode_matrix = (uint8_t *)plan->encode_bitmatrix;
    for (int i = 0; i < plan->data_strips; i++) {
        for (int j = 0; j < plan->data_strips; j++) {
            src_matrix[i * plan->data_strips + j] = encode_matrix[src_ids[i] * plan->data_strips + j];
        }
    }

    if (gf_invert_matrix(src_matrix, decode_matrix, plan->data_strips) != 0) {
        return -1;  // Singular matrix
    }

    ec_encode_data(block_size, plan->data_strips, nerasures, decode_matrix, tmp_data, recover_out);

    return 0;
}


// Generate an erasure plan with isa-l
lio_erasure_plan_t *et_generate_plan(long long int file_size, int method,
                                     int data_strips, int parity_strips, int w, int packet_low, int packet_high)
{
    lio_erasure_plan_t *plan;

    plan = (lio_erasure_plan_t *)malloc(sizeof(lio_erasure_plan_t));
    if (!plan) return NULL;

    assert(data_strips + parity_strips <= 32); // isa-l limit

    plan->strip_size = file_size / data_strips;
    plan->method = method;
    plan->data_strips = data_strips;
    plan->parity_strips = parity_strips;
    plan->w = w;
    plan->packet_size = (packet_high > packet_low) ? packet_high : packet_low;
    plan->base_unit = sizeof(long);
    plan->encode_matrix = NULL;
    plan->encode_bitmatrix = (int *)malloc(data_strips * parity_strips * 32);
    if (!plan->encode_bitmatrix) {
        free(plan);
        return NULL;
    }
    plan->encode_schedule = NULL;

    plan->form_encoding_matrix = form_encoding_matrix;
    plan->form_decoding_matrix = form_decoding_matrix;
    plan->encode_block = encode_block;
    plan->decode_block = decode_block;
    plan->erasure_print_plan = erasure_print_plan_internal;  // Set struct pointer

    plan->form_encoding_matrix(plan);

    return plan;
}

// Simplified plan creation matching header declaration
lio_erasure_plan_t *et_new_plan(int method, long long int strip_size, int data_strips, int parity_strips, int w, int packet_size, int base_unit)
{
    lio_erasure_plan_t *plan = et_generate_plan(strip_size * data_strips, method, data_strips, parity_strips, w, packet_size, packet_size);
    if (plan) {
        plan->strip_size = strip_size;
        plan->packet_size = packet_size;
        plan->base_unit = base_unit;
    }
    return plan;
}

// Free the erasure plan
void et_destroy_plan(lio_erasure_plan_t *plan)
{
    if (plan->encode_bitmatrix) free(plan->encode_bitmatrix);
    free(plan);
}

// Encode data into parity
int et_encode(lio_erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size)
{
    char *data = (char *)malloc(plan->data_strips * buffer_size);
    char *parity = (char *)malloc(plan->parity_strips * buffer_size);
    if (!data || !parity) {
        free(data);
        free(parity);
        return -1;
    }

    // Simulate reading from file offsets (dummy data for testing)
    for (int i = 0; i < plan->data_strips; i++) {
        memset(data + (i * buffer_size), 'A' + i, buffer_size);
    }
    memset(parity, 0, plan->parity_strips * buffer_size);

    // Split buffers into ptr array
    char *ptr[plan->data_strips + plan->parity_strips];
    for (int i = 0; i < plan->data_strips; i++) {
        ptr[i] = data + (i * buffer_size);
    }
    for (int i = 0; i < plan->parity_strips; i++) {
        ptr[i + plan->data_strips] = parity + (i * buffer_size);
    }

    plan->encode_block(plan, ptr, buffer_size);

    free(data);
    free(parity);
    return 0;
}

// Decode data
int et_decode(lio_erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures)
{
    char *data = (char *)malloc(plan->data_strips * buffer_size);
    char *parity = (char *)malloc(plan->parity_strips * buffer_size);
    if (!data || !parity) {
        free(data);
        free(parity);
        return -1;
    }

    // Simulate reading from file offsets (dummy data with erasures)
    for (int i = 0; i < plan->data_strips; i++) {
        memset(data + (i * buffer_size), 'A' + i, buffer_size);
    }
    memset(parity, 0, plan->parity_strips * buffer_size);
    for (int i = 0; i < plan->data_strips + plan->parity_strips; i++) {
        if (erasures[i] != -1) {
            if (i < plan->data_strips) {
                memset(data + (i * buffer_size), 0, buffer_size);
            } else {
                memset(parity + ((i - plan->data_strips) * buffer_size), 0, buffer_size);
            }
        }
    }

    // Split buffers into ptr array
    char *ptr[plan->data_strips + plan->parity_strips];
    for (int i = 0; i < plan->data_strips; i++) {
        ptr[i] = data + (i * buffer_size);
    }
    for (int i = 0; i < plan->parity_strips; i++) {
        ptr[i + plan->data_strips] = parity + (i * buffer_size);
    }

    int result = plan->decode_block(plan, ptr, buffer_size, erasures);

    free(data);
    free(parity);
    return result;
}

// --- isa-l Implementations ---




