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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <tbx/assert_result.h>
#include <tbx/log.h>
#include <tbx/io.h>

#include "erasure_tools_isal.h"

#define MMAX 255
#define KMAX 255

int bread(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    int i;
    size_t n = tbx_io_fread(ptr, size, nmemb, stream);

    if (n != nmemb) {
        i = (nmemb - n)*size;
        memset(ptr + n*size, BLANK_CHAR, i);
    }

    return(nmemb);
}

int form_encoding_matrix(lio_erasure_plan_t *plan)
{
    if (plan == NULL) return(-1);
    if (plan->method < 0 || plan->method >= N_ISA_METHODS) return(-1);
    if (plan->encode_matrix  != NULL) return(0);

    plan->encode_matrix = malloc((plan->parity_strips+plan->data_strips) * plan->data_strips);

    switch(plan->method){
        case REED_SOL_VAN_ISA:
            gf_gen_rs_matrix(plan->encode_matrix, plan->parity_strips+plan->data_strips, plan->data_strips);
            break;
        case CAUCHY_ORIG_ISA:
            gf_gen_cauchy1_matrix(plan->encode_matrix, plan->parity_strips+plan->data_strips, plan->data_strips);
            break;
        default:
            return -1;
    }

    if (plan->encode_matrix == NULL) return(-1);

    return(0);
}

int gen_g_tbls(lio_erasure_plan_t *plan){
    if (plan == NULL) return(-1);
    if (plan->encode_matrix  == NULL) return(-1);
    if (plan->g_tbls  != NULL) return(0);
    plan->g_tbls = malloc( plan->data_strips * plan->parity_strips * 32);
    ec_init_tables(plan->data_strips, plan->parity_strips, &plan->encode_matrix[plan->data_strips * plan->data_strips], plan->g_tbls);

    if(plan->g_tbls == NULL) return -1; else return 0;
}

lio_erasure_plan_t *et_generate_plan(long long int file_size,
                                    int method,
                                    int data_strips,
                                    int parity_strips,
                                    int w,
                                    int packet_low,
                                    int packet_high){

    int i, j, base_unit = 8;
    int packet_size;
    int packet_best;
    int plow, phigh;
    long long int strip_size, new_size, rem, smallest_excess, best_size;
    float increase;

    //** Tweak the packet search range based on the params
    strip_size = file_size / (w*base_unit*data_strips);
    log_printf(15, "Approximate max packet_size=%lld\n", strip_size);
    if (strip_size < 4*1024) {
        plow = strip_size/4;
        phigh = strip_size;
    } else {
        plow = 512;
        phigh = 4096;
    }

    if (packet_low < 0) packet_low = plow;
    if (packet_high < 0) packet_high = phigh;
    if (packet_low > packet_high) {
        printf("et_generate_plan: packet_low > packet_high!  packet_low=%d packet_high=%d\n", packet_low, packet_high);
        return(NULL);
    }


    //** Override w and packet_size if needed
    if (packet_low % base_unit != 0) packet_low = (packet_low /base_unit) * base_unit;
    if (packet_high % base_unit != 0) packet_high = (packet_high /base_unit) * base_unit;
    packet_size = packet_high;

    //** Tweak the packet search range based on the params
    strip_size = file_size / (w*base_unit*data_strips);
    log_printf(15, "Approximate max packet_size=%lld\n", strip_size);
    if (strip_size < 4*1024) {
        plow = strip_size/4;
        phigh = strip_size;
    } else {
        plow = 512;
        phigh = 4096;
    }

    if (packet_low < 0) packet_low = plow;
    if (packet_high < 0) packet_high = phigh;
    if (packet_low > packet_high) {
        printf("et_generate_plan: packet_low > packet_high!  packet_low=%d packet_high=%d\n", packet_low, packet_high);
        return(NULL);
    }


    //** Override w and packet_size if needed
    if (packet_low % base_unit != 0) packet_low = (packet_low /base_unit) * base_unit;
    if (packet_high % base_unit != 0) packet_high = (packet_high /base_unit) * base_unit;
    packet_size = packet_high;

    //** Determine strip size
    log_printf(15, "packet_low=%d packet_high=%d\n", packet_low, packet_high);
    smallest_excess = 10*file_size;
    packet_best = -1;

    for (packet_size = packet_high; packet_size > packet_low; packet_size = packet_size - base_unit) {
        i = data_strips*w*packet_size*base_unit;
        new_size = file_size;
        rem = new_size % i;
        if (rem > 0) new_size = new_size + (i-rem);
        rem = new_size % (data_strips*w*packet_size*base_unit);
        if (rem > 0) printf("ERROR with new size!!!!! new_Size=%lld rem=%lld\n", new_size, rem);

        j = new_size - file_size;
        if (j <= smallest_excess) {
            smallest_excess = j;
            packet_best = packet_size;
            best_size = new_size;
            increase = (1.0*j) / file_size * 100;
            if (increase < 1) break;
        }
    }

    packet_size = packet_best;
    new_size = best_size;
    strip_size = new_size / data_strips;
    i = data_strips*w*packet_size*base_unit;
    j = new_size - file_size;
    increase = (1.0*j) / file_size * 100;
    log_printf(15, "Best Divisor=%d New size=%lld Orig size=%lld increase=%d (%f%%)\n", i, new_size, file_size, j, increase);
    return(et_new_plan(method, strip_size, data_strips, parity_strips, w, packet_size, base_unit));
}

lio_erasure_plan_t *et_new_plan(int method,
                                long long int strip_size,
                                int data_strips,
                                int parity_strips,
                                int w,
                                int packet_size,
                                int base_unit) {
    if (method >= N_ISA_METHODS) {
        printf("et_new_plan: Invalid method!  method=%d\n", method);
        return(NULL);
    }

    lio_erasure_plan_t *plan = (lio_erasure_plan_t *)malloc(sizeof(lio_erasure_plan_t));
    FATAL_UNLESS(plan != NULL);

    plan->method = method;
    plan->strip_size = strip_size;
    plan->data_strips = data_strips;
    plan->parity_strips = parity_strips;
    plan->w = w;
    plan->base_unit = base_unit;
    plan->packet_size = packet_size;
    plan->encode_matrix = NULL;
    plan->g_tbls = NULL;
    plan->form_encoding_matrix = form_encoding_matrix;
    plan->gen_g_tbls = gen_g_tbls;
    if( plan->form_encoding_matrix(plan) == -1) return NULL;
    if( plan->gen_g_tbls(plan) == -1) return NULL;
    return plan;
}

void et_destroy_plan(lio_erasure_plan_t *plan)
{
    if (plan->encode_matrix != NULL) free(plan->encode_matrix);
    free(plan);
}

static void print_some_g_tbls(const lio_erasure_plan_t *plan, int max_tables)
{
    if (!plan || !plan->g_tbls) {
        printf("g_tbls not allocated\n");
        return;
    }

    int k = plan->data_strips;
    int m = plan->parity_strips;
    int tables_to_show = (max_tables < 0) ? (k * m) : max_tables;
    if (tables_to_show > k * m) tables_to_show = k * m;

    printf("\nShowing first %d multiplication table(s) from g_tbls:\n", tables_to_show);
    printf("   (each table = 32 bytes = mul by one fixed GF element)\n\n");

    const uint8_t *tbl = plan->g_tbls;

    for (int t = 0; t < tables_to_show; t++) {
        // Which matrix element does this table correspond to?
        int row    = t / k;           // 0 .. m-1   (parity row)
        int col    = t % k;           // 0 .. k-1   (data column)
        uint8_t a  = plan->encode_matrix[row * k + col];

        printf("Table %3d  (parity %d ← data %d)   multiplier = 0x%02x\n", t, row, col, a);

        for (int i = 0; i < 32; i++) {
            if (i % 16 == 0) printf("  ");
            printf("%02x ", tbl[i]);
            if (i % 7 == 6) printf(" ");
        }
        printf("\n\n");

        tbl += 32;
    }
}

static int
gf_gen_decode_matrix_simple(unsigned char *encode_matrix,
                            unsigned char *decode_matrix, unsigned char *invert_matrix,
                            unsigned char *temp_matrix,
                            unsigned char *decode_index,
                            unsigned char *frag_err_list,
                            int nerrs,
                            int k,
                            int m) {
        int i, j, p, r;
        unsigned char s, *b = temp_matrix;
        unsigned char frag_in_err[MMAX];

        memset(frag_in_err, 0, sizeof(frag_in_err));

        // Mark the input fragments with error for later processing
        for (i = 0; i < nerrs; i++)
                frag_in_err[frag_err_list[i]] = 1;

        // Construct b (matrix that encoded remaining frags) by removing erased rows
        for (i = 0, r = 0; i < k; i++, r++) {
                while (frag_in_err[r])
                        r++;
                for (j = 0; j < k; j++)
                        b[k * i + j] = encode_matrix[k * r + j];
                decode_index[i] = r;
        }

        // Invert matrix to get recovery matrix
        if (gf_invert_matrix(b, invert_matrix, k) < 0)
                return -1;

        // Get decode matrix with only wanted recovery rows
        for (i = 0; i < nerrs; i++) {
                if (frag_err_list[i] < k) // A src err
                        for (j = 0; j < k; j++)
                                decode_matrix[k * i + j] = invert_matrix[k * frag_err_list[i] + j];
        }

        // For non-src (parity) erasures need to multiply encode matrix * invert
        for (p = 0; p < nerrs; p++) {
                if (frag_err_list[p] >= k) { // A parity err
                        for (i = 0; i < k; i++) {
                                s = 0;
                                for (j = 0; j < k; j++)
                                        s ^= gf_mul(invert_matrix[j * k + i],
                                                    encode_matrix[k * frag_err_list[p] + j]);
                                decode_matrix[k * p + i] = s;
                        }
                }
        }
        return 0;
}

static void print_plan(lio_erasure_plan_t *plan){
    if(plan == NULL) return;
    printf("Erasure plan: \n");
    printf("Strip size: %lld\n",plan->strip_size);
    if(plan->method >0 && plan->method < N_ISA_METHODS){
        switch(plan->method){
            case REED_SOL_VAN_ISA:
                printf("Method: REED SOL\n");
                break;
            case CAUCHY_ORIG_ISA:
                printf("Method: CAUCHY ORIG\n");
                break;
            default:
                printf("Method: UNKNOWN");
        }
    }
    printf("No. of data strips: %d\n",plan->data_strips);
    printf("No. of parity strips: %d\n",plan->parity_strips);
    printf("Word size: %d\n",plan->w);
    printf("Packet size: %d\n",plan->packet_size);
    printf("Base unit: %d\n",plan->base_unit);
    if (plan->encode_matrix) {
        printf("Encoding Matrix (%d x %d):\n", plan->parity_strips, plan->data_strips);
        for (int i = 0; i < plan->parity_strips; i++) {
            for (int j = 0; j < plan->data_strips; j++) {
                printf("%3d ", plan->encode_matrix[i * plan->data_strips + j]);
            }
            printf("\n");
        }
    } else {
        printf("Encoding Matrix: Not available\n");
    }
    if(plan->g_tbls){
        print_some_g_tbls(plan, 4);
    } else {
        printf("GF tables are not available\n");
    }
}

int et_encode(lio_erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size)
{
    FILE *fd_file, *fd_parity;
    unsigned char **ptr, **data, **parity, *buffer;
    int i, block_size, bsize;
    long long int rpos, apos, bpos, ppos, file_size;

    // Open the files
    fd_file = tbx_io_fopen(fname, "r");
    if (fd_file == NULL) {
        printf("et_encode: Error opening data file %s\n", fname);
        return(1);
    }

    // Get the file size
    tbx_io_fseek(fd_file, 1, SEEK_END);
    file_size = tbx_io_ftell(fd_file);
    tbx_io_fseek(fd_file, 0, SEEK_SET);

    fd_parity = tbx_io_fopen(pname, "r+");
    if (fd_parity == NULL) {
        fd_parity = tbx_io_fopen(pname, "w");  // Doesn't exist so create it.
    }
    if (fd_parity == NULL) {
        printf("et_encode: Error opening parity file %s\n", pname);
        return(1);
    }

    plan->form_encoding_matrix(plan);  // Form the matrix if needed

    // Determine the buffersize
    buffer_size = (plan->data_strips+plan->parity_strips)*plan->w*plan->packet_size*plan->base_unit;
    block_size = buffer_size/(plan->data_strips + plan->parity_strips);

    //printf("HELLO:  buffer_size=%d block_size=%d strip_size=%lld\n", buffer_size, block_size, plan->strip_size);
    // allocate the buffer space
    ptr = (unsigned char **)malloc(sizeof(unsigned char *)*(plan->data_strips + plan->parity_strips));
    FATAL_UNLESS(ptr != NULL);

    buffer = (unsigned char *)malloc(sizeof(unsigned char)*buffer_size);
    FATAL_UNLESS(buffer != NULL);

    for (i=0; i < (plan->data_strips+plan->parity_strips); i++) {
        ptr[i] = &(buffer[i*block_size]);
    }
    data = &(ptr[0]);
    parity = &(ptr[plan->data_strips]);

    // Perform the encoding
    apos = foffset;
    ppos = poffset;
    rpos = 0;
    log_printf(15, "et_encode: strip_size=%lld buffer_size=%d block_size=%d\n", plan->strip_size, buffer_size, block_size);
    while (apos < file_size) {
        bsize = block_size;
        log_printf(15, "et_encode: rpos = %lld strip_size=%lld buffer_size=%d block_size=%d bsize=%d\n", rpos, plan->strip_size, buffer_size, block_size, bsize);

        // Read the data
        bpos = apos;
        for (i=0; i<plan->data_strips; i++) {
            tbx_io_fseek(fd_file, bpos, SEEK_SET);
            bread(data[i], 1, bsize, fd_file);
            bpos = bpos + plan->strip_size;
        }
        apos = bpos;

        // Perform the encoding
        //plan->encode_block(plan, ptr, bsize);
        ec_encode_data(bsize, plan->data_strips, plan->parity_strips, plan->g_tbls,data,parity);

        // Store the parity
        bpos = ppos;
        bsize = block_size;
        for (i=0; i<plan->parity_strips; i++) {
            tbx_io_fseek(fd_parity, bpos, SEEK_SET);
            tbx_io_fwrite(parity[i], 1, bsize, fd_parity);
            bpos = bpos + plan->strip_size;
        }
        ppos = bpos;
    }
    //** Free the ptrs
    free(ptr);
    free(buffer);

    tbx_io_fclose(fd_file);
    tbx_io_fclose(fd_parity);

    return(0);
}

int et_decode(lio_erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures)
{
    FILE *fd_file, *fd_parity;
    unsigned char **ptr, **data, **parity, *buffer;
    int i, block_size, bsize, msize, nbytes;
    unsigned char *missing, *missing_data, *missing_parity;
    long long int rpos, apos, bpos, ppos;

    //** Make the missing tables
    msize = sizeof(char) * (plan->data_strips+plan->parity_strips);
    missing = malloc(msize);
    memset(missing, 0, msize);
    missing_data = missing;
    missing_parity = &(missing[plan->data_strips]);
    i = 0;
    unsigned char frag_err_list[MMAX];
    while (erasures[i] != -1) {
        missing[erasures[i]] = 1;
        frag_err_list[i] = (unsigned)erasures[i];
        i++;
    }

    if (i == 0) return(0);  //**Nothing missing so exit
    int nerrs = i;
    printf("Nerrs: %d\n",nerrs);

    //** Open the files
    fd_file = tbx_io_fopen(fname, "r+");
    if (fd_file == NULL) {
        printf("et_decode: Error opening data file %s\n", fname);
        return(1);
    }

    fd_parity = tbx_io_fopen(pname, "r+");
    if (fd_parity == NULL) {
        printf("et_decode: Error opening parity file %s\n", pname);
        return(1);
    }

    //plan->form_decoding_matrix(plan);  //** Form the matrix if needed

    unsigned char *decode_matrix, *invert_matrix, *temp_matrix;
    unsigned char decode_index[MMAX];
    decode_matrix = malloc( (plan->data_strips+plan->parity_strips) * plan->data_strips);
    invert_matrix = malloc((plan->data_strips+plan->parity_strips) * plan->data_strips);
    temp_matrix = malloc((plan->data_strips+plan->parity_strips) * plan->data_strips);

    int ret = gf_gen_decode_matrix_simple(plan->encode_matrix, decode_matrix,invert_matrix, temp_matrix, decode_index,frag_err_list,nerrs,plan->data_strips,plan->data_strips+plan->parity_strips);
    if (ret != 0) {
        printf("Fail on generate decode matrix\n");
        return -1;
    }

    ec_init_tables(plan->data_strips,nerrs,decode_matrix,plan->g_tbls);

    //** Determine the buffersize
    buffer_size = (plan->data_strips+plan->parity_strips)*plan->w*plan->packet_size*plan->base_unit;

    block_size = buffer_size/(plan->data_strips + plan->parity_strips);

//printf("HELLO:  buffer_size=%d block_size=%d strip_size=%lld\n", buffer_size, block_size, plan->strip_size);

    //** allocate the buffer space
    ptr = (unsigned char **)malloc(sizeof(unsigned char *)*(plan->data_strips + plan->parity_strips));
    FATAL_UNLESS(ptr != NULL);

    buffer = (unsigned char *)malloc(sizeof(unsigned char)*buffer_size);
    FATAL_UNLESS(buffer != NULL);

    for (i=0; i < (plan->data_strips+plan->parity_strips); i++) {
        ptr[i] = &(buffer[i*block_size]);
    }
    data = &(ptr[0]);
    parity = &(ptr[plan->data_strips]);

    //** Perform the decoding
    apos = foffset;
    ppos = poffset;
    rpos = 0;
    log_printf(1, "et_decode: strip_size=%lld buffer_size=%d block_size=%d\n", plan->strip_size, buffer_size, block_size);
    while (apos < fsize) {
        bsize = block_size;

        log_printf(1, "et_decode: rpos = %lld strip_size=%lld buffer_size=%d block_size=%d bsize=%d\n", rpos, plan->strip_size, buffer_size, block_size, bsize);

        //** Read the data
        bpos = apos;
        for (i=0; i<plan->data_strips; i++) {
            if (missing_data[i] == 0) {
                tbx_io_fseek(fd_file, bpos, SEEK_SET);
                bread(data[i], 1, bsize, fd_file);
            }
            bpos = bpos + plan->strip_size;
        }

        bpos = ppos;
        for (i=0; i<plan->parity_strips; i++) {
            if (missing_parity[i] == 0) {
                tbx_io_fseek(fd_parity, bpos, SEEK_SET);
                if (tbx_io_fread(parity[i], 1, bsize, fd_parity) != (size_t)bsize) abort();
            }
            bpos = bpos + plan->strip_size;
        }
        ppos = bpos;

        unsigned char *recover_srcs[MMAX], *recover_outp[MMAX];
        for (i = 0; i < plan->parity_strips; i++) {
                if (NULL == (recover_outp[i] = malloc(bsize))) {
                        printf("alloc error: Fail\n");
                        return -1;
                }
        }

        for (i = 0; i < plan->data_strips; i++)
            recover_srcs[i] = data[decode_index[i]];

        // Perform the decoding
        //plan->decode_block(plan, ptr, bsize, erasures);
        ec_encode_data(bsize,plan->data_strips,nerrs,plan->g_tbls,recover_srcs,recover_outp);

        /*
        printf("The recovery blocks: \n");
        for (i=0; i<nerrs; i++){
           printf("%s \n",recover_outp[i]);
        }

        printf(" check recovery of block {");
        for (i = 0; i < nerrs; i++) {
                printf(" %d", frag_err_list[i]);
                if (memcmp(recover_outp[i], data[frag_err_list[i]], bsize)) {
                        printf(" Fail erasure recovery %d, frag %d\n", i, frag_err_list[i]);
                        printf("Data in buffer pointer: %s ",data[frag_err_list[i]]);
                        printf("Data in recovery output: %s",recover_outp[i]);
                        return -1;
                }
        }*/

        // Store the missing blocks
        bpos = apos;
        for (i=0; i<plan->data_strips; i++) {  // Skip the last block
            if (missing_data[i] == 1) {
                nbytes = (bpos + bsize) > fsize ? fsize - bpos : block_size;
                if (nbytes < 0) break;
                tbx_io_fseek(fd_file, bpos, SEEK_SET);
                for(int j=0; j<nerrs; j++){
                    if(i == frag_err_list[j])
                    tbx_io_fwrite(recover_outp[j], 1, nbytes, fd_file);
                }
            }
            bpos = bpos + plan->strip_size;
        }

        //** Update the file positions
        apos = apos + plan->data_strips * block_size;
    }

    //** Free the ptrs
    free(ptr);
    free(buffer);
    free(missing);

    tbx_io_fclose(fd_file);
    tbx_io_fclose(fd_parity);

    return(0);
}