#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "erasure_tools_isa.h"

#define DATA_STRIPS 4
#define PARITY_STRIPS 2
#define BLOCK_SIZE 1024
#define WORD_SIZE 8
#define FILE_SIZE 1048576  // 1MB
#define PACKET_SIZE 16384
#define BASE_UNIT sizeof(long)

void print_buffer(char *buf, int len, const char *label) {
    printf("%s: ", label);
    for (int i = 0; i < len && i < 16; i++) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
}

int main() {
    printf("\n=== Testing ISAL (erasure_tools_isa.h) ===\n");

    lio_erasure_plan_t *plan = et_new_plan(1, FILE_SIZE / DATA_STRIPS,
                                           DATA_STRIPS, PARITY_STRIPS, WORD_SIZE, PACKET_SIZE, BASE_UNIT);
    if (!plan) {
        printf("Failed to create plan\n");
        return 1;
    }
    //plan->erasure_print_plan(plan);

    char *data = (char *)malloc(DATA_STRIPS * BLOCK_SIZE);
    char *parity = (char *)malloc(PARITY_STRIPS * BLOCK_SIZE);
    char *original_data = (char *)malloc(DATA_STRIPS * BLOCK_SIZE);

    for (int i = 0; i < DATA_STRIPS; i++) {
        memset(data + (i * BLOCK_SIZE), 'A' + i, BLOCK_SIZE);
    }
    memcpy(original_data, data, DATA_STRIPS * BLOCK_SIZE);
    memset(parity, 0, PARITY_STRIPS * BLOCK_SIZE);

    if (et_encode(plan, "data_file", 0, "parity_file", 0, BLOCK_SIZE) != 0) {
        printf("Encoding failed\n");
        goto cleanup;
    }
    printf("Encoded successfully\n");
    for (int i = 0; i < DATA_STRIPS; i++) print_buffer(data + (i * BLOCK_SIZE), BLOCK_SIZE, "Data");
    for (int i = 0; i < PARITY_STRIPS; i++) print_buffer(parity + (i * BLOCK_SIZE), BLOCK_SIZE, "Parity");

    int erasures[DATA_STRIPS + PARITY_STRIPS] = {-1, -1, -1, -1, -1, -1};
    erasures[0] = 0;  // Lose data[0]
    erasures[5] = 5;  // Lose parity[1]
    memset(data, 0, BLOCK_SIZE);
    memset(parity + BLOCK_SIZE, 0, BLOCK_SIZE);

    if (et_decode(plan, FILE_SIZE, "data_file", 0, "parity_file", 0, BLOCK_SIZE, erasures) != 0) {
        printf("Decoding failed\n");
        goto cleanup;
    }
    printf("Decoded successfully\n");
    for (int i = 0; i < DATA_STRIPS; i++) print_buffer(data + (i * BLOCK_SIZE), BLOCK_SIZE, "Recovered Data");
    for (int i = 0; i < PARITY_STRIPS; i++) print_buffer(parity + (i * BLOCK_SIZE), BLOCK_SIZE, "Recovered Parity");

    int success = 1;
    if (memcmp(data, original_data, DATA_STRIPS * BLOCK_SIZE) != 0) {
        printf("Data mismatch\n");
        success = 0;
    }
    printf("Verification: %s\n", success ? "PASSED" : "FAILED");

cleanup:
    free(data);
    free(parity);
    free(original_data);
    et_destroy_plan(plan);
    return success ? 0 : 1;
}
