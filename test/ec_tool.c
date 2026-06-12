#include<jerasure.h>
#include<jerasure/cauchy.h>
#include<isa-l/erasure_code.h>


int main(int argc, char **argv) {

    int k = 6;
    int p = 3;
    int m = k + p;
    int w = 8;

    int *jerasure_matrix = cauchy_original_coding_matrix(k, p, w);
    printf("JErasure matrix: \n");
    jerasure_print_matrix(jerasure_matrix, p, k, w);

    return 1;
}