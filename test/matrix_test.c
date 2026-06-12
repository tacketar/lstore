#include<jerasure/jerasure.h>
#include<jerasure/cauchy.h>
#include<isa-l/erasure_code.h>
#include<string.h>

int generate_6strip_pattern_file(const char *filename)
{
    const size_t strip_size = 16 * 1024;   // 16384 bytes
    const int num_strips = 6;
    const size_t total_size = (size_t)num_strips * strip_size;

    FILE *f = fopen(filename, "wb");
    if (!f) {
        perror("fopen failed");
        return 1;
    }

    unsigned char *strip_buf = malloc(strip_size);
    if (!strip_buf) {
        fclose(f);
        perror("malloc failed");
        return 1;
    }

    printf("\t Generating 96 KB pattern file (%d strips of 16 KB):\n", num_strips);

    for (int s = 0; s < num_strips; s++) {
        char ch = 'a' + s;                    // a, b, c, d, e, f

        memset(strip_buf, ch, strip_size);

        size_t written = fwrite(strip_buf, 1, strip_size, f);
        if (written != strip_size) {
            perror("fwrite failed");
            free(strip_buf);
            fclose(f);
            return 1;
        }

        printf("\t   Strip %d: filled with '%c' (%zu bytes)\n", s, ch, strip_size);
    }

    free(strip_buf);
    fclose(f);

    printf("\t Success: Created '%s' with exactly %zu bytes (6 × 16KB)\n", 
           filename, total_size);

    return 0;
}

int main(int argc, char **argv) {

    int k = 6;
    int p = 3;
    int m = k + p;
    int w = 8;

    int *jerasure_matrix = cauchy_original_coding_matrix(k, p, w);
    printf("JErasure encoding matrix: \n");
    jerasure_print_matrix(jerasure_matrix, p, k, w);

    int *jerasure_bitmatrix = jerasure_matrix_to_bitmatrix(k, p, w, jerasure_matrix);
    printf("\n JErasure bit matrix: \n");
    jerasure_print_matrix(jerasure_bitmatrix, m, k, w);

    unsigned char *isal_matrix;
    isal_matrix = malloc(m * k);
    gf_gen_cauchy1_matrix(isal_matrix, m, k);

    unsigned char *g_tbls = malloc(k * p * 32);
    ec_init_tables(k, p, &isal_matrix[k * k], g_tbls);
    printf("\n ISAL encoding matrix: \n");
    for(int x = 0; x < m; x++){
        for(int y = 0; y < k; y++){
            printf("%u, ",isal_matrix[x * k + y]);
        }
        printf("\n");
    }



    printf("\n ISAL g tbls: \n");
    for(int x = 0; x < p; x++){
        for(int y = 0; y < k; y++){
            printf("%u, ",g_tbls[x * k + y]);
        }
        printf("\n");
    }

    //if(generate_6strip_pattern_file("input_16KB.txt") == 0) printf("done\n");
    return 1;
}