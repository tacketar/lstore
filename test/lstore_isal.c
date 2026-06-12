/*
 * lstore-isal - Command line tool using the pure ISAL erasure backend.
 *
 * This binary is built by compiling erasure_tools_isal.c with symbol renaming
 * (et_* → isal_et_*) so it can coexist with lio (which may bring Jerasure symbols).
 *
 * The tool itself calls the renamed isal_et_* functions.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <tbx/constructor.h>

// We only need the declarations for the renamed ISAL symbols.
// The actual implementation comes from compiling src/lio/erasure_tools_isal.c
// with the COMPILE_DEFINITIONS set in CMakeLists.txt.
typedef struct isal_lio_erasure_plan_t isal_lio_erasure_plan_t;

// Mirror of the common prefix of the ISAL plan struct so we can read fields
// (strip_size, packet_size, base_unit, etc.) without including the full header.
typedef struct {
    long long int strip_size;
    int method;
    int data_strips;
    int parity_strips;
    int w;
    int packet_size;
    int base_unit;
} isal_plan_layout_t;

isal_lio_erasure_plan_t *isal_et_new_plan(int method,
                                long long int strip_size,
                                int data_strips,
                                int parity_strips,
                                int w,
                                int packet_size,
                                int base_unit);
isal_lio_erasure_plan_t *isal_et_generate_plan(long long int file_size, int method, int data_strips, int parity_strips, int w, int packet_low, int packet_high);
int isal_et_encode(isal_lio_erasure_plan_t *plan, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size);
int isal_et_decode(isal_lio_erasure_plan_t *plan, long long int fsize, const char *fname, long long int foffset, const char *pname, long long int poffset, int buffer_size, int *erasures);
void isal_et_destroy_plan(isal_lio_erasure_plan_t *plan);

#define DEFAULT_INPUT     "1GB_random.bin"
#define DEFAULT_PARITY    "1GB_random.bin.parity"
#define DEFAULT_ERASED    "1GB_random_erased.bin"
#define DEFAULT_RECOVERED "1GB_random_recovered.bin"

#define DEFAULT_K       6
#define DEFAULT_M       3
#define DEFAULT_W       8
#define DEFAULT_PACKET  -1
#define DEFAULT_BASE    8
#define DEFAULT_METHOD  1   /* CAUCHY_ORIG_ISA */

static const int default_wipe_data[] = {2, 4};
static const int default_wipe_parity[] = {2};
#define DEFAULT_N_WIPE_DATA   2
#define DEFAULT_N_WIPE_PARITY 1

static void usage(const char *prog) {
    fprintf(stderr,
        "lstore-isal - Pure ISAL erasure coding tool (replacement backend)\n\n"
        "Encode:\n"
        "  %s [--default] encode <input> <parity> <k> <m> [w=8] [method=1] [strip=16k] [packet=-1] [base=-1]\n\n"
        "Decode:\n"
        "  %s [--default] decode <erased_data> <parity> <output> <k> <m> <w> <packet> <base> <strip> <n_miss> <idx1> [idx2 ...]\n\n"
        "--default: use %s with k=%d m=%d w=%d (single-iteration layout matching test_isal_jerasure)\n"
        "  encode writes %s; decode wipes data blocks 2,4 and parity block 2 then recovers to %s\n\n"
        "method: 1 = CAUCHY_ORIG_ISA (recommended for cross-compat)\n"
        "Indices: 0..(k-1) data, k..(k+m-1) parity\n",
        prog, prog,
        DEFAULT_INPUT, DEFAULT_K, DEFAULT_M, DEFAULT_W,
        DEFAULT_PARITY, DEFAULT_RECOVERED);
    exit(1);
}

static long long get_file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return -1;
    return st.st_size;
}

static int require_file(const char *path) {
    if (get_file_size(path) < 0) {
        fprintf(stderr, "Error: required file '%s' not found\n", path);
        return 1;
    }
    return 0;
}

/* Single-iteration layout: block_size == strip_size (see test_isal_jerasure 1 GiB section). */
static void default_layout(long long fsize, int k, int w, int base_unit,
                           long long *strip, int *packet) {
    long long raw_strip = fsize / k;
    *packet = (int)(raw_strip / (w * base_unit));
    long long blk = (long long)w * (*packet) * base_unit;
    *strip = blk;
}

static isal_lio_erasure_plan_t *default_plan(long long fsize) {
    long long strip;
    int packet;
    int base = DEFAULT_BASE;
    default_layout(fsize, DEFAULT_K, DEFAULT_W, base, &strip, &packet);
    return isal_et_new_plan(DEFAULT_METHOD, strip, DEFAULT_K, DEFAULT_M,
                            DEFAULT_W, packet, base);
}

static int copy_file(const char *src_path, const char *dst_path) {
    FILE *src = fopen(src_path, "rb");
    FILE *dst = fopen(dst_path, "wb");
    if (!src || !dst) {
        perror("copy_file");
        if (src) fclose(src);
        if (dst) fclose(dst);
        return 1;
    }

    char buf[1024 * 1024];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), src)) > 0) {
        if (fwrite(buf, 1, n, dst) != n) {
            perror("copy_file write");
            fclose(src);
            fclose(dst);
            return 1;
        }
    }

    fclose(src);
    fclose(dst);
    return 0;
}

static int wipe_blocks(isal_plan_layout_t *plan,
                       const char *fname, long long int foffset,
                       const char *pname, long long int poffset,
                       int num_wipe_file, int num_wipe_parity,
                       const int *indices_wipe_file,
                       const int *indices_wipe_parity) {
    size_t strip_size = (size_t)plan->strip_size;
    int k = plan->data_strips;
    int m = plan->parity_strips;
    unsigned char *strip_buf = malloc(strip_size);
    if (!strip_buf) {
        perror("malloc strip buffer");
        return 1;
    }

    FILE *fp_data = fopen(fname, "r+b");
    if (!fp_data) {
        perror("fopen data file");
        free(strip_buf);
        return 1;
    }
    if (fseek(fp_data, foffset, SEEK_SET) != 0) {
        perror("fseek data file");
        fclose(fp_data);
        free(strip_buf);
        return 1;
    }

    while (1) {
        for (int i = 0; i < k; i++) {
            long long strip_start = ftell(fp_data);
            size_t nread = fread(strip_buf, 1, strip_size, fp_data);
            if (nread == 0) {
                if (feof(fp_data)) goto data_done;
                perror("fread data strip");
                fclose(fp_data);
                free(strip_buf);
                return 1;
            }
            if (nread != strip_size) {
                fprintf(stderr, "Short read in data strip %d\n", i);
                fclose(fp_data);
                free(strip_buf);
                return 1;
            }

            int should_wipe = 0;
            for (int j = 0; j < num_wipe_file; j++) {
                if (i == indices_wipe_file[j]) {
                    should_wipe = 1;
                    break;
                }
            }
            if (should_wipe) {
                memset(strip_buf, 0xFF, strip_size);
            }

            if (fseek(fp_data, strip_start, SEEK_SET) != 0 ||
                fwrite(strip_buf, 1, strip_size, fp_data) != strip_size ||
                fseek(fp_data, strip_start + (long long)strip_size, SEEK_SET) != 0) {
                perror("rewrite data strip");
                fclose(fp_data);
                free(strip_buf);
                return 1;
            }
        }
    }

data_done:
    fclose(fp_data);

    FILE *fp_parity = fopen(pname, "r+b");
    if (!fp_parity) {
        perror("fopen parity file");
        free(strip_buf);
        return 1;
    }
    if (fseek(fp_parity, poffset, SEEK_SET) != 0) {
        perror("fseek parity file");
        fclose(fp_parity);
        free(strip_buf);
        return 1;
    }

    while (1) {
        for (int i = 0; i < m; i++) {
            long long strip_start = ftell(fp_parity);
            size_t nread = fread(strip_buf, 1, strip_size, fp_parity);
            if (nread == 0) {
                if (feof(fp_parity)) goto parity_done;
                perror("fread parity strip");
                fclose(fp_parity);
                free(strip_buf);
                return 1;
            }
            if (nread != strip_size) {
                fprintf(stderr, "Short read in parity strip %d\n", i);
                fclose(fp_parity);
                free(strip_buf);
                return 1;
            }

            int should_wipe = 0;
            for (int j = 0; j < num_wipe_parity; j++) {
                if (i == indices_wipe_parity[j]) {
                    should_wipe = 1;
                    break;
                }
            }
            if (should_wipe) {
                memset(strip_buf, 0xFF, strip_size);
            }

            if (fseek(fp_parity, strip_start, SEEK_SET) != 0 ||
                fwrite(strip_buf, 1, strip_size, fp_parity) != strip_size ||
                fseek(fp_parity, strip_start + (long long)strip_size, SEEK_SET) != 0) {
                perror("rewrite parity strip");
                fclose(fp_parity);
                free(strip_buf);
                return 1;
            }
        }
    }

parity_done:
    fclose(fp_parity);
    free(strip_buf);
    return 0;
}

static int build_default_erasures(isal_plan_layout_t *plan, int **erasures_out) {
    int n = DEFAULT_N_WIPE_DATA + DEFAULT_N_WIPE_PARITY;
    int *erasures = malloc(sizeof(int) * (n + 1));
    if (!erasures) return 1;

    for (int i = 0; i < DEFAULT_N_WIPE_DATA; i++) {
        erasures[i] = default_wipe_data[i];
    }
    for (int i = 0; i < DEFAULT_N_WIPE_PARITY; i++) {
        erasures[DEFAULT_N_WIPE_DATA + i] = default_wipe_parity[i] + plan->data_strips;
    }
    erasures[n] = -1;
    *erasures_out = erasures;
    return 0;
}

int main(int argc, char **argv) {
    tbx_construct_fn_static();

    bool use_default = false;
    int arg_start = 1;
    if (argc > 1 && strcmp(argv[1], "--default") == 0) {
        use_default = true;
        arg_start = 2;
    }

    if (argc < arg_start + 1) usage(argv[0]);

    if (use_default && require_file(DEFAULT_INPUT) != 0) {
        return 1;
    }

    const char *cmd = argv[arg_start];

    if (strcmp(cmd, "encode") == 0) {
        int min_args = use_default ? 1 : 3;
        if (argc < arg_start + min_args) usage(argv[0]);

        const char *input   = use_default ? DEFAULT_INPUT : argv[arg_start + 1];
        const char *parity  = use_default ? DEFAULT_PARITY : argv[arg_start + 2];
        int k = use_default ? DEFAULT_K : atoi(argv[arg_start + 3]);
        int m = use_default ? DEFAULT_M : atoi(argv[arg_start + 4]);
        int w = use_default ? DEFAULT_W : (argc > arg_start + 5 ? atoi(argv[arg_start + 5]) : 8);
        int method = use_default ? DEFAULT_METHOD : (argc > arg_start + 6 ? atoi(argv[arg_start + 6]) : 1);
        long long strip = use_default ? 0 : (argc > arg_start + 7 ? atoll(argv[arg_start + 7]) : 16 * 1024);
        int packet = use_default ? DEFAULT_PACKET : (argc > arg_start + 8 ? atoi(argv[arg_start + 8]) : -1);
        int base   = use_default ? DEFAULT_BASE : (argc > arg_start + 9 ? atoi(argv[arg_start + 9]) : -1);

        if (k <= 0 || m <= 0) {
            fprintf(stderr, "k and m must be > 0\n");
            return 1;
        }

        long long fsize = get_file_size(input);
        if (fsize < 0) {
            perror("stat input");
            return 1;
        }

        isal_lio_erasure_plan_t *plan;
        if (use_default) {
            plan = default_plan(fsize);
        } else {
            long long hint = (strip > 0) ? (long long)k * strip : fsize;
            plan = isal_et_generate_plan(hint, method, k, m, w, packet, base);
        }
        if (!plan) {
            fprintf(stderr, "Failed to create ISAL plan\n");
            return 1;
        }

        isal_plan_layout_t *pl = (isal_plan_layout_t *)plan;
        if (!use_default && (pl->packet_size <= 0 || pl->base_unit <= 0)) {
            int safe_packet = 4096;
            int safe_base = 8;
            long long safe_strip = (strip > 0) ? strip : (fsize / k);
            if (safe_strip < 64 * 1024) safe_strip = 64 * 1024;
            isal_et_destroy_plan(plan);
            plan = isal_et_new_plan(method, safe_strip, k, m, w, safe_packet, safe_base);
            if (!plan) {
                fprintf(stderr, "Failed to create ISAL plan (fallback)\n");
                return 1;
            }
            pl = (isal_plan_layout_t *)plan;
        }

        printf("lstore-isal encode (pure ISAL backend)\n");
        printf("  input=%s k=%d m=%d w=%d method=%d strip=%lld packet=%d base=%d\n",
               input, k, m, w, method, pl->strip_size, pl->packet_size, pl->base_unit);

        int rc = isal_et_encode(plan, input, 0, parity, 0, 0);
        isal_et_destroy_plan(plan);

        if (rc == 0) {
            long long psize = get_file_size(parity);
            if (psize > 0) {
                printf("Encode completed successfully. Parity file %s size=%lld bytes\n", parity, psize);
            } else {
                fprintf(stderr, "Encode returned 0 but parity file %s has size 0 or does not exist\n", parity);
                rc = 1;
            }
        } else {
            fprintf(stderr, "isal_et_encode returned error %d\n", rc);
        }
        return rc;

    } else if (strcmp(cmd, "decode") == 0) {
        int min_args = use_default ? 1 : 4;
        if (argc < arg_start + min_args) usage(argv[0]);

        const char *erased  = use_default ? DEFAULT_ERASED : argv[arg_start + 1];
        const char *parity  = use_default ? DEFAULT_PARITY : argv[arg_start + 2];
        const char *output  = use_default ? DEFAULT_RECOVERED : argv[arg_start + 3];
        int k = use_default ? DEFAULT_K : atoi(argv[arg_start + 4]);
        int m = use_default ? DEFAULT_M : atoi(argv[arg_start + 5]);
        int w = use_default ? DEFAULT_W : atoi(argv[arg_start + 6]);
        int packet = use_default ? DEFAULT_PACKET : atoi(argv[arg_start + 7]);
        int base   = use_default ? DEFAULT_BASE : atoi(argv[arg_start + 8]);
        long long strip = use_default ? 0 : atoll(argv[arg_start + 9]);
        int n_miss = use_default ? (DEFAULT_N_WIPE_DATA + DEFAULT_N_WIPE_PARITY) : atoi(argv[arg_start + 10]);

        if (!use_default && n_miss > 0 && argc < arg_start + 11 + n_miss) usage(argv[0]);

        int *erasures = NULL;
        if (use_default) {
            if (require_file(DEFAULT_PARITY) != 0) {
                return 1;
            }
            long long fsize = get_file_size(DEFAULT_INPUT);
            if (fsize < 0) {
                perror("stat input");
                return 1;
            }

            isal_lio_erasure_plan_t *plan = default_plan(fsize);
            if (!plan) {
                fprintf(stderr, "Failed to create ISAL plan\n");
                return 1;
            }
            isal_plan_layout_t *pl = (isal_plan_layout_t *)plan;

            printf("lstore-isal decode --default (pure ISAL backend)\n");
            printf("  k=%d m=%d w=%d strip=%lld packet=%d base=%d\n",
                   pl->data_strips, pl->parity_strips, pl->w,
                   pl->strip_size, pl->packet_size, pl->base_unit);

            if (copy_file(DEFAULT_INPUT, DEFAULT_ERASED) != 0 ||
                copy_file(DEFAULT_PARITY, "1GB_random_erased.parity") != 0) {
                isal_et_destroy_plan(plan);
                return 1;
            }

            const char *erased_parity = "1GB_random_erased.parity";
            if (wipe_blocks(pl, DEFAULT_ERASED, 0, erased_parity, 0,
                            DEFAULT_N_WIPE_DATA, DEFAULT_N_WIPE_PARITY,
                            default_wipe_data, default_wipe_parity) != 0) {
                isal_et_destroy_plan(plan);
                return 1;
            }

            if (copy_file(DEFAULT_ERASED, DEFAULT_RECOVERED) != 0) {
                isal_et_destroy_plan(plan);
                return 1;
            }

            if (build_default_erasures(pl, &erasures) != 0) {
                isal_et_destroy_plan(plan);
                return 1;
            }

            printf("  erasures:");
            for (int i = 0; erasures[i] != -1; i++) {
                printf(" %d", erasures[i]);
            }
            printf("\n");

            int rc = isal_et_decode(plan, fsize, DEFAULT_RECOVERED, 0,
                                    erased_parity, 0, 0, erasures);
            isal_et_destroy_plan(plan);
            free(erasures);

            if (rc == 0) {
                printf("Decode completed. Recovered data written to %s\n", DEFAULT_RECOVERED);
            } else {
                fprintf(stderr, "isal_et_decode returned error %d\n", rc);
            }
            return rc;
        }

        erasures = malloc(sizeof(int) * (n_miss + 1));
        for (int i = 0; i < n_miss; i++) {
            erasures[i] = atoi(argv[arg_start + 11 + i]);
        }
        erasures[n_miss] = -1;

        long long fsize = get_file_size(erased);
        if (fsize < 0) fsize = get_file_size(parity);

        isal_lio_erasure_plan_t *plan = isal_et_new_plan(1 /*CAUCHY_ORIG_ISA*/, strip, k, m, w, packet, base);
        if (!plan) {
            fprintf(stderr, "Failed to create ISAL plan\n");
            free(erasures);
            return 1;
        }

        if (copy_file(erased, output) != 0) {
            isal_et_destroy_plan(plan);
            free(erasures);
            return 1;
        }

        printf("lstore-isal decode (pure ISAL backend)\n");
        printf("  Writing recovered data to %s\n", output);

        int rc = isal_et_decode(plan, fsize, output, 0, parity, 0, 0, erasures);

        isal_et_destroy_plan(plan);
        free(erasures);

        if (rc == 0) {
            printf("Decode completed. Check %s\n", output);
        }
        return rc;

    } else {
        usage(argv[0]);
    }

    return 0;
}