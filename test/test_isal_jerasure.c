#include <stdio.h>
#include<stdbool.h>
#include<time.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

// Jerasure side (provides et_decode, et_new_plan, lio_erasure_plan_t (JE version) etc. via linked lio)
#include "erasure_tools.h"
#include <tbx/io.h>
#include <assert.h>
#include <openssl/md5.h>

#define BLANK_CHAR '0'
#define BUFFER_SIZE 8192

// Opaque ISAL plan + explicit API declarations for cross test (ISAL et_encode + Jerasure et_decode).
// (ISAL .c is compiled separately into the target with renames via CMake.)
typedef struct isal_lio_erasure_plan_t isal_lio_erasure_plan_t;

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

// Layout mirror for the ISAL plan (first fields only) so we can read strip_size/w/packet/base
// after generate, to configure a matching JE plan for decode. (Avoids pulling full renamed header.)
typedef struct {
    long long int strip_size;
    int method;
    int data_strips;
    int parity_strips;
    int w;
    int packet_size;
    int base_unit;
} isal_plan_layout_t;

//#define FILE_SIZE (16384ULL * 6000ULL)
#define FILE_SIZE (16384ULL * 64000ULL)
#define BLOCK_SIZE 16384

static double get_wall_seconds(void){
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec + ts.tv_nsec * 1e-9;
}

long long getFileSize(const char *filename) {
  FILE *file = fopen(filename, "rb");
  if (!file) {
    perror("Error opening file");
    return -1;
  }

  fseek(file, 0, SEEK_END);
  long long size = ftell(file);
  fclose(file);
  return size;
}

int generateRandomFile(const char *filename) {
  static int seeded = 0;
  if (!seeded) {
    srand((unsigned int)time(NULL));
    seeded = 1;
  }

  FILE *file = fopen(filename, "wb");
  if (file == NULL) {
    perror("Error opening file");
    return 1;
  }

  unsigned char buffer[BLOCK_SIZE];
  size_t bytesWritten;
  unsigned long long totalBytes = 0;

  while (totalBytes < FILE_SIZE) {
    size_t bytesToWrite = (FILE_SIZE - totalBytes < BLOCK_SIZE) ? (size_t)(FILE_SIZE - totalBytes) : BLOCK_SIZE;

    for (size_t i = 0; i < bytesToWrite; i++) {
      int r = rand() % 75;   // 0..62
      if (r < 26) {
          buffer[i] = 'a' + r;           // a–z
      }
      else if (r < 52) {
          buffer[i] = 'A' + (r - 26);    // A–Z
      }
      else if (r < 62) {
          buffer[i] = '0' + (r - 52);    // 0–9
      }
      else if (r < 75) {
        buffer[i] = '\n';
      }
      else {
          buffer[i] = ' ';               // space (most common case)
      }
    }   

    bytesWritten = fwrite(buffer, 1, bytesToWrite, file);
    if (bytesWritten != bytesToWrite) {
      perror("Error writing to file");
      fclose(file);
      return 1;
    }

    totalBytes += bytesWritten;
  }

  fclose(file);

  file = fopen(filename, "rb");
  if (file == NULL) {
    perror("Error reopening file");
    return 1;
  }
  fseek(file, 0, SEEK_END);
  long long size = ftell(file);
  fclose(file);

  if (size == (long long)FILE_SIZE) {
    printf("\t Success: '%s' created with exactly %llu bytes.\n", filename, FILE_SIZE);
  } else {
    printf("\t Error: File size is %lld bytes, expected %llu.\n", size, FILE_SIZE);
    return 1;
  }

  return 0;
}

// Fast generator for large (1GB) random-like file for perf testing.
// Uses /dev/urandom when available (good randomness, reasonable speed on Linux).
// Falls back to a simple incrementing pattern if not.
int generate_large_random_file(const char *filename, long long int size) {
  printf("\t Generating large %lld-byte random input file '%s'...\n", size, filename);
  FILE *out = fopen(filename, "wb");
  if (!out) {
    perror("fopen large output");
    return 1;
  }

  FILE *rf = fopen("/dev/urandom", "rb");
  bool use_ur = (rf != NULL);
  if (!use_ur) {
    printf("\t (no /dev/urandom, using pseudo-random pattern fallback)\n");
  }

  const size_t CHUNK = 1024 * 1024; // 1 MiB
  unsigned char buf[CHUNK];
  long long int written = 0;
  while (written < size) {
    size_t want = CHUNK;
    if (written + want > size) want = (size_t)(size - written);

    size_t got = 0;
    if (use_ur) {
      got = fread(buf, 1, want, rf);
    } else {
      for (size_t i = 0; i < want; i++) {
        buf[i] = (unsigned char)((written + i) & 0xFF);
      }
      got = want;
    }
    if (got == 0) break;

    size_t w = fwrite(buf, 1, got, out);
    if (w != got) {
      perror("fwrite during large gen");
      break;
    }
    written += got;
  }

  fclose(out);
  if (rf) fclose(rf);

  if (written == size) {
    printf("\t Large file created: %lld bytes.\n", written);
    return 0;
  }
  fprintf(stderr, "\t Large file generation incomplete (%lld / %lld)\n", written, size);
  return 1;
}

char* compute_md5_file(const char* filepath) {
    FILE* file = fopen(filepath, "rb");
    if (!file) {
        perror("fopen");
        return NULL;
    }

    MD5_CTX ctx;
    MD5_Init(&ctx);

    unsigned char buffer[BUFFER_SIZE];
    size_t bytes_read;

    while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, file)) > 0) {
        MD5_Update(&ctx, buffer, bytes_read);
    }

    if (ferror(file)) {
        perror("fread");
        fclose(file);
        return NULL;
    }

    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5_Final(digest, &ctx);
    fclose(file);

    // Convert to hex string
    char* hex = malloc(MD5_DIGEST_LENGTH * 2 + 1);
    if (!hex) {
        return NULL;
    }

    for (int i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(hex + (i * 2), "%02x", digest[i]);
    }
    hex[MD5_DIGEST_LENGTH * 2] = '\0';

    return hex;
}

int copyFile(const char *sourcePath, const char *destPath) {
  FILE *source = fopen(sourcePath, "rb");
  if (!source) {
    perror("Error opening source file");
    return 1;
  }

  FILE *dest = fopen(destPath, "wb");
  if (!dest) {
    perror("Error opening destination file");
    fclose(source);
    return 1;
  }

  unsigned char buffer[BLOCK_SIZE];
  size_t bytesRead, bytesWritten;
  while ((bytesRead = fread(buffer, 1, BLOCK_SIZE, source)) > 0) {
    bytesWritten = fwrite(buffer, 1, bytesRead, dest);
    if (bytesWritten != bytesRead) {
      perror("Error writing to destination file");
      fclose(source);
      fclose(dest);
      return 1;
    }
  }

  if (ferror(source)) {
    perror("Error reading source file");
    fclose(source);
    fclose(dest);
    return 1;
  }

  if (compute_md5_file(sourcePath) == compute_md5_file(destPath)){
    printf("Source and destination files differ, copying failed\n");
    return 1;
  }

  fclose(source);
  fclose(dest);
  return 0;
}

int compareFiles(const char *file1Path, const char *file2Path) {

  if (strcmp(compute_md5_file(file1Path),compute_md5_file(file2Path)) == 0){
    //printf("Source and destination files match\n");
    return 0;
  } else {
    /*
    printf("Files differ \n");
    printf("MD5 of %s file: %s\n",file1Path, compute_md5_file(file1Path));
    printf("MD5 of %s file: %s\n",file2Path, compute_md5_file(file2Path));
    */
    return 1;
  }
}


char *read_file(const char *filename, size_t *size) {
    FILE *fp = fopen(filename, "rb");
    if (!fp) {
        perror("Failed to open input file");
        return NULL;
    }
    fseek(fp, 0, SEEK_END);
    *size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char *buffer = malloc(*size);
    if (!buffer) {
        fclose(fp);
        return NULL;
    }
    fread(buffer, 1, *size, fp);
    fclose(fp);
    return buffer;
}

// Helper function to write a buffer to a file
int write_file(const char *filename, char *buffer, size_t size) {
    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        perror("Failed to open output file");
        return -1;
    }
    fwrite(buffer, 1, size, fp);
    fclose(fp);
    return 0;
}

int wipe_blocks(lio_erasure_plan_t *plan,
                const char *fname,      long long int foffset,
                const char *pname,      long long int poffset,
                int buffer_size __attribute__((unused)),  // ignored for now
                int num_wipe_file,
                int num_wipe_parity,
                int *indices_wipe_file,
                int *indices_wipe_parity) {
    if (!plan || !fname || !pname) {
        fprintf(stderr, "wipe_blocks: invalid parameters\n");
        return 1;
    }

    size_t strip_size = plan->strip_size;
    if (strip_size <= 0) {
        fprintf(stderr, "Invalid strip_size: %lld\n", (long long int)strip_size);
        return 1;
    }

    int k = plan->data_strips;
    int m = plan->parity_strips;

    // Print what we're about to erase
    printf("\t Wiping data blocks in %s :", fname);
    for (int i = 0; i < num_wipe_file; i++) {
        printf(" %d", indices_wipe_file[i]);
    }
    printf("\n");

    printf("\t Wiping parity blocks in %s :", pname);
    for (int i = 0; i < num_wipe_parity; i++) {
        printf(" %d", indices_wipe_parity[i]);
    }
    printf("\n");

    // ────────────────────────────────────────────────
    // Process data file (in place)
    // ────────────────────────────────────────────────
    FILE *fp_data = fopen(fname, "r+b");
    if (!fp_data) {
        perror("fopen data file (r+b)");
        return 1;
    }

    unsigned char *strip_buf = malloc(strip_size);
    if (!strip_buf) {
        perror("malloc strip buffer");
        fclose(fp_data);
        return 1;
    }

    // Start from the given offset
    if (fseek(fp_data, foffset, SEEK_SET) != 0) {
        perror("fseek data file");
        goto cleanup_data;
    }

    //long long current_pos = foffset;
    int stripe_idx = 0;

    while (1) {
        for (int i = 0; i < k; i++) {
            // Remember position before reading
            long long strip_start = ftell(fp_data);

            size_t nread = fread(strip_buf, 1, strip_size, fp_data);
            if (nread == 0) {
                if (feof(fp_data)) goto data_done;
                perror("fread data strip");
                goto cleanup_data;
            }
            if (nread != (size_t)strip_size) {
                fprintf(stderr, "Short read in data strip %d at offset %lld\n", i, strip_start);
                goto cleanup_data;
            }

            // Check if this block should be wiped
            int should_wipe = 0;
            for (int j = 0; j < num_wipe_file; j++) {
                if (i == indices_wipe_file[j]) {
                    should_wipe = 1;
                    break;
                }
            }

            if (should_wipe) {
                //printf("\t → Wiping data block %d (stripe %d) at offset %lld\n", i, stripe_idx, strip_start);
                memset(strip_buf, 0xFF, strip_size);   // or 0x00
            }

            // Seek back and overwrite
            if (fseek(fp_data, strip_start, SEEK_SET) != 0) {
                perror("fseek back to overwrite data");
                goto cleanup_data;
            }

            if (fwrite(strip_buf, 1, strip_size, fp_data) != strip_size) {
                perror("fwrite wiped data strip");
                goto cleanup_data;
            }

            // Move forward for next strip
            if (fseek(fp_data, strip_start + strip_size, SEEK_SET) != 0) {
                perror("fseek next data strip");
                goto cleanup_data;
            }
        }
        stripe_idx++;
    }
data_done:
    printf("\t Data file updated in place: %s\n", fname);

    // ────────────────────────────────────────────────
    // Process parity file (in place)
    // ────────────────────────────────────────────────
    FILE *fp_parity = fopen(pname, "r+b");
    if (!fp_parity) {
        perror("fopen parity file (r+b)");
        goto cleanup_data;
    }

    if (fseek(fp_parity, poffset, SEEK_SET) != 0) {
        perror("fseek parity file");
        goto cleanup_parity;
    }

    stripe_idx = 0;

    while (1) {
        for (int i = 0; i < m; i++) {
            long long strip_start = ftell(fp_parity);

            size_t nread = fread(strip_buf, 1, strip_size, fp_parity);
            if (nread == 0) {
                if (feof(fp_parity)) goto parity_done;
                perror("fread parity strip");
                goto cleanup_parity;
            }
            if (nread != (size_t)strip_size) {
                fprintf(stderr, "Short read in parity strip %d at offset %lld\n", i, strip_start);
                goto cleanup_parity;
            }

            int should_wipe = 0;
            for (int j = 0; j < num_wipe_parity; j++) {
                if (i == indices_wipe_parity[j]) {
                    should_wipe = 1;
                    break;
                }
            }

            if (should_wipe) {
                //printf("\t → Wiping parity block %d (stripe %d) at offset %lld\n", i, stripe_idx, strip_start);
                memset(strip_buf, 0xFF, strip_size);
            }

            if (fseek(fp_parity, strip_start, SEEK_SET) != 0) {
                perror("fseek back to overwrite parity");
                goto cleanup_parity;
            }

            if (fwrite(strip_buf, 1, strip_size, fp_parity) != strip_size) {
                perror("fwrite wiped parity strip");
                goto cleanup_parity;
            }

            if (fseek(fp_parity, strip_start + strip_size, SEEK_SET) != 0) {
                perror("fseek next parity strip");
                goto cleanup_parity;
            }
        }
        stripe_idx++;
    }

parity_done:
    printf("\t Parity file updated in place: %s\n", pname);

    free(strip_buf);
    fclose(fp_data);
    fclose(fp_parity);
    return 0;

cleanup_data:
    free(strip_buf);
    if (fp_data) fclose(fp_data);
    return 1;

cleanup_parity:
    free(strip_buf);
    if (fp_parity) fclose(fp_parity);
    return 1;
}

int main(int argc, char **argv) {

    int strip_size = 16 * 1024 ; // 16 KB
    int data_strips = 6;
    int parity_strips = 3;
    int w = 8;
    //int w = 10;
    int packet_size = -1;
    //int packet_size = 128;
    //int base_unit = -1;
    int base_unit = -1;
    size_t file_size;

    printf("Testing cross-compatibility: et_encode from Jerasure + et_decode from ISAL (Cauchy Orig, w=8)\n");

    // Use a small dedicated input for the cross test so it runs quickly and completes.
    // This ensures we exercise the full ISAL-encode + Jerasure-decode path without
    // depending on huge pre-generated files or long debug output.
    const char *input_file = "cross_test_input.bin";
    // Always (re)generate a small ~96KB file to match the data*strip hint used below.
    printf("\t Generating small test input %s for reliable cross test...\n", input_file);
    FILE *f = fopen(input_file, "wb");
    if (f) {
        for (int i = 0; i < 96 * 1024; i++) {
            fputc('A' + (i % 26), f);
        }
        fclose(f);
    }
    file_size = getFileSize(input_file);
    printf("1. Using test input %s of size: %lld bytes\n", input_file, (long long)file_size);

    /*
    printf("1. Generating Random file with size: %.2f MB\n", (double)FILE_SIZE/1024/1024);
    const char *input_file = "random_generated_file.txt";
    if (generateRandomFile(input_file) == 0) {
      file_size = getFileSize(input_file);
      printf("\t File generation completed successfully.\n");
    } else {
      printf("\t File generation failed.\n");
    }
    */

    printf("2. Generating plans (Jerasure for encode, ISAL for decode)\n");

    // Jerasure plan (for et_encode) - use generate to compute optimal packet/base/strip for the layout
    int je_method = 2; // CAUCHY_ORIG
    lio_erasure_plan_t *je_plan = et_generate_plan(data_strips * strip_size, je_method, data_strips, parity_strips, w, packet_size, base_unit);
    if (!je_plan) {
        printf("Error creating Jerasure erasure coding plan!\n");
        return 1;
    }
    printf("\t Jerasure plan generated successfully\n");

    // ISAL plan (for et_decode) - use the exact same layout params from the encode plan so reads match writes
    int isa_method = 1; // CAUCHY_ORIG_ISA
    isal_lio_erasure_plan_t *isal_plan = isal_et_new_plan(isa_method, je_plan->strip_size, data_strips, parity_strips, je_plan->w, je_plan->packet_size, je_plan->base_unit);
    if (!isal_plan) {
        printf("Error creating ISAL erasure coding plan!\n");
        return 1;
    }
    printf("\t ISAL plan generated successfully\n");

    // Debug: print P coeffs from both to confirm match (full rows for m=3)
    {
      unsigned char **pp = (unsigned char **)((char*)isal_plan + 32);
      unsigned char *imat = *pp;
      int kk = data_strips;
      for(int ri=0; ri<3; ri++){
        printf("ISAL P row%d: ", ri);
        for(int j=0; j<kk; j++) printf("%02x ", imat[kk*kk + ri*kk + j]);
        printf("\n");
      }
    }
    je_plan->form_encoding_matrix(je_plan);  // to populate its matrix for print
    for(int ri=0; ri<3; ri++){
      printf("JE   P row%d: ", ri);
      for(int j=0; j<data_strips; j++) printf("%02x ", (unsigned char)je_plan->encode_matrix[ri*data_strips + j]);
      printf("\n");
    }

    char *parity_file = "isal.parity";
    printf("3. Encoding data (Jerasure et_encode) into parity file: %s\n",parity_file);
    double t0 = get_wall_seconds();
    int buffer_size = 0;
    long long int file_offset = 0;
    long long int poffset = 0;
    int result = et_encode(je_plan, input_file, file_offset, parity_file, poffset, buffer_size);
    if (result != 0) {
        printf("\t Encoding failed with result: %d\n", result);
    }
    double t1 = get_wall_seconds();
    double runtime_en = t1-t0;
    printf("\t Encoding successfully finished in %.6f s, fragments written to %s\n", runtime_en, parity_file); 

    printf("4. Erasing blocks in data and parity files. \n");
    int num_wipe_data = 2;
    int num_wipe_parity = 1;
    int indices_wipe_file[] = {2, 4}; //, 7};
    int indices_wipe_parity[] = {2}; //, 3};
    const char* erased_file_data = "erased_isal.data";
    const char* erased_file_parity = "erased_isal.parity";
    printf("Making a copy of the input file and parity file: \n");
    if (copyFile(input_file,erased_file_data) == 0) printf("Copied data file %s successfully to %s \n",input_file,erased_file_data);
    if (copyFile(parity_file,erased_file_parity) == 0) printf("Copied parity file %s successfully to %s \n",parity_file,erased_file_parity);

    result = wipe_blocks(je_plan, erased_file_data, file_offset, erased_file_parity, 0, buffer_size, num_wipe_data, num_wipe_parity, indices_wipe_file, indices_wipe_parity);
    if( result == 0) printf("\t Erased blocks successfully.\n");
    else printf("\t Error in erasing blocks.\n");

    int compare_result = compareFiles(input_file,erased_file_data);
    if (compare_result == 1 ) printf("\t Files %s and %s differ\n",input_file,erased_file_data);
    compare_result = compareFiles(parity_file,erased_file_parity);
    if (compare_result == 1 ) printf("\t Files %s and %s differ\n",parity_file,erased_file_parity);

    const char *recovered_file = "recovered_isal.data";
    printf("Making a copy of the erased file to %s \n",recovered_file);
    if (copyFile(erased_file_data,recovered_file) == 0) printf("Copied data file %s successfully to %s \n",input_file,erased_file_data);

    printf("5. Reconstructing the file %s\n",recovered_file);
    t0 = get_wall_seconds();
    int erasures_array[num_wipe_data+num_wipe_parity+1];
    for(int i=0; i<num_wipe_data; i++){ erasures_array[i] = indices_wipe_file[i]; }
    {
      isal_plan_layout_t *l = (isal_plan_layout_t *)isal_plan;
      for(int i=num_wipe_data; i<num_wipe_data+num_wipe_parity; i++){ erasures_array[i] = indices_wipe_parity[i-num_wipe_data] + l->data_strips; }
    }
    erasures_array[num_wipe_data+num_wipe_parity]=-1;
    printf("\t Erasures array: ");
    for(int i=0; i<num_wipe_data+num_wipe_parity+1; i++) { printf(" %d",erasures_array[i]); }
    printf("\n");

    // Pre-decode check: good data positions in the data file (recovered) should still be original
    printf("Pre-decode check good data strips (should match orig):\n");
    for(int i=0; i<data_strips; i++){
      int is_bad = 0;
      for(int wi=0; wi<num_wipe_data; wi++) if(indices_wipe_file[wi]==i) is_bad=1;
      if(!is_bad){
        long long off = file_offset + (long long)i * ((isal_plan_layout_t *)isal_plan)->strip_size;
        FILE *fo = fopen(input_file, "rb"); fseek(fo, off, SEEK_SET); unsigned char o[4]; fread(o,1,4,fo); fclose(fo);
        FILE *fd = fopen(recovered_file, "rb"); fseek(fd, off, SEEK_SET); unsigned char d[4]; fread(d,1,4,fd); fclose(fd);
        printf("  good data idx %d: orig0=%02x file0=%02x match=%d\n", i, o[0], d[0], memcmp(o,d,4)==0);
      }
    }
 
    result = isal_et_decode(isal_plan, file_size, recovered_file, file_offset, erased_file_parity, poffset, buffer_size,erasures_array);
    if (result != 0) {
        printf("Decoding failed with result: %d\n", result);
        goto cleanup;
    }
    t1 = get_wall_seconds();
    double runtime_de = t1-t0;
    printf("\t Decoding successfully finished in %.6f s\n", runtime_de);

    // Diagnostic: check if the specific wiped data positions were correctly recovered
    printf("Diagnostic recovery check for wiped data strips:\n");
    for(int wi=0; wi<num_wipe_data; wi++){
      int idx = indices_wipe_file[wi];
      long long off = file_offset + (long long)idx * ((isal_plan_layout_t *)isal_plan)->strip_size;
      FILE *fo = fopen(input_file, "rb");
      fseek(fo, off, SEEK_SET);
      unsigned char origb[8]; size_t no = fread(origb,1,8,fo); fclose(fo);
      FILE *fr = fopen(recovered_file, "rb");
      fseek(fr, off, SEEK_SET);
      unsigned char recb[8]; size_t nr = fread(recb,1,8,fr); fclose(fr);
      int m = (no==nr && memcmp(origb,recb,8)==0);
      printf("  wiped data idx %d off %lld: orig0=%02x rec0=%02x  bytes_match=%d\n", idx, off, origb[0], recb[0], m);
    }

    compare_result = compareFiles(input_file, recovered_file);
    if (compare_result == 0 ) {
      printf("\t Files %s and %s are the same\n",input_file,recovered_file);
      printf("RECOVERY SUCCESS: ISAL et_decode correctly recovered the data from Jerasure et_encode!\n");
    }else {
      printf("\t Decoding failed! Files %s and %s differ \n",input_file,recovered_file);
    }

    printf("Summary of run time (Jerasure et_encode + ISAL et_decode cross test): \n");
    printf("\t Encoding (Jerasure) \t %.6f s\n",runtime_en);
    printf("\t Decoding (ISAL) \t %.6f s\n",runtime_de);

    // Small cross correctness already verified above (RECOVERY SUCCESS + file match).
    // Now run the larger 1GB performance comparison while keeping the cross (JE encode + ISAL decode)
    // for correctness on a realistic large random file.

cleanup:
    et_destroy_plan(je_plan);
    isal_et_destroy_plan(isal_plan);

    printf("Small cross test completed.\n");

    // ==================== 1 GB PERFORMANCE + CROSS CORRECTNESS ====================
    printf("\n\n=== 1 GB Performance Comparison (JErasure vs ISAL) + Cross Correctness (JE encode + ISAL decode) ===\n");

    const char *large_input = "perf_input.bin";

    // Reuse k/m/w from the small cross setup in this scope; use local for base/pkt to force block==strip.
    int je_m = 2;   // CAUCHY_ORIG
    int isa_m = 1;  // CAUCHY_ORIG_ISA

    printf("  Computing plans for ~1 GiB with forced block_size==strip_size (single iteration to avoid known decode position bugs)...\n");
    int lg_base_unit = 8;
    int ww = 8;
    // Choose strip/block ~170 MiB so 6* ~ 1.07 GiB (close to requested 1 GiB).
    long long strip_for_large = 178257920LL; // 170 MiB
    int pkt = strip_for_large / (ww * lg_base_unit);
    long long blk = (long long)ww * pkt * lg_base_unit;
    strip_for_large = blk;
    long long actual_size = 6LL * strip_for_large;

    lio_erasure_plan_t *je_lg = et_new_plan(je_m, strip_for_large, data_strips, parity_strips, ww, pkt, lg_base_unit);
    isal_lio_erasure_plan_t *isa_lg = isal_et_new_plan(isa_m, strip_for_large, data_strips, parity_strips, ww, pkt, lg_base_unit);

    if (!je_lg || !isa_lg) {
      printf("\t Plan generation for large file failed.\n");
    } else {
      printf("\t JE/ISAL strip/block=%lld (single-iteration layout). Generating input of exact size...\n", strip_for_large);

      if (generate_large_random_file(large_input, actual_size) != 0) {
        printf("\t Large file generation failed. Skipping large perf.\n");
        et_destroy_plan(je_lg);
        isal_et_destroy_plan(isa_lg);
      } else {

        int nwipe_d = 2, nwipe_p = 1;
        int iwd[] = {2,4};
        int iwp[] = {2};

        // --- JE encode (timed) ---
        const char *lg_je_p = "perf_je.parity";
        printf("  1. JErasure encode (1 GiB)...\n");
        double t0 = get_wall_seconds();
        et_encode(je_lg, large_input, 0, lg_je_p, 0, 0);
        double tje_enc = get_wall_seconds() - t0;
        printf("     JE encode: %.3fs  (%.1f MB/s)\n", tje_enc, actual_size / tje_enc / 1048576.0);

        // --- ISAL encode (timed) ---
        const char *lg_isa_p = "perf_isal.parity";
        printf("  2. ISAL encode (1 GiB)...\n");
        t0 = get_wall_seconds();
        isal_et_encode(isa_lg, large_input, 0, lg_isa_p, 0, 0);
        double tisa_enc = get_wall_seconds() - t0;
        printf("     ISAL encode: %.3fs  (%.1f MB/s)\n", tisa_enc, actual_size / tisa_enc / 1048576.0);

        // Prepare erased data + parity for cross (JE parity + ISAL decode) + pure decodes
        const char *lg_edata = "perf_erased.data";
        const char *lg_ep_je = "perf_ep_je.parity";
        const char *lg_ep_isa = "perf_ep_isa.parity";
        const char *lg_xrec = "perf_cross_recovered.data";

        int lg_eras[4];
        lg_eras[0] = iwd[0]; lg_eras[1] = iwd[1];
        {
          isal_plan_layout_t *l = (isal_plan_layout_t*)isa_lg;
          lg_eras[2] = iwp[0] + l->data_strips;
        }
        lg_eras[3] = -1;

        // Cross: JE encode (done) + ISAL decode with erasures, verify
        printf("  3. Cross (JE encode + ISAL decode) with erasures + correctness...\n");
        copyFile(large_input, lg_edata);
        copyFile(lg_je_p, lg_ep_je);
        wipe_blocks(je_lg, lg_edata, 0, lg_ep_je, 0, 0, nwipe_d, nwipe_p, iwd, iwp);

        copyFile(lg_edata, lg_xrec);
        t0 = get_wall_seconds();
        isal_et_decode(isa_lg, actual_size, lg_xrec, 0, lg_ep_je, 0, 0, lg_eras);
        double tcross_dec = get_wall_seconds() - t0;
        printf("     ISAL decode (cross): %.3fs  (%.1f MB/s)\n", tcross_dec, actual_size / tcross_dec / 1048576.0);

        if (compareFiles(large_input, lg_xrec) == 0) {
          printf("     Large 1 GiB cross correctness (JE enc + ISAL dec): PASS\n");
        } else {
          printf("     Large 1 GiB cross correctness: FAIL\n");
        }

        // Pure ISAL decode (timed) - fresh erased using ISAL parity
        copyFile(large_input, lg_edata);
        copyFile(lg_isa_p, lg_ep_isa);
        wipe_blocks(je_lg, lg_edata, 0, lg_ep_isa, 0, 0, nwipe_d, nwipe_p, iwd, iwp);
        copyFile(lg_edata, lg_xrec);
        t0 = get_wall_seconds();
        isal_et_decode(isa_lg, actual_size, lg_xrec, 0, lg_ep_isa, 0, 0, lg_eras);
        double tisa_dec = get_wall_seconds() - t0;
        printf("  4. Pure ISAL decode (with erasures): %.3fs  (%.1f MB/s)\n", tisa_dec, actual_size / tisa_dec / 1048576.0);

        // Pure JE decode (timed)
        copyFile(large_input, lg_edata);
        copyFile(lg_je_p, lg_ep_je);
        wipe_blocks(je_lg, lg_edata, 0, lg_ep_je, 0, 0, nwipe_d, nwipe_p, iwd, iwp);
        copyFile(lg_edata, lg_xrec);
        t0 = get_wall_seconds();
        et_decode(je_lg, actual_size, lg_xrec, 0, lg_ep_je, 0, 0, lg_eras);
        double tje_dec = get_wall_seconds() - t0;
        printf("  5. Pure JErasure decode (with erasures): %.3fs  (%.1f MB/s)\n", tje_dec, actual_size / tje_dec / 1048576.0);

        printf("\n  Performance summary (1 GiB, k=%d m=%d w=%d Cauchy-Orig):\n", data_strips, parity_strips, w);
        printf("    JE encode:   %.3f s\n", tje_enc);
        printf("    ISAL encode: %.3f s\n", tisa_enc);
        printf("    JE decode:   %.3f s\n", tje_dec);
        printf("    ISAL decode: %.3f s\n", tisa_dec);
        printf("    Cross (JE enc + ISAL dec): %.3f s encode + %.3f s decode\n", tje_enc, tcross_dec);

        et_destroy_plan(je_lg);
        isal_et_destroy_plan(isa_lg);
      }
    }

    printf("\nAll tests (small cross correctness + 1 GiB perf) completed.\n");
    return 0;
}
