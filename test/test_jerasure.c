#include <stdio.h>
#include<stdbool.h>
#include<time.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "erasure_tools.h"
#include <tbx/io.h>
#include <tbx/assert_result.h>
#include <assert.h>
#define BLANK_CHAR '0'
#define FILE_SIZE (16384ULL * 6000ULL)
#define BLOCK_SIZE 16384


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
      buffer[i] = (unsigned char)(32 + (rand() % 95));
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

  fclose(source);
  fclose(dest);
  return 0;
}

int compareFiles(const char *file1Path, const char *file2Path) {
  FILE *file1 = fopen(file1Path, "rb");
  if (!file1) {
    perror("Error opening first file");
    return -1;
  }

  FILE *file2 = fopen(file2Path, "rb");
  if (!file2) {
    perror("Error opening second file");
    fclose(file1);
    return -1;
  }

  fseek(file1, 0, SEEK_END);
  fseek(file2, 0, SEEK_END);
  long long size1 = ftell(file1);
  long long size2 = ftell(file2);
  rewind(file1);
  rewind(file2);

  if (size1 != size2) {
    fclose(file1);
    fclose(file2);
    return 0;
  }

  unsigned char buffer1[BLOCK_SIZE];
  unsigned char buffer2[BLOCK_SIZE];
  size_t bytesRead1, bytesRead2;

  while ((bytesRead1 = fread(buffer1, 1, BLOCK_SIZE, file1)) > 0) {
    bytesRead2 = fread(buffer2, 1, BLOCK_SIZE, file2);
    if (bytesRead1 != bytesRead2) {
      fclose(file1);
      fclose(file2);
      return 0;
    }

    for (size_t i = 0; i < bytesRead1; i++) {
      if (buffer1[i] != buffer2[i]) {
        fclose(file1);
        fclose(file2);
        return 0;
      }
    }
  }

  if (ferror(file1) || ferror(file2)) {
    perror("Error reading files during comparison");
    fclose(file1);
    fclose(file2);
    return -1;
  }

  fclose(file1);
  fclose(file2);
  return 1;
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


int wipe_blocks(lio_erasure_plan_t *plan, const char *fname, long long int foffset, 
                const char *pname, long long int poffset, int buffer_size,
                int num_wipe_file, int num_wipe_parity, int *indices_wipe_file, int *indices_wipe_parity,
                const char *output_fname, const char *output_pname)
{
    FILE *fd_file, *fd_parity;
    int i, bsize;
    long long int bpos;

    printf("\t Erasing the below blocks on file: %s :", fname);
    for(int i=0; i<num_wipe_file; i++) { printf(" %d",indices_wipe_file[i]); }
    printf("\n");
    printf("\t Erasing the below blocks on parity: %s :",pname);
    for(int i=0; i<num_wipe_parity; i++) { printf(" %d",indices_wipe_parity[i]); }
    printf("\n");

    //** Open the input files
    fd_file = tbx_io_fopen(fname, "r");
    if (fd_file == NULL) {
        printf("\t wipe_blocks: Error opening input data file %s\n", fname);
        return(1);
    }
    //** Open the input files
    fd_parity = tbx_io_fopen(pname, "r");
    if (fd_file == NULL) {
        printf("\t wipe_blocks: Error opening parity file %s\n", fname);
        return(1);
    }

    long long int file_size;
    tbx_io_fseek(fd_file, 0, SEEK_END);
    file_size = tbx_io_ftell(fd_file);
    tbx_io_fseek(fd_file, 0, SEEK_SET);

    long long int parity_size;
    tbx_io_fseek(fd_parity, 0, SEEK_END);
    parity_size = tbx_io_ftell(fd_parity);
    tbx_io_fseek(fd_parity, 0, SEEK_SET);


    buffer_size = (plan->data_strips + plan->parity_strips) * plan->w * plan->packet_size * plan->base_unit;
    bsize = buffer_size / (plan->data_strips + plan->parity_strips);

    bpos = foffset;
    FILE *reg_fd_file = fopen(fname,"rb"); FILE *reg_fd_output_file = fopen(output_fname,"wb");

    while(bpos < file_size){
      for (i = 0; i < plan->data_strips; i++) {
        unsigned char block_buffer[bsize];
        int readsize = fread(block_buffer,1,bsize,reg_fd_file);
        if ( readsize != bsize){
          printf("Error reading block: %lld Readsize: %d \n",bpos,readsize);
        }
        bool should_wipe = false;
        for(int j=0; j<num_wipe_file; j++) { if(i == indices_wipe_file[j]) should_wipe=true; break; }
        if (should_wipe) {
            //printf("Wiping block %d in file \n",i);
            memset(&block_buffer, 0xFF, bsize);
        }
        int writesize = fwrite(block_buffer,1,bsize,reg_fd_output_file);
        if ( writesize != bsize){
          printf("Error writing block: %lld Writesize: %d \n",bpos,writesize); 
        }
        bpos = bpos + bsize;
      }
    }

    bpos = poffset;
    FILE *reg_fd_parity = fopen(pname,"rb"); FILE *reg_fd_output_parity = fopen(output_pname,"wb");
    while(bpos < parity_size){
      for (i = 0; i < plan->parity_strips; i++) {
        unsigned char block_buffer[bsize];
        int readsize = fread(block_buffer,1,bsize,reg_fd_parity);
        if (readsize != bsize){
          printf("Error reading block: %lld Readsize: %d \n",bpos,readsize); 
        }
        bool should_wipe = false;
        for(int j=0; j<num_wipe_file; j++) { if(i == indices_wipe_parity[j]) should_wipe=true; break; }
        if (should_wipe) {
            //printf("Wiping block: %d in Parity file\n",i);
            memset(&block_buffer, 0xFF, bsize);
        }
        int writesize = fwrite(block_buffer,1,bsize,reg_fd_output_parity);
        if (writesize != bsize){
          printf("Error writing block: %lld Writesize: %d \n",bpos,writesize); 
        }
        bpos = bpos + bsize;
      }
    }

    return(0);
}

// Function to print plan details, including decoding matrix
void print_plan(lio_erasure_plan_t *plan) {
    printf("\n=== Erasure Plan Details ===\n");
    printf("Strip size: %lld bytes\n", plan->strip_size);
    printf("Method: %d\n", plan->method);
    printf("Data strips (k): %d\n", plan->data_strips);
    printf("Parity strips (m): %d\n", plan->parity_strips);
    printf("Word size (w): %d\n", plan->w);
    printf("Packet size: %d\n", plan->packet_size);
    printf("Base unit: %d\n", plan->base_unit);

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

    if (plan->encode_bitmatrix) {
        printf("Encoding Bitmatrix (%d x %d, first few elements):\n",
               plan->parity_strips * plan->w, plan->data_strips * plan->w);
        for (int i = 0; i < plan->parity_strips * plan->w && i < 4; i++) {
            for (int j = 0; j < plan->data_strips * plan->w && j < 8; j++) {
                printf("%d ", plan->encode_bitmatrix[i * plan->data_strips * plan->w + j]);
            }
            printf("\n");
        }
    } else {
        printf("Encoding Bitmatrix: Not available\n");
    }

    if (plan->encode_schedule) {
        printf("Encoding Schedule (first few entries):\n");
        for (int i = 0; i < plan->parity_strips && i < 2; i++) {
            printf("Parity %d: ", i);
            int *row = plan->encode_schedule[i];
            int j = 0;
            while (row[j] != -1 && j < 10) {
                printf("%d ", row[j]);
                j++;
            }
            printf("\n");
        }
    } else {
        printf("Encoding Schedule: Not available\n");
    }

    int n_data_missing = 2;
    int n_parity_missing = 0;
    int *erasures = malloc((n_data_missing + n_parity_missing + 1) * sizeof(int));
    erasures[0] = 0;
    erasures[1] = 1;
    erasures[n_data_missing] = -1;

    printf("Generating Decoding Matrix with erasures: Data[0], Data[1]\n");
    if (plan->form_decoding_matrix) {
        int result = plan->form_decoding_matrix(plan);
        if (result == 0) {
            if (plan->encode_bitmatrix) {
                printf("Decoding Bitmatrix (%d x %d, first few elements):\n",
                       plan->data_strips * plan->w, plan->data_strips * plan->w);
                for (int i = 0; i < plan->data_strips * plan->w && i < 4; i++) {
                    for (int j = 0; j < plan->data_strips * plan->w && j < 8; j++) {
                        printf("%d ", plan->encode_bitmatrix[i * plan->data_strips * plan->w + j]);
                    }
                    printf("\n");
                }
            } else {
                printf("Decoding Bitmatrix: Not available after forming\n");
            }
        } else {
            printf("Failed to form decoding matrix: %d\n", result);
        }
    } else {
        printf("Decoding Matrix: Cannot generate (form_decoding_matrix not present)\n");
    }
    free(erasures);

    printf("Form Encoding Matrix Function: %s\n", plan->form_encoding_matrix ? "Present" : "Not present");
    printf("Form Decoding Matrix Function: %s\n", plan->form_decoding_matrix ? "Present" : "Not present");
    printf("Encode Block Function: %s\n", plan->encode_block ? "Present" : "Not present");
    printf("Decode Block Function: %s\n", plan->decode_block ? "Present" : "Not present");
    printf("========================\n\n");
}

int main(int argc, char **argv) {

    int method = 3;
    int strip_size = 16 * 1024; // 16 KB
    int data_strips = 6;
    int parity_strips = 3;
    int w = 8;
    int packet_size = -1;
    int base_unit = -1;
    size_t file_size;

    printf("Testing JErasure Encoding with Cauchy Good Method, Data Strips: %d, Parity strips: %d, Strip Size: %d \n", data_strips, parity_strips, strip_size);

    printf("1. Generating Random file with size: %lld\n", FILE_SIZE);
    const char *input_file = "random_generated_file.txt";

    if (generateRandomFile(input_file) == 0) {
      file_size = getFileSize(input_file);
      printf("\t File generation completed successfully.\n");
    } else {
      printf("\t File generation failed.\n");
    }

    printf("2. Generating Erasure Plan\n");
    lio_erasure_plan_t *plan = et_generate_plan(data_strips * strip_size, method, data_strips, parity_strips, w, packet_size, base_unit);
    if (!plan) {
        printf("Error creating erasure coding plan!\n");
        return 1;
    }
    printf("\t Erasure plan generated successfully\n");

    printf("3. Encoding data into parity file: parity.file\n");
    char *parity_file = "parity.file";
    int buffer_size = 0;
    long long int file_offset = 0;
    long long int poffset = 0;
    int result = et_encode(plan, input_file, file_offset, parity_file, poffset, buffer_size);
    if (result != 0) {
        printf("\t Encoding failed with result: %d\n", result);
    }
    printf("\t Encoding successful, fragments written to parity.file\n");

    //Print plan details
    //print_plan(plan);

    printf("4. Erasing blocks in data and parity files. \n");
    int num_wipe_data = 2;
    int num_wipe_parity = 1;
    int indices_wipe_file[] = {1, 3};
    int indices_wipe_parity[] = {2};
    const char* erased_file_data = "erased_data.txt";
    const char* erased_file_parity = "erased_parity.txt";

    result = wipe_blocks(plan,
                        input_file,
                        file_offset,
                        parity_file,
                        0,
                        buffer_size,
                        num_wipe_data,
                        num_wipe_parity,
                        indices_wipe_file,
                        indices_wipe_parity,
                        erased_file_data,
                        erased_file_parity);
    if( result == 0) printf("\t Erased blocks successfully.\n");
    else printf("\t Error in erasing blocks.\n");

    int compare_result = compareFiles(input_file,erased_file_data);
    if (compare_result == 0 ) printf("\t Files %s and %s differ\n",input_file,erased_file_data);
    compare_result = compareFiles(parity_file,erased_file_parity);
    if (compare_result == 0 ) printf("\t Files %s and %s differ\n",parity_file,erased_file_parity);


    printf("5. Reconstructing the files\n");
    int erasures_array[num_wipe_data+num_wipe_parity+1];
    for(int i=0; i<num_wipe_data; i++){ erasures_array[i] = indices_wipe_file[i]; }
    for(int i=num_wipe_data; i<num_wipe_data+num_wipe_parity; i++){ erasures_array[i] = indices_wipe_parity[i-num_wipe_data] + plan->data_strips; }
    erasures_array[num_wipe_data+num_wipe_parity]=-1;
    printf("\t Erasures array: ");
    for(int i=0; i<num_wipe_data+num_wipe_parity+1; i++) { printf(" %d",erasures_array[i]); }
    printf("\n");
 
    result = et_decode(plan, file_size, erased_file_data, file_offset, erased_file_parity, poffset, buffer_size,erasures_array);
    if (result != 0) {
        printf("Decoding failed with result: %d\n", result);
        goto cleanup;
    }
    printf("\t Decoding successful\n");
    compare_result = compareFiles(input_file, erased_file_data);
    if (compare_result == 1 ) printf("\t Files %s and %s are the same\n",input_file,erased_file_data);

cleanup:
    et_destroy_plan(plan);

    printf("Test completed.\n");
    return result;
}
