/*
   Copyright 2026 Vanderbilt University

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

//*****************************************************************************
//  Tooling for tracking memory usage and profiling.
//    Information is stored in bins based on size:
//         bin memory start size = bin * TBX_MALLOC_BIN_BYTES + TBX_MALLOC_MIN_BYTES
//
//    Bin TBX_MALLOC_BIN_COUNT == Represents everything less than TBX_MALLOC_MIN_BYTES
//    Bin TBX_MALLOC_BIN_COUNT+1 == Represents requests greater than the last bin
//*****************************************************************************

#include <stdlib.h>

#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>

#ifndef TBX_MALLOC_MIN_BYTES
    #define TBX_MALLOC_MIN_BYTES 0    //** Bucket start size
#endif

#ifndef TBX_MALLOC_BIN_BYTES
    #define TBX_MALLOC_BIN_BYTES 128   //** Default to 128 bytes/bucket
#endif

#ifndef TBX_MALLOC_BIN_COUNT
    #define TBX_MALLOC_BIN_COUNT 49152   //** Total number of buckets. This gives a default of 6MB range
#endif

#define TBX_MALLOC_BIN_UNDERFLOW TBX_MALLOC_BIN_COUNT
#define TBX_MALLOC_BIN_OVERFLOW (TBX_MALLOC_BIN_COUNT+1)

typedef struct {  //** Structure holding the memory info for a bin
    tbx_atomic_int_t malloc_count;
    tbx_atomic_int_t free_count;
    tbx_atomic_int_t malloc_bytes;
    tbx_atomic_int_t free_bytes;
} tm_bin_t;

tm_bin_t mem_bin[TBX_MALLOC_BIN_COUNT+2];

typedef struct { //** Memory handle
    int64_t slot;
    int64_t nbytes;
    char data[];
} tm_handle_t;

//*****************************************************************************
//   _tm_slot - Returns the slot to use based on the size
//*****************************************************************************

int64_t _tm_slot(size_t nbytes)
{
    int64_t slot;

#if TBX_MALLOC_MIN_BYTES > 0  //** Without this a warning is generated if TBX_MALLOC_MIN_BYTES == 0
    if (nbytes < TBX_MALLOC_MIN_BYTES) return(TBX_MALLOC_BIN_COUNT);
#else
    if (nbytes == 0) return(TBX_MALLOC_BIN_COUNT);
#endif

    slot = (nbytes - TBX_MALLOC_MIN_BYTES) / TBX_MALLOC_BIN_BYTES;
    if (slot >= TBX_MALLOC_BIN_COUNT) slot = TBX_MALLOC_BIN_COUNT+1;

    return(slot);
}

//*****************************************************************************
//   _tm_malloc - Allocates the requested space, updates the counters, and returns
//       the pointer for the user
//*****************************************************************************

void *_tm_malloc(size_t nbytes)
{
    int64_t slot;
    tm_handle_t *handle;

    handle = malloc(nbytes + sizeof(tm_handle_t));
    if (handle == NULL) return(NULL);

    slot = _tm_slot(nbytes);
    handle->slot = slot;
    handle->nbytes = nbytes;

    tbx_atomic_inc(mem_bin[slot].malloc_count);
    tbx_atomic_add(mem_bin[slot].malloc_bytes, nbytes);

    return(handle->data);
}

//*****************************************************************************
//   _tm_aligned_alloc - Allocates the requested space, based on alignment,
//       updates the counters, and returns the pointer for the user
//
//   NOTE: Not an easy way to do this so we just don't try and hope the calling
//       program can handle it.  The issue is that the requested bytes must be
//       a power of 2 which isn't possible with our extra bits.
//*****************************************************************************

void *_tm_aligned_alloc(size_t alignment, size_t nbytes)
{
    return(_tm_malloc(nbytes));
}

void *WONTWORK_tm_aligned_alloc(size_t alignment, size_t nbytes)
{
    int64_t slot;
    tm_handle_t *handle;

    handle = aligned_alloc(alignment, nbytes + sizeof(tm_handle_t));
    if (handle == NULL) return(NULL);

    slot = _tm_slot(nbytes);
    handle->slot = slot;
    handle->nbytes = nbytes;

    tbx_atomic_inc(mem_bin[slot].malloc_count);
    tbx_atomic_add(mem_bin[slot].malloc_bytes, nbytes);

    return(handle->data);
}

//*****************************************************************************
//   _tm_relloc - Reallocates the requested space, updates the counters, and returns
//       the pointer for the user
//*****************************************************************************

void *_tm_realloc(void *ptr, size_t nbytes)
{
    tm_handle_t *handle;
    int slot;

    handle = (tm_handle_t *)(((char *)ptr) - offsetof(tm_handle_t, data));

    tbx_atomic_inc(mem_bin[handle->slot].free_count);
    tbx_atomic_add(mem_bin[handle->slot].free_bytes, handle->nbytes);

    handle = realloc(handle, nbytes + sizeof(tm_handle_t));
    if (handle == NULL) return(NULL);

    slot = _tm_slot(nbytes);
    handle->slot = slot;
    handle->nbytes = nbytes;

    tbx_atomic_inc(mem_bin[slot].malloc_count);
    tbx_atomic_add(mem_bin[slot].malloc_bytes, nbytes);

    return(handle->data);
}

//*****************************************************************************
//  _tm_free -Releases the memory and updates the counters
//*****************************************************************************

void _tm_free(void *ptr)
{
    tm_handle_t *handle;

    if (ptr == NULL) {  //** check if we have a NULL ptr
        tbx_atomic_inc(mem_bin[TBX_MALLOC_BIN_UNDERFLOW].free_count);
        return;
    }

    handle = (tm_handle_t *)(((char *)ptr) - offsetof(tm_handle_t, data));
//fprintf(stderr, "_tm_free: slot=" I64T " nbytes=" I64T " handle=%p ptr=%p\n", handle->slot, handle->nbytes, handle, ptr);

    tbx_atomic_inc(mem_bin[handle->slot].free_count);
    tbx_atomic_add(mem_bin[handle->slot].free_bytes, handle->nbytes);

    free(handle);
}

//*****************************************************************************
// tbx_type_malloc_print_stats - Prints the memory buffer stats
//*****************************************************************************

void _tm_stats_printf(FILE *fd)
{
    int i;
    int64_t malloc_count, free_count, pending_count;
    int64_t malloc_nbytes, free_nbytes, pending_nbytes;
    int64_t bsize, btotal;
    char ppbuf1[100], ppbuf2[100], ppbuf3[100];
    char ppbuf4[100], ppbuf5[100], ppbuf6[100], ppbuf7[100];
    tm_bin_t *b;


    fprintf(fd, "TBX_TYPE_MALLOC Profiling: min_size=%d n_bins=%d nbytes/bin=%d -------------------------------\n", TBX_MALLOC_MIN_BYTES, TBX_MALLOC_BIN_COUNT, TBX_MALLOC_BIN_BYTES);

    btotal = 0;
    bsize = TBX_MALLOC_MIN_BYTES;
    for (i=0; i<TBX_MALLOC_BIN_COUNT; i++) {
        b = &(mem_bin[i]);
        malloc_count = tbx_atomic_get(b->malloc_count);
        if (malloc_count > 0) {  //** Only print non-zero values
            free_count = tbx_atomic_get(b->free_count);
            pending_count = malloc_count - free_count;
            malloc_nbytes = tbx_atomic_get(b->malloc_bytes);
            free_nbytes = tbx_atomic_get(b->free_bytes);
            pending_nbytes = malloc_nbytes - free_nbytes;
            fprintf(fd, "%d:%s  m_cnt=%s (" I64T ")  f_cnt=%s (" I64T ")  p_cnt=%s (" I64T ")   m_size=%s f_size=%s p_size=%s\n",
                i, tbx_stk_pretty_print_double_with_scale(1024, bsize, ppbuf7),
                tbx_stk_pretty_print_double_with_scale(1024, malloc_count, ppbuf1), malloc_count,
                tbx_stk_pretty_print_double_with_scale(1024, free_count, ppbuf2), free_count,
                tbx_stk_pretty_print_double_with_scale(1024, pending_count, ppbuf3), pending_count,
                tbx_stk_pretty_print_double_with_scale(1024, malloc_nbytes, ppbuf4),
                tbx_stk_pretty_print_double_with_scale(1024, free_nbytes, ppbuf5),
                tbx_stk_pretty_print_double_with_scale(1024, pending_nbytes, ppbuf6));

            btotal = btotal + pending_nbytes;
        }
        bsize = bsize + TBX_MALLOC_BIN_BYTES;
    }

    b = &(mem_bin[TBX_MALLOC_BIN_UNDERFLOW]);
    malloc_count = tbx_atomic_get(b->malloc_count);
    free_count = tbx_atomic_get(b->free_count);
    pending_count = malloc_count - free_count;
    malloc_nbytes = tbx_atomic_get(b->malloc_bytes);
    free_nbytes = tbx_atomic_get(b->free_bytes);
    pending_nbytes = malloc_nbytes - free_nbytes;
    fprintf(fd, "UNDERFLOW (less than %s)  m_cnt=%s (" I64T ")  f_cnt=%s (" I64T ")  p_cnt=%s (" I64T ")   m_size=%s f_size=%s p_size=%s\n",
            tbx_stk_pretty_print_double_with_scale(1024, TBX_MALLOC_MIN_BYTES, ppbuf7),
            tbx_stk_pretty_print_double_with_scale(1024, malloc_count, ppbuf1), malloc_count,
            tbx_stk_pretty_print_double_with_scale(1024, free_count, ppbuf2), free_count,
            tbx_stk_pretty_print_double_with_scale(1024, pending_count, ppbuf3), pending_count,
            tbx_stk_pretty_print_double_with_scale(1024, malloc_nbytes, ppbuf4),
            tbx_stk_pretty_print_double_with_scale(1024, free_nbytes, ppbuf5),
            tbx_stk_pretty_print_double_with_scale(1024, pending_nbytes, ppbuf6));

    b = &(mem_bin[TBX_MALLOC_BIN_OVERFLOW]);
    malloc_count = tbx_atomic_get(b->malloc_count);
    free_count = tbx_atomic_get(b->free_count);
    pending_count = malloc_count - free_count;
    malloc_nbytes = tbx_atomic_get(b->malloc_bytes);
    free_nbytes = tbx_atomic_get(b->free_bytes);
    pending_nbytes = malloc_nbytes - free_nbytes;
    fprintf(fd, "OVERFLOW (greater than %s) m_cnt=%s (" I64T ")  f_cnt=%s (" I64T ")  p_cnt=%s (" I64T ")   m_size=%s f_size=%s p_size=%s\n",
            tbx_stk_pretty_print_double_with_scale(1024, bsize, ppbuf7),
            tbx_stk_pretty_print_double_with_scale(1024, malloc_count, ppbuf1), malloc_count,
            tbx_stk_pretty_print_double_with_scale(1024, free_count, ppbuf2), free_count,
            tbx_stk_pretty_print_double_with_scale(1024, pending_count, ppbuf3), pending_count,
            tbx_stk_pretty_print_double_with_scale(1024, malloc_nbytes, ppbuf4),
            tbx_stk_pretty_print_double_with_scale(1024, free_nbytes, ppbuf5),
            tbx_stk_pretty_print_double_with_scale(1024, pending_nbytes, ppbuf6));
    fprintf(fd, "TBX_TYPE_MALLOC Total allocated: %s (" I64T ") -----------------------------------\n",  tbx_stk_pretty_print_double_with_scale(1024, btotal, ppbuf1), btotal);
    fprintf(fd, "\n");
}


//*****************************************************************************
//  tbx_type_malloc_stats_printf - Prints the malloc stats
//*****************************************************************************

void tbx_type_malloc_stats_printf(FILE *fd)
{
#ifdef ENABLE_TBX_TYPE_MALLOC_PROFILE
    _tm_stats_printf(fd);
#else
    fprintf(fd, "TBX_TYPE_MALLOC memory profiling is DISABLED\n\n");
#endif
}
