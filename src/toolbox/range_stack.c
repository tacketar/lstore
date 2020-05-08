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

//***********************************************************************
//  Routines for managing an ordered non-overlapping list
//***********************************************************************

#include <tbx/append_printf.h>
#include <tbx/fmttypes.h>
#include <tbx/range_stack.h>
#include <tbx/string_token.h>
#include <tbx/stack.h>
#include <tbx/type_malloc.h>

void _range_stack_merge(tbx_stack_t **range_stack_ptr, int overlap_only, int64_t *new_rng);

//*******************************************************************************
// tbx_range_stack_string2range - Converts a string containing a range to a list
//*******************************************************************************

tbx_stack_t *tbx_range_stack_string2range(char *string, char *range_delimiter, int overlap_only)
{
    tbx_stack_t *range_stack = NULL;
    char *token, *bstate;
    int64_t *rng;
    int fin, good;

    if (string == NULL) return(NULL);
    token = strdup(string);
    bstate = NULL;

    do {
        tbx_type_malloc(rng, int64_t, 2);
        good = 1;
        if (sscanf(tbx_stk_escape_string_token((bstate==NULL) ? token : NULL, ":", '\\', 0, &bstate, &fin), I64T, &rng[0]) != 1) { good = 0; break; }
        if (sscanf(tbx_stk_escape_string_token(NULL, range_delimiter, '\\', 0, &bstate, &fin), I64T, &rng[1]) != 1) { good = 0; break; }
        _range_stack_merge(&range_stack, overlap_only, rng);
    } while (fin == 0);

    free(token);
    if (!good) free(rng);

    return(range_stack);
}
//*******************************************************************************
// tbx_range_stack_range2string - Prints the byte range list
//*******************************************************************************

char *tbx_range_stack_range2string(tbx_stack_t *range_stack, char *range_delimiter)
{
    int64_t *rng;
    tbx_stack_ele_t *cptr;
    char *string;
    int used, bufsize;

    if (range_stack == NULL) return(NULL);

    bufsize = tbx_stack_count(range_stack)*(2*20+2);
    tbx_type_malloc(string, char, bufsize);
    string[0] = 0;
    used = 0;
    cptr = tbx_stack_get_current_ptr(range_stack);
    tbx_stack_move_to_top(range_stack);
    while ((rng = tbx_stack_get_current_data(range_stack)) != NULL) {
        tbx_append_printf(string, &used, bufsize, I64T":" I64T "%s", rng[0], rng[1], range_delimiter);
        tbx_stack_move_down(range_stack);
    }

    tbx_stack_move_to_ptr(range_stack, cptr);

    return(string);
}

//*******************************************************************************
//  _range_collapse - Collapses the byte ranges.  Starts processing
//    from the current range and iterates if needed.
//*******************************************************************************

void _range_collapse(tbx_stack_t *range_stack)
{
    int64_t *rng, *trng, hi1;
    int more;

    trng = tbx_stack_get_current_data(range_stack);  //** This is the range just expanded
    hi1 = trng[1]+1;

    tbx_stack_move_down(range_stack);
    more = 1;
    while (((rng = tbx_stack_get_current_data(range_stack)) != NULL) && (more == 1)) {
        if (hi1 >= rng[0]) { //** Got an overlap so collapse
            if (rng[1] > trng[1]) {
                trng[1] = rng[1];
                more = 0;  //** Kick out this is the last range
            }
            tbx_stack_delete_current(range_stack, 0, 1);
        } else {
            more = 0;
        }
    }

//    log_printf(5, "n_ranges=%d\n", tbx_stack_count(range_stack));
}

//*******************************************************************************
// _range_stack_merge_only - Adds and merges the range w/ existing ranges.
//*******************************************************************************

void _range_stack_merge_only(tbx_stack_t **range_stack_ptr, int64_t *new_rng)
{
    int64_t *rng, *prng, trng[2];
    int64_t lo, hi;
    tbx_stack_t *range_stack;

    if (!(*range_stack_ptr)) *range_stack_ptr = tbx_stack_new();
    range_stack = *range_stack_ptr;

    //** If an empty stack can handle it quickly
    if (tbx_stack_count(range_stack) == 0) {
        tbx_stack_push(range_stack, new_rng);
        return;
    }


    //** Find the insertion point
    lo = new_rng[0]; hi = new_rng[1];
    tbx_stack_move_to_top(range_stack);
    prng = NULL;
    while ((rng = tbx_stack_get_current_data(range_stack)) != NULL) {
        if (lo <= rng[0]) break;  //** Got it
        prng = rng;
        tbx_stack_move_down(range_stack);
    }

    if (prng == NULL) {  //** Fudge to get proper logic
        trng[0] = 12345;
        trng[1] = lo - 10;
        prng = trng;
    }

    if (lo <= prng[1]+1) { //** Expand prev range
        if (prng[1] < hi) {
            prng[1] = hi;  //** Extend the range
            if (rng != NULL) {  //** Move back before collapsing.  Otherwise we're at the end and we've already extended the range
                tbx_stack_move_up(range_stack);
                _range_collapse(range_stack);
            }
        }
        free(new_rng);
    } else if (rng != NULL) {  //** Check if overlap on curr range
        if (rng[0] <= hi+1) {  //** Got an overlap
            free(new_rng);
            rng[0] = lo;
            if (rng[1] < hi) {  //** Expanding on the hi side so need to check for collapse
                rng[1] = hi;
                _range_collapse(range_stack);
            }
        } else {  //** No overlap.  This is a new range to insert
            tbx_stack_insert_above(range_stack, new_rng);
        }
    } else {  //** Adding to the end
        tbx_stack_move_to_bottom(range_stack);
        tbx_stack_insert_below(range_stack, new_rng);
    }

    return;
}

//*******************************************************************************
// _range_stack_overlap_only - Adds and merges fully overlapping ranges.
//     Adjacent ranges are not merged together and are
//     instead kept as separate entries. Entries are only merged if they fully
//     overelap an existing entry. Partial overlaps are treated as separate ranges
//     in this case.
//*******************************************************************************

void _range_stack_overlap_only(tbx_stack_t **range_stack_ptr, int64_t *new_rng)
{
    int64_t *rng;
    int64_t lo, hi;
    tbx_stack_t *range_stack;

    if (!(*range_stack_ptr)) *range_stack_ptr = tbx_stack_new();
    range_stack = *range_stack_ptr;

    //** If an empty stack can handle it quickly
    if (tbx_stack_count(range_stack) == 0) {
        tbx_stack_push(range_stack, new_rng);
        return;
    }

    //** Find the insertion point
    lo = new_rng[0]; hi = new_rng[1];
    tbx_stack_move_to_top(range_stack);
    while ((rng = tbx_stack_get_current_data(range_stack)) != NULL) {
        if ((lo >= rng[0]) && (hi <= rng[1])) { //** Complete overlap so kick out
            free(new_rng);
            return;
        }
        if (lo <= rng[0]) { //** Insert here
            tbx_stack_insert_above(range_stack, new_rng);
            return;
        }
        tbx_stack_move_down(range_stack);
    }

    if (rng == NULL) tbx_stack_move_to_bottom(range_stack);
    tbx_stack_insert_below(range_stack, new_rng);

    return;
}

//*******************************************************************************
// _range_stack_merge - Lightweight wrapper
//*******************************************************************************

void _range_stack_merge(tbx_stack_t **range_stack_ptr, int overlap_only, int64_t *new_rng)
{
    if (overlap_only) {
        _range_stack_overlap_only(range_stack_ptr, new_rng);
    } else {
        _range_stack_merge_only(range_stack_ptr, new_rng);
    }
}

//*******************************************************************************
// tbx_range_stack_merge - Adds and merges the range w/ existing ranges
//*******************************************************************************

void tbx_range_stack_merge(tbx_stack_t **range_stack_ptr, int64_t *new_rng)
{
    _range_stack_merge(range_stack_ptr, 0, new_rng);
}


void tbx_range_stack_merge2(tbx_stack_t **range_stack_ptr, int64_t lo, int64_t hi)
{
    int64_t *rng;

    tbx_type_malloc(rng, int64_t, 2);
    rng[0] = lo; rng[1] = hi;
    _range_stack_merge(range_stack_ptr, 0, rng);
    return;
}

//*******************************************************************************
// tbx__range_stack_overlap - Adds and possiblly merges the range w/ existing ranges
//    if the the new range fully overlaps with an existing range. PArtial overlaps
//    are treated as separate ranges.
//*******************************************************************************

void tbx_range_stack_overlap(tbx_stack_t **range_stack_ptr, int64_t *new_rng)
{
    _range_stack_merge(range_stack_ptr, 1, new_rng);
}

void tbx_range_stack_overlap2(tbx_stack_t **range_stack_ptr, int64_t lo, int64_t hi)
{
    int64_t *rng;

    tbx_type_malloc(rng, int64_t, 2);
    rng[0] = lo; rng[1] = hi;
    _range_stack_merge(range_stack_ptr, 1, rng);
    return;
}
