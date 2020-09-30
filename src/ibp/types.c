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

#define _log_module_index 134

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tbx/assert_result.h>
#include <tbx/string_token.h>
#include <tbx/type_malloc.h>
#include <tbx/log.h>
#include <time.h>

#include "misc.h"
#include "op.h"
#include "types.h"

//*****************************************************************
//  ibp_depot_new -Creates a new ibp_depot_t structure
//*****************************************************************

ibp_depot_t *ibp_depot_new()
{
    return((ibp_depot_t *)malloc(sizeof(ibp_depot_t)));
}


//*****************************************************************
//  destroy_ibp_depot - Destrots the ibp_depot_t structure
//*****************************************************************

void ibp_depot_destroy(ibp_depot_t *d)
{
    free(d);
}


//*****************************************************************
//  ibp_depot_set - Initializes an ibp_depot_struct
//*****************************************************************

void ibp_depot_set(ibp_depot_t *d, char *host, int port, ibp_rid_t rid)
{
    strncpy(d->host, host, sizeof(d->host));
    d->host[sizeof(d->host)-1]='\0';

    d->port = port;
    d->rid = rid;
}

//*****************************************************************
//  ibp_cap2depot - Takes a capability and strips out the depot bits and stores it
//*****************************************************************

void ibp_cap2depot(const ibp_cap_t *cap, ibp_depot_t *d)
{
    char host[MAX_HOST_SIZE];
    char *bstate;
    char *rid;
    int port, finished;

    host[MAX_HOST_SIZE-1] = '\0';
    host[0] = '\0';
    port = -1;

    char *temp = strdup(cap);
    char *ptr;
    tbx_stk_string_token(temp, "/", &bstate, &finished); //** gets the ibp:/
    ptr = tbx_stk_string_token(NULL, ":", &bstate, &finished); //** This should be the hostname
    ptr = &(ptr[1]);  //** Skip the extra "/"
    sscanf(tbx_stk_string_token(NULL, "/", &bstate, &finished), "%d", &port);
    strncpy(host,  ptr, MAX_HOST_SIZE-1); //** This should be the host name

    ptr = tbx_stk_string_token(NULL, "#", &bstate, &finished); //** This should be the RID
    rid = tbx_stk_unescape_text('\\', ptr);

    ibp_depot_set(d, host, port, ibp_str2rid(rid));

    free(temp); //** Cleanup
    free(rid);
}

//===================================================================

//*****************************************************************
//  new_ibp_attributes -Creates a new ibp_attributes_t structure
//*****************************************************************

ibp_attributes_t *new_ibp_attributes()
{
    return((ibp_attributes_t *)malloc(sizeof(ibp_attributes_t)));
}


//*****************************************************************
//  destroy_ibp_attributes - Destrots the ibp_attributes_t structure
//*****************************************************************

void destroy_ibp_attributes(ibp_attributes_t *attr)
{
    free(attr);
}

//*****************************************************************
// ibp_attributes_set - Initializes the data structure
//*****************************************************************

void ibp_attributes_set(ibp_attributes_t *attr, time_t duration, int reliability, int type)
{
    attr->duration = duration;
    attr->reliability = reliability;
    attr->type = type;
}

//*****************************************************************
// ibp_attributes_get - Returns the ibp attributes
//*****************************************************************

void ibp_attributes_get(ibp_attributes_t *attr, time_t *duration, int *reliability, int *type)
{
    *duration = attr->duration;
    *reliability = attr->reliability;
    *type = attr->type;
}


//===================================================================

//*****************************************************************
//  new_ibp_timer -Creates a new ibp_timer_t structure
//*****************************************************************

ibp_timer_t *new_ibp_timer()
{
    ibp_timer_t *t = (ibp_timer_t *)malloc(sizeof(ibp_timer_t));
    if (t == NULL) return(NULL);

    t->ClientTimeout = 0;
    t->ServerSync = 0;

    return(t);
}


//*****************************************************************
//  destroy_ibp_attributes - Destrots the ibp_attributes_t structure
//*****************************************************************

void destroy_ibp_timer(ibp_timer_t *t)
{
    free(t);
}

//*****************************************************************
// ibp_timer_set - Initializes the data structure
//*****************************************************************

void ibp_timer_set(ibp_timer_t *t, int client_timeout, int server_timeout)
{
    t->ClientTimeout = client_timeout;
    t->ServerSync = server_timeout;
}

//*****************************************************************
// ibp_timer_set - Retreives the timer information
//*****************************************************************

void get_ibp_timer(ibp_timer_t *t, int *client_timeout, int *server_timeout)
{
    *client_timeout = t->ClientTimeout;
    *server_timeout = t->ServerSync;
}

//===================================================================


//*****************************************************************
//  ibp_cap_destroy
//*****************************************************************

void ibp_cap_destroy(ibp_cap_t *cap)
{
    free(cap);
}

//*****************************************************************
// dup_ibp_cap - Duplicates an IBP capability
//*****************************************************************

ibp_cap_t *dup_ibp_cap(ibp_cap_t *src)
{
    return(strdup(src));
}

//===================================================================

//*****************************************************************
//  ibp_capset_new -Creates a new ibp_capset_t structure
//*****************************************************************

ibp_capset_t *ibp_capset_new()
{
    ibp_capset_t *c = (ibp_capset_t *)malloc(sizeof(ibp_capset_t));
    if (c == NULL) return(NULL);

    c->readCap = NULL;
    c->writeCap = NULL;;
    c->manageCap = NULL;

    return(c);
}

//*****************************************************************
// ibp_capset_clear - Frees the existing caps in the set
//*****************************************************************

void ibp_capset_clear(ibp_capset_t *caps)
{
    ibp_cap_destroy(caps->readCap);   caps->readCap = NULL;
    ibp_cap_destroy(caps->writeCap);  caps->writeCap = NULL;
    ibp_cap_destroy(caps->manageCap); caps->manageCap = NULL;
}

//*****************************************************************
//  ibp_capset_destroy - Destroys the ibp_capset_t structure
//*****************************************************************

void ibp_capset_destroy(ibp_capset_t *caps)
{
    ibp_capset_clear(caps);
    free(caps);
}

//*****************************************************************
// copy_ibp_cap - Copies a set of IBP capabilities
//*****************************************************************

void copy_ibp_capset(ibp_capset_t *src, ibp_capset_t *dest)
{
    dest->readCap = dup_ibp_cap(src->readCap);
    dest->writeCap = dup_ibp_cap(src->writeCap);
    dest->manageCap = dup_ibp_cap(src->manageCap);
}

//*****************************************************************
// ibp_cap_get - Returns the requested capability
//*****************************************************************

ibp_cap_t *ibp_cap_get(ibp_capset_t *caps, int ctype)
{
    ibp_cap_t *c;

    if (ctype == IBP_READCAP) {
        c = caps->readCap;
    } else if (ctype == IBP_WRITECAP) {
        c = caps->writeCap;
    } else if (ctype == IBP_MANAGECAP) {
        c = caps->manageCap;
    } else {
        c = NULL;
    }

    return(c);
}

//*****************************************************************
// ibp_cap_set - Sets the requested capability and optionally frees the old one
//*****************************************************************

int ibp_cap_set(ibp_capset_t *caps, int ctype, ibp_cap_t *c, int dofree)
{
    int err = 0;

    if (ctype == IBP_READCAP) {
        if (dofree && caps->readCap) free(caps->readCap);
        caps->readCap = c;
    } else if (ctype == IBP_WRITECAP) {
        if (dofree && caps->writeCap) free(caps->writeCap);
        caps->writeCap = c;
    } else if (ctype == IBP_MANAGECAP) {
        if (dofree && caps->manageCap) free(caps->manageCap);
        caps->manageCap = c;
    } else {
        err = 1;
    }

    return(err);
}

//===================================================================

//*****************************************************************
//  new_ibp_proxy_capstatus -Creates a new ibp_proxy_capstatus_t structure
//*****************************************************************

ibp_proxy_capstatus_t *new_ibp_proxy_capstatus()
{
    ibp_proxy_capstatus_t *cs = (ibp_proxy_capstatus_t *)malloc(sizeof(ibp_proxy_capstatus_t));
    if (cs == NULL) return(NULL);


    cs->read_refcount = -1;
    cs->write_refcount = -1;
    cs->offset = 0;
    cs->size = 0;
    cs->duration = 0;

    return(cs);
}


//*****************************************************************
//  destroy_ibp_proxy_capstatus - Destroys the ibp_proxy_capstatus_t structure
//*****************************************************************

void destroy_ibp_proxy_capstatus(ibp_proxy_capstatus_t *cs)
{
    free(cs);
}

//*****************************************************************
// copy_ibp_proxy_capstatus - Copies a the proxy_capstatus info
//*****************************************************************

void copy_ibp_proxy_capstatus(ibp_proxy_capstatus_t *src, ibp_proxy_capstatus_t *dest)
{
    memcpy(dest, src, sizeof(ibp_proxy_capstatus_t));
}

//*****************************************************************
// get_ibp_proxy_capstatus - Returns the requested proxy caps status info
//*****************************************************************

void get_ibp_proxy_capstatus(ibp_proxy_capstatus_t *cs, int *readcount, int *writecount,
                             ibp_off_t *offset, ibp_off_t *size, int *duration)
{
    *readcount = cs->read_refcount;
    *writecount = cs->write_refcount;
    *offset = cs->offset;
    *size = cs->size;
    *duration = cs->duration;
}

//===================================================================

//*****************************************************************
//  new_ibp_capstatus -Creates a new ibp_capstatus_t structure
//*****************************************************************

ibp_capstatus_t *new_ibp_capstatus()
{
    ibp_capstatus_t *cs = (ibp_capstatus_t *)malloc(sizeof(ibp_capstatus_t));
    if (cs == NULL) return(NULL);


    cs->readRefCount = -1;
    cs->writeRefCount = -1;
    cs->currentSize = -1;
    cs->maxSize = 0;
    ibp_attributes_set(&(cs->attrib), 0, -1, -1);

    return(cs);
}


//*****************************************************************
//  ibp_cap_destroystatus - Destroys the ibp_capstatus_t structure
//*****************************************************************

void ibp_cap_destroystatus(ibp_capstatus_t *cs)
{
    free(cs);
}

//*****************************************************************
// copy_ibp_capstatus - Copies a the capstatus info
//*****************************************************************

void copy_ibp_capstatus(ibp_capstatus_t *src, ibp_capstatus_t *dest)
{
    memcpy(dest, src, sizeof(ibp_capstatus_t));
}

//*****************************************************************
// ibp_cap_getstatus - Returns the requested caps status info
//    The attrib pointer is redirected to the CapStatus's attrib
//    field.  As a result it should NOT point to "new"ed attribute
//    structure.
//*****************************************************************

void ibp_cap_getstatus(ibp_capstatus_t *cs, int *readcount, int *writecount,
                       int *current_size, int *max_size, ibp_attributes_t *attrib)
{
    *readcount = cs->readRefCount;
    *writecount = cs->writeRefCount;
    *current_size = cs->currentSize;
    *max_size = cs->maxSize;
    *attrib = cs->attrib;
}

//===================================================================
//  ibp_ridlist_t manipulation routines
//===================================================================

//*****************************************************************

void ridlist_init(ibp_ridlist_t *rlist, int size)
{
    rlist->rl = (ibp_rid_t *)malloc(sizeof(ibp_rid_t)*size);
    FATAL_UNLESS(rlist->rl != NULL);

    rlist->n = size;
}

//*****************************************************************

ibp_ridlist_t *ibp_ridlist_create()
{
  ibp_ridlist_t *rlist;

  tbx_type_malloc_clear(rlist, ibp_ridlist_t, 1);
  return(rlist);
}

//*****************************************************************

void ibp_ridlist_destroy(ibp_ridlist_t *rlist)
{
    free(rlist->rl);
    free(rlist);
}

//*****************************************************************

int ibp_ridlist_size_get(ibp_ridlist_t *rlist)
{
    return(rlist->n);
}

//*****************************************************************

ibp_rid_t ibp_ridlist_element_get(ibp_ridlist_t *rlist, int index)
{
    if (index >= rlist->n) {
        log_printf(0, "ibp_ridlist_element_get:  Invalid index!  size=%d index=%d\n", rlist->n, index);
        ibp_rid_t rid;
        ibp_rid_empty(&rid);
        return(rid);
    }

    return(rlist->rl[index]);
}

//*****************************************************************

char *ibp_rid2str(ibp_rid_t rid, char *buffer)
{
    strncpy(buffer, rid.name, RID_LEN);

    return(buffer);
}

//*****************************************************************

ibp_rid_t ibp_str2rid(char *rid_str)
{
    ibp_rid_t rid;
    strncpy(rid.name, rid_str, RID_LEN);

    return(rid);
}

//*****************************************************************

void ibp_rid_empty(ibp_rid_t *rid)
{
    sprintf(rid->name, "0");
}

//*****************************************************************

int ibp_rid_is_empty(ibp_rid_t rid)
{
    if (strcmp(rid.name, "0") == 0) {
        return(1);
    }

    return(0);
}

//*****************************************************************

int ibp_rid_compare(ibp_rid_t rid1, ibp_rid_t rid2)
{
    return(strncmp(rid1.name, rid2.name, RID_LEN));
}




