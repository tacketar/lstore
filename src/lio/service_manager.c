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
// Routines for managing the automatic data service framework
//***********************************************************************

#define _log_module_index 191

#include <apr.h>
#include <apr_strings.h>
#include <stdlib.h>
#include <tbx/assert_result.h>
#include <tbx/fmttypes.h>
#include <tbx/iniparse.h>
#include <tbx/string_token.h>
#include <tbx/log.h>
#include <tbx/type_malloc.h>

#include "service_manager.h"

#define SF_INT    0
#define SF_DOUBLE 1
#define SF_STRING 2

typedef struct {   //** Service flag info
    int type;        //** flag type
    union {
        int64_t n;
        double d;
        char *string;
    };
} service_flag_t;

typedef struct {   //** Service section
    apr_hash_t *table;   //** Normal Section objects
    apr_hash_t *flags;   //** Section flag objects
} service_section_t;

service_flag_t *_lookup_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name);

//=======================================================================
//  Service flag routines
//=======================================================================

//***********************************************************************
//  add_flag_service - Adds a service flag to the appropriate list
//***********************************************************************

int add_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, int ftype, void *flag)
{
    char *key;
    service_section_t *section;
    service_flag_t *sflag;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        section = apr_pcalloc(sm->pool, sizeof(service_section_t));
        section->table = apr_hash_make(sm->pool);
        section->flags = apr_hash_make(sm->pool);
        key = apr_pstrdup(sm->pool, service_section);
        apr_hash_set(sm->table, key, APR_HASH_KEY_STRING, section);
    }

    sflag = apr_pcalloc(sm->pool, sizeof(service_flag_t));
    sflag->type = ftype;
    if (ftype == SF_INT) {
        sflag->n = *(int64_t *)flag;
    } else if (ftype == SF_DOUBLE) {
        sflag->d = *(double *)flag;
    } else {
        sflag->string = flag;
    }
    key = apr_pstrdup(sm->pool, flag_name);
    apr_hash_set(section->flags, key, APR_HASH_KEY_STRING, sflag);

    apr_thread_mutex_unlock(sm->lock);

    return(0);
}

//***********************************************************************
//  remove_flag_service - Removes a service flag
//***********************************************************************

int remove_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name)
{
    service_section_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) goto finished;

    apr_hash_set(section->flags, flag_name, APR_HASH_KEY_STRING, NULL);

finished:
    apr_thread_mutex_unlock(sm->lock);

    return(0);
}

//***********************************************************************
// lio_add_integer_flag_service - Adds an integer flag
//***********************************************************************

int lio_add_integer_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, int64_t value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
        add_flag_service(sm, service_section, flag_name, SF_INT, &value);
    } else if (flag->type != SF_INT) {  //** Already exists with a different type type
        return(-1);
    } else {      //** Simple update
        flag->n = value;
    }

    return(0);
}

//***********************************************************************
// lio_add_double_flag_service - Adds a double flag
//***********************************************************************

int lio_add_double_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, double value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
        add_flag_service(sm, service_section, flag_name, SF_DOUBLE, &value);
    } else if (flag->type != SF_DOUBLE) {  //** Already exists with a different type type
        return(-1);
    } else {      //** Simple update
        flag->d = value;
    }

    return(0);
}

//***********************************************************************
// lio_add_string_flag_service - Adds a string flag
//***********************************************************************

int lio_add_string_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, char *value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
        add_flag_service(sm, service_section, flag_name, SF_STRING, apr_pstrdup(sm->pool, value));
    } else if (flag->type != SF_DOUBLE) {  //** Already exists with a different type type
        return(-1);
    } else {      //** Simple update
        flag->string = apr_pstrdup(sm->pool, value);
    }

    return(0);
}


//***********************************************************************
// _lookup_flag_service - Returns the currrent object associated with the service flag
//***********************************************************************

service_flag_t *_lookup_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name)
{
    service_flag_t *flag = NULL;
    service_section_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        log_printf(10, "No matching section for section=%s name=%s\n", service_section, flag_name);
        apr_thread_mutex_unlock(sm->lock);
        return(NULL);
    }

    flag = apr_hash_get(section->flags, flag_name, APR_HASH_KEY_STRING);
    if (flag == NULL) {
        log_printf(10, "No matching object for section=%s name=%s\n", service_section, flag_name);
    }
    apr_thread_mutex_unlock(sm->lock);

    return(flag);
}

//***********************************************************************
// lio_lookup_integer_flag_service - Returns an integer flag
//***********************************************************************

int64_t lio_lookup_integer_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, int64_t default_value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
        add_flag_service(sm, service_section, flag_name, SF_INT, &default_value);
        return(default_value);
    }

    if (flag->type != SF_INT) return(default_value);

    return(flag->n);
}

//***********************************************************************
// lio_lookup_double_flag_service - Returns an double flag
//***********************************************************************

double lio_lookup_double_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, double default_value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
            add_flag_service(sm, service_section, flag_name, SF_DOUBLE, &default_value);
        return(default_value);
    }

    if (flag->type != SF_DOUBLE) return(default_value);

    return(flag->d);
}

//***********************************************************************
// lio_lookup_string_flag_service - Returns a string flag
//***********************************************************************

char *lio_lookup_string_flag_service(lio_service_manager_t *sm, char *service_section, char *flag_name, char *default_value)
{
    service_flag_t *flag;

    flag = _lookup_flag_service(sm, service_section, flag_name);
    if (!flag) {
        add_flag_service(sm, service_section, flag_name, SF_STRING, apr_pstrdup(sm->pool, default_value));
        return(default_value);
    }

    if (flag->type != SF_STRING) return(default_value);

    return(flag->string);
}

//***********************************************************************
// lio_load_inip_flag_service - Loads servie flags from the INI file
//***********************************************************************

void lio_load_inip_flag_service(lio_service_manager_t *sm, char *service_section, tbx_inip_file_t *ifd, char *ini_section)
{
    tbx_inip_group_t *ig;
    tbx_inip_element_t *ele;
    char *s, *e, *key, *flag;
    int n;

    //** Find our group
    ig = tbx_inip_group_first(ifd);
    while ((ig != NULL) && (strcmp(ini_section, tbx_inip_group_get(ig)) != 0)) {
        ig = tbx_inip_group_next(ig);
    }

    if (ig == NULL) return;  //** Nothing to do so kick out

    //**Cycle through the tags
    ele = tbx_inip_ele_first(ig);
    while (ele) {
        //** See if we have a flag "type"
        key = tbx_inip_ele_get_key(ele);
        s = index(key, '(');
        if (!s) goto next;
        s++;
        e = index(key, ')');
        if (!e) goto next;
        e--;
        n = e - s + 1;

        if (strncasecmp(s, "int", n) == 0) { //** Got an integer
            n = (s-1) - key;
            flag = strndup(key, n);
            lio_add_integer_flag_service(sm, service_section, flag, tbx_stk_string_get_integer(tbx_inip_ele_get_value(ele)));
            free(flag);
        } if (strncasecmp(s, "double", n) == 0) { //** Got a double
            n = (s-1) - key;
            flag = strndup(key, n);
            lio_add_double_flag_service(sm, service_section, flag, tbx_stk_string_get_double(tbx_inip_ele_get_value(ele)));
            free(flag);
        } if (strncasecmp(s, "string", n) == 0) { //** Got a string
            n = (s-1) - key;
            flag = strndup(key, n);
            lio_add_string_flag_service(sm, service_section, flag, apr_pstrdup(sm->pool, tbx_inip_ele_get_value(ele)));
            free(flag);
        }

next:
        ele = tbx_inip_ele_next(ele);
    }
}

//***********************************************************************
// lio_print_flag_service - Dumps the flag service info to the file
//***********************************************************************

void lio_print_flag_service(lio_service_manager_t *sm, char *service_section, FILE *fd)
{
    service_section_t *section;
    apr_ssize_t hlen;
    apr_hash_index_t *hi;
    service_flag_t *flag;
    char *key;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);

    for (hi=apr_hash_first(NULL, section->flags); hi != NULL; hi = apr_hash_next(hi)) {
        apr_hash_this(hi, (const void **)&key, &hlen, (void **)&flag);
        if (flag->type == SF_INT) {
            fprintf(fd, "%s(int) = " I64T "\n", key, flag->n);
        } else if (flag->type == SF_DOUBLE) {
            fprintf(fd, "%s(double) = %lf\n", key, flag->d);
        } else if (flag->type == SF_STRING) {
            fprintf(fd, "%s(string) = %s\n", key, flag->string);
        }
    }

    apr_thread_mutex_unlock(sm->lock);
}

//=======================================================================
// Core service routines
//=======================================================================

//***********************************************************************
//  remove_service - Removes a service
//***********************************************************************

int remove_service(lio_service_manager_t *sm, char *service_section, char *service_name)
{
    service_section_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) goto finished;

    apr_hash_set(section->table, service_name, APR_HASH_KEY_STRING, NULL);

finished:
    apr_thread_mutex_unlock(sm->lock);

    return(0);
}


//***********************************************************************
//  add_service - Adds a service to the appropriate list
//***********************************************************************

int add_service(lio_service_manager_t *sm, char *service_section, char *service_name, void *service)
{
    char *key;
    service_section_t *section;

    apr_thread_mutex_lock(sm->lock);

    log_printf(15, "adding section=%s service=%s\n", service_section, service_name);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        log_printf(15, "Creating section=%s\n", service_section);
        section = apr_pcalloc(sm->pool, sizeof(service_section_t));
        section->table = apr_hash_make(sm->pool);
        section->flags = apr_hash_make(sm->pool);
        key = apr_pstrdup(sm->pool, service_section);
        apr_hash_set(sm->table, key, APR_HASH_KEY_STRING, section);
    }

    key = apr_pstrdup(sm->pool, service_name);
    apr_hash_set(section->table, key, APR_HASH_KEY_STRING, service);

    apr_thread_mutex_unlock(sm->lock);

    return(0);
}

//***********************************************************************
// lio_lookup_service - Returns the currrent object associated with the service
//***********************************************************************

void *lio_lookup_service(lio_service_manager_t *sm, char *service_section, char *service_name)
{
    void *s;
    service_section_t *section;

    apr_thread_mutex_lock(sm->lock);

    section = apr_hash_get(sm->table, service_section, APR_HASH_KEY_STRING);
    if (section == NULL) {  //** New section so create the table and insert it
        log_printf(10, "No matching section for section=%s name=%s\n", service_section, service_name);
        apr_thread_mutex_unlock(sm->lock);
        return(NULL);
    }

    s = apr_hash_get(section->table, service_name, APR_HASH_KEY_STRING);
    if (s == NULL) {
        log_printf(10, "No matching object for section=%s name=%s\n", service_section, service_name);
    }
    apr_thread_mutex_unlock(sm->lock);

    return(s);
}

//***********************************************************************
// _clone_flags - Clones the flags in the service manager
//***********************************************************************

void _clone_flags(lio_service_manager_t *clone, char *section_name, service_section_t *section)
{
    apr_ssize_t klen;
    apr_hash_index_t *his;
    service_flag_t *sflag;
    char *flag_name;

    for (his = apr_hash_first(NULL, section->flags); his != NULL; his = apr_hash_next(his)) {
        apr_hash_this(his, (const void **)&flag_name, &klen, (void **)&sflag);
        if (sflag->type == SF_INT) {
            add_flag_service(clone, section_name, flag_name, sflag->type, &(sflag->n));
        } else if (sflag->type == SF_DOUBLE) {
            add_flag_service(clone, section_name, flag_name, sflag->type, &(sflag->d));
        } else {
            add_flag_service(clone, section_name, flag_name, sflag->type, apr_pstrdup(clone->pool, sflag->string));
        }
    }
}

//***********************************************************************
// clone_service_manager - Clones an existing SM
//***********************************************************************

lio_service_manager_t *clone_service_manager(lio_service_manager_t *sm)
{
    apr_ssize_t klen;
    lio_service_manager_t *clone;
    apr_hash_index_t *his;
    service_section_t *section, *clone_section;
    char *key;

    //** Make an empty SM
    clone = create_service_manager(sm);

    //** Now cycle through all the tables and copy them
    apr_thread_mutex_lock(sm->lock);
    for (his = apr_hash_first(NULL, sm->table); his != NULL; his = apr_hash_next(his)) {
        apr_hash_this(his, (const void **)&key, &klen, (void **)&section);
        clone_section = apr_pcalloc(clone->pool, sizeof(service_section_t));
        clone_section->table = apr_hash_copy(clone->pool, section->table);
        clone_section->flags = apr_hash_make(clone->pool);
        apr_hash_set(clone->table, apr_pstrdup(clone->pool, key), APR_HASH_KEY_STRING, clone_section);
        _clone_flags(clone, key, section);
    }
    apr_thread_mutex_unlock(sm->lock);

    return(clone);
}

//***********************************************************************
//  destroy_service_manager - Destroys an existing SM.
//***********************************************************************

void destroy_service_manager(lio_service_manager_t *sm)
{
    apr_pool_destroy(sm->pool);
    free(sm);
}


//***********************************************************************
// create_service_manager - Creates a new SM for use
//***********************************************************************

lio_service_manager_t *create_service_manager()
{
    lio_service_manager_t *sm;

    tbx_type_malloc_clear(sm, lio_service_manager_t, 1);

    apr_pool_create(&sm->pool, NULL);
    apr_thread_mutex_create(&sm->lock, APR_THREAD_MUTEX_DEFAULT, sm->pool);

    sm->table = apr_hash_make(sm->pool);

    return(sm);
}


