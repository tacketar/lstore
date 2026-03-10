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

//**********************************************************************************
//  This proviudes wrapper routines for APR memopry pools so that it's possible
//    to free all the memory associated with pool to be free'ed back to the kernel
//**********************************************************************************

#include <tbx/apr_pool_wrapper.h>

#include <stdio.h>

//**********************************************************************************
// tbx_apr_pool_create - If no parent is given then it's assumed you want the
//      memory released back to the kernel on destroy so we create an unmanaged pool.
//      This also means if we had extra handlers they won't get called properly.
//      But for all of LStore we don't use those.
//**********************************************************************************

apr_status_t tbx_apr_pool_create(apr_pool_t **new_pool, apr_pool_t *parent)
{
    if (parent == NULL) {
        return(apr_pool_create_unmanaged_ex(new_pool, NULL, NULL));
    }

    return(apr_pool_create(new_pool, parent));
}

//**********************************************************************************
//  tbx_apr_pool_destroy - This just calls apr_pool_destroy() but is used as a placeholder
//       in case something more sophisticated is needed in the future.
//**********************************************************************************

void tbx_apr_pool_destroy(apr_pool_t *pool)
{

    apr_pool_destroy(pool);
}
