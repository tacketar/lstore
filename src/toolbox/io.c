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

#include <dlfcn.h>
#include <tbx/io.h>
#include <tbx/assert_result.h>

//** Pull in all the fn symbols and declare them
#pragma push_macro("TBX_IO_DECLARE_BEGIN")
#pragma push_macro("TBX_IO_DECLARE_END")
#define TBX_IO_DECLARE_BEGIN
#define TBX_IO_DECLARE_END = NULL;
#include <tbx/io_declare.h>
#pragma pop_macro("TBX_IO_DECLARE_END")
#pragma pop_macro("TBX_IO_DECLARE_BEGIN")

//*********************************************************************************
//  tbx_io_init - Init all the IO symbols for use by the libraries.
//        handle - Passed to dlsym for resolotion. Normally RTLD_DEFAULT or RTLD_NEXT
//*********************************************************************************

#define ASSERT_EXISTS(name) if ((TBX_IO_WRAP_NAME(name) == NULL) || (do_overwrite_fn)) { assert_result_not_null(TBX_IO_WRAP_NAME(name) = dlsym(handle, #name)); }

void tbx_io_init(void *handle, int do_overwrite_fn)
{
#include <tbx/io_assign.h>
}

