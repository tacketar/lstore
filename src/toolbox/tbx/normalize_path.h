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

#ifndef ACCRE_NORMALIZE_PATH_H_INCLUDED
#define ACCRE_NORMALIZE_APTH_H_INCLUDED

#include <sys/types.h>
#include <regex.h>
#include <tbx/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

TBX_API regex_t *tbx_normalize_check_make();
TBX_API void tbx_normalize_check_destroy(regex_t *r);
TBX_API int tbx_path_is_normalized(regex_t *r, const char *path);
TBX_API char *tbx_normalize_path(const char *path, char *buffer);

#ifdef __cplusplus
}
#endif

#endif