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

#pragma once
#ifndef ACCRE_LIO_MONITOR_H_INCLUDED
#define ACCRE_LIO_MONITOR_H_INCLUDED

#include <tbx/visibility.h>
#include <tbx/monitor.h>

#ifdef __cplusplus
extern "C" {
#endif

//** Just define all the type indices for monitoring for the various toolbox, gop, IBP, IBP server, and LIO tools

#define MON_INDEX_NSSEND  1
#define MON_INDEX_NSRECV  2
#define MON_INDEX_GOP     3
#define MON_INDEX_QUE     4
#define MON_INDEX_HPSEND  5
#define MON_INDEX_HPRECV  6

#define MON_INDEX_IBP    10

#define MON_INDEX_LIO    20
#define MON_INDEX_AUTHN  21
#define MON_INDEX_DS     22
#define MON_INDEX_RS     23
#define MON_INDEX_OS     24
#define MON_INDEX_OSAZ   25
#define MON_INDEX_SEG    26
#define MON_INDEX_FS     27

#ifdef __cplusplus
}
#endif

#endif
