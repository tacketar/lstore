#!/bin/bash

OWNER="root"

mkdir -p osfile/file/_^FA^_  osfile/hardlink/{{0..255},_^FA^_}/_^FA^_

DEFAULT_EXNODE="[exnode]
id=0

#-------------------------------------
#Base LUN device
#-------------------------------------

[segment-10002]
type=lun
ref_count=1
query_default=simple:1:host:1:any:67
n_devices= 9
n_shift = 0
chunk_size = 16388
max_size=0
used_size=0
max_block_size = 1024Mi
excess_block_size = 50Mi

#-------------------------------------
#Jeraseure device
#-------------------------------------

[segment-10001]
type = jerasure
ref_count = 1
segment = 10002
method = cauchy_good
n_data_devs = 6
n_parity_devs = 3
chunk_size = 16384
w = -1
max_parity = 20Mi

#-------------------------------------
#Caching device exported
#-------------------------------------

[segment-10000]
type=cache
ref_count=1
used_size=0
segment=10001

#-------------------------------------

[view]
default=10000
segment=10000
"



echo $OWNER > osfile/file/_^FA^_/system.owner
echo $OWNER > osfile/hardlink/_^FA^_/system.owner

echo "$DEFAULT_EXNODE" > osfile/file/_^FA^_/system.exnode
echo "$DEFAULT_EXNODE" > osfile/hardlink/_^FA^_/system.exnode


