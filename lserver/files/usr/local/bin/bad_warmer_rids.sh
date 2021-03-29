#!/bin/bash

if [ "$1" == "" ]; then
    echo "$0 warmer_log"
    exit 1
fi

grep ERROR: $1 | sed -e 's|.*cap=ibp://||g' | cut -f1 -d# | sort | uniq -c | sort -nr
