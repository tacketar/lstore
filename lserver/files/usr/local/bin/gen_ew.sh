#!/bin/bash

source warmer_helpers.sh

warmer_query_write_errors > /tmp/w
grep -E '^ERROR:' /lio/log/warmer_run.log | sed 's|ERROR: \(.*\)  cap=.*|@:\1|g' | sort | uniq > /tmp/e
cat /tmp/{w,e} | sort | uniq > /tmp/x
wc -l /tmp/{e,w,x}

