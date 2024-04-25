#~/bin/bash

#if [ "$1" == "" ]; then
#    echo "$0 prefix"
#    exit 1
#fi

PREFIX=$1

if [ -e ${PREFIX}/build_fuse3_lio-deb ]; then
    cp -a /usr/lib/x86_64-linux-gnu/libfuse3-lio.so*   /tmp/lstore-package/debian/tmp/usr/lib/x86_64-linux-gnu/
fi
