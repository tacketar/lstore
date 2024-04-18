#!/bin/bash

if [ "$2" == "" ]; then
    echo "$0 build-dir install-dir"
    exit 1
fi

BUILD=$1
INSTALL=$2

[ ! -e ${BUILD} ] && echo "BUILD ${BUILD} doesn't exist!!" && exit 1
[ ! -e ${INSTALL} ] && echo "INSTALL ${INSTALL} doesn't exist!!" && exit 1

echo "$0: BUILD=${BUILD}  INSTALL=${INSTALL}"

mkdir -p ${BUILD}/libfuse3-lio
cd ${BUILD}/libfuse3-lio
git clone https://github.com/libfuse/libfuse.git
cd libfuse
sed -i.bak -e "s/project('libfuse3',/project('libfuse3-lio',/" meson.build
cp lib/meson.build lib/meson.build.bak
cat lib/meson.build.bak | sed -e "s/library('fuse3',/library('fuse3-lio',/" | sed -e "s/name: 'fuse3',/name: 'fuse3-lio' ,/" > lib/meson.build
mkdir build
cd build
meson setup ..
meson configure -D prefix=${INSTALL}
ninja -v
ninja install
