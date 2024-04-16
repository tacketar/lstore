#!/bin/bash

#
#  package-internal.sh - Runs within docker container to package LStore
#

#
# Preliminary bootstrapping
#
set -eux
ABSOLUTE_PATH=$(cd `dirname "${BASH_SOURCE[0]}"` && pwd)
source $ABSOLUTE_PATH/functions.sh
umask 0000
echo "Parsing $@"
#
# Argument parsing
#
TARBALL=0
BUILD_LIBFUSE=0
CMAKE_ARGS=""
while getopts ":c:htf" opt; do
    case $opt in
        c)
            CMAKE_ARGS="$CMAKE_ARGS $OPTARG"
            ;;
        t)
            TARBALL=1
            ;;
        f)
            BUILD_LIBFUSE=1
            CMAKE_ARGS="${CMAKE_ARGS} -DCMAKE_PREFIX_PATH=${LSTORE_RELEASE_BASE}/local"
            export ENABLE_FUSE3_LIO=on
            echo "Building libfuse CMAKE_ARGS=${CMAKE_ARGS}"
            ;;
        \?|h)
            1>&2 echo "OPTARG: ${OPTARG:-}"
            1>&2 echo "Usage: $0 [-t] [-f] [-c ARGUMENT] [distribution ...]"
            1>&2 echo "       -t: Produce static tarballs only"
            1>&2 echo "       -f: Build libfuse from latest and statically link"
            1>&2 echo "       -c: Add ARGUMENT to cmake"
            exit 1
            ;;
    esac
done
shift $((OPTIND-1))
PACKAGE_DISTRO=${1:-unknown_distro}
PACKAGE_SUBDIR=$PACKAGE_DISTRO

# todo could probe this from docker variables
REPO_BASE=$LSTORE_RELEASE_BASE/build/package/$PACKAGE_SUBDIR
PACKAGE_BASE=/tmp/lstore-package

#See if we build the latest libfuse
if [ "${ENABLE_FUSE3_LIO}" == "on" ]; then
    export BUILD_FUSE3_LIO=on
    CMAKE_ARGS="$CMAKE_ARGS -DBUILD_FUSE3_LIO=on"
fi

if [[ $TARBALL -eq 0 ]]; then
    case $PACKAGE_DISTRO in
    undefined)
        # TODO Fail gracefully
        ;;
    ubuntu-jammy)
        # switch to gdebi if automatic dependency resolution is needed
        PACKAGE_INSTALL="dpkg -i"
        PACKAGE_SUFFIX=deb
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_DEB_RELEASE=ON"
        export INSTALL_DEB_RELEASE="on"
        export EMAIL=alan.tackett@vanderbilt.edu
        #This is is only way to get JErasure to build.
        export JERASURE_CC=gcc
        ;;
    ubuntu-*|debian-*)
        # switch to gdebi if automatic dependency resolution is needed
        PACKAGE_INSTALL="dpkg -i"
        PACKAGE_SUFFIX=deb
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_DEB_RELEASE=ON"
        export INSTALL_DEB_RELEASE="on"
        export EMAIL=alan.tackett@vanderbilt.edu
        ;;
    centos-*)
        PACKAGE_INSTALL="rpm -i"
        PACKAGE_SUFFIX=rpm
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_YUM_RELEASE=ON"
        export INSTALL_YUM_RELEASE="on"
        ;;
    rockylinux-*)
        PACKAGE_INSTALL="rpm -i"
        PACKAGE_SUFFIX=rpm
        CMAKE_ARGS="$CMAKE_ARGS -DINSTALL_YUM_RELEASE=ON"
        export INSTALL_YUM_RELEASE="on"
        #This is is only way to get JErasure to build.
        if [ "$PACKAGE_DISTRO" == "rockylinux-9" ]; then
            export JERASURE_CC=gcc
        fi
        ;;
    *)
        fatal "Unexpected distro name $PACKAGE_DISTRO"
        ;;
    esac
fi

rm /tmp/build_fuse3_lio-deb || echo "No earlier build_fuse3_lio-deb flag"

if [ "$BUILD_FUSE3_LIO" == "on" ]; then
    #Clean up old installs
    echo "Removing Old build of FUSE3"
    rm -rf ${LSTORE_RELEASE_BASE}/vendor/libfuse3_lio
    rm -rf ${LSTORE_RELEASE_BASE}/build/libfuse3_lio-prefix
    rm -rf ${LSTORE_RELEASE_BASE}/build/src/libfuse3_lio*
    rm -rf ${LSTORE_RELEASE_BASE}/vendor/libfuse3_lio
    touch /tmp/build_fuse3_lio-${PACKAGE_SUFFIX}
fi

note "Telling git that the $LSTORE_RELEASE_BASE is a legit repo location"
git config --global --add safe.directory $LSTORE_RELEASE_BASE

note "Beginning packaging at $(date) for $PACKAGE_SUBDIR"

TAG_NAME="$(cd $LSTORE_RELEASE_BASE && git describe --match 'v*' --exact-match 2>/dev/null || true)"

if [ ! -z "$TAG_NAME" ]; then
    IS_RELEASE=1
else
    IS_RELEASE=0
fi
if [ -z "$TAG_NAME" ]; then
    TAG_NAME="$(cd $LSTORE_RELEASE_BASE &&
                ( git update-index -q --refresh &>/dev/null || true ) && \
                git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                    --match 'v*' | sed 's,^v,,' || true)"
fi
if [ -z "$TAG_NAME" ]; then
    TAG_NAME="0.0.0-$(cd $LSTORE_RELEASE_BASE &&
            ( git update-index -q --refresh &>/dev/null || true ) && \
            git describe --abbrev=32 --dirty="-dev" --candidates=100 \
                --match ROOT --always || true)"
fi

TAG_NAME=${TAG_NAME:-"0.0.0-undefined-tag"}

(cd $LSTORE_RELEASE_BASE && note "$(git status)")
PACKAGE_REPO=$REPO_BASE/$TAG_NAME

set -x
mkdir -p $PACKAGE_BASE/build
cp -r ${LSTORE_RELEASE_BASE}/{scripts,src,vendor,doc,debian,test,cmake,CMakeLists.txt,lstore.spec,VERSION} \
        $PACKAGE_BASE
ln -s ${LSTORE_RELEASE_BASE}/.git $PACKAGE_BASE/.git

echo "Building with CMAKE_ARGS=${CMAKE_ARGS}"
export CMAKE_ARGS

if [[ "${TARBALL:-}" -eq 1 ]]; then
    cd $PACKAGE_BASE/build
    cmake $CMAKE_ARGS ..
    make package
    ls -lah
(
    umask 0000
    mkdir -p $PACKAGE_REPO
    cp *gz $PACKAGE_REPO
)
elif [[ $PACKAGE_SUFFIX == deb ]]; then
    cd $PACKAGE_BASE
    DISTANCE=$(git describe --match 'v*' --long | awk -F '-' '{ print $2 }')
    # Attempt to automatically bump the debian version
    if [ $IS_RELEASE -eq 1 ]; then
        gbp dch --auto --ignore-branch --id-length=8
    else
        gbp dch --auto --snapshot --snapshot-number "$DISTANCE" \
                    --ignore-branch --id-length=8
    fi

    dpkg-buildpackage -uc -us
    (
        umask 000
        mkdir -p $PACKAGE_REPO
        cp -r ../lstore*.{deb,ddeb,tar.*z,changes} $PACKAGE_REPO
        chmod -R u=rwX,g=rwX,o=rwX $PACKAGE_REPO/*
        # Update lstore-release if we built it
        if test -n "$(shopt -s nullglob; set +u; echo lstore-release*.deb)"; then
            cp lstore-release*.deb $REPO_BASE/lstore-release.deb
        fi
    )
else
    cd $PACKAGE_BASE/build
    cmake $CMAKE_ARGS ..
    make $PACKAGE_SUFFIX VERBOSE=1
(
    umask 000
    mkdir -p $PACKAGE_REPO
    cp -r {,s}rpm_output/ $PACKAGE_REPO
    chmod -R u=rwX,g=rwX,o=rwX $PACKAGE_REPO/*
    # Update lstore-release if we built it
    if test -n "$(shopt -s nullglob; set +u; echo lstore-release*.rpm)"; then
        cp lstore-release*.rpm $REPO_BASE/lstore-release.rpm
    fi
)
fi

set +x

note "Done! The new packages can be found in ./build/package/$PACKAGE_SUBDIR"
