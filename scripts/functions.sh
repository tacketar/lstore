# Helper functions for lstore-release

#
# Globals
#
LSTORE_SCRIPT_BASE=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
LSTORE_RELEASE_BASE=$(cd $(dirname "${LSTORE_SCRIPT_BASE}") && pwd)
LSTORE_TARBALL_ROOT=$LSTORE_RELEASE_BASE/tarballs/
LSTORE_LOCAL_REPOS="toolbox ibp gop lio meta release"
LSTORE_VENV=${LSTORE_RELEASE_BASE}/venv

#
# Informational messages
#
function lstore_message() {
    MESSAGE_TYPE=$1
    shift
    echo "$@" | >&2 sed -e "s,^,$MESSAGE_TYPE: ,g"
}
function fatal() {
    lstore_message FATAL "$@"
    exit 1
}
function note() {
    lstore_message NOTE "$@"
}

#
# Additional helpers
#
function get_repo_master() {
    for VAR in $LSTORE_HEAD_BRANCHES; do
        if [ "${VAR%=*}" == "$1" ]; then
            echo "${VAR##*=}"
        fi
    done
}

function get_repo_source_path() {
    if [[ "${LSTORE_LOCAL_REPOS}" =~ "$1" ]]; then
        echo "$LSTORE_RELEASE_BASE/src/$1"
    else
        echo "$LSTORE_RELEASE_BASE/vendor/$1"
    fi
}

function build_lstore_binary() {
    # In-tree builds (are for chumps)
    #     Make out-of-tree builds by default and then let someone force the
    #     build to the source tree if they're a masochist.
    build_lstore_binary_outof_tree $1 $(pwd) $2
}

function build_lstore_binary_outof_tree() {
    set -e
    TO_BUILD=$1
    SOURCE_PATH=$2
    INSTALL_PREFIX=${3:-${LSTORE_RELEASE_BASE}/build/local}
    BUILD_STATIC=${4:-0}
    case $TO_BUILD in
        jerasure|toolbox|gop|ibp|lio|gridftp)
            EXTRA_ARGS=""
            MAKE_COMMAND="make install"
            if [ $BUILD_STATIC -ne 0 ]; then
                EXTRA_ARGS="-DCMAKE_C_COMPILER=/usr/local//Cellar/llvm36/3.6.2/share/clang-3.6/tools/scan-build/ccc-analyzer"
                MAKE_COMMAND="/usr/local//Cellar/llvm/3.6.2/bin/scan-build -o $(pwd) make"

            fi
            cmake ${SOURCE_PATH} ${EXTRA_ARGS} \
                                 -DCMAKE_PREFIX_PATH=${INSTALL_PREFIX} \
                                 -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX}
            $MAKE_COMMAND
            ;;
        release)
            :
            ;;
        *)
            fatal "Invalid package: $TO_BUILD"
            ;;
    esac

}

function build_lstore_package() {
    set -e
    TO_BUILD=$1
    SOURCE_PATH=${2:-$(get_repo_source_path ${TO_BUILD})}
    TAG_NAME=${3:-test}
    DISTRO_NAME=${4:-undefined}
    case $DISTRO_NAME in
        undefined)
            CPACK_ARG=""
            CMAKE_ARG=""
            NATIVE_PKG=""
            PKG_TYPE=""
            ;;
        ubuntu-*|debian-*)
            CPACK_ARG="-G DEB"
            CMAKE_ARG="-DCPACK_GENERATOR=DEB:TGZ:TBZ2 -DCPACK_SOURCE_GENERATOR=DEB"
            NATIVE_PKG="cp -ra $SOURCE_PATH ./ ; pushd $PACKAGE ; dpkg-buildpackage -uc -us ; popd"
            PKG_TYPE="deb"
            ;;
        centos-*|rockylinux-*)
            CPACK_ARG="-G RPM"
            CMAKE_ARG="-DCPACK_GENERATOR=RPM:TGZ:TBZ2 -DCPACK_SOURCE_GENERATOR=RPM"
            NATIVE_PKG=""
            PKG_TYPE="rpm"
            ;;
        *)
            fatal "Unexpected distro name $DISTRO_NAME"
            ;;
    esac

    case $TO_BUILD in
        jerasure|lio|ibp|gop|toolbox|gridftp|meta)
            # This is gross, but works for now..
            set -x
            cmake -DWANT_PACKAGE:BOOL=ON "-DLSTORE_PROJECT_VERSION=$TAG_NAME"\
                $(echo "$CMAKE_ARG" | tr ':' ';') --debug --verbose $SOURCE_PATH
            set +x
            make package
            ;;
        release)
            case $PKG_TYPE in
                rpm)
                    cmake $(echo "$CMAKE_ARG" | tr ':' ';') --debug --verbose \
                        -DCMAKE_INSTALL_PREFIX="/" $SOURCE_PATH/rpm-release
                    make package
                    ;;
                deb)
                    cmake $(echo "$CMAKE_ARG" | tr ':' ';') --debug --verbose \
                        -DCMAKE_INSTALL_PREFIX="/" $SOURCE_PATH/deb-release
                    make package
                    ;;
                *)
                    :
                    ;;
            esac
            ;;
        *)
            fatal "Invalid package: $TO_BUILD"
            ;;
    esac
}

function get_cmake_tarballs(){
    if [ ! -d ${LSTORE_TARBALL_ROOT} ]; then
        mkdir ${LSTORE_TARBALL_ROOT}
    fi
    curl https://cmake.org/files/v3.3/cmake-3.3.2-Linux-x86_64.tar.gz > \
            ${LSTORE_TARBALL_ROOT}/cmake-3.3.2-Linux-x86_64.tar.gz
}


function build_helper() {
    # Don't copy/paste code twice for build-local and build-external
    set -e
    BUILD_BASE="$LSTORE_RELEASE_BASE/build"
    SOURCE_BASE="$LSTORE_RELEASE_BASE/src"
    VENDOR_BASE="$LSTORE_RELEASE_BASE/vendor"
    mkdir -p $LSTORE_RELEASE_BASE/build/logs

    PREFIX=$LSTORE_RELEASE_BASE/build/local
    check_cmake
    if [ $1 == "STATIC" ]; then
        STATIC=1
        PREFIX="${PREFIX}-static"
        shift
    else
        STATIC=0
    fi

    pushd $BUILD_BASE
    for p in $@; do
        TARGET="${p}"
        if [ $STATIC -ne 0 ]; then
            TARGET="${p}-static"
        fi
        BUILT_FLAG="${PREFIX}/built-$TARGET"
        if [ -e $BUILT_FLAG ]; then
            note "Not building $TARGET, was already built. To change this behavior,"
            note "    remove $BUILT_FLAG"
            continue
        fi
        [ ! -d $TARGET ] && mkdir -p $TARGET
        pushd $TARGET
        SOURCE_PATH=$(get_repo_source_path ${p})
        build_lstore_binary_outof_tree ${p} ${SOURCE_PATH} ${PREFIX} ${STATIC} 2>&1 | tee $LSTORE_RELEASE_BASE/build/logs/${TARGET}-build.log
        [ ${PIPESTATUS[0]} -eq 0 ] || fatal "Could not build ${TARGET}"
        touch $BUILT_FLAG
        popd
    done
    popd
}

function load_github_token() {
    if [ ! -z "${LSTORE_GITHUB_TOKEN+}" ]; then
        return
    elif [ -e $HOME/.lstore_release ]; then
        source $HOME/.lstore_release
    fi
    set +u
    [ -z "${LSTORE_GITHUB_TOKEN}" ] && \
        fatal "Need a github authentication token to perform this action. To get
a token, use the following FAQ (be sure to remove all scopes).

https://help.github.com/articles/creating-an-access-token-for-command-line-use/

This token should be set to \$LSTORE_GITHUB_TOKEN. Alternately, the file
\$HOME/.lstore_release can be used to store secrets. The following will set
your github token only when needed:

export LSTORE_GITHUB_TOKEN=<token from github page>"
    set -u
    return 0

}

function create_release_candidate() {
    # Make a release candidate in the current directory
    PROPOSED_TAG=$1
    PREVIOUS_TAG=$2
    PROPOSED_BRANCH=$3

    PROPOSED_URL=https://github.com/accre/lstore-${REPO}/tree/ACCRE_${PROPOSED_TAG}
    PROPOSED_DIFF=https://github.com/accre/lstore-${REPO}/compare/${PREVIOUS_TAG}...ACCRE_${PROPOSED_TAG}

    # Sanity check things look okay.
    RET="$(get_repo_status $(pwd))"
    GIT=${RET% *}
    CLEAN=${RET##* }
    if [ $CLEAN != "CLEAN" ]; then
        fatal "Package $REPO isn't clean."
    fi
    git show-ref "ACCRE_${PROPOSED_TAG}" &>/dev/null && \
            fatal "The release ${PROPOSED_TAG} already exists"

    if [ ! -z "$(git branch --list  $PROPOSED_BRANCH)" ]; then
        git checkout $PROPOSED_BRANCH
    else
        fatal "Could not find release branch $PROPOSED_BRANCH"
    fi

    NEW_CHANGELOG=$(update_changelog ${PROPOSED_TAG} ${PREVIOUS_TAG})
    git commit CHANGELOG.md -m "Release ${PROPOSED_TAG}

$NEW_CHANGELOG"
    git tag -a "ACCRE_${PROPOSED_TAG}" -m "Release ${PROPOSED_TAG}

$NEW_CHANGELOG"
}

function update_changelog() {
    # Modify the changelog in the current directory
    PROPOSED_TAG=$1
    PREVIOUS_TAG=$2
    CURRENT_TAG=${3:-$(git rev-parse HEAD)}

    # TODO: config this
    UPSTREAM_REMOTE=origin
    ORIGIN_REMOTE=origin
    TARGET_BRANCH=accre-release

    REPO=$(basename $(pwd))

    PROPOSED_URL=https://github.com/accre/lstore-${REPO}/tree/ACCRE_${PROPOSED_TAG}
    PROPOSED_DIFF=https://github.com/accre/lstore-${REPO}/compare/${PREVIOUS_TAG}...ACCRE_${PROPOSED_TAG}

    # Update CHANGELOG.md
    echo -n "# **[$PROPOSED_TAG]($PROPOSED_URL)** $(date '+(%F)')

## Changes ([full changelog]($PROPOSED_DIFF))
$(git log --oneline --no-merges  ${PREVIOUS_TAG}..${CURRENT_TAG} | \
sed 's/^/*  /')

" > CHANGELOG.md
    cat CHANGELOG.md
    git show HEAD:CHANGELOG.md >> CHANGELOG.md

}
function check_cmake(){
    # Obnoxiously, we need cmake 2.8.12 to build RPM, and even Centos7 only
    #   packages 2.8.11
    set +e
    # TODO: Detect architechture
    CMAKE_LOCAL_TARBALL=${LSTORE_TARBALL_ROOT}/cmake-3.3.2-Linux-x86_64.tar.gz
    CMAKE_VERSION=$(cmake --version 2>/dev/null | head -n 1 | awk '{ print $3 }')
    [ -z "$CMAKE_VERSION" ] && CMAKE_VERSION="0.0.0"
    set -e
    INSTALL_PATH=${1:-${LSTORE_RELEASE_BASE}/build}
    IFS="." read -a VERSION_ARRAY <<< "${CMAKE_VERSION}"

    if [ "${VERSION_ARRAY[0]}" -gt 2 ]; then
        # We're good if we're at cmake 3
        return
    fi
    if [[ "${VERSION_ARRAY[1]}" -lt 8 || "${VERSION_ARRAY[2]}" -lt 12 ]]; then
        [ $CMAKE_VERSION == "0.0.0" ] ||  \
            note "Using bundled version of cmake - the system version is too old '$CMAKE_VERSION'" &&
            note "Couldn't find cmake, pulling our own"

        # Download cmake
        if [ ! -d $INSTALL_PATH/cmake ]; then
            if [ ! -e $CMAKE_LOCAL_TARBALL ]; then
                get_cmake_tarballs
            fi
            pushd $INSTALL_PATH
            tar xzf $CMAKE_LOCAL_TARBALL
            mv cmake-3.3.2-Linux-x86_64 cmake
            popd
        fi
        export PATH="$INSTALL_PATH/cmake/bin:$PATH"
    fi
    hash -r
    CMAKE_VERSION=$(cmake --version | head -n 1 |  awk '{ print $3 }')
    note "Bundled version of cmake is version '$CMAKE_VERSION'"
    note "Bundled cmake can be found at $(which cmake)"
}
