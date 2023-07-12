#!/bin/bash

OSFILE_CFG=/etc/lio/osfile.sh

#Make sure the osfile config exists
if [ ! -e ${OSFILE_CFG} ]; then
    echo "Missing osfile configuration! Looking for ${OSFILE_CFG}."
    echo "If it's in a different location please edit this script, $0"
    echo "It should define 2 environment variables:"
    echo "  NAMESPACE_PREFIX - Path to the OSFile root directory.  Same as the 'base_path' in the osfile config section for the server."
    echo "  SHARD_PREFIXES   - A bash ARRAY of all the shard prefixes. All the 'shard_prefix' declarations in the osfile config section."
    exit 1
fi

source ${OSFILE_CFG}

WORK_DIR=/tmp/fsck-hardlinks
TO_ORPHANED=${DIR}/to-orphaned.sh
FROM_ORPHANED=${DIR}/from-orphaned.sh

DEDUP=dedup.sh
RM=/usr/bin/rm

HARDLINK_LOCAL_FILES=${WORK_DIR}/hardlink-local-files.log
HARDLINK_LOCAL_ATTRS=${WORK_DIR}/hardlink-local-attrs.log
HARDLINK_LOCAL_BROKEN=${WORK_DIR}/hardlink-local-broken.log
HARDLINK_LOCAL_PAIRED=${WORK_DIR}/hardlink-local-paired.log
HARDLINK_FILES_BROKEN=${WORK_DIR}/hardlink-files-broken.log
HARDLINK_ATTRS_BROKEN=${WORK_DIR}/hardlink-attrs-broken.log
HARDLINK_PAIRED=${WORK_DIR}/hardlink-paired.log
HARDLINK_ORPHANED=${WORK_DIR}/hardlink-orphaned.log

TO_BROKEN_LOG=${WORK_DIR}/to-broken.log
FROM_BROKEN_LOG=${WORK_DIR}/from-broken.log
TO_ORPHANED_LOG=${WORK_DIR}/to-orphaned.log
FROM_ORPHANED_LOG=${WORK_DIR}/from-orphaned.log

#Move the work directory if needed and make the new work direcotry
work_dir_create() {
    if [ -e ${WORK_DIR} ]; then
        echo "Moving old work directoy out of the way"
        WDIR_OLD="${WORK_DIR}_$(date -I)_$$"
        echo "mv ${WORK_DIR} ${WDIR_OLD}"
        mv ${WORK_DIR} ${WDIR_OLD}
    fi

    mkdir ${WORK_DIR}
}

#Dump all the hardlink info in the hardlink subdirectory
dump_hardlink_info() {
    local f
    local hpath

    hpath=$1

    #This generates a list of all the hardlink file placeholders
    find ${hpath}/* -maxdepth 0 -type f 2>/dev/null | xargs -P1 -I{} basename {} > ${HARDLINK_LOCAL_FILES}

    #Now do the same for the attribute dirs
    find ${hpath}/_^FA^_/* -maxdepth 0 -type d 2>/dev/null |  sed 's/.*_^FA^_//g' > ${HARDLINK_LOCAL_ATTRS}

    #Sort the good and bad
    cat ${HARDLINK_LOCAL_FILES} ${HARDLINK_LOCAL_ATTRS} | sort | uniq -c | grep ' 1 ' | awk '{print $2}' > ${HARDLINK_LOCAL_BROKEN}
    cat ${HARDLINK_LOCAL_FILES} ${HARDLINK_LOCAL_ATTRS} | sort | uniq -c | grep -v ' 1 ' | awk '{print $2}' > ${HARDLINK_LOCAL_PAIRED}

    #Now separate out the bad based on how it's broken
    ${DEDUP} ${HARDLINK_LOCAL_FILES} ${HARDLINK_LOCAL_PAIRED} | awk -v prefix="${hpath}" '{print prefix"/"$1}' >> ${HARDLINK_FILES_BROKEN}
    ${DEDUP} ${HARDLINK_LOCAL_ATTRS} ${HARDLINK_LOCAL_PAIRED} | awk -v prefix="${hpath}/_^FA^_/_^FA^_" '{print prefix$1}' >> ${HARDLINK_ATTRS_BROKEN}
    cat ${HARDLINK_LOCAL_PAIRED} | awk -v prefix="${hpath}" '{print prefix"/"$1}' >>  ${HARDLINK_PAIRED}
}

#Find broken/orphaned hardlinks
find_broken() {
    local path

    touch ${HARDLINK_FILES_BROKEN} ${HARDLINK_ATTRS_BROKEN} ${HARDLINK_PAIRED}

    echo "Finding broken hardlinks"
    for path in $( ls -d ${NAMESPACE_PREFIX}/hardlink/* | grep -v _^FA^_); do
        echo "Processing ${path}"
        dump_hardlink_info ${path}
    done

    echo "Broken hardlink files: $(wc -l ${HARDLINK_FILES_BROKEN})"
    echo "Broken hardlink attrs: $(wc -l ${HARDLINK_ATTRS_BROKEN})"
    echo "Paired hardlink objects: $(wc -l ${HARDLINK_PAIRED})"
}

#Find unused hardlinks
find_orphaned() {
    local hl
    local count

    touch ${HARDLINK_ORPHANED}

    echo "Finding orphaned hardlinks"
    for hl in $( cat ${HARDLINK_PAIRED} ); do
        count=$(stat --format="%h" ${hl})
        if (( ${count} < 2 )); then
            echo ${hl} >> ${HARDLINK_ORPHANED}
        fi
    done

    echo "Orphaned hardlinks: $(wc -l ${HARDLINK_ORPHANED})"
}

#Move to orphaned directory
move_to_orphaned()
{
    local ok
    local hl
    local hpath
    local bname
    local dname
    local opath

    echo "Moving orphaned hardlinks to orphaned directories"
    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi


    for hl in $( cat ${HARDLINK_ORPHANED} ); do
        bname="$(basename ${hl})"
        dname="$(dirname ${hl})"
        opath="${dname}/orphaned"
        hpath="${dname}/_^FA^_/_^FA^_${bname}"
        echo "mv ${hl} ${opath}"
        echo "mv ${hpath} ${opath}"
        mv ${hl} ${opath}
        mv ${hpath} ${opath}
    done
}

#Move FROM the orphaned directory back to it's original location
move_from_orphaned()
{
    local ok
    local hl
    local hpath
    local bname
    local dname

    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue moved orphaned shards back. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    for hl in $(find ${NAMESPACE_PREFIX}/hardlink/*/orphaned/* -maxdepth 0 -type f); do
        bname="$(basename ${hl})"
        dname="$(dirname $(dirname ${hl}) )"
        echo "mv ${hl} ${dname}/"
        echo "mv ${dname}/orphaned/_^FA^_${bname} ${dname}/_^FA^_/"
        mv ${hl} ${dname}/
        mv ${dname}/orphaned/_^FA^_${bname} ${dname}/_^FA^_/
    done
}

#Purge the orphaned directories. This cannot be undone!
purge_orphaned()
{
    local ok
    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue.  Any other key aborts. All orphaned objects will be destroyed. This cannot be undone"
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    echo "rm -rf ${NAMESPACE_PREFIX}/hardlink/*/orphaned/*"
    rm -rf ${NAMESPACE_PREFIX}/hardlink/*/orphaned/*
}

#Move to broken directories
move_to_broken()
{
    local ok
    local hl
    local hpath
    local bname
    local dname
    local opath

    echo "Moving broken hardlinks to the broken directories"
    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi


    for hl in $( cat ${HARDLINK_FILES_BROKEN} ); do
        dname="$(dirname ${hl})"
        opath="${dname}/orphaned"
        echo "mv ${hl} ${opath}"
        mv ${hl} ${opath}
    done

    for hl in $( cat ${HARDLINK_ATTRS_BROKEN} ); do
        bname="$(basename ${hl})"
        dname="$(dirname $(dirname ${hl}) )"
        opath="${dname}/orphaned"
        hpath="${dname}/_^FA^_/_^FA^_${bname}"
        echo "mv ${hl} ${opath}"
        mv ${hl} ${opath}
    done
}

#Move FROM the broken directories back to it's original location
move_from_broken()
{
    local ok
    local hl
    local hpath
    local bname
    local dname

    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue movinvg broken hardlinks back to their original location. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    for hl in $(find ${NAMESPACE_PREFIX}/hardlink/*/orphaned/* -maxdepth 0 -type f); do
        bname="$(basename ${hl})"
        dname="$(dirname $(dirname ${hl}) )"
        echo "mv ${hl} ${dname}/"
        mv ${hl} ${dname}/
    done

    for hl in $(find ${NAMESPACE_PREFIX}/hardlink/*/orphaned/* -maxdepth 0 -type d); do
        bname="$(basename ${hl})"
        dname="$(dirname $(dirname ${hl}) )"
        echo "mv ${hl} ${dname}/_^FA^_/"
        mv ${hl} ${dname}/_^FA^_/
    done
}

#Purge the broken directories. This cannot be undone!
purge_broken()
{
    local ok
    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue.  Any other key aborts. All broken objects will be destroyed. This cannot be undone"
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    echo "rm -rf ${NAMESPACE_PREFIX}/hardlink/*/broken/*"
    rm -rf ${NAMESPACE_PREFIX}/hardlink/*/broken/*
}

#Help text
hardlink_help() {
    echo "$0 --generate-unused-broken | --to-orphaned [-y] | --from-orphaned [-y] | --purge-orphaned [-y]"
    echo "                            | --to-broken [-y]   | --from-broken [-y]   | --purge-broken [-y]"
    echo "    --generate-unused-broken  - Generate the unused/orphaned and broken hardlinks. This can take a while"
    echo "    --to-orphaned      - Take the unsed/orphaned hardlinks and move them to the orphaned directories for later removal"
    echo "    --from-orphaned    - Move orphaned hardlinks back to their original location"
    echo "    --purge-orphaned   - Permanently remove orphaned hardlinks. This can not be undone"
    echo "    --to-broken        - Take the broken hardlinks and move them to the broken directories for later removal"
    echo "    --from-broken      - Move broken hardlinks back to their original location"
    echo "    --purge-broken     - Permanently remove broken hardlinks. This can not be undone"
    echo "    -y                 - The options supporting this flag require ask for an affirmation before continuing unless this flag is provided"
    echo ""
    echo "Namespace prefix: ${NAMESPACE_PREFIX}"
    echo "Output files are in ${WORK_DIR}"
    echo ""
    exit 0
}

#-------------------------------------------------------------------
# Main control
#-------------------------------------------------------------------

if [ "$1" == "" ]; then
    shard_help
fi

echo "Output files are being stored in ${WORK_DIR}"

if [ "$1" == "--generate-unused-broken" ]; then
    work_dir_create
    find_broken
    find_orphaned
elif [ "$1" == "--to-orphaned" ]; then
    move_to_orphaned $2
elif [ "$1" == "--from-orphaned" ]; then
    move_from_orphaned $2
elif [ "$1" == "--purge-orphaned" ]; then
    purge_orphaned $2
elif [ "$1" == "--to-broken" ]; then
    move_to_broken $2
elif [ "$1" == "--from-broken" ]; then
    move_from_broken $2
#elif [ "$1" == "--purge-broken" ]; then
#    purge_broken $2
else
    hardlink_help
fi
