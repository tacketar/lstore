#!/bin/bash

#DIR=/data/source/lstore/build/fsck

#OSFILE_CFG=${DIR}/osfile.sh
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

#Make sure the paths all exist
if [ "${NAMESPACE_PREFIX}" == "" ]; then
    echo "ERROR: NAMESPACE_PREFIX is missing! Same as the 'base_path' in the osfile config section for the server."
    exit 1
fi
if [ ! -e ${NAMESPACE_PREFIX} ]; then
    echo "ERROR: NAMESPACE_PREFIX points to ${NAMESPACE_PREFIX} but doesn't exist! Same as the 'base_path' in the osfile config section for the server."
    exit 1
fi

if [ "${#SHARD_PREFIXES[@]}" == "0" ]; then
    echo "ERROR: SHARD_PREFIXES bash array is missing! All the 'shard_prefix' declarations in the osfile config section."
    exit 1
fi
for shard in ${SHARD_PREFIXES[@]}; do
    if [ ! -e ${shard} ]; then
        echo "Shard missing: ${shard}"
        exit 1
    fi
done

# Now seet up the output files
DEDUP=dedup.sh
RM=/usr/bin/rm

WORK_DIR=/tmp/fsck-shards
NAMESPACE_SHARDS=${WORK_DIR}/namespace.log
NAMESPACE_FILE_SHARDS=${WORK_DIR}/namespace-file.log
NAMESPACE_HARDLINK_SHARDS=${WORK_DIR}/namespace-hardlink.log
NAMESPACE_USED_SHARDS=${WORK_DIR}/namespace-used.log

SHARDS_FOUND=${WORK_DIR}/shards-found.log
SHARDS_UNUSED=${WORK_DIR}/shards-unused.log

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

#Symlink helper
sdump() {
    local target
    target=$(readlink -f $1)
    echo "$1 : ${target}"
}

# Moves the orphaned shard to the orphaned directory
to_orphaned() {
    local fname
    local bname
    local dname
    local oname

    fname=${1}

    #Make sure it exists
    stat ${fname} >& /dev/null
    [ "$?" != "0" ] && echo "Unable to stat ${fname}" && exit 1

    #Now split the name to make the orphaned location
    bname=$( basename ${fname} )
    dname=$( dirname ${fname} )
    oname="${dname}/orphaned/${bname}"

    echo "mv ${fname} ${oname}"
    mv ${fname} ${oname}
}

# Moves the orphaned shard back to where it came from
from_orphaned() {
    local fname
    local bname
    local dname
    local oname

    fname=${1}

    #Make sure it exists
    stat ${fname} >& /dev/null
    [ "$?" != "0" ] && echo "Unable to stat ${fname}" && exit 1

    #Now split the name to make the old orphaned location
    bname=$( basename ${fname} )
    dname=$( dirname $( dirname ${fname} ) )
    oname="${dname}/${bname}"

    echo "mv ${fname} ${oname}"
    mv ${fname} ${oname}
}

#Dump all the sharded attribute directories '_^FA^_'
#Normally we just shard whole directorires attributes leaving the file stub back in the namespace.
dump_namespace_file_shards() {
    local f
    find ${NAMESPACE_PREFIX}/file/ -name _^FA^_ -type l | while read f; do sdump ${f}; done > ${NAMESPACE_FILE_SHARDS}
}

#Same but for the hardlinks in the namespace
# For hardlinks each hardlinked *file* object directory is sharded to a hardlink shard attr directory.
dump_namespace_hardlink_shards() {
    local f
    find ${NAMESPACE_PREFIX}/hardlink/ -type l | grep -v orphaned | while read f; do sdump ${f}; done > ${NAMESPACE_HARDLINK_SHARDS}
}

#Shortcut to do both namespace used shards
dump_namespace_shards() {
    dump_namespace_file_shards
    dump_namespace_hardlink_shards
    cat ${NAMESPACE_FILE_SHARDS} ${NAMESPACE_HARDLINK_SHARDS} > ${NAMESPACE_SHARDS}
}

#Dump all the shard directories
dump_all_shards() {
    touch ${SHARDS_FOUND}
    for shard in ${SHARD_PREFIXES[@]}; do
        echo "Processing shard: ${shard}"
        ls -d ${shard}/*/* | grep -vE '(hardlink|orphaned)' >> ${SHARDS_FOUND}
        ls -d ${shard}/*/hardlink/* | grep -v orphaned >> ${SHARDS_FOUND}
    done
}

#Find orphaned directories
find_orphaned() {
    echo "Dumping namespace shards used"
    dump_namespace_shards
    cat ${NAMESPACE_SHARDS} | cut -f2 -d: | sed 's|^ /|/|g' >> ${NAMESPACE_USED_SHARDS}

    echo "Dumping shard directory shards"
    dump_all_shards

    ${DEDUP} ${SHARDS_FOUND} ${NAMESPACE_USED_SHARDS} > ${SHARDS_UNUSED}

    echo "Namespace shards used: $(wc -l ${NAMESPACE_USED_SHARDS}) (${NAMESPACE_USED_SHARDS})"
    echo "Shard prefixes found: $(wc -l ${SHARDS_FOUND}) (${SHARDS_FOUND})"
    echo "Unused shards: $(wc -l ${SHARDS_UNUSED}) (${SHARDS_UNUSED})"
}

#Move to orphaned directory
move_to_orphaned()
{
    local ok
    local f
    echo "Moving shards in ${SHARDS_UNUSED} to orphaned directories"
    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    cat ${SHARDS_UNUSED} | while read f; do to_orphaned $f; done |& tee ${TO_ORPHANED_LOG}
}

#Move FROM the orphaned directory back to it's original location
move_from_orphaned()
{
    local ok
    local f

    if [ "${1}" != "-y" ]; then
        echo "Please enter 'yes' to continue moved orphaned shards back. Any other key aborts."
        read ok

        if [ "${ok}" != "yes" ]; then
            return
        fi
    fi

    for shard in ${SHARD_PREFIXES[@]}; do
        ls -d ${shard}/*/orphaned/* ${shard}/*/hardlink/orphaned/* 2>/dev/null
    done |  while read f; do from_orphaned $f; done | tee ${FROM_ORPHANED_LOG}
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

    for shard in ${SHARD_PREFIXES[@]}; do
        echo "rm -rf ${shard}/*/orphaned/*"
        rm -rf ${shard}/*/orphaned/*
    done
}

#Help text
shard_help() {
    echo "$0 --generate-unused | --to-orphaned [-y] | --from-orphaned [-y] | --purge-orphaned [-y]"
    echo "    --generate-unused  - Generate the unused shards. This can take a while"
    echo "    --to-orphaned      - Take the unsed shards and move them to the orphaned directories for later removal"
    echo "    --from-orphaned    - Move orphaned shards back to their original location"
    echo "    --purge-orphaned   - Permanently remove orphaned shards. This can not be undone"
    echo "    -y                 - The options supporting this flag require ask for an affirmation before continuing unless this flag is provided"
    echo ""
    echo "Output files are in ${WORK_DIR}"
    echo ""
    echo "Namespace prefix: ${NAMESPACE_PREFIX}"
    echo ""
    echo "Configured shard prefixes:"
    for shard in ${SHARD_PREFIXES[@]}; do
        echo "    ${shard}"
    done
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

if [ "$1" == "--generate-unused" ]; then
    work_dir_create
    find_orphaned
elif [ "$1" == "--to-orphaned" ]; then
    move_to_orphaned $2
elif [ "$1" == "--from-orphaned" ]; then
    move_from_orphaned $2
elif [ "$1" == "--purge-orphaned" ]; then
    purge_orphaned $2
else
    shard_help
fi
