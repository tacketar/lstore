#!/bin/bash
#--------------------------------------------------------------------------------------
#  lfs_bind_and_nfs_update - Swings the bind and optional NFS mounts to the
#      latest LFS mount
#
#      LIO_INFO    - Location of lio_fuse state dump, i.e. /tmp/lio_info.txt
#      LOG_FILE    - Location ofthe service log file, i.e. /lfs_roots/<NS>/service.log
#      BIND_MNT    - Bind mount to swing, i.e. /lfs_roots/<NS>/bmnt
#      BIND_TARGET - LFS target for bind mount, i.e. /lfs_roots/<NS>/lmnt
#      NFS_MNT     - Optional BIND_MNT subdirectoy used as an NFS export
#--------------------------------------------------------------------------------------

#******************************************************************************
# log_message - Write a log message
#     <message>  - Message to store
#******************************************************************************

log_message() {
    NOW=$(date "+%y-%m-%d-%H-%M-%S")
    echo "${NOW} $*" >> ${LOG_FILE}
}

#--------------------------------------------------------------------------------------
# fetch_exportfs_info - Stores the NFS exports info for future use
#--------------------------------------------------------------------------------------

fetch_exportfs_info() {
    if [ "${NFS_MNT}" != "" ]; then
        NFS_EXPORTS=$(grep ${NFS_MNT} /var/lib/nfs/etab | awk '{print $2}' | tr "(" " " | tr -d ")" )
    fi
}


#--------------------------------------------------------------------------------------
# swing_bind_mount - Swings the bind mount
#    NOTE: Assumes fetch_exportfs_info() has been called
#--------------------------------------------------------------------------------------

swing_bind_mount() {
    lfs_exportfs_mgmt unexport
    #exit 1
    umount ${BIND_MNT}
    if [ "$?" != "0" ]; then
       echo "ERROR with umount ${BIND_MNT}"
       exit 1
    fi
    mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}
    lfs_exportfs_mgmt export
}

#--------------------------------------------------------------------------------------
# lfs_in_use_by_nfs
#--------------------------------------------------------------------------------------

lfs_in_use_by_nfs() {
    PID=$1
    kill -USR1 ${PID}
    sleep 0.1
    while [ $(grep -cF 'Thread Pool Concurrency Stats' ${LIO_INFO}) == "0" ]; do
        sleep 1
    done

    #Get all the open files inodes
    LFS=$(grep ino= ${LIO_INFO} | awk '{print $1}' | cut -f2 -d=)
    if [[ -z "${LFS}" ]]; then
        return 0
    fi

    #Now do the same for NFS
    NFS=$(nfsdclnts -q | cut -f1 -d\| | sort | uniq)
    echo "NFS=${NFS}"

    INUSE=$(echo -e "${NFS}\n${LFS}" | sort | uniq -d | wc -l)
    echo "INUSE=${INUSE}"

    if [ "${INUSE}" == "0" ]; then
        return 0
    fi

    return 1
}


#--------------------------------------------------------------------------------------
# lfs_exportfs_mgmt - Does a manual NFS unexport/export of the share
#--------------------------------------------------------------------------------------

lfs_exportfs_mgmt() {
    MODE=$1

    if [ "${NFS_MNT}" == "" ]; then
        return;
    fi

    echo "${NFS_EXPORTS}" | while IFS=$' ' read ip opts; do
#        echo "IP=${ip}  OPTS=${opts}"

        #export
        if [ "${MODE}" == "export" ]; then
            echo "exportfs -o ${opts} ${ip}:${NFS_MNT}"
            exportfs -o ${opts} ${ip}:${NFS_MNT}
        else #unexport
            echo "exportfs -u ${ip}:${NFS_MNT}"
            exportfs -u ${ip}:${NFS_MNT}
        fi
    done
}

#--------------------------------------------------------------------------------------
#  main - Main processing loop
#--------------------------------------------------------------------------------------

main() {
    #Get the shared ID
    SHARED=$(grep -F "/ ${BIND_MNT}" /proc/self/mountinfo | cut -f7 -d\ )
    if [ "${SHARED}" == "" ]; then
        echo "No existing bind mount to clear"
        echo "mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}"
        mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}
        log_message "LFS_AND_NFS_BIND_UPDATE  END SIMPLE: mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}"
        return 0
    fi

    #Now get the instance
    INSTANCE_MNT=$(grep ${SHARED} /proc/self/mountinfo  | grep instances | cut -f5 -d\ )
    if [ "${INSTANCE_MNT}" == "" ]; then
        echo "Unable to find instance mount: ${BIND_MNT}.  Assuming the lio_fuse process is dead."
        fetch_exportfs_info
        swing_bind_mount
        log_message "LFS_AND_NFS_BIND_UPDATE  END DEAD: mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}"
        return 0
    fi

    #Get the PID
    PID=$(ps agux | grep lio_fuse | grep ${INSTANCE_MNT} | grep -v grep  | awk '{print $2}')

    echo "Bind mount info: ${BIND_MNT} (${SHARED}) -> ${INSTANCE_MNT}  (PID:${PID})"

    #Now get the latest instance
    LATEST_MNT=$(realpath ${BIND_TARGET})
    if [ "${LATEST_MNT}" == "" ]; then
        echo "ERROR:  Can't determine the latest instance! Path:${BIND_TARGET}"
        return 1
    fi

    echo "Latest mount instance: ${LATEST_MNT}"

    if [ "${LATEST_MNT}" == "${INSTANCE_MNT}" ]; then
        echo "Bind mounts already points to the latest.  No need to update."
        log_message "LFS_AND_NFS_BIND_UPDATE  END ALREADY-LATEST"
        return 0
    fi

    if [ "${PID}" != "" ]; then
        #Wait for to clear
        lfs_in_use_by_nfs ${PID}
        while [ "$?" != "0" ]; do
            sleep 1
            lfs_in_use_by_nfs ${PID}
        done
    fi

    echo "Swinging mount"
    #return 1

    #Get the existing NFS info for the mount and store it for use by the unexport/export
    fetch_exportfs_info
    #return 1

    #Now swing the bind to the latest
    swing_bind_mount

    log_message "LFS_AND_NFS_BIND_UPDATE  END FULL: mount --bind $(realpath ${BIND_TARGET}) ${BIND_MNT}"
}

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

if [ "${5}" == "" ]; then
    echo "$0 lio_info lock_file log_file bind_mnt bind_target [nfs_mnt]"
    echo "    lock_file   - Lock file to use, i.e. /lfs_roots/<NS>/lock.bind"
    echo "    lio_info    - Location of lio_fuse state dump, i.e. /tmp/lio_info.txt"
    echo "    log_file    - Location ofthe service log file, i.e. /lfs_roots/<NS>/service.log"
    echo "    bind_mnt    - Bind mount to swing, i.e. /lfs_roots/<NS>/bmnt"
    echo "    bind_target - LFS target for bind mount, i.e. /lfs_roots/<NS>/lmnt"
    echo "    nfs_mnt     - Optional BIND_MNT subdirectoy used as an NFS export"
    exit 1
fi

LOCK_FILE="$1"
LIO_INFO="$2"
LOG_FILE="$3"
BIND_MNT="$4"
BIND_TARGET="$5"
NFS_MNT="$6"

if [ ! -d ${BIND_MNT} ]; then
    echo "Invalid path: ${BIND_MNT}"
    exit 1
fi

log_message "LFS_AND_NFS_BIND_UPDATE  START $*"

(
    flock -xn 100
    if [ $? -eq 1 ]; then
        log_message "FS_AND_NFS_BIND_UPDATE  BLOCKED  $*"
        echo "BLOCKED: $*"
        exit 0
    fi

    main
) 100>${LOCK_FILE}
