#!/usr/bin/bash
#--------------------------------------------------------------------------------------
#  lfs_bind_and_nfs_update - Swings the bind and optional NFS mounts to the
#      latest LFS mount
#
#      LOCK_FILE   - Lock file to use, i.e. /lfs_roots/<NS>/lock.bind
#      LIO_INFO    - Location of lio_fuse state dump, i.e. /tmp/lio_info.txt
#      LOG_FILE    - Location of the service log file, i.e. /lfs_roots/<NS>/service.log
#      BIND_MNT    - Bind mount to swing, i.e. /lfs_roots/<NS>/bmnt
#      BIND_TARGET - LFS target for bind mount, i.e. /lfs_roots/<NS>/lmnt
#      NFS_MNT     - Optional BIND_MNT subdirectory used as an NFS export
#--------------------------------------------------------------------------------------

#******************************************************************************
# log_message - Write a log message
#     <message>  - Message to store
#******************************************************************************

log_message() {
    NOW=$(date "+%y-%m-%d-%H-%M-%S")
    echo "${NOW} $*" >> "${LOG_FILE}"
}

#--------------------------------------------------------------------------------------
# fetch_exportfs_info - Stores the NFS exports info for future use
#--------------------------------------------------------------------------------------

fetch_exportfs_info() {
    NFS_EXPORTS=""
    if [ -n "${NFS_MNT}" ]; then
        # Match the export path as a whole field prefix to avoid partial matches
	NFS_EXPORTS=$(grep ${NFS_MNT} /var/lib/nfs/etab | awk '{print $2}' | tr "(" " " | tr -d ")" )
    fi
}


#--------------------------------------------------------------------------------------
# swing_bind_mount - Swings the bind mount
#    NOTE: Assumes fetch_exportfs_info() has been called
#--------------------------------------------------------------------------------------

swing_bind_mount() {
    local target

    target=$(realpath "${BIND_TARGET}")
    if [ -z "${target}" ] || [ ! -d "${target}" ]; then
        echo "ERROR: invalid bind target: ${BIND_TARGET}"
        exit 1
    fi

    # Unmount sequence:
    echo "Stopping NFS export (if configured) before umount of ${BIND_MNT}"
    lfs_exportfs_mgmt unexport

    echo "Lazy-unmounting bind mount: umount -l ${BIND_MNT}"
    if ! umount -l "${BIND_MNT}"; then
       log_message "LFS_AND_NFS_BIND_UPDATE  ERROR umount -l ${BIND_MNT}"
       exit 1
    fi

    echo "Binding latest target: mount --bind ${target} ${BIND_MNT}"
    if ! mount --bind "${target}" "${BIND_MNT}"; then
       echo "ERROR with mount --bind ${target} ${BIND_MNT}"
       log_message "LFS_AND_NFS_BIND_UPDATE  ERROR mount --bind ${target} ${BIND_MNT}"
       exit 1
    fi

    lfs_exportfs_mgmt export
}

#--------------------------------------------------------------------------------------
# lfs_in_use_by_nfs
#   Returns 0 if not in use (safe to swing), 1 if still in use.
#--------------------------------------------------------------------------------------

lfs_in_use_by_nfs() {
    local pid=$1
    local waited=0
    local max_wait=${LIO_DUMP_TIMEOUT:-60}

    # Drop stale dump content so we wait for a fresh USR1 dump
    : > "${LIO_INFO}"

    log_message "Requesting lio_fuse state dump via kill -USR1 ${pid}"
    if ! kill -USR1 "${pid}" 2>/dev/null; then
        # Process is already gone; treat as not in use
        log_message "lio_fuse PID ${pid} is gone; treating as not in use"
        return 0
    fi

    sleep 0.1
    while [ "$(grep -cF 'Thread Pool Concurrency Stats' "${LIO_INFO}" 2>/dev/null || true)" = "0" ]; do
        if ! kill -0 "${pid}" 2>/dev/null; then
            # Process died before producing a dump
            echo "lio_fuse PID ${pid} died before dump completed; treating as not in use"
            return 0
        fi
        if [ "${waited}" -ge "${max_wait}" ]; then
            echo "ERROR: timed out waiting for lio_fuse state dump from PID ${pid} (${max_wait}s)"
            log_message "LFS_AND_NFS_BIND_UPDATE  ERROR dump-timeout pid=${pid}"
            return 2
        fi
        if [ $((waited % 10)) -eq 0 ]; then
            echo "Waiting for lio_fuse dump marker in ${LIO_INFO} (pid=${pid}, waited=${waited}s)"
        fi
        sleep 1
        waited=$((waited + 1))
    done

    # Get all the open file inodes from lio_fuse
    LFS=$(grep -oE 'ino=[0-9]+' "${LIO_INFO}" | cut -f2 -d= | sort -u)
    if [ -z "${LFS}" ]; then
        echo "No open LFS inodes in dump; not in use"
        return 0
    fi

    # Now do the same for NFS client opens
    NFS=$(nfsdclnts -q 2>/dev/null | cut -f1 -d'|' | sort -u)
    echo "NFS=${NFS}"

    INUSE=$(printf '%s\n%s\n' "${NFS}" "${LFS}" | sort | uniq -d | wc -l)
    # trim whitespace from wc output
    INUSE=$(echo "${INUSE}" | tr -d '[:space:]')
    echo "INUSE=${INUSE}"

    if [ "${INUSE}" = "0" ]; then
        return 0
    fi

    return 1
}


#--------------------------------------------------------------------------------------
# lfs_exportfs_mgmt - NFS unexport / re-export of the share
#   unexport: drop active clients for NFS_MNT (from saved etab info)
#   export:   reload exports from /etc/exports with exportfs -ra
#--------------------------------------------------------------------------------------

lfs_exportfs_mgmt() {
    local MODE=$1
    local ip opts

    if [ -z "${NFS_MNT}" ]; then
        return 0
    fi

    if [ "${MODE}" = "export" ]; then
        # Re-apply configured exports (including NFS_MNT) from /etc/exports
        if ! exportfs -ra; then
            echo "ERROR: exportfs -ra failed"
            log_message "LFS_AND_NFS_BIND_UPDATE  ERROR exportfs -ra"
            exit 1
        fi
        return 0
    fi

    # unexport: only if we captured active clients for this path
    if [ -z "${NFS_EXPORTS}" ]; then
        echo "No active NFS exports found for ${NFS_MNT}; skipping unexport"
        return 0
    fi

    while IFS=' ' read -r ip opts; do
        if [ -z "${ip}" ]; then
            continue
        fi
        exportfs -u "${ip}:${NFS_MNT}"
    done <<< "${NFS_EXPORTS}"
}

#--------------------------------------------------------------------------------------
#  main - Main processing loop
#--------------------------------------------------------------------------------------

main() {
    local SHARED INSTANCE_MNT PID LATEST_MNT

    # Get the shared ID from optional mountinfo fields (shared:N / master:N).
    # Match mount point field ($5) exactly to avoid prefix matches.
    SHARED=$(awk -v mnt="${BIND_MNT}" '
        $5 == mnt {
            for (i = 7; i < NF; i++) {
                if ($i == "-") break
                if ($i ~ /^(shared|master):[0-9]+$/) {
                    print $i
                    exit
                }
            }
        }' /proc/self/mountinfo)
    if [ -z "${SHARED}" ]; then
        echo "No existing bind mount to clear"
        echo "mount --bind $(realpath "${BIND_TARGET}") ${BIND_MNT}"
        mount --bind "$(realpath "${BIND_TARGET}")" "${BIND_MNT}"
        log_message "LFS_AND_NFS_BIND_UPDATE  END SIMPLE: mount --bind $(realpath "${BIND_TARGET}") ${BIND_MNT}"
        return 0
    fi

    # Now get the instance mount that belongs to the same peer group
    INSTANCE_MNT=$(awk -v shared="${SHARED}" '
        /instances/ {
            for (i = 7; i < NF; i++) {
                if ($i == "-") break
                if ($i == shared) {
                    print $5
                    exit
                }
            }
        }' /proc/self/mountinfo)
    log_message "Instance MNT is: $INSTANCE_MNT"
    if [ -z "${INSTANCE_MNT}" ]; then
        log_message "Unable to find instance mount: ${BIND_MNT}.  Assuming the lio_fuse process is dead."
        fetch_exportfs_info
        swing_bind_mount
        log_message "LFS_AND_NFS_BIND_UPDATE  END DEAD: mount --bind $(realpath "${BIND_TARGET}") ${BIND_MNT}"
        return 0
    fi

    # Get the PID (take the first match if multiple)
    PID=$(ps ax -o pid=,args= | awk -v mnt="${INSTANCE_MNT}" '
        $0 ~ /lio_fuse/ && index($0, mnt) { print $1; exit }
    ')

    echo "Bind mount info: ${BIND_MNT} (${SHARED}) -> ${INSTANCE_MNT}  (PID:${PID})"

    # Now get the latest instance
    LATEST_MNT=$(realpath "${BIND_TARGET}" 2>/dev/null || true)
    if [ -z "${LATEST_MNT}" ]; then
        echo "ERROR:  Can't determine the latest instance! Path:${BIND_TARGET}"
        return 1
    fi

    echo "Latest mount instance: ${LATEST_MNT}"

    if [ "${LATEST_MNT}" = "${INSTANCE_MNT}" ]; then
        echo "Bind mount already points to the latest.  No need to update."
        log_message "LFS_AND_NFS_BIND_UPDATE  END ALREADY-LATEST"
        return 0
    fi

    if [ -n "${PID}" ]; then
        # Wait until NFS is no longer using inodes from this LFS instance.
        # This runs BEFORE any umount. A plain umount of a busy NFS-exported
        # bind mount will fail/hang, so the script polls until safe.
        #
        # NOTE: do not use "while [ $? != 0 ]" — the test builtin resets $?.
        local wait_rc=0
        local inuse_waited=0
        local inuse_max_wait=${NFS_INUSE_TIMEOUT:-300}

        echo "Checking whether LFS instance is still in use by NFS (pid=${PID})"
        while true; do
            lfs_in_use_by_nfs "${PID}"
            wait_rc=$?
            if [ "${wait_rc}" -eq 0 ]; then
                echo "LFS instance is not in use by NFS; safe to swing"
                break
            fi
            if [ "${wait_rc}" -eq 2 ]; then
                # dump timeout / hard error from helper
                return 1
            fi
            if [ "${inuse_waited}" -ge "${inuse_max_wait}" ]; then
                echo "ERROR: timed out after ${inuse_max_wait}s waiting for NFS clients to release LFS inodes"
                log_message "LFS_AND_NFS_BIND_UPDATE  ERROR nfs-inuse-timeout pid=${PID} waited=${inuse_waited}s"
                return 1
            fi
            echo "Still in use by NFS; retrying in 1s (waited=${inuse_waited}s/${inuse_max_wait}s)"
            sleep 1
            inuse_waited=$((inuse_waited + 1))
        done
    else
        echo "No lio_fuse PID found for ${INSTANCE_MNT}; skipping NFS in-use check"
    fi

    echo "Swinging mount"

    # Get the existing NFS info for the mount and store it for use by unexport/export
    fetch_exportfs_info
    if [ -n "${NFS_MNT}" ]; then
        if [ -z "${NFS_EXPORTS}" ]; then
            echo "WARNING: NFS_MNT=${NFS_MNT} set but no matching exports found in /var/lib/nfs/etab"
            echo "         umount may fail if NFS clients still hold the export"
        else
            echo "Saved NFS exports for later re-export:"
            echo "${NFS_EXPORTS}"
        fi
    fi

    # Now swing the bind to the latest (unexport -> umount -> bind -> export)
    swing_bind_mount

    log_message "LFS_AND_NFS_BIND_UPDATE  END FULL: mount --bind $(realpath "${BIND_TARGET}") ${BIND_MNT}"
}

#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------

if [ -z "${5}" ]; then
    echo "$0 lock_file lio_info log_file bind_mnt bind_target [nfs_mnt]"
    echo "    lock_file   - Lock file to use, i.e. /lfs_roots/<NS>/lock.bind"
    echo "    lio_info    - Location of lio_fuse state dump, i.e. /tmp/lio_info.txt"
    echo "    log_file    - Location of the service log file, i.e. /lfs_roots/<NS>/service.log"
    echo "    bind_mnt    - Bind mount to swing, i.e. /lfs_roots/<NS>/bmnt"
    echo "    bind_target - LFS target for bind mount, i.e. /lfs_roots/<NS>/lmnt"
    echo "    nfs_mnt     - Optional BIND_MNT subdirectory used as an NFS export"
    exit 1
fi

LOCK_FILE="$1"
LIO_INFO="$2"
LOG_FILE="$3"
BIND_MNT="$4"
BIND_TARGET="$5"
NFS_MNT="$6"
NFS_EXPORTS=""

if [ ! -d "${BIND_MNT}" ]; then
    echo "Invalid path: ${BIND_MNT}"
    exit 1
fi

log_message "LFS_AND_NFS_BIND_UPDATE  START $*"

(
    flock -xn 100
    rc=$?
    if [ "${rc}" -ne 0 ]; then
        log_message "LFS_AND_NFS_BIND_UPDATE  BLOCKED  $*"
        echo "BLOCKED: $*"
        exit 0
    fi

    main
) 100>"${LOCK_FILE}"
