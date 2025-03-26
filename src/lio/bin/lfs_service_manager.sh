#!/usr/bin/env bash
#******************************************************************************
#
# lfs_service_manager.sh - Script for managing LFS namespace mounts
#
# Parameters:
#   LFS_NAME       - Mount name
#   LFS_ROOTS      - Prefix for LFS mounts to run out of
#   LFS_CFG        - LFS config to load
#   CLI_CFG        - CLI config to load.  Only used for commands when setting up shortcuts/known_hosts
#   LIO_CREDS      - lio_fuse options for the creds.  This includes any '--fcreds <creds file>' and '-u <user>' needed
#   LIO_PKEY_DIR   - Override the directory looking for the known host file: ${LIO_PKEY_DIR}/known_hosts.
#                    It is passed to systemd so should be of the form '--setenv=LIO_KEY_PREFIX=/dir/of-known-hosts'
#   LIO_OPTS       - LStore options
#   LFS_USER       - Which local user to run the LFS process as (defailt: lfs)
#   FUSE_OPTS      - FUSE options
#   FUSE_PATH      - Path containing FUSE tools -- fusermount, fusermount3 (OPTIONAL)
#   LIMIT_FD       - FD limit (default: 20480)
#   LIMIT_NPROC    - Nprocs/threads ulimit (default: 100000)
#   LIMIT_MEM      - Memory limit (default: 16G)
#   LIMIT_CORE     - Core dump size (default: 0)
#   TCM_ENABLE     - Enable use of TCMalloc if available
#   TCM_LIB        - TCMalloc shared library.  If not specified it will be searched for
#   TCM_OPTS       - TCMalloc options
#   LOG_MAX_GB     - Max amount of space to use for namespace logs
#   LOG_MIN_GB     - Low water mark on when to stop deleting one LOG_MAX_GB has been hit
#   LOG_PREFIX      - Prefix to use for log file names.  Assumes the PID of the lio_fuse is embedded in the name.
#   LOG_SIGNAL     - Signal to trigger a log rotate.  IF blank the LFS mount isn't rotated.
#   CK_TIMEOUT     - Mount health check timeout
#   CK_PENDING_TIMEOUT - Background Mount health check timeout
#   CK_FILE        - File to stat for determining health
#   CK_MEM_GB      - If the mounts memory usage reaches this level then go ahead and cycle the mount if it's the primary
#
# Organization of files
#    ${LFS_ROOTS}
#    ${LFS_ROOTS}/service.log - Logging for restarts, failed mounts, etc
#    ${LFS_ROOTS}/vars.sh     - Environment variables
#    ${LFS_ROOTS}/mnt         - Always points to the current active mount
#    ${LFS_ROOTS}/current     - This is a symlink to the current active instance
#    ${LFS_ROOTS}/instances   - List of all the mount instances. Format: YY-MM-DD-HH-MM-SS
#
#******************************************************************************


#******************************************************************************
#  vars_default - Set the default variabgles
#******************************************************************************

vars_default() {
    #Set the default falues to be overridden by the ENV file
    # don't use trailing slashes for directories
    LFS_NAME=
    LFS_ROOTS=/lfs_roots
    LFS_CFG=
    CLI_CFG=
    LIO_PKEY_DIR=
    LFS_USER=lfs
    LIO_OPTS="-d 1"
    LIO_CREDS="--fcreds /etc/lio/lfs.psk -u lfs"
    FUSE_OPTS="-f -o auto_cache,allow_other,fsname=lfs:lio,max_threads=200"
    FUSE_PATH=""
    LIMIT_FD=20480
    LIMIT_NPROC=100000
    LIMIT_MEM=16G
    LIMIT_CORE=0
    TCM_ENABLE=1
    TCM_LIB=$(ldconfig -p |grep libtcmalloc.so | sort | tail -n 1 | awk '{print $4}')
    TCM_OPTS="--setenv=TCMALLOC_RELEARSE_RATE=5"
    LOG_MAX_GB="10"
    LOG_MIN_GB="5"
    LOG_PREFIX=""
    LOG_SIGNAL=""
    CK_TIMEOUT="30"
    CK_PENDING_TIMEOUT="600"
    CK_FILE=""
    CK_MEM_GB=""
}

#******************************************************************************
# install - Used to make a new base LFS_ROOTS mount point
#    path - New install path
#    user - Local user to own the files
#    default_vars.sh - Existing VARS file to use for setting defaults.
#******************************************************************************

install() {
    if [ "$3" == "" ]; then
        echo "The install sub-command requires arguements as follows:"
        echo "Usage: $0 install <path> <user> [default_vars.sh]"
        echo "   <path>  - New LFS_ROOTS path defining the mount point"
        echo "   <user>  - Local user to run lio_fuse binaries as"
        echo "   default_vars.sh - Existing LFS_ROOTS mount point config to use as a skeleton"
        exit 2;
    fi

    #Set up the parameters
    script_path=$1
    install_target=$2
    user=$3

    # Check if we've already run an install. If so kick out with a warning.
    if [ -e ${install_target} ]; then
        echo "WARNING: It looks like LFS was already installed for ${install_target}.  Kicking out."
        exit 0
    fi

    #Configure the service_manager variables
    vars_default
    LFS_NAME=$(basename ${install_target})
    LFS_ROOTS=${install_target}
    [ -e "$4" ] && source $4
    LFS_USER=${user}
    VARS=${install_target}/vars.sh

    new_script_path="${install_target}/$(basename $script_path)"
    echo "Installing to $install_target and setting up service to be run by user: $user"
    if ! id $user; then
        echo "User $user does not exist, creating the user as a system user that is a fuse group member..."
        useradd -r -s /bin/false -G fuse lfs
    fi
    echo "Note that the current permissions for 'fusermount3' are:"
    ls -l $(which ${FUSE_PATH}/bin/fusermount3)
    echo "if you experience 'fusermount3' permission problems you may want to consider doing something like the following:"
    echo "   chown root:fuse ${FUSE_PATH}/bin/fusermount3"
    echo "   chmod u+s ${FUSE_PATH}/bin/fusermount3"
    echo
    echo "setting up file structure and copying service manager script"
    mkdir -p "${install_target}/instances"
    chmod 0775 ${install_target}
    chmod 0775 ${install_target}/instances
    chown "${user}:" "${install_target}"
    cp $script_path $new_script_path
    if [ ! -f /etc/fuse.conf ]; then
        echo "/etc/fuse.conf doesn't exist, creating it..."
        echo "user_allow_other" >> /etc/fuse.conf
        chown root:lfs /etc/fuse.conf
    fi

    #Now dump them to the new vars file
    echo "LFS_NAME=\"${LFS_NAME}\"" > ${VARS}
    echo "LFS_ROOTS=\"${LFS_ROOTS}\"" >> ${VARS}
    echo "LFS_CFG=\"${LFS_CFG}\"" >> ${VARS}
    echo "CLI_CFG=\"${CLI_CFG}\"" >> ${VARS}
    echo "LIO_CREDS=\"${LIO_CREDS}\"" >> ${VARS}
    echo "LIO_PKEY_DIR=\"${LIO_PKEY_DIR}\"" >> ${VARS}
    echo "LFS_USER=\"${LFS_USER}\"" >> ${VARS}
    echo "LIO_OPTS=\"${LIO_OPTS}\"" >> ${VARS}
    echo "FUSE_OPTS=\"${FUSE_OPTS}\"" >> ${VARS}
    echo "FUSE_PATH=\"${FUSE_PATH}\"" >> ${VARS}
    echo "LIMIT_FD=\"${LIMIT_FD}\"" >> ${VARS}
    echo "LIMIT_NPROC=\"${LIMIT_NPROC}\"" >> ${VARS}
    echo "LIMIT_MEM=\"${LIMIT_MEM}\"" >> ${VARS}
    echo "LIMIT_CORE=\"${LIMIT_CORE}\"" >> ${VARS}
    echo "TCM_ENABLE=\"${TCM_ENABLE}\"" >> ${VARS}
    echo "TCM_OPTS=\"${TCM_OPTS}\"" >> ${VARS}
    echo "TCM_LIB=\"${TCM_LIB}\"" >> ${VARS}
    echo "LOG_MAX_GB=\"${LOG_MAX_GB}\"" >> ${VARS}
    echo "LOG_MIN_GB=\"${LOG_MIN_GB}\"" >> ${VARS}
    echo "LOG_PREFIX=\"${LOG_PREFIX}\"" >> ${VARS}
    echo "LOG_SIGNAL=\"${LOG_SIGNAL}\"" >> ${VARS}
    echo "CK_TIMEOUT=\"${CK_TIMEOUT}\"" >> ${VARS}
    echo "CK_PENDING_TIMEOUT=\"${CK_PENDING_TIMEOUT}\"" >> ${VARS}
    echo "CK_FILE=\"${CK_FILE}\"" >> ${VARS}
    echo "CK_MEM_GB=\"${CK_MEM_GB}\"" >> ${VARS}

    chown "${user}:" "${VARS}"
    chmod +x ${VARS}
    echo "Please edit ${VARS} as needed"
    echo "done"
}

#******************************************************************************
# log_message - Write a log message
#     <message>  - Message to store
#******************************************************************************

log_message() {
    NOW=$(date "+%y-%m-%d-%H-%M-%S")
    echo "${NOW} $*" >> ${LFS_ROOTS}/service.log
}


#******************************************************************************
# update_symlink - Updates the symlink
#       SYMLINK    - New symlink
#       NEW_TARGET - Symlink target
#******************************************************************************

update_symlink() {
    SYMLINK=$1
    SYMLINK_TMP="$1.new"
    NEW_TARGET=$2
    if ([ -e "$SYMLINK" ] && [ ! -L "$SYMLINK" ]) || ([ -e "$SYMLINK_TMP" ] && [ ! -L "$SYMLINK_TMP" ]); then
        echo "WARN: something exists at $SYMLINK or $SYMLINK_TMP that it is not a symlink, skipping"
    else
        # make the change as atomic as possible, mv should be atomic on POSIX systems
        ln -snf "${NEW_TARGET}" "$SYMLINK_TMP"  # need -nf to overwrite with no dereference
        mv -T "$SYMLINK_TMP" "$SYMLINK"     # need -T to stop dereference
    fi
}

#******************************************************************************
# systemd_clear_instance - Clears the lfs@<INSTANCE_ID>.service from systemd failed list
#    INSTANCE_ID - Local instance ID
#******************************************************************************

systemd_clear_instance() {
    INSTANCE_ID=$1

    systecmctl reset-failed lfs@${INSTANCE_ID}.service 2>/dev/null
}

#******************************************************************************
# start_instance - Starts a new instance
#    INSTANCE_ID - Local instance ID
#******************************************************************************

start_instance() {
    INSTANCE_ID=$1
    set_instance_vars $INSTANCE_ID
    mkdir -p "$INSTANCE_PATH"
    chmod 0755 "${INSTANCE_PATH}"
    mkdir -p "$INSTANCE_LOGS"
    mkdir -p "$INSTANCE_MNT"
    mkdir -p "$INSTANCE_CFG"
    date +%s > "$INSTANCE_PATH/created"
    chown -R "${LFS_USER}:" "$INSTANCE_PATH"

    # allow coredumps, set working dir
    cd $WDIR
    echo "Core dumps enabled (unlimited size), working directory is '$(pwd)' and the current destination/handler is '$(cat /proc/sys/kernel/core_pattern)' (set using /proc/sys/kernel/core_pattern)"
    COMMAND="lio_fuse ${FUSE_OPTS} $INSTANCE_MNT --lio -C ${WDIR} ${LFS_CFG} ${LIO_CREDS} ${LIO_OPTS}"
    echo "COMMAND=${COMMAND}"
    # ulimits are done twice, once as root to raise the hard limit and once
    # after the sudo to raise user's soft limit
    ulimit -c ${LIMIT_CORE}
    ulimit -n ${LIMIT_FD}

    if [ "${TCM_ENABLE}" != "1" ]; then   #** Disable TCMalloc use
        TCMOPT=""
    fi

    # Drop set +u in subshell because function library doesn't work
    # This may be centos-specific
    (
        set +u
        DAEMON_COREFILE_LIMIT=${LIMIT_CORE}
        log_message "START_INSTANCE ${INSTANCE_ID}  COMMAND: systemd-run --uid=${LFS_USER} --unit=lfs@${INSTANCE_ID} -p MemoryMax=${LIMIT_MEM} -p LimitNOFILE=${LIMIT_FD} -p LimitNPROC=${LIMIT_NPROC} ${TCMOPT} ${LIO_PKEY_DIR} $COMMAND"
        systemd-run --uid=${LFS_USER} --unit=lfs@${INSTANCE_ID} -p MemoryLimit=${LIMIT_MEM} -p LimitNOFILE=${LIMIT_FD} -p LimitNPROC=${LIMIT_NPROC} ${TCMOPT} ${LIO_PKEY_DIR} $COMMAND
    )
    update_symlink $MOUNT_SYMLINK $INSTANCE_MNT
    update_symlink $CURRENT_SYMLINK $INSTANCE_PATH
    update_symlink $NESTED_CURRENT_SYMLINK $INSTANCE_RELATIVE_MNT
    echo "Instance with ID $INSTANCE_ID based in $INSTANCE_PATH started, link to mount: $MOUNT_SYMLINK"
}

#******************************************************************************
# stop_instance - Stops an instance
#    INSTANCE_ID - Local instance ID
#******************************************************************************

stop_instance() {
    INSTANCE_ID=$1
    log_message "STOP_INSTANCE ${INSTANCE_ID}"
    set_instance_vars $INSTANCE_ID
    ${FUSE_PATH}/bin/fusermount3 -u $INSTANCE_MNT
    status=$?
    if [ "${status}" != "0" ] && [ "${FORCE_UMOUNT}" == "1" ]; then
        ${FUSE_PATH}/bin/fusermount3 -u -z $INSTANCE_MNT
        #FPID=$(ps -eo pid,comm,args | grep ${INSTANCE_ID} | grep fuse | cut -f1 -d" ")
        FPID=$(ps -eo pid,comm,args | grep ${INSTANCE_ID} | grep fuse | awk '{print $1}')
        if [ "${FPID}" != "" ]; then
            kill -9 ${FPID}
        fi
    fi

    #Clear it from the transient SystemD list which shows it as failed
    systemd_clear_instance $INSTANCE_ID
}

#******************************************************************************
# remove_intance - Removes the instance and cleans up
#    INSTANCE_ID - Local instance ID
#******************************************************************************

remove_instance() {
    INSTANCE_ID=$1
    log_message "REMOVE_INSTANCE ${INSTANCE_ID}"
    set_instance_vars $INSTANCE_ID

    # provide some protection against accidental deletion
    if [ "$(id -u)" != "0" ]; then
        local MY_PREFIX="sudo -u $LFS_USER rm "
    fi
    echo Command: ${MY_PREFIX:-} --one-file-system -rf "$INSTANCE_PATH"
    echo "Ctrl+C now to cancel (3 sec)"
    sleep 3
    ${MY_PREFIX:-} rm --one-file-system -rf $INSTANCE_PATH

    #Clear it from the transient SystemD list which shows it as failed
    systemd_clear_instance $INSTANCE_ID
}


#******************************************************************************
# get_stat_instance - Does a stat on the instance and reports the time
#    INSTANCE_ID - Local instance ID
#******************************************************************************

get_stat_instance() {
    INSTANCE_ID=$1
    set_instance_vars $INSTANCE_ID

    TMP=$( $(which time) -p -o ${INSTANCE_LOGS}/stat_time timeout -s 9 ${CK_TIMEOUT} stat ${INSTANCE_MNT}/${CK_FILE} 2>&1 >/dev/null )
    rcode=$?

    if [ "${rcode}" == "0" ]; then
        DT=$(grep real < ${INSTANCE_LOGS}/stat_time | awk '{print $2}')
    elif [ "${rcode}" == "1" ]; then
        DT="MISSING-FILE"
    else
        DT="TIMEOUT"
    fi

    echo "${DT}"
    return;
}

#******************************************************************************
# get_instance_pending_stat - Checks for and returns any pending background
#    stat calls.
#******************************************************************************

get_pending_stat_instance() {
    INSTANCE_ID=$1
    set_instance_vars $INSTANCE_ID
    STATUS=""

    # If nothing pending kick out
    if [ ! -e ${INSTANCE_LOGS}/pending_stat ]; then
        return;
    fi

    # Get the pending info: <stat_dt or PENDING> <Pending start time in epoch sec>
    STAT_DT=$( awk '{print $1}' < ${INSTANCE_LOGS}/pending_stat)
    STIME=$( awk '{print $2}' < ${INSTANCE_LOGS}/pending_stat)

    # Find out how long ago we completed or started
    NOW=$(date +%s)
    DT=$(( NOW - STIME ))

    echo "started=${DT},stat_dt=${STAT_DT}"
    return
}

#******************************************************************************
# get_instance_health - Collects health info of the instance
#    INSTANCE_ID - Local instance ID
#******************************************************************************

get_instance_health() {
    INSTANCE_ID=$1
    set_instance_vars $INSTANCE_ID
    HEALTHY=true
    STATUS=""

    # primary instance?
    if [ "$INSTANCE_PATH" = "$(readlink -f $CURRENT_SYMLINK)" ]; then
        STATUS+=" primary=yes"
     else
        STATUS+=" primary=NO "
    fi

    # Does it exist?
    if [ -e "$INSTANCE_PATH" ]; then
        STATUS+=" exists=yes"
    else
        STATUS+=" exists=NO "
        echo "$STATUS"
        return
    fi

    # Is it mounted?
    if mount | grep -q "$INSTANCE_MNT"; then
        STATUS+=" mounted=yes"
    else
        STATUS+=" mounted=NO "
        HEALTHY=false
    fi

    # Is the filesystem process running?
    PSINFO=$(ps -eo pid,rss,args | grep -v grep | grep ${INSTANCE_ID})
    if [ "${PSINFO}" == "" ]; then
        STATUS+=" is_running=NO"
        echo "$STATUS"
        return
    fi

    # Get the RSS and PID and add it
    IPID=$(echo ${PSINFO} | awk '{print $1}')
    IMEM=$(echo ${PSINFO} | awk '{printf "%.2f", $2/1024}')
    STATUS+=$(printf " %-23s" "is_running=yes(${IPID})")
    STATUS+=$(printf " %-16s" "rss_mb=${IMEM}")

    # Is it in use?
    FILE_USE_COUNT=$($TIMEOUT 10 bash -c "lsof +f --  $INSTANCE_MNT 2>/dev/null | grep -Ev '^COMMAND ' | wc -l" || echo 'TIMEOUT')
    STATUS+=$( printf " %-18s" "files_in_use=${FILE_USE_COUNT}")

    # Do a stat
    STATINFO=$(get_stat_instance ${INSTANCE_ID})
    PENDINGINFO=$(get_pending_stat_instance ${INSTANCE_ID})
    if [ "${PENDINGINFO}" == "" ]; then
        STATUS+=$( printf " %-20s" "stat=${STATINFO}")
    else
        STATUS+=$(printf " %-20s" "stat=${STATINFO}(pending:${PENDINGINFO})")
    fi
    echo "$STATUS"
}

#******************************************************************************
#  generate_instance_id - Generates a new instance ID
#******************************************************************************

generate_instance_id() {
    date "+%y-%m-%d-%H-%M-%S"
}

#******************************************************************************
# set_instance_vars - Sets up the instance variables
#    INSTANCE_ID - Local instance ID
#******************************************************************************

set_instance_vars() {
    INSTANCE_ID=$1
    INSTANCE_PATH="${INSTANCE_ROOT}/${INSTANCE_ID}"
    INSTANCE_LOGS="${INSTANCE_PATH}/logs"
    INSTANCE_MNT="${INSTANCE_PATH}/mnt"
    INSTANCE_CFG="${INSTANCE_PATH}/cfg"
    INSTANCE_RELATIVE_MNT="${INSTANCE_ID}/mnt"  # See NESTED_CURRENT_SYMLINK for description
}

#******************************************************************************
# service_status - Prints the service status and state of all instances
#******************************************************************************

service_status() {
    if [ ! -e "$MOUNT_SYMLINK" ]; then
        PRIMARY_ID='service_is_stopped'
    else
        PRIMARY_ID=$(basename $(readlink -f $MOUNT_SYMLINK))
    fi

    if [ "$PRIMARY_ID" = 'service_is_stopped' ]; then
        echo "Service is stopped"
    else
        echo "Service is running"
    fi

    echo "Status of instances:"
    ls -r $INSTANCE_ROOT | grep -v lfs | while read i; do
        ID=$(basename $i)
        echo "${ID} $(get_instance_health $ID)"
    done
}

#******************************************************************************
# service_start - Starts the service
#******************************************************************************

service_start() {
    log_message "START_SERVICE"
    PRIMARY_ID=$(basename $(readlink -f $CURRENT_SYMLINK))
    if [ "$PRIMARY_ID" = 'service_is_stopped' ] || get_instance_health $PRIMARY_ID | grep -q -E 'exists=NO|is_running=NO|mounted=NO'; then
        start_instance $(generate_instance_id)
    else
        echo "Service is already running and it appears to be operational, instance='${INSTANCE_ROOT}/$PRIMARY_ID'. Run the 'restart' command if you wish to force a new instance to be created."
    fi
}

#******************************************************************************
# service_stop - Stops the service
#******************************************************************************

service_stop() {
    log_message "STOP_SERVICE"
    echo "Preventing new activity..."
    update_symlink $MOUNT_SYMLINK 'service_is_stopped'
    echo "Waiting a bit so lfs is more likely to be idle" # TODO make this more sophisticated
    sleep 5
    stop_all
    echo "Retry again later if any instances were unable to be stopped"
}

#******************************************************************************
# service_restart - Restarts the service by creating a new instance and swinging the pointers
#******************************************************************************

service_restart() {
    log_message "RESTART"
    start_instance $(generate_instance_id)
}

#******************************************************************************
# stop_inactive - Stops inactive instances
#******************************************************************************

stop_inactive() {
    log_message "STOP_INACTIVE"
    echo "Stopping inactive instances ..."
    service_status | grep -E 'is_running=yes|mounted=yes' | grep 'files_in_use:0' | grep 'primary=NO' | while read id everything_else; do
        echo "Stopping inactive instance '$id'"
        stop_instance $id
    done
}

#******************************************************************************
# remove_old - Removes old instances
#******************************************************************************

remove_old() {
    DEFAULT_THRESH="now - 7 days"
    DATE_THRESH="$1"
    DATE_THRESH=${DATE_THRESH:-$DEFAULT_THRESH}
    ABS_DATE_EPOCH=$(date -d "$DATE_THRESH" +%s)
    ABS_DATE_HR="$(date -d @$ABS_DATE_EPOCH)"
    echo "Removing files associated with stopped instances older than \"${DATE_THRESH}\" ($ABS_DATE_HR) ..."
    service_status | grep 'is_running=NO' | grep 'mounted=NO' | while read id everything_else; do
        INSTANCE_TIME="$(cat $INSTANCE_ROOT/$id/created)"
        echo "$id: $INSTANCE_TIME ?< $ABS_DATE_EPOCH"
        if [[ "$INSTANCE_TIME" -lt "$ABS_DATE_EPOCH" || -z "$INSTANCE_TIME" || -z "ABS_DATE_EPOCH" ]]; then # TODO, use a last modified date rather then relying on the id to be the creation time
            echo "Removing files for instance '$id'"
            remove_instance $id
        fi
    done
}


#******************************************************************************
# service_cleanup - Cleans up old instances
#******************************************************************************

service_cleanup() {
    if [ "${1}" == "-f" ]; then
        FORCE_UMOUNT="1"
        shift
    fi
    stop_inactive
    remove_old "$1"
}

#******************************************************************************
# stop_all - Stops all running instances
#******************************************************************************

stop_all() {
    log_message "STOPPING_ALL"
    echo "Attempting to stop all instances ..."
    service_status | grep 'is_running=yes' | while read id everything_else; do
        echo "Stopping instance '$id'"
        stop_instance $id
    done
}

#******************************************************************************
# remove_root  - Remove the previously installed mount
#******************************************************************************

remove_root() {
    echo "Removing framework for ${LFS_ROOTS}"

    stop_all

    #Make sure they are all dead
    for pid in $(ps agux |grep lio_fuse | grep -F ${LFS_ROOTS}  | awk '{print $2}'); do
        kill -9 ${pid}
    done

    #Now delete the shell
    rm --one-file-system -rf ${LFS_ROOTS}
}

#******************************************************************************
# health_check_instance - Checks the health of the instance by processing the
#     output of service_status which is passed in as arguments.
#
# Return states:
#     GOOD - Mount is good. Nothing to do
#     PENDING - Mount is sluggish and there is a pending stat call to determine
#               next steps
#     HI_MEM  - The mount has hit the high water mark on memoery
#     HUNG    - Mount is unresponsive and should be killed
#     DEAD    - Mount is dead and can be cleaned up
#     UNUSED  - Mount is up but not used
#******************************************************************************

health_check_instance() {
    # Read the status args
    ID=$1
    set_instance_vars ${ID}
    PRIMARY=$2
    EXISTS=$3
    MOUNTED=$4
    IS_RUNNING=$5
    if [[ "$IS_RUNNING" =~ "is_running=yes" ]]; then
        d=$(echo "$6" | cut -f2 -d=)
        RSS=$( echo "scale=4; ${d} * 1024 * 1024" | bc | cut -f1 -d\. )
        FILES_IN_USE=$7
        STAT=$8
    fi

    MAX_SIZE=$( echo "scale=4; ${CK_MEM_GB} * 1024 * 1024 * 1024" | bc | cut -f1 -d\. )

    # Now check them

    # See if it's running
    if [ "${IS_RUNNING}" == "is_running=NO" ]; then
        echo "DEAD"
        return
    fi

    #We do a few special checks if it's the primary
    if [ "${PRIMARY}" == "primary=yes" ]; then
        # Check the memory
        if (( RSS > MAX_SIZE )) ; then
            echo "HI_MEM"
            return
        fi
    elif [ "${FILES_IN_USE}" == "files_in_use=0" ]; then # Not being used and not the primary
        echo "UNUSED"
        return
    fi

    # Check the stat arg to see if everything is good
    if [[ ! ${STAT} =~ PENDING|TIMEOUT ]]; then
        echo "GOOD"
        return
    fi

    # If we made it here then there is a problem with stat we need to look at
    # Do an initial split on the stat varible into current and pending
    SCURR=$(echo ${STAT} | tr "(" " " | awk '{print $1}' )
    SPENDING=$(echo ${STAT} | tr "(" " " | awk '{print $2}' )

    # 1st check if the current stat completed successfully.
    if [[ ! ${SCURR} =~ TIMEOUT ]]; then # It did so see if we need to cleanup
        if [[ ! ${SPENDING} =~ PENDING ]]; then  # No background task so remove it
            rm ${INSTANCE_LOGS}/pending_stat
        fi
        echo "GOOD"
        return
    fi

    # So we have a timeout on the current stat.  See if we have one as well on the pending
    if [[ ${SPENDING} =~ PENDING ]]; then
        echo "PENDING"
        if [[ ${SCURR} -gt ${CK_PENDING_TIMEOUT} ]]; then   ## The time is to long so we err out anyway
            echo "HUNG"
        fi
        return
    elif [[ ${SPENDING} =~ TIMEOUT ]]; then  # The pending also failed so kill it
        echo "HUNG"
        return
    fi

    # If we made it here then the current stat hung but the pending succeeded or doesn't exist so kick on off
    ${LFS_PENDING_STAT_CHECK_SCRIPT} ${CK_PENDING_TIMEOUT} ${INSTANCE_LOGS}/pending_stat ${INSTANCE_MNT}/${CK_FILE} ${LFS_ROOTS}/service.log >/dev/null &

    # We're in a holding pattern
    echo "PENDING"
}

#******************************************************************************
# health_checkup - Checks the health of all the mounts and takes corrective
#      action as needed.
#******************************************************************************

health_checkup() {
    log_message "HEALTH-CHECKUP  START"
    service_status | grep primary | while read id primary everything_else; do
        STATE=$(health_check_instance $id $primary $everything_else)

        case "${STATE}" in
            GOOD)
                ;;
            HI_MEM)
                log_message "HEALTH-CHECKUP ${id} HI_MEM on primary"
                service_restart
                ;;
            HUNG)
                log_message "HEALTH-CHECKUP ${id} HUNG"
                stop_instance $id
                remove_instance $id
                ;;
            DEAD)
                log_message "HEALTH-CHECKUP ${id} DEAD"
                stop_instance $id
                remove_instance $id
                ;;
            UNUSED)
                log_message "HEALTH-CHECKUP ${id} UNUSED"
                stop_instance $id
                remove_instance $id
                ;;
            *)
                log_message "ERROR: health_checkup invalid state=${STATE} id=${id}"
                ;;
        esac
    done

    log_message "HEALTH-CHECKUP  END"

}


#******************************************************************************
# log_status - Prints the log usage information
#******************************************************************************

log_status() {
    if [ "${1}" != "-" ]; then #Looks like we have a df unit
        UNITS=$1;
        shift
    else
        UNITS=""
    fi
    INFO=$( ${LIO_LOG_SCRIPT} status ${UNITS} ${LOG_PREFIX}* )
    echo ${INFO}
    log_message "LOG-STATUS ${INFO}"
}

#******************************************************************************
# log_rotate_instance - Does a log rotation on the instance
#******************************************************************************

log_rotate_instance() {
    INSTANCE_ID=$1

    # Get the PID
    FPID=$(ps -eo pid,comm,args | grep ${INSTANCE_ID} | grep fuse | awk '{print $1}')
    if [ "${FPID}" == "" ]; then
        return
    fi

    # Now find the PIDs open log file
    LNAME=$(readlink /proc/${FPID}/fd/* | grep -F ${LOG_PREFIX})
    if [ "${LNAME}" == "" ]; then
        return
    fi

    # Find the next "part" we can use
    part=1
    while [ -e "${LNAME}.p${part}" ]; do
       (( part = part + 1 ))
    done

    #Move the current log file
    mv ${LNAME} ${LNAME}.p${part}

    #Now trigger the rotation which will create a new log file using the old name
    kill -${LOG_SIGNAL} ${FPID}
}

#******************************************************************************
# log_cleanup - Clears out log space if needed
#******************************************************************************

log_cleanup() {
    #Convert the size from GB to Bytes
    MAX_SIZE=$( echo "scale=4; ${LOG_MAX_GB} * 1024 * 1024 * 1024" | bc | cut -f1 -d\. )

    USED_SIZE=$( ${LIO_LOG_SCRIPT} status -b ${LOG_PREFIX}* | tr -s " " | cut -f4 -d\  )
    if [[ ${USED_SIZE} -lt ${MAX_SIZE} ]]; then    # Nothing to do so log the action and kick out
        log_message "LOG-CLEANUP  Used_bytes: ${USED_SIZE} < ${MAX_SIZE}"
        return
    fi

    if [ "${LOG_SIGNAL}" != "" ]; then
        # Cycle through each mount and do a log rotate
        service_status | grep 'is_running=yes' | while read id everything_else; do
            echo "Rotating logs for instance '$id'"
            log_rotate_instance $id
        done
    fi

    # Log the start size
    INFO_START=$( ${LIO_LOG_SCRIPT} status -h ${LOG_PREFIX}* )

    # Now call the log cleanup script
    ${LIO_LOG_SCRIPT} cleanup ${LOG_MIN_GB} ${LOG_PREFIX}*

    # Get the final size
    INFO_END=$( ${LIO_LOG_SCRIPT} status -h ${LOG_PREFIX}* )

    # Log that we are finished
    log_message "LOG-CLEANUP  Start -- ${INFO_START}   End -- ${INFO_END}"
    echo "Start -- ${INFO_START}   End -- ${INFO_END}"
}

#******************************************************************************
# usage - Help message
#******************************************************************************

usage() {
    echo "Usage: $0 install <path> <user> [<vars.sh>] | <lfs_roots_prefix> OPTION"
    echo "install <path> <user> [<vars.sh>] - Install a new namespace"
    echo "     <path>    - Prefix to use for storing hte namespace.  The path is created if it doesn'texist"
    echo "     <user>    - Local user for running the lio_fuse binary under. Defaults to lfs"
    echo "     <vars.sh> - Optional file containing the lfs_service_manager.sh configuration options."
    echo "<lfs_roots_prefix>  - Directory used for managing the LFS namespace."
    echo "Valid OPTIONs"
    echo "    remove  - Removes the mount root framework"
    echo "    start   - Start the LFS service"
    echo "    stop    - Stop the LFS service"
    echo "    status  - List the state of all LFS mounts in the namespace"
    echo "    restart - Restart/cycle the LFS mount"
    echo "    cleanup [-f] [<older-than-date>]   - Cleanup mounts"
    echo "        -f                - Use \"-z\" with fusermount3 if needed to clear the mount"
    echo "        <older-than-date> - Force the shutdown of in-use mounts that are older than the data provided"
    echo "    install <path> <user> [<vars.sh>] - Install a new namespace"
    echo "         <path>    - Prefix to use for storing hte namespace.  The path is created if it doesn'texist"
    echo "         <user>    - Local user for running the lio_fuse binary under. Defaults to lfs"
    echo "         <vars.sh> - Optional file containing the lfs_service_manager.sh configuration options."
    echo "    log-status [<size-units>] - Prints log file disk usage for the namespace"
    echo "         <size-units> - Output units passed to df.  See \"df\" for details"
    echo "    log-cleanup - Clean up log files if needed"
    echo "    health-checkup - Checks on all mounts help and cycles, clean's up as needed."

    exit 1
}

#---------------------------------------------------------------------------------------
#-------------------------MAIN----------------------------------------------------------
#---------------------------------------------------------------------------------------

#Flag any unbound variables
set -u

#If no options given then print help and kick out
if [ "$#" == "0" ]; then
    usage
fi

if [ "$1" == "install" ]; then  #We're doing an install
    install "$0" "$2" "$3" "${4:-}"
    exit
elif [ -f ${1}/vars.sh ]; then  # Make sure the vars file exists for the other commands
    vars_default
    TCM_LIB_OS=${TCM_LIB}    #Preserver the OS library in case the user leaves it as default
    source ${1}/vars.sh

    [ "${TCM_LIB}" == "" ] && TCM_LIB=${TCM_LIB_OS}   #** Use the OS version if blank
    if [ "${TCM_LIB}" == "" ]; then
        TCM_ENABLE="0"
    elif [ ! -f ${TCM_LIB} ]; then
        TCM_ENABLE="0"
        echo "Disabling Use of TCMalloc since ${TCM_LIB} does not exist"
    fi
else    # No vars file so print help
    usage
    exit
fi

#Now we can start making the local variables
LIO_LOG_SCRIPT=$(dirname $(realpath $0) )/lio_log_manager.sh
LFS_PENDING_STAT_CHECK_SCRIPT=$(dirname $(realpath $0) )/lfs_pending_stat_check.sh
INSTANCE_ROOT="${LFS_ROOTS}/instances"
MOUNT_SYMLINK="${LFS_ROOTS}/mnt"
CURRENT_SYMLINK="${LFS_ROOTS}/current"
WDIR="${CURRENT_SYMLINK}/logs"

# To bind-mount LStore inside of a container, any symlinks need to be resolvable
# to locations beneath the mount. Set up a symlink to current mount point at
# ${LFS_ROOTS}/instances/lfs which lets us mount ${LFS_ROOTS}/instances/ to /lio
# within a container. This makes it so /lio/lfs can point to the current LStore
# mount
NESTED_CURRENT_SYMLINK="${INSTANCE_ROOT}/lfs"
FORCE_UMOUNT="0"
TIMEOUT="timeout -s 9"
if [ "${TCM_ENABLE}" == "1" ]; then
    TCMOPT="${TCM_OPTS} --setenv=LD_PRELOAD=${TCM_LIB}"
fi

case "${2:-}" in
    remove)
        remove_root
        ;;
    start)
        service_start
        ;;
    stop)
        service_stop
        ;;
    restart)
        service_restart
        ;;
    status)
        service_status
        ;;
    cleanup)
        service_cleanup "${3:-}" "${4:-}"
        ;;
    log-status)
        log_status "${3:-}"
        ;;
    log-cleanup)
        log_cleanup
        ;;
    health-checkup)
        health_checkup
        ;;
    *)
        usage;
        exit 1;
        ;;
esac
exit 0
