#!/bin/bash
#******************************************************************************
#
# Parameters:
#   LFS_ROOTS    - Prefix for LFS mounts to run out of
#   LFS_CFG      - LFS config to load
#   LIO_OPTS     - LStore options
#   FUSE_OPTS    - FUSE options
#   FUSE_PATH    - Path containing FUSE tools -- fusermount, fusermount3 (OPTIONAL)
#   LFS_USER     - Which local user to run the LFS process as (defailt: lfs)
#   LIMIT_FD     - FD limit (default: 20480)
#   LIMIT_NPROC  - Nprocs/threads ulimit (default: 100000)
#   LIMIT_MEM    - Memory limit (default: 16G)
#   LIMIT_CORE   - Core dump size (default: 0)
#   TCM_ENABLE   - Enable use of TCMalloc if available
#   TCM_LIB      - TCMalloc shared library.  If not specified it will be searched for
#   TCM_OPTS     - TCMalloc options
#
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
    LFS_ROOTS=/lfs_roots
    LFS_CFG=
    LFS_USER=lfs
    LIO_OPTS="-d 1 --fcreds /etc/lio/lfs.psk"
    FUSE_OPTS="-f -o auto_cache,allow_other,fsname=${LFS_USER}:lio,max_threads=200"
    FUSE_PATH=""
    LIMIT_FD=20480
    LIMIT_NPROC=100000
    LIMIT_MEM=16G
    LIMIT_CORE=0
    TCM_ENABLE=1
    TCM_LIB=$(ldconfig -p |grep libtcmalloc.so | sort | tail -n 1 | awk '{print $4}')
    TCM_OPTS="--setenv=TCMALLOC_RELEARSE_RATE=5"
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

    #Configure the service_manager variables
    vars_default
    [ -e "$4" ] && source $4
    LFS_ROOTS=${install_target}
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
    chown "${user}:" "${install_target}/instances"
    cp $script_path $new_script_path
    if [ ! -f /etc/fuse.conf ]; then
        echo "/etc/fuse.conf doesn't exist, creating it..."
        echo "user_allow_other" >> /etc/fuse.conf
        chown root:lfs /etc/fuse.conf
    fi

    #Now dump them to the new vars file
    echo "LFS_ROOTS=\"${LFS_ROOTS}\"" > ${VARS}
    echo "LFS_CFG=\"${LFS_CFG}\"" >> ${VARS}
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
    mkdir -p "$INSTANCE_LOGS"
    mkdir -p "$INSTANCE_MNT"
    mkdir -p "$INSTANCE_CFG"
    date +%s > "$INSTANCE_PATH/created"
    chown -R "${LFS_USER}:" "$INSTANCE_PATH"

    # allow coredumps, set working dir
    cd $WDIR
    echo "Core dumps enabled (unlimited size), working directory is '$(pwd)' and the current destination/handler is '$(cat /proc/sys/kernel/core_pattern)' (set using /proc/sys/kernel/core_pattern)"
    COMMAND="lio_fuse ${FUSE_OPTS} $INSTANCE_MNT --lio -C ${WDIR} ${LFS_CFG} $LIO_OPTS"
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
        log_message "START_INSTANCE ${INSTANCE_ID}  COMMAND: systemd-run --uid=${LFS_USER} --unit=lfs@${INSTANCE_ID} -p MemoryLimit=${LIMIT_MEM} -p LimitNOFILE=${LIMIT_FD} -p LimitNPROC=${LIMIT_NPROC} ${TCMOPT} $COMMAND"
        systemd-run --uid=${LFS_USER} --unit=lfs@${INSTANCE_ID} -p MemoryLimit=${LIMIT_MEM} -p LimitNOFILE=${LIMIT_FD} -p LimitNPROC=${LIMIT_NPROC} ${TCMOPT} $COMMAND
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
    ${MY_PREFIX:-} rm --one-file-system -rf $INSTANCE_PATH

    #Clear it from the transient SystemD list which shows it as failed
    systemd_clear_instance $INSTANCE_ID

}

#******************************************************************************
# check_instance_health - Checks the health of the instance
#    INSTANCE_ID - Local instance ID
#******************************************************************************

check_instance_health() {
    INSTANCE_ID=$1
    set_instance_vars $INSTANCE_ID
    HEALTHY=true
    STATUS=""
    # primary instance?
    if [ "$INSTANCE_PATH" = "$(readlink -f $CURRENT_SYMLINK)" ]; then
                STATUS+=" primary=yes"
        else
                STATUS+=" primary=NO"
        fi
    # Does it exist?
    if [ -e "$INSTANCE_PATH" ]; then
        STATUS+=" exists=yes"
    else
        STATUS+=" exists=NO"
        echo $STATUS
        return
    fi
    # Is in mounted?
    if mount | grep -q "$INSTANCE_MNT"; then
        STATUS+=" mounted=yes"
    else
        STATUS+=" mounted=NO"
        HEALTHY=false
    fi
    # Is the filesystem process running?
    if pgrep -f "lio_fuse"'.*'"$INSTANCE_MNT" >/dev/null; then
        STATUS+=" is_running=yes"
    else
        STATUS+=" is_running=NO"
        HEALTHY=false
    fi
    if $HEALTHY; then
        # Is it in use?
        FILE_USE_COUNT=$($TIMEOUT 10 bash -c "lsof +f --  $INSTANCE_MNT 2>/dev/null | grep -Ev '^COMMAND ' | wc -l" || echo 'TIMEOUT')
        STATUS+=" files_in_use:$FILE_USE_COUNT"
        # Does a metadata operation work?
        # TODO
    else
        STATUS+=" files_in_use:0"
    fi
    echo $STATUS
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
            #echo "   $ID: $(check_instance_health $ID)"
            for i in $(echo $ID $(check_instance_health $ID)); do
                printf "%-24s" $i
            done | sed 's/ *$//'
            printf "\n"
        done
}

#******************************************************************************
# service_start - Starts the service
#******************************************************************************

service_start() {
    log_message "START_SERVICE"
    PRIMARY_ID=$(basename $(readlink -f $CURRENT_SYMLINK))
    if [ "$PRIMARY_ID" = 'service_is_stopped' ] || check_instance_health $PRIMARY_ID | grep -q -E 'exists=NO|is_running=NO|mounted=NO'; then
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
# usage - Help message
#******************************************************************************

usage() {
    echo "Usage: $0 lfs_roots_prefix {start|stop|status|restart|cleanup [-f] [older-than-date]|install <path> <user> [<vars.sh>]}"
    echo "lfs_roots_prefix  - Directory used for install containing the LFS configuration options and mount info"
    echo "<vars.sh>         - Base LFS variables file to use for install.  If none given then a default is created that can be edited."
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
    *)
        usage;
        exit 1;
        ;;
esac
exit 0
