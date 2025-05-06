#!/usr/bin/bash
#**************************************************************************************************
#
# lfs_mgmt.sh - LFS management script to handle multiple LFS sites and filesets
#
# /etc/lio/lfs.local  - Local LFS mounts (key:value
#        <name>.local: v=lstore mount=lstore:....|link:.....  [root=<root-prefix>] [pkey=<LServer-public-key]
# /etc/lio/lfs.vars   - Common vars.sh for each mount. It's tweaked as needed for instantiation
# /lfs/*              - Directory containing al lthe <lfs_mounts>
# /lfs_roots/*        - The working dorectiries for each of the mounts
# /lfs_roots/<lfs_mount>/info - Global info about the mount
#                        TYPE:   link | config
#                        CONFIG: link | <config-url>
#                        ROOT:   <prefix into config to treat as the TLD>
#
#
#**************************************************************************************************

#**************************************************************************************************
# read_mount_info - Reads he mounts "info" file and sets the global variables: INFO_TYPE, INFO_CFG, INFO_ROOT
#
#**************************************************************************************************

read_mount_info() {
    local mnt="${1}"
    local finfo="${LFS_ROOTS}/${mnt}/info"

    #Clear the globals
    INFO_TYPE=""
    INFO_CFG=""
    INFO_ROOT="/"

    #Make sure it exists
    if [ ! -e ${finfo} ]; then
        return
    fi

    INFO_TYPE=$(cat ${finfo} | grep '^TYPE: ' | sed 's/^TYPE: *//g' | sed 's/ *$//g')
    INFO_CFG=$(cat ${finfo} | grep '^CONFIG: ' | sed 's/^CONFIG: *//g' | sed 's/ *$//g')
    INFO_ROOT=$(cat ${finfo} | grep '^ROOT: ' | sed 's/^ROOT: *//g' | sed 's/ *$//g')
}

#**************************************************************************************************
# parse_mount_info - Parse the mount record info. The format is below.
#    v=lstore mount=lstore:....|link:.....  [root=<root-prefix>] [pkey=<LServer-public-key]
#
# It sets the global mount variables: MNT_TYPE, MNT_CFG, MNT_ROOT, MNT_PKEY, CLI_CFG, REPAIR_CFG
#
#**************************************************************************************************

parse_mount_info() {
    #This strips any stray '"'
    record=$(echo ${1} | tr '"' ' ')

    #Verify the record is the correct type
    if [[ ! "${record}" == *"v=lstore "* ]]; then
        echo "ERROR:  Incorrect record type! Record: ${record}"
        exit 1
    fi

    #Clear the current mount values
    MNT_TYPE=""
    MNT_CFG=""
    MNT_ROOT=""
    MNT_PKEY=""
    CLI_CFG=""
    REPAIR_CFG=""

    #Now start the parsing
    str=$(echo "${record}" | grep -Po 'mount=\K[^ ]*')
    if [[ "${str}" == "link:"* ]]; then # Got a link to another mount
        MNT_TYPE="link"
        MNT_CFG=$(echo "${str}" | grep -Po 'link:\K[^ ]*')
    elif [[ "${str}" == "lstore:"* ]]; then # Got an LServer
        MNT_TYPE="lstore"
        MNT_CFG="${str}"
    else #  Malformed flag so flag it
        echo "Unknown mount type. record=${record}"
        exit 1
    fi

    #See if we have a root prefix
    MNT_ROOT=$(echo "${record}" | grep -Po 'root=\K[^ ]*')
    if [ "${MNT_ROOT:0:1}" != "/" ]; then
        echo "Invalid root prefix!  It must start with a '/'!  record=${record}"
        exit 1
    fi

    #Get the public key
    MNT_PKEY=$(echo "${record}" | grep -Po 'pkey=\K[^ ]*')

    #Get the CLI config if provided
    CLI_CFG=$(echo "${record}" | grep -Po 'cli=\K[^ ]*')

    #Lastly get the repair config if provided
    REPAIR_CFG=$(echo "${record}" | grep -Po 'repair=\K[^ ]*')
}

#**************************************************************************************************
# mount_link_string  -Formats the link mount as a string.
#      $1 - Config/host
#      $2 - Roor prefix
#**************************************************************************************************

mount_link_string() {
    local host="${1}"
    local root="${2}"

    if [ "${root}" == "/" ]; then
        echo "${host}"
    else
        echo "${host}${root}"
    fi
}

#**************************************************************************************************
#  lookup_dns_txt_mount_info - Fetches the mount info from the DNS TXT record (v=lstore)
#**************************************************************************************************

lookup_dns_txt_mount_info() {
    txt=$(host -t txt ${1} | grep "v=lstore" | sed 's/^.* descriptive text //g')

    #Clear the current mount values
    MNT_TYPE=""
    MNT_CFG=""
    MNT_ROOT=""
    MNT_PKEY=""

    if [ "${txt}" == "" ]; then
        return
    fi

    parse_mount_info "${record}"
}

#**************************************************************************************************
#  lookup_local_mount_info - Fetches the mount info from the local config file
#**************************************************************************************************

lookup_local_mount_info() {
    record=$(egrep "^${1}:" ${LFS_LOCAL_CONFIG} | cut -f2- -d:)

    #Clear the current mount values
    MNT_TYPE=""
    MNT_CFG=""
    MNT_ROOT=""
    MNT_PKEY=""

    if [ "${record}" == "" ]; then
        return
    fi

    parse_mount_info "${record}"
}

#**************************************************************************************************
# lookup_mount_info  - Fetches the mount info from either the local config or the DNS TXT record
#**************************************************************************************************

lookup_mount_info() {
    #Clear the mnt type
    MNT_TYPE=""
    lookup_local_mount_info "${1}"
    if [[ "${MNT_TYPE}" == "" ]]; then  #If not found then do a DNS query
        lookup_dns_txt_mount_info "${1}"
    fi

    if [[ "${MNT_TYPE}" == "" ]]; then  #If still now found kick out
        echo "ERROR: Could not find information about the mount: ${1}"
        exit 1
    fi
}

#**************************************************************************************************
# update_ini_section - Updates or creates the INI section's key/value
#**************************************************************************************************

update_ini_section() {
    local fname="${1}"
    local section="${2}"
    local key="${3}"
    local value="${4}"
    local rng
    local lo
    local hi
    local line
    local nlines
    local str

    #See if the file exists.  If not create it
    [ -e ${fname} ] || touch ${fname}

    #Generate the section line ranges
    rng=$(grep -n -E ' *\[.*\]' ${fname} | sed 's/: *\[/:\[/g' | grep -A1 -F "[${section}]")

    #If missing the section append it and return
    if [ "${rng}" == "" ]; then
        echo "" >> ${fname}
        echo "[${section}]" >> ${fname}
        echo "${key} = ${value}" >> ${fname}
        return
    fi

    #If we made it here the section exists
    #See if we have avalid rng
    lo=$(echo "${rng}" | head -n1 | cut -f1 -d:)
    hi=$(echo "${rng}" | tail -n1 | cut -f1 -d:)
    nlines=$(wc -l < ${fname})

    if [ "${lo}" == "${hi}" ]; then  #If they are the same then we are at the end so use the line count
        hi=${nlines}
    fi

    #Get the line number of the key if it exists
    str=$( echo "${key}" | sed -e 's/[]\/$*.^[]/\\&/g')
    line=$(nl -ba -n'ln' -w1 -s: ${fname} | sed -n "${lo},${hi}p" | sed 's/#.*//g' | grep -E ": *${str}[ =]" | cut -f1 -d: | head -n1)

    if [ "${line}" == "" ]; then # Doesnt exist so insert it after the section
        (( lo = lo + 1 ))
        sed --in-place "${lo}i ${key} = ${value}" ${fname}
    else # Exists so replace it
        cat ${fname} | sed "${line}d" > ${fname}.bak
        if [ "${line}" == "${nlines}" ]; then
            mv ${fname}.bak ${fname}
            echo "${key} = ${value}" >> ${fname}
        else
            cat ${fname}.bak | sed "${line}i ${key} = ${value}" > ${fname}
        fi
    fi
}

#**************************************************************************************************
#  make_host_mapping_key - Makes the host mapping key given the LStore URI
#**************************************************************************************************

make_host_mapping_key() {
    local cfg="${1}"

    LPPC=$(which lio_parse_path_check)
    host=$(${LPPC} "${cfg}" | grep host= | cut -f2 -d=)
    port=$(${LPPC} "${cfg}" | grep port= | cut -f2 -d=)

    echo "tcp://${host}:${port}"
}

#**************************************************************************************************
# update_known_hosts - Adds/updates the information related to the mount to the given known_hosts file.
#     The file is created if needed.
#
#     $1 - Mount name, ie /lfs_roots/<mnt>
#     $2 - Known_hosts file to update
#**************************************************************************************************

update_known_hosts() {
    local mnt="${1}"
    local fpkey="${2}"
    local tcp
    local line

    #Add the mapping entry
    tcp=$(make_host_mapping_key "${MNT_CFG}")
    update_ini_section "${fpkey}" "mappings" "${tcp}" "${mnt}"

    #Now add the pkey
    update_ini_section "${fpkey}" "${mnt}" "public_key" "${MNT_PKEY}"

    #Add shortcuts
    [ "${MNT_CFG}" != "" ] && update_ini_section "${fpkey}" "shortcuts" "lfs.${mnt}" "${MNT_CFG}"

    if [ "${CLI_CFG}" != "" ]; then
        tcp=$(make_host_mapping_key "${CLI_CFG}")
        update_ini_section "${fpkey}" "mappings" "${tcp}" "${mnt}"
        update_ini_section "${fpkey}" "shortcuts" "${mnt}" "${CLI_CFG}"
    fi

    if [ "${REPAIR_CFG}" != "" ]; then
        tcp=$(make_host_mapping_key "${REPAIR_CFG}")
        update_ini_section "${fpkey}" "mappings" "${tcp}" "${mnt}"
        update_ini_section "${fpkey}" "shortcuts" "repair.${mnt}" "${REPAIR_CFG}"
    fi

}

#**************************************************************************************************
# create_lstore_mount - Creates an LStore mount
#  NOTE: Uses the MNT_* global vars
#**************************************************************************************************

create_lstore_mount() {
    local creds="${1}"
    local user="${2}"
    local luser="${3}"
    local mnt="${4}"

    #Make the temp vars.sh
    vars=$(mktemp)
    cat ${LFS_GLOBAL_VARS} | \
        sed "s/^LFS_NAME=.*/LFS_NAME=\"${mnt}\"/g" | \
        sed "s%^LFS_CFG=.*%LFS_CFG=\"-c ${MNT_CFG}\"%g" | \
        sed "s%^CLI_CFG=.*%CLI_CFG=\"-c ${CLI_CFG}\"%g" | \
        sed "s%^LIO_PKEY_DIR=.*%LIO_PKEY_DIR=\"--setenv=LIO_KEY_PREFIX=${LFS_ROOTS}/${mnt}\"%g" | \
        sed "s%^LIO_CREDS=.*%LIO_CREDS=\"--fcreds ${creds} -u ${user}\"%g"  > ${vars}

    # Make the /lfs_roots directory if needed
    if [ ! -f "${LFS_ROOTS}" ]; then
        mkdir "${LFS_ROOTS}"
        chown "${luser}:" "${LFS_ROOTS}"
        chmod 0755 "${LFS_ROOTS}"
    fi

    # Make the /lfs directory if needed
    if [ ! -f "${LFS_MNT}" ]; then
        mkdir "${LFS_MNT}"
        chown "${luser}:" "${LFS_MNT}"
        chmod 0755 "${LFS_MNT}"
    fi

    #Make the mount
    ${LFS_SERVICE_MANAGER} install "${LFS_ROOTS}/${mnt}" "${luser}" "${vars}"
    rm ${vars}

    #Add the public key if needed
    if [ "${MNT_PKEY}" != "" ]; then
        if [ ! -f "${LFS_ROOTS}/${mnt}/known_hosts" ]; then
            touch "${LFS_ROOTS}/${mnt}/known_hosts"
            chown "${luser}:" "${LFS_ROOTS}/${mnt}/known_hosts"
            chmod 0755 "${LFS_ROOTS}/${mnt}/known_hosts"
        fi
        update_known_hosts "${mnt}" "${LFS_ROOTS}/${mnt}/known_hosts"

        if [ ! -f "/etc/lio/known_hosts" ]; then
            touch "/etc/lio/known_hosts"
            chown "${luser}:" "/etc/lio/known_hosts"
            chmod 0755 "/etc/lio/known_hosts"
        fi
        update_known_hosts "${mnt}" "/etc/lio/known_hosts"
    fi

    #See if we have a relative mount
    if [ "${MNT_ROOT}" == "/" ]; then
        ln -s ${LFS_ROOTS}/${mnt}/mnt ${LFS_ROOTS}/${mnt}/lmnt
    else
        ln -s ${LFS_ROOTS}/${mnt}/mnt${MNT_ROOT} ${LFS_ROOTS}/${mnt}/lmnt
    fi

    #Create the info file
    echo "TYPE: lstore"  > ${LFS_ROOTS}/${mnt}/info
    echo "CONFIG: ${MNT_CFG}" >> ${LFS_ROOTS}/${mnt}/info
    echo "ROOT: ${MNT_ROOT}" >> ${LFS_ROOTS}/${mnt}/info

    #Finally add the global entry
    ln -s ${LFS_ROOTS}/${mnt}/lmnt ${LFS_MNT}/${mnt}
}

#**************************************************************************************************
# create_link_mount - Creates an LStore mount
#  NOTE: Uses the MNT_* global vars
#**************************************************************************************************

create_link_mount() {
    local creds="${1}"
    local user="${2}"
    local luser="${3}"
    local mnt="${4}"

    #Make the temp vars.sh
    vars=$(mktemp)

    #Make a dummy mount.  We won't actually fire up FUSE on it
    ${LFS_SERVICE_MANAGER} install "${LFS_ROOTS}/${mnt}" "${luser}" "${vars}"
    rm ${vars}


    #See if we have a relative mount
    if [ "${MNT_ROOT}" == "/" ]; then
        ln -s ${LFS_ROOTS}/${MNT_CFG}/lmnt ${LFS_ROOTS}/${mnt}/lmnt
    else
        ln -s ${LFS_ROOTS}/${MNT_CFG}/lmnt${MNT_ROOT} ${LFS_ROOTS}/${mnt}/lmnt
    fi

    #Create the info file
    echo "TYPE: link"  > ${LFS_ROOTS}/${mnt}/info
    echo "CONFIG: ${MNT_CFG}" >> ${LFS_ROOTS}/${mnt}/info
    echo "ROOT: ${MNT_ROOT}" >> ${LFS_ROOTS}/${mnt}/info

    #Finally add the global entry
    ln -s ${LFS_ROOTS}/${mnt}/lmnt ${LFS_MNT}/${mnt}
}


#**************************************************************************************************
# add_mount - Adds amount
#    <creds-file> <creds-user> <local-user> <lfs_mount>
#**************************************************************************************************

add_mount() {
    local fcreds="${1}"
    local user="${2}"
    local luser="${3}"
    local lmnt="${4}"

    local my_type=""
    local my_cfg=""
    local my_root=""
    local my_pkey=""

    # Check and make sure the mount doesn't alread yexist
    if [ -e ${LFS_ROOTS}/${lmnt} ]; then
        echo "add_mount: Mount already exists: ${lmnt}"
        echo "add_mount: Use the 'update' option to update the mount info"
        exit 1
    fi

    echo "add_mount: fcreds=${fcreds} user=${user} lmnt=${lmnt}"

    #Get the mount info
    lookup_mount_info "${lmnt}"
    if [ "${MNT_TYPE}" == "" ]; then
        echo "ERROR:  No entry for ${lmnt}"
        exit 1
    fi

    # Make the entry in the LFS_ROOTS directory
    #    NOTE: the MNT_* are global variables so the create routines pick them up automatically
    if [ "${MNT_TYPE}" == "lstore" ]; then
        create_lstore_mount "${fcreds}" "${user}" "${luser}" "${lmnt}"
    else # It's a link so we may need to recurse
        #Preserve our mounting vars in case we recurse
        my_type="${MNT_TYPE}"
        my_cfg="${MNT_CFG}"
        my_root="${MNT_ROOT}"
        my_pkey="${MNT_PKEY}"

        # Recurse if needed to create the links
        if [ ! -e ${LFS_ROOTS}/${my_cfg} ]; then
            echo "Recursing to create ${my_cfg}"
            add_mount "${fcreds}" "${user}" "${luser}" "${my_cfg}"
        fi

        # Now we can make our LFS_ROOTS
        MNT_TYPE="${my_type}"
        MNT_CFG="${my_cfg}"
        MNT_ROOT="${my_root}"
        MNT_PKEY="${my_pkey}"
        create_link_mount "${fcreds}" "${user}" "${luser}" "${lmnt}"
    fi
}


#**************************************************************************************************
# start_mount - Starts a mount up if not running
#**************************************************************************************************

start_mount() {
    local mnt="${1}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${mnt} start
    else
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"

        if [ "$(readlink ${LFS_ROOTS}/${INFO_CFG}/mnt)" == "service_is_stopped" ]; then
            start_mount "${INFO_CFG}"
        fi
    fi

}

#**************************************************************************************************
# mount_start - Wrapper to start either  a single or all mounts not running
#**************************************************************************************************

mount_start() {
    if [ "${1}" != "all" ]; then
        start_mount "${1}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        start_mount ${mnt}
    done
}


#**************************************************************************************************
# remove_mount - Removes the mount from the registered list
#**************************************************************************************************

remove_mount() {
    lmnt="${1}"

    read_mount_info "${lmnt}"

    #Verify we have an entry
    if [ ! -e ${LFS_ROOTS}/${lmnt} ]; then
        echo "ERROR: It doesn't look like '${lmnt}' is installed."
        exit 1
    fi

    #Stop the mount if running
    ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${lmnt} stop

    #Uninstall it
    ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${lmnt} remove

    #Remove the global symlink
    rm ${LFS_MNT}/${lmnt}
}

#**************************************************************************************************
# list_mounts - Lists all mounts registered
#**************************************************************************************************

list_mounts() {
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})
        read_mount_info "${mnt}"

        if [ "$(readlink ${i}/lmnt)" == "service_is_stopped" ]; then
            state="stopped"
        elif [ ! -e ${i}/lmnt ]; then
            state="stopped"
        else
            state="running"
        fi
        echo "${mnt}: state=${state} type=${INFO_TYPE} cfg=${INFO_CFG} root=${INFO_ROOT}"
    done
}

#**************************************************************************************************
# mount_remove - Wrapper to remove either a single or all mounts
#**************************************************************************************************

mount_remove() {
    if [ "${1}" != "all" ]; then
        remove_mount "${1}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        remove_mount "${mnt}"
    done
}


#**************************************************************************************************
# status_mount - Prints the status of the mount
#**************************************************************************************************

status_mount() {
    local mnt="${1}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} status
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_status - Wrapper to list the status of either a single or all mounts
#**************************************************************************************************

mount_status() {
    if [ "${1}" != "all" ]; then
        status_mount "${1}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        status_mount "${mnt}"
    done
}

#**************************************************************************************************
# stop_mount - Stops the mount
#**************************************************************************************************

stop_mount() {
    local mnt="${1}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} stop
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_stop - Wrapper to stop either a single or all mounts
#**************************************************************************************************

mount_stop() {
    if [ "${1}" != "all" ]; then
        stop_mount "${1}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        stop_mount "${mnt}"
    done
}

#**************************************************************************************************
# restart_mount - Restarts the mount if needed
#**************************************************************************************************

restart_mount() {
    local mnt="${1}"
    local recurse="${2}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} restart
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
        if [ "${recurse}" == "1" ]; then
            restart_mount ${INFO_CFG} 1
        fi
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_stop - Wrapper to stop either a single or all mounts
#**************************************************************************************************

mount_restart() {
    if [ "${1}" != "all" ]; then
        restart_mount "${1}" 1
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        restart_mount "${mnt}" 0
    done
}

#**************************************************************************************************
# cleanup_mount - Cleans up a mount if needed
#**************************************************************************************************

cleanup_mount() {
    local mnt="${1}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} cleanup "${2:-}" "${3:-}"
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_cleanup - Wrapper to cleanup either a single or all mounts
#**************************************************************************************************

mount_cleanup() {
    if [ "${1}" != "all" ]; then
        cleanup_mount "${1}" "${2:-}" "${3:-}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        cleanup_mount "${mnt}" "${2:-}" "${3:-}"
    done
}

#**************************************************************************************************
# health_checkup_mount -Performs a health check on the mount if needed
#**************************************************************************************************

health_checkup_mount() {
    local mnt="${1}"
    local recurse="${2}"


    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} health-checkup
    elif [ "${INFO_TYPE}" == "link" ]; then
        if [ "${recurse}" == "1" ]; then
            health_checkup_mount ${INFO_CFG} 1
        fi
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_health_checkup - Wrapper to perform a health check on a single or all mounts
#**************************************************************************************************

mount_health_checkup() {
    if [ "${1}" != "all" ]; then
        health_checkup_mount "${1}" 1
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        health_checkup_mount "${mnt}" 0
    done
}

#**************************************************************************************************
# log_status_mount -Performs a log status check on the mount if needed
#**************************************************************************************************

log_status_mount() {
    local mnt="${1}"
    local recurse="${2}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} log-status "${3:-}"
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
        if [ "${recurse}" == "1" ]; then
            log_status_mount ${INFO_CFG} 1 "${3:-}"
        fi
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_log_status - Wrapper to perform a log status check on a single or all mounts
#**************************************************************************************************

mount_log_status() {
    if [ "${1}" != "all" ]; then
        log_status_mount "${1}" 1 "${2:-}"
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        log_status_mount "${mnt}" 0 "${2:-}"
    done
}

#**************************************************************************************************
# log_cleanup_mount -Performs a log cleanup on the mount if needed
#**************************************************************************************************

log_cleanup_mount() {
    local mnt="${1}"
    local recurse="${2}"

    read_mount_info "${mnt}"

    if [ "${INFO_TYPE}" == "lstore" ]; then
        ${LFS_SERVICE_MANAGER} ${LFS_ROOTS}/${1} log-cleanup
    elif [ "${INFO_TYPE}" == "link" ]; then
        lstring=$(mount_link_string "${INFO_CFG}" "${INFO_ROOT}")
        echo "link -> ${lstring}"
        if [ "${recurse}" == "1" ]; then
            log_cleanup_mount ${INFO_CFG} 1
        fi
    else
        echo "ERROR: Unknown mount type!"
    fi
}

#**************************************************************************************************
# mount_log_cleanup - Wrapper to perform a log cleanup on a single or all mounts
#**************************************************************************************************

mount_log_cleanup() {
    if [ "${1}" != "all" ]; then
        log_cleanup_mount "${1}" 1
        return
    fi

    #If we made it here we're iterating
    for i in ${LFS_ROOTS}/*; do
        mnt=$(basename ${i})

        echo " "
        echo "${mnt} -----------------------------------------"
        log_cleanup_mount "${mnt}" 0
    done
}

#**************************************************************************************************
# usage - Help text
#**************************************************************************************************

usage() {
    echo "Usage: $0 OPTION <lfs_mount>"
    echo "Valid OPTIONs:"
    echo "    add <creds-file> <creds-user> <local-user> - Add an <lfs_mount> to the supported/configured for use to the list"
    echo "                     Use the <creds-user> from the provided <creds-file>."
    echo "                     <local-user> is the local user for owning the mount"
    echo "    remove         - Remove an <lfs_mount> from the configured list"
    echo "    update         - Update <lfs_mount> config"
    echo "    list           - List configured LFS mounts"
    echo "    start          - Start an <lfs_mount> or 'all' to start all configured mounts"
    echo "    stop           - Stop an <lfs_mount> or 'all' to stop all running mounts"
    echo "    restart        - Restart an <lfs_mount> or 'all' to restart all running mounts"
    echo "    status         - Provide the status of an <lfs_mount> or 'all' to list everything"
    echo "    health-checkup - Do a health check on a mount or all"
    echo "    log-cleanup    - Cleanup mount log usage"
    echo "    log-status [<size-units>] - Report the mounts log usage"
    echo "    cleanup [-f] [<older-than-date>] - Cleanup unused mounts"
    echo "   "
    echo "<lfs_mount>  - This the mount operating on.  It should be in the form of a hostname or 'all'"
    echo "               'all' means to perform the operation on all registered mounts. By default, the"
    echo "               mount configuration information is retreived from the host DNS TXT record. The"
    echo "               special domain '.local' is reserved for pulling the information from the"
    echo "               global config file."
    echo "    "
    echo "Directory layout:"
    echo "    /lfs/<lfs_mount>  - This is the hostname and is where the active mount is located"
    echo "    /lfs_roots/<lfs_mount> - This houses all the supporting objects to track the mount."
    echo "    "

    exit 1
}


#--------------------------------------------------------------------------------------------------
#-----------------------------------------MAIN-----------------------------------------------------
#--------------------------------------------------------------------------------------------------

#Flag any unbound variables
set -u

#Setup the constants
LFS_MNT=${LFS_MNT:="/lfs"}
LFS_ROOTS=${LFS_ROOTS:="/lfs_roots"}
LFS_LOCAL_CONFIG=${LFS_LOCAL_CONFIG:="/etc/lio/lfs.local"}
LFS_GLOBAL_VARS=${LFS_GLOBAL_VARS:="/etc/lio/lfs.vars"}
LFS_SERVICE_MANAGER=${LFS_SERVICE_MANAGER:=$(which lfs_service_manager.sh)}

#If no options given then print help and kick out
if [ $# -lt 1 ]; then
    usage
elif [ "${1}" == "list" ]; then  #Handle the list-ing here since it's a single arg
    list_mounts
    exit 0
fi

#Everything else requires more args so check it
if [ $# -lt 2 ]; then
    usage
fi

case "${1}" in
    add)
        if [ $# -lt 5 ]; then
            echo "******Missing required parameters for add a mount****"
            usage
        fi
        add_mount "${2}" "${3}" "${4}" "${5}"
        ;;
    remove)
        mount_remove "${2}"
        ;;
    status)
        mount_status "${2}"
        ;;
    start)
        mount_start "${2}"
        ;;
    restart)
        mount_restart "${2}"
        ;;
    stop)
        mount_stop "${2}"
        ;;
    list)
        list_mounts
        ;;
    cleanup)
        mount_cleanup "${2}" "${3:-}" "${4:-}"
        ;;
    health-checkup)
        mount_health_checkup "${2}"
        ;;
    log-status)
        mount_log_status "${2}" "${3:-}"
        ;;
    log-cleanup)
        mount_log_cleanup "${2}"
        ;;
    *)
        usage
        exit 1
        ;;
esac

exit 0
