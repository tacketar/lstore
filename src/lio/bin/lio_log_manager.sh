#!/usr/bin/bash
#******************************************************************************
#
# lfs_log_manager.sh - Script for managing LFS namespace logging
#
#******************************************************************************

#******************************************************************************
# log_status - Summarizes the log usage
#******************************************************************************

log_status() {
    if [ "${1:0:1}" == "-" ]; then #Looks like we have a df unit
        UNITS=$1;
        shift
    else
        UNITS=""
    fi

    FILES=$*

    COUNT=$(ls ${FILES} 2>/dev/null| wc -l)
    SIZE=0
    if [ "${COUNT}" != "0" ]; then
        SIZE=$(du -c ${UNITS} ${FILES}| grep total | cut -f1)
    fi
    echo "File_count: ${COUNT}  Total_Size: ${SIZE}"
}

#******************************************************************************
# log_cleanup - Removes older files to get under the needed max size
#******************************************************************************

log_cleanup() {

    #Convert the size from GB to Bytes
    MAX_SIZE=$( echo "scale=4; ${1} * 1024 * 1024 * 1024" | bc | cut -f1 -d\. )
    shift  #Remove the size from the args list

    #Get the file list in oldest->newest order and store it in an array to iterate over
    FILES=()
    flist=$( ls -rt $* | tr "\n" " " )
    read -a FILES <<< "${flist}"

    #See how much is used
    USED_SIZE=$(du -cb ${FILES[@]}| grep total | cut -f1)

    # See if we can kick out
    if [[ ${USED_SIZE} -lt ${MAX_SIZE} ]]; then
        return;
    fi

    # Need to do some cleanup
    for (( i=0; i<${#FILES[@]}; i++ )); do

        echo "Removing ${FILES[i]}"
        rm --one-file-system ${FILES[i]}

        #Update the used size
        if [[ ${i} -eq ${#FILES[@]}-1 ]]; then
            USED_SIZE="0"
        else
            USED_SIZE=$(du -cb ${FILES[@]:i+1:${#FILES[@]}}| grep total | cut -f1)
        fi

        # See if we can kick out
        if [[ ${USED_SIZE} -lt ${MAX_SIZE} ]]; then
            return;
        fi
    done
}


#******************************************************************************
# usage - Print help information
#******************************************************************************

usage() {
     echo "Usage: $0 status [units] [<fname-wildcard> | cleanup <max-used-GB> <fname-wildcard>"
     echo "   status  - Print a summary of namespace log usage"
     echo "   cleanup - Remove older files to get the log usage below <max-used-GB>"
     echo "   units            - Output units passed to df.  See \"df\" for details"
     echo "   <fname-wildcard> - List of files to operate on"
     echo "   <max-used-GB>    - Max space to use in GB"
}

#------------------------------------------------------------------------------
#------------------------------ Main ------------------------------------------
#------------------------------------------------------------------------------

OPTION=${1:-}
shift
case "${OPTION}" in
    status)
        log_status $*
        ;;
    cleanup)
        log_cleanup $*
        ;;
    *)
        usage
        exit 1
        ;;
esac

exit 0

