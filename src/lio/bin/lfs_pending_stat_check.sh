#!/usr/bin/bash
#************************************************************************************
#
# lfs_pending_stat_check.sh - Performs a background stat operation in an LFS instance
#
# timeout  - How long to wait before aborting
# outfile  - Where to store the stat output. File Format: <stat_dt> <start_epoch>
# statfile - File to stat
# servicefile - Also add an entry to the service log for the NS
#
#************************************************************************************

TIMEOUT=$1
OUTFILE=$2
STATFILE=$3

LFS_TIMEOUT_SCRIPT=$(dirname $(realpath $0) )/lfs_timeout.sh

echo "PENDING $(date +'%s')" > $OUTFILE

$(which time) -p -o ${OUTFILE}.time bash -c "${LFS_TIMEOUT_SCRIPT} -v ${TIMEOUT} 9 stat ${STATFILE} 2>&1" >/dev/null
rcode=$?

if [ "${rcode}" == "0" ]; then
    DT=$(grep real < ${OUTFILE}.time | awk '{print $2}')
elif [ "${rcode}" == "1" ]; then
    DT="MISSING-FILE"
else
    DT="TIMEOUT"
fi

/bin/rm ${OUTFILE}.time

echo "${DT} $(date +'%s')" > $OUTFILE
