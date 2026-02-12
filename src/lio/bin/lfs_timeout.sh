#!/bin/bash
#*****************************************************************************
#
# lfs_timeout.sh wait_time signal <command>
#      Similar to "timeout" but will still return if the command is stuck
#      in an uninterruptible sleep state.  The process itself will still be
#      hung and is reflected with the return code of 255.
#
#*****************************************************************************

if [ "${3}" == "" ]; then
    echo "$0 [-v] wait_time signal <command>"
    echo "-v        Verbose output tracking the subcommands proceess"
    echo "wait_time How long to wait in seconds before sending the signal"
    echo "signal    Signel to use with 'kill -${singal}'"
    exit 1
fi

#----------------------------------------------------------------

debug_echo() {
    if [ "${debug}" == "0" ]; then
        return;
    fi

    echo "$*" >> /tmp/debug
    echo "$*" >&2
}

debug=0
if [ "$1" == "-v" ]; then
    debug=1
    shift;
fi

DT_MAX=$1
SIG=$2
shift; shift

if [ "${debug}" == "1" ]; then
    echo "" > /tmp/debug
    debug_echo "COMMAND: $*"
fi

#Test using setsid to see if it's any better
nohup $* > /tmp/timeout.$$ &
pid=$!
#pid=$(jobs -pr)

DT=0
while [ "$(ps -h ${pid})" != "" ]; do
    debug_echo "DT=${DT}  pid=${pid}"
    (( DT = DT + 1 ))
    if [[ ${DT} -gt ${DT_MAX} ]]; then  # Exceeded time
        debug_echo "Killing PID"
        kill -${SIG} ${pid}
        sleep 0.1  # Give us a breather to see if it clears
        if [ "$(ps -h ${pid})" != "" ]; then # See if it survived
            sleep 1  # Ok try one more time
            if [ "$(ps -h ${pid})" != "" ]; then # See if it survived
                debug_echo "Hung PID"
                disown ${pid}
                cat /tmp/timeout.$$; rm /tmp/timeout.$$
                exit 255
            fi
        fi

        debug_echo "Killed PID"
        cat /tmp/timeout.$$; rm /tmp/timeout.$$
        exit 254
    fi

    sleep 1
done

wait ${pid}
status=$?
debug_echo "PID exited with ${status}"
cat /tmp/timeout.$$; rm /tmp/timeout.$$
exit ${status}

