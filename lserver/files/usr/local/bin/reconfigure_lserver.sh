#!/usr/bin/env /bin/bash

HOST=$(hostname -f)
BPORT=6711

while (( "$#" )); do
    case "$1" in
        -h)
            echo "${0} [-h] [--host host] [--port base_port]"
            exit
            ;;
        --host)
            HOST=$2
            shift 2
            ;;
        --port)
            BPORT=$2
            shift 2
            ;;
        *)
            echo "Unsupported argument: $1"
            exit
            ;;
    esac
done

echo_info() {
    echo "# This should *ONLY* define the parameters section. If so it's possible to then add"
    echo "# additional parameters in the calling config file immediately after this include"
    echo "# ******* This is autogenerated by the reconfigure_lserver.sh install script! *******"
    echo " "
    echo "[_parameters]"
    echo "hostname=${HOST}"
    echo 'host=tcp://${hostname}'
    echo 'hostport=${host}:'${BPORT}
    echo 'the_host=THE|${hostport}'
}

echo_info > /etc/lio/params.cfg
echo_info > /etc/lio/clients/params.cfg
echo "lstore://THE|${HOST}:${BPORT}:lio" > /etc/lio/default
sed -i "s@QHOSTQ@tcp://${HOST}:${BPORT}@g" /root/.lio/authorized_keys
sed -i "s@QHOSTQ@tcp://${HOST}:${BPORT}@g" /etc/lio/known_hosts


