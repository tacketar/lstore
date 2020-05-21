#!/bin/bash
os_fsck -c /etc/lio/warmer.cfg '/*' |& tee /tmp/os.fsck

