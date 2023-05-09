#!/usr/bin/env bash

set -e

if which timedatectl &>/dev/null; then
	# modern systemd way, should support RHEL 7-9 (https://access.redhat.com/solutions/1449713)
	# ...and probably new Ubuntu too?
	# UPDATE: actually timedatectl is not included in the typical minimal Docker image
	timedatectl set-timezone America/Chicago
elif [ -e /etc/redhat-release ]; then # any RHEL-like distro seems to have this file (and lsb_release may not be installed in minimal images)
	echo "America/Chicago" > /etc/timezone
	ln -snf /usr/share/zoneinfo/America/Chicago /etc/localtime
	echo 'LANG="en_US.UTF-8"'>/etc/locale.conf
else
	# old Ubuntu way
	echo "America/Chicago" > /etc/timezone
	ln -snf /usr/share/zoneinfo/America/Chicago /etc/localtime
	sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
	echo 'LANG="en_US.UTF-8"'>/etc/default/locale
	dpkg-reconfigure --frontend=noninteractive locales
	update-locale LANG=en_US.UTF-8
fi
