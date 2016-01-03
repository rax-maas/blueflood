#!/bin/bash
#
# Setup the the box. This runs as root

# Update the box

apt-get -y update >/dev/null
apt-get -y install facter linux-headers-$(uname -r) build-essential zlib1g-dev libssl-dev libreadline-gplv2-dev curl unzip git dkms >/dev/null


# You can install anything you need here.
