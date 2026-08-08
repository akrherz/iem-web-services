#!/bin/bash
# Setup data things

# Setup a local folder to write into
mkdir -p _local/mesonet/data/iemre
sudo ln -s "$(pwd)/_local/mesonet" /mesonet

# Get current CFS file for drydown service to use
curl --fail --silent --show-error \
  --output "/mesonet/data/iemre/cfs_current.nc.tmp" \
  "https://mesonet.agron.iastate.edu/onsite/iemre/cfs_current.nc"
mv "/mesonet/data/iemre/cfs_current.nc.tmp" "/mesonet/data/iemre/cfs_current.nc"

sudo mkdir /opt/bufkit
sudo git clone --recurse-submodules https://github.com/iowamesonet/bufkit.git /opt/bufkit
