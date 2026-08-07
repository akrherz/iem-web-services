#!/bin/bash
# Setup data things

# Setup a local folder to write into
mkdir -p _local/mesonet/data/iemre
sudo ln -s "$(pwd)/_local/mesonet" /mesonet

# Get a recent CFS file for drydown service to use
dt="$(date -u --date '7 days ago' +'%Y%m%d')00"
curl --fail --silent --show-error \
  --output "/mesonet/data/iemre/cfs_${dt}.nc.tmp" \
  "https://mesonet.agron.iastate.edu/onsite/iemre/cfs_${dt}.nc"
mv "/mesonet/data/iemre/cfs_${dt}.nc.tmp" "/mesonet/data/iemre/cfs_${dt}.nc"

sudo mkdir /opt/bufkit
sudo git clone --recurse-submodules https://github.com/iowamesonet/bufkit.git /opt/bufkit
