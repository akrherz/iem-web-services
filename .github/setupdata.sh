#!/bin/bash
# Setup data things

# /mesonet is setup earlier
mkdir -p /mesonet/data/iemre
dt="$(date -u --date '7 days ago' +'%Y%m%d')00"
curl -s "https://mesonet.agron.iastate.edu/onsite/iemre/cfs_${dt}.nc" > "/mesonet/data/iemre/cfs_${dt}.nc"

sudo mkdir /opt/bufkit
sudo git clone https://github.com/iowamesonet/bufkit.git /opt/bufkit
