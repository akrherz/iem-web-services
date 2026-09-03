#!/bin/bash
# Setup data things
set -euo pipefail

# Setup a local folder to write into
mkdir -p _local/mesonet/data/iemre
sudo ln -s "$(pwd)/_local/mesonet" /mesonet

sudo mkdir /opt/bufkit
sudo git clone --recurse-submodules https://github.com/iowamesonet/bufkit.git /opt/bufkit
