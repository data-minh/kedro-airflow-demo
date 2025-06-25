#!/bin/bash

# Create a new virtual environment
python3.9 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Link command python with python2
# ln -sfn /usr/bin/python2 $(which python)

# Install packages from requirements.txt
# pip3 install --no-index --find-links /opt/kedro/packages/ -r requirements.txt
pip3.9 install --no-index --trusted-host 10.53.2.214 --find-links http://10.53.2.214/pypi_local/packages -r requirements.txt 

# deactivate .venv
deactivate