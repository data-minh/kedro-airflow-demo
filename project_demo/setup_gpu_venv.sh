#!/bin/bash

# Create a new virtual environment
python3 -m venv .gpu_venv

# Activate the virtual environment
source .venv/bin/activate

# Link command python with python2
ln -sfn /usr/bin/python2 $(which python)

# Install packages from requirements.txt
pip3 install --no-index --find-links /opt/kedro/packages/ -r requirements.txt

# deactivate .venv
deactivate