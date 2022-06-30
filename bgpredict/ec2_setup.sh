#!/bin/bash

# A few commands to setup an ec2 instance to run the project. Make sure to run from the BGPredict directory

# install poetry
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

# Add poetry to path
source $HOME/.poetry/env

# Ensure repository is up to date with remote
git pull

# Install/update bgpredict environment
poetry update