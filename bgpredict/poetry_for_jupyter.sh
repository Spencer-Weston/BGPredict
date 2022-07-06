#!/bin/bash

# Short example on how to run jupyter using a poetry python environment. This will create a kernel named "poetry_kernel"
# which can be selected as a kernel inside of jupyter. It will have every package specified in pyproject.toml
poetry run ipython kernel install --user --name=poetry_kernel
poetry run jupyter lab