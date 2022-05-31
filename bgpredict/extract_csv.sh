#!/bin/bash

# This script is essentially documentation of the process used to extract CSV's from the original dataset into a
# "csv only" folder in the bgpredict s3 bucket

aws s3api put-object --bucket 'bgpredict' --key 'openaps_csvs/'

aws s3 cp s3://bgpredict/openaps_data s3://bgpredict/openaps_csvs --recursive --exclude "*" --include "*.csv"