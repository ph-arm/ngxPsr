#!/bin/bash
full_path=$(realpath $0)
dir_path=$(dirname $full_path)

export PYTHONPATH=$dir_path/NxPsr/
py.test -vv --durations=0 $dir_path
