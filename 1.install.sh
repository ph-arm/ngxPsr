#!/bin/bash
full_path=$(realpath $0)
dir_path=$(dirname $full_path)

pip3 install -r $dir_path/requirements.txt
