#!/bin/bash
trap "exit" INT TERM ERR
trap "kill 0" EXIT
full_path=$(realpath $0)
dir_path=$(dirname $full_path)

logtmpdir_=$(mktemp -d 2>/dev/null || mktemp -d -t 'logtmpdir')
log="$logtmpdir_/log"
python3 $dir_path/NxPsr/log_demo.py -o 100  $log&
python3 $dir_path/NxPsr/app.py -a 110  $log

rm -r $logtmpdir
wait
