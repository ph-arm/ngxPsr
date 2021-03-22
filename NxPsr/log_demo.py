#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  7 19:34:01 2021

@author: parm
"""
import argparse
import datetime
import random
import time


def main():
    # parameter args
    parser = argparse.ArgumentParser(
        description="Log processor for Nginx Access")
    parser.add_argument('logfile', nargs='?', default='/tmp/log')
    parser.add_argument('-n', '--nginx', dest='nginx', action='store_true')
    parser.set_defaults(nginx=False)
    parser.add_argument("-o", "--output",
                        default=30,
                        type=int,
                        help="request by second +/- 10")
    args = parser.parse_args()

    # generate logs
    logfile = args.logfile
    buffer = ''
    section = ["work", "Test", "log%3f", "ca_st"]
    rate = args.output
    if args.nginx == 1:
        single_line = '''10.10.14.5 - - [$time +0200] "GET /$section/info.php HTTP/1.1" 404 153 "-" "Mozilla/5.0 (X11; Linux x86_64; rv:69.0) Gecko/20100101 Firefox/69.0"
'''
    else:
        single_line = '''10.10.14.5 - - [$time +0200] "GET /$section/info.php HTTP/1.1" 404 153
'''
    increasing = 1
    current_rate = rate
    while 1:

        buffer = ''
        log_time = datetime.datetime.now()
        formatted_time = log_time.strftime("%d/%b/%Y:%H:%M:%S")
        if increasing == 1:
            current_rate += rate//60
        else:
            current_rate -= rate//60
        for line in range(max(0, current_rate+random.randint(-10, 10))):
            buffer += single_line.replace("$time", formatted_time).replace(
                "$section", section[random.randint(0, 3)])
        f_file = open(logfile, 'a')
        f_file.write(buffer)
        f_file.close()

        if current_rate > (2*rate):
            increasing = 0
        if current_rate < (rate//2):
            increasing = 1
        time.sleep(1)


if __name__ == "__main__":
    main()
