#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  5 21:29:38 2021

@author: parm
"""
import dataplane  # Everythinf relative to data and ingestion
import wrapper
import pytest
import random
import datetime
import time

SCOPE = "function"
# SCOPE="session"
sample = """10.10.14.5 - - [29/Jun/2020:16:57:59 +0200] "GET / HTTP/1.1" 200 612
10.10.14.5 - - [29/Jun/2020:16:58:30 +0200] "GET /info.php HTTP/1.1" 404 153
10.10.14.5 - - [29/Jun/2020:16:59:21 +0200] "GET /info.php?aaa HTTP/1.1" 200 20
10.10.14.5 - - [29/Jun/2020:17:11:35 +0200] "GET /test%41/a HTTP/1.1" 304 0
10.10.14.5 - - [29/Jun/2020:17:11:39 +0200] "GET /test%65/ HTTP/1.1" 304 0
10.10.14.5 - - [29/Jun/2020:17:14:47 +0200] "GET /dd HTTP/1.1" 304 0
10.10.14.5 - - [29/Jun/2020:17:14:51 +0200] "GET / HTTP/1.
"""


@pytest.fixture(scope=SCOPE)
def shared_wrapper(tmpdir_factory):
    """simple wrapper, no file loaded"""
    fn = tmpdir_factory.mktemp("data_test").join("log")
    file = open(str(fn), 'w')
    file.write(sample)
    file.close()
    nxing_config = '$remote_addr - $user [$timestamp] "$request" $response_code $response_size'
    dataPlane = dataplane.DataPlane(nxing_config, str(fn), 100, str(fn)+'.db')

    dataWrapper = wrapper.Wrapper(dataPlane, 500)
    instance = dataWrapper

    yield instance


@pytest.fixture(scope=SCOPE)
def shared_wrapper_with_loaded_log(tmpdir_factory):
    """wrapper with file filly loaded with logs"""
    fn = tmpdir_factory.mktemp("data_test").join("log")
    file = open(str(fn), 'w')
    file.write(sample)
    file.close()
    nxing_config = '$remote_addr - $user [$timestamp] "$request" $response_code $response_size'

    # Possible alternative but would slow benchmark : dataPlane=dataplane.DataPlane(nxing_config, str(fn), 5000 ,str(fn)+'.db')
    dataPlane = dataplane.DataPlane(nxing_config, str(fn), 5000, ':memory:')

    dataWrapper = wrapper.Wrapper(dataPlane, 500)
    logfile = dataWrapper.dataplane.logfile

    # feeding a log
    buffer = ''
    section = ["work", "Test", "log%3f", "ca_st"]
    single_line = '''10.10.14.5 - - [$time] "GET /$section/info.php HTTP/1.1" 404 153
    '''
    log_time = datetime.datetime.now() - datetime.timedelta(0, 110)
    formatted_time = log_time.strftime("%d/%b/%Y:%H:%M:%S %z")
    for line in range(50000):
        buffer += single_line.replace("$time", formatted_time).replace(
            "$section", section[random.randint(0, 3)])
    f_file = open(logfile, 'w+')
    f_file.write(buffer)
    f_file.close()
    instance = dataWrapper

    yield instance


def test_single_line_parsing(shared_wrapper):
    """parse a line of log, without processing it"""
    dataWrapper = shared_wrapper
    single_line = '''10.10.14.5 - - [29/Jun/2020:16:58:30 +0200] "GET /test/info.php HTTP/1.1" 404 153
'''
    result = dataWrapper.dataplane.parse_log(single_line)
    print(result)
    assert result == {"remote_addr": "10.10.14.5",
                      "user": "-",
                      "timestamp": "29/Jun/2020:16:58:30 +0200",
                      "request": "GET /test/info.php HTTP/1.1",
                      "response_code": "404",
                      "response_size": "153"
                      }


def test_parse_request(shared_wrapper):
    """parse the request during processing of the  log"""
    dataWrapper = shared_wrapper
    parsed_log = {"remote_addr": "10.10.14.5",
                  "user": "-",
                  "timestamp": "29/Jun/2020:16:58:30 +0200",
                  "request": "GET /test/info.php HTTP/1.1",
                  "response_code": "404",
                  "response_size": "153"
                  }

    result = dataWrapper.dataplane.parse_request(parsed_log["request"])
    assert result == ("GET", "/test/info.php", "", "HTTP/1.1", "/test/")


def test_parse_insert_into_db(shared_wrapper):
    """verify process of insertion in db, query to verify all logs were ingested"""
    dataWrapper = shared_wrapper
    dataWrapper.dataplane.load()
    result = dataWrapper.dataplane.query(
        'select count(1) from requests;')[0][0]
    assert result == 6  # last line shouldn't be processed


def test_section_into_db(shared_wrapper):
    """ check if the sections expected are found into the db"""
    dataWrapper = shared_wrapper
    dataWrapper.dataplane.load()
    result = dataWrapper.dataplane.query(
        'select count(1), section from requests group by section;')

    assert len(result) == 3
    assert result[0][1] in ['/test%65/', '/test%41/', '/']
    assert result[1][1] in ['/test%65/', '/test%41/', '/']
    assert result[2][1] in ['/test%65/', '/test%41/', '/']
    assert result[2][1] != result[1][1]
    assert result[2][1] != result[0][1]
    assert result[1][1] != result[0][1]


def test_alert_raising(shared_wrapper_with_loaded_log):
    """load data, check if it raise the alarm"""
    dataWrapper = shared_wrapper_with_loaded_log

    # check the alert is indeed off at start
    assert (not dataWrapper.alert_state)

    # loading the log to the dataplane
    dataWrapper.dataplane.load()
    dataWrapper.update()

    assert dataWrapper.alert_state  # check the alert was raised


def test_alert_stoping(shared_wrapper):
    """set alarm on, verify if log deactivatrte it"""
    dataWrapper = shared_wrapper
    logfile = dataWrapper.dataplane.logfile
    assert (not dataWrapper.alert_state)  # check the alert is off

    dataWrapper.alert_state = 1
    dataWrapper.events = [[600, datetime.datetime.now()]]
    buffer = ''
    section = ["work", "Test", "log%3f", "ca_st"]
    single_line = '''10.10.14.5 - - [$time] "GET /$section/info.php HTTP/1.1" 404 153
    '''

    log_time = datetime.datetime.now()
    formatted_time = log_time.strftime("%d/%b/%Y:%H:%M:%S %z")
    for line in range(100):
        buffer += single_line.replace("$time", formatted_time).replace(
            "$section", section[random.randint(0, 3)])
    f_file = open(logfile, 'w+')
    f_file.write(buffer)
    f_file.close()
    dataWrapper.update()

    assert (not dataWrapper.alert_state)  # check the alert is back off
