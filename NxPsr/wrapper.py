#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar  5 09:51:28 2021

@author: parm
"""
import datetime
import calendar


class Wrapper:
    def __init__(self, dataplane, alert_limit):
        # initialize the variable
        self.dataplane = dataplane
        self.events = []
        self.last_ID = 0
        self.alert_limit = alert_limit
        self.alert_state = 0

        # initialuize the stats
        self.update()

    def stats_update(self):
        '''Upddate long terms stats on status code and size'''
        QUERY_STD = '''SELECT
       count(1)                                         AS count,
       avg(response_size)                               AS avg_bytes_sent,
       count(CASE WHEN response_code >= 200 AND response_code<300  THEN 1 END) AS '2xx',
       count(CASE WHEN response_code >= 300 AND response_code<400 THEN 1 END) AS '3xx',
       count(CASE WHEN response_code >= 400 AND response_code<=500 THEN 1 END) AS '4xx',
       count(CASE WHEN response_code >= 500 THEN 1 END) AS '5xx'
     FROM requests;
     '''
        result = -1
        try:
            result = self.dataplane.query(QUERY_STD)[0]
        except:
            pass
        self.stats = result

    def last10s_update(self):
        '''get the count and the sections of the last 10 sec requests'''

        epoch = calendar.timegm(self.last_update.timetuple())
        QUERY_STD = '''SELECT 
       section                                  AS section,
	   count(1)                                 AS count,   
       avg(response_size)                       AS avg_bytes_sent
       FROM requests
       WHERE  unix_epoch >= $epoch
       GROUP BY section
       ORDER BY count DESC;
     '''.replace("$epoch", str(epoch-10))
        result = 0
        try:
            result = self.dataplane.query(QUERY_STD)
        except:
            pass
        self.last10 = result

    def last2min_count(self):
        '''get the count of the last 2 min requests'''
        # GMT epoch
        epoch = calendar.timegm(self.last_update.timetuple())
        QUERY_STD = '''SELECT 
	   count(1)                                 AS count,   
       avg(response_size)                       AS avg_bytes_sent
       FROM requests
       WHERE  unix_epoch >= $epoch;
     '''.replace("$epoch", str(epoch-120))
        result = 0
        try:
            result = self.dataplane.query(QUERY_STD)
        except:
            pass
        if result:
            self.last2min = result[0][0]
        else:
            self.last2min = 0

    def alert_finished(self):
        '''save the total request during the aleert period'''
        self.alert_state = 0
        start = self.events[-1][-2]
        epoch_start = calendar.timegm(start.timetuple())
        QUERY_STD = '''SELECT 
	   count(1)                                 AS count,   
       avg(response_size)                       AS avg_bytes_sent
       FROM requests
       WHERE  unix_epoch >= $epoch ;
     '''.replace("$epoch", str(epoch_start-120))
        result = 0
        try:
            result = self.dataplane.query(QUERY_STD)
        except:
            pass
        if result:
            self.events[-1][1] = result[0][0]

    def update(self):
        '''update every 10 second : the data, stats, 10s, 2min'''
        self.last_update = datetime.datetime.now()
        self.dataplane.load()
        self.stats_update()
        self.last10s_update()
        self.last2min_count()

        # alert management : might move to a separate function
        # event = tuple of alerts, alerts tupple of 2 dates, start and end

        # logic: if alert if not engaged and we get the trigger then add event and activate alert
        if (not self.alert_state) and (self.last2min > self.alert_limit):
            self.events.append(
                [self.last2min, self.last2min, datetime.datetime.now()])
            self.alert_state = 1
        # Logic: if alert is engaged and we fall below the number to triggerr the alert
        # add event and deactivate alert
        elif self.alert_state and (self.last2min < self.alert_limit):
            self.events[-1].append(datetime.datetime.now())
            self.alert_finished()
