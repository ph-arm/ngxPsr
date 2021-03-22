#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 17:52:21 2021

@author: parm
"""

import re
import copy
import logging
import sqlite3
import datetime
import calendar
import itertools
import sys
from dateutil import parser

# SQL table creation
CREATE_REQUESTS_TABLE = """
create table if not exists requests (
  id integer primary key,
  source text,
  user text,
  response_size integer,
  response_code integer,
  user_agent text,
  referer text,
  section text,
  raw_request text,
  remote_addr text,
  forwarded_for text,
  timestamp text,
  unix_epoch int,
  method text,
  path text,
  query_string text,
  protocol text,
  upstream_time float,
  nginx_time float
);
"""

CREATE_INDEX_ON_REQUEST_EPOCH = '''CREATE index idx_epoch on requests(unix_epoch);'''


# dev nota bene
#config = '$remote_addr - $user [$timestamp] "$request" $response_code $response_size "$referer" "$user_agent"'
#config_cr = '$remote_addr - $user [$timestamp] "$request" $response_code $response_size'


class DataPlane:

    def __init__(self, config, logfile, batch, db_name):
        """set variable and compile the patern from config"""
        self.config = config  # config of the parser
        self.logfile = logfile  # name of the file

        try:
            self.file = open(logfile)  # content of the file
        except Exception as ERROR:
            # fail correctly if it can t find log file
            print(ERROR, file=sys.stderr)
            sys.exit('Error with the log file')

        self.batch = batch
        self.db_name = db_name
        self.db = 0

        # standard regex compiling
        regex = ''.join(
            '(?P<' + g + '>.*?)' if g else re.escape(c)
            for g, c in re.findall(r'\$(\w+)|(.)', self.config))
        # fix the regex if the last element has nothng afer:
        if regex[-2] == '?':
            regex = regex[:-2]+')'

        self.reg_ = re.compile(regex)

        # not used but could be an improvement
        config_url = '$verb /$shortpath/$fpath $proto'
        regex_url = ''.join(
            '(?P<' + g + '>.*?)' if g else re.escape(c)
            for g, c in re.findall(r'\$(\w+)|(.)', config_url))
        self.reg_url = re.compile(regex_url)

        # regex for the section
        self.reg_section = re.compile(r'/([^/]*)/')

    def migrate_db(self, db):
        """Run database migrations (create tables, etc)"""

        cur = db.cursor()
        cur.execute(CREATE_REQUESTS_TABLE)
        cur.execute(CREATE_INDEX_ON_REQUEST_EPOCH)
        db.commit()

    def setup_db(self, path, migrations=True):
        """Initialize database connection"""

        db = sqlite3.connect(path, check_same_thread=False)
        db.row_factory = sqlite3.Row
        if migrations:
            self.migrate_db(db)
        return db

    def parse_log(self, log_line):
        """Parse a single log line"""

        match = self.reg_.search(log_line.strip())
        return match.groupdict() if match else None

    def parse_date_legacy(self, timestamp):
        """Parse the nginx time format into datetime without the utc information"""

        fmt = '%d/%b/%Y:%H:%M:%S'
        dt = datetime.datetime.strptime(timestamp.split(' ')[0], fmt)
        return dt, calendar.timegm(dt.timetuple())

    def parse_date(self, timestamp):
        """Parse the nginx time format into datetime"""

        dt = parser.parse(timestamp.replace('/', ' ').replace(':', ' ', 1))
        return dt.isoformat(), calendar.timegm(dt.timetuple())

    def parse_request(self, request):
        """Parse the request into method, path, query string, and HTTP protocol"""

        req = request
        method, rest = req.split(" ", 1)
        full_path, protocol = rest.rsplit(" ", 1)
        parts = full_path.split("?", 1)

        section = self.reg_section.search(parts[0])
        if not section:  # if it didn t find anything, default to '/' else take first group
            section = '/'
        else:
            section = section[0]

        path, qs = parts if len(parts) > 1 else (parts[0], "")
        return method, path, qs, protocol, section

    def normalize_log(self, parsed):
        """Clean up a parsed log data : return dictionnary of field"""
        raw_request = -1
        section = ''
        n = {}
        ts, epoch = self.parse_date(parsed['timestamp'])
        try:
            method, path, qs, protocol, section = self.parse_request(
                parsed['request'])
        except:
            method, path, qs, protocol = "ERROR", "ERROR", "ERROR", "ERROR"
            raw_request = parsed['request']

        n['response_size'] = int(parsed['response_size'])
        n['response_code'] = int(parsed['response_code'])
        n['remote_addr'] = parsed['remote_addr']
        n['timestamp'] = ts
        n['unix_epoch'] = epoch
        n['method'] = method
        n['path'] = path
        n['query_string'] = qs
        n['protocol'] = protocol
        n['user'] = parsed['user']
        n['section'] = section
        if 'user_agent' in parsed.keys():
            n['user_agent'] = parsed['user_agent']
            n['referer'] = parsed['referer']

        if raw_request != -1:
            n['raw_request'] = raw_request[0:200]  # limit size insertion

        return n

    def prepare_log(self, log, **kwargs):
        """enrich log a normalized log for database insertion"""

        p = copy.deepcopy(log)
        p['timestamp'] = p['timestamp'].isoformat()
        p.update(kwargs)
        return p

    def insert_log(self, cur, log):
        """Insert a prepared log line into the database"""

        items = log.items()
        keys = [e[0] for e in items]
        values = [e[1] for e in items]
        sql = """insert into requests ({}) values ({})""".format(
            ", ".join(keys),
            ", ".join(["?"] * len(keys)))
        cur.execute(sql, values)

    def load_db(self, logs, **kwargs):
        """Load logs into database"""
        # might need rewrite, what is the performance inpact to not use logs directly
        # it force to travel 2 times all entries. Even if we do one commit

        cur = self.db.cursor()
        for (_raw, _parsed, normalized) in logs:
            prepared = normalized
            # if we need to enrich them, costly because of deepcopy
            #prepared = self.prepare_log(normalized, **kwargs)
            self.insert_log(cur, prepared)
        self.db.commit()

    def batches(self, l, length=500):
        it = iter(l)
        while True:
            b = list(itertools.islice(it, length))
            if b:
                yield b
            else:
                break

    def load(self):

        if not self.db:
            self.db = self.setup_db(self.db_name)

        def process(f):
            for line in f:
                log = line.strip()
                parsed = self.parse_log(log)
                if not parsed:
                    logging.debug("Invalid log: %s", log)
                    continue
                normalized = self.normalize_log(parsed)
                yield log, parsed, normalized

        for batch in self.batches(process(self.file), self.batch):
            self.load_db(batch, source=self.logfile)

    def query(self, query):
        cursor = self.db.cursor()
        cursor.execute(query)
        return cursor.fetchall()
