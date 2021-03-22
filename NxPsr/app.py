#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 23:52:48 2021

@author: parm
"""
import wrapper  # Managing stats and alerts
import dataplane  # Everythinf relative to data and ingestion
from rich.panel import Panel
from rich.console import render_group
from rich.table import Table
from rich.live import Live
from signal import signal, SIGINT
import sys
import time
import argparse
import threading
debug = 0
# local imports


def handler(signal_received, frame):
    """helper for graceful exit"""
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    sys.exit("Thank you, goodbye")

# rendering in main : could be improved and moved to its own class.

# all the renders for the console:


@render_group()
def render_visual(dataWrapper):
    yield Panel("Last update done at : "+dataWrapper.last_update.strftime("%Y-%m-%d %H:%M:%S"),
                style="on red" if dataWrapper.alert_state else "on blue")
    yield generate_alert(dataWrapper.alert_state, dataWrapper.events,
                         dataWrapper.last2min, dataWrapper.alert_limit)
    yield generate_alert_past_events(dataWrapper.alert_state, dataWrapper.events)
    yield generate_table_stats(dataWrapper.stats)
    yield generate_table_last10(dataWrapper.last10)


def generate_alert(alert, events, hits, limit):
    if alert:
        return Panel(f"High traffic generated an alert - hits = {events[-1][0]}" +
                     " during the last 2 minutes detected at time " +
                     events[-1][2].strftime("%Y-%m-%d %H:%M:%S")+'\n' +
                     f"Currently {hits:d} hits these last 2 minutes",
                     style="on red")
    return Panel(f" Alert is not currently triggered - Alert is set at {limit//120} hits / sec averaged over 2 min",
                 style="blue")


def generate_alert_past_events(alert, events):
    if (alert and len(events) > 1) or ((not alert) and len(events) > 0):
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Alert started at")
        table.add_column("Alert Stopped at")
        table.add_column("Hits at trigger")
        table.add_column("Total hit during alert ON")
        for alerts in events[::-1]:
            if len(alerts) < 4:
                continue
            table.add_row(
                f"{alerts[2].strftime('%Y-%m-%d %H:%M:%S')}",
                f"{alerts[3].strftime('%Y-%m-%d %H:%M:%S')}",
                f"{alerts[0]:d}",
                f"{alerts[1]:d}"
            )
        return table
    return Panel("No previous alert were recorded", style="bold magenta")


def generate_table_stats(stats) -> "Table":
    """Make a table for the global stats."""
    # print(stats)
    values = ["request_count", "average_size", "x200", "x300", "x400"]
    table = Table(show_header=True, header_style="bold magenta")
    table.title = (
        "[b] global statistics on request status : [/b]")
    table.add_column("status code")
    table.add_column("count")
    # table.add_column("Status")

    for index, row in enumerate(values):
        table.add_row(
            f"{row}", f"{stats[index]}"
        )
    return table


def generate_table_last10(last10) -> "Table":
    """Make a table for the last 10sec or just render a message no input"""
    # print(stats)
    if not last10:
        return Panel("No hits on the last 10s", style="on blue")
    values = ["Section", "hits count", "average byte sent"]
    table = Table(show_header=True, header_style="bold magenta")
    table.title = (
        "[b] Statistics on sections for the last 10 secondes : [/b]")
    for element in values:
        table.add_column(element)
    for row in last10:
        table.add_row(
            f"{row[0]}", f"{row[1]:d}", f"{row[2]:.2f}"
        )
    return table

# end of helpers


def main():
    # handler
    signal(SIGINT, handler)

    # Sys arg parsing
    parser = argparse.ArgumentParser(
        description="Log processor for Nginx Access")
    parser.add_argument('logfile', nargs='?', default='/tmp/log')
    parser.add_argument("-b", "--batch",
                        default=5000,
                        type=int,
                        help="Batch size for inserts")
    parser.add_argument("-a", "--alert",
                        default=10,
                        type=int,
                        help="number of connection per seconds over 2 min raising the alert")
    parser.add_argument("-d", "--db",
                        default=":memory:" if debug == 0 else "debug.db",
                        help="Path to sqlite3 database")
    parser.add_argument('-n', '--nginx', dest='nginx', action='store_true')

    parser.set_defaults(nginx=False)

    args = parser.parse_args()

    # log config
    if args.nginx:
        config = '$remote_addr - $user [$timestamp] "$request" $response_code' +\
            ' $response_size "$referer" "$user_agent"'
    else:
        config = '$remote_addr - $user [$timestamp] "$request" $response_code' +\
            ' $response_size'

    # Dataplane initialisation
    dataPlane = dataplane.DataPlane(config, args.logfile, args.batch, args.db)
    dataPlane.load()
    dataWrapper = wrapper.Wrapper(dataPlane, args.alert*120)

    # Start Orchestration
    def orchestration(dataWrapper):
        dataWrapper.update()
        thread = threading.Timer(10, orchestration, [dataWrapper])
        thread.daemon = True  # exit if main is stoped
        thread.start()
    orchestration(dataWrapper)

    # init console
    with Live(render_visual(dataWrapper), refresh_per_second=1) as live:
        while 1:
            time.sleep(0.5)
            live.update(render_visual(dataWrapper))


if __name__ == "__main__":
    main()
