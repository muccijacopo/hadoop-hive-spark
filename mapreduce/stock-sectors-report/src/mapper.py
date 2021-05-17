#!/usr/bin/env python3

import sys

# heuristic to determine which portion is
FIRST_FILE_LABELS = 8
SECOND_FILE_LABELS = 5

# First file labels position
TICKER = 0
PRICE_OPEN = 1
CLOSE_PRICE = 2
ADJ_CLOSE = 3
LOW_PRICE = 4
HIGH_PRICE = 5
VOLUME = 6
DATE = 7

# SECOND FILE LABELS POSITION
TICKER = 0
EXCHANGE = 1
NAME = 2
SECTOR = 3
INDUSTRY = 4

TABLE_A = 'TABLE_A'
TABLE_B = 'TABLE_B'

years_report = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]

for line in sys.stdin:
    line = line.strip()
    labels = line.split(',')
    if len(labels) == FIRST_FILE_LABELS:
        date = labels[DATE]
        year = date.split("-")[0]
        if int(year) in years_report:
            print(f"{labels[TICKER]}\t{TABLE_A}\t{labels[DATE]}\t{labels[VOLUME]}\t{labels[CLOSE_PRICE]}")
    elif len(labels) == SECOND_FILE_LABELS:
        print(f"{labels[TICKER]}\t{TABLE_B}\t{labels[EXCHANGE]}\t{labels[NAME]}\t{labels[SECTOR]}\t{labels[INDUSTRY]}")
    else:
        continue
