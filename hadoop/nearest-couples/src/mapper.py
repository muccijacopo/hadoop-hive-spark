#!/usr/bin/env python3

import sys
import datetime

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


for line in sys.stdin:
    line = line.strip()
    labels = line.split(',')
    if len(labels) == FIRST_FILE_LABELS:
        date = labels[DATE]
        year, month, day = date.split("-")
        if int(year) != 2017:
            continue
        else:
            print(f"{labels[TICKER]}\t{TABLE_A}\t{date}\t{labels[CLOSE_PRICE]}")
    elif len(labels) == SECOND_FILE_LABELS:
        print(f"{labels[TICKER]}\t{TABLE_B}\t{labels[NAME]}")
    else:
        continue