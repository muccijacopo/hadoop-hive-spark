#!/usr/bin/env python3

import sys
from enum import IntEnum


class FirstFileLabels(IntEnum):
    TICKER = 0
    OPEN_PRICE = 1
    CLOSE_PRICE = 2
    ADJ_CLOSE_PRICE = 3
    LOWER_PRICE = 4
    HIGHER_PRICE = 5
    VOLUME = 6
    DATE = 7


class SecondFileLabels(IntEnum):
    TICKER = 0
    EXCHANGE = 1
    COMPANY = 2
    SECTOR = 3
    INDUSTRY = 4


FIRST_FILE_LABELS = 8
SECOND_FILE_LABELS = 5
TABLE_A = 'TABLE_A'
TABLE_B = 'TABLE_B'

years_report = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]

for line in sys.stdin:
    line = line.strip()
    labels = line.split(',')
    if len(labels) == FIRST_FILE_LABELS:
        date = labels[FirstFileLabels.DATE]
        year = date.split("-")[0]
        if int(year) in years_report:
            print(f"{labels[FirstFileLabels.TICKER]}\t{TABLE_A}\t{labels[FirstFileLabels.DATE]}\t{labels[FirstFileLabels.VOLUME]}\t{labels[FirstFileLabels.CLOSE_PRICE]}")
    elif len(labels) == SECOND_FILE_LABELS:
        print(f"{labels[SecondFileLabels.TICKER]}\t{TABLE_B}\t{labels[SecondFileLabels.EXCHANGE]}\t{labels[SecondFileLabels.COMPANY]}\t{labels[SecondFileLabels.SECTOR]}\t{labels[SecondFileLabels.INDUSTRY]}")
    else:
        continue
