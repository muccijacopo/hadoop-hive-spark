#!/usr/bin/env python3

import sys
import datetime
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


FIRST_FILE_LABELS_NUMBER = 8
SECOND_FILE_LABELS_NUMBER = 5
TABLE_A = 'TABLE_A'
TABLE_B = 'TABLE_B'


for line in sys.stdin:
    line = line.strip()
    labels = line.split(',')
    if len(labels) == FIRST_FILE_LABELS_NUMBER:
        date = labels[FirstFileLabels.DATE]
        year, month, day = date.split("-")
        if int(year) != 2017:
            continue
        else:
            print(f"{labels[FirstFileLabels.TICKER]}\t{TABLE_A}\t{date}\t{labels[FirstFileLabels.CLOSE_PRICE]}")
    elif len(labels) == SECOND_FILE_LABELS_NUMBER:
        print(f"{labels[SecondFileLabels.TICKER]}\t{TABLE_B}\t{labels[SecondFileLabels.COMPANY]}")