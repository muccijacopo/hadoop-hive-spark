#!/usr/bin/env python3

import sys

TICKER = 0
PRICE_OPEN = 1
CLOSE_PRICE = 2
ADJ_CLOSE = 3
LOW_PRICE = 4
HIGH_PRICE = 5
VOLUME = 6
DATE = 7

for line in sys.stdin:
    line = line.strip()
    data = line.split(',')
    print("%s\t%s\t%s\t%s\t%s" %
          (data[TICKER],
           data[DATE],
           data[LOW_PRICE],
           data[HIGH_PRICE],
           data[CLOSE_PRICE])
          )
