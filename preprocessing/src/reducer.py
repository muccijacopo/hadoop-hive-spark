#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()
    columns = line.split("\t")

    print(",".join(columns))