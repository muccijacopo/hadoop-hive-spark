#!/usr/bin/env python3

import sys
import re

for line in sys.stdin:
    line = line.strip()
    newline = re.sub(',(?=[^"]*"[^"]*(?:"[^"]*"[^"]*)*$)',";", line)
    columns = newline.split(",")
    print("\t".join(columns))