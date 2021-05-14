#!/bin/bash

hadoop fs -rm -R /app/output
spark-submit --master yarn ./spark/stock-prices-report/job.py
hadoop fs -cat /app/output/part-00000 | head -20