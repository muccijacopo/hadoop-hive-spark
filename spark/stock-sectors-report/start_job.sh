#!/bin/bash

hadoop fs -rm -R /app/output/spark/stock-sectors-report
spark-submit --master yarn ./spark/stock-sectors-report/job.py
hadoop fs -cat /app/output/spark/stock-sectors-report/part-00000 | head -20
