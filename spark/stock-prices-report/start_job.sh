#!/bin/bash

hadoop fs -rm -R /app/output/spark/stock-prices-report
hadoop fs -mkdir /app/output/spark/stock-prices-report
spark-submit --master yarn ./spark/stock-prices-report/job.py
hadoop fs -cat /app/output/spark/stock-prices-report/part-00000 | head -20