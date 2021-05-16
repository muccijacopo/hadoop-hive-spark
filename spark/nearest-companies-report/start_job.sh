#!/bin/bash

hadoop fs -rm -R /app/output/spark/nearest-companies-report
spark-submit --master yarn ./spark/nearest-companies-report/job.py
hadoop fs -cat /app/output/spark/nearest-companies-report/part-00000 | head -20