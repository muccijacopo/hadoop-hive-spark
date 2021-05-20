#!/bin/bash

hadoop fs -rm -R /app/output/spark/nearest-companies-report
spark-submit --master yarn ./spark/nearest-companies-report/job.py
