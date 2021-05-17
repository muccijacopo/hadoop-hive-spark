#!/bin/bash

hadoop fs -rm -r /app/output/mapreduce/nearest-companies-report
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./mapreduce/nearest-companies-report/src/mapper.py -reducer ./mapreduce/nearest-companies-report/src/reducer.py -input /app/input/stocks_complete.csv -output /app/output/mapreduce/nearest-companies-report