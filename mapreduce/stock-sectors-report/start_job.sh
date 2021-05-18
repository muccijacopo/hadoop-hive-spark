#!/bin/bash

hadoop fs -rm -r /app/output/mapreduce/stock-sectors-report
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./mapreduce/stock-sectors-report/src/mapper.py -reducer ./mapreduce/stock-sectors-report/src/reducer.py -input /app/input/stocks_complete.csv -output /app/output/mapreduce/stock-sectors-report
