#!/bin/bash

hadoop fs -rm -r /app/output/mapreduce/stock-prices-report
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar \
      -mapper ./mapreduce/stock-prices-report/src/mapper.py \
      -reducer ./mapreduce/stock-prices-report/src/reducer.py \
      -input /app/input/stock_prices_50.csv \
      -output /app/output/mapreduce/stock-prices-report
