#!/bin/bash

echo "Removing old output folder..."
hadoop fs -rm -r /output
echo "Creating new output folder..."
hadoop fs -mkdir /output
echo "Starting MapReduce Job..."
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper python-mapreduce/src/mapper.py -reducer python-mapreduce/src/reducer.py -input /input/stockprice/historical_stock_prices.csv -output /output/stockprice
