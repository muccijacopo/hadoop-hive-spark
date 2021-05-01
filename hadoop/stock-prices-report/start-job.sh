#!/bin/bash

echo "Removing old output folder..."
hadoop fs -rm -r /output
echo "Creating new output folder..."
hadoop fs -mkdir /output
echo "Starting MapReduce Job..."
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./hadoop/stock-prices-report/src/mapper.py -reducer ./hadoop/stock-prices-report/src/reducer.py -input /input/stockprices/first.csv -output /output/stockprices/prices
