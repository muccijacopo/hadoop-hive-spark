#!/bin/bash

echo "Deleting output folder..."
hadoop fs -rm -r /output/stockprices/sectors-report
echo "Starting job..."
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./hadoop/stock-sectors-report/src/mapper.py -reducer ./hadoop/stock-sectors-report/src/reducer.py -input /input/stockprices/merge.csv -output /output/stockprices/sectors-report
