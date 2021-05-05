#!/bin/bash

echo "Deleting output folder..."
hadoop fs -rm -r /output/stockprices/companies-couples
echo "Starting job..."
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./hadoop/nearest-couples/src/mapper.py -reducer ./hadoop/nearest-couples/src/reducer.py -input /input/stockprices/merge.csv -output /output/stockprices/companies-couples