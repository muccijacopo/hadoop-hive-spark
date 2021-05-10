#!/bin/bash

hadoop fs -rm -R /output/preprocessing/stock-sectors
hadoop jar /usr/local/Cellar/hadoop/3.3.0/streaming/hadoop-streaming.jar -mapper ./preprocessing/mapper.py -reducer ./preprocessing/reducer.py -input /input/stockprices/second.csv -output /output/preprocessing/stock-sectors
# sed '1d' files/original/historical_stocks.csv > files/modified/second.csv