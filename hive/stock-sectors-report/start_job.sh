#!/bin/bash

hadoop fs -cp /app/input/stock_prices.csv /app/input/stock_prices_copy.csv
hadoop fs -cp /app/input/stock_sectors.csv /app/input/stock_sectors_copy.csv

hive --f ./hive/stock-sectors-report/init_tables.hql