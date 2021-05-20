#!/bin/bash

hadoop fs -cp /app/input/stock_prices_50.csv /app/input/stock_prices_50_copy.csv
hadoop fs -cp /app/input/stock_prices.csv /app/input/stock_prices_copy.csv
hadoop fs -cp /app/input/stock_sectors.csv /app/input/stock_sectors_copy.csv

# hive --f ./hive/nearest-companies-report/drop_tables.hql
hive --f ./hive/nearest-companies-report/init_tables.hql
hive --f ./hive/nearest-companies-report/job.hql
