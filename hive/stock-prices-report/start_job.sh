#!/bin/bash

hadoop fs -cp /app/input/stock_prices.csv /app/input/stock_prices_copy.csv
hadoop fs -cp /app/input/stock_prices_50.csv /app/input/stock_prices_50_copy.csv
hive --f hive/stock-prices-report/drop_tables.hql
hive --f hive/stock-prices-report/init_tables.hql
hive --f hive/stock-prices-report/job.hql

# hive --e "select * from stock_prices_report limit 20;"

