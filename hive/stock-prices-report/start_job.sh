#!/bin/bash

hadoop fs -cp /input/stockprices/first.csv /input/stockprices/first_copy.csv
echo "Cleaning previous work..."
hive --f hive/stock-prices-report/clean_before.hql
echo "Creating first table from file..."
hive --f hive/stock-prices-report/init_tables.hql
echo "Starting job..."
hive --f hive/stock-prices-report/job.hql
echo "Cleaning temp data..."
hive --f hive/stock-prices-report/clean_after.hql

hive --e "select * from stock_prices_report limit 20;"

