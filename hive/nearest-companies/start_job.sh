#!/bin/bash

hadoop fs -cp /input/stockprices/stock_prices.csv /input/stockprices/stock_prices_copy.csv
hadoop fs -cp /input/stockprices/stock_sectors.csv /input/stockprices/stock_sectors_copy.csv

hive --f ./hive/nearest-companies/init_tables.hql