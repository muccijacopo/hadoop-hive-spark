#!/bin/bash

hadoop fs -cp /input/stockprices/first.csv /input/stockprices/first_copy.csv
hadoop fs -cp /input/stockprices/stock_sectors.csv /input/stockprices/stock_sectors_copy.csv

hive --f ./hive/stock-sectors-report/init_tables.hql