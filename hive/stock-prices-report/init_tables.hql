CREATE TABLE IF NOT EXISTS stock_prices (ticker STRING, open_price FLOAT, close_price FLOAT, close_adj FLOAT, lowest_price FLOAT, highest_price FLOAT, volume INT, date_ STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'hdfs:///app/input/stock_prices_100_copy.csv' INTO TABLE stock_prices;
