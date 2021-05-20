DROP TABLE IF EXISTS stock_prices;
DROP TABLE IF EXISTS stock_sectors;

CREATE TABLE stock_prices (ticker STRING, open_price FLOAT, close_price FLOAT, close_adj FLOAT, lowest_price FLOAT, highest_price FLOAT, volume INT, date_ STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'hdfs:///app/input/stock_prices_50_copy.csv' INTO TABLE stock_prices;

CREATE TABLE stock_sectors (ticker STRING, exchange_ STRING, company STRING, sector STRING, industry STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'hdfs:///app/input/stock_sectors_copy.csv' INTO TABLE stock_sectors;
