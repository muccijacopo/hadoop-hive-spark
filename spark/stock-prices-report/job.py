from pyspark import SparkConf, SparkContext
from pyspark.rdd import PipelinedRDD
import argparse

TICKER = 0
OPEN_PRICE = 1
CLOSE_PRICE = 2
ADJ_CLOSE_PRICE = 3
LOWER_PRICE = 4
HIGHER_PRICE = 5
VOLUME = 6
DATE = 7

parser = argparse.ArgumentParser()
parser.add_argument("--output_path", type=str, help="Output folder path")

input_folder_path = "hdfs:///app/input"
output_path = "hdfs:///app/output"

conf = SparkConf().setAppName('Spark App')
sc = SparkContext(conf=conf)

data = sc.textFile(input_folder_path + "/stock_prices.csv").map(f=lambda row: row.split(","))

ticker2stock_data = data.map(lambda row: (row[TICKER], {
    'open_price': float(row[OPEN_PRICE]),
    'close_price': float(row[CLOSE_PRICE]),
    'lower_price': float(row[LOWER_PRICE]),
    'higher_price': float(row[HIGHER_PRICE]),
    'date': row[DATE]
}))

stocks_first_tx = ticker2stock_data\
    .reduceByKey(lambda x, y: x if (x['date'] < y['date']) else y)\
    .mapValues(lambda x: {
        'date': x['date'],
        'close_price': x['close_price']
    })

stocks_last_tx = ticker2stock_data\
    .reduceByKey(lambda x, y: y if (x['date'] < y['date']) else x)\
    .mapValues(lambda x: {
        'date': x['date'],
        'close_price': x['close_price']
    })

stocks_min_price = ticker2stock_data\
    .reduceByKey(func=lambda a, b: a if a['lower_price'] < b['lower_price'] else b) \
    .mapValues(lambda x: {
        'lower_price': x['lower_price']
    })

stocks_max_price = ticker2stock_data\
    .reduceByKey(func=lambda a, b: b if a['higher_price'] < b['higher_price'] else a) \
    .mapValues(lambda x: {
        'higher_price': x['higher_price']
    })

stocks_prices = stocks_min_price\
        .join(stocks_max_price)\
        .mapValues(lambda x: (x[0]['lower_price'], x[1]['higher_price']))

stocks_first_last = stocks_first_tx\
    .join(stocks_last_tx)\
    .mapValues(lambda x: (x[0]['date'], x[1]['date'], (x[1]['close_price'] - x[0]['close_price']) / x[0]['close_price'] * 100))

stocks_first_last.join(stocks_prices).sortByKey(ascending=True).saveAsTextFile(output_path)