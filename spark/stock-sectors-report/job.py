from pyspark import SparkConf, SparkContext, RDD
from enum import IntEnum


class FirstFileLabels(IntEnum):
    TICKER = 0
    OPEN_PRICE = 1
    CLOSE_PRICE = 2
    ADJ_CLOSE_PRICE = 3
    LOWER_PRICE = 4
    HIGHER_PRICE = 5
    VOLUME = 6
    DATE = 7


class SecondFileLabels(IntEnum):
    TICKER = 0
    EXCHANGE = 1
    COMPANY = 2
    SECTOR = 3
    INDUSTRY = 4


def get_year_from_date(date: str):
    year, month, day = date.split("-")
    return int(year)


input_folder_path = "hdfs:///app/input"
output_folder_path = "hdfs:///app/output"

conf = SparkConf().setAppName('Spark App')
sc = SparkContext(conf=conf)

stock_prices_data = sc.textFile(input_folder_path + "/stock_prices.csv")\
    .map(lambda row: row.split(","))\
    .filter(lambda x: get_year_from_date(x[FirstFileLabels.DATE]) > 2008 and get_year_from_date(x[FirstFileLabels.DATE]) < 2019)\
    .map(lambda x: (x[FirstFileLabels.TICKER], x))
stock_sectors_data = sc.textFile(input_folder_path + "/stock_sectors.csv")\
    .map(lambda row: row.split(","))\
    .map(lambda x: (x[SecondFileLabels.TICKER], x))

complete_data: RDD = stock_prices_data.join(stock_sectors_data)\
    .map(lambda data: {
        'ticker': data[1][0][FirstFileLabels.TICKER],
        'open_price': float(data[1][0][FirstFileLabels.OPEN_PRICE]),
        'close_price': float(data[1][0][FirstFileLabels.CLOSE_PRICE]),
        'lower_price': float(data[1][0][FirstFileLabels.LOWER_PRICE]),
        'higher_price': float(data[1][0][FirstFileLabels.HIGHER_PRICE]),
        'volume': int(data[1][0][FirstFileLabels.VOLUME]),
        'date': data[1][0][FirstFileLabels.DATE],
        'exchange': data[1][1][SecondFileLabels.EXCHANGE],
        'company': data[1][1][SecondFileLabels.COMPANY],
        'sector': data[1][1][SecondFileLabels.SECTOR],
        'industry': data[1][1][SecondFileLabels.INDUSTRY],
    })


top_stock_by_volume_year = complete_data\
    .map(lambda x: ((x['sector'], get_year_from_date(x['date']), x['ticker']), x['volume']))\
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda x: ((x[0][0], x[0][1]), (x[0][2], x[1])))\
    .reduceByKey(lambda a, b: a if a[1] > b[1] else b)\
    .sortByKey()\





