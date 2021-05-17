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


def get_month_from_date(date: str):
    year, month, day = date.split("-")
    return int(month)


input_folder_path = "hdfs:///app/input"
output_folder_path = "hdfs:///app/output/spark/nearest-companies-report"

conf = SparkConf().setAppName('Spark App')
sc = SparkContext(conf=conf)

stock_sectors_data = sc.textFile(input_folder_path + "/stock_sectors.csv") \
    .map(lambda row: row.split(",")) \
    .map(lambda x: (x[SecondFileLabels.TICKER], x[SecondFileLabels.COMPANY]))

stock_prices_data = sc.textFile(input_folder_path + "/stock_prices.csv") \
    .map(lambda row: row.split(",")) \
    .filter(lambda x: get_year_from_date(x[FirstFileLabels.DATE]) == 2017) \
    .map(lambda x: ((x[FirstFileLabels.TICKER], get_month_from_date(x[FirstFileLabels.DATE])), x))

stocks_first_tx_month = stock_prices_data.reduceByKey(
    lambda a, b: a if a[FirstFileLabels.DATE] < b[FirstFileLabels.DATE] else b)
stocks_last_tx_month = stock_prices_data.reduceByKey(
    lambda a, b: b if a[FirstFileLabels.DATE] < b[FirstFileLabels.DATE] else a)

stocks_first_last_month: RDD = stocks_first_tx_month.join(stocks_last_tx_month)
companies_stocks_prices_month: RDD = stocks_first_last_month.map(lambda d: (d[0][0], d[1])).join(stock_sectors_data)
companies_var_month = companies_stocks_prices_month \
    .map(lambda d: ((d[1][1], get_month_from_date(d[1][0][0][FirstFileLabels.DATE])),
                    (float(d[1][0][0][FirstFileLabels.CLOSE_PRICE]), float(d[1][0][1][FirstFileLabels.CLOSE_PRICE])))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: ((v[1] - v[0]) / v[0] * 100)) \
    .map(lambda x: (x[0][1], (x[0][0], x[1])))

companies_couples_var_month: RDD = companies_var_month.join(companies_var_month)
companies_couples_var_month\
    .filter(lambda x: abs(x[1][0][1] - x[1][1][1]) <= 1 and x[1][0][0] != x[1][1][0])\
    .sortBy(lambda x: (x[1][0][0], x[1][1][0], x[0]), ascending=True)\
    .saveAsTextFile(output_folder_path)
