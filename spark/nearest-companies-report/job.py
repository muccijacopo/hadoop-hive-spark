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
companies_stocks_prices_month_list = companies_stocks_prices_month \
    .map(lambda d: ((d[1][1], get_month_from_date(d[1][0][0][FirstFileLabels.DATE])),
                    (float(d[1][0][0][FirstFileLabels.CLOSE_PRICE]), float(d[1][0][1][FirstFileLabels.CLOSE_PRICE])))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: ((v[1] - v[0]) / v[0] * 100)) \
    .map(lambda x: (x[0][1], (x[0][0], x[1]))) \
    # .aggregateByKey(
    #     [],
    #     seqFunc=lambda acc, el: acc + [el[0], el[1]] if abs(el[1][1] - el[1][0]) <= 1 else acc,
    #     combFunc=lambda acc, el: acc + [el[0], el[1]] if abs(el[1][1] - el[1][0]) <= 1 else acc
    # )\
    .saveAsTextFile(output_folder_path)

# couples = []
# for companyA in companies_stocks_prices_month_list:
#     for companyB in companies_stocks_prices_month_list:
#         if companyA['company_name'] == companyB['company_name']:
#             continue


#
# complete_data: RDD = stock_prices_data.join(stock_sectors_data)\
#     .map(lambda data: {
#         'ticker': data[1][0][FirstFileLabels.TICKER],
#         'open_price': float(data[1][0][FirstFileLabels.OPEN_PRICE]),
#         'close_price': float(data[1][0][FirstFileLabels.CLOSE_PRICE]),
#         'lower_price': float(data[1][0][FirstFileLabels.LOWER_PRICE]),
#         'higher_price': float(data[1][0][FirstFileLabels.HIGHER_PRICE]),
#         'volume': int(data[1][0][FirstFileLabels.VOLUME]),
#         'date': data[1][0][FirstFileLabels.DATE],
#         'exchange': data[1][1][SecondFileLabels.EXCHANGE],
#         'company': data[1][1][SecondFileLabels.COMPANY],
#         'sector': data[1][1][SecondFileLabels.SECTOR],
#         'industry': data[1][1][SecondFileLabels.INDUSTRY],
#     })

# stocks_first_tx_month = stock_prices_data\
#     .map(lambda x: ((x[0], get_month_from_date(x[1][FirstFileLabels.DATE])), {
#         'date': x[1][FirstFileLabels.DATE],
#         'close_price': x[1][FirstFileLabels.CLOSE_PRICE]
# }))
