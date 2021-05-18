from enum import IntEnum
import random
import datetime

class FirstFileLabels(IntEnum):
    TICKER = 0
    OPEN_PRICE = 1
    CLOSE_PRICE = 2
    ADJ_CLOSE_PRICE = 3
    LOWER_PRICE = 4
    HIGHER_PRICE = 5
    VOLUME = 6
    DATE = 7


start_date = datetime.date(1970, 1, 1)
end_date = datetime.date(2020, 12, 31)
time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days

input_folder_path = "./files/modified/stock_prices_big.csv"

tickers_list = set()

f = open(input_folder_path, "r")
lines = f.readlines()

for line in lines:
    labels = line.split(",")
    ticker = labels[FirstFileLabels.TICKER]
    tickers_list.add(ticker)

f.close()

f = open(input_folder_path, "a")
for stock in range(200):
    ticker = random.sample(tickers_list, k=1)[0]
    open_price = random.uniform(8, 20)
    close_price = random.uniform(8, 20)
    close_price_adj = random.uniform(8, 20)
    lower_price = random.uniform(8, 20)
    while lower_price > open_price or lower_price > close_price:
        lower_price = random.uniform(8, 20)
    higher_price = random.uniform(8, 20)
    while higher_price < close_price or higher_price < open_price or higher_price < lower_price:
        higher_price = random.uniform(8, 20)

    volume = random.randint(4000000, 6000000)
    random_number_of_days_from_start_date = random.randrange(days_between_dates)
    date = start_date + datetime.timedelta(days=random_number_of_days_from_start_date)

    stock_data = [ticker, open_price, close_price, close_price_adj, lower_price, higher_price, volume, date]
    f.write(','.join([str(el) for el in stock_data]) + "\n")

f.close()
