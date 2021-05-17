#!/usr/bin/env python3

import sys
import datetime

ticker_data = {}

for line in sys.stdin:
    line.strip()
    ticker, date, low_price, high_price, close_price = line.split("\t")

    # convert prices to float
    try:
        low_price = float(low_price)
        high_price = float(high_price)
        close_price = float(close_price)
    except ValueError:
        continue

    if ticker not in ticker_data:
        ticker_data[ticker] = {
            'dates': [date, date],
            'prices': [low_price, high_price],
            'first_close_price': close_price,
            'last_close_price': close_price
        }

    first_date, last_date = ticker_data[ticker]['dates']
    if date < first_date:
        ticker_data[ticker]['dates'][0] = date
        ticker_data[ticker]['first_close_price'] = close_price
    if date > last_date:
        ticker_data[ticker]['dates'][1] = date
        ticker_data[ticker]['last_close_price'] = close_price

    if low_price < ticker_data[ticker]['prices'][0]:
        ticker_data[ticker]['prices'][0] = low_price
    if high_price > ticker_data[ticker]['prices'][1]:
        ticker_data[ticker]['prices'][1] = high_price

ordered_ticker_data = dict(sorted(ticker_data.items(), key=lambda t: t[1]['dates'][1], reverse=True))

for ticker, data in ordered_ticker_data.items():
    stock_var = round((
            (data['last_close_price'] - data['first_close_price']) / data['first_close_price'] * 100), 2)

    print(f"{ticker}, {data['dates'][0]}, {data['dates'][1]}, {data['prices'][0]}, {data['prices'][1]}, {stock_var}%")
