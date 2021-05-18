#!/usr/bin/env python3

import sys
import datetime

TABLE_A = "TABLE_A"
TABLE_B = "TABLE_B"


def init_stock(close_price, date):
    return {
        'total_volume': 0,
        'first_close_price': {
            'date': date,
            'value': close_price
        },
        'last_close_price': {
            'date': date,
            'value': close_price
        }
    }


def get_best_stock_by_increment(stocks, year):
    best_increment = 0
    best_ticker = None
    try:
        for stock in stocks:
            close_price_increment = (stock[1][year]['last_close_price']['value'] - stock[1][year]['first_close_price']['value']) / stock[1][year]['first_close_price']['value'] * 100
            if close_price_increment >= best_increment:
                best_increment = close_price_increment
                best_ticker = stock[0]
    except:
        return best_ticker, best_increment

    return best_ticker, best_increment


def get_best_stock_by_volume(stocks, year):
    best_volume = 0
    best_stock_ticker = None
    for stock in stocks:
        ticker, stock_data = stock
        if stock_data[year]['total_volume'] > best_volume:
            best_volume = stock_data[year]['total_volume']
            best_stock_ticker = ticker
    return best_stock_ticker, best_volume


def get_sector_year_var(stocks, year):
    total_first_close_price = 0
    total_last_close_price = 0
    for stock in stocks:
        ticker, stock_data = stock
        total_first_close_price += stock_data[year]['first_close_price']['value']
        total_last_close_price += stock_data[year]['last_close_price']['value']
    if total_first_close_price == 0:
        return 0
    else:
        return round((total_last_close_price - total_first_close_price) / total_first_close_price * 100, 2)


sector_2_stocks = {}
stocks_data = {}

for line in sys.stdin:
    line = line.strip()
    data = line.split('\t')
    if len(data) == 0:
        continue

    try:
        ticker = data[0]
        table = data[1]
    except IndexError:
        continue

    if ticker not in stocks_data:
        stocks_data[ticker] = {}

    # Stock info (sector, exchange, ...)
    if table == TABLE_B:
        try:
            sector = data[4]
            if sector not in sector_2_stocks:
                sector_2_stocks[sector] = set()

            sector_2_stocks[sector].add(ticker)
        except:
            continue

    # Stock prices by day
    if table == TABLE_A:
        try:
            year, month, day = data[2].split("-")
            date = datetime.datetime(int(year), int(month), int(day))
            volume = int(data[3])
            close_price = float(data[4])
            if year not in stocks_data[ticker]:
                stocks_data[ticker][year] = init_stock(close_price, date)

            first_close_price_date = stocks_data[ticker][year]['first_close_price']['date']
            last_close_price_date = stocks_data[ticker][year]['last_close_price']['date']
            if date < first_close_price_date:
                stocks_data[ticker][year]['first_close_price']['date'] = date
                stocks_data[ticker][year]['first_close_price']['value'] = close_price
            if date > last_close_price_date:
                stocks_data[ticker][year]['last_close_price']['date'] = date
                stocks_data[ticker][year]['last_close_price']['value'] = close_price
            stocks_data[ticker][year]['total_volume'] += volume
        except:
            continue

ordered_sectors = sorted(sector_2_stocks.keys())
for sector in ordered_sectors:
    years_report = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    sector_stocks = [(ticker, stocks_data[ticker]) for ticker in sector_2_stocks[sector]]
    for year in years_report:
        year = str(year)
        filtered_sector_stocks = [stock for stock in sector_stocks if year in stock[1]]
        best_stock_by_increment = get_best_stock_by_increment(filtered_sector_stocks, year)
        best_stock_by_volume = get_best_stock_by_volume(filtered_sector_stocks, year)
        sector_var = get_sector_year_var(filtered_sector_stocks, year)
        print(f"{sector}, {year}, {sector_var}%, {best_stock_by_increment[0] or 'N/D'}({round(best_stock_by_increment[1], 2)}%), {best_stock_by_volume[0] or 'N/D'}({best_stock_by_volume[1]})")
