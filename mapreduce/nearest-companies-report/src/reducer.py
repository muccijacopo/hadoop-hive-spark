#!/usr/bin/env python3

import sys
import datetime

TABLE_A = 'TABLE_A'
TABLE_B = 'TABLE_B'

stocks_data = {}
companies_to_tickers = {}
companies = set()


def get_date_from_string(date: str):
    year, month, day = date.split("-")
    formatted_date = datetime.datetime(int(year), int(month), int(day))
    return formatted_date


def init_stock():
    return {
        'prices': {},
        'company_name': ''
    }


def init_stock_prices(date, close_price):
    return {
        'first_close_price': {
            'date': date,
            'price': float(close_price)
        },
        'last_close_price': {
            'date': date,
            'price': float(close_price)
        }
    }

def calculate_variation(value1, value2):
    return (value2 - value1) / value1 * 100


for line in sys.stdin:
    line = line.strip()
    data = line.split("\t")

    ticker = data[0]
    table = data[1]

    if ticker not in stocks_data:
        stocks_data[ticker] = init_stock()

    # Stock data
    if table == TABLE_A:
        date_string = data[2]
        close_price = data[3]

        month = date_string.split("-")[1]
        date = get_date_from_string(date_string)

        if month not in stocks_data[ticker]['prices']:
            stocks_data[ticker]['prices'][month] = init_stock_prices(date, close_price)

        if date < stocks_data[ticker]['prices'][month]['first_close_price']['date']:
            stocks_data[ticker]['prices'][month]['first_close_price']['date'] = date
            stocks_data[ticker]['prices'][month]['first_close_price']['price'] = float(close_price)

        if date > stocks_data[ticker]['prices'][month]['last_close_price']['date']:
            stocks_data[ticker]['prices'][month]['last_close_price']['date'] = date
            stocks_data[ticker]['prices'][month]['last_close_price']['price'] = float(close_price)

    if table == TABLE_B:
        company_name = data[2]
        stocks_data[ticker]['company_name'] = company_name
        companies.add(company_name)


companies_variation_per_month = {}
for company in sorted(companies):
    company_stocks = [(ticker, values) for ticker, values in stocks_data.items() if values['company_name'] == company]
    for ticker, stock_values in company_stocks:
        for month in stock_values['prices']:
            total_first_close_price = 0
            total_last_close_price = 0
            total_first_close_price += stock_values['prices'][month]['first_close_price']['price']
            total_last_close_price += stock_values['prices'][month]['last_close_price']['price']
            month_variation = (total_last_close_price - total_first_close_price) / total_first_close_price * 100
            if company not in companies_variation_per_month:
                companies_variation_per_month[company] = {}
            companies_variation_per_month[company][month] = month_variation

results = []
threshold = 1
for companyA, companyA_values in companies_variation_per_month.items():
    for companyB, companyB_values in companies_variation_per_month.items():
        if companyB == companyA:
            continue
        # try:
        #     matching_months = [month for (month, variation) in companyB_values.items() if abs(variation - companyA_values[month]) <= threshold]
        # except:
        #     continue

        for month, var in companyB_values.items():
            try:
                if abs(companyA_values[month] - var) <= threshold:
                    results.append({
                        'companyA': companyA,
                        'companyA_var': companyA_values[month],
                        'companyB': companyB,
                        'companyB_var': var,
                        'month': month
                    })
            except:
                continue

        #if len(matching_months) == len(companyA_values.keys()):
        # if len(matching_months) > 1:
        #     couples.append((companyA, companyB))


for couple in results:
    # for month in companies_variation_per_month[companyA]:
    #     try:
    #         print(f"{month}, {companyA}, {companyB}, {companies_variation_per_month[companyA][month]}%, {companies_variation_per_month[companyB][month]}%")
    #     except:
    #         continue
    print(
        f"{couple['month']}, {couple['companyA']}, {couple['companyA_var']}%, {couple['companyB']}, {couple['companyB_var']}%"
    )