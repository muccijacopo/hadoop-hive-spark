sed '1d' files/original/historical_stock_prices.csv > files/modified/first.csv
sed '1d' files/original/historical_stocks.csv > files/modified/second.csv
cat files/modified/first 
