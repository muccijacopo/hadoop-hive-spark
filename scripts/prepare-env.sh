hadoop fs -mkdir /app
hadoop fs -mkdir /app/input
hadoop fs -put ./files/modified/stock_prices.csv /app/input
hadoop fs -put ./files/modified/stock_sectors.csv /app/input
hadoop fs -put ./files/modified/merge.csv /app/input
