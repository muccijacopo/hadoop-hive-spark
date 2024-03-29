hadoop fs -mkdir /app
hadoop fs -mkdir /app/input
hadoop fs -put ./files/modified/stock_prices.csv /app/input
hadoop fs -put ./files/modified/stock_sectors.csv /app/input
hadoop fs -put ./files/modified/stocks_complete.csv /app/input
hadoop fs -put ./files/modified/stock_prices_50.csv /app/input
hadoop fs -put ./files/modified/stocks_complete_50.csv /app/input
hadoop fs -put ./files/modified/stock_prices_100.csv /app/input
hadoop fs -put ./files/modified/stocks_complete_100.csv /app/input



echo "Creating output folders"
hadoop fs -mkdir /app/output
hadoop fs -mkdir /app/output/mapreduce
hadoop fs -mkdir /app/output/hive
hadoop fs -mkdir /app/output/spark

