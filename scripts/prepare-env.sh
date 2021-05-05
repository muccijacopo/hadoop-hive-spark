hadoop fs -mkdir /input
hadoop fs -mkdir /input/stockprices
hadoop fs -put ./files/modified/first.csv /input/stockprices
hadoop fs -put ./files/modified/merge.csv /input/stockprices
hadoop fs -mkdir /output
hadoop fs -mkdir /output/stockprices
