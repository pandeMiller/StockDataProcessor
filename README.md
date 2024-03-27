# StockDataProcessor

Data pipeline built on top of apache beam that extracts data from csv file, perform computations like moving averages and persist it in Postgres database. Currently, it only does 10-day moving average, but more computations will be added later. 
