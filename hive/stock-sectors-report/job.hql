CREATE TABLE stock_prices_sectors AS
SELECT t1.ticker as ticker, t1.date_ as date_, t1.volume as volume, t2.company as company, t2.sector as sector
FROM (
    stock_prices t1 JOIN stock_sectors t2
    ON t1.ticker = t2.ticker
)
WHERE year(t1.date_) > 2008 AND year(t1.date_) < 2019 

