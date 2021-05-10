CREATE TABLE stocks_first_date AS
    SELECT *
    FROM (
        SELECT ticker, close_price, date_, row_number() OVER (PARTITION BY ticker ORDER BY date_ ASC) rn
        FROM stock_prices
    ) t
    WHERE t.rn = 1;

CREATE TABLE stocks_last_date as
    SELECT *
    FROM (
        SELECT ticker, close_price, date_, row_number() OVER (PARTITION BY ticker ORDER BY date_ DESC) rn
        FROM stock_prices
    ) t
    WHERE t.rn = 1;

CREATE TABLE stocks_variation AS
    SELECT t1.ticker, (t2.close_price - t1.close_price) / t1.close_price * 100 as var
    FROM (
        stocks_first_date t1 
        JOIN stocks_last_date t2 ON t1.ticker = t2.ticker
    );

CREATE TABLE stocks_minmax AS
    SELECT ticker, min(date_) as first_date, max(date_) as last_date, min(lowest_price) as lowest_price, max(highest_price) as highest_price
    FROM stock_prices
    GROUP BY ticker;


--- CREATION OF RESULT TABLE
CREATE TABLE stock_prices_report as
    SELECT t1.ticker, t2.first_date, t2.last_date, t2.lowest_price, t2.highest_price, t1.var
    FROM (
        stocks_variation t1 JOIN stocks_minmax t2
        ON t1.ticker = t2.ticker
    )
    ORDER BY t2.last_date DESC;

--- CLEANING
DROP TABLE stocks_first_date;
DROP TABLE stocks_last_date;
DROP TABLE stocks_variation;
DROP TABLE stocks_minmax;