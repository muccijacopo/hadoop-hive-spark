CREATE TABLE IF NOT EXISTS stock_prices_filtered AS
    SELECT * 
    FROM stock_prices
    WHERE year(date_) = 2017;

CREATE TABLE IF NOT EXISTS stock_prices_first_date_of_month AS
    SELECT DISTINCT t1.ticker, t1.date_, month(t1.date_) as month_, t2.close_price
    FROM (
        SELECT ticker, MIN(date_) date_
        FROM stock_prices_filtered
        GROUP BY ticker, month(date_)
    ) t1 JOIN stock_prices_filtered t2 ON (t1.ticker = t2.ticker AND t1.date_ = t2.date_);

CREATE TABLE IF NOT EXISTS stock_prices_last_date_of_month AS
    SELECT DISTINCT t1.ticker, t1.date_, month(t1.date_) as month_, t2.close_price
    FROM (
        SELECT ticker, MAX(date_) date_
        FROM stock_prices_filtered
        GROUP BY ticker, month(date_)
    ) t1 JOIN stock_prices_filtered t2 ON (t1.ticker = t2.ticker AND t1.date_ = t2.date_);

CREATE TABLE IF NOT EXISTS company_total_stocks_price_var_month AS
    SELECT t3.company, t3.month_, (t3.end_month_close_price - t3.start_month_close_price) / t3.start_month_close_price * 100 as var
    FROM (   
        SELECT t.company, t1.month_, SUM(t1.close_price) as start_month_close_price, SUM(t2.close_price) as end_month_close_price
        FROM stock_sectors t
        JOIN stock_prices_first_date_of_month t1 ON t.ticker = t1.ticker
        JOIN stock_prices_last_date_of_month t2 ON t.ticker = t2.ticker
        WHERE t1.month_ = t2.month_
        GROUP BY t.company, t1.month_
    ) t3
    ORDER BY t3.company, t3.month_;

CREATE TABLE IF NOT EXISTS companies_nearest_couples AS
    SELECT t1.month_, t1.company as company1_name, t1.var as company1_var, t2.company as company2, t2.var as company2_var
    FROM company_total_stocks_price_var_month t1
    JOIN company_total_stocks_price_var_month t2
    ON (t1.company != t2.company AND abs(t1.var - t2.var) <= 1 AND t1.month_ = t2.month_) 
    ORDER BY t1.company, t2.company, t1.month_