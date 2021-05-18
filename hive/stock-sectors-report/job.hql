DROP TABLE IF EXISTS stock_sectors_report;

CREATE TABLE IF NOT EXISTS stock_prices_filtered AS
    SELECT *
    FROM stock_prices
    WHERE year(date_) > 2008 AND year(date_) < 2019;

CREATE TABLE IF NOT EXISTS stock_total_volumes_year AS
    SELECT ticker, year(date_) as year_, SUM(volume) as total_volume
    FROM stock_prices_filtered
    GROUP BY ticker, year(date_);

-- TASK C: azione del settore che ha avuto il maggior volume di transazioni nell’anno
CREATE TABLE IF NOT EXISTS sectors_best_stock_by_volumes_year AS
    SELECT t3.sector, t3.year_, t3.ticker, t3.total_volume
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY t2.sector, t2.year_ ORDER BY t2.total_volume DESC) rn
        FROM (
            SELECT t2.sector, t1.year_, t1.ticker, t1.total_volume
            FROM stock_total_volumes_year t1
            JOIN stock_sectors t2
            ON t1.ticker = t2.ticker
        ) t2
    ) t3
    WHERE t3.rn = 1;

CREATE TABLE IF NOT EXISTS first_stock_tx_by_year AS
    SELECT t2.ticker, year(t2.date_) as year_, t2.date_, t2.close_price
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY date_ ASC) rn 
            FROM stock_prices_filtered
        ) as t1
        WHERE t1.rn = 1
    ) t2;

CREATE TABLE IF NOT EXISTS last_stock_tx_by_year AS
    SELECT t2.ticker, year(t2.date_) as year_, t2.date_, t2.close_price
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY date_ DESC) rn 
            FROM stock_prices_filtered 
        ) as t1
        WHERE t1.rn = 1
    ) t2;

CREATE TABLE IF NOT EXISTS stocks_var_year AS
    SELECT t1.ticker, t1.year_, ROUND(((t2.close_price - t1.close_price) / t1.close_price * 100), 2) as var
    FROM first_stock_tx_by_year t1 
    JOIN last_stock_tx_by_year t2
    ON (t1.ticker = t2.ticker AND t1.year_ = t2.year_);

-- TASK B: azione del settore che ha avuto il maggior incremento percentuale nell’anno
CREATE TABLE IF NOT EXISTS sectors_best_stock_by_increment_year AS
    SELECT t4.sector, t4.year_, t4.ticker, t4.var
    FROM (
        SELECT t3.sector, t3.year_, t3.ticker, t3.var, ROW_NUMBER() OVER (PARTITION BY t3.sector, t3.year_ ORDER BY t3.var DESC) rn
        FROM (
            SELECT t1.sector, t2.year_, t2.ticker, t2.var
            FROM stock_sectors t1 JOIN stocks_var_year t2 ON t1.ticker = t2.ticker 
        ) t3
    ) t4
    WHERE t4.rn = 1;

-- TASK A: variazione percentuale della quotazione dei settori per anno
CREATE TABLE IF NOT EXISTS sectors_var_year AS
    SELECT t4.sector, t4.year_, ROUND(((t4.total_end - t4.total_start) / t4.total_start * 100), 2) as var
    FROM (
        SELECT t3.sector, t3.date_2 as year_, SUM(t3.close_price_1) as total_start, SUM(t3.close_price_2) as total_end
        FROM (
            SELECT t.sector, t.ticker, t1.date_ as date_1, t1.close_price as close_price_1, t2.year_ as date_2, t2.close_price as close_price_2
            FROM stock_sectors t
            JOIN first_stock_tx_by_year t1 ON (t.ticker = t1.ticker)
            JOIN last_stock_tx_by_year t2 ON (t.ticker = t2.ticker)
            WHERE t1.year_ = t2.year_
        ) t3
        GROUP BY t3.sector, t3.date_2
    ) t4;


CREATE TABLE IF NOT EXISTS stock_sectors_report AS
    SELECT t1.sector sector, t1.year_ year_, t1.var sector_var, t2.ticker best_stock_1_ticker, t2.var best_stock_1_var, t3.ticker best_stock_2_ticker, t3.total_volume best_stock_2_total_volume
    FROM sectors_var_year t1
    JOIN sectors_best_stock_by_increment_year t2 ON (t1.sector = t2.sector AND t1.year_ = t2.year_)
    JOIN sectors_best_stock_by_volumes_year t3 ON (t1.sector = t3.sector AND t1.year_ = t3.year_)
    ORDER BY t1.sector, t1.year_ ASC;


-- DROP TABLE IF EXISTS stock_prices_filtered;
-- DROP TABLE IF EXISTS sectors_total_price_var_year;
-- DROP TABLE IF EXISTS sectors_best_stock_by_increment_year;
-- DROP TABLE IF EXISTS stocks_increment_by_year;
-- DROP TABLE IF EXISTS last_stock_tx_by_year;
-- DROP TABLE IF EXISTS first_stock_tx_by_year;
-- DROP TABLE IF EXISTS sectors_best_stock_by_volumes_year;
-- DROP TABLE IF EXISTS stock_total_volumes_year;