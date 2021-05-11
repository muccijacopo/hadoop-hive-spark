CREATE TABLE IF NOT EXISTS stock_prices_sectors AS
    SELECT t1.ticker as ticker, t1.date_ as date_, t1.volume as volume, t2.company as company, t2.sector as sector
    FROM (
        stock_prices t1 JOIN stock_sectors t2
        ON t1.ticker = t2.ticker
    )
    WHERE year(t1.date_) > 2008 AND year(t1.date_) < 2019;

-- 

-- CREATE TABLE sector_best_stock_by_year AS
--     SELECT *
--     FROM (
--         SELECT t1.ticker, t1.total_volume, t2.sector, ROW_NUMBER() OVER (PARTITION BY t2.sector ORDER BY t2.sector )
--         FROM (
--             stock_total_volumes as t1 
--             JOIN stock_prices_sectors as t2
--             ON t1.ticker = t2.ticker
--         )
        
--     )

-- CREATE TABLE sector_best_stock_by_year AS
--     SELECT *
--     FROM (
--         SELECT t2.sector as sector, year(t2.date_) as year_, MAX(t1.total_volume) as total_volume
--         FROM (
--             stock_total_volumes t1 JOIN stock_prices_sectors t2
--             ON t1.ticker = t2.ticker
--         )
--         GROUP BY t2.sector, year(t2.date_)
--     ) t3;

-- CREATE TABLE stock_total_volumes AS
--     SELECT ticker, year(date_) as year_, SUM(volume) as total_volume
--     FROM stock_prices_sectors
--     GROUP BY ticker, year(date_);


-- CREATE TABLE sector_stock_volume_year AS
--     SELECT t3.sector, t2.year_, t2.ticker, t2.total_volume
--     FROM stock_total_volumes t2 
--     JOIN stock_sectors t3 
--     ON t3.ticker = t2.ticker
--     ORDER BY t3.sector, t2.year_ DESC;

-- CREATE TABLE sector_stock_volumes AS
--     SELECT * 
--     FROM (
--         SELECT *, ROW_NUMBER() OVER (PARTITION BY sector, year_ ORDER BY sector, year_ DESC) rn
--         FROM sector_stock_volume_year
--     ) t2
--     WHERE t2.rn = 1

CREATE TABLE IF NOT EXISTS first_stock_tx_by_year AS
    SELECT *
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY ticker, year(date_) ASC) rn 
            FROM stock_prices  
            WHERE year(date_) > 2008 AND year(date_) < 2019
        ) as t1
        WHERE t1.rn = 1
        ORDER BY t1.ticker ASC
    ) t2;

CREATE TABLE IF NOT EXISTS last_stock_tx_by_year AS
    SELECT t2.ticker, year(t2.date_) as year_, t2.close_price
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY ticker, year(date_) DESC) rn 
            FROM stock_prices  
            WHERE year(date_) > 2008 AND year(date_) < 2019
        ) as t1
        WHERE t1.rn = 1
    ) t2
    ORDER BY t2.ticker, t2.date_ ASC;


-- CREATE TABLE IF NOT EXISTS stocks_increment_by_year AS
--     SELECT t1.ticker, year(t1.date_) as year_, (t2.close_price - t1.close_price) / t1.close_price * 100 as var
--     FROM first_stock_tx_by_year t1 
--     JOIN last_stock_tx_by_year t2
--     ON (t1.ticker = t2.ticker AND year(t1.date_) = year(t2.date_));

-- CREATE TABLE IF NOT EXISTS sectors_best_stock_by_increment_year AS
--     SELECT t4.sector, t4.year_, t4.ticker, t4.var
--     FROM (
--         SELECT t3.sector, t3.year_, t3.ticker, t3.var, ROW_NUMBER() OVER (PARTITION BY t3.sector, t3.year_ ORDER BY t3.sector, t3.year_, t3.var DESC) rn
--         FROM (
--             SELECT t1.sector, t2.year_, t2.ticker, t2.var
--             FROM stock_sectors t1 JOIN stocks_increment_by_year t2 ON t1.ticker = t2.ticker 
--         ) t3
--     ) t4
--     WHERE t4.rn = 1
--     ORDER BY t4.sector, t4.year_ ASC


CREATE TABLE IF NOT EXISTS sectors_stock_price_year AS
    SELECT t4.sector, t4.year_, (t4.total_end - t4.total_start) / t4.total_start * 100 as var
    FROM (
        SELECT t3.sector, t3.date_2 as year_, SUM(t3.close_price_1) as total_start, SUM(t3.close_price_2) as total_end
        FROM (
            SELECT t.sector, t.ticker, t1.date_ as date_1, t1.close_price as close_price_1, t2.year_ as date_2, t2.close_price as close_price_2
            FROM stock_sectors t
            JOIN first_stock_tx_by_year t1 ON (t.ticker = t1.ticker)
            JOIN last_stock_tx_by_year t2 ON (t.ticker = t2.ticker)
            WHERE year(t1.date_) = t2.year_
        ) t3
        GROUP BY t3.sector, t3.date_2
    ) t4
    ORDER BY t4.sector, t4.year_ ASC;

