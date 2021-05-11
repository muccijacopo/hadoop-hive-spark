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

CREATE TABLE IF NOT EXISTS first_stock_per_year AS
    SELECT *
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY ticker, year(date_) ASC) rn 
            FROM stock_prices  
        ) as t1
        WHERE t1.rn = 1
        ORDER BY t1.ticker ASC
    ) t2;

CREATE TABLE IF NOT EXISTS last_stock_per_year AS
    SELECT *
    FROM (
        SELECT t1.ticker, t1.date_, t1.close_price
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker, year(date_) ORDER BY ticker, year(date_) DESC) rn 
            FROM stock_prices  
            WHERE year(date_) > 2008 AND year(date_) < 2019
        ) as t1
        WHERE t1.rn = 1
        ORDER BY t1.ticker ASC
    ) t2;


CREATE TABLE IF NOT EXISTS stock_first_last AS
    SELECT t1.ticker, t1.date_ as first_date, t1.close_price as first_close_price, t2.date_ as last_date, t2.close_price as last_close_price
    FROM first_stock_per_year t1 
    JOIN last_stock_per_year t2
    ON (t1.ticker = t2.ticker AND year(t1.date_) = year(t2.date_))

-- CREATE TABLE stock_min_dates AS
--     SELECT *
--     FROM (
--         SELECT ticker, MIN(date_) as date_
--         FROM stock_prices  
--         GROUP BY ticker, year(date_)
--     ) t1 
--     ORDER BY t1.ticker, year(date_)

