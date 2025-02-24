DROP EXTERNAL TABLE gold.vw_sales_by_region;

CREATE EXTERNAL TABLE gold.vw_sales_by_region
WITH (
    LOCATION = 'Sales_By_Region/',
    DATA_SOURCE = g_layer,
    FILE_FORMAT = f_parquet
) 
AS 
SELECT 
    t.region,
    t.country,
    FORMAT(s.orderdate, 'yyyy-MM') AS sales_month,
    SUM(s.orderquantity * s.productprice) AS total_revenue,
    LAG(SUM(s.orderquantity * s.productprice), 1) OVER (
        PARTITION BY t.region, t.country ORDER BY YEAR(s.orderdate), MONTH(s.orderdate)
    ) AS prev_month_revenue,
    CASE 
        WHEN LAG(SUM(s.orderquantity * s.productprice), 1) OVER (
            PARTITION BY t.region, t.country ORDER BY YEAR(s.orderdate), MONTH(s.orderdate)
        ) > 0 
        THEN 
            ((SUM(s.orderquantity * s.productprice) - LAG(SUM(s.orderquantity * s.productprice), 1) OVER (
                PARTITION BY t.region, t.country ORDER BY YEAR(s.orderdate), MONTH(s.orderdate)
            )) / LAG(SUM(s.orderquantity * s.productprice), 1) OVER (
                PARTITION BY t.region, t.country ORDER BY YEAR(s.orderdate), MONTH(s.orderdate)
            )) * 100
        ELSE NULL
    END AS growth_rate_percentage
FROM gold.sales s
JOIN gold.territories t ON s.territorykey = t.salesterritorykey
WHERE YEAR(s.orderdate) BETWEEN 2015 AND 2017
GROUP BY t.region, t.country, YEAR(s.orderdate), MONTH(s.orderdate), FORMAT(s.orderdate, 'yyyy-MM');


SELECT * FROM gold.vw_sales_by_region;