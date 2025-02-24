CREATE OR ALTER VIEW gold.vw_monthly_sales AS
SELECT 
    YEAR(orderdate) AS sales_year,
    MONTH(orderdate) AS sales_month,
    CONCAT(YEAR(orderdate), '-', FORMAT(MONTH(orderdate), '00')) AS sales_month_formatted,
    SUM(orderquantity * productprice) AS total_revenue
FROM gold.sales
WHERE YEAR(orderdate) BETWEEN 2015 AND 2017
GROUP BY YEAR(orderdate), MONTH(orderdate);

DROP EXTERNAL TABLE gold.vw_sales_anomalies;

CREATE EXTERNAL TABLE gold.vw_sales_anomalies
WITH (
    LOCATION = 'Sales_Anomalies/',
    DATA_SOURCE = g_layer,
    FILE_FORMAT = f_parquet
) 
AS 
SELECT 
    sales_year,
    sales_month,
    sales_month_formatted,
    total_revenue,
    LAG(total_revenue, 1) OVER (ORDER BY sales_year, sales_month) AS prev_month_revenue,
    total_revenue - LAG(total_revenue, 1) OVER (ORDER BY sales_year, sales_month) AS revenue_change,
    CASE 
        WHEN total_revenue > LAG(total_revenue, 1) OVER (ORDER BY sales_year, sales_month) * 1.2 THEN 'Unusual Increase'
        WHEN total_revenue < LAG(total_revenue, 1) OVER (ORDER BY sales_year, sales_month) * 0.8 THEN 'Unusual Drop'
        ELSE 'Normal'
    END AS anomaly_status
FROM gold.vw_monthly_sales;



SELECT * FROM gold.vw_sales_anomalies;