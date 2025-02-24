DROP EXTERNAL TABLE gold.vw_product_bundle_analysis;

CREATE EXTERNAL TABLE gold.vw_product_bundle_analysis
WITH (
    LOCATION = 'Product_Bundle_Analysis/',
    DATA_SOURCE = g_layer,
    FILE_FORMAT = f_parquet
) 
AS 
SELECT 
    s1.productkey AS product_1,
    s2.productkey AS product_2,
    COUNT(DISTINCT s1.ordernumber) AS times_bought_together
FROM gold.sales s1
JOIN gold.sales s2 
    ON s1.ordernumber = s2.ordernumber 
    AND s1.productkey <> s2.productkey
WHERE YEAR(s1.orderdate) BETWEEN 2015 AND 2017
GROUP BY s1.productkey, s2.productkey
HAVING COUNT(DISTINCT s1.ordernumber) > 10
ORDER BY times_bought_together DESC;


SELECT * FROM gold.vw_product_bundle_analysis;