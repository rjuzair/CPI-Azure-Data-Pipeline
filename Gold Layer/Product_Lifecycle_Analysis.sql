DROP EXTERNAL TABLE gold.vw_product_lifecycle;

CREATE EXTERNAL TABLE gold.vw_product_lifecycle
WITH (
    LOCATION = 'Product_Lifecycle_Analysis/',
    DATA_SOURCE = g_layer,
    FILE_FORMAT = f_parquet
) 
AS 
SELECT 
    p.productkey,
    p.productname,
    SUM(s.orderquantity) AS total_sold_last_12_months,
    SUM(CASE WHEN s.orderdate >= DATEADD(MONTH, -3, '2017-12-31') THEN s.orderquantity ELSE 0 END) AS sold_last_3_months,
    CASE 
        WHEN SUM(s.orderquantity) < 50 THEN 'New Product'
        WHEN SUM(s.orderquantity) > 500 
             AND SUM(CASE WHEN s.orderdate >= DATEADD(MONTH, -3, '2017-12-31') THEN s.orderquantity ELSE 0 END) 
                 > SUM(s.orderquantity) * 0.3 THEN 'Trending Product'
        ELSE 'Declining Product'
    END AS productstatus
FROM gold.sales s
JOIN gold.products p ON s.productkey = p.productkey
WHERE s.orderdate BETWEEN '2016-01-01' AND '2017-12-31'
GROUP BY p.productkey, p.productname;


SELECT * FROM gold.vw_product_lifecycle;

