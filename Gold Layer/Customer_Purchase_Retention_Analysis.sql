DROP EXTERNAL TABLE gold.vw_customer_retention;

CREATE EXTERNAL TABLE gold.vw_customer_retention
WITH (
    LOCATION = 'Customer_Purchase_Retention_Analysis/',
    DATA_SOURCE = g_layer,
    FILE_FORMAT = f_parquet
) 
AS 
SELECT 
    c.customerkey,
    CONCAT(c.firstname, ' ', c.lastname) AS customername,
    COUNT(DISTINCT s.ordernumber) AS total_orders,
    SUM(s.orderquantity * s.productprice) AS total_spent,
    AVG(s.orderquantity * s.productprice) AS avg_order_value,
    MAX(YEAR(s.orderdate)) AS last_purchase_year,
    CASE 
        WHEN MAX(YEAR(s.orderdate)) = 2015 THEN 'Churned in 2016 or 2017'
        WHEN MAX(YEAR(s.orderdate)) = 2016 THEN 'Churned in 2017'
        ELSE 'Active in 2017'
    END AS churn_risk
FROM gold.customers c
JOIN gold.sales s ON c.customerkey = s.customerkey
GROUP BY c.customerkey, c.firstname, c.lastname;


SELECT * FROM gold.vw_customer_retention;


