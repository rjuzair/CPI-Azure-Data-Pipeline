-- =============================================
-- Create Views in the Gold Schema
-- Extracting Data from the Silver Layer
-- =============================================

------------------------
-- CREATE VIEW: Calendar
------------------------
CREATE OR ALTER VIEW gold.calendar AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Calenders/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Customers
------------------------
CREATE VIEW gold.customers AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Customers/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Products
------------------------
CREATE VIEW gold.products AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Products/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Returns
------------------------
CREATE VIEW gold.returns AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Returns/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Sales
------------------------
CREATE VIEW gold.sales AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Sales/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Subcategories
------------------------
CREATE VIEW gold.subcategories AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Sub_Categories/',
    FORMAT = 'PARQUET'
) AS source;

------------------------
-- CREATE VIEW: Productcategories
------------------------
CREATE VIEW gold.productcategories AS
SELECT *
FROM OPENROWSET(
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Product_Categories/',
    FORMAT = 'PARQUET'
) AS source;



------------------------
-- CREATE VIEW: Territories
------------------------
CREATE VIEW gold.territories AS
SELECT *
FROM OPENROWSET (
    BULK 'https://a1projectstorage.dfs.core.windows.net/silver/Territories/',
    FORMAT = 'PARQUET'
) AS source;

