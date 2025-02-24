CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'your_password';

CREATE DATABASE SCOPED CREDENTIAL a1cred
WITH IDENTITY = 'Managed Identity';

CREATE EXTERNAL DATA SOURCE SilverDataLake
WITH (
    LOCATION = 'https://a1projectstorage.dfs.core.windows.net/silver/',
    CREDENTIAL = a1cred
);


CREATE EXTERNAL DATA SOURCE g_layer
WITH (
    LOCATION = 'https://a1projectstorage.dfs.core.windows.net/gold',
    CREDENTIAL= a1cred
);

CREATE EXTERNAL FILE FORMAT f_parquet
WITH (
    FORMAT_TYPE = Parquet,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

SELECT * FROM sys.external_data_sources;

