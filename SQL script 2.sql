CREATE MASTER KEY  ENCRYPTION BY PASSWORD ='Tejaswini@123456' 
CREATE DATABASE SCOPED CREDENTIAL cred_teju
WITH
    IDENTITY ='managed identity'


CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION ='https://tejaswinifirststorage.blob.core.windows.net/silver',
    CREDENTIAL = cred_teju
)
CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION ='https://tejaswinifirststorage.blob.core.windows.net/gold',
    CREDENTIAL =cred_teju
)
CREATE EXTERNAL FILE FORMAT format_paraquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

---create external table

CREATE EXTERNAL TABLE gold_extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_paraquet
)
AS
SELECT * FROM gold.sale

SELECT * FROM gold_extsales