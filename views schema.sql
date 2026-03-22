--customer
CREATE VIEW gold.customer
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Customer/',
    FORMAT = 'PARQUET'
) AS query1;

--calender
CREATE VIEW gold.calender
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Calender/',
    FORMAT = 'PARQUET'
) AS query1;


--AdventureWorks_Product_Subcategories
CREATE VIEW gold.subcat
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
    FORMAT = 'PARQUET'
) AS query1;

--AdventureWorks_Products
CREATE VIEW gold.prod
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) AS query1;

--AdventureWorks_Returns
CREATE VIEW gold.ret
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) AS query1;

--AdventureWorks_Sales*
CREATE VIEW gold.sale
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Sales*/',
    FORMAT = 'PARQUET'
) AS query1;

--AdventureWorks_Territories
CREATE VIEW gold.ter
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://tejaswinifirststorage.blob.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) AS query1;


