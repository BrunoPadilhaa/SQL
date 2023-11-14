CREATE PROCEDURE [dbo].[load_Sales] AS

DROP TABLE IF EXISTS ##sales;

--CREATE A TEMP TABLE TO STORE THE DATA THAT NEEDS TO BE CLEANING
CREATE TABLE ##sales (
  invoiceID					VARCHAR(1000) NULL
, city						VARCHAR(1000) NULL
, customer					VARCHAR(1000) NULL
, gender					VARCHAR(1000) NULL
, productLine				VARCHAR(1000) NULL
, unitPrice					VARCHAR(1000) NULL
, quantity					VARCHAR(1000) NULL
, tax						VARCHAR(1000) NULL
, total						VARCHAR(1000) NULL
, date						VARCHAR(1000) NULL
, payment					VARCHAR(1000) NULL
, cogs						VARCHAR(1000) NULL
, grossMarginPercentage		VARCHAR(1000) NULL
, groosIncome				VARCHAR(1000) NULL
, rating					VARCHAR(1000) NULL
)


-- INSERT THE DATA FROM THE TXT FILE TO THE TEMP TABLE
BULK INSERT ##sales
FROM 'C:\Users\Bruno Padilha\OneDrive\01. Data\06. SQL\01. Data Cleaning\supermarket_sales.txt'
WITH (
		FIELDTERMINATOR = '\t',
		ROWTERMINATOR = '\n',
		CODEPAGE = 'utf8',
		FIRSTROW = 2
	);


--REMOVE DUPLICATE DATA
WITH DuplicateRecords AS (
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY invoiceID ORDER BY (SELECT NULL)) AS RowNum
	FROM ##sales
	)  DELETE FROM DuplicateRecords WHERE RowNum > 1


--TRUNCATE THE DATA IN THE TARGET TABLE
TRUNCATE TABLE sales;


--INSERT THE NEW DATA
INSERT INTO sales
	SELECT
		invoiceID
	,	city
	,	UPPER(SUBSTRING(customer,1,1)) + LOWER(SUBSTRING(customer, 2,CHARINDEX(' ',customer)-1)) +
	    UPPER(SUBSTRING(customer,CHARINDEX(' ',customer)+1,1)) + LOWER(SUBSTRING(customer, CHARINDEX(' ', customer)+2,99))											AS customerName
	,	CASE
			WHEN gender = 'Female' then 'F' 
			WHEN gender = 'Male'   then 'M' 
		ELSE gender END																																				AS gender
	,   CASE
	      WHEN LEN(productLine) - LEN(REPLACE(productLine,' ','')) = 1 THEN
	      	UPPER(LEFT(productLine, 1)) + LOWER(SUBSTRING(productLine,2,CHARINDEX(' ', productLine)-1)) + 
				UPPER(SUBSTRING(productLine,CHARINDEX(' ', productLine)+1,1)) + LOWER(SUBSTRING(productLine,CHARINDEX(' ', productLine)+2,99))
	      ELSE
	      	UPPER(LEFT(productLine, 1)) + LOWER(SUBSTRING(productLine,2,CHARINDEX(' ', productLine)-1)) + 'and ' +
				UPPER(SUBSTRING(productLine, CHARINDEX('and',productLine)+4, 1)) + LOWER(SUBSTRING(productLine, CHARINDEX('and',productLine)+5, 99))
	      END																																						AS productLine
	,	ROUND(CAST(REPLACE(REPLACE(unitPrice,'€',''),'EUR','') AS FLOAT),2)																							AS unitPrice
	,   quantity
	,	ROUND(CAST(tax AS FLOAT),2)																																	AS tax
	,	ROUND(CAST(total AS FLOAT),2)																																AS total
	,   TRY_CONVERT(datetime,REPLACE(REPLACE(REPLACE(date,'st',''),'nd',''),'th',''))																				AS date
	,	CASE 
		WHEN LEN(payment) - LEN(REPLACE(payment,' ','')) = 0 THEN
				UPPER(LEFT(payment,1)) + LOWER(SUBSTRING(payment,2,99))
		WHEN LEN(payment) - LEN(REPLACE(payment,' ','')) = 1 THEN
				UPPER(LEFT(payment,1)) + LOWER(SUBSTRING(payment, 2,CHARINDEX(' ',payment + ' ')-2)) + ' '
			  + UPPER(SUBSTRING(payment,CHARINDEX(' ',payment)+1,1))
			  + LOWER(SUBSTRING(payment,CHARINDEX(' ',payment)+2,99))
				END																																					AS payment
	,	ROUND(CAST(cogs AS FLOAT),2)																																AS cogs
	,	ROUND(CAST(grossMarginPercentage AS FLOAT),2)																												AS grossMarginPercentage
	,	ROUND(CAST(groosIncome AS FLOAT),2)																															AS groosIncome
	,	ROUND(CAST(rating AS FLOAT),2)																																AS rating
	FROM ##sales
