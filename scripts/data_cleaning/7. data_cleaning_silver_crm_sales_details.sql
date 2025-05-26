/*
DATA TRANSFORMATION: silver.crm_sales_deatils
>> Transform Bronze data and Insert into Silver


>> TO DO 
-- Integer Dates to Dates
-- Integirity of Common Business rules


>> INSERT INTO Silver Table
*/

USE DataWarehouse;

-- ======================
-- Converting Integer to Dates
-- ======================
-- sls_order_dt, sls_ship_dt, sls_due_dt
-- ------------------------------------------------------------


SElECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM bronze.crm_sales_details;

-- Identify format of Integer date
-- yyyymmdd, Length = 8
-- Check Invalid dates

SElECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) !=8 ;


SElECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) !=8 ;


SElECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) !=8 ;


-- Converting Invalid dates to NULL
-- Converting Integer dates to Date (INT -> VARCHAR -> DATE)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt, 
CASE 
	WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
sls_ship_dt, 
CASE 
	WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
sls_due_dt,
CASE 
	WHEN sls_due_dt =0 OR LEN(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details;



-- Check for Invalid Date Orders
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt> sls_ship_dt OR sls_order_dt > sls_due_dt;




-- ======================
-- CHECK COMMON BUSINESS RULES
-- ======================
1) Sales = Quantity * Price
2) Sales CANNOT be Negative, 0 or NULL
-- ------------------------------------------------------------


SELECT 
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL
	OR sls_quantity IS NULL
	OR sls_price IS NULL
	OR sls_sales <=0
	OR sls_quantity <=0
	OR sls_price <=0
ORDER BY
	sls_sales,
	sls_quantity,
	sls_price;


-- SUGGESTED CORRECTIONS
-- If Sales is Negative/NULL/0; Sales = Quantity*Price
-- If Price is NEGATIV/NULL/0; Price = Sales/Quantity



SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt, 
CASE 
	WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
sls_ship_dt, 
CASE 
	WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
sls_due_dt,
CASE 
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
sls_sales,
CASE 
	WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
sls_quantity,
sls_price,
CASE 
	WHEN sls_price <= 0 OR sls_price IS NULL  THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;



/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/

SELECT * FROM bronze.crm_sales_details;
SELECT * FROM silver.crm_sales_details;

INSERT INTO silver.crm_sales_details 
	(sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
	WHEN sls_order_dt =0 OR LEN(sls_order_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
CASE 
	WHEN sls_ship_dt =0 OR LEN(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
CASE 
	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
CASE 
	WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
sls_quantity,
CASE 
	WHEN sls_price <= 0 OR sls_price IS NULL  THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;



-- Check insert
SELECT COUNT(*) FROM silver.crm_sales_details;
SELECT * FROM silver.crm_sales_details;

