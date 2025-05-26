/*
DATA TRANSFORMATION: silver.crm_prd_info
>> Transform Bronze data and Insert into Silver


>> TO DO 
-- Split prd_key into category_id to unable table joins
-- HANDLING NUll, replacing with '0'
-- ABBREVIATIONS HANDLING - Data Enrichment
-- DATES VALIDATION


>> INSERT INTO Silver Table
*/

USE DataWarehouse;



-- ======================
-- SPLIT COLUMNs
-- ======================
-- prd_key
-- Split column : Create new cat_id & prd_key
-- Replace '-' with '_' to match id of 'bronze.erp_px_cat_gv12'
-- ------------------------------------------------------------

-- >> SPLIT
SELECT
prd_id,
prd_key,
SUBSTRING(prd_key, 1, 5) AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;

-- Check it with the related table
SELECT DISTINCT id
FROM bronze.erp_px_cat_gv12;


-- >> REPLACE  '-' with '_' 

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;


-- >> prd_key with Remaining Letter
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;

-- Check prd_key with Related table
SELECT sls_prd_key 
FROM bronze.crm_sales_details;



-- ======================
-- HANDLING NUll, replacing with '0'
-- ======================
-- prd_cost
-- ------------------------------------------------------------

-- CHECK
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL;

-- Replace
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;




-- ======================
-- ABBREVIATIONS HANDLING
-- ======================
-- prd_line
-- ------------------------------------------------------------


-- CHECK
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Replacing
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;



-- >> Dates Validation
-- start date must be younger than end date
-- there should not be any overlapping period

-- ======================
-- DATES VALIDATION
-- ======================
-- prd_start_dt, prd_end_dt
-- start date must be younger than end date
-- there should not be any overlapping period
-- ------------------------------------------------------------


SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
DATEADD(DAY, -1, LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info;


/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/


INSERT INTO silver.crm_prd_info 
	(prd_id, 
	 cat_id, 
	 prd_key,
	 prd_nm ,
	prd_cost,
	prd_line ,
	prd_start_dt,
	prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
DATEADD(DAY, -1, LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
FROM bronze.crm_prd_info;


-- CHECKING silver table
SELECT COUNT(*) FROm silver.crm_prd_info;
SELECT * FROm silver.crm_prd_info;



