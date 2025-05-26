/*
DATA TRANSFORMATION: silver.erp_cust_az12
>> Transform Bronze data and Insert into Silver


>> TO DO 
-- Inconsistent Primary Key (cid)
-- Date range error in Birthdate
-- Abbreviation Consistency in Gender

>> INSERT INTO Silver Table
*/

USE DataWarehouse;

-- ======================
-- Inconsisten Primary Key
-- ======================
-- cid
-- New key 10 digits
-- Older key has additional 3 letters 'NAS'
-- Drop these three letters
-- ------------------------------------------------------------

SELECT DISTINCT(LEN(cid)) 
FROM bronze.erp_cust_az12;

SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, 13)
	ELSE cid
	END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12;


-- ==========================
-- Birthdate Range Correction
-- ==========================
-- bdate
-- ERROR: Emplyees aged more than 100 years or Not yet born
-- CORRECTION: NULL if bdate > Today
-- ------------------------------------------------------------

SELECT MAX(bdate), MIN(bdate)
FROM bronze.erp_cust_az12;

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
ORDER BY bdate DESC;


SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, 13)
	ELSE cid
	END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END AS bdate,
gen
FROM bronze.erp_cust_az12;


-- ==========================
-- CATEGORY ABBREVIATION CONSISTENCY
-- ==========================
-- gen
-- ERROR: Multiple categories for Male and Female
-- ------------------------------------------------------------

SELECT DISTINCT(gen) FROM bronze.erp_cust_az12;

SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, 13)
	ELSE cid
	END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END AS bdate,
CASE 
	WHEN gen LIKE 'F%' THEN 'Female'
	WHEN gen LIKE 'M%' THEN 'Male'
	ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;



/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/


INSERT INTO silver.erp_cust_az12 
	(cid,
	bdate,
	gen
	)
SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, 13)
	ELSE cid
	END AS cid,
CASE
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END AS bdate,
CASE 
	WHEN gen LIKE 'F%' THEN 'Female'
	WHEN gen LIKE 'M%' THEN 'Male'
	ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;


-- CHECKING silver table
SELECT COUNT(*) FROm silver.erp_cust_az12;
SELECT * FROm silver.erp_cust_az12;