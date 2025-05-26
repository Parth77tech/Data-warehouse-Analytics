/*
DATA TRANSFORMATION: silver.erp_loc_a101
>> Transform Bronze data and Insert into Silver


>> TO DO 
-- Inconsistent Primary Key (cid) which can be used to connect with crum_cust_info
-- ABBREVIATION CORRECTION

>> INSERT INTO Silver Table
*/

USE DataWarehouse;

-- ======================
-- Format Primary Key
-- ======================
-- cid
-- ERROR: An additional '-' hyphen
-- CORRECTION: Replace with no-space
-- ------------------------------------------------------------

SELECT * FROM bronze.erp_loc_a101;

SELECT 
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101;


-- ======================
-- ABBREVIATION CORRECTION
-- ======================
-- cid
-- ERROR: An additional '-' hyphen
-- CORRECTION: Replace with no-space
-- ------------------------------------------------------------

SELECT DISTINCT(cntry)
FROM bronze.erp_loc_a101
ORDER BY cntry;

SELECT
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'United States', 'USA') THEN 'USA'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;



/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/

INSERT INTO silver.erp_loc_a101 
	(cid,
	cntry
	)
SELECT
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'United States', 'USA') THEN 'USA'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;


-- CHECKING silver table
SELECT COUNT(*) FROM silver.erp_loc_a101;
SELECT * FROm silver.erp_loc_a101;