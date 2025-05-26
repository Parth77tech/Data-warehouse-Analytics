/*
DATA TRANSFORMATION: silver.crm_cust_info
>> Transform Bronze data and Insert into Silver


>> TO DO
-- Check for Nulls or Duplicates in Primary key
-- Strings: Unwanted spaces
-- Data Standardization & Consistency in Category names
-- Data type: Date


>> INSERT INTO Silver Table
*/



USE DataWarehouse;
-- ======================
-- PRIMARY KEY CORRECTION
-- ======================
-- CRM Customer Info
-- PK: cst_id
-- ------------------------------------------------------------

SELECT * FROM bronze.crm_cust_info;

-- IDENTIFYING duplicates
SELECT 
	cst_id,
	COUNT(*) AS DuplicateCheck
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1 OR cst_id IS NULL;

-- Retaining latest records from the duplicate records
SELECT *
FROM (SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last_rec
	  FROM bronze.crm_cust_info
	  WHERE cst_id IS NOT NULL) AS temptab
WHERE flag_last_rec = 1;


-- ======================
-- UNWANTED WHITE SPACE
-- ======================
-- cst_firstname, cst_lastname, cst_material_status, cst_gndr
-- ------------------------------------------------------------

-- CHECK
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);


-- Updating query
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastnmae,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last_rec
	  FROM bronze.crm_cust_info
	  WHERE cst_id IS NOT NULL) AS temptab
WHERE flag_last_rec = 1;


-- ===================================
-- DATA STANDARDIZATION & CONSISTENCY
-- ===================================
-- cst_material_status, cst_gndr
-- ------------------------------------------------------------

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
SELECT 
	CASE cst_gndr
		WHEN UPPER(TRIM('F')) THEN 'Female'
		WHEN UPPER(TRIM('M')) THEN 'Male'
		ELSE 'unknown'
	END AS cst_gndr
FROM bronze.crm_cust_info;


SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
SELECT 
	CASE cst_marital_status
		WHEN UPPER(TRIM('S')) THEN 'Single'
		WHEN UPPER(TRIM('M')) THEN 'Married'
		ELSE 'unknown'
	END AS cst_marital_status
FROM bronze.crm_cust_info;


-- Updating query
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastnmae,
CASE cst_marital_status
		WHEN UPPER(TRIM('S')) THEN 'Single'
		WHEN UPPER(TRIM('M')) THEN 'Married'
		ELSE 'unknown'
	END AS cst_marital_status,
CASE cst_gndr
		WHEN UPPER(TRIM('F')) THEN 'Female'
		WHEN UPPER(TRIM('M')) THEN 'Male'
		ELSE 'unknown'
	END AS cst_gndr,
cst_create_date
FROM (SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last_rec
	  FROM bronze.crm_cust_info
	  WHERE cst_id IS NOT NULL) AS temptab
WHERE flag_last_rec = 1;



-- ======================
-- DATA TYPE
-- ======================
-- cst_create_date
-- ------------------------------------------------------------


EXEC sp_help 'bronze.crm_cust_info'
-- NO ACTION NEEDED


/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastnmae,
		CASE cst_marital_status
				WHEN UPPER(TRIM('S')) THEN 'Single'
				WHEN UPPER(TRIM('M')) THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
		CASE cst_gndr
				WHEN UPPER(TRIM('F')) THEN 'Female'
				WHEN UPPER(TRIM('M')) THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,
		cst_create_date
		FROM (SELECT *,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last_rec
			  FROM bronze.crm_cust_info
			  WHERE cst_id IS NOT NULL) AS temptab
		WHERE flag_last_rec = 1;

SELECT COUNT(*) FROM silver.crm_cust_info;
