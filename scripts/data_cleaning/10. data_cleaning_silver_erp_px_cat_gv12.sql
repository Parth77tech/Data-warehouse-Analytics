/*
DATA TRANSFORMATION: silver.erp_px_cat_gv12
>> Transform Bronze data and Insert into Silver


>> TO DO 
-- No Action Needed

>> INSERT INTO Silver Table
*/

USE DataWarehouse;
SELECT * FROM bronze.erp_px_cat_gv12;

/*
-- =======================================
>> INSERTING CLEAN DATA INTO SILVER TABLE
-- =======================================
*/

INSERT INTO silver.erp_px_cat_gv12 
	(id,
	cat,
	subcat,
	maintenance)
SELECT 
	id,
	cat,
	subcat,
	maintenance 
FROM bronze.erp_px_cat_gv12;


-- CHECKING silver table
SELECT COUNT(*) FROM silver.erp_px_cat_gv12;
SELECT * FROm silver.erp_px_cat_gv12;