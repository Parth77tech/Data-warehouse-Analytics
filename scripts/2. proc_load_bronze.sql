-- DATA IMPORTING: BULK INSERT FROM CSV
-- TRUNCATE AND INSERT

EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	PRINT '=====================================';
	PRINT 'Loading Bronze Layer';
	PRINT '=====================================';
	
	
	PRINT '=====================================';
	PRINT 'Load CRM Tabs';
	PRINT '=====================================';
	TRUNCATE TABLE bronze.crm_cust_info;
	BULK INSERT bronze.crm_cust_info
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.crm_cust_info;
	SELECT * FROM bronze.crm_cust_info;


	TRUNCATE TABLE bronze.crm_prd_info;
	BULK INSERT bronze.crm_prd_info
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.crm_prd_info;
	SELECT * FROM bronze.crm_prd_info;


	TRUNCATE TABLE bronze.crm_sales_details;
	BULK INSERT bronze.crm_sales_details
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.crm_sales_details;
	SELECT * FROM bronze.crm_sales_details;

	PRINT '=====================================';
	PRINT 'Load ERP Tabs';
	PRINT '=====================================';
	TRUNCATE TABLE bronze.erp_cust_az12;
	BULK INSERT bronze.erp_cust_az12
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2, 
		FIELDTERMINATOR = ',',
		TABLOCK
	)
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.erp_cust_az12;
	SELECT * FROM bronze.erp_cust_az12;


	TRUNCATE TABLE bronze.erp_loc_a101;
	BULK INSERT bronze.erp_loc_a101
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.erp_loc_a101;
	SELECT * FROM bronze.erp_loc_a101;



	TRUNCATE TABLE bronze.erp_px_cat_gv12;
	BULK INSERT bronze.erp_px_cat_gv12
	FROM 'D:\MS SQL SERVER\Data With Barra\0. sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2, 
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	-- Checking correct upate
	SELECT COUNT(*) FROM bronze.erp_px_cat_gv12;
	SELECT * FROM bronze.erp_px_cat_gv12;
END

