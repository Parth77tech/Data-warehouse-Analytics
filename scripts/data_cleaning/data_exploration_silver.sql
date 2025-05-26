/*
Initial data Exploration was conducted to understand the relationships among the tables.
----------------------------------------------------------------------------------------
1) Identifying Primary keys and Foreign key constraints to establish table linkages.
2) Assessing data types and structure for consistency.
3) Determining necessary data transformations (e.g., normalization, data type conversions, handling nulls) to prepare for analysis or reporting.



*/

USE DataWarehouse;

SELECT TOP (1000) [cst_id]
      ,[cst_key]
      ,[cst_firstname]
      ,[cst_lastname]
      ,[cst_marital_status]
      ,[cst_gndr]
      ,[cst_create_date]
  FROM [DataWarehouse].[bronze].[crm_cust_info];


SELECT TOP (1000) [prd_id]
      ,[prd_key]
      ,[prd_nm]
      ,[prd_cost]
      ,[prd_line]
      ,[prd_start_dt]
      ,[prd_end_dt]
  FROM [DataWarehouse].[bronze].[crm_prd_info];

SELECT TOP (1000) [sls_ord_num]
      ,[sls_prd_key]
      ,[sls_cust_id]
      ,[sls_order_dt]
      ,[sls_ship_dt]
      ,[sls_due_dt]
      ,[sls_sales]
      ,[sls_quantity]
      ,[sls_price]
  FROM [DataWarehouse].[bronze].[crm_sales_details];


SELECT TOP (1000) [cid]
      ,[bdate]
      ,[gen]
  FROM [DataWarehouse].[bronze].[erp_cust_az12];


SELECT TOP (1000) [cid]
      ,[cntry]
  FROM [DataWarehouse].[bronze].[erp_loc_a101];


SELECT TOP (1000) [id]
      ,[cat]
      ,[subcat]
      ,[maintenance]
  FROM [DataWarehouse].[bronze].[erp_px_cat_gv12];
