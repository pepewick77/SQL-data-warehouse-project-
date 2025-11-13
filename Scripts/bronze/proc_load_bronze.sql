
ALTER PROCEDURE bronze.load_bronze
AS
BEGIN 

DECLARE @START_TIME DATETIME 
DECLARE @END_TIME DATETIME 
DECLARE @BATCH_START_TIME DATETIME 
DECLARE @BATCH_END_TIME DATETIME 

	BEGIN TRY
		
		SET @BATCH_START_TIME = GETDATE ()
		PRINT '==============================================================================================='
		PRINT '-- lOADING BRONZE LAYER --'
		PRINT '==============================================================================================='


		PRINT '-----------------------------------------------------------------------------------------------'
		PRINT '-- lOADING CRM TABLES'
		PRINT '-----------------------------------------------------------------------------------------------'

		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> INSERING DATA INTO TABLE: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'
		-----------------------------------------------------------------------------------------------
		-- bronze.crm_prd_info
		-----------------------------------------------------------------------------------------------
		
		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> INSERING DATA INTO TABLE: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'
		-----------------------------------------------------------------------------------------------
		-- bronze.crm_sales_details
		-----------------------------------------------------------------------------------------------
		
		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> INSERING DATA INTO TABLE: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'

		PRINT '-----------------------------------------------------------------------------------------------'
		PRINT '-- lOADING CRM TABLES'
		PRINT '-----------------------------------------------------------------------------------------------'


		-----------------------------------------------------------------------------------------------
		-- bronze.erp_cust_az12
		-----------------------------------------------------------------------------------------------
		
		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> INSERING DATA INTO TABLE: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'
		-----------------------------------------------------------------------------------------------
		-- bronze.erp_loc_a101
		-----------------------------------------------------------------------------------------------
		
		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> INSERING DATA INTO TABLE: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'
		-----------------------------------------------------------------------------------------------
		-- bronze.erp_px_cat_g1v2
		-----------------------------------------------------------------------------------------------
		
		SET @START_TIME = GETDATE ()
		PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> INSERING DATA INTO TABLE: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\Priyanjana\OneDrive\Desktop\Projects\SQL\SQL warehouse Project\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH 
		(
			FIRSTROW = 2
		,	FIELDTERMINATOR = ','
		,	TABLOCK
		)
		SET @END_TIME = GETDATE ()
		PRINT '>> LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' SECONDS'
		PRINT '>> -------------------------------------------------------------------------------------'

		SET @BATCH_END_TIME = GETDATE ()
		PRINT '========================================================================================'
		PRINT 'LOADING BRONZE LAYER IS COMPLETE:'
		PRINT 'TOTAL DURATION: ' + CAST(DATEDIFF(SECOND, @BATCH_START_TIME, @BATCH_END_TIME) AS NVARCHAR) + ' SECONDS' 
		PRINT '========================================================================================'

	END TRY

	BEGIN CATCH 

		
		PRINT '==============================================================================================='
		PRINT '-- ERRROR OCCURED DURING LOADING BRONZE LAYER --'
		PRINT '-- ERROR MESSAGE ' + ERROR_MESSAGE ()
		PRINT '-- ERROR MESSAGE ' + CAST (ERROR_NUMBER () AS NVARCHAR)
		PRINT '-- ERROR MESSAGE ' + CAST (ERROR_STATE () AS NVARCHAR)
		PRINT '==============================================================================================='


	END CATCH

END
