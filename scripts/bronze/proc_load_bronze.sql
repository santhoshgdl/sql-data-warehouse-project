/*
  Stored Procedure: Load Bronze layer (Source -> Bronze)

  Sript Purpose: 
    This storage procedure loads the data into the 'bronze' schema from external CSV files.
    It performs the following actions: 
      - Truncates the bronze tables before loading data.
      - Uses the 'BULK - INSERT' command to load data from csv files to bronze tables.
*/

-- CREATE STORED PROCEDURE.
-- Hint: save frequently used SQL code in stored procedures in database.
-- check the database -> programmability -> stored procedures
-- TRY CATCH -> SQL runs the TRY CATCH Block, and if it fails, it runs the CATCH block to handle the error.

-- Track ETL Duration - Helps to identify bottle necks, optimize performance, monitor trends, detect issues.

-- Calculate the duration of loading bronze layer "Whole layer".

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT 'loading Bronze layer';
		RAISERROR('Loading Bronze layer', 0, 1) WITH NOWAIT;
		--Add prints - Add prints to track execution, debug issues and understand its flow.
		-- DEVELOP SQL LOAD SCRIPTS

		--TRUNCATE - quickly delete all the rows from a table, resetting it into an empty state.
		PRINT 'Loading CRM Tables';
		PRINT '>> Truncating table : bronze.crm_cust_info'

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data into : bronze.crm_cust_info '
		BULK INSERT bronze.crm_cust_info 
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK -- its a practice to increase the loading performance by locking the table.
		);
		SET @end_time = GETDATE();

		-- DATEDIFF() - calculates the difference between two dates, returns days, months or years.
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	    -- Quality Check - Check that the data has not shifted and is in the correct columns.

		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT 'Loading CRM Tables';
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Data Engineer\SQL\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @batch_end_time = GETDATE();

		PRINT 'Loading Bronze layer is Completed!'
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT 'Error Occurred during bronze layer ! ';
		PRINT 'Error Message ' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
	END CATCH
END

SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_px_cat_g1v2;


EXEC bronze.load_bronze;


